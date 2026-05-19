//! ws_recorder — enregistreur de ticks WebSocket pour la phase de recon BBOT.
//!
//! Se connecte aux WebSockets de Lighter et Hyperliquid, reconstruit le carnet
//! L2 de chaque venue et écrit chaque mise à jour (haut du carnet, profondeur N)
//! dans un fichier JSONL pour analyse ultérieure (`feed_analyzer`, `spread_replay`).
//!
//! Chaque tick contient : timestamp local de réception (ns monotone + epoch ms),
//! timestamp serveur de la venue, numéro de séquence, et les N meilleurs niveaux.
//!
//! Usage :
//!   cargo run --release --bin ws_recorder
//!   cargo run --release --bin ws_recorder -- --symbols BTC,ETH --duration 3600 --depth 20
//!
//! Sortie : data/ticks_YYYYMMDD_HHMMSS.jsonl
//!
//! NOTE recon : le format exact du canal `l2Book` d'Hyperliquid (snapshot vs
//! delta, profondeur) est précisément un point à confirmer — le recorder logge
//! les premiers messages non reconnus pour faciliter le diagnostic.

use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use exchanges::tick::{Level, Tick};

// ============================ Configuration =================================

const LIGHTER_WS_URL: &str = "wss://mainnet.zklighter.elliot.ai/stream";
const LIGHTER_REST_MARKETS: &str =
    "https://mainnet.zklighter.elliot.ai/api/v1/orderBookDetails";
const HYPERLIQUID_WS_URL: &str = "wss://api.hyperliquid.xyz/ws";

/// Durée d'enregistrement par défaut : 24 h.
const DEFAULT_DURATION_SECS: u64 = 86_400;
/// Nombre de niveaux de carnet conservés par côté, par défaut.
const DEFAULT_DEPTH: usize = 20;
/// Le writer vide son tampon disque tous les N secondes.
const FLUSH_INTERVAL: Duration = Duration::from_secs(5);
/// Si aucun message WS n'arrive en 60 s, la connexion est considérée morte.
const WS_READ_TIMEOUT: Duration = Duration::from_secs(60);
/// Intervalle entre deux pings applicatifs.
const PING_INTERVAL: Duration = Duration::from_secs(20);
/// Délai avant reconnexion après une coupure.
const RECONNECT_DELAY: Duration = Duration::from_secs(2);
/// Échelle entière utilisée comme clé du carnet Lighter (prix * 1e6).
const PRICE_SCALE: f64 = 1_000_000.0;

// ============================ Helpers =======================================

/// Horodatage local courant en millisecondes depuis l'epoch.
fn epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Tronque une chaîne à `n` caractères (pour des logs lisibles).
fn truncate(s: &str, n: usize) -> String {
    let t: String = s.chars().take(n).collect();
    if t.len() < s.len() {
        format!("{t}…")
    } else {
        t
    }
}

// ============================ Arguments =====================================

struct Args {
    symbols: Vec<String>,
    duration: Duration,
    depth: usize,
    out_dir: String,
}

fn parse_args() -> Args {
    let argv: Vec<String> = std::env::args().collect();
    let get = |flag: &str| -> Option<String> {
        argv.iter()
            .position(|a| a == flag)
            .and_then(|i| argv.get(i + 1))
            .cloned()
    };

    let symbols = get("--symbols")
        .map(|s| {
            s.split(',')
                .map(|x| x.trim().to_uppercase())
                .filter(|x| !x.is_empty())
                .collect()
        })
        .unwrap_or_else(|| vec!["BTC".into(), "ETH".into(), "SOL".into()]);

    let duration = Duration::from_secs(
        get("--duration")
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_DURATION_SECS),
    );

    let depth = get("--depth")
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_DEPTH);

    let out_dir = get("--out").unwrap_or_else(|| "data".to_string());

    Args {
        symbols,
        duration,
        depth,
        out_dir,
    }
}

// ============================ Writer ========================================

/// Tâche d'écriture : reçoit les ticks et les écrit en JSONL, un par ligne.
async fn writer_task(
    mut rx: mpsc::UnboundedReceiver<Tick>,
    path: String,
    cancel: Arc<AtomicBool>,
) {
    let file = std::fs::File::create(&path).expect("création du fichier de sortie impossible");
    let mut w = std::io::BufWriter::new(file);

    let mut last_flush = Instant::now();
    let mut total: u64 = 0;
    let mut lighter: u64 = 0;
    let mut hyper: u64 = 0;

    loop {
        match tokio::time::timeout(Duration::from_millis(500), rx.recv()).await {
            Ok(Some(tick)) => {
                match serde_json::to_string(&tick) {
                    Ok(line) => {
                        if writeln!(w, "{line}").is_err() {
                            eprintln!("[writer] échec d'écriture sur disque, arrêt");
                            break;
                        }
                        total += 1;
                        match tick.exchange.as_str() {
                            "lighter" => lighter += 1,
                            "hyperliquid" => hyper += 1,
                            _ => {}
                        }
                    }
                    Err(e) => eprintln!("[writer] sérialisation impossible: {e}"),
                }
                if last_flush.elapsed() >= FLUSH_INTERVAL {
                    let _ = w.flush();
                    last_flush = Instant::now();
                    eprintln!("[writer] {total} ticks (lighter={lighter}, hyperliquid={hyper})");
                }
            }
            Ok(None) => break, // tous les readers ont fermé le canal
            Err(_) => {
                if cancel.load(Ordering::Relaxed) {
                    break;
                }
            }
        }
    }

    let _ = w.flush();
    eprintln!(
        "[writer] terminé : {total} ticks (lighter={lighter}, hyperliquid={hyper}) → {path}"
    );
}

// ============================ Lighter =======================================

/// État local d'un carnet Lighter, reconstruit à partir des snapshots/deltas.
struct LighterBook {
    symbol: String,
    bids: BTreeMap<i64, f64>,
    asks: BTreeMap<i64, f64>,
}

/// Résout les `market_id` Lighter pour les symboles demandés via l'API REST.
async fn resolve_lighter_markets(symbols: &[String]) -> Vec<(String, u8)> {
    let resp = match reqwest::get(LIGHTER_REST_MARKETS).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("[lighter] requête REST orderBookDetails échouée: {e}");
            return vec![];
        }
    };
    let val: serde_json::Value = match resp.json().await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("[lighter] réponse JSON orderBookDetails invalide: {e}");
            return vec![];
        }
    };

    let mut out = Vec::new();
    if let Some(markets) = val["order_book_details"].as_array() {
        for sym in symbols {
            let id = markets
                .iter()
                .find(|m| m["symbol"].as_str() == Some(sym.as_str()))
                .and_then(|m| m["market_id"].as_u64());
            match id {
                Some(id) => {
                    eprintln!("[lighter] {sym} → market_id={id}");
                    out.push((sym.clone(), id as u8));
                }
                None => eprintln!("[lighter] ATTENTION: symbole {sym} introuvable, ignoré"),
            }
        }
    } else {
        eprintln!("[lighter] champ order_book_details absent de la réponse REST");
    }
    out
}

/// Applique un delta Lighter (liste de niveaux) à un côté du carnet local.
/// Une taille nulle signifie « supprimer ce niveau ».
fn apply_lighter_delta(map: &mut BTreeMap<i64, f64>, arr: &serde_json::Value) {
    if let Some(levels) = arr.as_array() {
        for lvl in levels {
            let price = lvl["price"]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .or_else(|| lvl["price"].as_f64());
            let size = lvl["size"]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .or_else(|| lvl["size"].as_f64());
            if let (Some(p), Some(q)) = (price, size) {
                let key = (p * PRICE_SCALE).round() as i64;
                if q <= 0.0 {
                    map.remove(&key);
                } else {
                    map.insert(key, q);
                }
            }
        }
    }
}

/// Extrait les `depth` meilleurs niveaux d'un carnet Lighter.
fn lighter_top_levels(book: &LighterBook, depth: usize) -> (Vec<Level>, Vec<Level>) {
    let bids = book
        .bids
        .iter()
        .rev()
        .take(depth)
        .map(|(&k, &q)| Level {
            price: k as f64 / PRICE_SCALE,
            size: q,
        })
        .collect();
    let asks = book
        .asks
        .iter()
        .take(depth)
        .map(|(&k, &q)| Level {
            price: k as f64 / PRICE_SCALE,
            size: q,
        })
        .collect();
    (bids, asks)
}

/// Reader Lighter : se connecte, souscrit aux carnets, reconstruit et émet les ticks.
async fn lighter_reader(
    markets: Vec<(String, u8)>,
    depth: usize,
    tx: mpsc::UnboundedSender<Tick>,
    start: Instant,
    cancel: Arc<AtomicBool>,
) {
    if markets.is_empty() {
        eprintln!("[lighter] aucun marché à enregistrer, reader arrêté");
        return;
    }
    let mut unparsed_logged = 0u32;

    while !cancel.load(Ordering::Relaxed) {
        eprintln!("[lighter] connexion à {LIGHTER_WS_URL}");
        let ws = match connect_async(LIGHTER_WS_URL).await {
            Ok((ws, _)) => ws,
            Err(e) => {
                eprintln!("[lighter] connexion échouée: {e}, nouvelle tentative dans 3s");
                tokio::time::sleep(Duration::from_secs(3)).await;
                continue;
            }
        };
        let (sink, mut stream) = ws.split();
        let sink = Arc::new(tokio::sync::Mutex::new(sink));

        // Souscription : un canal order_book par marché.
        {
            let mut s = sink.lock().await;
            for (_sym, id) in &markets {
                let sub = serde_json::json!({
                    "type": "subscribe",
                    "channel": format!("order_book/{id}"),
                });
                if let Err(e) = s.send(Message::Text(sub.to_string())).await {
                    eprintln!("[lighter] souscription order_book/{id} échouée: {e}");
                }
            }
        }
        eprintln!("[lighter] souscrit à {} marché(s)", markets.len());

        // Tâche de ping applicatif.
        let ping_sink = sink.clone();
        let ping_cancel = cancel.clone();
        let ping = tokio::spawn(async move {
            loop {
                tokio::time::sleep(PING_INTERVAL).await;
                if ping_cancel.load(Ordering::Relaxed) {
                    break;
                }
                let mut s = ping_sink.lock().await;
                if s.send(Message::Ping(vec![])).await.is_err() {
                    break;
                }
            }
        });

        // Carnets locaux, indexés par market_id.
        let mut books: HashMap<u8, LighterBook> = markets
            .iter()
            .map(|(sym, id)| {
                (
                    *id,
                    LighterBook {
                        symbol: sym.clone(),
                        bids: BTreeMap::new(),
                        asks: BTreeMap::new(),
                    },
                )
            })
            .collect();

        // Boucle de lecture.
        loop {
            if cancel.load(Ordering::Relaxed) {
                break;
            }
            let msg = match tokio::time::timeout(WS_READ_TIMEOUT, stream.next()).await {
                Ok(Some(Ok(m))) => m,
                Ok(Some(Err(e))) => {
                    eprintln!("[lighter] erreur WS: {e}");
                    break;
                }
                Ok(None) => {
                    eprintln!("[lighter] flux terminé");
                    break;
                }
                Err(_) => {
                    eprintln!("[lighter] timeout de lecture (60s)");
                    break;
                }
            };
            let text = match msg {
                Message::Text(t) => t,
                Message::Close(_) => {
                    eprintln!("[lighter] connexion fermée par le serveur");
                    break;
                }
                _ => continue,
            };

            let val: serde_json::Value = match serde_json::from_str(&text) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let msg_type = val["type"].as_str().unwrap_or("");

            // Ping protocolaire de Lighter → répondre pong.
            if msg_type == "ping" {
                let mut s = sink.lock().await;
                let _ = s.send(Message::Text(r#"{"type":"pong"}"#.to_string())).await;
                continue;
            }
            if msg_type == "connected" {
                continue;
            }

            let is_snapshot = msg_type == "subscribed/order_book";
            if !is_snapshot && msg_type != "update/order_book" {
                if unparsed_logged < 3 {
                    eprintln!("[lighter] message non reconnu: {}", truncate(&text, 200));
                    unparsed_logged += 1;
                }
                continue;
            }

            // Le marché est identifié par le champ "channel" : "order_book:0".
            let market_id = match val["channel"]
                .as_str()
                .and_then(|c| c.rsplit(':').next())
                .and_then(|n| n.parse::<u8>().ok())
            {
                Some(id) => id,
                None => continue,
            };
            let book = match books.get_mut(&market_id) {
                Some(b) => b,
                None => continue,
            };
            if is_snapshot {
                book.bids.clear();
                book.asks.clear();
            }

            if let Some(ob) = val.get("order_book") {
                apply_lighter_delta(&mut book.bids, &ob["bids"]);
                apply_lighter_delta(&mut book.asks, &ob["asks"]);
                let (bids, asks) = lighter_top_levels(book, depth);
                if bids.is_empty() && asks.is_empty() {
                    continue;
                }
                let tick = Tick {
                    recv_ns: start.elapsed().as_nanos() as u64,
                    recv_ms: epoch_ms(),
                    server_ms: val["timestamp"].as_u64().unwrap_or(0),
                    exchange: "lighter".to_string(),
                    symbol: book.symbol.clone(),
                    seq: ob["nonce"].as_u64().unwrap_or(0),
                    prev_seq: ob["begin_nonce"].as_u64().unwrap_or(0),
                    is_snapshot,
                    bids,
                    asks,
                };
                if tx.send(tick).is_err() {
                    return; // canal fermé : le recorder s'arrête
                }
            }
        }

        ping.abort();
        if cancel.load(Ordering::Relaxed) {
            break;
        }
        eprintln!("[lighter] déconnecté, reconnexion dans 2s");
        tokio::time::sleep(RECONNECT_DELAY).await;
    }
    eprintln!("[lighter] reader arrêté");
}

// ============================ Hyperliquid ===================================

/// Parse un côté du carnet l2Book Hyperliquid (liste de niveaux `{px, sz, n}`).
fn hyperliquid_side(side: &serde_json::Value, depth: usize) -> Vec<Level> {
    side.as_array()
        .map(|arr| {
            arr.iter()
                .take(depth)
                .filter_map(|lvl| {
                    let price = lvl["px"].as_str().and_then(|s| s.parse::<f64>().ok())?;
                    let size = lvl["sz"].as_str().and_then(|s| s.parse::<f64>().ok())?;
                    Some(Level { price, size })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Reader Hyperliquid : se connecte, souscrit au canal l2Book et émet les ticks.
async fn hyperliquid_reader(
    symbols: Vec<String>,
    depth: usize,
    tx: mpsc::UnboundedSender<Tick>,
    start: Instant,
    cancel: Arc<AtomicBool>,
) {
    if symbols.is_empty() {
        eprintln!("[hyperliquid] aucun symbole, reader arrêté");
        return;
    }
    let mut unparsed_logged = 0u32;

    while !cancel.load(Ordering::Relaxed) {
        eprintln!("[hyperliquid] connexion à {HYPERLIQUID_WS_URL}");
        let ws = match connect_async(HYPERLIQUID_WS_URL).await {
            Ok((ws, _)) => ws,
            Err(e) => {
                eprintln!("[hyperliquid] connexion échouée: {e}, nouvelle tentative dans 3s");
                tokio::time::sleep(Duration::from_secs(3)).await;
                continue;
            }
        };
        let (sink, mut stream) = ws.split();
        let sink = Arc::new(tokio::sync::Mutex::new(sink));

        // Souscription l2Book, 50 ms entre chaque (HL coupe la connexion en cas de rafale).
        for coin in &symbols {
            let sub = serde_json::json!({
                "method": "subscribe",
                "subscription": { "type": "l2Book", "coin": coin },
            });
            {
                let mut s = sink.lock().await;
                if let Err(e) = s.send(Message::Text(sub.to_string())).await {
                    eprintln!("[hyperliquid] souscription l2Book {coin} échouée: {e}");
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        eprintln!("[hyperliquid] souscrit à {} symbole(s)", symbols.len());

        // Tâche de ping applicatif.
        let ping_sink = sink.clone();
        let ping_cancel = cancel.clone();
        let ping = tokio::spawn(async move {
            loop {
                tokio::time::sleep(PING_INTERVAL).await;
                if ping_cancel.load(Ordering::Relaxed) {
                    break;
                }
                let mut s = ping_sink.lock().await;
                if s.send(Message::Text(r#"{"method":"ping"}"#.to_string()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        // Boucle de lecture.
        loop {
            if cancel.load(Ordering::Relaxed) {
                break;
            }
            let msg = match tokio::time::timeout(WS_READ_TIMEOUT, stream.next()).await {
                Ok(Some(Ok(m))) => m,
                Ok(Some(Err(e))) => {
                    eprintln!("[hyperliquid] erreur WS: {e}");
                    break;
                }
                Ok(None) => {
                    eprintln!("[hyperliquid] flux terminé");
                    break;
                }
                Err(_) => {
                    eprintln!("[hyperliquid] timeout de lecture (60s)");
                    break;
                }
            };
            let text = match msg {
                Message::Text(t) => t,
                Message::Close(_) => {
                    eprintln!("[hyperliquid] connexion fermée par le serveur");
                    break;
                }
                _ => continue,
            };

            // Message de bienvenue en texte clair.
            if text.starts_with("Websocket connection") {
                continue;
            }
            let val: serde_json::Value = match serde_json::from_str(&text) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let channel = val["channel"].as_str().unwrap_or("");
            if channel == "pong" || channel == "subscriptionResponse" {
                continue;
            }
            if channel != "l2Book" {
                if unparsed_logged < 3 {
                    eprintln!("[hyperliquid] message non reconnu: {}", truncate(&text, 200));
                    unparsed_logged += 1;
                }
                continue;
            }

            let data = &val["data"];
            let levels = match data["levels"].as_array() {
                Some(l) if l.len() == 2 => l,
                _ => {
                    if unparsed_logged < 3 {
                        eprintln!(
                            "[hyperliquid] l2Book au format inattendu: {}",
                            truncate(&text, 300)
                        );
                        unparsed_logged += 1;
                    }
                    continue;
                }
            };
            let bids = hyperliquid_side(&levels[0], depth);
            let asks = hyperliquid_side(&levels[1], depth);
            if bids.is_empty() && asks.is_empty() {
                continue;
            }
            let tick = Tick {
                recv_ns: start.elapsed().as_nanos() as u64,
                recv_ms: epoch_ms(),
                server_ms: data["time"].as_u64().unwrap_or(0),
                exchange: "hyperliquid".to_string(),
                symbol: data["coin"].as_str().unwrap_or("").to_string(),
                seq: 0,
                prev_seq: 0,
                is_snapshot: true, // l2Book est un snapshot complet à chaque message
                bids,
                asks,
            };
            if tx.send(tick).is_err() {
                return;
            }
        }

        ping.abort();
        if cancel.load(Ordering::Relaxed) {
            break;
        }
        eprintln!("[hyperliquid] déconnecté, reconnexion dans 2s");
        tokio::time::sleep(RECONNECT_DELAY).await;
    }
    eprintln!("[hyperliquid] reader arrêté");
}

// ============================ Main ==========================================

#[tokio::main]
async fn main() {
    let args = parse_args();

    if let Err(e) = std::fs::create_dir_all(&args.out_dir) {
        eprintln!("impossible de créer le dossier {}: {e}", args.out_dir);
        std::process::exit(1);
    }
    let filename = format!(
        "{}/ticks_{}.jsonl",
        args.out_dir,
        chrono::Local::now().format("%Y%m%d_%H%M%S")
    );

    eprintln!("=== ws_recorder — recon BBOT ===");
    eprintln!("Symboles   : {}", args.symbols.join(", "));
    eprintln!("Durée      : {} s", args.duration.as_secs());
    eprintln!("Profondeur : {} niveaux", args.depth);
    eprintln!("Sortie     : {filename}");
    eprintln!();

    let cancel = Arc::new(AtomicBool::new(false));
    let start = Instant::now();
    let (tx, rx) = mpsc::unbounded_channel::<Tick>();

    // Writer.
    let writer = {
        let path = filename.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move { writer_task(rx, path, cancel).await })
    };

    // Résolution des marchés Lighter (REST) avant de lancer le reader.
    let lighter_markets = resolve_lighter_markets(&args.symbols).await;

    // Readers.
    let l_handle = {
        let tx = tx.clone();
        let cancel = cancel.clone();
        let depth = args.depth;
        tokio::spawn(async move { lighter_reader(lighter_markets, depth, tx, start, cancel).await })
    };
    let h_handle = {
        let tx = tx.clone();
        let cancel = cancel.clone();
        let depth = args.depth;
        let symbols = args.symbols.clone();
        tokio::spawn(async move { hyperliquid_reader(symbols, depth, tx, start, cancel).await })
    };
    drop(tx); // le canal se ferme quand les readers s'arrêtent

    tokio::select! {
        _ = tokio::time::sleep(args.duration) => {
            eprintln!("\n[main] durée atteinte, arrêt en cours…");
        }
        _ = tokio::signal::ctrl_c() => {
            eprintln!("\n[main] Ctrl+C reçu, arrêt en cours…");
        }
    }

    cancel.store(true, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_secs(1)).await;
    l_handle.abort();
    h_handle.abort();
    let _ = writer.await;
    eprintln!("[main] terminé.");
}
