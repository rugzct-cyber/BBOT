//! spread_replay — rejoue un enregistrement et chiffre les opportunités d'arbitrage.
//!
//! Lit un fichier `.jsonl` produit par `ws_recorder`, parcourt les ticks dans
//! l'ordre chronologique, maintient l'état du carnet de chaque venue, et à chaque
//! événement évalue l'opportunité d'arb dans **les deux directions** pour un
//! notional cible, sur la base du **VWAP exécutable** (donc inclut le slippage
//! au-delà du best bid/ask) :
//!   - buy Lighter / sell Hyperliquid
//!   - buy Hyperliquid / sell Lighter
//!
//! Pour chaque évaluation on enregistre aussi `age_other_ms` : l'âge du carnet
//! de l'autre venue au moment du tick. Le rapport balaie ensuite plusieurs
//! seuils de **staleness gate** pour montrer combien d'opportunités sont
//! vraies vs combien sont des **spreads fantômes** dus à l'asynchronie des
//! flux. C'est le chiffrage du problème.
//!
//! Usage :
//!   cargo run --release --bin spread_replay
//!   cargo run --release --bin spread_replay -- --notional 1000 --hl-fee-bps 4.5 data/ticks_xxx.jsonl

use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use exchanges::tick::{Level, Tick};

// ============================ Configuration =================================

const LIGHTER: &str = "lighter";
const HYPERLIQUID: &str = "hyperliquid";

/// Seuils balayés pour montrer la sensibilité au staleness gate (ms).
/// `u64::MAX` représente « pas de filtre ».
const GATE_THRESHOLDS_MS: &[u64] = &[u64::MAX, 1000, 500, 300, 200, 100, 50];

// ============================ Arguments =====================================

struct Args {
    file: Option<PathBuf>,
    symbol_filter: Option<Vec<String>>,
    notional_usd: f64,
    lighter_fee_bps: f64,
    hl_fee_bps: f64,
}

fn parse_args() -> Args {
    let argv: Vec<String> = std::env::args().collect();
    let mut file: Option<PathBuf> = None;
    let mut symbols: Option<Vec<String>> = None;
    let mut notional = 1000.0_f64;
    let mut lighter_fee = 0.0_f64;
    let mut hl_fee = 4.5_f64;

    let mut i = 1;
    while i < argv.len() {
        match argv[i].as_str() {
            "--symbols" | "--symbol" => {
                if let Some(v) = argv.get(i + 1) {
                    symbols = Some(
                        v.split(',')
                            .map(|s| s.trim().to_uppercase())
                            .filter(|s| !s.is_empty())
                            .collect(),
                    );
                    i += 2;
                    continue;
                }
            }
            "--notional" => {
                if let Some(v) = argv.get(i + 1).and_then(|s| s.parse::<f64>().ok()) {
                    notional = v;
                    i += 2;
                    continue;
                }
            }
            "--lighter-fee-bps" => {
                if let Some(v) = argv.get(i + 1).and_then(|s| s.parse::<f64>().ok()) {
                    lighter_fee = v;
                    i += 2;
                    continue;
                }
            }
            "--hl-fee-bps" | "--hyperliquid-fee-bps" => {
                if let Some(v) = argv.get(i + 1).and_then(|s| s.parse::<f64>().ok()) {
                    hl_fee = v;
                    i += 2;
                    continue;
                }
            }
            "--file" => {
                if let Some(v) = argv.get(i + 1) {
                    file = Some(PathBuf::from(v));
                    i += 2;
                    continue;
                }
            }
            other if !other.starts_with("--") => {
                file = Some(PathBuf::from(other));
            }
            _ => {}
        }
        i += 1;
    }
    Args {
        file,
        symbol_filter: symbols,
        notional_usd: notional,
        lighter_fee_bps: lighter_fee,
        hl_fee_bps: hl_fee,
    }
}

fn find_latest_jsonl(dir: &Path) -> Option<PathBuf> {
    let mut best: Option<(std::time::SystemTime, PathBuf)> = None;
    let entries = std::fs::read_dir(dir).ok()?;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("jsonl") {
            continue;
        }
        let mtime = entry.metadata().ok().and_then(|m| m.modified().ok());
        if let Some(mtime) = mtime {
            match &best {
                Some((t, _)) if *t >= mtime => {}
                _ => best = Some((mtime, path)),
            }
        }
    }
    best.map(|(_, p)| p)
}

// ============================ VWAP ==========================================

/// VWAP pour un *taker buy* : balaie les `asks` (par ordre croissant de prix)
/// jusqu'à couvrir `size`. Retourne `None` si la profondeur est insuffisante.
fn vwap_buy(asks: &[Level], size: f64) -> Option<f64> {
    if size <= 0.0 {
        return None;
    }
    let mut remaining = size;
    let mut total = 0.0;
    for lvl in asks {
        if remaining <= 0.0 {
            break;
        }
        let take = remaining.min(lvl.size);
        total += take * lvl.price;
        remaining -= take;
    }
    if remaining > 1e-12 {
        None
    } else {
        Some(total / size)
    }
}

/// VWAP pour un *taker sell* : balaie les `bids` (par ordre décroissant de prix).
fn vwap_sell(bids: &[Level], size: f64) -> Option<f64> {
    if size <= 0.0 {
        return None;
    }
    let mut remaining = size;
    let mut total = 0.0;
    for lvl in bids {
        if remaining <= 0.0 {
            break;
        }
        let take = remaining.min(lvl.size);
        total += take * lvl.price;
        remaining -= take;
    }
    if remaining > 1e-12 {
        None
    } else {
        Some(total / size)
    }
}

/// Mid-price approximatif à partir du best bid/ask, pour convertir notional → taille.
fn mid_from(book: &Tick) -> Option<f64> {
    let bid = book.bids.first().map(|l| l.price)?;
    let ask = book.asks.first().map(|l| l.price)?;
    if bid > 0.0 && ask > 0.0 {
        Some((bid + ask) / 2.0)
    } else {
        None
    }
}

// ============================ Évaluations ===================================

#[derive(Clone, Copy, PartialEq, Eq)]
enum Direction {
    /// buy Lighter / sell Hyperliquid
    LongL,
    /// buy Hyperliquid / sell Lighter
    LongH,
}

impl Direction {
    fn label(self) -> &'static str {
        match self {
            Direction::LongL => "long lighter / short hyperliquid",
            Direction::LongH => "long hyperliquid / short lighter",
        }
    }
}

#[derive(Clone)]
struct Eval {
    recv_ms: u64,
    age_other_ms: u64,
    direction: Direction,
    net_bps: f64,
    vwap_buy: f64,
    vwap_sell: f64,
}

// ============================ Percentiles ===================================

fn sorted_f64(v: &[f64]) -> Vec<f64> {
    let mut s = v.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    s
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

// ============================ Main ==========================================

fn main() {
    let args = parse_args();

    let path = match args.file.or_else(|| find_latest_jsonl(Path::new("data"))) {
        Some(p) => p,
        None => {
            eprintln!("aucun fichier .jsonl trouvé dans data/ — précise un chemin en argument");
            std::process::exit(1);
        }
    };

    // Charger tous les ticks ; on triera ensuite par recv_ms.
    let file = match File::open(&path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("impossible d'ouvrir {}: {e}", path.display());
            std::process::exit(1);
        }
    };
    let reader = BufReader::new(file);

    let mut ticks: Vec<Tick> = Vec::new();
    let mut symbols_seen: BTreeSet<String> = BTreeSet::new();
    for line in reader.lines().map_while(|r| r.ok()) {
        if line.is_empty() {
            continue;
        }
        let t: Tick = match serde_json::from_str(&line) {
            Ok(t) => t,
            Err(_) => continue,
        };
        if let Some(filter) = &args.symbol_filter {
            if !filter.contains(&t.symbol) {
                continue;
            }
        }
        // On ne s'intéresse qu'aux deux venues qu'on compare.
        if t.exchange != LIGHTER && t.exchange != HYPERLIQUID {
            continue;
        }
        symbols_seen.insert(t.symbol.clone());
        ticks.push(t);
    }
    if ticks.is_empty() {
        eprintln!("aucun tick exploitable dans {}", path.display());
        std::process::exit(1);
    }
    ticks.sort_by_key(|t| t.recv_ns);

    // Pour chaque symbole, on agrège les évaluations puis on les analyse.
    let mut evals_by_symbol: HashMap<String, Vec<Eval>> = HashMap::new();
    let mut total_ticks_by_symbol: HashMap<String, u64> = HashMap::new();
    let mut books: HashMap<(String, String), Tick> = HashMap::new();

    for tick in ticks.iter() {
        *total_ticks_by_symbol
            .entry(tick.symbol.clone())
            .or_insert(0) += 1;

        let key = (tick.exchange.clone(), tick.symbol.clone());
        books.insert(key.clone(), tick.clone());

        let other_ex = if tick.exchange == LIGHTER {
            HYPERLIQUID
        } else {
            LIGHTER
        };
        let other_key = (other_ex.to_string(), tick.symbol.clone());
        let other = match books.get(&other_key) {
            Some(b) => b,
            None => continue, // pas encore vu l'autre venue pour ce symbole
        };

        let age_other_ms = tick.recv_ms.saturating_sub(other.recv_ms);

        // Convertit le notional en taille via le mid de la venue qui vient de ticker.
        let mid = match mid_from(tick) {
            Some(m) => m,
            None => continue,
        };
        let size = args.notional_usd / mid;
        if size <= 0.0 || !size.is_finite() {
            continue;
        }

        // Direction LongL : achat sur Lighter (VWAP des asks Lighter),
        // vente sur Hyperliquid (VWAP des bids HL).
        let (lighter_book, hl_book) = if tick.exchange == LIGHTER {
            (tick, other)
        } else {
            (other, tick)
        };

        if let (Some(buy_l), Some(sell_h)) =
            (vwap_buy(&lighter_book.asks, size), vwap_sell(&hl_book.bids, size))
        {
            let gross_bps = (sell_h - buy_l) / buy_l * 10_000.0;
            let net_bps = gross_bps - args.lighter_fee_bps - args.hl_fee_bps;
            evals_by_symbol
                .entry(tick.symbol.clone())
                .or_default()
                .push(Eval {
                    recv_ms: tick.recv_ms,
                    age_other_ms,
                    direction: Direction::LongL,
                    net_bps,
                    vwap_buy: buy_l,
                    vwap_sell: sell_h,
                });
        }

        // Direction LongH : achat sur Hyperliquid, vente sur Lighter.
        if let (Some(buy_h), Some(sell_l)) =
            (vwap_buy(&hl_book.asks, size), vwap_sell(&lighter_book.bids, size))
        {
            let gross_bps = (sell_l - buy_h) / buy_h * 10_000.0;
            let net_bps = gross_bps - args.lighter_fee_bps - args.hl_fee_bps;
            evals_by_symbol
                .entry(tick.symbol.clone())
                .or_default()
                .push(Eval {
                    recv_ms: tick.recv_ms,
                    age_other_ms,
                    direction: Direction::LongH,
                    net_bps,
                    vwap_buy: buy_h,
                    vwap_sell: sell_l,
                });
        }
    }

    // ----- Rapport -----
    let round_trip = args.lighter_fee_bps + args.hl_fee_bps;
    println!("=== spread_replay — rapport BBOT ===");
    println!("Fichier   : {}", path.display());
    println!(
        "Symbole(s): {}",
        symbols_seen
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!("Notional  : ${:.2}", args.notional_usd);
    println!(
        "Frais     : lighter={:.2} bps · hyperliquid={:.2} bps (round-trip taker = {:.2} bps)",
        args.lighter_fee_bps, args.hl_fee_bps, round_trip
    );
    println!();

    for sym in &symbols_seen {
        println!("════════ {} ════════", sym);
        let total = total_ticks_by_symbol.get(sym).copied().unwrap_or(0);
        let evals = evals_by_symbol.get(sym).cloned().unwrap_or_default();
        println!(
            "Ticks ingérés         : {} · Évaluations (2 livres présents) : {}",
            total,
            evals.len()
        );
        println!();

        if evals.is_empty() {
            println!("(aucune évaluation — l'enregistrement n'a probablement pas couvert");
            println!(" un instant où les deux venues étaient présentes)");
            println!();
            continue;
        }

        for dir in [Direction::LongL, Direction::LongH] {
            let dir_evals: Vec<&Eval> = evals.iter().filter(|e| e.direction == dir).collect();
            let net_bps_vec: Vec<f64> = dir_evals.iter().map(|e| e.net_bps).collect();
            let sorted = sorted_f64(&net_bps_vec);
            let total_dir = dir_evals.len();
            let above_0 = sorted.iter().filter(|&&v| v > 0.0).count();
            let above_5 = sorted.iter().filter(|&&v| v > 5.0).count();
            let above_10 = sorted.iter().filter(|&&v| v > 10.0).count();
            let pct_0 = if total_dir > 0 {
                100.0 * above_0 as f64 / total_dir as f64
            } else {
                0.0
            };
            let max = dir_evals.iter().map(|e| e.net_bps).fold(f64::NEG_INFINITY, f64::max);
            let best = dir_evals
                .iter()
                .max_by(|a, b| a.net_bps.partial_cmp(&b.net_bps).unwrap_or(Ordering::Equal));

            println!("--- Direction « {} » ---", dir.label());
            println!("  Évaluations       : {total_dir}");
            println!(
                "  net_edge > 0 bps  : {above_0} ({pct_0:.1}%)"
            );
            println!("  net_edge > 5 bps  : {above_5}");
            println!("  net_edge > 10 bps : {above_10}");
            if !sorted.is_empty() {
                println!(
                    "  p50 net_edge      : {:>7.2} bps",
                    percentile(&sorted, 0.50)
                );
                println!(
                    "  p95 net_edge      : {:>7.2} bps",
                    percentile(&sorted, 0.95)
                );
                println!(
                    "  p99 net_edge      : {:>7.2} bps",
                    percentile(&sorted, 0.99)
                );
                println!("  max net_edge      : {:>7.2} bps", max);
            }
            if let Some(b) = best {
                println!(
                    "    └ meilleur trade : recv_ms={} · vwap_buy={:.4} · vwap_sell={:.4} · age_other={}ms",
                    b.recv_ms, b.vwap_buy, b.vwap_sell, b.age_other_ms
                );
            }
            println!();

            // Sensibilité au staleness gate.
            println!("  Sensibilité au staleness gate :");
            for &thr in GATE_THRESHOLDS_MS {
                let label = if thr == u64::MAX {
                    "∞     ".to_string()
                } else {
                    format!("{thr:>4}ms")
                };
                let kept: Vec<&&Eval> = dir_evals
                    .iter()
                    .filter(|e| e.age_other_ms <= thr)
                    .collect();
                let n0 = kept.iter().filter(|e| e.net_bps > 0.0).count();
                let n5 = kept.iter().filter(|e| e.net_bps > 5.0).count();
                println!(
                    "    {label} : {:>4} évals · {:>4} opp >0bps · {:>3} opp >5bps",
                    kept.len(),
                    n0,
                    n5
                );
            }
            println!();
        }
    }
}

