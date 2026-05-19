//! feed_analyzer — analyse un fichier d'enregistrement du `ws_recorder`.
//!
//! Lit un fichier `.jsonl` produit par `ws_recorder` et calcule, par couple
//! (venue, symbole) :
//!   - cadence (ticks/s),
//!   - jitter (intervalles entre ticks : p50/p95/p99/max),
//!   - latence de transport (`recv_ms - server_ms` : min/p50/p95),
//!   - gaps de séquence (Lighter uniquement),
//!   - profondeur moyenne du carnet enregistré.
//!
//! Affiche un rapport sur la sortie standard, et termine par la comparaison
//! croisée du *tick rate* entre les deux venues — la mesure du « spread fantôme
//! potentiel » dont on cherche à se prémunir avec un *staleness gate*.
//!
//! Usage :
//!   cargo run --release --bin feed_analyzer
//!   cargo run --release --bin feed_analyzer -- data/ticks_20260519_223215.jsonl
//!   cargo run --release --bin feed_analyzer -- --symbol BTC data/ticks_xxx.jsonl

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use exchanges::tick::Tick;

// ============================ Agrégats par flux =============================

#[derive(Default)]
struct PerStream {
    count: u64,
    snapshots: u64,
    first_recv_ms: u64,
    last_recv_ms: u64,
    last_recv_ns: Option<u64>,
    intervals_ns: Vec<u64>,
    latencies_ms: Vec<i64>,
    last_seq: Option<u64>,
    seq_gaps: u64,
    bids_depth_sum: u64,
    asks_depth_sum: u64,
}

impl PerStream {
    fn ingest(&mut self, t: &Tick) {
        self.count += 1;
        if self.first_recv_ms == 0 {
            self.first_recv_ms = t.recv_ms;
        }
        self.last_recv_ms = t.recv_ms;

        if let Some(prev) = self.last_recv_ns {
            if t.recv_ns >= prev {
                self.intervals_ns.push(t.recv_ns - prev);
            }
        }
        self.last_recv_ns = Some(t.recv_ns);

        if t.server_ms > 0 {
            self.latencies_ms.push(t.recv_ms as i64 - t.server_ms as i64);
        }

        // Détection de gap de séquence (Lighter).
        if t.is_snapshot {
            self.snapshots += 1;
            self.last_seq = if t.seq > 0 { Some(t.seq) } else { None };
        } else if t.seq > 0 && t.prev_seq > 0 {
            if let Some(prev) = self.last_seq {
                if t.prev_seq != prev {
                    self.seq_gaps += 1;
                }
            }
            self.last_seq = Some(t.seq);
        }

        self.bids_depth_sum += t.bids.len() as u64;
        self.asks_depth_sum += t.asks.len() as u64;
    }
}

// ============================ Percentiles ===================================

fn percentile_u64(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn percentile_i64(sorted: &[i64], p: f64) -> i64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn fmt_ms_from_ns(ns: u64) -> String {
    format!("{:.1}", ns as f64 / 1_000_000.0)
}

// ============================ Arguments =====================================

struct Args {
    file: Option<PathBuf>,
    symbol_filter: Option<String>,
}

fn parse_args() -> Args {
    let argv: Vec<String> = std::env::args().collect();
    let mut file: Option<PathBuf> = None;
    let mut symbol_filter: Option<String> = None;
    let mut i = 1;
    while i < argv.len() {
        match argv[i].as_str() {
            "--symbol" => {
                if let Some(v) = argv.get(i + 1) {
                    symbol_filter = Some(v.to_uppercase());
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
        symbol_filter,
    }
}

/// Cherche le fichier `.jsonl` le plus récent dans `data/`.
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

// ============================ Main ==========================================

fn main() {
    let args = parse_args();

    let path = match args.file.or_else(|| find_latest_jsonl(Path::new("data"))) {
        Some(p) => p,
        None => {
            eprintln!(
                "aucun fichier .jsonl trouvé dans data/ — précise un chemin en argument"
            );
            std::process::exit(1);
        }
    };

    let file = match File::open(&path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("impossible d'ouvrir {}: {e}", path.display());
            std::process::exit(1);
        }
    };
    let reader = BufReader::new(file);

    // Flux indexés par (venue, symbole). BTreeMap → ordre stable dans le rapport.
    let mut streams: BTreeMap<(String, String), PerStream> = BTreeMap::new();
    let mut total_lines: u64 = 0;
    let mut parse_errors: u64 = 0;

    for line in reader.lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => continue,
        };
        if line.is_empty() {
            continue;
        }
        total_lines += 1;
        let tick: Tick = match serde_json::from_str(&line) {
            Ok(t) => t,
            Err(_) => {
                parse_errors += 1;
                continue;
            }
        };
        if let Some(filter) = &args.symbol_filter {
            if &tick.symbol != filter {
                continue;
            }
        }
        let key = (tick.exchange.clone(), tick.symbol.clone());
        streams.entry(key).or_default().ingest(&tick);
    }

    if streams.is_empty() {
        eprintln!("aucun tick ingéré (fichier vide ou filtre trop strict)");
        std::process::exit(1);
    }

    // ----- Rapport -----
    let total_ticks: u64 = streams.values().map(|s| s.count).sum();
    let first = streams.values().map(|s| s.first_recv_ms).min().unwrap_or(0);
    let last = streams.values().map(|s| s.last_recv_ms).max().unwrap_or(0);
    let duration_s = last.saturating_sub(first) as f64 / 1000.0;

    println!("=== feed_analyzer — rapport BBOT ===");
    println!("Fichier  : {}", path.display());
    println!(
        "Lignes   : {total_lines} (parse errors : {parse_errors})"
    );
    println!("Ticks    : {total_ticks} sur {} flux", streams.len());
    println!("Durée    : {duration_s:.1} s");
    println!();

    // Détail par flux.
    for ((ex, sym), s) in &streams {
        let mut intervals = s.intervals_ns.clone();
        intervals.sort_unstable();
        let mut latencies = s.latencies_ms.clone();
        latencies.sort_unstable();

        let dur = s.last_recv_ms.saturating_sub(s.first_recv_ms) as f64 / 1000.0;
        let rate = if dur > 0.0 { s.count as f64 / dur } else { 0.0 };
        let avg_bids = if s.count > 0 {
            s.bids_depth_sum as f64 / s.count as f64
        } else {
            0.0
        };
        let avg_asks = if s.count > 0 {
            s.asks_depth_sum as f64 / s.count as f64
        } else {
            0.0
        };

        println!("--- {} · {} ---", ex.to_uppercase(), sym);
        println!(
            "  Ticks            : {} ({} snapshots)",
            s.count, s.snapshots
        );
        println!("  Cadence          : {rate:.2} ticks/s sur {dur:.1}s");
        if !intervals.is_empty() {
            println!(
                "  Intervalles (ms) : p50={} · p95={} · p99={} · max={}",
                fmt_ms_from_ns(percentile_u64(&intervals, 0.50)),
                fmt_ms_from_ns(percentile_u64(&intervals, 0.95)),
                fmt_ms_from_ns(percentile_u64(&intervals, 0.99)),
                fmt_ms_from_ns(*intervals.last().unwrap()),
            );
        }
        if !latencies.is_empty() {
            println!(
                "  Latence transp.  : min={}ms · p50={}ms · p95={}ms (n={}) — inclut le clock skew",
                latencies.first().unwrap(),
                percentile_i64(&latencies, 0.50),
                percentile_i64(&latencies, 0.95),
                latencies.len(),
            );
        }
        if ex == "lighter" {
            println!("  Gaps de séquence : {}", s.seq_gaps);
        }
        println!(
            "  Profondeur moy.  : bids≈{avg_bids:.1} · asks≈{avg_asks:.1}"
        );
        println!();
    }

    // ----- Comparaison croisée -----
    let mut by_symbol: BTreeMap<String, BTreeMap<String, f64>> = BTreeMap::new();
    for ((ex, sym), s) in &streams {
        let dur = s.last_recv_ms.saturating_sub(s.first_recv_ms) as f64 / 1000.0;
        let rate = if dur > 0.0 { s.count as f64 / dur } else { 0.0 };
        by_symbol
            .entry(sym.clone())
            .or_default()
            .insert(ex.clone(), rate);
    }
    println!("--- Comparaison croisée du tick rate ---");
    for (sym, rates) in &by_symbol {
        let l = rates.get("lighter").copied().unwrap_or(0.0);
        let h = rates.get("hyperliquid").copied().unwrap_or(0.0);
        if l > 0.0 && h > 0.0 {
            let (fast_rate, slow_rate, leader) = if l >= h {
                (l, h, "lighter")
            } else {
                (h, l, "hyperliquid")
            };
            let ratio = if slow_rate > 0.0 {
                fast_rate / slow_rate
            } else {
                0.0
            };
            println!(
                "  {sym:6} · lighter={l:6.2}/s · hyperliquid={h:6.2}/s · ratio={ratio:.1}× ({leader} mène)"
            );
        } else {
            println!("  {sym:6} · lighter={l:6.2}/s · hyperliquid={h:6.2}/s (un côté manquant)");
        }
    }
}
