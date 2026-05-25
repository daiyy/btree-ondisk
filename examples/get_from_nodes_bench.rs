//! End-to-end baseline benchmark for the `BMap::lookup` hot path whose
//! cost is dominated by `BtreeMap::get_from_nodes(id).await`.
//!
//! The purpose is to establish a reproducible baseline **before** any
//! optimization of the `get_from_nodes` path, so later changes can be
//! measured against it.
//!
//! What we actually measure
//! ------------------------
//! `get_from_nodes` itself is `pub(crate)` and cannot be called from an
//! example, so we exercise it indirectly through `BMap::lookup`. A single
//! lookup through a btree of height `H` issues `H - 1` calls to
//! `get_from_nodes` (root is inlined). By sweeping tree heights and cache
//! configurations we separate the three practically distinct paths:
//!
//!   scenario "warm"  — cache unlimited, every internal + leaf node is
//!                      resident in the `self.nodes` HashMap. Lookup cost
//!                      ≈ (H - 1) × {HashMap::get + Rc/Arc::clone}.
//!   scenario "cold"  — cache_limit=1 (only root stays resident). Every
//!                      non-root level forces a miss → block_loader.read.
//!                      Lookup cost ≈ (H - 1) × {alloc_zeroed(meta_size)
//!                      + tiered_cache.load (NullNodeCache: no-op) +
//!                      block_loader.read + HashMap::insert}.
//!   scenario "mixed" — cache_limit set to ~half of the level-1 node
//!                      count. Represents a steady-state LRU-like load.
//!
//! The baseline is printed for several tree heights so that fixed overhead
//! (HashMap lookup, async state-machine, Rc clone) can be separated from
//! per-level cost (HashMap ops × depth).
//!
//! Run:  cargo run --release --example get_from_nodes_bench
//! Env:  BASELINE_ITERS     per-measurement iteration count (default 200_000)
//!       BASELINE_DATASET   "small", "mid", "large", or "all" (default "all")

use std::env;
use std::hint::black_box;
use std::io::Result;
use std::time::Instant;

use btree_ondisk::bmap::{BMap, BMapStat};
use btree_ondisk::{MemoryBlockLoader, NullNodeCache, DEFAULT_CACHE_UNLIMITED};

const ROOT_NODE_SIZE: usize = 56;
const META_NODE_SIZE: usize = 4096;
const DATA_BLOCK_SIZE: usize = 4096;
const VALID_EXTERNAL_ASSIGN_MASK: u64 = 0xFFFF_0000_0000_0000;

type TestBMap<'a> = BMap<'a, u64, u64, u64, MemoryBlockLoader<u64>, NullNodeCache>;

/// Fixture: a BMap with `num_keys` sequential keys written and flushed
/// into an in-memory loader, so every lookup that misses the HashMap will
/// hit the backend exactly once.
struct Fixture<'a> {
    bmap: TestBMap<'a>,
    loader: MemoryBlockLoader<u64>,
    keys: Vec<u64>,
    seq: u64,
}

impl<'a> Fixture<'a> {
    #[maybe_async::maybe_async]
    async fn build(num_keys: u64) -> Result<Self> {
        let loader = MemoryBlockLoader::new(DATA_BLOCK_SIZE);
        let bmap = BMap::<u64, u64, u64, _, _>::new(
            ROOT_NODE_SIZE,
            META_NODE_SIZE,
            loader.clone(),
            NullNodeCache,
        )?;

        let mut fx = Self {
            bmap,
            loader,
            keys: Vec::with_capacity(num_keys as usize),
            seq: VALID_EXTERNAL_ASSIGN_MASK + 1,
        };

        // insert sequential keys; sequential keeps leaves mostly full which
        // maximizes tree height per key count.
        for i in 0..num_keys {
            fx.bmap.insert(i, i + 1).await?;
            fx.keys.push(i);
        }
        fx.flush().await?;
        Ok(fx)
    }

    /// Persist every dirty meta node to the in-memory backend. Mirrors the
    /// existing meta_node_* examples so our fixture is well-understood.
    #[maybe_async::maybe_async]
    async fn flush(&mut self) -> Result<()> {
        if !self.bmap.dirty() {
            return Ok(());
        }
        let dirty_meta_vec = self.bmap.lookup_dirty();
        let mut seqs = std::collections::VecDeque::new();
        for n in &dirty_meta_vec {
            let blk_ptr = self.seq;
            self.bmap.assign_meta_node(blk_ptr, n.clone()).await?;
            seqs.push_back(blk_ptr);
            self.seq += 1;
        }
        for n in &dirty_meta_vec {
            let s = seqs.pop_front().expect("seq");
            self.loader.write(s, n.as_slice());
        }
        for n in dirty_meta_vec {
            n.clear_dirty();
        }
        self.bmap.clear_dirty();
        Ok(())
    }

    fn stat(&self) -> BMapStat {
        self.bmap.get_stat()
    }
}

#[derive(Clone, Copy)]
struct BenchOutput {
    total_ns: u128,
    iters: u64,
}

impl BenchOutput {
    fn ns_per_op(&self) -> f64 {
        self.total_ns as f64 / self.iters as f64
    }
    fn mops_per_sec(&self) -> f64 {
        self.iters as f64 / self.total_ns as f64 * 1000.0
    }
}

/// Run `iters` lookups against `bmap`, iterating `queries` in order.
/// If `evict_each` is true, call `set_cache_limit(1)` before every lookup
/// to force the previous lookup's newly-cached nodes out of `self.nodes`.
/// This includes the evict cost inside the per-op timing and is intended
/// to simulate a workload where every level-1/2 access misses the L1
/// HashMap and must go through the backend loader.
#[maybe_async::maybe_async]
async fn measure_lookup(
    bmap: &TestBMap<'_>,
    queries: &[u64],
    iters: u64,
    evict_each: bool,
) -> BenchOutput {
    // Warmup.
    let warm = (iters / 16).max(1024);
    let mut sink: u64 = 0;
    for i in 0..warm {
        if evict_each {
            bmap.set_cache_limit(1);
        }
        let k = queries[(i as usize) % queries.len()];
        if let Ok(v) = bmap.lookup(&k).await {
            sink = sink.wrapping_add(v);
        }
    }
    black_box(sink);

    let mut checksum: u64 = 0;
    let start = Instant::now();
    for i in 0..iters {
        if evict_each {
            bmap.set_cache_limit(1);
        }
        let k = queries[(i as usize) % queries.len()];
        if let Ok(v) = bmap.lookup(black_box(&k)).await {
            checksum = checksum.wrapping_add(v);
        }
    }
    let total_ns = start.elapsed().as_nanos();
    black_box(checksum);
    BenchOutput {
        total_ns,
        iters,
    }
}

fn shuffle_queries(keys: &[u64], n: usize, seed: u64) -> Vec<u64> {
    // LCG for reproducible "random" order without pulling rand into hot
    // measurement (rand thread_rng has its own overhead).
    let mut state = seed;
    let mut q = Vec::with_capacity(n);
    if keys.is_empty() {
        return q;
    }
    for _ in 0..n {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let idx = (state as usize) % keys.len();
        q.push(keys[idx]);
    }
    q
}

/// Three scenarios, each reporting (label, ns_per_op, Mops/s, stat_snapshot).
#[maybe_async::maybe_async]
async fn run_scenarios(num_keys: u64, iters: u64) -> Result<Vec<(String, BenchOutput, BMapStat)>> {
    let mut out: Vec<(String, BenchOutput, BMapStat)> = Vec::new();

    // Build fixture once; each scenario reconfigures cache_limit on it and
    // runs the measurement. Fixture construction is not timed.
    let fx = Fixture::build(num_keys).await?;
    let queries_seq: Vec<u64> = fx.keys.clone();
    let queries_rand = shuffle_queries(&fx.keys, queries_seq.len().min(1 << 20), 0xC0FFEE_u64);

    let stat0 = fx.stat();
    let tree_height = stat0.level + 1; // level is 0-based; root.level+1 == height

    // --------------- scenario: warm (L1 hit) ---------------
    fx.bmap.set_cache_limit(DEFAULT_CACHE_UNLIMITED);
    // Force one full sweep to repopulate self.nodes after any prior evict.
    for &k in &queries_seq {
        let _ = fx.bmap.lookup(&k).await;
    }
    let warm_seq = measure_lookup(&fx.bmap, &queries_seq, iters, false).await;
    out.push((format!("warm/seq         h={}", tree_height), warm_seq, fx.stat()));
    let warm_rand = measure_lookup(&fx.bmap, &queries_rand, iters, false).await;
    out.push((format!("warm/rand        h={}", tree_height), warm_rand, fx.stat()));

    // --------------- scenario: cold (L1 miss → backend every lookup) ---
    // Force evict before every lookup so that all non-root levels must go
    // through block_loader.read. The evict cost is included in the per-op
    // time; because cache_limit=1 keeps self.nodes tiny (only the single
    // just-loaded path) the evict pass is short.
    fx.bmap.set_cache_limit(1);
    let cold_seq = measure_lookup(&fx.bmap, &queries_seq, iters, true).await;
    out.push((format!("cold-evict/seq   h={}", tree_height), cold_seq, fx.stat()));
    fx.bmap.set_cache_limit(1);
    let cold_rand = measure_lookup(&fx.bmap, &queries_rand, iters, true).await;
    out.push((format!("cold-evict/rand  h={}", tree_height), cold_rand, fx.stat()));

    // --------------- scenario: mixed (LRU-like steady state) ----------
    // Pick a limit that can hold roughly half of the level-1 nodes so that
    // a random workload thrashes ~50% of the time. level-1 node count is
    // approximately ceil(num_keys / 255) for meta=4096, u64/u64 layout.
    let approx_l1 = (num_keys as usize).div_ceil(255);
    let mixed_limit = (approx_l1 / 2).max(1);
    fx.bmap.set_cache_limit(mixed_limit);
    let mixed_rand = measure_lookup(&fx.bmap, &queries_rand, iters, false).await;
    out.push((
        format!("mixed/rand       h={} lim={}", tree_height, mixed_limit),
        mixed_rand,
        fx.stat(),
    ));

    Ok(out)
}

fn parse_iters_env() -> u64 {
    env::var("BASELINE_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200_000)
}

fn parse_dataset_env() -> Vec<(String, u64)> {
    let s = env::var("BASELINE_DATASET").unwrap_or_else(|_| "all".to_string());
    let presets = [
        ("small", 250u64),      // tree height 2-3
        ("mid", 64_000u64),     // tree height 3-4
        ("large", 500_000u64),  // tree height 4
    ];
    match s.as_str() {
        "all" => presets.iter().map(|(k, v)| (k.to_string(), *v)).collect(),
        name => presets
            .iter()
            .filter(|(k, _)| *k == name)
            .map(|(k, v)| (k.to_string(), *v))
            .collect(),
    }
}

fn print_header() {
    println!(
        "| {:<32} | {:>12} | {:>14} | {:>14} | {:>6} | {:>8} | {:>8} |",
        "scenario", "ns/lookup", "Mops/s", "ns per level", "level", "n_total", "n_l1"
    );
    println!(
        "|{:-<34}|{:-<14}|{:-<16}|{:-<16}|{:-<8}|{:-<10}|{:-<10}|",
        "", "", "", "", "", "", ""
    );
}

fn print_row(label: &str, r: &BenchOutput, stat: &BMapStat) {
    let height = stat.level + 1;
    let ns_per_level = if height > 1 {
        r.ns_per_op() / (height - 1) as f64
    } else {
        r.ns_per_op()
    };
    println!(
        "| {:<32} | {:>12.2} | {:>14.2} | {:>14.2} | {:>6} | {:>8} | {:>8} |",
        label,
        r.ns_per_op(),
        r.mops_per_sec(),
        ns_per_level,
        stat.level,
        stat.nodes_total,
        stat.nodes_l1,
    );
}

#[maybe_async::maybe_async]
async fn run() -> Result<()> {
    let iters = parse_iters_env();
    let datasets = parse_dataset_env();

    println!("# get_from_nodes baseline (via BMap::lookup)");
    println!("# root={} meta={} data_block={}", ROOT_NODE_SIZE, META_NODE_SIZE, DATA_BLOCK_SIZE);
    println!("# iters per measurement: {}", iters);
    println!("# backend: MemoryBlockLoader, tiered cache: NullNodeCache");
    println!();

    for (label, num_keys) in datasets {
        println!("## dataset: {} ({} keys)", label, num_keys);
        print_header();
        let results = run_scenarios(num_keys, iters).await?;
        for (lbl, r, stat) in &results {
            print_row(lbl, r, stat);
        }
        println!();
    }
    Ok(())
}

fn main() {
    env_logger::init();

    #[cfg(not(feature = "sync-api"))]
    let res = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async { run().await });

    #[cfg(feature = "sync-api")]
    let res = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let join = tokio::task::spawn_blocking(move || run());
            join.await
        });

    println!("{res:?}");
}
