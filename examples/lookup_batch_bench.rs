//! Batch-lookup benchmark with an artificial-delay loader.
//!
//! Purpose: prove that `BMap::lookup_batch` actually converts N serial
//! backend reads into one (or H-1) parallel fan-outs when the loader
//! implements `read_batch` concurrently.
//!
//! The existing `MemoryBlockLoader` is a synchronous `HashMap::get`
//! with no real I/O, so a straight batch-vs-serial comparison against
//! it would only measure async state-machine overhead. Here we wrap
//! it in a `SlowLoader` that sleeps for a configurable number of
//! microseconds on each `read`, so the RTT dominates. Its
//! `read_batch` override uses `futures::future::join_all` so N
//! concurrent reads take one RTT, not N RTTs.
//!
//! Run:  cargo run --release --example lookup_batch_bench
//! Env:  BATCH_KEYS         number of keys stored in the btree (default 5000)
//!       BATCH_DELAY_US     per-read sleep in microseconds (default 100)
//!       BATCH_SIZES        comma-separated batch sizes to bench (default 1,8,32,128)
//!       BATCH_ITERS        repeat each batch N times for stability (default 5)

#[cfg(any(feature = "sync-api", feature = "mt"))]
fn main() {
    eprintln!("lookup_batch_bench: unavailable under sync-api / mt feature sets");
}

#[cfg(all(not(feature = "sync-api"), not(feature = "mt")))]
use std::env;
#[cfg(all(not(feature = "sync-api"), not(feature = "mt")))]
use std::io::Result;
#[cfg(all(not(feature = "sync-api"), not(feature = "mt")))]
use std::sync::Arc;
#[cfg(all(not(feature = "sync-api"), not(feature = "mt")))]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(all(not(feature = "sync-api"), not(feature = "mt")))]
use std::time::{Duration, Instant};

#[cfg(all(not(feature = "sync-api"), not(feature = "mt")))]
use btree_ondisk::bmap::BMap;
#[cfg(all(not(feature = "sync-api"), not(feature = "mt")))]
use btree_ondisk::{BlockLoader, MemoryBlockLoader, NullNodeCache, VALID_EXTERNAL_ASSIGN_MASK};

#[cfg(all(not(feature = "sync-api"), not(feature = "mt")))]
const ROOT: usize = 56;
#[cfg(all(not(feature = "sync-api"), not(feature = "mt")))]
const META: usize = 256;

/// A pass-through loader that inserts a fixed per-call delay before
/// forwarding to an inner `MemoryBlockLoader`. The purpose is purely
/// to make backend IO latency visible to the benchmark — real
/// loaders would rely on their own backend for RTT.
///
/// The batched `read_batch` overload uses `futures::future::join_all`
/// so N concurrent reads complete in roughly one `delay` interval
/// rather than N of them.
#[cfg(all(not(feature = "sync-api"), not(feature = "mt")))]
#[derive(Clone)]
struct SlowLoader {
    inner: MemoryBlockLoader<u64>,
    delay_us: Arc<AtomicU64>,
    reads: Arc<AtomicU64>,
    batch_calls: Arc<AtomicU64>,
    batch_ids: Arc<AtomicU64>,
}

#[cfg(all(not(feature = "sync-api"), not(feature = "mt")))]
impl SlowLoader {
    fn new(inner: MemoryBlockLoader<u64>, delay_us: u64) -> Self {
        Self {
            inner,
            delay_us: Arc::new(AtomicU64::new(delay_us)),
            reads: Arc::new(AtomicU64::new(0)),
            batch_calls: Arc::new(AtomicU64::new(0)),
            batch_ids: Arc::new(AtomicU64::new(0)),
        }
    }

    fn reset_counters(&self) {
        self.reads.store(0, Ordering::Relaxed);
        self.batch_calls.store(0, Ordering::Relaxed);
        self.batch_ids.store(0, Ordering::Relaxed);
    }

    fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.reads.load(Ordering::Relaxed),
            self.batch_calls.load(Ordering::Relaxed),
            self.batch_ids.load(Ordering::Relaxed),
        )
    }
}

#[cfg(all(not(feature = "sync-api"), not(feature = "mt")))]
impl BlockLoader<u64> for SlowLoader {
    async fn read(&self, v: u64, buf: &mut [u8], user_data: u32) -> Result<Vec<(u64, Vec<u8>)>> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        let delay = self.delay_us.load(Ordering::Relaxed);
        if delay > 0 {
            tokio::time::sleep(Duration::from_micros(delay)).await;
        }
        self.inner.read(v, buf, user_data).await
    }

    async fn read_batch(
        &self,
        ids: &[u64],
        bufs: &mut [Vec<u8>],
        user_data: u32,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        assert_eq!(ids.len(), bufs.len());
        self.batch_calls.fetch_add(1, Ordering::Relaxed);
        self.batch_ids.fetch_add(ids.len() as u64, Ordering::Relaxed);

        let delay = self.delay_us.load(Ordering::Relaxed);
        let mut futures_vec = Vec::with_capacity(ids.len());
        for (id, buf) in ids.iter().zip(bufs.iter_mut()) {
            let id = *id;
            let inner = self.inner.clone();
            let buf_slice: &mut [u8] = buf.as_mut_slice();
            futures_vec.push(async move {
                if delay > 0 {
                    tokio::time::sleep(Duration::from_micros(delay)).await;
                }
                inner.read(id, buf_slice, user_data).await
            });
        }

        let results = futures::future::join_all(futures_vec).await;
        let mut more: Vec<(u64, Vec<u8>)> = Vec::new();
        for r in results {
            more.extend(r?);
        }
        Ok(more)
    }

    fn dup_from_new_path(self, _: &str) -> Self {
        self
    }
}

#[cfg(all(not(feature = "sync-api"), not(feature = "mt")))]
type Fixture<'a> = BMap<'a, u64, u64, u64, SlowLoader, NullNodeCache>;

#[cfg(all(not(feature = "sync-api"), not(feature = "mt")))]
async fn build_fixture<'a>(num_keys: u64, delay_us: u64) -> (Fixture<'a>, SlowLoader) {
    let inner = MemoryBlockLoader::<u64>::new(META);
    let loader = SlowLoader::new(inner.clone(), 0);
    let mut bmap =
        BMap::<u64, u64, u64, _, _>::new(ROOT, META, loader.clone(), NullNodeCache).unwrap();

    for k in 0..num_keys {
        let v = k.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 0x8000_0000_0000_0000;
        bmap.insert(k, v).await.expect("insert");
    }

    let mut seq = VALID_EXTERNAL_ASSIGN_MASK + 1;
    let dirty_meta = bmap.lookup_dirty();
    let mut assigned = Vec::new();
    for n in &dirty_meta {
        let id = seq;
        seq += 1;
        bmap.assign_meta_node(id, n.clone()).await.expect("assign");
        assigned.push(id);
    }
    for (n, id) in dirty_meta.iter().zip(assigned.iter()) {
        inner.write(*id, n.as_slice());
    }
    for n in dirty_meta {
        n.clear_dirty();
    }
    bmap.clear_dirty();
    bmap.set_cache_limit(1);

    loader.delay_us.store(delay_us, Ordering::Relaxed);
    (bmap, loader)
}

#[cfg(all(not(feature = "sync-api"), not(feature = "mt")))]
fn parse_sizes() -> Vec<usize> {
    env::var("BATCH_SIZES")
        .ok()
        .map(|s| {
            s.split(',')
                .filter_map(|x| x.trim().parse::<usize>().ok())
                .collect::<Vec<_>>()
        })
        .filter(|v: &Vec<usize>| !v.is_empty())
        .unwrap_or_else(|| vec![1, 8, 32, 128])
}

#[cfg(all(not(feature = "sync-api"), not(feature = "mt")))]
fn parse<T: std::str::FromStr>(var: &str, default: T) -> T {
    env::var(var).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

#[cfg(all(not(feature = "sync-api"), not(feature = "mt")))]
async fn run_once(bmap: &Fixture<'_>, keys: &[u64]) -> (Duration, Duration) {
    bmap.set_cache_limit(1);
    let t0 = Instant::now();
    for k in keys {
        let _ = bmap.lookup(k).await;
    }
    let serial = t0.elapsed();

    bmap.set_cache_limit(1);
    let t0 = Instant::now();
    let _ = bmap.lookup_batch(keys).await;
    let batch = t0.elapsed();

    (serial, batch)
}

#[cfg(all(not(feature = "sync-api"), not(feature = "mt")))]
fn median(mut v: Vec<Duration>) -> Duration {
    v.sort();
    v[v.len() / 2]
}

#[cfg(all(not(feature = "sync-api"), not(feature = "mt")))]
#[tokio::main]
async fn main() {
    let num_keys: u64 = parse("BATCH_KEYS", 5_000);
    let delay_us: u64 = parse("BATCH_DELAY_US", 100);
    let iters: usize = parse("BATCH_ITERS", 5);
    let sizes = parse_sizes();

    eprintln!("# lookup_batch_bench");
    eprintln!("# num_keys={num_keys} per_read_delay_us={delay_us} iters={iters}");

    let (bmap, loader) = build_fixture(num_keys, delay_us).await;
    let stat = bmap.get_stat();
    eprintln!(
        "# tree_height={} nodes_total={}",
        stat.level + 1,
        stat.nodes_total
    );
    eprintln!();

    println!(
        "| {:>10} | {:>14} | {:>14} | {:>8} | {:>10} | {:>10} | {:>10} |",
        "batch_size", "serial (ms)", "batch (ms)", "speedup", "reads", "batches", "batch_ids"
    );
    println!(
        "|{:-<12}|{:-<16}|{:-<16}|{:-<10}|{:-<12}|{:-<12}|{:-<12}|",
        "", "", "", "", "", "", ""
    );

    for &size in &sizes {
        let keys: Vec<u64> = (0..size as u64).map(|i| (i * 17) % num_keys).collect();

        let _ = run_once(&bmap, &keys).await;

        let mut serials = Vec::with_capacity(iters);
        let mut batches = Vec::with_capacity(iters);
        loader.reset_counters();
        for _ in 0..iters {
            let (s, b) = run_once(&bmap, &keys).await;
            serials.push(s);
            batches.push(b);
        }
        let (reads, batch_calls, batch_ids) = loader.snapshot();

        let s_med = median(serials);
        let b_med = median(batches);
        let speedup = s_med.as_secs_f64() / b_med.as_secs_f64();
        println!(
            "| {:>10} | {:>14.3} | {:>14.3} | {:>6.2}x | {:>10} | {:>10} | {:>10} |",
            size,
            s_med.as_secs_f64() * 1e3,
            b_med.as_secs_f64() * 1e3,
            speedup,
            reads,
            batch_calls,
            batch_ids
        );
    }

    // ----------------------------------------------------------------
    // lookup_contig sibling-prefetch section
    // ----------------------------------------------------------------
    //
    // When `lookup_contig(key, N)` spans more than one leaf, the path
    // walk needs to load the right-sibling leaves under the current
    // parent. Without prefetching that's `(L - 1)` serial loader calls
    // where L is the number of leaves involved. With sibling
    // prefetching (the patched code path), one `read_batch` is issued
    // up front for all needed sibling ids before the walk begins.
    //
    // We exercise this by running `lookup_contig(0, run_length)` for a
    // few growing run lengths against the same SlowLoader fixture.
    // Reads the loader records reveal whether the walk hit the cache
    // (one `batches=1` row) or had to issue serial reads.
    let contig_lengths: Vec<usize> = env::var("CONTIG_LENGTHS")
        .ok()
        .map(|s| {
            s.split(',')
                .filter_map(|x| x.trim().parse::<usize>().ok())
                .collect::<Vec<_>>()
        })
        .filter(|v: &Vec<usize>| !v.is_empty())
        .unwrap_or_else(|| vec![1, 64, 256, 1024, 2048]);

    println!();
    println!("# lookup_contig with sibling prefetch:");
    println!(
        "| {:>11} | {:>14} | {:>10} | {:>10} | {:>10} | {:>10} |",
        "run_length", "contig (ms)", "got", "reads", "batches", "batch_ids"
    );
    println!(
        "|{:-<13}|{:-<16}|{:-<12}|{:-<12}|{:-<12}|{:-<12}|",
        "", "", "", "", "", ""
    );
    for &n in &contig_lengths {
        // Warmup
        bmap.set_cache_limit(1);
        let _ = bmap.lookup_contig(&0u64, n).await;

        let mut samples = Vec::with_capacity(iters);
        loader.reset_counters();
        let mut got = 0usize;
        for _ in 0..iters {
            bmap.set_cache_limit(1); // force cold for every sample
            let t0 = Instant::now();
            let r = bmap.lookup_contig(&0u64, n).await;
            samples.push(t0.elapsed());
            if let Ok((_, c)) = r {
                got = c;
            }
        }
        let (reads, batch_calls, batch_ids) = loader.snapshot();
        let med = median(samples);
        println!(
            "| {:>11} | {:>14.3} | {:>10} | {:>10} | {:>10} | {:>10} |",
            n,
            med.as_secs_f64() * 1e3,
            got,
            reads,
            batch_calls,
            batch_ids
        );
    }
}
