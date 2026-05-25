//! Lookup algorithm benchmark: classic binary search (current
//! implementation) vs. branchless binary search vs. SIMD (AVX2, u64).
//!
//! The node memory layout is **not** modified: all three algorithms operate
//! on the raw keymap slice produced by `BtreeNode::<u64, u64, u64>`.
//!
//! Usage:
//!   cargo run --release --example lookup_bench
//!
//! Optional env vars:
//!   LOOKUP_BENCH_ITERS   number of lookups per measurement (default 2_000_000)
//!   LOOKUP_BENCH_SIZES   comma-separated node sizes in bytes, overrides default sweep
//!
//! The sweep walks typical on-disk node sizes from the 56-byte root node up
//! to 4 MiB and reports ns/op, Mops/s, and cycles/op estimates for each
//! algorithm. Correctness is cross-checked against the classic algorithm on
//! both "hit" and "miss" queries for every node size before timing runs.

use std::env;
use std::hint::black_box;
use std::time::Instant;

use btree_ondisk::node::BtreeNode;

// --- shared helpers ---------------------------------------------------------

/// Mirror of `node.rs` constants so the example is self-contained.
const BTREE_NODE_LEVEL_MIN: usize = 1; // leaf level

/// Index-adjustment rule applied by `BtreeNode::lookup` after the search
/// loop finishes. `level > BTREE_NODE_LEVEL_MIN` == internal node.
#[inline(always)]
fn adjust_index(level: usize, s: bool, mut index: isize) -> usize {
    if level > BTREE_NODE_LEVEL_MIN {
        if s && index > 0 {
            index -= 1;
        }
    } else if !s {
        index += 1;
    }
    index as usize
}

// --- algorithm 1: classic binary search (copy of node.rs::lookup core) -----

#[inline]
fn lookup_classic(keys: &[u64], nchildren: usize, level: usize, key: u64) -> (bool, usize) {
    if nchildren == 0 {
        return (false, 0);
    }
    let mut low: isize = 0;
    let mut high: isize = (nchildren - 1) as isize;
    let mut s = false;
    let mut index: isize = 0;

    while low <= high {
        index = (low + high) / 2;
        // SAFETY: index in [0, nchildren) by invariant low<=high<nchildren.
        let nkey = unsafe { *keys.get_unchecked(index as usize) };
        if nkey == key {
            return (true, index as usize);
        } else if nkey < key {
            low = index + 1;
            s = false;
        } else {
            high = index - 1;
            s = true;
        }
    }
    (false, adjust_index(level, s, index))
}

// --- algorithm 2: branchless binary search ---------------------------------
//
// Standard lower_bound-style search: walks a half-open range [base, base+len)
// using cmov-friendly code. Produces `lb` = the first index with
// `keys[lb] >= key`. From `lb` we recover the `(found, index, s)` triple the
// classic algorithm would have produced and then apply the same leaf/internal
// adjustment so the return value is bit-for-bit identical.

#[inline]
fn lookup_branchless(keys: &[u64], nchildren: usize, level: usize, key: u64) -> (bool, usize) {
    if nchildren == 0 {
        return (false, 0);
    }

    // lower_bound in [0, nchildren)
    let mut base: usize = 0;
    let mut len: usize = nchildren;
    while len > 1 {
        let half = len / 2;
        let mid = base + half;
        // SAFETY: mid < nchildren by construction.
        let v = unsafe { *keys.get_unchecked(mid) };
        // Branchless: advance `base` when keys[mid] < key, otherwise keep it.
        // rustc turns this into a cmov on x86-64 in release mode.
        base = if v < key { mid } else { base };
        len -= half;
    }
    // `base` is now the first index whose key is >= key, except when even
    // keys[0] is still < key, in which case `base` is 0 and we need to
    // advance by one. Resolve both cases with one final compare.
    // SAFETY: base < nchildren.
    let base_key = unsafe { *keys.get_unchecked(base) };
    let lb = base + (base_key < key) as usize;

    if lb < nchildren {
        // SAFETY: lb < nchildren.
        let k_at = unsafe { *keys.get_unchecked(lb) };
        if k_at == key {
            return (true, lb);
        }
    }

    // Reconstruct classic (index, s):
    //   The classic loop's last comparison determines `s`.
    //   - If `key` is greater than every element, classic exits with s=false
    //     and index == nchildren-1; lb == nchildren.
    //   - Otherwise, the last comparison looked at keys[lb] and found it > key
    //     (s=true) and index == lb.
    let (index, s): (isize, bool) = if lb == nchildren {
        ((nchildren - 1) as isize, false)
    } else {
        (lb as isize, true)
    };
    (false, adjust_index(level, s, index))
}

// --- algorithm 3: SIMD (AVX2) binary search for u64 ------------------------
//
// Strategy: narrow to a small window with branchless binary search, then
// resolve the final <=4-element window with a single 256-bit compare and
// movemask. The main loop still halves the range, but each iteration uses
// an unaligned 256-bit load to look at 4 keys around the midpoint so that
// the hot leaf (which is tiny relative to L1) benefits from wide loads and
// the branch predictor is not consulted inside the loop.
//
// Falls back to `lookup_branchless` when AVX2 is unavailable at runtime.

#[cfg(target_arch = "x86_64")]
mod simd_avx2 {
    use super::*;
    use std::arch::x86_64::*;

    /// Test once at startup; avoids the per-call cost of `is_x86_feature_detected!`.
    pub fn avx2_available() -> bool {
        is_x86_feature_detected!("avx2")
    }

    /// AVX2 implementation. Caller must guarantee AVX2 is available.
    ///
    /// Invariant: the narrowing loop keeps `lower_bound(key) ∈ [base, base+len]`
    /// (closed on the right). When the loop exits with `len <= 4`, the final
    /// window is `[base, base+len)`. If every key in the window is `< key`,
    /// the lower bound is `base+len` — which may lie at `nchildren` or at a
    /// key we haven't inspected yet. We resolve that case with one extra
    /// scalar load.
    #[target_feature(enable = "avx2")]
    pub unsafe fn lookup_avx2(
        keys: &[u64],
        nchildren: usize,
        level: usize,
        key: u64,
    ) -> (bool, usize) {
        if nchildren == 0 {
            return (false, 0);
        }

        // Narrow to a window of at most 3 remaining candidates with
        // branchless halving. We stop at `len <= 3` rather than `<= 4` so
        // that the final 4-lane SIMD window [base, base+4) always covers
        // the closed interval `[base, base+len]` that the loop invariant
        // keeps the true lower bound inside — in particular the right
        // endpoint `base+len`, which a half-open window of width `len`
        // would miss.
        let mut base: usize = 0;
        let mut len: usize = nchildren;
        while len > 3 {
            let half = len / 2;
            let mid = base + half;
            let v = *keys.get_unchecked(mid);
            base = if v < key { mid } else { base };
            len -= half;
        }

        // Examine the 4-wide window in one shot. If `len < 4` we still need
        // to load 4 lanes; pad the tail with u64::MAX so lanes past `len`
        // never match. We can't just load past `nchildren` because that
        // might read out of bounds, so copy to a local buffer when the
        // window reaches the end.
        let need_pad = base + 4 > nchildren;
        let (v_reg, lanes_valid) = if need_pad {
            let mut buf = [u64::MAX; 4];
            let tail = nchildren - base;
            // Fill `buf[..tail]` with `keys[base..base+tail]`. Already
            // inside an `unsafe fn` block; copy_nonoverlapping is the
            // direct equivalent of the previous explicit loop and lets
            // clippy stop nagging about needless_range_loop.
            std::ptr::copy_nonoverlapping(
                keys.as_ptr().add(base),
                buf.as_mut_ptr(),
                tail,
            );
            (_mm256_loadu_si256(buf.as_ptr() as *const __m256i), tail)
        } else {
            let p = keys.as_ptr().add(base) as *const __m256i;
            (_mm256_loadu_si256(p), 4)
        };
        let k_reg = _mm256_set1_epi64x(key as i64);

        let eq = _mm256_cmpeq_epi64(v_reg, k_reg);
        let eq_mask = _mm256_movemask_epi8(eq) as u32;
        let sign = _mm256_set1_epi64x(i64::MIN);
        let v_s = _mm256_xor_si256(v_reg, sign);
        let k_s = _mm256_xor_si256(k_reg, sign);
        let gt = _mm256_cmpgt_epi64(v_s, k_s);
        let gt_mask = _mm256_movemask_epi8(gt) as u32;

        // Restrict attention to valid lanes. 4 lanes == 32 valid bits.
        let valid_bits: u32 = if lanes_valid >= 4 {
            u32::MAX
        } else {
            (1u32 << (lanes_valid * 8)) - 1
        };
        let eq_masked = eq_mask & valid_bits;
        let gt_masked = gt_mask & valid_bits;

        if eq_masked != 0 {
            let lane = (eq_masked.trailing_zeros() / 8) as usize;
            return (true, base + lane);
        }
        if gt_masked != 0 {
            // First lane with keys[lane] > key is the lower bound.
            let lane = (gt_masked.trailing_zeros() / 8) as usize;
            let lb = base + lane;
            return (false, adjust_index(level, true, lb as isize));
        }

        // Every inspected lane is `< key`. With `len <= 3` and window width 4
        // (or `nchildren - base` when we padded), the lanes we inspected
        // cover at least `[base, base+len]` closed. If all of them are
        // still `< key`, the only position the true lower bound can occupy
        // within the loop invariant is `nchildren` itself, which means the
        // key is larger than every stored key. Replicate classic's tail
        // state: s=false with index at the last inspected slot, then let
        // adjust_index bump it to `nchildren` for leaf nodes.
        debug_assert_eq!(
            base + lanes_valid,
            nchildren,
            "simd window did not reach end-of-node but failed to find lower bound"
        );
        let last = base + lanes_valid - 1;
        (false, adjust_index(level, false, last as isize))
    }
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn lookup_simd(keys: &[u64], nchildren: usize, level: usize, key: u64) -> (bool, usize) {
    // SAFETY: caller must only reach this path when avx2 was detected.
    unsafe { simd_avx2::lookup_avx2(keys, nchildren, level, key) }
}

#[cfg(not(target_arch = "x86_64"))]
#[inline]
fn lookup_simd(keys: &[u64], nchildren: usize, level: usize, key: u64) -> (bool, usize) {
    lookup_branchless(keys, nchildren, level, key)
}

// --- correctness harness ---------------------------------------------------

fn build_keys(nchildren: usize) -> Vec<u64> {
    // Use sparse keys so "miss" queries land strictly between stored keys.
    (0..nchildren as u64).map(|i| i.wrapping_mul(2) + 1).collect()
}

fn build_queries(keys: &[u64], count: usize, hit_ratio: f64) -> Vec<u64> {
    // Deterministic interleaving of hits and misses, no rand dep needed.
    let n = keys.len();
    if n == 0 {
        return vec![0; count];
    }
    let mut q = Vec::with_capacity(count);
    // Simple LCG for reproducible pseudo-random indices.
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let hit_threshold = (hit_ratio * u32::MAX as f64) as u32;
    for _ in 0..count {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let r = (state >> 32) as u32;
        let idx = (state as usize) % n;
        if r < hit_threshold {
            q.push(keys[idx]);
        } else {
            // land strictly between two stored keys, or below the smallest,
            // or above the largest (even numbers by construction).
            q.push(keys[idx].wrapping_sub(1));
        }
    }
    q
}

fn check_correctness(keys: &[u64], has_avx2: bool) {
    let n = keys.len();
    // Probe across the full keyspace: every stored key, every neighbour.
    let mut probes: Vec<u64> = Vec::with_capacity(n * 3 + 2);
    for &k in keys {
        probes.push(k.wrapping_sub(1));
        probes.push(k);
        probes.push(k.wrapping_add(1));
    }
    if !keys.is_empty() {
        probes.push(keys[0].wrapping_sub(2));
        probes.push(keys[n - 1].wrapping_add(2));
    }

    for level in [BTREE_NODE_LEVEL_MIN, BTREE_NODE_LEVEL_MIN + 1] {
        for &k in &probes {
            let exp = lookup_classic(keys, n, level, k);
            let got_b = lookup_branchless(keys, n, level, k);
            assert_eq!(
                exp, got_b,
                "branchless mismatch: n={} level={} key={} classic={:?} branchless={:?}",
                n, level, k, exp, got_b
            );
            if has_avx2 {
                let got_s = lookup_simd(keys, n, level, k);
                assert_eq!(
                    exp, got_s,
                    "simd mismatch: n={} level={} key={} classic={:?} simd={:?}",
                    n, level, k, exp, got_s
                );
            }
        }
    }
}

/// Random stress test: generate a sorted key array with irregular gaps and
/// probe with uniformly-random u64 values. Run for a small number of node
/// sizes where looping through thousands of cases is cheap.
fn stress_correctness(has_avx2: bool) {
    let sizes = [1usize, 2, 3, 4, 5, 7, 8, 15, 16, 17, 31, 63, 127, 255, 256];
    let mut state: u64 = 0xDEAD_BEEF_CAFE_0001;
    let mut next = || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        state
    };

    for &n in &sizes {
        // Build a strictly increasing random key vector.
        let mut keys: Vec<u64> = Vec::with_capacity(n);
        let mut acc: u64 = next() & 0xFFFF;
        for _ in 0..n {
            acc = acc.wrapping_add((next() & 0xFFFF) + 1);
            keys.push(acc);
        }

        for _ in 0..2000 {
            // Mix of "around a stored key" and fully random u64 probes.
            let r = next();
            let key = if r & 3 == 0 && n > 0 {
                keys[(r as usize) % n]
            } else if r & 3 == 1 && n > 0 {
                keys[(r as usize) % n].wrapping_add((r >> 16) & 1)
            } else if r & 3 == 2 && n > 0 {
                keys[(r as usize) % n].wrapping_sub((r >> 16) & 1)
            } else {
                r
            };

            for level in [BTREE_NODE_LEVEL_MIN, BTREE_NODE_LEVEL_MIN + 1] {
                let exp = lookup_classic(&keys, n, level, key);
                let got_b = lookup_branchless(&keys, n, level, key);
                assert_eq!(
                    exp, got_b,
                    "stress branchless mismatch: n={} level={} key={} classic={:?} branchless={:?}\nkeys={:?}",
                    n, level, key, exp, got_b, keys
                );
                if has_avx2 {
                    let got_s = lookup_simd(&keys, n, level, key);
                    assert_eq!(
                        exp, got_s,
                        "stress simd mismatch: n={} level={} key={} classic={:?} simd={:?}\nkeys={:?}",
                        n, level, key, exp, got_s, keys
                    );
                }
            }
        }
    }
}

// --- benchmark harness -----------------------------------------------------

#[derive(Clone, Copy)]
struct BenchResult {
    total_ns: u128,
    iters: u64,
    checksum: u64,
}

impl BenchResult {
    fn ns_per_op(&self) -> f64 {
        self.total_ns as f64 / self.iters as f64
    }
    fn mops_per_sec(&self) -> f64 {
        (self.iters as f64) / (self.total_ns as f64) * 1_000.0
    }
}

fn bench<F>(name: &str, iters: u64, queries: &[u64], mut f: F) -> BenchResult
where
    F: FnMut(u64) -> (bool, usize),
{
    let _ = name;
    // Warmup.
    let warm = (iters / 16).max(1024);
    let mut sink: u64 = 0;
    for i in 0..warm {
        let q = queries[(i as usize) % queries.len()];
        let (found, idx) = f(q);
        sink = sink.wrapping_add(idx as u64).wrapping_add(found as u64);
    }
    black_box(sink);

    let mut checksum: u64 = 0;
    let start = Instant::now();
    for i in 0..iters {
        let q = queries[(i as usize) % queries.len()];
        let (found, idx) = f(black_box(q));
        checksum = checksum.wrapping_add(idx as u64).wrapping_add(found as u64);
    }
    let total_ns = start.elapsed().as_nanos();
    black_box(checksum);
    BenchResult {
        total_ns,
        iters,
        checksum,
    }
}

fn node_capacity(node_size: usize) -> usize {
    // Mirrors BtreeNode::from_raw_ptr for K=V=u64, leaf layout.
    // hdr=8, key=8, val=8.
    let hdr = 8usize;
    if node_size <= hdr {
        return 0;
    }
    (node_size - hdr) / (8 + 8)
}

fn default_node_sizes() -> Vec<usize> {
    vec![
        56,              // typical root node
        4096,
        8192,
        16384,
        32768,
        65536,
        256 * 1024,
        1024 * 1024,
    ]
}

fn parse_sizes_env() -> Option<Vec<usize>> {
    let s = env::var("LOOKUP_BENCH_SIZES").ok()?;
    let v: Vec<usize> = s
        .split(',')
        .filter_map(|x| x.trim().parse().ok())
        .collect();
    if v.is_empty() { None } else { Some(v) }
}

fn parse_iters_env() -> u64 {
    env::var("LOOKUP_BENCH_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_000_000)
}

fn human_bytes(n: usize) -> String {
    const KB: usize = 1024;
    const MB: usize = 1024 * 1024;
    if n >= MB {
        format!("{:>6.2} MiB", n as f64 / MB as f64)
    } else if n >= KB {
        format!("{:>6.2} KiB", n as f64 / KB as f64)
    } else {
        format!("{:>6} B  ", n)
    }
}

fn print_header() {
    println!(
        "| {:>10} | {:>8} | {:>8} | {:>10} | {:>14} | {:>10} | {:>14} | {:>10} | {:>14} |",
        "node size", "n", "workload", "classic ns", "classic Mops/s", "branch ns",
        "branch Mops/s", "simd ns", "simd Mops/s"
    );
    println!(
        "|{:-<12}|{:-<10}|{:-<10}|{:-<12}|{:-<16}|{:-<12}|{:-<16}|{:-<12}|{:-<16}|",
        "", "", "", "", "", "", "", "", ""
    );
}

fn print_row(
    node_size: usize,
    n: usize,
    overcommit: bool,
    workload: &str,
    classic: BenchResult,
    branch: BenchResult,
    simd: Option<BenchResult>,
) {
    let simd_ns = simd.map(|r| format!("{:>10.2}", r.ns_per_op())).unwrap_or_else(|| "    n/a   ".to_string());
    let simd_mops = simd.map(|r| format!("{:>14.2}", r.mops_per_sec())).unwrap_or_else(|| "     n/a      ".to_string());
    let n_str = if overcommit {
        format!("{}*", n)
    } else {
        format!("{}", n)
    };
    println!(
        "| {:>10} | {:>8} | {:>8} | {:>10.2} | {:>14.2} | {:>10.2} | {:>14.2} | {} | {} |",
        human_bytes(node_size),
        n_str,
        workload,
        classic.ns_per_op(),
        classic.mops_per_sec(),
        branch.ns_per_op(),
        branch.mops_per_sec(),
        simd_ns,
        simd_mops,
    );
}

fn run_one(node_size: usize, iters: u64, has_avx2: bool) {
    let raw_cap = node_capacity(node_size);
    if raw_cap == 0 {
        println!("# node_size={} skipped (capacity 0)", node_size);
        return;
    }
    // BtreeNode stores `nchildren` in a u16, so a single node can never
    // actually hold more than 65_535 keys regardless of how much byte space
    // the layout reserves. Clamp the populated count so that `node.lookup`
    // and our local `lookup_classic` agree; the excess bytes are simply
    // unused (and correctly reflect the on-disk reality).
    let cap = raw_cap.min(u16::MAX as usize);
    let overcommit = raw_cap > cap;

    // Build a real BtreeNode and fill it so the keymap layout is exactly
    // what production code sees. Then share the keymap as a read-only slice
    // with the three algorithms.
    let node = BtreeNode::<u64, u64, u64>::new(node_size)
        .expect("BtreeNode::new");
    node.set_leaf();
    let keys_vec = build_keys(cap);
    for (i, &k) in keys_vec.iter().enumerate() {
        node.insert(i, &k, &k);
    }

    // Sanity: classic via the node's own lookup method must agree with our
    // local `lookup_classic` copy.
    let probe = keys_vec[cap / 2];
    let via_node = node.lookup(&probe);
    let via_local = lookup_classic(&keys_vec, cap, BTREE_NODE_LEVEL_MIN, probe);
    assert_eq!(
        via_node, via_local,
        "node.lookup disagrees with lookup_classic; invariants drifted"
    );

    // Cross-validate all three algorithms.
    check_correctness(&keys_vec, has_avx2);

    // Workloads: 100% hits and 0% hits.
    let hit_queries = build_queries(&keys_vec, iters.min(1 << 20) as usize, 1.0);
    let miss_queries = build_queries(&keys_vec, iters.min(1 << 20) as usize, 0.0);
    let mix_queries = build_queries(&keys_vec, iters.min(1 << 20) as usize, 0.5);

    for (label, qs) in [("hit", &hit_queries), ("miss", &miss_queries), ("mix50", &mix_queries)] {
        let classic = bench("classic", iters, qs, |k| {
            lookup_classic(&keys_vec, cap, BTREE_NODE_LEVEL_MIN, k)
        });
        let branch = bench("branch", iters, qs, |k| {
            lookup_branchless(&keys_vec, cap, BTREE_NODE_LEVEL_MIN, k)
        });
        let simd = if has_avx2 {
            Some(bench("simd", iters, qs, |k| {
                lookup_simd(&keys_vec, cap, BTREE_NODE_LEVEL_MIN, k)
            }))
        } else {
            None
        };

        // Extra sanity: checksums of the three runs must match.
        assert_eq!(
            classic.checksum, branch.checksum,
            "classic vs branchless checksum mismatch at node_size={} workload={}",
            node_size, label
        );
        if let Some(s) = simd {
            assert_eq!(
                classic.checksum, s.checksum,
                "classic vs simd checksum mismatch at node_size={} workload={}",
                node_size, label
            );
        }

        print_row(node_size, cap, overcommit, label, classic, branch, simd);
    }
}

fn main() {
    let iters = parse_iters_env();
    let sizes = parse_sizes_env().unwrap_or_else(default_node_sizes);

    let has_avx2 = {
        #[cfg(target_arch = "x86_64")]
        {
            simd_avx2::avx2_available()
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            false
        }
    };

    println!("# btree-ondisk lookup algorithm benchmark");
    println!("# iters per measurement: {}", iters);
    #[cfg(target_arch = "x86_64")]
    {
        println!("# target_arch: x86_64, avx2 detected: {}", has_avx2);
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        println!("# target_arch: non-x86_64, SIMD path falls back to branchless");
    }

    // Run the stress test before any timing runs so mismatches surface early.
    println!("# running randomized correctness stress test ...");
    stress_correctness(has_avx2);
    println!("# stress test passed");
    println!("# NOTE: BtreeNode stores nchildren in u16; for node sizes whose");
    println!("#       raw capacity exceeds 65_535 we populate only 65_535 keys.");
    println!("#       Such rows are marked with a '*' after the `n` column.");
    println!();

    print_header();
    for size in sizes {
        run_one(size, iters, has_avx2);
    }
}
