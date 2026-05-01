# Batch Lookup Prefetch (scheme B v1)

Branch: `prefetch` (7 commits on top of `dev`). Ready for review and
merge; all additions are opt-in and preserve existing API behaviour.

## Motivation

Single-key `BMap::lookup` walks a tree of height `H` by issuing
`H - 1` sequential `get_from_nodes(id).await` calls. Each level's
`next_level_id` is only known *after* the previous level's node has
been loaded and searched, so within a single lookup the per-level I/O
cannot be parallelised — it is a hard data dependency.

But when the caller has multiple keys to look up, the picture changes.
Different lookups descending through the same tree level at the same
time have *different* `next_level_id` values, and those can be fetched
in parallel. Across a whole batch, the critical path shrinks from
`N * (H - 1)` serial RTTs to `H - 1` parallel RTTs.

## Scope

### What this prototype implements

- `BlockLoader::read_batch` trait method with a default sequential
  fallback. Every existing loader keeps working unchanged (the
  default calls `read` in a loop).
- `BtreeMap::get_from_nodes_batch` internal helper that resolves a
  slice of ids through one batched loader call (with cache hits
  filtered first).
- `BtreeMap::do_lookup_batch` that walks one tree level at a time,
  collects the distinct `next_level_id`s across all active keys,
  and issues one `get_from_nodes_batch` call per level.
- Public `BMap::lookup_batch(&[K])` and
  `BMap::lookup_at_level_batch(&[K], level)` that return
  `Vec<Result<V>>` — per-key NotFound is expressed inside the Vec,
  outer errors apply to the whole batch.
- `examples/lookup_batch_bench.rs` with a `SlowLoader` wrapper that
  adds a configurable per-read delay to make backend RTT visible,
  plus a real concurrent `read_batch` override via
  `futures::future::join_all`.
- `tests/lookup_batch.rs` oracle suite cross-checking batch results
  against per-key lookup on 2000-key btrees, on the Direct arm, and
  with duplicated / empty inputs.

### What this prototype does NOT address (deliberate non-goals)

- **Single-lookup I/O parallelisation.** As discussed above, the
  `next_level_id` data dependency rules this out without speculation,
  which v1 does not attempt.
- **Scan / range prefetch.** `seek_key` / `lookup_contig` walks don't
  use the batch API and still run serially. The speculation needed to
  prefetch the next sibling leaf is future work.
- **Loader-hinted prefetch.** `read` does not gain a PrefetchHint
  argument. Loaders that want to side-load neighbours continue to do
  so via the existing `more: Vec<(V, Vec<u8>)>` return value, which
  `get_from_nodes_batch` already honours.
- **Batched tiered cache.** The `NodeCache` trait exposes only a
  single-id `load`. The batch path calls it once per id in a loop.
  Tiered-cache hits are in-memory and cheap compared to backend RTT,
  which is what `read_batch` exists to parallelise.

### Unchanged existing APIs

Everything from `dev` is preserved as-is:

- `BlockLoader::read` signature untouched. Existing implementations
  (`NullBlockLoader`, `MemoryBlockLoader`, `u64`, any downstream
  loader) keep compiling without changes.
- `BMap::lookup`, `lookup_at_level`, `lookup_contig`, `seek_key`,
  `insert`, `delete` all unchanged in both shape and behaviour.
- `BtreeMap::get_from_nodes` / `do_lookup` untouched. The batch
  variants are completely separate code paths.

## Design choices worth calling out

### Why a trait method on `BlockLoader`, not a free function

`read_batch` has to be polymorphic over the concrete loader so each
backend can implement batching in whatever way makes sense for it —
`MemoryBlockLoader` can use a simple fallback, an S3-backed loader
might use `futures::join_all` or a native batch-GET API, a
local-disk loader might use `io_uring`. Giving the trait a
default-sequential implementation means the responsibility is
zero-cost for implementors who don't want to optimise yet.

### Why `Vec<Result<V>>`, not `Result<Vec<V>>`

Per-key NotFound is a normal outcome, not an error. Packing all
per-key results in a single flat `Vec` and reserving the outer
`Result::Err` for "the whole batch failed" (allocation failure,
backend IO error) keeps the common case straightforward.

### Why `mt` is left out

The `mt` feature adds `+ Send` bounds to the loader's futures to
ensure they can travel across executor threads. Those bounds cascade
into `V: Send + Sync`, `Self: Sync`, and several others on every
method that calls `read_batch`. Retrofitting the full `BtreeMap`
impl with those bounds would touch every downstream user of the
library. v1 therefore gates the batch path out under `mt` and
documents it — callers fall back to per-key `lookup`. This is
additive (nothing that used to work stops working) and the
restriction can be lifted later if the demand appears.

### Why `MemoryBlockLoader::read_batch` is not overridden

`MemoryBlockLoader` is pure `HashMap::get` + `copy_from_slice`. It
does not `await` anything. A `read_batch` override using
`futures::join_all` would execute its futures sequentially anyway
because each one is already Ready on first poll. The default
sequential fallback is therefore both correct and maximally fast
for this loader. The real override is in `SlowLoader`
(`examples/lookup_batch_bench.rs`) where there is actual sleep to
parallelise.

## Measurements

### Oracle test

`cargo test --test lookup_batch` on both the default (`rc`) and
`arc` feature sets: 3 cases, all pass. Includes a 2000-key btree
with cache evicted so every lookup traverses the loader, duplicate
and empty input handling, and the Direct arm.

### Benchmark

`cargo run --release --example lookup_batch_bench`, 5000 keys,
tree height 5, per-read delay 100 μs, 5 iterations/row, medians
reported:

| batch_size | serial (ms) | batch (ms) | speedup |
|-----------:|------------:|-----------:|--------:|
|          1 |        2.1  |       2.1  |   1.00× |
|          8 |       10.7  |       3.2  |   3.31× |
|         32 |       41.7  |       3.3  |  12.70× |
|        128 |      162.1  |       3.4  |  47.84× |

The batch latency is essentially constant at ~3.3 ms across batch
sizes 8…128, matching the predicted `H_batched * delay + overhead`
where `H_batched ≈ 3` (the levels below the in-memory root that
actually fan out to the loader). Serial scales linearly with batch
size as expected. Speedup at size 128 is 48×, close to the 128 ×
(H - 1) / H theoretical ceiling under this workload.

Loader counters corroborate the behaviour: the serial path issues
one `read` per key per descended level (~5 reads × 128 keys × 3
levels = ~695 single reads for the size-128 row), while the batch
path issues exactly one `read_batch` per level per iteration
(15 = 5 iters × 3 levels).

### Feature matrix

`run_tests.sh` passes on all four existing configurations (rc/arc ×
async/sync-api). Additional builds verified for
`arc + tokio-runtime + mt` (the batch API is gated out there).

## Commits

1. `ce3dd9a` — `BlockLoader::read_batch` trait method with
   sequential fallback.
2. `05b2366` — `BtreeMap::get_from_nodes_batch` internal helper.
3. `c3711bf` — `BtreeMap::do_lookup_batch` level walker.
4. `48a53fb` — public `BMap::lookup_batch` + `lookup_at_level_batch`.
5. `08a083a` — `tests/lookup_batch.rs` oracle suite.
6. `2afa7a0` — `examples/lookup_batch_bench.rs` + `SlowLoader`.
7. this commit.

## Future work (not in this PR)

- Wiring `read_batch` on a real backend loader (e.g. an S3 or
  local-file loader) and re-running the bench against actual RTT.
- Lifting the `mt`-feature restriction, once the Send/Sync
  retrofit work is justified by a concrete need.
- Range/scan prefetch via speculation on the next-sibling id.
  `seek_key`/`lookup_contig` could be rewritten on top of this
  plumbing once the basic shape is comfortable.
- Batched `NodeCache` / tiered-cache path, if the per-id `load`
  call ever shows up as material in a profile.
