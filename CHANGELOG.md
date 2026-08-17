# Changelog

## 0.19.0

Theme: caller-driven node placement. A caller that places nodes
itself can now write a dirty node back to the pointer it already
occupies, instead of being forced to consume a fresh pointer on every
flush and leave the previous location unreclaimable.

### Added

- **`BtreeNodeDirty::id() -> P`** — the pointer a dirty node
  currently occupies. `BtreeNode::id()` was already public and `node`
  is already an exported module; the missing link was reaching the
  node from a `BtreeNodeDirty`, whose `clone_node_ref()` is
  `pub(crate)`.

  Intended for the `lookup_dirty` / `assign_meta_node` protocol:

  ```rust
  for n in bmap.lookup_dirty() {
      let cur = n.id();
      let off = if cur.is_valid_extern_assign() { cur } else { allocate()? };
      bmap.assign_meta_node(off, n.clone()).await?;
      write(off, n.as_slice())?;
  }
  ```

  Without it, every flush had to assign a fresh pointer to every dirty
  node, so the location the previous version occupied was
  unidentifiable and therefore unreclaimable. On a local block device
  that is a space leak; on a log-structured object store it prevents
  reclamation outright, because a node written to a fresh offset is
  never overwritten, its mapping never goes away, and the segment it
  landed in stays referenced forever. Writing the node back to its
  existing offset makes the write an overwrite of that logical
  address, and the storage holding the old version immediately loses
  its last reference.

  **Choosing the destination.** A node that has never been placed
  carries an *internal sequence number*, not `invalid_value()`, so
  discriminate with `NodeValue::is_valid_extern_assign()`. Using
  `is_invalid()` reports `false` for those sequence numbers and hands
  one back as though it were a storage location — rejected by
  `assign_meta_node` under the (default) `value-check` feature,
  silently accepted without it.

  **Assigning a node to its current pointer is a supported no-op.**
  The parent's child pointer is rewritten with the value it already
  held; no short-circuit is required for correctness.

  Returns `P` by value rather than `&P` deliberately — see Audit.

### Fixed

- `cargo clippy` / `cargo test --all-targets` now compile under
  default features. `mt-rc-sync-api` / `mt-arc-sync-api` are written
  for the `sync-api` feature but declared no `required-features`, so
  Cargo built them as async and failed; and `examples/mt.rs` is a
  shared helper module with no `main` that Cargo nevertheless
  auto-discovered as an example. The helper moved to
  `examples/mt/mod.rs` (not auto-discovered); `mod mt;` consumers
  resolve it unchanged.

### Audit

- `BtreeNodeDirty::id()` returns `P` by value, not `&P`, and the
  distinction is load-bearing rather than stylistic. `BtreeNode`
  stores the pointer in a `Cell<P>` and produces references through
  `unsafe { &*self.id.as_ptr() }`, while `assign_meta_node` writes
  that same cell via `set_id`. The natural flush loop — read `id()`,
  then assign — would hold a `SharedReadOnly` tag across a `Unique`
  retag and read through it afterwards. Miri reports that as
  Undefined Behavior; a normal build does not, because the read
  returns the value the cell already held. Returning by value makes
  the misuse unrepresentable instead of merely documented. Recorded
  as audit finding 8 in `docs/audit.md`.
- Miri coverage grew from four targets to six: `bigvalue` (added in
  0.18.1 but never registered) and `node_id` now run under both `rc`
  and `arc`, alongside `--lib`, `coverage_boost`, `bmap_tests` and
  `lookup_batch`. All pass with no UB reported.
- `run_tests.sh` and `run_audit.sh` enumerate their targets
  explicitly, so a new `tests/*.rs` file does not run until listed.
  `bigvalue` was unregistered in both and `lookup_batch` was missing
  from the functional matrix, so neither ran across the feature
  combinations they were written to protect. Both are now registered.

### Tests

- New `tests/node_id.rs` pins: an unplaced node reports an internal
  sequence number and never claims an external pointer; reusing the
  reported pointer keeps every key readable while an overwrite-only
  flush consumes no new pointer at all; and assigning a node back to
  its current pointer is idempotent. Verified against mutation —
  forcing always-fresh placement fails the growth assertion, and
  substituting `is_invalid()` as the discriminator fails two of the
  three tests.

### Compatibility

Additive. No existing signature or behaviour changes: every 0.18.1
public API keeps its shape, no feature flags were added or altered,
and callers that ignore `BtreeNodeDirty::id()` are unaffected.

## 0.18.1

Theme: V != P correctness. Three latent bugs along the convert /
lookup_last / delete paths -- silently broken in 0.16 and earlier,
exposed as panics by 0.17's added safety checks (commits f9ba42c
and c084ddb) -- prevented `BMap` from being used with a value type
`V` that is much larger than the pointer type `P` (e.g.
`K=u64`, `V=400-byte struct`, `P=u64` with a 56-byte inline root,
as Hyperfile's directory index requires). All three are now fixed
with minimal, non-breaking changes.

### Fixed

- `BtreeNode::from_raw_ptr` no longer rejects buffers whose
  capacity-by-V works out to zero. The 0.17 safeguard against
  zero-capacity nodes was an over-rejection: the existing
  `nchildren <= capacity` check already covers every unsafe input
  the original commit cited (which all require `nchildren > 0` to
  misbehave). `BMap::convert_and_insert`'s big-V split path
  intentionally builds a transient `LEAF | LARGE` root with
  capacity == 0 and nchildren == 0 before reshaping it into an
  internal root via `do_reinit::<P>`; that path is reachable
  again. Existing fuzz coverage still catches `nchildren > 0 +
  capacity == 0` inputs.
- `BtreeMap::do_lookup_last` no longer issues a dead
  `get_val::<P>` read on the leaf at the bottom of the rightmost
  spine. The walk now `while level > LEAF` over internal levels
  only, and reads the leaf's last key directly via `get_key`
  outside the loop. This also prevents the same size assert from
  firing on the leaf-root + V != P shape (small V, large root
  buffer).
- `BtreeMap::prepare_delete` records the leaf level's `oldseq`
  via `node.id()` (the node's own block id) instead of
  `*node.get_val(dindex)` (which read a V slot through P). The
  rollback semantics demanded by the field require a P-typed
  value that is actually a block id; only `id` provides that on a
  leaf. Internal-level and root-level entries continue to use
  `*node.get_val(dindex)` (well-defined: a child's P slot inside
  an internal node).

### Tests

- New `tests/bigvalue.rs` regression suite (`K=u64`, `V=432-byte
  Big`, `P=u64`, 56-byte root) covering `insert + lookup`,
  `insert × 8 + last_key`, and `insert × 40 + lookup × 40 +
  last_key + delete × 40`. The third case exercises the borrow /
  concat / shrink propagation that prepare_delete drives.
- `tests/coverage_boost.rs::btree_node_zero_capacity_is_legal`
  flips the previous `_rejects_zero_capacity` assertion to match
  the new (and 0.16-era) behaviour.

### Compatibility

No public API change. No feature flag change. No semantic change
for existing callers where `V` and `P` are size-equivalent (every
0.18.0 user). Callers with `V != P` now work where 0.17.0 / 0.18.0
panicked.

## 0.18.0

Theme: lookup latency. The internal `get_from_nodes` hot path is
faster; a new public batch lookup API folds N × (H − 1) serial
backend reads into (H − 1) parallel batched reads on loaders that
implement `BlockLoader::read_batch` concurrently;
`BMap::lookup_contig` performs sibling-leaf prefetch within its
parent. Existing single-key APIs are unchanged.

### Added

- **`BlockLoader::read_batch`** — new trait method with a default
  sequential fallback that loops `read`. Existing `BlockLoader`
  implementations compile and behave unchanged. Loaders that want
  real concurrency override the method (see
  `examples/lookup_batch_bench.rs::SlowLoader` for a
  `futures::future::join_all` pattern).
- **`BMap::lookup_batch(&[K]) -> Vec<Result<V>>`** and
  **`BMap::lookup_at_level_batch(&[K], usize) -> Vec<Result<V>>`** —
  new public batch lookup APIs. Per-key NotFound is reported inside
  the `Vec`; a whole-batch failure is fanned out to one `Err` per
  input so the result's shape is stable. Available on every feature
  combination including `mt` (see "Internal" below for the marker
  trait technique used to forward Send/Sync bounds without
  duplicating `BtreeMap`'s impl block).
- Sibling-leaf prefetch in **`BMap::lookup_contig`**. Before the
  walk loop begins, one `get_from_nodes_batch` issues for as many
  right-sibling leaves under the current parent as the requested
  run length might still need. Best-effort; semantics unchanged
  even on loader failure. Gated out under `mt`.
- New benches and examples covering this work:
  `examples/lookup_bench.rs` (classic vs. branchless vs. AVX2 SIMD
  in-node lookup), `examples/get_from_nodes_bench.rs` (the
  pre-existing `get_from_nodes` baseline), and
  `examples/lookup_batch_bench.rs` with a `SlowLoader` wrapper that
  injects per-read delays to make backend RTT visible (and a
  `lookup_contig` section).
- `tests/lookup_batch.rs` cross-checks `lookup_batch` results
  against per-key `lookup` and a `std::collections::BTreeMap`
  oracle, and pins the regression for the audit finding below.
- `fuzz/fuzz_targets/bmap_lookup_batch.rs` — fifth libFuzzer
  target. Seeded regression input lives at
  `fuzz/seeds/bmap_lookup_batch/regression_invalid_first_leaf_id`.
- `docs/prefetch.md` — full design write-up: motivation, scope,
  conditional-bound trait pattern, measurements, future work.

### Performance

- `BtreeMap::get_from_nodes` hot path is split into a `#[inline]`
  cache-probe and a `#[cold]` async miss helper. Cache-hit lookups
  no longer carry the miss path's async-state-machine setup cost.
- The miss path skips the previous full `alloc_zeroed` of the
  meta-node buffer and only zeroes the 8-byte `NodeHeader` prefix
  needed for `from_raw_ptr` to classify the node. Loader-side
  reads still overwrite the entire buffer before the node is
  published.
- `BMap::lookup_batch` collapses N × (H − 1) serial loader RTTs
  into (H − 1) parallel ones. Measured under `lookup_batch_bench`
  with a 100 µs synthetic per-read delay: 1.0× speedup at
  `batch_size=1`, 3.3× at 8, 12.7× at 32, 47.8× at 128.
- `BMap::lookup_contig` sibling prefetch collapses the within-parent
  serial sibling reads into one batched read. Measured under
  `lookup_batch_bench`: 1.99× at `run_length=64`, 5.62×–5.92× for
  larger run lengths up to the parent boundary.

### Fixed

- `BtreeMap::do_lookup_batch` no longer short-circuits to NotFound
  when a child id happens to equal `NodeValue::invalid_value()`.
  The single-key `do_lookup` does not check `is_invalid()` either:
  `BMap::convert_and_insert` legitimately initialises the very
  first leaf with `last_seq == 0`, which equals `invalid_value()`
  for `u64`. The earlier guard in the batch path caused
  `lookup_batch` to return NotFound for a key the corresponding
  single-key `lookup` would resolve. Found by the new
  `bmap_lookup_batch` fuzz target on its first 30-second run;
  recorded as audit finding 7. Regression test:
  `tests/lookup_batch.rs::lookup_batch_resolves_invalid_first_leaf_id`.

### Internal

- New `pub(crate)` `MaybeSendSync` / `MaybeSync` marker traits in
  `src/lib.rs`, blanket-implemented for any `T` under non-`mt`
  features and gated to `Send + Sync` / `Sync` under `mt`. The
  batch entry points use them as method-level `where` clauses,
  letting the new APIs forward `BlockLoader::read_batch`'s
  conditional Send/Sync requirements without modifying the main
  `BtreeMap` impl block. The `lookup_contig` prefetch path remains
  gated under `mt` because trait method bodies cannot add their
  own `where` clauses; lifting that would force the marker bounds
  onto every `BMap` method delegating into `VMap`.
- `docs/audit.md` updated for the 0.18 cycle. Miri covers all four
  test files (lib, coverage_boost, bmap_tests, lookup_batch) under
  both `rc` and `arc` feature sets. `fuzz-quick` and `fuzz-arc`
  runs across all five targets are clean.
- `README.md` and the crate-level rustdoc in `src/lib.rs` advertise
  the new batch API.
- Three clippy lints introduced by this cycle's new examples were
  cleaned up. Pre-existing lints in `src/bmap.rs` under the
  `sync-api` feature, and pre-existing example/Cargo.toml
  configuration issues for `examples/mt-{rc,arc}-sync-api`, are
  unchanged and out of scope.

### Compatibility

- No breaking changes. Every existing public API (`BMap::lookup`,
  `lookup_at_level`, `lookup_contig`, `insert`, `delete`,
  `seek_key`, `BlockLoader::read`, `BlockLoader::dup_from_new_path`,
  every `NodeCache` method) keeps the exact signature and
  observable behaviour from 0.17.0. Existing `BlockLoader`
  implementations do not need to add `read_batch` (default
  fallback is provided).

## 0.17.0

### Breaking

- `BMap::new` now returns `Result<Self, io::Error>` instead of `Self`.
  Callers must handle allocation / size validation errors; the previous
  panic sites (`root_node_size > meta_block_size/2`, allocation failure,
  direct root parse failure) have been replaced with `InvalidInput`.
- `BMap::read` now returns `Result<Self, io::Error>` instead of `Self`.
  Malformed root buffers propagate the parser error instead of panicking.
- `BtreeNode::move_left` / `move_right` are now `pub(crate)`; they were
  internal helpers with a caller-enforced safety contract and should
  never have been in the public API.
- `BtreeNode::from_raw_ptr` (and therefore `new`, `from_slice`,
  `from_slice_ref`, `copy_from_slice`) now rejects buffers too small to
  hold at least one `(K, V|P)` slot and returns `InvalidInput`.
  `DirectNode` is unchanged; tiny direct roots with zero capacity stay
  supported.

### Fixed

- Miri / Tree-Borrows: resolved all aliasing UB reported by
  `cargo +nightly miri test` under both the default (`rc`) and `arc`
  feature sets. Key changes: aligned-buffer storage (`AlignedBuffer`)
  replacing `Vec<u8>` for node memory; `header`/`keymap`/`valmap`
  stored as raw pointers with short-lived reborrows; `copy_from_slice`
  copies via `ptr::copy_nonoverlapping` before exposing the node view;
  `set_id` uses `Cell<P>`.
- `BMap::assign` / `assign_meta_node` / `assign_data_node`: return
  `Err(InvalidInput)` instead of panicking when the external id lacks
  `VALID_EXTERNAL_ASSIGN_MASK` or when `V != P` on the direct-assign
  path.
- `DirectMap::{lookup,lookup_contig,insert,insert_or_update,delete}`:
  bounds checks changed from `index > capacity - 1` to
  `index >= capacity`, eliminating an `usize::MAX` underflow when
  capacity is zero.
- `BtreeMap::do_check_convert`: guard against `nchild == 0` on both
  the height-3 and leaf paths to avoid `nchild - 1` underflow.
- `BtreeMap::evict`: filter predicate inverted so externally-assigned,
  clean nodes are actually eligible for eviction (was excluding them).
- `BtreeNode::Display`: leaf arm prints slots as `V` instead of `K`.
- `DirectMap::delete`: off-by-one bounds check (`index > capacity`)
  fixed to `index >= capacity`; reproduced by cargo-fuzz as an
  AddressSanitizer heap-buffer-overflow.

### Added

- `AlignedBuffer` is now a public type with `new` / `as_slice` /
  `as_mut_slice` / `len` / `is_empty` / `from_slice_copy` and
  `Clone + Send + Sync` impls.
- `BtreeNode::get_val` / `set_val` / `insert` / `delete` and both
  `from_slice_ref` implementations gained `# Caller contract` doc
  blocks and `debug_assert!` guards on sizes/indices where possible.
- `tests/bmap_tests.rs`, `tests/coverage_boost.rs`, `tests/sync_smoke.rs`:
  full integration suite covering direct/btree insert/delete/lookup/
  truncate/seek/lookup_contig, node parse errors, `AlignedBuffer`,
  loader / cache traits, random oracle cross-check against
  `std::collections::BTreeMap`. Line coverage ~93%.
- `fuzz/` harness with four libFuzzer targets
  (`btree_node_from_slice`, `direct_node_from_slice`, `bmap_read`,
  `bmap_ops`), regression seeds under `fuzz/seeds/`, and the
  `rc` / `arc` feature switch.
- `run_tests.sh`, `run_audit.sh` (`miri` / `miri-arc` / `miri-all` /
  `fuzz` / `fuzz-arc` / `fuzz-all` / `fuzz-quick` / `seed`) and
  `docs/audit.md` summarising findings and resolutions.
- Crate-level doc note describing the `P: From<u64> + Into<u64>`
  round-trip contract required under the `arc` feature.
