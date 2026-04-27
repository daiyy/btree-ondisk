# Changelog

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
