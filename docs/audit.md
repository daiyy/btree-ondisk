# Unsafe Audit Findings

Run with `./run_audit.sh miri` and `./run_audit.sh fuzz [secs]`.

## Tools

- **Miri** (`cargo +nightly miri test`) — detects UB including
  Stacked / Tree Borrows violations.
- **cargo-fuzz** targets in `fuzz/fuzz_targets/` (libFuzzer +
  AddressSanitizer) — stress the public entry points that drive
  `unsafe fn from_raw_ptr`.

## Current status

Running Miri on the full test suite passes under both feature
configurations:

- **Default (`rc` + tokio-runtime)** via `./run_audit.sh miri`
  - `cargo +nightly miri test --lib` — 1/1
  - `cargo +nightly miri test --test coverage_boost` — 23/23
  - `cargo +nightly miri test --test bmap_tests` — 11/11
  - `cargo +nightly miri test --test lookup_batch` — 4/4
  - `cargo +nightly miri test --test bigvalue` — 3/3
  - `cargo +nightly miri test --test node_id` — 3/3
- **`arc` + tokio-runtime** via `./run_audit.sh miri-arc`
  - same six targets, same counts, no UB reported

The `lookup_batch` target was added as part of the 0.18 prefetch
work and exercises the new batch-cache-fill path
(`BtreeMap::get_from_nodes_batch`,
`BtreeMap::do_lookup_batch`, `BMap::lookup_batch`,
`BMap::lookup_at_level_batch`) plus the `lookup_contig`
sibling-prefetch path.

`bigvalue` covers the `V != P` shapes fixed in 0.18.1 (a root buffer
too small to hold one `(K, V)` slot, leaf-level reads that must not go
through `get_val::<P>`).

`node_id` covers the caller-driven node placement protocol
(`BMap::lookup_dirty` / `BtreeNodeDirty::id` /
`BMap::assign_meta_node`), including reassigning a node to the pointer
it already occupies. Miri is load-bearing here: an earlier draft of
`BtreeNodeDirty::id` returned `&P` borrowed out of the node's
`Cell<P>`, and Miri reported a Stacked-Borrows violation when the
reference was held across the `Cell::set` inside `assign_meta_node`
(read through a `SharedReadOnly` tag invalidated by a later `Unique`
retag) — invisible in a normal build, where the read merely returned
the pre-existing value. Returning `P` by value removes the hazard
entirely; see finding 8.

cargo-fuzz `btree_node_from_slice` / `direct_node_from_slice` survive
multi-million inputs; `bmap_read` no longer crashes on malformed input
after its signature was changed to return `Result` (see finding 6).

## Resolved findings

1. **Aliasing UB in `Btree/DirectNode::copy_from_slice`** — fixed by
   copying bytes via `ptr::copy_nonoverlapping` *before* the node view
   is constructed, so no `&mut [u8]` aliases the internal borrows.
2. **Aliasing UB in node field access** (`header`, `keymap`, `valmap`)
   — fixed by storing them as raw pointers and reborrowing short-lived
   references on demand. `as_u8_ref` / `as_u8_mut` now derive their
   slice from `self.header as *mut u8` so the returned slice shares
   provenance with the node's internal raw pointers.
3. **Aliasing UB in `set_id`** — `ptr::addr_of!(self.id) as *mut P`
   produced a SharedReadOnly tag that was then written through. Fixed
   by storing `id` inside `Cell<P>` and using `Cell::set` / `as_ptr`.
4. **Aliasing UB in `insert` src pointer** — `&*self.keymap.add(index)
   as *const K` created a SharedReadOnly borrow that was invalidated
   by the sibling `*mut K` destination borrow. Fixed by casting the
   raw pointer directly (no reborrow).
5. **Alignment panic on miri / strict allocators** — `Vec<u8>` does
   not guarantee 8-byte alignment, which `BtreeNode::from_raw_ptr`
   requires. Fixed by switching the node storage containers
   (`BMap::new`, `DirectMap::data`, `BtreeMap::data`,
   `convert_and_insert`, `convert_to_direct`) from `Vec<u8>` to a new
   `pub` `AlignedBuffer` type that allocates via `alloc::alloc_zeroed`
   with an 8-byte layout.
6. **`BMap::read` DoS panic** — previously `.expect("failed to parse
   root node")` on any malformed input. Signature is now
   `pub fn read(...) -> Result<Self, io::Error>`; the parser's error
   is propagated instead. Verified against the recorded fuzz artifacts
   and a 14M-iteration fuzz run.
7. **`do_lookup_batch` short-circuited on invalid id** — the
   batch lookup walker treated `next_level_id == invalid_value()`
   as an unconditional NotFound. The single-key `do_lookup` does
   not do that — it dispatches to `get_from_nodes(&id)` regardless
   and lets the cache (which can legitimately hold an entry whose
   id equals `invalid_value()`, see `BMap::convert_and_insert`
   that uses `last_seq=0` as the first leaf id during direct →
   btree promotion) resolve it. The asymmetric short-circuit
   caused `lookup_batch` to return `NotFound` for keys
   `lookup` would resolve normally. Fixed by removing the
   `is_invalid()` skip from both id-collection and the per-key
   step; behaviour is now identical to `do_lookup`. Found by the
   new `bmap_lookup_batch` fuzz target on its first 30-second
   run; regression test added at
   `tests/lookup_batch.rs::lookup_batch_resolves_invalid_first_leaf_id`
   and a regression seed at
   `fuzz/seeds/bmap_lookup_batch/regression_invalid_first_leaf_id`.
8. **`BtreeNodeDirty::id` would have handed out a `Cell` interior
   borrow** — the accessor added to let callers place a node back at
   the pointer it already occupies was first drafted as
   `pub fn id(&self) -> &P`, mirroring `BtreeNode::id`. `BtreeNode`
   stores the pointer in a `Cell<P>` and produces the reference via
   `unsafe { &*self.id.as_ptr() }`, while `BMap::assign_meta_node`
   writes that same cell through `set_id`. The natural flush loop —
   read `id()`, then assign — therefore holds a `SharedReadOnly` tag
   across a `Unique` retag, and reading through it afterwards is UB.
   Miri flagged it; a normal build did not, because the read returned
   the value the cell already held. Fixed before release by returning
   `P` by value, which makes the misuse unrepresentable rather than
   merely documented. Covered by `tests/node_id.rs` under both Miri
   feature sets.

## Open findings

_None tracked in this audit round._

## Clean fuzz runs

- `btree_node_from_slice`: ~7M iterations, no ASan / panic findings.
- `direct_node_from_slice`: ~4M iterations, no findings.
- `bmap_lookup_batch`: 215k iterations after the finding-7 fix,
  no further crashes.
- 30-second `fuzz-quick` (rc) clean across all five targets.
- 30-second `fuzz-arc` clean across all five targets:
    - btree_node_from_slice 6.7M iter,
    - direct_node_from_slice 8.3M iter,
    - bmap_read 11.5M iter,
    - bmap_ops 37k iter,
    - bmap_lookup_batch 143k iter.
