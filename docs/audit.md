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
  - `cargo +nightly miri test --test lookup_batch` — 3/3
- **`arc` + tokio-runtime** via `./run_audit.sh miri-arc`
  - same four targets, same counts, no UB reported

The `lookup_batch` target was added as part of the 0.18 prefetch
work and exercises the new batch-cache-fill path
(`BtreeMap::get_from_nodes_batch`,
`BtreeMap::do_lookup_batch`, `BMap::lookup_batch`,
`BMap::lookup_at_level_batch`) plus the `lookup_contig`
sibling-prefetch path.

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

## Open findings

_None tracked in this audit round._

## Clean fuzz runs

- `btree_node_from_slice`: ~7M iterations, no ASan / panic findings.
- `direct_node_from_slice`: ~4M iterations, no findings.
