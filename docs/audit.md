# Unsafe Audit Findings

This document records results from two complementary tools run against
the existing `unsafe` code in this crate:

- **Miri** (`cargo +nightly miri test --lib` and selected integration
  tests). Miri detects Undefined Behaviour including Stacked/Tree Borrows
  violations.
- **cargo-fuzz** targets in `fuzz/fuzz_targets/` (coverage-guided
  libFuzzer + AddressSanitizer). These stress the public entry points
  that drive `unsafe fn from_raw_ptr`.

Run everything with `./run_audit.sh miri` and `./run_audit.sh fuzz [secs]`.

## Confirmed findings

### 1. Aliasing UB in `copy_from_slice` (Miri, Tree Borrows)

`BtreeNode::copy_from_slice` and `DirectNode::copy_from_slice` first
call `Self::new(size)`, which constructs a node whose internal
`header`/`keymap`/`valptr` hold live borrows into the allocated buffer,
then mutate the same buffer through `n.as_u8_mut().copy_from_slice(buf)`
and return `n`.

Miri (Tree Borrows) reports:

```
error: Undefined Behavior: reborrow through <...> at alloc.../.. is forbidden
   --> src/node.rs:908
    |
908 |             return Some(n);
    |                         ^ Undefined Behavior occurred here
```

Stacked Borrows also flags the reborrow performed inside
`AlignedBuffer::as_mut` when another live borrow (e.g. `header`) still
aliases the same allocation.

**Impact**: violations of Rust's aliasing model. With current rustc
codegen this does not miscompile in practice, but it remains UB by the
language spec and may become observable under future optimisations or
different codegen backends.

**Suggested remediation**: rebuild the node view *after* the raw copy,
e.g. allocate via `AlignedBuffer::new`, `copy_from_slice`, then
construct the node (`from_raw_ptr`) once — never mutating the backing
buffer through a separate handle while other borrows are live.

### 2. DoS panic in `BMap::read` (cargo-fuzz)

`BMap::read` unwraps the `from_slice_ref` result:

```rust
let root = BtreeNode::<K, V, P>::from_slice_ref(buf)
    .expect("failed to parse root node");
```

A malformed 8-byte header with `nchildren > capacity` panics the
process. Repro: `echo -n -e '\xff\xff\x0a\xe0\xe0\xe0\xe0\xe0'`.

libFuzzer finds this in seconds; two independent crash inputs are
reproduced in `fuzz/artifacts/bmap_read/`.

**Impact**: callers passing untrusted byte buffers can trigger a panic
that aborts the current thread (or the process if `panic = 'abort'`).

**Suggested remediation**: change `BMap::read`'s signature to
`Result<Self, io::Error>` (mirroring `BtreeNode::from_slice_ref`), or
document that callers must pre-validate. The underlying parsers already
return `Result`; the unwrap is gratuitous.

## Clean results

`cargo +nightly fuzz run btree_node_from_slice` and
`direct_node_from_slice` ran for ~30s each (~7M / ~4M executions)
without triggering AddressSanitizer or panics. Combined with Miri's
findings, the conclusion is that the parser is robust against malformed
bytes when its `Result` return is respected; misuse comes from callers
(`BMap::read`) and from the `copy_from_slice` aliasing pattern.
