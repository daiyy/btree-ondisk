# btree-ondisk

A Rust implementation of BTree structure on persistent storage in userspace.

Codebase is inspired by and partially derived from [NILFS2](https://docs.kernel.org/filesystems/nilfs2.html).

NILFS2 is a log-structured file system implementation for the Linux kernel.

**NOTICE**: This library itself does not include persistent part, user should implement persistent process on top of this library.

## Under Developement

:warning: This library is currently under developement and is **NOT** recommended for production.

## Examples

See [examples](examples/) for how to use.

## Batch lookup

`BMap::lookup_batch(&[K])` resolves a slice of keys in a single tree
walk that issues one batched backend read per tree level instead of
one per key per level. On loaders that override
[`BlockLoader::read_batch`] to actually fan out reads (e.g. via
`futures::future::join_all`), this collapses `N × (H − 1)` serial
RTTs into `H − 1` parallel ones. `BMap::lookup_contig` performs the
same kind of sibling-leaf prefetch within its parent.

```rust
let results: Vec<std::io::Result<V>> = bmap.lookup_batch(&keys).await;
```

Existing single-key APIs (`lookup`, `lookup_at_level`,
`lookup_contig`) are unchanged. See [docs/prefetch.md](docs/prefetch.md)
for the design and measured speedups (~48× at batch_size=128 against
a 100 µs-per-read backend; ~5.9× for `lookup_contig` over the
full sibling-prefetch window).

## Testing & Audit

Functional tests:

```
./run_tests.sh        # feature matrix: rc / arc x async / sync-api
```

Core-file line coverage is around 93% (measure with `cargo llvm-cov`).

Unsafe / correctness audit tooling:

```
./run_audit.sh miri       # Miri under the default (rc) feature set
./run_audit.sh miri-arc   # Miri under arc + tokio-runtime
./run_audit.sh miri-all   # both
./run_audit.sh fuzz-quick # 30s cargo-fuzz pass, seeded from fuzz/seeds/
./run_audit.sh fuzz       # longer fuzz run (default 60s per target)
```

Findings and resolutions are recorded in [docs/audit.md](docs/audit.md).
Regression seeds for previously-crashing inputs live under
[`fuzz/seeds/`](fuzz/seeds/).

## Credits

In loving memory of my father, Mr. Dai Wenhua, Who bought me my first computer.

## License

This library is licensed under the GPLv2 or later License. See the [LICENSE](LICENSE) file.
