#![no_main]
//! Fuzz `BMap::read` — the public API that parses a root buffer and
//! selects the direct/btree variant. Exercises the same unsafe pointer
//! reinterpretation paths as the node fuzz targets, but via the top-level
//! entry point users actually call.

use libfuzzer_sys::fuzz_target;
use btree_ondisk::bmap::BMap;
use btree_ondisk::{NullBlockLoader, NullNodeCache};

fuzz_target!(|data: &[u8]| {
    if data.len() < 8 {
        return;
    }
    let _ = BMap::<u64, u64, u64, NullBlockLoader, NullNodeCache>::read(
        data, 256, NullBlockLoader, NullNodeCache,
    );
});
