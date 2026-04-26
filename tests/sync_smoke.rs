//! Smoke test exercising the sync (`sync-api`) feature path.
//! Only compiled when `sync-api` is enabled; methods are plain sync fns.

#![cfg(feature = "sync-api")]

use std::io::ErrorKind;
use btree_ondisk::bmap::BMap;
use btree_ondisk::{NullBlockLoader, NullNodeCache};

#[test]
fn sync_api_insert_lookup_delete_truncate() {
    let mut m = BMap::<u64, u64, u64, NullBlockLoader, NullNodeCache>::new(
        56, 256, NullBlockLoader, NullNodeCache,
    ).unwrap();
    for k in 0..64u64 {
        let _ = m.insert(k, k + 1).unwrap();
    }
    for k in 0..64u64 {
        assert_eq!(m.lookup(&k).unwrap(), k + 1);
    }
    m.truncate(&32).unwrap();
    assert_eq!(m.last_key().unwrap(), 31);
    m.delete(&0).unwrap();
    assert!(matches!(
        m.lookup(&0).err().map(|e| e.kind()),
        Some(ErrorKind::NotFound)
    ));
}
