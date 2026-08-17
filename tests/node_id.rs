#![cfg(not(feature = "sync-api"))]
//! Regression coverage for `BtreeNodeDirty::id()` and the in-place node
//! placement it enables.
//!
//! A caller that places nodes itself (the `lookup_dirty` / `assign_meta_node`
//! protocol) needs to write a dirty node back to the pointer it already
//! occupies; otherwise every flush burns a fresh pointer and the location the
//! previous version occupied stays referenced forever. These tests pin:
//!
//!   1. what `id()` reports for a node that has never been placed,
//!   2. that reusing the reported pointer keeps the tree readable,
//!   3. that pointer consumption stops growing once the tree stops splitting.

use std::collections::HashMap;
use btree_ondisk::bmap::BMap;
use btree_ondisk::NodeValue;
use btree_ondisk::{NullBlockLoader, NullNodeCache};

type Map<'a> = BMap<'a, u64, u64, u64, NullBlockLoader, NullNodeCache>;

/// External pointers must carry `VALID_EXTERNAL_ASSIGN_MASK` so they can never
/// collide with the internal sequence numbers the library hands out.
const EXTERN: u64 = btree_ondisk::VALID_EXTERNAL_ASSIGN_MASK;

const ROOT_NODE_SIZE: usize = 56;
const META_BLOCK_SIZE: usize = 4096;

fn new_map<'a>() -> Map<'a> {
    BMap::<u64, u64, u64, NullBlockLoader, NullNodeCache>::new(
        ROOT_NODE_SIZE, META_BLOCK_SIZE, NullBlockLoader, NullNodeCache,
    )
    .expect("BMap::new")
}

/// Place every dirty node, reusing its current pointer when it already has an
/// external one. Returns (freshly allocated, reused in place).
async fn flush(
    m: &Map<'_>,
    next_off: &mut u64,
    store: &mut HashMap<u64, Vec<u8>>,
) -> (usize, usize) {
    let (mut fresh, mut reused) = (0, 0);
    for n in m.lookup_dirty() {
        let cur = n.id();
        let off = if cur.is_valid_extern_assign() {
            reused += 1;
            cur
        } else {
            fresh += 1;
            *next_off += 1;
            EXTERN | *next_off
        };
        store.insert(off, n.as_slice().to_vec());
        m.assign_meta_node(off, n.clone()).await.expect("assign_meta_node");
        n.clear_dirty();
    }
    (fresh, reused)
}

/// A dirty node that has never been placed reports an internal sequence
/// number, which is *not* `invalid_value()`. Callers must therefore
/// discriminate with `is_valid_extern_assign()`; using `is_invalid()` would
/// mistake an internal sequence number for a reusable storage location.
#[tokio::test]
async fn id_of_unplaced_node_is_an_internal_seq_not_invalid() {
    let mut m = new_map();
    for k in 0..600u64 {
        let _: Option<u64> = m.insert(k, k + 1).await.expect("insert");
    }

    let dirty = m.lookup_dirty();
    assert!(dirty.len() > 1, "expected a multi-node tree, got {}", dirty.len());

    // Nothing has been placed yet, so no node may claim an external pointer.
    for n in &dirty {
        assert!(
            !n.id().is_valid_extern_assign(),
            "unplaced node reported an external pointer: {}",
            n.id()
        );
    }

    // And the majority are *not* is_invalid(): they hold internal sequence
    // numbers. This is what makes is_invalid() the wrong discriminator.
    let not_invalid = dirty.iter().filter(|n| !n.id().is_invalid()).count();    assert!(
        not_invalid > 0,
        "expected unplaced nodes carrying internal seqs, none reported"
    );
}

/// Reusing the pointer reported by `id()` keeps every key readable, and stops
/// consuming new pointers once the tree stops splitting.
#[tokio::test]
async fn in_place_reuse_preserves_reads_and_stops_growing() {
    let mut m = new_map();
    let mut next_off = 0u64;
    let mut store: HashMap<u64, Vec<u8>> = HashMap::new();

    for k in 0..600u64 {
        let _: Option<u64> = m.insert(k, k + 1).await.expect("insert");
    }

    // First flush: nothing is placed yet, so every node takes a fresh pointer.
    let (fresh1, reused1) = flush(&m, &mut next_off, &mut store).await;
    assert!(fresh1 > 0, "first flush should place nodes");
    assert_eq!(reused1, 0, "nothing could be reused on the first flush");
    let after_first = store.len();

    // Second flush: new keys split a node, so one fresh pointer is expected,
    // but the nodes that merely changed must land back where they were.
    for k in 0..50u64 {
        let _: Option<u64> = m.insert(10_000 + k, k).await.expect("insert");
    }
    let (_fresh2, reused2) = flush(&m, &mut next_off, &mut store).await;
    assert!(reused2 > 0, "second flush reused nothing in place");

    // Third flush: pure overwrites of existing keys must not consume any new
    // pointer at all -- this is the property that lets old storage be freed.
    for k in 0..50u64 {
        let _: Option<u64> = m.insert(k, k + 999).await.expect("update");
    }
    let before_third = store.len();
    let (fresh3, reused3) = flush(&m, &mut next_off, &mut store).await;
    assert_eq!(fresh3, 0, "overwrite-only flush allocated {fresh3} new pointers");
    assert!(reused3 > 0, "overwrite-only flush placed nothing");
    assert_eq!(
        store.len(), before_third,
        "overwrite-only flush grew the pointer set"
    );

    // Total pointer consumption stayed near the tree's node count instead of
    // growing once per flush.
    assert!(
        store.len() <= after_first + 2,
        "pointer set grew from {after_first} to {} across flushes",
        store.len()
    );

    // The tree must still read back correctly after in-place reassignment.
    for k in 0..600u64 {
        let want = if k < 50 { k + 999 } else { k + 1 };
        assert_eq!(m.lookup(&k).await.expect("lookup"), want, "mismatch at {k}");
    }
    for k in 0..50u64 {
        assert_eq!(m.lookup(&(10_000 + k)).await.expect("lookup"), k);
    }
}

/// Assigning a node back to the pointer it already occupies is a supported
/// no-op, and `id()` keeps reporting that pointer afterwards.
#[tokio::test]
async fn assign_to_current_id_is_idempotent() {
    let mut m = new_map();
    let mut next_off = 0u64;
    let mut store: HashMap<u64, Vec<u8>> = HashMap::new();

    for k in 0..600u64 {
        let _: Option<u64> = m.insert(k, k + 1).await.expect("insert");
    }
    let _ = flush(&m, &mut next_off, &mut store).await;

    // Dirty the tree, then assign each node to its own current pointer twice.
    for k in 0..50u64 {
        let _: Option<u64> = m.insert(k, k + 7).await.expect("update");
    }
    for n in m.lookup_dirty() {
        let cur = n.id();
        if !cur.is_valid_extern_assign() {
            continue;
        }
        m.assign_meta_node(cur, n.clone()).await.expect("first in-place assign");
        assert_eq!(n.id(), cur, "id changed after in-place assign");
        m.assign_meta_node(cur, n.clone()).await.expect("second in-place assign");
        assert_eq!(n.id(), cur, "id changed after repeated in-place assign");
    }

    for k in 0..600u64 {
        let want = if k < 50 { k + 7 } else { k + 1 };
        assert_eq!(m.lookup(&k).await.expect("lookup"), want, "mismatch at {k}");
    }
}
