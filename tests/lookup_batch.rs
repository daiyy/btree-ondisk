//! Oracle correctness test for BMap::lookup_batch.
//!
//! Gated by #[cfg(not(feature = "sync-api"))] because the helper
//! BMap APIs are async-only when `sync-api` is off. The batch
//! lookup path is now available under all feature combinations
//! including `mt`; the marker-trait pattern in src/lib.rs
//! (MaybeSendSync / MaybeSync) supplies the conditional Send/Sync
//! bound that read_batch needs there.

#![cfg(not(feature = "sync-api"))]

use std::collections::BTreeMap;
use std::io::ErrorKind;

use btree_ondisk::bmap::BMap;
use btree_ondisk::{MemoryBlockLoader, NullNodeCache};
use btree_ondisk::VALID_EXTERNAL_ASSIGN_MASK;

const ROOT: usize = 56;
const META: usize = 256;

type Fixture<'a> = BMap<'a, u64, u64, u64, MemoryBlockLoader<u64>, NullNodeCache>;

fn make_bmap<'a>() -> (Fixture<'a>, MemoryBlockLoader<u64>) {
    // The loader's internal block size must match the btree's
    // meta_block_size because the btree hands loader.read a buffer
    // of exactly meta_block_size bytes (via node.as_u8_mut()).
    let loader = MemoryBlockLoader::<u64>::new(META);
    let bmap = BMap::<u64, u64, u64, _, _>::new(
        ROOT, META, loader.clone(), NullNodeCache,
    ).unwrap();
    (bmap, loader)
}

/// Populate a bmap with a known set of keys and fully flush so all
/// meta nodes are persisted into the MemoryBlockLoader backend.
/// After flush, a fresh lookup has to go through read / read_batch.
async fn populate_and_flush(num_keys: u64) -> (Fixture<'static>, BTreeMap<u64, u64>) {
    let (mut bmap, loader) = make_bmap();
    let mut oracle = BTreeMap::new();
    let mut seq = VALID_EXTERNAL_ASSIGN_MASK + 1;

    for k in 0..num_keys {
        let v = k.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 0x8000_0000_0000_0000;
        bmap.insert(k, v).await.unwrap();
        oracle.insert(k, v);
    }

    // Flush: assign external ids to every dirty meta node, then
    // persist their bytes into the loader, then clear dirty.
    let dirty_meta = bmap.lookup_dirty();
    let mut assigned = Vec::new();
    for n in &dirty_meta {
        let id = seq;
        seq += 1;
        bmap.assign_meta_node(id, n.clone()).await.unwrap();
        assigned.push(id);
    }
    for (n, id) in dirty_meta.iter().zip(assigned.iter()) {
        loader.write(*id, n.as_slice());
    }
    for n in dirty_meta {
        n.clear_dirty();
    }
    bmap.clear_dirty();

    // Force eviction so the next lookup must go through the loader.
    bmap.set_cache_limit(1);
    (bmap, oracle)
}

#[tokio::test]
async fn lookup_batch_matches_single_lookup() {
    // Large enough to reach height > 2 with META=256.
    let num_keys: u64 = 2_000;
    let (bmap, oracle) = populate_and_flush(num_keys).await;

    // Probe set: every stored key, plus some misses between keys,
    // plus a couple extreme values.
    let mut probes: Vec<u64> = Vec::new();
    for k in 0..num_keys {
        probes.push(k);
    }
    for k in 0..10u64 {
        probes.push(num_keys + k); // guaranteed miss: above range
    }
    probes.push(u64::MAX);
    probes.push(0);

    // Reference: per-key lookup.
    let mut reference: Vec<Result<u64, ErrorKind>> = Vec::with_capacity(probes.len());
    for k in &probes {
        match bmap.lookup(k).await {
            Ok(v) => reference.push(Ok(v)),
            Err(e) => reference.push(Err(e.kind())),
        }
    }

    // Under test: batch lookup in a single call.
    let batch = bmap.lookup_batch(&probes).await;
    assert_eq!(batch.len(), probes.len());

    // Normalize and compare elementwise.
    for (i, (got, want)) in batch.iter().zip(reference.iter()).enumerate() {
        let got_norm: Result<u64, ErrorKind> = match got {
            Ok(v) => Ok(*v),
            Err(e) => Err(e.kind()),
        };
        assert_eq!(
            &got_norm, want,
            "batch/single divergence at probe[{i}] = {}, oracle={:?}",
            probes[i],
            oracle.get(&probes[i]),
        );

        // Cross-check against the std BTreeMap oracle for hits.
        if let Ok(v) = got {
            assert_eq!(Some(v), oracle.get(&probes[i]),
                "batch lookup returned wrong value for key {}", probes[i]);
        }
    }
}

#[tokio::test]
async fn lookup_batch_handles_duplicates_and_empty() {
    let (bmap, _oracle) = populate_and_flush(500).await;

    // Empty input should yield empty output, no IO at all.
    let empty = bmap.lookup_batch(&[]).await;
    assert!(empty.is_empty());

    // Duplicate keys: answer must be identical to the single
    // lookup repeated, and get_from_nodes_batch should dedup ids
    // internally without producing wrong results for the
    // duplicates.
    let keys = vec![42u64, 42, 42, 7, 100, 42, 7];
    let batch = bmap.lookup_batch(&keys).await;
    assert_eq!(batch.len(), keys.len());
    for (i, k) in keys.iter().enumerate() {
        let single = bmap.lookup(k).await;
        match (&batch[i], single) {
            (Ok(a), Ok(b)) => assert_eq!(*a, b),
            (Err(a), Err(b)) => assert_eq!(a.kind(), b.kind()),
            (a, b) => panic!("duplicate-key divergence at i={i} key={k}: batch={a:?} single={b:?}"),
        }
    }
}

#[tokio::test]
async fn lookup_batch_on_direct_map_still_works() {
    // A small map that stays in the Direct (non-btree) arm: ROOT
    // capacity is (56 - 8) / 16 = 3 entries.
    let (mut bmap, _loader) = make_bmap();
    bmap.insert(1, 101).await.unwrap();
    bmap.insert(2, 202).await.unwrap();

    let keys = vec![1u64, 2, 3];
    let batch = bmap.lookup_batch(&keys).await;
    assert_eq!(batch.len(), 3);
    assert_eq!(batch[0].as_ref().ok().copied(), Some(101));
    assert_eq!(batch[1].as_ref().ok().copied(), Some(202));
    assert!(batch[2].is_err()); // key 3 not inserted
}

/// Regression for a fuzz finding (`bmap_lookup_batch` /
/// crash-42d55cc3...): lookup_batch returned NotFound for a key
/// that lookup() found and the BTreeMap oracle held.
///
/// Root cause: `BMap::convert_and_insert` initialises its very
/// first leaf with `last_seq=0`, which equals
/// `NodeValue::invalid_value()` for `u64`. The single-key
/// `do_lookup` ignores that invalidness and still resolves the
/// child via the in-memory cache; the original
/// `do_lookup_batch` short-circuited on `is_invalid()` and
/// reported NotFound.
///
/// Fix: align `do_lookup_batch` with `do_lookup` and let the
/// cache / loader resolve the id without a special invalid
/// short-circuit.
#[tokio::test]
async fn lookup_batch_resolves_invalid_first_leaf_id() {
    use btree_ondisk::bmap::BMap;
    use btree_ondisk::{NullBlockLoader, NullNodeCache};

    let mut m: BMap<u64, u64, u64, NullBlockLoader, NullNodeCache> =
        BMap::new(56, 256, NullBlockLoader, NullNodeCache).unwrap();
    // Reproduces the convert_and_insert-then-batch-lookup window.
    m.insert(5, 5).await.unwrap();
    m.insert(2, 2).await.unwrap();
    m.insert(1, 1).await.unwrap();
    m.insert(383, 383).await.unwrap();

    let batch = m.lookup_batch(&[5]).await;
    assert_eq!(batch.len(), 1);
    assert_eq!(*batch[0].as_ref().unwrap(), 5);
    let single = m.lookup(&5).await.unwrap();
    assert_eq!(single, 5);
}
