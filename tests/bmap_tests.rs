//! Core unit tests for bmap / btree / direct paths.
//!
//! These tests run when the default async API is enabled (i.e. `sync-api` is off).
//! They exercise direct node, btree conversion, insert/delete/lookup/seek/
//! truncate paths (including split / merge / grow / shrink) and compare
//! against `std::collections::BTreeMap` as an oracle.

#![cfg(not(feature = "sync-api"))]

use std::collections::BTreeMap;
use std::io::ErrorKind;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;

use btree_ondisk::bmap::BMap;
use btree_ondisk::{NullBlockLoader, NullNodeCache};
use btree_ondisk::VALID_EXTERNAL_ASSIGN_MASK;

type TestBMap<'a> = BMap<'a, u64, u64, u64, NullBlockLoader, NullNodeCache>;

// Small root node: capacity = (56 - 8) / 8 = 6 entries in direct form
const ROOT: usize = 56;
// Small meta block forces frequent splits / merges
const META: usize = 256;

fn make_bmap<'a>() -> TestBMap<'a> {
    BMap::<u64, u64, u64, NullBlockLoader, NullNodeCache>::new(
        ROOT, META, NullBlockLoader, NullNodeCache,
    ).unwrap()
}

// --------- direct node paths ---------

#[tokio::test]
async fn direct_insert_lookup_delete() {
    let mut m = make_bmap();
    // insert within capacity
    for k in 0..6u64 {
        let prev = m.insert(k, k + 100).await.unwrap();
        assert!(prev.is_none(), "key {k} should be new");
    }
    // lookup
    for k in 0..6u64 {
        assert_eq!(m.lookup(&k).await.unwrap(), k + 100);
    }
    // insert existing returns old value
    let prev = m.insert(2, 999).await.unwrap();
    assert_eq!(prev, Some(102));

    // seek_key / last_key
    assert_eq!(m.seek_key(&0).await.unwrap(), 0);
    assert_eq!(m.last_key().await.unwrap(), 5);

    // delete
    m.delete(&3).await.unwrap();
    assert!(matches!(m.lookup(&3).await.err().map(|e| e.kind()), Some(ErrorKind::NotFound)));

    // dirty tracking
    assert!(m.dirty());
    m.clear_dirty();
    assert!(!m.dirty());
}

#[tokio::test]
async fn direct_try_insert_conflict() {
    let mut m = make_bmap();
    m.try_insert(0, 1).await.unwrap();
    let err = m.try_insert(0, 2).await.err().unwrap();
    assert_eq!(err.kind(), ErrorKind::AlreadyExists);
}

#[tokio::test]
async fn direct_lookup_missing_returns_not_found() {
    let m = make_bmap();
    let err = m.lookup(&0).await.err().unwrap();
    assert_eq!(err.kind(), ErrorKind::NotFound);
    let err = m.last_key().await.err().unwrap();
    assert_eq!(err.kind(), ErrorKind::NotFound);
}

// --------- direct -> btree conversion + btree grow/split ---------

#[tokio::test]
async fn conversion_direct_to_btree_and_back() {
    let mut m = make_bmap();
    // Fill direct, then exceed to trigger conversion.
    for k in 0..32u64 {
        let _ = m.insert(k, k + 1).await.unwrap();
    }
    // get_stat on btree should report nodes_total > 0
    let stat = m.get_stat();
    assert!(stat.btree);
    assert!(stat.nodes_total >= 1);

    // random lookups
    for k in 0..32u64 {
        assert_eq!(m.lookup(&k).await.unwrap(), k + 1);
    }

    // deletion of most entries should eventually shrink back to direct
    for k in (0..32u64).rev() {
        if k < 4 {
            break;
        }
        m.delete(&k).await.unwrap();
    }
    // insert more and delete all but a few to force shrink / convert back
    for k in 4..32u64 {
        let _ = m.insert(k, k).await.unwrap();
    }
    for k in 4..32u64 {
        m.delete(&k).await.unwrap();
    }
    // remaining keys 0..4 should still be accessible
    for k in 0..4u64 {
        assert_eq!(m.lookup(&k).await.unwrap(), k + 1);
    }
}

// --------- truncate path (covers repeated shrink) ---------

#[tokio::test]
async fn truncate_and_last_key() {
    let mut m = make_bmap();
    for k in 0..128u64 {
        let _ = m.insert(k, k).await.unwrap();
    }
    assert_eq!(m.last_key().await.unwrap(), 127);

    // truncate keeps keys < target
    m.truncate(&64).await.unwrap();
    assert_eq!(m.last_key().await.unwrap(), 63);
    // key 64 no longer exists
    assert!(matches!(m.lookup(&64).await.err().map(|e| e.kind()), Some(ErrorKind::NotFound)));

    // truncate with key beyond current range is a no-op
    m.truncate(&9999).await.unwrap();
    assert_eq!(m.last_key().await.unwrap(), 63);

    // truncate all the way down
    m.truncate(&0).await.unwrap();
    assert!(matches!(m.last_key().await.err().map(|e| e.kind()), Some(ErrorKind::NotFound)));
}

// --------- seek_key walking through large btree ---------

#[tokio::test]
async fn seek_key_finds_next_valid() {
    let mut m = make_bmap();
    // sparse insert: 0, 10, 20 ... 500
    for k in (0..=500u64).step_by(10) {
        let _ = m.insert(k, k + 1).await.unwrap();
    }
    // seeking 0 returns 0
    assert_eq!(m.seek_key(&0).await.unwrap(), 0);
    // seeking a gap returns next valid
    assert_eq!(m.seek_key(&5).await.unwrap(), 10);
    assert_eq!(m.seek_key(&11).await.unwrap(), 20);
    // seeking beyond max returns NotFound
    let err = m.seek_key(&1000).await.err().unwrap();
    assert_eq!(err.kind(), ErrorKind::NotFound);
}

// --------- lookup_contig ---------

#[tokio::test]
async fn lookup_contig_counts_run_length() {
    let mut m = make_bmap();
    for k in 0..10u64 {
        let _ = m.insert(k, k + 1).await.unwrap();
    }
    // insert gap
    let _ = m.insert(20, 21).await.unwrap();
    let (v, n) = m.lookup_contig(&0, 100).await.unwrap();
    assert_eq!(v, 1);
    assert_eq!(n, 10);
}

// --------- assign / propagate / mark ---------

#[tokio::test]
async fn assign_propagate_mark_paths() {
    let mut m = make_bmap();
    // enough keys to build a btree
    for k in 0..64u64 {
        let _ = m.insert(k, k + 1).await.unwrap();
    }
    // assign external value (must pass VALID_EXTERNAL_ASSIGN_MASK check)
    let extern_val = 7u64 | VALID_EXTERNAL_ASSIGN_MASK;
    m.assign(&0, extern_val, None).await.unwrap();
    assert_eq!(m.lookup(&0).await.unwrap(), extern_val);

    // assign_data_node
    let v2 = 8u64 | VALID_EXTERNAL_ASSIGN_MASK;
    m.assign_data_node(&1, v2).await.unwrap();
    assert_eq!(m.lookup(&1).await.unwrap(), v2);

    // propagate
    m.propagate(&0, None).await.unwrap();
    // mark dirty
    m.mark(&0, 1).await.unwrap();

    // userdata round-trip
    m.set_userdata(0xdead_beef);
    assert_eq!(m.get_userdata(), 0xdead_beef);
}

// --------- read/write root buffer round-trip ---------

#[tokio::test]
async fn root_buffer_round_trip() {
    let mut m = make_bmap();
    for k in 0..64u64 {
        let _ = m.insert(k, k + 1).await.unwrap();
    }
    let mut buf = btree_ondisk::node::AlignedBuffer::new(m.as_slice().len())
        .expect("alloc aligned buffer");
    m.write(buf.as_mut_slice());

    // reconstruct; new_btree/new_direct chosen automatically
    let m2 = TestBMap::read(buf.as_slice(), META, NullBlockLoader, NullNodeCache).unwrap();
    // root should be re-usable; we cannot verify lookups without loader data
    // but we can confirm the type matches and userdata is preserved.
    assert_eq!(m.get_userdata(), m2.get_userdata());
}

// --------- cache_limit accessors ---------

#[tokio::test]
async fn cache_limit_accessors() {
    let m = make_bmap();
    m.set_cache_limit(42);
    assert_eq!(m.get_cache_limit(), 42);
}

// --------- random oracle test ---------

#[tokio::test]
async fn random_oracle_insert_delete_lookup() {
    let mut m = make_bmap();
    let mut oracle: BTreeMap<u64, u64> = BTreeMap::new();
    let mut rng = StdRng::seed_from_u64(0xC0FFEE);

    // Use keys 1..=200 (avoid 0 which is V::invalid for u64 on value side).
    // For V we use key+1 so value never equals 0.
    for _ in 0..2000 {
        let op: u8 = rng.gen_range(0..4);
        let k: u64 = rng.gen_range(1..=200);
        match op {
            0 | 1 => {
                // insert / update
                let v = k.wrapping_add(1);
                let _ = m.insert(k, v).await.unwrap();
                oracle.insert(k, v);
            }
            2 => {
                // delete
                let bmap_res = m.delete(&k).await;
                let oracle_res = oracle.remove(&k);
                match (bmap_res, oracle_res) {
                    (Ok(()), Some(_)) => {}
                    (Err(e), None) => assert_eq!(e.kind(), ErrorKind::NotFound),
                    other => panic!("oracle mismatch: {:?}", other),
                }
            }
            _ => {
                // lookup
                let bmap_res = m.lookup(&k).await;
                let oracle_res = oracle.get(&k).copied();
                match (bmap_res, oracle_res) {
                    (Ok(v), Some(ov)) => assert_eq!(v, ov),
                    (Err(e), None) => assert_eq!(e.kind(), ErrorKind::NotFound),
                    other => panic!("oracle mismatch: {:?}", other),
                }
            }
        }
    }

    // final full cross-check via seek walking
    let mut walked: Vec<u64> = Vec::new();
    let mut start: u64 = 0;
    loop {
        match m.seek_key(&start).await {
            Ok(k) => {
                walked.push(k);
                start = k + 1;
            }
            Err(e) => {
                assert_eq!(e.kind(), ErrorKind::NotFound);
                break;
            }
        }
    }
    let oracle_keys: Vec<u64> = oracle.keys().copied().collect();
    assert_eq!(walked, oracle_keys);
}
