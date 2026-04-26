//! Additional coverage tests targeting paths missed by `bmap_tests.rs`:
//!  - direct-node branches in BMap helpers (assign/propagate/mark/lookup_contig/lookup_dirty/seek)
//!  - MemoryBlockLoader + eviction round-trip
//!  - BtreeNodeDirty accessors (size/as_slice/clear_dirty) via flush-like flow
//!  - Display impls
//!  - node.rs error paths (bad size / alignment)
//!  - u64 default BlockLoader impl
//!  - NullBlockLoader / NullNodeCache / MemoryBlockLoader behavior

#![cfg(not(feature = "sync-api"))]

use std::io::ErrorKind;

use btree_ondisk::bmap::BMap;
use btree_ondisk::node::{BtreeNode, DirectNode};
use btree_ondisk::{
    BlockLoader, MemoryBlockLoader, NodeCache, NullBlockLoader, NullNodeCache,
    VALID_EXTERNAL_ASSIGN_MASK,
};

type NullBMap<'a> = BMap<'a, u64, u64, u64, NullBlockLoader, NullNodeCache>;
type MemBMap<'a> = BMap<'a, u64, u64, u64, MemoryBlockLoader<u64>, NullNodeCache>;

const ROOT: usize = 56;
const META: usize = 256;

fn null_bmap<'a>() -> NullBMap<'a> {
    BMap::new(ROOT, META, NullBlockLoader, NullNodeCache)
}

// --- BMap direct-node arms ---

#[tokio::test]
async fn direct_arm_paths() {
    let mut m = null_bmap();
    // on empty direct: dirty, clear_dirty, lookup_contig, lookup_dirty,
    // seek_key, mark, propagate, truncate
    assert!(!m.dirty());
    m.clear_dirty();
    assert!(m.lookup_dirty().is_empty());
    assert_eq!(m.seek_key(&0).await.err().unwrap().kind(), ErrorKind::NotFound);
    assert_eq!(
        m.lookup_contig(&0, 8).await.err().unwrap().kind(),
        ErrorKind::NotFound
    );
    m.mark(&0, 1).await.unwrap();
    m.propagate(&0, None).await.unwrap();
    // truncate when key is beyond last_key returns Err(NotFound) -> Ok on no-op path
    let _ = m.truncate(&0).await; // last_key is NotFound so the fn may error; both OK

    // populate direct and hit assign path (V==P so assign_data_node works)
    for k in 0..6u64 {
        let _ = m.insert(k, k + 1).await.unwrap();
    }
    let v = 7u64 | VALID_EXTERNAL_ASSIGN_MASK;
    m.assign(&0, v, None).await.unwrap();
    m.assign_data_node(&1, v).await.unwrap();

    // assign_meta_node on direct returns Ok(()) immediately
    // build a dummy btree node to pass in
    let mut other = null_bmap();
    for k in 0..64u64 {
        let _ = other.insert(k, k + 1).await.unwrap();
    }
    let dirty = other.lookup_dirty();
    assert!(!dirty.is_empty());
    let sample = dirty[0].clone();
    // size / as_slice / clear_dirty on BtreeNodeDirty
    assert_eq!(sample.size(), sample.as_slice().len());
    m.assign_meta_node(v, sample.clone()).await.unwrap();
    sample.clear_dirty();

    // direct arm lookup_contig with some valid entries
    let (val, cnt) = m.lookup_contig(&0, 8).await.unwrap();
    assert!(cnt >= 1);
    let _ = val;
}

#[tokio::test]
async fn direct_assign_error_paths() {
    let m = null_bmap();
    // assign when key exceeds direct capacity -> InvalidData
    let err = m
        .assign(&999, 1u64 | VALID_EXTERNAL_ASSIGN_MASK, None)
        .await
        .err()
        .unwrap();
    assert_eq!(err.kind(), ErrorKind::InvalidData);

    // assign_data_node when key not yet inserted -> NotFound
    let err = m
        .assign_data_node(&0, 2u64 | VALID_EXTERNAL_ASSIGN_MASK)
        .await
        .err()
        .unwrap();
    assert_eq!(err.kind(), ErrorKind::NotFound);
}

#[tokio::test]
async fn direct_delete_and_seek_errors() {
    let mut m = null_bmap();
    // delete on empty direct -> NotFound
    assert_eq!(
        m.delete(&0).await.err().unwrap().kind(),
        ErrorKind::NotFound
    );
}

#[tokio::test]
async fn direct_lookup_level_and_tiny_root() {
    let m = null_bmap();
    // lookup at level != 1 on direct returns NotFound
    let err = m.lookup_at_level(&0, 2).await.err().unwrap();
    assert_eq!(err.kind(), ErrorKind::NotFound);

    // a root so small it has zero capacity for V=u64
    // header is 8 bytes so anything < 16 makes capacity = 0
    let m2: NullBMap = BMap::new(8, META, NullBlockLoader, NullNodeCache);
    let err = m2.lookup(&0).await.err().unwrap();
    assert_eq!(err.kind(), ErrorKind::NotFound);
}

#[tokio::test]
async fn direct_arm_accessors() {
    // small bmap that stays as Direct for all accessor calls
    let m = null_bmap();
    // get_stat on direct returns default
    let s = m.get_stat();
    assert!(!s.btree);
    // userdata
    m.set_userdata(0x1234);
    assert_eq!(m.get_userdata(), 0x1234);
    // cache_limit
    m.set_cache_limit(7);
    assert_eq!(m.get_cache_limit(), 7);
    // block_loader / node_cache
    let _ = m.get_block_loader();
    let _ = m.get_node_cache();
}

#[tokio::test]
async fn new_direct_new_btree_constructors() {
    // build a populated bmap, snapshot as_slice, then reconstruct
    let mut m = null_bmap();
    for k in 0..4u64 {
        let _ = m.insert(k, k + 1).await.unwrap();
    }
    let buf = m.as_slice().to_vec();
    // new_direct on the captured direct root
    let m2 = NullBMap::new_direct(&buf, META, NullBlockLoader, NullNodeCache);
    assert_eq!(m2.lookup(&0).await.unwrap(), 1);

    // grow to btree then dump, use new_btree directly
    let mut m3 = null_bmap();
    for k in 0..64u64 {
        let _ = m3.insert(k, k + 1).await.unwrap();
    }
    let buf = m3.as_slice().to_vec();
    let m4 = NullBMap::new_btree(&buf, META, NullBlockLoader, NullNodeCache);
    // stat path on btree
    let _ = m4.get_stat();
}

#[tokio::test]
async fn display_impls() {
    let mut m = null_bmap();
    // Display on direct
    let _ = format!("{}", m);
    for k in 0..32u64 {
        let _ = m.insert(k, k + 1).await.unwrap();
    }
    // Display on btree
    let _ = format!("{}", m);

    // Display on DirectNode / BtreeNode via copy_from_slice (aligned alloc)
    let bn = BtreeNode::<u64, u64, u64>::copy_from_slice(0u64, m.as_slice()).unwrap();
    let _ = format!("{}", bn);

    // direct node display
    let dn = DirectNode::<u64>::new(64).unwrap();
    dn.init(0, 1, 0);
    let _ = format!("{}", dn);
}

// --- node.rs error paths ---

#[test]
fn btree_node_from_slice_errors() {
    // too small
    let mut small = vec![0u8; 4];
    assert!(BtreeNode::<u64, u64, u64>::from_slice(&mut small).is_err());
    assert!(DirectNode::<u64>::from_slice(&mut small).is_err());
    // from_slice_ref error
    assert!(BtreeNode::<u64, u64, u64>::from_slice_ref(&small).is_err());
    assert!(DirectNode::<u64>::from_slice_ref(&small).is_err());
}

#[test]
fn btree_node_from_slice_bad_nchild() {
    // bad header: capacity < nchildren -> InvalidData.
    // Use AlignedBuffer to guarantee an 8-aligned backing buffer (works
    // under Miri where Vec<u8> isn't 8-aligned).
    let mut buf = btree_ondisk::node::AlignedBuffer::new(64).unwrap();
    let bytes = buf.as_mut_slice();
    bytes[2] = 0xFF;
    bytes[3] = 0xFF;
    assert!(BtreeNode::<u64, u64, u64>::from_slice(bytes).is_err());

    let mut buf2 = btree_ondisk::node::AlignedBuffer::new(64).unwrap();
    let bytes2 = buf2.as_mut_slice();
    bytes2[2] = 0xFF;
    bytes2[3] = 0xFF;
    assert!(DirectNode::<u64>::from_slice(bytes2).is_err());
}

#[test]
fn btree_node_from_slice_misaligned() {
    // Offset 1 from an 8-aligned buffer guarantees a non-8-aligned view,
    // exercising the alignment-error branch under any allocator.
    let mut buf = btree_ondisk::node::AlignedBuffer::new(80).unwrap();
    let raw: &mut [u8] = unsafe {
        std::slice::from_raw_parts_mut(buf.as_mut_slice().as_mut_ptr().add(1), 40)
    };
    let err = BtreeNode::<u64, u64, u64>::from_slice(raw).err().unwrap();
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
}

#[test]
fn btree_node_as_u8_mut_and_eq() {
    let mut n = BtreeNode::<u64, u64, u64>::new(256).unwrap();
    let _ = n.as_u8_mut().len();
    let n2 = BtreeNode::<u64, u64, u64>::new(256).unwrap();
    // PartialEq
    assert!(n == n);
    assert!(n != n2);

    // DirectNode as_u8_mut and Display
    let mut d = DirectNode::<u64>::new(64).unwrap();
    let _ = d.as_u8_mut();
    let _ = format!("{}", d);
}

#[test]
fn btree_node_new_copy_from_slice() {
    // new() succeeds for a valid size
    let node = BtreeNode::<u64, u64, u64>::new(256).unwrap();
    assert!(node.as_u8_ref().len() >= 256);
    // copy_from_slice
    let buf = vec![0u8; 256];
    let n2 = BtreeNode::<u64, u64, u64>::copy_from_slice(0u64, &buf).unwrap();
    assert_eq!(n2.as_u8_ref().len(), 256);
    // DirectNode new/copy_from_slice
    let dn = DirectNode::<u64>::new(64).unwrap();
    assert_eq!(dn.as_u8_ref().len(), 64);
    let buf = vec![0u8; 64];
    let _ = DirectNode::<u64>::copy_from_slice(&buf).unwrap();
}

// --- u64 default BlockLoader impl ---

#[tokio::test]
async fn u64_blockloader_default_impl() {
    let l: u64 = 0;
    let mut buf = vec![0u8; 16];
    let v = <u64 as BlockLoader<u64>>::read(&l, 0, &mut buf, 0).await.unwrap();
    assert!(v.is_empty());
    let l2 = <u64 as BlockLoader<u64>>::dup_from_new_path(l, "anything");
    assert_eq!(l, l2);
}

// --- NullBlockLoader ---

#[tokio::test]
async fn null_block_loader() {
    let l = NullBlockLoader;
    let mut buf = vec![0u8; 16];
    let v = <NullBlockLoader as BlockLoader<u64>>::read(&l, 0, &mut buf, 0)
        .await
        .unwrap();
    assert!(v.is_empty());
    let _ = <NullBlockLoader as BlockLoader<u64>>::dup_from_new_path(l.clone(), "x");
}

// --- NullNodeCache ---

#[tokio::test]
async fn null_node_cache_methods() {
    let c = NullNodeCache;
    <NullNodeCache as NodeCache<u64>>::push(&c, &1u64, &[0u8; 4]);
    let mut buf = [0u8; 4];
    assert!(
        !<NullNodeCache as NodeCache<u64>>::load(&c, 1u64, &mut buf)
            .await
            .unwrap()
    );
    <NullNodeCache as NodeCache<u64>>::invalid(&c, &1u64);
    <NullNodeCache as NodeCache<u64>>::evict(&c);
    let _ = <NullNodeCache as NodeCache<u64>>::get_stats(&c);
    <NullNodeCache as NodeCache<u64>>::shutdown(&c);
}

// --- MemoryBlockLoader directly ---

#[tokio::test]
async fn memory_loader_read_write_dup() {
    let l = MemoryBlockLoader::<u64>::new(META);
    // read of missing key -> NotFound
    let mut buf = vec![0u8; META];
    let err = <MemoryBlockLoader<u64> as BlockLoader<u64>>::read(&l, 1, &mut buf, 0)
        .await
        .err()
        .unwrap();
    assert_eq!(err.kind(), ErrorKind::NotFound);
    // write then read back
    let data = vec![0xAB; META];
    l.write(1u64, &data);
    <MemoryBlockLoader<u64> as BlockLoader<u64>>::read(&l, 1, &mut buf, 0)
        .await
        .unwrap();
    assert_eq!(buf, data);
    // dup_from_new_path
    let _ = <MemoryBlockLoader<u64> as BlockLoader<u64>>::dup_from_new_path(l, "other");
}

// --- MemoryBlockLoader round-trip + eviction ---

// Full flush -> evict -> backend-reload round trip.
#[tokio::test]
async fn memory_loader_flush_and_evict() {
    let loader = MemoryBlockLoader::<u64>::new(META);
    let mut m: MemBMap = BMap::new(ROOT, META, loader.clone(), NullNodeCache);

    for k in 0..256u64 {
        let _ = m.insert(k, k + 1).await.unwrap();
    }

    // Flush: assign every dirty node an external seq, persist its bytes to
    // the loader keyed by that seq, then clear the node's dirty bit.
    let dirty = m.lookup_dirty();
    let mut seq = VALID_EXTERNAL_ASSIGN_MASK + 1u64;
    let mut assigned: Vec<u64> = Vec::with_capacity(dirty.len());
    for n in &dirty {
        let _ = n.size();
        let _ = n.as_slice().len();
        m.assign_meta_node(seq, n.clone()).await.unwrap();
        assigned.push(seq);
        seq += 1;
    }
    for (n, s) in dirty.iter().zip(assigned.into_iter()) {
        loader.write(s, n.as_slice());
    }
    for n in dirty { n.clear_dirty(); }
    m.clear_dirty();

    let nodes_before = m.get_stat().nodes_total;
    m.set_cache_limit(2);
    let nodes_after = m.get_stat().nodes_total;
    assert!(nodes_after < nodes_before,
        "expected eviction to shrink node set: {nodes_before} -> {nodes_after}");

    // Subsequent lookups must succeed by reloading evicted nodes from the
    // backend loader (exercises btree.rs get_from_nodes load path).
    for k in (0..256u64).step_by(17) {
        assert_eq!(m.lookup(&k).await.unwrap(), k + 1);
    }
}

// --- shrink-down btree to direct (delete many keys) ---

#[tokio::test]
async fn btree_shrink_back_to_direct() {
    let mut m = null_bmap();
    // grow to btree
    for k in 0..64u64 {
        let _ = m.insert(k, k + 1).await.unwrap();
    }
    assert!(m.get_stat().btree);

    // delete down to only 2 keys -> should trigger the convert_to_direct path
    for k in 2..64u64 {
        m.delete(&k).await.unwrap();
    }
    // remaining keys still readable
    assert_eq!(m.lookup(&0).await.unwrap(), 1);
    assert_eq!(m.lookup(&1).await.unwrap(), 2);
}

// --- btree lookup_contig across leaf siblings ---

#[tokio::test]
async fn btree_lookup_contig_across_siblings() {
    let mut m = null_bmap();
    // contiguous span of 200 keys -> will span multiple leaves with small meta
    for k in 0..200u64 {
        let _ = m.insert(k, k + 1).await.unwrap();
    }
    let (v, n) = m.lookup_contig(&0, 200).await.unwrap();
    assert_eq!(v, 1);
    assert!(n >= 2, "expected multi-leaf contiguous span, got {}", n);

    // maxblocks == 1 early-return path
    let (_v, n) = m.lookup_contig(&0, 1).await.unwrap();
    assert_eq!(n, 1);

    // maxblocks hit in the middle
    let (_v, n) = m.lookup_contig(&0, 5).await.unwrap();
    assert_eq!(n, 5);

    // gap forces early break
    let mut m2 = null_bmap();
    for k in 0..200u64 {
        if k == 50 { continue; }
        let _ = m2.insert(k, k + 1).await.unwrap();
    }
    let (_v, n) = m2.lookup_contig(&0, 200).await.unwrap();
    assert_eq!(n, 50);
}

// --- BtreeNode flag helpers ---

#[test]
fn btree_node_flag_helpers() {
    let mut buf = btree_ondisk::node::AlignedBuffer::new(256).unwrap();
    let n = BtreeNode::<u64, u64, u64>::from_slice(buf.as_mut_slice()).unwrap();
    n.set_flags(0b11);
    assert_eq!(n.get_flags(), 0b11);
    n.clear_large();
    assert_eq!(n.get_flags() & 0b01, 0);
}

// --- NonLeafNodeIter over direct (arm returns None early) ---

#[test]
fn nonleafnode_iter_direct_arm() {
    let m = null_bmap();
    let count = m.nonleafnode_iter().count();
    assert_eq!(count, 0);
}

// NonLeafNodeIter requires every node id to be a validly-assigned external
// id (is_valid_extern_assign() == true). The library-level precondition is
// that users have flushed the map once (lookup_dirty + assign_meta_node)
// before calling the iterator.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nonleafnode_iter_btree_arm() {
    let mut m = null_bmap();
    for k in 0..64u64 {
        let _ = m.insert(k, k + 1).await.unwrap();
    }
    assert!(m.get_stat().btree);

    // simulate a flush: assign every dirty node an external seq.
    let dirty = m.lookup_dirty();
    let mut seq = VALID_EXTERNAL_ASSIGN_MASK + 1u64;
    for n in &dirty {
        m.assign_meta_node(seq, n.clone()).await.unwrap();
        seq += 1;
    }
    for n in dirty { n.clear_dirty(); }
    m.clear_dirty();

    let count = tokio::task::block_in_place(|| m.nonleafnode_iter().count());
    assert!(count >= 1);
}
