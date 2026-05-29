//! Minimal reproduction for btree-ondisk: a value type V that is much larger
//! than the pointer type P and does not fit in the fixed-size root node.
//!
//! Real-world trigger: a directory index that maps name-hash (u64) ->
//! a ~400-byte directory-entry struct, stored as a Hyperfile whose inode
//! holds a fixed 56-byte inline root. So:
//!   K = u64 (8 bytes)
//!   V = Big  (>> root capacity, and != size_of::<P>())
//!   P = u64 (8 bytes)
//!

#![cfg(not(feature = "sync-api"))]

use std::fmt;
use btree_ondisk::bmap::BMap;
use btree_ondisk::NodeValue;
use btree_ondisk::{MemoryBlockLoader, NullNodeCache};

const ROOT_NODE_SIZE: usize = 56;     // fixed inline root, same as Hyperfile
const META_BLOCK_SIZE: usize = 4096;  // meta-block leaves live here
const VALUE_LEN: usize = 432;         // >> root capacity, and != size_of::<P>() (8)

/// A large value: 432 bytes, far bigger than the 56-byte root and != 8.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Big {
    data: [u8; VALUE_LEN],
}

impl Big {
    fn new(seed: u8) -> Self {
        Self { data: [seed; VALUE_LEN] }
    }
}

impl Default for Big {
    fn default() -> Self {
        Self { data: [0u8; VALUE_LEN] }
    }
}

impl fmt::Display for Big {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Big[{}..]", self.data[0])
    }
}

impl NodeValue for Big {
    fn is_invalid(&self) -> bool {
        *self == Self::default()
    }
    fn invalid_value() -> Self {
        Self::default()
    }
    fn is_valid_extern_assign(&self) -> bool {
        false
    }
}

type Map<'a> = BMap<'a, u64, Big, u64, MemoryBlockLoader<u64>, NullNodeCache>;

fn make_map<'a>() -> Map<'a> {
    let loader = MemoryBlockLoader::<u64>::new(META_BLOCK_SIZE);
    BMap::<u64, Big, u64, MemoryBlockLoader<u64>, NullNodeCache>::new(
        ROOT_NODE_SIZE, META_BLOCK_SIZE, loader, NullNodeCache,
    )
    .expect("BMap::new")
}

#[tokio::test]
async fn bigvalue_insert_one() {
    // Fails at step 1 (convert_and_insert) on pristine btree-ondisk.
    let mut m = make_map();
    let _ = m.insert(1u64, Big::new(1)).await.expect("insert one");
    let got = m.lookup(&1u64).await.expect("lookup one");
    assert_eq!(got, Big::new(1));
}

#[tokio::test]
async fn bigvalue_last_key() {
    // Fails at step 2 (do_lookup_last) once step 1 is fixed.
    let mut m = make_map();
    for k in 0u64..8 {
        let _ = m.insert(k, Big::new(k as u8)).await.expect("insert");
    }
    let last = m.last_key().await.expect("last_key");
    assert_eq!(last, 7);
}

#[tokio::test]
async fn bigvalue_insert_many_then_delete_all() {
    // Fails at step 3 then step 4 (delete / merge / shrink, stack overflow)
    // once steps 1-2 are fixed.
    let mut m = make_map();
    const N: u64 = 40;
    for k in 0..N {
        let _ = m.insert(k, Big::new(k as u8)).await.expect("insert");
    }
    // reads across the internal-root + meta-block-leaf structure
    for k in 0..N {
        let got = m.lookup(&k).await.expect("lookup");
        assert_eq!(got, Big::new(k as u8));
    }
    let _ = m.last_key().await.expect("last_key");
    // deletes exercise borrow / concat / shrink propagation
    for k in 0..N {
        m.delete(&k).await.expect("delete");
    }
    assert!(m.lookup(&0u64).await.is_err(), "all deleted");
}
