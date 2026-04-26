#![no_main]
//! Fuzz `BtreeNode::from_slice` / `from_slice_ref` / `copy_from_slice`:
//! these are the public entry points for the `unsafe fn from_raw_ptr`.
//! The fuzzer should only ever observe Ok(_) or a well-formed `Err(io::Error)`
//! — never a panic, out-of-bounds read, or UB (detected under sanitizers).

use libfuzzer_sys::fuzz_target;
use btree_ondisk::node::BtreeNode;

fuzz_target!(|data: &[u8]| {
    // from_slice_ref (read-only view)
    let _ = BtreeNode::<u64, u64, u64>::from_slice_ref(data);

    // from_slice (mutable view): requires a mutable buffer
    let mut buf = data.to_vec();
    if let Ok(node) = BtreeNode::<u64, u64, u64>::from_slice(&mut buf) {
        // exercise a few read-only methods that walk the parsed layout
        let _ = node.get_nchild();
        let _ = node.get_capacity();
        let _ = node.get_level();
        let _ = node.get_flags();
        let _ = node.is_leaf();
        let _ = node.is_large();
    }

    // copy_from_slice allocates then copies — it must not crash on any length
    let _ = BtreeNode::<u64, u64, u64>::copy_from_slice(0u64, data);
});
