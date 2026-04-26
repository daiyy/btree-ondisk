#![no_main]
//! Fuzz `DirectNode::from_slice` / `from_slice_ref` / `copy_from_slice`.

use libfuzzer_sys::fuzz_target;
use btree_ondisk::node::DirectNode;

fuzz_target!(|data: &[u8]| {
    let _ = DirectNode::<u64>::from_slice_ref(data);

    let mut buf = data.to_vec();
    if let Ok(node) = DirectNode::<u64>::from_slice(&mut buf) {
        let _ = node.get_capacity();
        let _ = node.get_userdata();
        for idx in 0..node.get_capacity() {
            let _ = node.get_val(idx);
        }
    }

    let _ = DirectNode::<u64>::copy_from_slice(data);
});
