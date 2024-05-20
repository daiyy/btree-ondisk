use std::time::Instant;
use btree_ondisk::node_v1;
use btree_ondisk::node;

fn main() {
    let size = 1024 * 1024 * 1024; // 1 GB node

    // use node_v1
    let mut v1 = node_v1::BtreeNode::<u64, u64>::new(0, size).unwrap();
    let now = Instant::now();
    for i in 0..v1.get_capacity() {
        let k = i as u64;
        v1.insert(i, &k, &k);
    }
    println!("btree node v1 insert performance {:?}", now.elapsed());
    let now = Instant::now();
    for i in (0..v1.get_capacity()).rev() {
        let k = i as u64;
        let _ = v1.lookup(&k);
    }
    println!("btree node v1 lookup performance {:?}", now.elapsed());

    // use node
    let now = Instant::now();
    let n = node::BtreeNode::<u64, u64>::new(size).unwrap();
    for i in 0..n.get_capacity() {
        let k = i as u64;
        n.insert(i, &k, &k);
    }
    println!("btree node insert performance {:?}", now.elapsed());
    let now = Instant::now();
    for i in (0..n.get_capacity()).rev() {
        let k = i as u64;
        let _ = n.lookup(&k);
    }
    println!("btree node lookup performance {:?}", now.elapsed());
}
