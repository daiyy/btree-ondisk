use std::time::Instant;
use btree_ondisk::node;

fn main() {
    let size = 1024 * 1024 * 1024; // 1 GB node

    let now = Instant::now();
    let n = node::BtreeNode::<u64, u64, u64>::new(size).unwrap();
    for i in 0..n.get_capacity() {
        let k = i as u64;
        n.insert(i, &k, &k);
    }
    println!("btree node insert performance {:?}, avg: {:?}", now.elapsed(), now.elapsed() / n.get_capacity() as u32);
    let now = Instant::now();
    for i in (0..n.get_capacity()).rev() {
        let k = i as u64;
        let _ = n.lookup(&k);
    }
    println!("btree node lookup performance {:?}, avg: {:?}", now.elapsed(), now.elapsed() / n.get_capacity() as u32);
}
