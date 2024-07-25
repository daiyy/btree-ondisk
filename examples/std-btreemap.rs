use std::time::Instant;
use indicatif::ProgressBar;
use std::collections::BTreeMap;

const DEFAULT_FILE_SIZE: usize = 100 * 1024 * 1024 * 1024;

fn main() {
    let file_size = DEFAULT_FILE_SIZE;
    let data_block_size = 4096;
    let mut btree = BTreeMap::new();

    let num_blocks = file_size / data_block_size;
    let bar = ProgressBar::new(num_blocks as u64);
    bar.enable_steady_tick(std::time::Duration::new(1, 0));
    for i in 0..num_blocks as u64 {
        let _ = btree.insert(i, i);
        if i % 100000 == 0 {
            bar.inc(100000);
        }
    }
    bar.finish();

    let (last_key, _) = btree.last_key_value().expect("failed to get last key");
    let mut loop_count = 1;

    println!("==== std BTreeMap ====");
    println!("now last key is {last_key}, run loop count: {loop_count}");

    let iter = 1000000;
    let blk_idx_start = last_key + 1;
    loop {
        let start = Instant::now();
        for i in 0..iter as u64 {
            let _ = btree.get(&i);
        }
        let avg = start.elapsed() / iter;
        println!("{} iters of {:>10} total time {:>12?}, avg latency {:>10?}", iter, "LOOKUP", start.elapsed(), avg);

        let start = Instant::now();
        for i in blk_idx_start..blk_idx_start + iter as u64 {
            let _ = btree.insert(i, i);
        }
        let avg = start.elapsed() / iter;
        println!("{} iters of {:>10} total time {:>12?}, avg latency {:>10?}", iter, "INSERT", start.elapsed(), avg);

        loop_count -= 1;
        if loop_count == 0 {
            break;
        }
    }
}
