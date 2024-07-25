use std::time::Instant;
use std::ops::Bound::Unbounded;
use indicatif::ProgressBar;
use crossbeam_skiplist::SkipMap;

const DEFAULT_FILE_SIZE: usize = 100 * 1024 * 1024 * 1024;

fn main() {
    let file_size = DEFAULT_FILE_SIZE;
    let data_block_size = 4096;
    let map = SkipMap::new();

    let num_blocks = file_size / data_block_size;
    let bar = ProgressBar::new(num_blocks as u64);
    bar.enable_steady_tick(std::time::Duration::new(1, 0));
    for i in 0..num_blocks as u64 {
        let _ = map.insert(i, i);
        if i % 100000 == 0 {
            bar.inc(100000);
        }
    }
    bar.finish();

    let entry = map.upper_bound(Unbounded).expect("failed to get last key");
    let last_key = entry.key();
    let mut loop_count = 1;

    println!("==== CrossBeam SkipMap ====");
    println!("now last key is {last_key}, run loop count: {loop_count}");

    let iter = 1000000;
    let blk_idx_start = last_key + 1;
    loop {
        let start = Instant::now();
        for i in 0..iter as u64 {
            let _ = map.get(&i);
        }
        let avg = start.elapsed() / iter;
        println!("{} iters of {:>10} total time {:>12?}, avg latency {:>10?}", iter, "LOOKUP", start.elapsed(), avg);

        let start = Instant::now();
        for i in blk_idx_start..blk_idx_start + iter as u64 {
            let _ = map.insert(i, i);
        }
        let avg = start.elapsed() / iter;
        println!("{} iters of {:>10} total time {:>12?}, avg latency {:>10?}", iter, "INSERT", start.elapsed(), avg);

        loop_count -= 1;
        if loop_count == 0 {
            break;
        }
    }
}
