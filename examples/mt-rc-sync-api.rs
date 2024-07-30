mod mt;
use std::time::Instant;

const DEFAULT_FILE_SIZE: usize = 100 * 1024 * 1024 * 1024;

#[tokio::main]
async fn main() {
    let file_size = DEFAULT_FILE_SIZE;
    let mut file = mt::File::new(file_size);

    file.build();

    let last_key = file.bmap.last_key().expect("failed to get last key");
    let mut loop_count = 1;

    println!("now last key is {last_key}, run loop count: {loop_count}");

    let iter = 1000000;
    let blk_idx_start = last_key + 1;
    loop {
        let start = Instant::now();
        for i in 0..iter as u64 {
            let _ = file.bmap.lookup(i);
        }
        let avg = start.elapsed() / iter;
        println!("{} iters of {:>10} total time {:>12?}, avg latency {:>10?}", iter, "LOOKUP", start.elapsed(), avg);

        let start = Instant::now();
        for i in blk_idx_start..blk_idx_start + iter as u64 {
            let _ = file.bmap.insert(i, i);
        }
        let avg = start.elapsed() / iter;
        println!("{} iters of {:>10} total time {:>12?}, avg latency {:>10?}", iter, "INSERT", start.elapsed(), avg);

        let start = Instant::now();
        for i in blk_idx_start..blk_idx_start + iter as u64 {
            let _ = file.bmap.assign(i, i, None);
        }
        let avg = start.elapsed() / iter;
        println!("{} iters of {:>10} total time {:>12?}, avg latency {:>10?}", iter, "ASSIGN", start.elapsed(), avg);

        let start = Instant::now();
        for i in blk_idx_start..blk_idx_start + iter as u64 {
            let _ = file.bmap.propagate(i, None);
        }
        let avg = start.elapsed() / iter;
        println!("{} iters of {:>10} total time {:>12?}, avg latency {:>10?}", iter, "PROPAGATE", start.elapsed(), avg);

        loop_count -= 1;
        if loop_count == 0 {
            break;
        }
    }
}
