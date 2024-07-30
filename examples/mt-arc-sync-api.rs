mod mt;
use std::time::{Instant, Duration};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

const DEFAULT_FILE_SIZE: usize = 100 * 1024 * 1024 * 1024;
const MAX_CONCURRENCY: usize = 4;
const MAX_ITER: usize = 1000000;

#[tokio::main]
async fn main() {
    println!("##### start single task #####");
    single().await;
    println!("");
    println!("##### start concurrncy with {} tasks #####", MAX_CONCURRENCY);
    multi().await;
    println!("");
    println!("##### start concurrncy use atomic inc counter with {} tasks #####", MAX_CONCURRENCY);
    multi_atomic().await;
}

async fn single() {
    let file_size = DEFAULT_FILE_SIZE;
    let f = Arc::new(Mutex::new(mt::File::new(file_size)));

    let clone = f.clone();
    let h = tokio::spawn(async move {
        let mut file = clone.lock().await;
        file.build();
    });
    let _ = h.await;

    let mut file = f.lock().await;
    let last_key = file.bmap.last_key().expect("failed to get last key");
    let mut loop_count = 1;

    println!("now last key is {last_key}, run loop count: {loop_count}");

    let iter = MAX_ITER as u32;
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

async fn multi() {
    let file_size = DEFAULT_FILE_SIZE;
    let f = Arc::new(Mutex::new(mt::File::new(file_size)));

    let clone = f.clone();
    let h = tokio::spawn(async move {
        let mut file = clone.lock().await;
        file.build();
    });
    let _ = h.await;

    let file = f.lock().await;
    let last_key = file.bmap.last_key().expect("failed to get last key");
    println!("now last key is {last_key}");
    drop(file);

    let iter = MAX_ITER / MAX_CONCURRENCY;
    let blk_idx_start = last_key + 1;

    let mut set = JoinSet::new();
    let start = Instant::now();
    for _ in 0..MAX_CONCURRENCY {
        let clone = f.clone();
        set.spawn(async move {
            let start = Instant::now();
            for i in 0..iter as u64 {
                let file = clone.lock().await;
                let _ = file.bmap.lookup(i);
            }
            let avg = start.elapsed() / iter as u32;
            avg
        });
    }
    let mut total_avg: Duration = Duration::new(0, 0);
    while let Some(res) = set.join_next().await {
        let avg = res.unwrap();
        total_avg += avg;
    }
    println!("{} iters of {:>10} total time {:>12?}, avg latency {:>10?}", MAX_ITER, "LOOKUP", start.elapsed(), total_avg / MAX_CONCURRENCY as u32);

    let mut set = JoinSet::new();
    let start = Instant::now();
    for x in 0..MAX_CONCURRENCY {
        let clone = f.clone();
        set.spawn(async move {
            let start = Instant::now();
            let begin = blk_idx_start + (x * iter) as u64;
            let end = blk_idx_start + ((x + 1) * iter) as u64;
            for i in begin..end as u64 {
                let mut file = clone.lock().await;
                let _ = file.bmap.insert(i, i);
            }
            let avg = start.elapsed() / iter as u32;
            avg
        });
    }
    let mut total_avg: Duration = Duration::new(0, 0);
    while let Some(res) = set.join_next().await {
        let avg = res.unwrap();
        total_avg += avg;
    }
    println!("{} iters of {:>10} total time {:>12?}, avg latency {:>10?}", MAX_ITER, "INSERT", start.elapsed(), total_avg / MAX_CONCURRENCY as u32);

    let mut set = JoinSet::new();
    let start = Instant::now();
    for x in 0..MAX_CONCURRENCY {
        let clone = f.clone();
        set.spawn(async move {
            let start = Instant::now();
            let begin = blk_idx_start + (x * iter) as u64;
            let end = blk_idx_start + ((x + 1) * iter) as u64;
            for i in begin..end as u64 {
                let file = clone.lock().await;
                let _ = file.bmap.assign(i, i, None);
            }
            let avg = start.elapsed() / iter as u32;
            avg
        });
    }
    let mut total_avg: Duration = Duration::new(0, 0);
    while let Some(res) = set.join_next().await {
        let avg = res.unwrap();
        total_avg += avg;
    }
    println!("{} iters of {:>10} total time {:>12?}, avg latency {:>10?}", MAX_ITER, "ASSIGN", start.elapsed(), total_avg / MAX_CONCURRENCY as u32);

    let mut set = JoinSet::new();
    let start = Instant::now();
    for x in 0..MAX_CONCURRENCY {
        let clone = f.clone();
        set.spawn(async move {
            let start = Instant::now();
            let begin = blk_idx_start + (x * iter) as u64;
            let end = blk_idx_start + ((x + 1) * iter) as u64;
            for i in begin..end as u64 {
                let file = clone.lock().await;
                let _ = file.bmap.propagate(i, None);
            }
            let avg = start.elapsed() / iter as u32;
            avg
        });
    }
    let mut total_avg: Duration = Duration::new(0, 0);
    while let Some(res) = set.join_next().await {
        let avg = res.unwrap();
        total_avg += avg;
    }
    println!("{} iters of {:>10} total time {:>12?}, avg latency {:>10?}", MAX_ITER, "PROPAGATE", start.elapsed(), total_avg / MAX_CONCURRENCY as u32);
}

use std::sync::atomic::{AtomicU64, Ordering};
// use atomic u64 as global key/val counter
async fn multi_atomic() {
    let file_size = DEFAULT_FILE_SIZE;
    let f = Arc::new(Mutex::new(mt::File::new(file_size)));

    let clone = f.clone();
    let h = tokio::spawn(async move {
        let mut file = clone.lock().await;
        file.build();
    });
    let _ = h.await;

    let file = f.lock().await;
    let last_key = file.bmap.last_key().expect("failed to get last key");
    println!("now last key is {last_key}");
    drop(file);

    let iter = MAX_ITER / MAX_CONCURRENCY;
    let blk_idx_start = last_key + 1;

    let begin_atomic = Arc::new(AtomicU64::new(blk_idx_start));
    let end = blk_idx_start + iter as u64;

    let mut set = JoinSet::new();
    let start = Instant::now();
    for _ in 0..MAX_CONCURRENCY {
        let clone = f.clone();
        let begin = begin_atomic.clone();
        set.spawn(async move {
            let start = Instant::now();
            loop {
                let i = begin.fetch_add(1, Ordering::SeqCst);
                if i >= end { break; }
                let file = clone.lock().await;
                let _ = file.bmap.lookup(i);
            }
            let avg = start.elapsed() / iter as u32;
            avg
        });
    }
    let mut total_avg: Duration = Duration::new(0, 0);
    while let Some(res) = set.join_next().await {
        let avg = res.unwrap();
        total_avg += avg;
    }
    println!("{} iters of {:>10} total time {:>12?}, avg latency {:>10?}", MAX_ITER, "LOOKUP", start.elapsed(), total_avg / MAX_CONCURRENCY as u32);

    let begin_atomic = Arc::new(AtomicU64::new(blk_idx_start));
    let end = blk_idx_start + iter as u64;

    let mut set = JoinSet::new();
    let start = Instant::now();
    for _x in 0..MAX_CONCURRENCY {
        let clone = f.clone();
        let begin = begin_atomic.clone();
        set.spawn(async move {
            let start = Instant::now();
            loop {
                let i = begin.fetch_add(1, Ordering::SeqCst);
                if i >= end { break; }
                let mut file = clone.lock().await;
                let _ = file.bmap.insert(i, i);
            }
            let avg = start.elapsed() / iter as u32;
            avg
        });
    }
    let mut total_avg: Duration = Duration::new(0, 0);
    while let Some(res) = set.join_next().await {
        let avg = res.unwrap();
        total_avg += avg;
    }
    println!("{} iters of {:>10} total time {:>12?}, avg latency {:>10?}", MAX_ITER, "INSERT", start.elapsed(), total_avg / MAX_CONCURRENCY as u32);

    let begin_atomic = Arc::new(AtomicU64::new(blk_idx_start));
    let end = blk_idx_start + iter as u64;

    let mut set = JoinSet::new();
    let start = Instant::now();
    for _x in 0..MAX_CONCURRENCY {
        let clone = f.clone();
        let begin = begin_atomic.clone();
        set.spawn(async move {
            let start = Instant::now();
            loop {
                let i = begin.fetch_add(1, Ordering::SeqCst);
                if i >= end { break; }
                let file = clone.lock().await;
                let _ = file.bmap.assign(i, i, None);
            }
            let avg = start.elapsed() / iter as u32;
            avg
        });
    }
    let mut total_avg: Duration = Duration::new(0, 0);
    while let Some(res) = set.join_next().await {
        let avg = res.unwrap();
        total_avg += avg;
    }
    println!("{} iters of {:>10} total time {:>12?}, avg latency {:>10?}", MAX_ITER, "ASSIGN", start.elapsed(), total_avg / MAX_CONCURRENCY as u32);

    let begin_atomic = Arc::new(AtomicU64::new(blk_idx_start));
    let end = blk_idx_start + iter as u64;

    let mut set = JoinSet::new();
    let start = Instant::now();
    for _x in 0..MAX_CONCURRENCY {
        let clone = f.clone();
        let begin = begin_atomic.clone();
        set.spawn(async move {
            let start = Instant::now();
            loop {
                let i = begin.fetch_add(1, Ordering::SeqCst);
                if i >= end { break; }
                let file = clone.lock().await;
                let _ = file.bmap.propagate(i, None);
            }
            let avg = start.elapsed() / iter as u32;
            avg
        });
    }
    let mut total_avg: Duration = Duration::new(0, 0);
    while let Some(res) = set.join_next().await {
        let avg = res.unwrap();
        total_avg += avg;
    }
    println!("{} iters of {:>10} total time {:>12?}, avg latency {:>10?}", MAX_ITER, "PROPAGATE", start.elapsed(), total_avg / MAX_CONCURRENCY as u32);
}
