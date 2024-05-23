use std::time::Instant;
use tokio::io::Result;
use indicatif::ProgressBar;
use human_bytes::human_bytes;
use btree_ondisk::bmap::BMap;
use btree_ondisk::BlockLoader;

#[derive(Clone)]
struct NullBlockLoader;

impl BlockLoader<u64> for NullBlockLoader {
    async fn read(&self, v: &u64, buf: &mut [u8]) -> Result<()> {
        println!("null block loader read v: {}, len: {}", v, buf.len());
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    // NOTICE: root_node_size should be half of meta_node_size
    let root_node_size = 56;
    let meta_node_size = 4096;
    let data_block_size = 4096;
    let five_tb: u64 = 5 * 1024 * 1024 * 1024 * 1024;
    let num_blocks = five_tb / data_block_size;

    let null_loader = NullBlockLoader;
    let mut bmap = BMap::<u64, u64, NullBlockLoader>::new(root_node_size, meta_node_size, null_loader);

    println!("Creating metadata for 5TiB file, root node size: {}, meta node size: {}, data block size: {} ...",
        root_node_size, meta_node_size, data_block_size);
    let bar = ProgressBar::new(num_blocks);
    bar.enable_steady_tick(std::time::Duration::new(1, 0));
    let now = Instant::now();
    for i in 0..num_blocks as u64 {
        // NOTICE:
        //   0 is invalid value for type u64, play very carefully with invalid value
        let _ = bmap.insert(i, i).await;
        if i % 100000 == 0 {
            bar.inc(100000);
        }
    }
    bar.finish();
    let stat = bmap.get_stat();
    let total_bytes = stat.meta_block_size * (stat.nodes_total - 1);
    println!("  time cost to create 5TiB metadata [{:?}], memory usage: [{}]", now.elapsed(), human_bytes(total_bytes as f64));
    println!("  metadata stat {:?}", bmap.get_stat());
    drop(bar);
    drop(bmap);
    println!("");

    // creating another
    let root_node_size = 56;
    let meta_node_size = 4096 * 32; // 128k
    let data_block_size = 4096;
    let five_tb: u64 = 5 * 1024 * 1024 * 1024 * 1024;
    let num_blocks = five_tb / data_block_size;

    let null_loader = NullBlockLoader;
    let mut bmap = BMap::<u64, u64, NullBlockLoader>::new(root_node_size, meta_node_size, null_loader);

    println!("Creating metadata for 5TiB file, root node size: {}, meta node size: {}, data block size: {} ...",
        root_node_size, meta_node_size, data_block_size);
    let bar = ProgressBar::new(num_blocks);
    bar.enable_steady_tick(std::time::Duration::new(1, 0));
    let now = Instant::now();
    for i in 0..num_blocks as u64 {
        // NOTICE:
        //   0 is invalid value for type u64, play very carefully with invalid value
        let _ = bmap.insert(i, i).await;
        if i % 100000 == 0 {
            bar.inc(100000);
        }
    }
    bar.finish();
    let stat = bmap.get_stat();
    let total_bytes = stat.meta_block_size * (stat.nodes_total - 1);
    println!("  time cost to create 5TiB metadata [{:?}], memory usage: [{}]", now.elapsed(), human_bytes(total_bytes as f64));
    println!("  metadata stat {:?}", bmap.get_stat());
    drop(bar);
    println!("");

    // truncate file
    let trunc_to = 3;
    println!("Truncating bmap to {} ...", trunc_to);
    let now = Instant::now();
    let _ = bmap.truncate(trunc_to).await;
    let stat = bmap.get_stat();
    let total_bytes = stat.meta_block_size * (stat.nodes_total - 1);
    println!("  time cost [{:?}], memory usage: [{}]", now.elapsed(), human_bytes(total_bytes as f64));
    println!("  metadata stat {:?}", bmap.get_stat());
    println!("  last key now is {:?}", bmap.last_key().await.unwrap());
    drop(bmap);
    println!("");

    // enlarge data block size
    let root_node_size = 56;
    let meta_node_size = 4096 * 32; // 128k
    let data_block_size = 4096 * 32; // 128k
    let five_tb: u64 = 5 * 1024 * 1024 * 1024 * 1024;
    let num_blocks = five_tb / data_block_size;

    let null_loader = NullBlockLoader;
    let mut bmap = BMap::<u64, u64, NullBlockLoader>::new(root_node_size, meta_node_size, null_loader);

    println!("Creating metadata for 5TiB file, root node size: {}, meta node size: {}, data block size: {} ...",
        root_node_size, meta_node_size, data_block_size);
    let bar = ProgressBar::new(num_blocks);
    bar.enable_steady_tick(std::time::Duration::new(1, 0));
    let now = Instant::now();
    for i in 0..num_blocks as u64 {
        // NOTICE:
        //   0 is invalid value for type u64, play very carefully with invalid value
        let _ = bmap.insert(i, i).await;
        if i % 100000 == 0 {
            bar.inc(100000);
        }
    }
    bar.finish();
    let stat = bmap.get_stat();
    let total_bytes = stat.meta_block_size * (stat.nodes_total - 1);
    println!("  time cost to create 5TiB metadata [{:?}], memory usage: [{}]", now.elapsed(), human_bytes(total_bytes as f64));
    println!("  metadata stat {:?}", bmap.get_stat());
    drop(bar);
    println!("");

    // truncate file
    let trunc_to = 3;
    println!("Truncating bmap to {} ...", trunc_to);
    let now = Instant::now();
    let _ = bmap.truncate(trunc_to).await;
    let stat = bmap.get_stat();
    let total_bytes = stat.meta_block_size * (stat.nodes_total - 1);
    println!("  time cost [{:?}], memory usage: [{}]", now.elapsed(), human_bytes(total_bytes as f64));
    println!("  metadata stat {:?}", bmap.get_stat());
    println!("  last key now is {:?}", bmap.last_key().await.unwrap());
}
