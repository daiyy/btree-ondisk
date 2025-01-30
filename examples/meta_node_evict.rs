use std::io::Result;
use std::collections::{BTreeSet, VecDeque, HashSet};
use rand::Rng;
use btree_ondisk::bmap::BMap;
use btree_ondisk::MemoryBlockLoader;

const VALID_EXTERNAL_ASSIGN_MASK: u64 = 0xFFFF_0000_0000_0000;
const CACHE_LIMIT: usize = 10;

struct MemoryFile<'a> {
    bmap: BMap<'a, u64, u64, MemoryBlockLoader<u64>>,
    loader: MemoryBlockLoader<u64>,
    data_block_size: usize,
    data_blocks_dirty: BTreeSet<u64>,
    seq: u64,
}

// impl a simple file in memory that don't actually read and write real data
impl<'a> MemoryFile<'a> {
    fn new(root_node_size: usize, meta_node_size: usize, data_block_size: usize) -> Self {
        let loader = MemoryBlockLoader::new(data_block_size);
        let bmap = BMap::<u64, u64, MemoryBlockLoader<u64>>::new(root_node_size, meta_node_size, loader.clone());
        // limit max cached meta data nodes
        bmap.set_cache_limit(CACHE_LIMIT);
        Self {
            bmap,
            loader,
            data_block_size,
            data_blocks_dirty: BTreeSet::new(),
            seq: VALID_EXTERNAL_ASSIGN_MASK + 1,
        }
    }

    #[maybe_async::maybe_async]
    async fn read(&self, off: usize) -> Result<()> {
        let blk_idx = (off / self.data_block_size) as u64;
        let _ = self.bmap.lookup(&blk_idx).await?;
        Ok(())
    }

    #[maybe_async::maybe_async]
    async fn write(&mut self, off: usize) -> Result<()> {
        let blk_idx = (off / self.data_block_size) as u64;
        let _  = self.bmap.insert(blk_idx, 0).await;
        self.data_blocks_dirty.insert(blk_idx);
        Ok(())
    }

    fn dirty_count(&self) -> usize {
        let dirty_meta_vec = self.bmap.lookup_dirty();
        return dirty_meta_vec.len();
    }

    #[maybe_async::maybe_async]
    async fn flush(&mut self) -> Result<()> {
        if self.data_blocks_dirty.len() == 0 && !self.bmap.dirty() {
            return Ok(());
        }

        let dirty_meta_vec = self.bmap.lookup_dirty();

        let mut meta_nodes: VecDeque<u64> = VecDeque::new();

		for n in &dirty_meta_vec {
            let blk_ptr = self.seq;
            self.bmap.assign(&0, blk_ptr, Some(n.clone())).await?;
            meta_nodes.push_back(blk_ptr);
            self.seq += 1;
        }

        for blk_idx in self.data_blocks_dirty.iter() {
            let blk_ptr = self.seq;
            self.bmap.assign(&blk_idx, blk_ptr, None).await?;
            self.seq += 1;
        }

		for n in &dirty_meta_vec {
            let seq = meta_nodes.pop_front().expect("failed to get seq id");
            self.loader.write(seq, n.as_slice());
        }

        for n in dirty_meta_vec {
            n.clear_dirty();
        }

        self.bmap.clear_dirty();
        self.data_blocks_dirty.clear();

        Ok(())
    }

    fn dump_stat(&self) {
        let stat = self.bmap.get_stat();
        println!("metadata stat {:?}", stat);
    }
}

#[maybe_async::maybe_async]
async fn run() -> Result<()> {
    let root_node_size = 56;
    let meta_node_size = 4096;
    let data_block_size = 4096;
    let mut file = MemoryFile::new(root_node_size, meta_node_size, data_block_size);

    let mut rng = rand::thread_rng();
    // remember all block offset random generated for check later
    let mut blkoffs = HashSet::new();

    // 5 TiB file
    let max_file_size = 5 * 1024 * 1024 * 1024 * 1024;
    let write_iter = 1000;
    let read_iter = 10;
    for _ in 0..write_iter {
        let blk_off = rng.gen_range(0..max_file_size);
        let _ = blkoffs.insert(blk_off);
        let _ = file.write(blk_off).await?;
    }
    file.flush().await?;
    assert!(file.dirty_count() == 0);
    file.dump_stat();

    for _ in 0..read_iter {
        let blk_off = rng.gen_range(0..max_file_size);
        // ignore any error for read
        let _ = file.read(blk_off).await;
    }
    assert!(file.dirty_count() == 0);
    file.dump_stat();

    let write_iter = 100_0000;
    for _ in 0..write_iter {
        let blk_off = rng.gen_range(0..max_file_size);
        let _ = blkoffs.insert(blk_off);
        let _ = file.write(blk_off).await?;
    }
    file.flush().await?;
    assert!(file.dirty_count() == 0);
    file.dump_stat();

    println!("start to check ...");
    println!("  block offset count {}", blkoffs.len());
    // check all block offset inserted is there
    for blk_off in blkoffs.iter() {
        let res = file.read(*blk_off).await;
        if res.is_err() {
            println!("block off {blk_off:x} res {res:?}");
            return Ok(());
        }
    }
    println!("all OK");
    Ok(())
}

fn main() {
    env_logger::init();

    #[cfg(not(feature = "sync-api"))]
	let res = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
        	run().await
		});
    #[cfg(feature = "sync-api")]
	let res = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let join = tokio::task::spawn_blocking(move || {
                run()
            });
            join.await
		});

    println!("{res:?}");
}
