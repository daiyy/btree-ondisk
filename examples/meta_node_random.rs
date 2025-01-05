use std::io::{ErrorKind, Result};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use rand::Rng;
use indicatif::{ProgressBar, ProgressStyle};
use btree_ondisk::bmap::BMap;
use btree_ondisk::MemoryBlockLoader;

const VALID_EXTERNAL_ASSIGN_MASK: u64 = 0xFFFF_0000_0000_0000;
const CACHE_LIMIT: usize = 10;

struct MemoryFile<'a> {
    bmap: BMap<'a, u64, u64, MemoryBlockLoader<u64>>,
    loader: MemoryBlockLoader<u64>,
    #[allow(dead_code)]
    data_block_size: usize,
    max_file_blk_idx: u64,
    data_blocks_dirty: BTreeSet<u64>,
    data_blocks_tracker: BTreeMap<u64, u64>,
    seq: u64,
}

// impl a simple file in memory that don't actually read and write real data
impl<'a> MemoryFile<'a> {
    fn new(root_node_size: usize, meta_node_size: usize, data_block_size: usize, max_file_blk_idx: u64) -> Self {
        let loader = MemoryBlockLoader::new(data_block_size);
        let bmap = BMap::<u64, u64, MemoryBlockLoader<u64>>::new(root_node_size, meta_node_size, loader.clone());
        // limit max cached meta data nodes
        bmap.set_cache_limit(CACHE_LIMIT);
        Self {
            bmap,
            loader,
            data_block_size,
            max_file_blk_idx,
            data_blocks_dirty: BTreeSet::new(),
            data_blocks_tracker: BTreeMap::new(),
            seq: VALID_EXTERNAL_ASSIGN_MASK + 1,
        }
    }

    #[maybe_async::maybe_async]
    async fn read(&self, blk_idx: u64) -> Result<()> {
        let res = self.bmap.lookup(blk_idx).await;
        if let Some(blk_ptr) = self.data_blocks_tracker.get(&blk_idx) {
            assert!(res.unwrap() == *blk_ptr);
            return Ok(());
        }
        assert!(res.unwrap_err().kind() == ErrorKind::NotFound);
        Ok(())
    }

    #[maybe_async::maybe_async]
    async fn write(&mut self, blk_idx: u64) -> Result<()> {
        let Some(_) = self.data_blocks_tracker.get(&blk_idx) else {
            // if blk idx not exists
            let res = self.bmap.try_insert(blk_idx, 0).await;
            assert!(res.is_ok());
            let res = self.data_blocks_tracker.insert(blk_idx, 0);
            assert!(res.is_none());
            // update dirty list
            let _ = self.data_blocks_dirty.insert(blk_idx);
            return Ok(());
        };

        // if blk idx already there
        // verify by read
        let _ = self.read(blk_idx).await;

        let res = self.bmap.try_insert(blk_idx, 0).await;
        assert!(res.unwrap_err().kind() == ErrorKind::AlreadyExists);
        let res = self.data_blocks_tracker.insert(blk_idx, 0);
        assert!(res.is_some());
        // update dirty list
        let _ = self.data_blocks_dirty.insert(blk_idx);
        return Ok(());
    }

    #[maybe_async::maybe_async]
    async fn delete(&mut self, blk_idx: u64) -> Result<()> {
        let res = self.bmap.delete(blk_idx).await;
        if res.is_ok() {
            // could be on dirty list or not
            let _ = self.data_blocks_dirty.remove(&blk_idx);
        }
        if let Some(_) = self.data_blocks_tracker.remove(&blk_idx) {
            assert!(res.is_ok());
            return Ok(());
        }
        assert!(res.unwrap_err().kind() == ErrorKind::NotFound);
        Ok(())
    }

    fn update_tracker(&mut self, blk_idx: u64, blk_ptr: u64) {
        if let Some(v) = self.data_blocks_tracker.get_mut(&blk_idx) {
            *v = blk_ptr;
        } else {
            panic!("{blk_idx} not in data blocks tracker list");
        }
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
            self.bmap.assign(0, blk_ptr, Some(n.clone())).await?;
            meta_nodes.push_back(blk_ptr);
            self.seq += 1;
        }

        let mut v = Vec::new();
        for blk_idx in self.data_blocks_dirty.iter() {
            let blk_ptr = self.seq;
            self.bmap.assign(*blk_idx, blk_ptr.clone(), None).await?;
            v.push((*blk_idx, blk_ptr));
            self.seq += 1;
        }

        for (blk_idx, blk_ptr) in v.into_iter() {
            self.update_tracker(blk_idx, blk_ptr);
        }

		for n in &dirty_meta_vec {
            let seq = meta_nodes.pop_front().expect("failed to get seq id");
            self.loader.write(seq, n.as_ref().as_ref().as_ref());
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
async fn rand_rwd(file: &mut MemoryFile<'_>, iter: usize) {
    let mut rng = rand::thread_rng();
    let bar = ProgressBar::new(iter as u64);
    bar.set_style(ProgressStyle::with_template("[{eta}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
        .unwrap()
        .progress_chars("##-"));
    bar.enable_steady_tick(std::time::Duration::new(1, 0));
    let mut op_read = 0;
    let mut op_write = 0;
    let mut op_delete = 0;
    let total = iter;
    for i in 0..iter {
        let op = if rng.gen_ratio(2, 10) {
            // 20% for read
            op_read += 1;
            0
        } else if rng.gen_ratio(1, 10) {
            // 10% for delete
            op_delete += 1;
            2
        } else {
            op_write += 1;
            1
        };
        let blk_idx = rng.gen_range(0..file.max_file_blk_idx);
        let _ = do_op(op, file, blk_idx).await;
        if i % 10000 == 0 {
            bar.inc(10000);
        }
    }
    bar.finish();
    println!("total iterations: {}", total);
    println!("ratio: read - {:2.2}%, write - {:2.2}%, delete - {:2.2}%",
        (op_read as f64 * 100.0)/total as f64,
        (op_write as f64 * 100.0)/total as f64,
        (op_delete as f64 * 100.0)/total as f64);
}

// random read write and delete
#[maybe_async::maybe_async]
async fn do_op(op: usize, file: &mut MemoryFile<'_>, blk_idx: u64) -> Result<()> {
    match op {
        0 => {
            let _ = file.read(blk_idx).await;
        },
        1 => {
            let _ = file.write(blk_idx).await;
        },
        2 => {
            let _ = file.delete(blk_idx).await;
        },
        _ => todo!(),
    }
    Ok(())
}

#[maybe_async::maybe_async]
async fn run() -> Result<()> {
    let root_node_size = 56;
    let meta_node_size = 4096;
    let data_block_size = 4096;
    // for 4KiB data block, it's about 20 PiB file size
    let max_file_blk_idx = 5 * 1024 * 1024 * 1024 * 1024;
    let mut file = MemoryFile::new(root_node_size, meta_node_size, data_block_size, max_file_blk_idx);

    println!("=== random read/write/delete ===");
    let iter = 1_000_000;
    rand_rwd(&mut file, iter).await;
    file.flush().await?;
    assert!(file.dirty_count() == 0);
    file.dump_stat();
    println!("");

    println!("=== random read/write/delete ===");
    let iter = 10_000_000;
    rand_rwd(&mut file, iter).await;
    file.flush().await?;
    assert!(file.dirty_count() == 0);
    file.dump_stat();
    println!("");

    println!("=== random read/write/delete ===");
    let iter = 100_000_000;
    rand_rwd(&mut file, iter).await;
    file.flush().await?;
    assert!(file.dirty_count() == 0);
    file.dump_stat();
    println!("");

    println!("=== random read/write/delete ===");
    let iter = 10_000_000;
    rand_rwd(&mut file, iter).await;
    file.flush().await?;
    assert!(file.dirty_count() == 0);
    file.dump_stat();
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
