use std::fmt;
use std::io::{ErrorKind, Result};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use rand::Rng;
use indicatif::{ProgressBar, ProgressStyle};
use btree_ondisk::bmap::BMap;
use btree_ondisk::NodeValue;
use btree_ondisk::MemoryBlockLoader;

const CACHE_LIMIT: usize = 10;
const DEFAULT_VALUE_SIZE: usize = 32;
const ROOT_NODE_SIZE: usize = 56;
const META_BLOCK_SIZE: usize = 4096;
const DATA_BLOCK_SIZE: usize = 4096;
// 5TiB address space
const DEFAULT_MAX_FILE_BLOCK_INDEX: u64 = 5 * 1024 * 1024 * 1024 * 1024;

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, PartialOrd)]
struct CustomValue {
    // [DEFAULT_VALUE_SIZE -1] for external assigned value flag
    // [0..8] for encode/decode u64
    data: [u8; DEFAULT_VALUE_SIZE],
}

impl CustomValue {
    fn random_value() -> Self {
        let mut v = [0u8; DEFAULT_VALUE_SIZE];
        rand::thread_rng().fill(&mut v[..DEFAULT_VALUE_SIZE-1]);
        Self { data: v }
    }

    fn external_seq_start() -> Self {
        let mut v = [0u8; DEFAULT_VALUE_SIZE];
        v[DEFAULT_VALUE_SIZE-1] = 0xff;
        Self { data: v }
    }
}

impl Default for CustomValue {
    fn default() -> Self {
        Self { data: [0u8; DEFAULT_VALUE_SIZE] }
    }
}

impl fmt::Display for CustomValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let v: u64 = (*self).into();
        write!(f, "CustomValue [value {} - flag: {:x}]", v, self.data[DEFAULT_VALUE_SIZE-1])
    }
}

use std::ops::AddAssign;
impl AddAssign<u64> for CustomValue {
    fn add_assign(&mut self, other: u64) {
        // save flag byte
        let flag = self.data[DEFAULT_VALUE_SIZE-1];

        let mut v: u64 = (*self).into();
        v += other;
        *self = v.into();

        // restore flag byte
        self.data[DEFAULT_VALUE_SIZE-1] = flag;
    }
}

impl From<u64> for CustomValue {
    fn from(s: u64) -> Self {
        let mut v = Self::default();
        let (a, _) = v.data.split_at_mut(8);
        a.copy_from_slice(&s.to_le_bytes());
        v
    }
}

impl From<CustomValue> for u64 {
    fn from(s: CustomValue) -> u64 {
        let (v, _) = s.data.split_at(8);
        let mut a = [0u8; 8];
        a.copy_from_slice(v);
        u64::from_ne_bytes(a)
    }
}

impl NodeValue<CustomValue> for CustomValue {
    fn is_invalid(&self) -> bool {
        if self == &Self::default() {
            return true;
        }
        false
    }

    fn invalid_value() -> Self {
        Self::default()
    }

    fn is_valid_extern_assign(&self) -> bool {
        if self.data[DEFAULT_VALUE_SIZE-1] == 0xff {
            return true;
        }
        false
    }
}

struct MemoryFile<'a> {
    bmap: BMap<'a, u64, CustomValue, MemoryBlockLoader<CustomValue>>,
    loader: MemoryBlockLoader<CustomValue>,
    #[allow(dead_code)]
    data_block_size: usize,
    data_blocks_dirty: BTreeSet<u64>,
    data_blocks_tracker: BTreeMap<u64, CustomValue>,
    seq: CustomValue,
    max_file_blk_idx: u64,
}

// impl a simple file in memory that don't actually read and write real data
impl<'a> MemoryFile<'a> {
    fn new(root_node_size: usize, meta_block_size: usize, data_block_size: usize) -> Self {
        let loader = MemoryBlockLoader::<CustomValue>::new(data_block_size);
        let bmap = BMap::<u64, CustomValue, MemoryBlockLoader<CustomValue>>::new(root_node_size, meta_block_size, loader.clone());
        // limit max cached meta data nodes
        bmap.set_cache_limit(CACHE_LIMIT);
        let mut start_seq = CustomValue::external_seq_start();
        start_seq += 1;
        Self {
            bmap,
            loader,
            data_block_size,
            data_blocks_dirty: BTreeSet::new(),
            data_blocks_tracker: BTreeMap::new(),
            seq: start_seq,
            max_file_blk_idx: DEFAULT_MAX_FILE_BLOCK_INDEX,
        }
    }

    #[maybe_async::maybe_async]
    async fn read(&self, blk_idx: u64) -> Result<()> {
        let res = self.bmap.lookup(&blk_idx).await;
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
            let v = CustomValue::random_value();
            // if blk idx not exists
            let res = self.bmap.try_insert(blk_idx, v).await;
            assert!(res.is_ok());
            let res = self.data_blocks_tracker.insert(blk_idx, v);
            assert!(res.is_none());
            // update dirty list
            let _ = self.data_blocks_dirty.insert(blk_idx);
            return Ok(());
        };

        // if blk idx already there
        // verify by read
        let _ = self.read(blk_idx).await;

        let v = CustomValue::random_value();
        let res = self.bmap.try_insert(blk_idx, v).await;
        assert!(res.unwrap_err().kind() == ErrorKind::AlreadyExists);
        let res = self.data_blocks_tracker.insert(blk_idx, v);
        assert!(res.is_some());
        // update dirty list
        let _ = self.data_blocks_dirty.insert(blk_idx);
        return Ok(());
    }

    #[maybe_async::maybe_async]
    async fn delete(&mut self, blk_idx: u64) -> Result<()> {
        let res = self.bmap.delete(&blk_idx).await;
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

    fn update_tracker(&mut self, blk_idx: u64, blk_ptr: CustomValue) {
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

        let mut meta_nodes: VecDeque<CustomValue> = VecDeque::new();

		for n in &dirty_meta_vec {
            let blk_ptr: CustomValue = self.seq;
            self.bmap.assign_meta_node(blk_ptr, n.clone()).await?;
            meta_nodes.push_back(blk_ptr);
            self.seq += 1;
        }

        let mut v = Vec::new();
        for blk_idx in self.data_blocks_dirty.iter() {
            let blk_ptr: CustomValue = self.seq;
            self.bmap.assign_data_node(blk_idx, blk_ptr.clone()).await?;
            v.push((*blk_idx, blk_ptr));
            self.seq += 1;
        }

        for (blk_idx, blk_ptr) in v.into_iter() {
            self.update_tracker(blk_idx, blk_ptr);
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
async fn cleanup(file: &mut MemoryFile<'_>) -> Result<()> {
    file.bmap.truncate(&0).await
}

#[maybe_async::maybe_async]
async fn run() -> Result<()> {
    let mut file = MemoryFile::new(ROOT_NODE_SIZE, META_BLOCK_SIZE, DATA_BLOCK_SIZE);

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
    println!("");

    println!("=== cleanup ====");
    let res = cleanup(&mut file).await;
    println!("truncate result {res:?}");
    let res = file.flush().await?;
    println!("flush result {res:?}");
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
