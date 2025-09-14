use std::fmt;
use std::io::{ErrorKind, Result};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use rand::Rng;
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use btree_ondisk::bmap::BMap;
use btree_ondisk::NodeValue;
use btree_ondisk::MemoryBlockLoader;
use btree_ondisk::VALID_EXTERNAL_ASSIGN_MASK;

const CACHE_LIMIT: usize = 10;
const ROOT_NODE_SIZE: usize = 56;
const META_BLOCK_SIZE: usize = 4096;
const DATA_BLOCK_SIZE: usize = 4096;
// 5TiB address space
const DEFAULT_MAX_FILE_BLOCK_INDEX: u64 = 5 * 1024 * 1024 * 1024 * 1024;

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, PartialOrd)]
struct CustomValue<const N: usize> {
    // [DEFAULT_VALUE_SIZE -1] for external assigned value flag
    // [0..8] for encode/decode u64
    data: [u8; N],
}

impl<const N: usize> CustomValue<N> {
    fn random_value() -> Self {
        let mut v: [u8; N] = [0u8; N];
        rand::thread_rng().fill(&mut v[..]);
        Self { data: v }
    }
}

impl<const N: usize> Default for CustomValue<N> {
    fn default() -> Self {
        Self { data: [0u8; N] }
    }
}

impl<const N: usize> fmt::Display for CustomValue<N> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CustomValue [{:x?}]", &self.data)
    }
}

impl<const N: usize> NodeValue for CustomValue<N> {
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
        false
    }
}

struct MemoryFile<'a, const N: usize> {
    bmap: BMap<'a, u64, CustomValue<N>, u64, MemoryBlockLoader<u64>>,
    loader: MemoryBlockLoader<u64>,
    #[allow(dead_code)]
    data_block_size: usize,
    data_blocks_dirty: BTreeSet<u64>,
    kv_tracker: BTreeMap<u64, CustomValue<N>>,
    seq: u64,
    max_file_blk_idx: u64,
}

// impl a simple file in memory that don't actually read and write real data
impl<'a, const N: usize> MemoryFile<'a, N> {
    fn new(root_node_size: usize, meta_block_size: usize, data_block_size: usize) -> Self {
        let loader = MemoryBlockLoader::<u64>::new(data_block_size);
        let bmap = BMap::<u64, CustomValue<N>, u64, MemoryBlockLoader<u64>>::new(root_node_size, meta_block_size, loader.clone());
        // limit max cached meta data nodes
        bmap.set_cache_limit(CACHE_LIMIT);
        let mut start_seq = VALID_EXTERNAL_ASSIGN_MASK;
        start_seq += 1;
        Self {
            bmap,
            loader,
            data_block_size,
            data_blocks_dirty: BTreeSet::new(),
            kv_tracker: BTreeMap::new(),
            seq: start_seq,
            max_file_blk_idx: DEFAULT_MAX_FILE_BLOCK_INDEX,
        }
    }

    #[maybe_async::maybe_async]
    async fn read(&self, blk_idx: u64) -> Result<()> {
        let res = self.bmap.lookup(&blk_idx).await;
        if let Some(blk_ptr) = self.kv_tracker.get(&blk_idx) {
            assert!(res.unwrap() == *blk_ptr);
            return Ok(());
        }
        assert!(res.unwrap_err().kind() == ErrorKind::NotFound);
        Ok(())
    }

    #[maybe_async::maybe_async]
    async fn write(&mut self, blk_idx: u64) -> Result<()> {
        let Some(_) = self.kv_tracker.get(&blk_idx) else {
            let v = CustomValue::random_value();
            // if blk idx not exists
            let res = self.bmap.try_insert(blk_idx, v).await;
            assert!(res.is_ok());
            let res = self.kv_tracker.insert(blk_idx, v);
            assert!(res.is_none());
            // update dirty list
            let _ = self.data_blocks_dirty.insert(blk_idx);
            return Ok(());
        };

        // if blk idx already there
        // verify by read
        let _ = self.read(blk_idx).await;

        let v = CustomValue::random_value();
        // force update block index value if already exists
        let res = self.bmap.insert(blk_idx, v).await;
        assert!(res.unwrap().is_some());
        let res = self.kv_tracker.insert(blk_idx, v);
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
        if let Some(_) = self.kv_tracker.remove(&blk_idx) {
            assert!(res.is_ok());
            return Ok(());
        }
        assert!(res.unwrap_err().kind() == ErrorKind::NotFound);
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
            self.bmap.assign_meta_node(blk_ptr, n.clone()).await?;
            meta_nodes.push_back(blk_ptr);
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
async fn rand_rwd<const N: usize>(file: &mut MemoryFile<'_, N>, iter: usize) {
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
async fn do_op<const N: usize>(op: usize, file: &mut MemoryFile<'_, N>, blk_idx: u64) -> Result<()> {
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
async fn cleanup<const N: usize>(file: &mut MemoryFile<'_, N>) -> Result<()> {
    file.bmap.truncate(&0).await
}

#[maybe_async::maybe_async]
async fn run<const N: usize>(iter: usize, loop_count: usize) -> Result<()> {
    println!("@@@ run custom value example with iter: {}, loop count: {}, value size: {} @@@", iter, loop_count, N);

    let mut file: MemoryFile<'_, N> = MemoryFile::new(ROOT_NODE_SIZE, META_BLOCK_SIZE, DATA_BLOCK_SIZE);

    for l in 1..=loop_count {
        println!("=== loop #{} iter {} ===", l, iter);
        println!("=== random read/write/delete ===");
        rand_rwd(&mut file, iter).await;
        file.flush().await?;
        assert!(file.dirty_count() == 0);
        file.dump_stat();
        println!("");
    }

    println!("=== cleanup ====");
    let res = cleanup(&mut file).await;
    println!("truncate result {res:?}");
    let res = file.flush().await?;
    println!("flush result {res:?}");
    assert!(file.dirty_count() == 0);
    file.dump_stat();
    Ok(())
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cmd {
    /// rwd iterations
    #[arg(short, long, default_value_t = 1_000_000)]
    iter: usize,

    /// loop count
    #[arg(short, long, default_value_t = 4)]
    r#loop: usize,
}

fn main() {
    env_logger::init();
    let args = Cmd::parse();
    let i = args.iter;
    let l = args.r#loop;

    #[cfg(not(feature = "sync-api"))]
	let res = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            run::<32>(i, l).await?;
            println!("");
            run::<64>(i, l).await?;
            println!("");
            run::<128>(i, l).await
		});
    #[cfg(feature = "sync-api")]
	let res = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let join = tokio::task::spawn_blocking(move || {
                run::<32>(i, l)?;
                println!("");
                run::<64>(i, l)?;
                println!("");
                run::<128>(i, l)
            });
            join.await
		});

    println!("{res:?}");
}
