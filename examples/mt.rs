use std::time::Instant;
use indicatif::ProgressBar;
use human_bytes::human_bytes;
use btree_ondisk::bmap::BMap;
use btree_ondisk::NullBlockLoader;

pub(crate) struct File<'a> {
    pub(crate) bmap: BMap<'a, u64, u64, NullBlockLoader>,
    pub(crate) root_node_size: usize,
    pub(crate) meta_block_size: usize,
    pub(crate) data_block_size: usize,
    pub(crate) file_size: usize,
}

impl<'a> File<'a> {
    pub(crate) fn new(file_size: usize) -> Self {
        let root_size = 56;
        let meta_block_size = 4096;
        let data_block_size = 4096;

        let loader = NullBlockLoader;
        let bmap = BMap::<u64, u64, NullBlockLoader>::new(root_size, meta_block_size, loader);
        Self {
            bmap: bmap,
            root_node_size: root_size,
            meta_block_size: meta_block_size,
            data_block_size: data_block_size,
            file_size: file_size,
        }
    }

    #[maybe_async::maybe_async(AFIT)]
    pub(crate) async fn build(&mut self) {
        println!("Creating metadata for {} file, root node size: {}, meta node size: {}, data block size: {} ...",
            human_bytes(self.file_size as f64), self.root_node_size, self.meta_block_size, self.data_block_size);
        let num_blocks = self.file_size / self.data_block_size;
        let bar = ProgressBar::new(num_blocks as u64);
        bar.enable_steady_tick(std::time::Duration::new(1, 0));
        let now = Instant::now();
        for i in 0..num_blocks as u64 {
            let _ = self.bmap.insert(i, i).await;
            if i % 100000 == 0 {
                bar.inc(100000);
            }
        }
        bar.finish();
        let stat = self.bmap.get_stat();
        let total_bytes = stat.meta_block_size * (stat.nodes_total - 1);
        println!("  time cost to create {} metadata [{:?}], memory usage: [{}]", human_bytes(self.file_size as f64), now.elapsed(), human_bytes(total_bytes as f64));
        println!("  metadata stat {:?}", self.bmap.get_stat());
        drop(bar);
    }
}
