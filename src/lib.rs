use std::fmt;
use tokio::io::Result;

pub mod ondisk;
pub mod node;
pub mod btree;
mod direct;
pub mod bmap;
mod utils;

pub trait VMap<K, V>
    where
        K: Copy + Default + fmt::Display + PartialOrd + Eq + std::hash::Hash,
        V: Copy + Default + fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        K: From<V>,
        V: From<K>
{
    fn new(data: Vec<u8>, meta_block_size: usize) -> Self;
    async fn lookup(&self, key: K, level: usize) -> Result<V>;
    async fn lookup_contig(&self, key: K, maxblocks: usize) -> Result<(V, usize)>;
    async fn insert(&self, key: K, val: V) -> Result<()>;
    async fn delete(&self, key: K) -> Result<()>;
    async fn seek_key(&self, start: K) -> Result<K>;
    async fn last_key(&self) -> Result<K>;
}

trait NodeValue<V> {
    fn is_invalid(&self) -> bool;
    fn invalid_value() -> V;
}

impl<V> NodeValue<V> for u64
    where V: From<u64>
{
    fn is_invalid(&self) -> bool {
        self == &u64::MIN 
    }

    fn invalid_value() -> V {
       u64::MIN.into()
    }
}
