//! Key/Value map implementation.
//!
//! Starting from a flat map of signle direct node which for small k/v set.
//!
//! Converted to btree based map when key exceed direct node's capacity.
//!
//! Individual btree node in the map can be load back from backend storage by [`BlockLoader`].
use std::fmt;
use std::io::Result;

pub mod ondisk;
pub mod node;
pub mod btree;
#[allow(dead_code)]
#[cfg(not(feature = "sync-api"))]
pub mod node_v1;
#[allow(dead_code)]
#[cfg(not(feature = "sync-api"))]
pub mod btree_v1;
#[allow(dead_code)]
#[cfg(not(feature = "sync-api"))]
pub mod direct_v1;
mod direct;
pub mod bmap;
mod utils;
mod loader;
pub use crate::loader::null::NullBlockLoader;
pub use crate::loader::memory::MemoryBlockLoader;

#[maybe_async::maybe_async(AFIT)]
#[allow(async_fn_in_trait)]
pub trait VMap<K, V>
    where
        K: Copy + Default + fmt::Display + PartialOrd + Eq + std::hash::Hash,
        V: Copy + Default + fmt::Display,
{
    async fn lookup(&self, key: &K, level: usize) -> Result<V>;
    async fn lookup_contig(&self, key: &K, maxblocks: usize) -> Result<(V, usize)>;
    async fn insert(&self, key: K, val: V) -> Result<()>;
    async fn insert_or_update(&self, key: K, val: V) -> Result<Option<V>>;
    async fn delete(&self, key: &K) -> Result<()>;
    async fn seek_key(&self, start: &K) -> Result<K>;
    async fn last_key(&self) -> Result<K>;
}

pub const VALID_EXTERNAL_ASSIGN_MASK: u64 = 0xFFFF_0000_0000_0000;
pub const DEFAULT_CACHE_UNLIMITED: usize = usize::MAX; // unlimited by default

pub trait NodeValue {
    fn is_invalid(&self) -> bool;
    fn invalid_value() -> Self;
    fn is_valid_extern_assign(&self) -> bool;
}

pub trait BlockLoader<V> {
    // return: potentially more meta blocks in vec
    #[cfg(feature = "mt")]
    fn read(&self, v: V, buf: &mut [u8], user_data: u32) -> impl std::future::Future<Output = Result<Vec<(V, Vec<u8>)>>> + Send;
    #[cfg(not(feature = "mt"))]
    fn read(&self, v: V, buf: &mut [u8], user_data: u32) -> impl std::future::Future<Output = Result<Vec<(V, Vec<u8>)>>>;
    fn from_new_path(self, new_path: &str) -> Self;
}

impl NodeValue for u64 {
    fn is_invalid(&self) -> bool {
        self == &u64::MIN 
    }

    fn invalid_value() -> u64 {
       u64::MIN
    }

    fn is_valid_extern_assign(&self) -> bool {
        (self & VALID_EXTERNAL_ASSIGN_MASK) != 0
    }
}

impl<V: Send> BlockLoader<V> for u64 {
    async fn read(&self, v: V, buf: &mut [u8], user_data: u32) -> Result<Vec<(V, Vec<u8>)>> {
        let _ = v;
        let _ = buf;
        let _ = user_data;
        Ok(Vec::new())
    }

    fn from_new_path(self, new_path: &str) -> Self {
        let _ = new_path;
        self.clone()
    }
}
