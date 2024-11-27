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

#[maybe_async::maybe_async(AFIT)]
#[allow(async_fn_in_trait)]
pub trait VMap<K, V>
    where
        K: Copy + Default + fmt::Display + PartialOrd + Eq + std::hash::Hash,
        V: Copy + Default + fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        K: From<V>,
        V: From<K>
{
    async fn lookup(&self, key: K, level: usize) -> Result<V>;
    async fn lookup_contig(&self, key: K, maxblocks: usize) -> Result<(V, usize)>;
    async fn insert(&self, key: K, val: V) -> Result<()>;
    async fn insert_or_update(&self, key: K, val: V) -> Result<Option<V>>;
    async fn delete(&self, key: K) -> Result<()>;
    async fn seek_key(&self, start: K) -> Result<K>;
    async fn last_key(&self) -> Result<K>;
}

pub trait NodeValue<V> {
    fn is_invalid(&self) -> bool;
    fn invalid_value() -> V;
}

pub trait BlockLoader<V> {
    // return: potentially more meta blocks in vec
    fn read(&self, v: V, buf: &mut [u8]) -> impl std::future::Future<Output = Result<Vec<(V, Vec<u8>)>>> + Send;
    fn from_new_path(self, new_path: &str) -> Self;
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

impl<V: Send> BlockLoader<V> for u64 {
    async fn read(&self, v: V, buf: &mut [u8]) -> Result<Vec<(V, Vec<u8>)>> {
        let _ = v;
        let _ = buf;
        Ok(Vec::new())
    }

    fn from_new_path(self, new_path: &str) -> Self {
        let _ = new_path;
        self.clone()
    }
}

// null block loader for test purpose
#[derive(Clone)]
pub struct NullBlockLoader;

impl<V: Send> BlockLoader<V> for NullBlockLoader {
    async fn read(&self, _v: V, _buf: &mut [u8]) -> Result<Vec<(V, Vec<u8>)>> {
        todo!()
    }

    fn from_new_path(self, _: &str) -> Self {
        todo!()
    }
}
