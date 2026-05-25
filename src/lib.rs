//! Key/Value map implementation.
//!
//! Starting from a flat map of signle direct node which for small k/v set.
//!
//! Converted to btree based map when key exceed direct node's capacity.
//!
//! Individual btree node in the map can be load back from backend storage by [`BlockLoader`].
//!
//! # Batch lookup
//!
//! For workloads that issue many lookups against a cold cache,
//! [`bmap::BMap::lookup_batch`] resolves a slice of keys with one
//! batched backend read per tree level instead of one per key per
//! level. Loaders that override [`BlockLoader::read_batch`] to fan
//! out concurrently (e.g. via `futures::future::join_all` or a
//! native batch GET API) collapse `N × (H − 1)` serial RTTs into
//! `H − 1` parallel ones.
//!
//! [`bmap::BMap::lookup_contig`] performs the same kind of
//! sibling-leaf prefetch within a single parent on its own. Both
//! single-key APIs (`lookup`, `lookup_at_level`, `lookup_contig`)
//! preserve their existing semantics; the batch entry points are
//! purely additive.
//!
//! See `docs/prefetch.md` for the design and measured speedups.
//!
//! # Contract for the pointer type `P`
//!
//! `BMap` is generic over the child-pointer type `P`. Because the internal
//! sequence counter is stored as an `AtomicU64` under the `arc` feature,
//! **the `From<u64>` and `Into<u64>` impls on `P` must be mutual inverses**:
//! for every `x: u64`,
//!
//! ```text
//! let p: P = x.into();
//! let y: u64 = p.into();
//! assert_eq!(x, y);
//! ```
//!
//! In practice this means `P` should either be `u64` itself or a
//! transparent wrapper that preserves all 64 bits of the input. Lossy
//! conversions (e.g. truncating to `u32`) are not supported and will
//! silently desynchronise the internal sequence under `arc`.

#[cfg(all(feature = "rc", feature = "arc"))]
compile_error!("features `rc` and `arc` are mutually exclusive");

#[cfg(all(feature = "rc", feature = "mt"))]
compile_error!("feature `mt` requires `arc` (single-threaded `rc` cannot produce Send futures)");

use std::fmt;
use std::io::Result;

pub mod ondisk;
pub mod node;
pub mod btree;
mod direct;
pub mod bmap;
mod loader;
mod cache;
pub use crate::loader::null::NullBlockLoader;
pub use crate::loader::memory::MemoryBlockLoader;
pub use crate::cache::null::NullNodeCache;

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

// --- mt-feature conditional bound markers -----------------------------
//
// `BlockLoader::read_batch` requires `V: Send + Sync` and `Self: Sync`
// under the `mt` feature so its returned future is `Send`. Methods that
// invoke `read_batch` therefore need to surface those requirements
// onto whatever type carries the loader (typically `BtreeMap<...>`).
// Doing so by hand with `#[cfg(feature = "mt")]` on every `where`
// clause is unstable (attributes-in-where is feature-gated). Two
// marker traits below provide the same effect via blanket impls that
// flip with the feature flag:
//
//   * Under `mt`:    `MaybeSendSync` ⇔ `Send + Sync`,
//                    `MaybeSync`     ⇔ `Sync`.
//   * Under non-mt:  both are blanket-implemented for every type,
//                    so they impose no real bound.
//
// Methods that touch `read_batch` write a single `where T: MaybeSync`
// (or `MaybeSendSync`) clause; the actual bound expands per feature.
// The traits are `pub(crate)` so they don't leak into the public API,
// and `#[doc(hidden)]` so re-exports through inner items don't surface
// them in rustdoc.

#[doc(hidden)]
#[cfg(feature = "mt")]
pub trait MaybeSendSync: Send + Sync {}
#[doc(hidden)]
#[cfg(feature = "mt")]
impl<T: Send + Sync> MaybeSendSync for T {}

#[doc(hidden)]
#[cfg(not(feature = "mt"))]
pub trait MaybeSendSync {}
#[doc(hidden)]
#[cfg(not(feature = "mt"))]
impl<T> MaybeSendSync for T {}

#[doc(hidden)]
#[cfg(feature = "mt")]
pub trait MaybeSync: Sync {}
#[doc(hidden)]
#[cfg(feature = "mt")]
impl<T: Sync> MaybeSync for T {}

#[doc(hidden)]
#[cfg(not(feature = "mt"))]
pub trait MaybeSync {}
#[doc(hidden)]
#[cfg(not(feature = "mt"))]
impl<T> MaybeSync for T {}

pub trait BlockLoader<V> {
    // return: potentially more meta blocks in vec
    #[cfg(feature = "mt")]
    fn read(&self, v: V, buf: &mut [u8], user_data: u32) -> impl std::future::Future<Output = Result<Vec<(V, Vec<u8>)>>> + Send;
    #[cfg(not(feature = "mt"))]
    fn read(&self, v: V, buf: &mut [u8], user_data: u32) -> impl std::future::Future<Output = Result<Vec<(V, Vec<u8>)>>>;

    /// Batched read: load `ids[i]` into `bufs[i]` for all `i`, concurrently
    /// if the implementor supports it. Return value aggregates any
    /// "side-loaded" neighbours reported by the individual `read` calls.
    ///
    /// The default implementation simply loops over `read` sequentially,
    /// preserving exact back-compat for existing `BlockLoader` impls. To
    /// get real concurrency, override this method (see
    /// `MemoryBlockLoader::read_batch` for an example using
    /// `futures::future::join_all`).
    ///
    /// Precondition: `ids.len() == bufs.len()`. Panics otherwise.
    #[cfg(feature = "mt")]
    fn read_batch(
        &self,
        ids: &[V],
        bufs: &mut [Vec<u8>],
        user_data: u32,
    ) -> impl std::future::Future<Output = Result<Vec<(V, Vec<u8>)>>> + Send
    where
        V: Copy + Send + Sync,
        Self: Sync,
    {
        assert_eq!(ids.len(), bufs.len(), "read_batch: ids and bufs length must match");
        async move {
            let mut more: Vec<(V, Vec<u8>)> = Vec::new();
            for (id, buf) in ids.iter().zip(bufs.iter_mut()) {
                let m = self.read(*id, buf.as_mut_slice(), user_data).await?;
                more.extend(m);
            }
            Ok(more)
        }
    }
    #[cfg(not(feature = "mt"))]
    fn read_batch(
        &self,
        ids: &[V],
        bufs: &mut [Vec<u8>],
        user_data: u32,
    ) -> impl std::future::Future<Output = Result<Vec<(V, Vec<u8>)>>>
    where
        V: Copy,
    {
        assert_eq!(ids.len(), bufs.len(), "read_batch: ids and bufs length must match");
        async move {
            let mut more: Vec<(V, Vec<u8>)> = Vec::new();
            for (id, buf) in ids.iter().zip(bufs.iter_mut()) {
                let m = self.read(*id, buf.as_mut_slice(), user_data).await?;
                more.extend(m);
            }
            Ok(more)
        }
    }

    fn dup_from_new_path(self, new_path: &str) -> Self;
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

    fn dup_from_new_path(self, new_path: &str) -> Self {
        let _ = new_path;
        self
    }
}

pub use crate::cache::NodeTieredCacheStats;

pub trait NodeCache<P> {
    fn push(&self, p: &P, data: &[u8]);
    #[cfg(feature = "mt")]
    fn load(&self, p: P, data: &mut [u8]) -> impl std::future::Future<Output = Result<bool>> + Send;
    #[cfg(not(feature = "mt"))]
    fn load(&self, p: P, data: &mut [u8]) -> impl std::future::Future<Output = Result<bool>>;
    fn invalid(&self, p: &P);
    fn evict(&self);
    fn get_stats(&self) -> cache::NodeTieredCacheStats;
    fn shutdown(&self);
}
