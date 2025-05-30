use std::fmt;
#[cfg(feature = "rc")]
use std::rc::Rc;
#[cfg(feature = "rc")]
use std::cell::RefCell;
#[cfg(feature = "arc")]
use std::sync::Arc;
#[cfg(feature = "arc")]
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::marker::PhantomData;
use std::io::{Error, ErrorKind, Result};
use crate::VMap;
use crate::NodeValue;
use crate::node::*;
use crate::DEFAULT_CACHE_UNLIMITED;

#[cfg(feature = "rc")]
pub struct DirectMap<'a, K, V, P> {
    pub data: Vec<u8>,
    pub root: Rc<Box<DirectNode<'a, V>>>,
    pub last_seq: RefCell<P>,
    pub dirty: RefCell<bool>,
    pub cache_limit: RefCell<usize>,
    pub marker: PhantomData<K>,
}

#[cfg(feature = "arc")]
pub struct DirectMap<'a, K, V, P> {
    pub data: Vec<u8>,
    pub root: Arc<Box<DirectNode<'a, V>>>,
    pub last_seq: Arc<AtomicU64>,
    pub dirty: Arc<AtomicBool>,
    pub cache_limit: Arc<AtomicUsize>,
    pub marker: PhantomData<K>,
    pub marker_p: PhantomData<P>,
}

impl<'a, K, V, P> fmt::Display for DirectMap<'a, K, V, P>
    where
        V: Copy + fmt::Display
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.root)
    }
}

impl<'a, K, V, P> DirectMap<'a, K, V, P>
    where
        K: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        V: Copy + Default + std::fmt::Display + NodeValue,
        K: Into<u64>,
        P: Copy + NodeValue + std::ops::AddAssign<u64> + From<u64> + Into<u64>,
{
    #[cfg(feature = "rc")]
    #[allow(dead_code)]
    #[inline]
    fn get_next_seq(&self) -> P {
        let old_value = *self.last_seq.borrow();
        *self.last_seq.borrow_mut() += 1;
        old_value
    }

    #[cfg(feature = "arc")]
    #[allow(dead_code)]
    #[inline]
    fn get_next_seq(&self) -> P {
        let old_value = self.last_seq.fetch_add(1, Ordering::SeqCst);
        From::<u64>::from(old_value)
    }

    #[inline]
    pub(crate) fn is_key_exceed(&self, key: &K) -> bool {
        let index = (*key).into() as usize;
        // if key's index is exceeded
        index >= self.root.get_capacity()
    }

    #[inline]
    fn is_dirty(&self) -> bool {
        #[cfg(feature = "rc")]
        return self.dirty.borrow().clone();
        #[cfg(feature = "arc")]
        return self.dirty.load(Ordering::SeqCst);
    }

    #[cfg(feature = "rc")]
    #[inline]
    fn set_dirty(&self) {
        *self.dirty.borrow_mut() = true;
    }

    #[cfg(feature = "arc")]
    #[inline]
    fn set_dirty(&self) {
        self.dirty.store(true, Ordering::SeqCst);
    }

    #[cfg(feature = "rc")]
    #[inline]
    pub(crate) fn clear_dirty(&self) {
        *self.dirty.borrow_mut() = false;
    }

    #[cfg(feature = "arc")]
    #[inline]
    pub(crate) fn clear_dirty(&self) {
        self.dirty.store(false, Ordering::SeqCst);
    }

    pub fn as_slice(&self) -> &[u8] {
        self.data.as_slice()
    }

    #[inline]
    pub(crate) fn get_userdata(&self) -> u32 {
        self.root.get_userdata()
    }

    #[inline]
    pub(crate) fn set_userdata(&self, data: u32) {
        self.root.set_userdata(data);
    }

    #[inline]
    pub(crate) fn get_cache_limit(&self) -> usize {
        #[cfg(feature = "rc")]
        return self.cache_limit.borrow().to_owned();
        #[cfg(feature = "arc")]
        return self.cache_limit.load(Ordering::SeqCst);
    }

    #[cfg(feature = "rc")]
    #[inline]
    pub(crate) fn set_cache_limit(&self, limit: usize) {
        *self.cache_limit.borrow_mut() = limit;
    }

    #[cfg(feature = "arc")]
    #[inline]
    pub(crate) fn set_cache_limit(&self, limit: usize) {
        self.cache_limit.store(limit, Ordering::SeqCst);
    }

    // test if map is dirty, expose to crate
    pub(crate) fn dirty(&self) -> bool {
        self.is_dirty()
    }

    #[allow(dead_code)]
    #[maybe_async::maybe_async]
    pub(crate) async fn assign(&self, key: &K, newval: V) -> Result<()> {
        if self.is_key_exceed(key) {
            return Err(Error::new(ErrorKind::InvalidData, "assign key exceed direct node space"));
        }
        let index = (*key).into() as usize;
        let val = self.root.get_val(index);
        if val.is_invalid() {
            return Err(Error::new(ErrorKind::NotFound, "assign key not found in direct node"));
        }
        self.root.set_val(index, &newval);
        self.set_dirty();
        Ok(())
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn propagate(&self, _: &K) -> Result<()> {
        // do nothing for direct node
        Ok(())
    }

    pub(crate) fn new(data: &[u8]) -> Self {
        let mut v = Vec::with_capacity(data.len());
        v.extend_from_slice(data);
        Self {
            #[cfg(feature = "rc")]
            root: Rc::new(Box::new(DirectNode::<V>::from_slice(&v))),
            #[cfg(feature = "arc")]
            root: Arc::new(Box::new(DirectNode::<V>::from_slice(&v))),
            data: v,
            #[cfg(feature = "rc")]
            last_seq: RefCell::new(P::invalid_value()),
            #[cfg(feature = "rc")]
            dirty: RefCell::new(false),
            #[cfg(feature = "arc")]
            last_seq: Arc::new(AtomicU64::new(P::invalid_value().into())),
            #[cfg(feature = "arc")]
            dirty: Arc::new(AtomicBool::new(false)),
            #[cfg(feature = "rc")]
            cache_limit: RefCell::new(DEFAULT_CACHE_UNLIMITED),
            #[cfg(feature = "arc")]
            cache_limit: Arc::new(AtomicUsize::new(DEFAULT_CACHE_UNLIMITED)),
            marker: PhantomData,
            #[cfg(feature = "arc")]
            marker_p: PhantomData,
        }
    }

}

impl<'a, K, V, P> VMap<K, V> for DirectMap<'a, K, V, P>
    where
        K: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        V: Copy + Default + std::fmt::Display + NodeValue,
        K: Into<u64>,
        P: Copy + NodeValue + std::ops::AddAssign<u64> + From<u64> + Into<u64>,
{
    #[maybe_async::maybe_async]
    async fn lookup(&self, key: &K, level: usize) -> Result<V> {
        let index = (*key).into() as usize;
        if self.root.get_capacity() == 0 {
            // handle case if root node is too small for a V
            return Err(Error::new(ErrorKind::NotFound, "direct node not eligible for lookup"));
        }
        if index > self.root.get_capacity() - 1 || level != 1 {
            return Err(Error::new(ErrorKind::NotFound, "lookup key exceed direct node space"));
        }
        let val = self.root.get_val(index);
        if val.is_invalid() {
            return Err(Error::new(ErrorKind::NotFound, "lookup key not found in direct node"));
        }
        return Ok(*val);
    }

    #[maybe_async::maybe_async]
    async fn lookup_contig(&self, key: &K, maxblocks: usize) -> Result<(V, usize)> {
        let index = (*key).into() as usize;
        if index > self.root.get_capacity() - 1 {
            return Err(Error::new(ErrorKind::NotFound, "lookup key exceed direct node space"));
        }
        let val = self.root.get_val(index);
        if val.is_invalid() {
            return Err(Error::new(ErrorKind::NotFound, "lookup key not found in direct node"));
        }
        let max = std::cmp::min(maxblocks, self.root.get_capacity() - 1 - index + 1);
        let mut count = 1;
        while count < max {
            if self.root.get_val(index + count).is_invalid() {
                break;
            }
            count += 1;
        }
        return Ok((*val, count));
    }

    #[maybe_async::maybe_async]
    async fn insert(&self, key: K, val: V) -> Result<()> {
        let index = key.into() as usize;
        if index > self.root.get_capacity() - 1 {
            return Err(Error::new(ErrorKind::NotFound, "insert key exceed direct node space"));
        }
        if !self.root.get_val(index).is_invalid() {
            return Err(Error::new(ErrorKind::AlreadyExists, "insert key exists in direct node"));
        }
        let index = key.into() as usize;
        self.root.set_val(index, &val);
        self.set_dirty();
        Ok(())
    }

    #[maybe_async::maybe_async]
    async fn insert_or_update(&self, key: K, val: V) -> Result<Option<V>> {
        let index = key.into() as usize;
        if index > self.root.get_capacity() - 1 {
            return Err(Error::new(ErrorKind::NotFound, "insert key exceed direct node space"));
        }
        let old_val = if !self.root.get_val(index).is_invalid() {
            // old val
            Some(*self.root.get_val(index))
        } else {
            None
        };
        let index = key.into() as usize;
        self.root.set_val(index, &val);
        self.set_dirty();
        Ok(old_val)
    }

    #[maybe_async::maybe_async]
    async fn delete(&self, key: &K) -> Result<()> {
        let index = (*key).into() as usize;
        if index > self.root.get_capacity() ||
                self.root.get_val(index).is_invalid() {
            return Err(Error::new(ErrorKind::NotFound, "delete key exceed direct node space or not exists"));
        }
        let _ = self.root.set_val(index, &V::invalid_value());
        self.set_dirty();
        Ok(())
    }

    #[maybe_async::maybe_async]
    async fn seek_key(&self, start: &K) -> Result<K> {
        let mut key = *start;
        let start_idx = (*start).into() as usize;
        for index in start_idx..self.root.get_capacity() {
            if !self.root.get_val(index).is_invalid() {
                return Ok(key);
            }
            key += 1;
        }
        Err(Error::new(ErrorKind::NotFound, "seek key not found in direct node"))
    }

    #[maybe_async::maybe_async]
    async fn last_key(&self) -> Result<K> {
        let mut key = K::default();
        let mut last_key: Option<K> = None;
        for index in 0..self.root.get_capacity() {
            if !self.root.get_val(index).is_invalid() {
                last_key = Some(key);
            }
            key += 1;
        }
        if last_key.is_some() {
            return Ok::<K, Error>(last_key.unwrap());
        }
        Err(Error::new(ErrorKind::NotFound, "last key not found in direct node"))
    }
}
