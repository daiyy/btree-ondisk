use std::fmt;
use std::rc::Rc;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::io::{Error, ErrorKind, Result};
use crate::VMap;
use crate::NodeValue;
use crate::node::*;

pub struct DirectMap<'a, K, V> {
    pub data: Vec<u8>,
    pub root: Rc<RefCell<DirectNode<'a, V>>>,
    pub last_seq: RefCell<V>,
    pub dirty: RefCell<bool>,
    pub marker: PhantomData<K>,
}

impl<'a, K, V> fmt::Display for DirectMap<'a, K, V>
    where
        V: Copy + fmt::Display
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", (*self.root).borrow())
    }
}

impl<'a, K, V> DirectMap<'a, K, V>
    where
        K: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        V: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        K: From<V> + Into<u64>,
        V: From<K> + NodeValue<V>
{
    #[allow(dead_code)]
    #[inline]
    fn get_next_seq(&self) -> V {
        let old_value = *self.last_seq.borrow();
        *self.last_seq.borrow_mut() += 1;
        old_value
    }

    #[inline]
    pub(crate) fn is_key_exceed(&self, key: &K) -> bool {
        let index = (*key).into() as usize;
        // if key's index is exceeded
        index >= self.root.borrow().get_capacity()
    }

    #[inline]
    fn is_dirty(&self) -> bool {
        self.dirty.borrow().clone()
    }

    #[inline]
    fn set_dirty(&self) {
        *self.dirty.borrow_mut() = true;
    }

    #[inline]
    pub(crate) fn clear_dirty(&self) {
        *self.dirty.borrow_mut() = false;
    }

    pub fn as_slice(&self) -> &[u8] {
        self.data.as_slice()
    }

    // test if map is dirty, expose to crate
    pub(crate) fn dirty(&self) -> bool {
        self.is_dirty()
    }

    pub(crate) async fn assign(&self, key: &K, newval: V) -> Result<()> {
        if self.is_key_exceed(key) {
            return Err(Error::new(ErrorKind::InvalidData, ""));
        }
        let index = (*key).into() as usize;
        let val = *self.root.borrow().get_val(index);
        if val.is_invalid() {
            return Err(Error::new(ErrorKind::InvalidData, ""));
        }
        self.root.borrow_mut().set_val(index, &newval);
        Ok(())
    }

    pub(crate) async fn propagate(&self, _: &K) -> Result<()> {
        // do nothing for direct node
        Ok(())
    }

    pub(crate) fn new(data: &[u8]) -> Self {
        let mut v = Vec::with_capacity(data.len());
        v.extend_from_slice(data);
        Self {
            root: Rc::new(RefCell::new(DirectNode::<V>::from_slice(&v))),
            data: v,
            last_seq: RefCell::new(V::invalid_value()),
            dirty: RefCell::new(false),
            marker: PhantomData,
        }
    }

}

impl<'a, K, V> VMap<K, V> for DirectMap<'a, K, V>
    where
        K: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        V: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        K: From<V> + Into<u64>,
        V: From<K> + NodeValue<V>
{
    async fn lookup(&self, key: &K, level: usize) -> Result<V> {
        let index = (*key).into() as usize;
        if index > self.root.borrow().get_capacity() - 1 || level != 1 {
            return Err(Error::new(ErrorKind::NotFound, ""));
        }
        let val = *self.root.borrow().get_val(index);
        if val.is_invalid() {
            return Err(Error::new(ErrorKind::NotFound, ""));
        }
        return Ok(val);
    }

    async fn lookup_contig(&self, key: &K, maxblocks: usize) -> Result<(V, usize)> {
        let index = (*key).into() as usize;
        if index > self.root.borrow().get_capacity() - 1 {
            return Err(Error::new(ErrorKind::NotFound, ""));
        }
        if self.root.borrow().get_val(index).is_invalid() {
            return Err(Error::new(ErrorKind::NotFound, ""));
        }
        let max = std::cmp::min(maxblocks, self.root.borrow().get_capacity() - 1 - index + 1);
        let mut count = 1;
        while count < max {
            if self.root.borrow().get_val(index + count).is_invalid() {
                break;
            }
            count += 1;
        }
        let val = *self.root.borrow().get_val(index + count);
        return Ok((val, count));
    }

    async fn insert(&self, key: K, val: V) -> Result<()> {
        let index = key.into() as usize;
        if index > self.root.borrow().get_capacity() - 1 {
            return Err(Error::new(ErrorKind::NotFound, ""));
        }
        if !self.root.borrow().get_val(index).is_invalid() {
            return Err(Error::new(ErrorKind::AlreadyExists, ""));
        }
        self.set_dirty();
        let index = key.into() as usize;
        self.root.borrow_mut().set_val(index, &val);
        Ok(())
    }

    async fn insert_or_update(&self, key: K, val: V) -> Result<Option<V>> {
        let index = key.into() as usize;
        if index > self.root.borrow().get_capacity() - 1 {
            return Err(Error::new(ErrorKind::NotFound, ""));
        }
        let old_val = if !self.root.borrow().get_val(index).is_invalid() {
            // old val
            Some(*self.root.borrow().get_val(index))
        } else {
            None
        };
        self.set_dirty();
        let index = key.into() as usize;
        self.root.borrow_mut().set_val(index, &val);
        Ok(old_val)
    }

    async fn delete(&self, key: &K) -> Result<()> {
        let index = (*key).into() as usize;
        if index > self.root.borrow().get_capacity() ||
                self.root.borrow().get_val(index).is_invalid() {
            return Err(Error::new(ErrorKind::NotFound, ""));
        }
        let _ = self.root.borrow_mut().set_val(index, &V::invalid_value());
        Ok(())
    }

    async fn seek_key(&self, start: &K) -> Result<K> {
        let mut key = *start;
        let start_idx = (*start).into() as usize;
        for index in start_idx..self.root.borrow().get_capacity() {
            if !self.root.borrow().get_val(index).is_invalid() {
                return Ok(key);
            }
            key += 1;
        }
        Err(Error::new(ErrorKind::NotFound, ""))
    }

    async fn last_key(&self) -> Result<K> {
        let mut key = K::default();
        let mut last_key: Option<K> = None;
        for index in 0..self.root.borrow().get_capacity() {
            if !self.root.borrow().get_val(index).is_invalid() {
                last_key = Some(key);
            }
            key += 1;
        }
        if last_key.is_some() {
            return Ok::<K, Error>(last_key.unwrap());
        }
        Err(Error::new(ErrorKind::NotFound, ""))
    }
}
