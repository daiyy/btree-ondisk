use std::fmt;
use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;
use tokio::io::{Error, ErrorKind, Result};
use crate::VMap;
use crate::NodeValue;
use crate::node::*;

type BtreeNodeRef<'a, K, V> = Rc<RefCell<BtreeNode<'a, K, V>>>;

pub struct DirectMap<'a, K, V> {
    pub data: Vec<u8>,
    pub root: BtreeNodeRef<'a, K, V>,
    pub nodes: RefCell<HashMap<K, BtreeNodeRef<'a, K, V>>>, // list of btree node in memory
    pub last_seq: RefCell<V>,
    pub dirty: RefCell<bool>,
    pub meta_block_size: usize,
}

impl<'a, K, V> fmt::Display for DirectMap<'a, K, V>
    where
        K: Copy + fmt::Display + std::cmp::PartialOrd,
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
    #[inline]
    fn get_next_seq(&self) -> V {
        let old_value = *self.last_seq.borrow();
        *self.last_seq.borrow_mut() += 1;
        old_value
    }

    #[allow(dead_code)]
    #[inline]
    fn get_val(&self) -> V {
        *self.last_seq.borrow_mut() += 1;
        *self.last_seq.borrow()
    }

    #[inline]
    pub(crate) fn is_key_exceed(&self, key: K) -> bool {
        let index = key.into() as usize;
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
    fn clear_dirty(&self) {
        *self.dirty.borrow_mut() = false;
    }

    pub fn as_slice(&self) -> &[u8] {
        self.data.as_slice()
    }

    pub(crate) fn lookup_dirty(&self) -> Vec<BtreeNodeRef<'a, K, V>> {
        let mut v = Vec::new();
        if self.is_dirty() {
            v.push(self.root.clone());
        }
        v
    }

    pub(crate) async fn assign(&self, key: K, newval: V, _: Option<BtreeNodeRef<'_, K, V>>) -> Result<()> {
        let index = key.into() as usize;
        let val = self.root.borrow().get_val(index);
        if val.is_invalid() {
            return Err(Error::new(ErrorKind::InvalidData, ""));
        }
        self.root.borrow_mut().set_val(index, &newval);
        Ok(())
    }

    pub(crate) fn new(data: &[u8], meta_block_size: usize) -> Self {
        let mut v = Vec::with_capacity(data.len());
        v.extend_from_slice(data);
        Self {
            root: Rc::new(RefCell::new(BtreeNode::<K, V>::from_slice(&v))),
            data: v,
            nodes: RefCell::new(HashMap::new()),
            last_seq: RefCell::new(V::invalid_value()),
            dirty: RefCell::new(false),
            meta_block_size: meta_block_size,
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
    async fn lookup(&self, key: K, level: usize) -> Result<V> {
        return Err(Error::new(ErrorKind::NotFound, ""));
    }

    async fn lookup_contig(&self, key: K, maxblocks: usize) -> Result<(V, usize)> {
        let index = key.into() as usize;
        if index > self.root.borrow().get_capacity() - 1 {
            return Err(Error::new(ErrorKind::NotFound, ""));
        }
        if self.root.borrow().get_val(index).is_invalid() {
            return Err(Error::new(ErrorKind::NotFound, ""));
        }
        let max = std::cmp::max(maxblocks, self.root.borrow().get_capacity() - 1 - index + 1);
        let mut count = 1;
        while count < max {
            if self.root.borrow().get_val(index + count).is_invalid() {
                break;
            }
            count += 1;
        }
        let val = self.root.borrow().get_val(index + count);
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
        self.root.borrow_mut().insert(index, &key, &val);
        Ok(())
    }

    async fn delete(&self, key: K) -> Result<()> {
        let index = key.into() as usize;
        if index > self.root.borrow().get_capacity() ||
                self.root.borrow().get_val(index).is_invalid() {
            return Err(Error::new(ErrorKind::NotFound, ""));
        }
        let value = self.root.borrow_mut().set_val(index, &V::invalid_value());
        Ok(())
    }

    async fn seek_key(&self, start: K) -> Result<K> {
        let mut key = start;
        let mut count = 0;
        let max = self.root.borrow().get_capacity();
        while count < max {
            let index = key.into() as usize;
            if !self.root.borrow().get_val(index).is_invalid() {
                return Ok(start);
            }
            key += 1;
            count += 1;
        }
        Err(Error::new(ErrorKind::NotFound, ""))
    }

    async fn last_key(&self) -> Result<K> {
        let mut key = K::default();
        let mut last_key: Option<K> = None;
        let mut count = 0;
        let max = self.root.borrow().get_capacity();
        while count <= max - 1 {
            let index = key.into() as usize;
            if !self.root.borrow().get_val(index).is_invalid() {
                last_key = Some(key);
            }
            key += 1;
            count += 1;
        }
        if last_key.is_some() {
            Ok::<K, Error>(last_key.unwrap());
        }
        Err(Error::new(ErrorKind::NotFound, ""))
    }
}
