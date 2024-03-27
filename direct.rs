use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;
use tokio::io::{Error, ErrorKind, Result};
use crate::VMap;
use crate::InvalidValue;
use crate::node::*;

type BtreeNodeRef<'a, K, V> = Rc<RefCell<BtreeNode<'a, K, V>>>;

pub struct DirectMap<'a, K, V> {
    pub data: Vec<u8>,
    pub root: BtreeNodeRef<'a, K, V>,
    pub nodes: HashMap<K, BtreeNodeRef<'a, K, V>>, // list of btree node in memory
    pub last_seq: RefCell<K>,
}

impl<'a, K, V> DirectMap<'a, K, V>
    where
        K: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        V: Copy + Default + std::fmt::Display + From<K>,
        K: From<V>
{
    #[inline]
    fn get_next_seq(&self) -> K {
        *self.last_seq.borrow_mut() += 1;
        *self.last_seq.borrow()
    }

    #[inline]
    fn get_val(&self) -> K {
        *self.last_seq.borrow_mut() += 1;
        *self.last_seq.borrow()
    }
}

impl<'a, K, V> VMap<K, V> for DirectMap<'a, K, V>
    where
        K: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        V: Copy + Default + std::fmt::Display + From<K> + InvalidValue,
        K: From<V> + Into<usize>
{
    fn new(data: Vec<u8>) -> Self {
        let root = BtreeNode::<K, V>::new(&data);
        let mut list = HashMap::new();

        Self {
            data: data,
            root: Rc::new(RefCell::new(root)),
            nodes: list,
            last_seq: RefCell::new(K::default()),
        }
    }

    async fn insert(&self, key: K, val: V) -> Result<()> {
        if key.into() > self.root.borrow().get_capacity() {
            return Err(Error::new(ErrorKind::NotFound, ""));
        }
        if !self.root.borrow().get_val(key.into()).is_invalid() {
            return Err(Error::new(ErrorKind::AlreadyExists, ""));
        }
        let next_seq = self.get_next_seq();
        self.root.borrow_mut().set_val(next_seq.into(), &val);

        Ok(())
    }

    async fn delete(&self, key: K) -> Result<()> {
        Ok(())
    }

    async fn seek_key(&self, start: K) -> Result<K> {

        Err(Error::new(ErrorKind::NotFound, ""))
    }

    async fn last_key(&self) -> Result<K> {
        Err(Error::new(ErrorKind::NotFound, ""))
    }
}
