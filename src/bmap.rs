use std::fmt;
use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;
use tokio::io::Result;
use crate::VMap;
use crate::InvalidValue;
use crate::direct::DirectMap;
use crate::btree::BtreeMap;
use crate::node::BtreeNode;
use crate::btree::BtreeNodeRef;

pub enum NodeType<'a, K, V> {
    Direct(DirectMap<'a, K, V>),
    Btree(BtreeMap<'a, K, V>),
}

impl<'a, K, V> fmt::Display for NodeType<'a, K, V>
    where
        K: Copy + fmt::Display + std::cmp::PartialOrd,
        V: Copy + fmt::Display
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            NodeType::Direct(direct) => {
                write!(f, "{}", direct)
            },
            NodeType::Btree(btree) => {
                write!(f, "{}", btree)
            }
        }
    }
}

pub struct BMap<'a, K, V> {
    inner: NodeType<'a, K, V>,
}

impl<'a, K, V> fmt::Display for BMap<'a, K, V>
    where
        K: Copy + fmt::Display + std::cmp::PartialOrd,
        V: Copy + fmt::Display
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl<'a, K, V> BMap<'a, K, V>
    where
        K: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        V: Copy + Default + std::fmt::Display + From<K> + InvalidValue<V>,
        K: From<V> + Into<u64>
{
    async fn convert_and_insert(&mut self, data: Vec<u8>, last_seq: K, key: K, val: V) -> Result<()> {
        // create new btree map
        let mut v = Vec::with_capacity(data.len());
        v.extend(&data);
        let btree = BtreeMap {
            root: Rc::new(RefCell::new(BtreeNode::<K, V>::from_slice(&v))),
            data: v,
            nodes: RefCell::new(HashMap::new()),
            last_seq: RefCell::new(last_seq),
            dirty: RefCell::new(true),
        };

        let first_root_key;
        // create child node @level 1
        {

        let mut node = btree.get_from_nodes(last_seq).await?;
        (*node).borrow_mut().set_flags(0);
        (*node).borrow_mut().set_nchild(0);
        (*node).borrow_mut().set_level(1);
        let root = btree.root.borrow_mut();
        // save first key
        first_root_key = root.get_key(0);
        let mut index = 0;
        for i in 0..root.get_nchild() {
            let k = root.get_key(i);
            let v = root.get_val(i);
            (*node).borrow_mut().insert(i, &k, &v);
            index += 1;
        }
        (*node).borrow_mut().insert(index, &key, &val);
        (*node).borrow_mut().mark_dirty();
        btree.nodes.borrow_mut().insert(last_seq, node);

        }

        // create root node @level 2
        {

        let mut root = btree.root.borrow_mut();
        root.set_nchild(0);
        root.init_root(2);
        let seq: V = V::from(last_seq);
        root.insert(0, &first_root_key, &seq);

        }

        // modify inner
        self.inner = NodeType::Btree(btree);
        Ok(())
    }
}

impl<'a, K, V> BMap<'a, K, V>
    where
        K: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        V: Copy + Default + std::fmt::Display + From<K> + InvalidValue<V>,
        K: From<V> + Into<u64>
{
    pub fn new(data: Vec<u8>) -> Self {
        // start from small
        Self {
            inner: NodeType::Direct(DirectMap::<K, V>::new(data)),
        }
    }

    pub async fn do_insert(&mut self, key: K, val: V) -> Result<()> {
        match &self.inner {
            NodeType::Direct(direct) => {
                if direct.is_key_exceed(key) {
                    // convert and insert
                    let data = direct.data.clone();
                    let last_seq = direct.last_seq.take();
                    return self.convert_and_insert(data, last_seq, key, val).await;
                }
                return direct.insert(key, val).await;
            },
            NodeType::Btree(btree) => {
                return btree.insert(key, val).await;
            },
        }
    }

    async fn do_delete(&mut self, key: K) -> Result<()> {
        match &self.inner {
            NodeType::Direct(direct) => {
                todo!();
            },
            NodeType::Btree(btree) => {
                return btree.delete(key).await;
            },
        }
    }

    pub async fn delete(&mut self, key: K) -> Result<()> {
        self.do_delete(key).await
    }

    pub async fn lookup_at_level(&self, key: K, level: usize) -> Result<V> {
        match &self.inner {
            NodeType::Direct(direct) => {
                return direct.lookup(key, level).await;
            },
            NodeType::Btree(btree) => {
                return btree.lookup(key, level).await;
            },
        }
    }

    pub async fn lookup_contig(&self, key: K, maxblocks: usize) -> Result<(V, usize)> {
        match &self.inner {
            NodeType::Direct(direct) => {
                return direct.lookup_contig(key, maxblocks).await;
            },
            NodeType::Btree(btree) => {
                return btree.lookup_contig(key, maxblocks).await;
            },
        }
    }

    pub fn lookup_dirty(&self) -> Vec<BtreeNodeRef<'a, K, V>> {
        match &self.inner {
            NodeType::Direct(direct) => {
                return Vec::new();
            },
            NodeType::Btree(btree) => {
                return btree.lookup_dirty();
            },
        }
    }

    pub async fn seek_key(&self, start: K) -> Result<K> {
        match &self.inner {
            NodeType::Direct(direct) => {
                return direct.seek_key(start).await;
            },
            NodeType::Btree(btree) => {
                return btree.seek_key(start).await;
            },
        }
    }

    pub async fn last_key(&self) -> Result<K> {
        match &self.inner {
            NodeType::Direct(direct) => {
                return direct.last_key().await;
            },
            NodeType::Btree(btree) => {
                return btree.last_key().await;
            },
        }
    }
}
