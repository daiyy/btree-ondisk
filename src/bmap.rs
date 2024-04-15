use std::fmt;
use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;
use tokio::io::Result;
use crate::VMap;
use crate::{NodeValue, BlockLoader};
use crate::direct::DirectMap;
use crate::btree::BtreeMap;
use crate::node::BtreeNode;
use crate::btree::BtreeNodeRef;

pub enum NodeType<'a, K, V, L: BlockLoader<V>> {
    Direct(DirectMap<'a, K, V>),
    Btree(BtreeMap<'a, K, V, L>),
}

impl<'a, K, V, L> fmt::Display for NodeType<'a, K, V, L>
    where
        K: Copy + fmt::Display + std::cmp::PartialOrd,
        V: Copy + fmt::Display,
        L: BlockLoader<V>
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

pub struct BMap<'a, K, V, L: BlockLoader<V>> {
    inner: NodeType<'a, K, V, L>,
    block_loader: Option<L>,
}

impl<'a, K, V, L> fmt::Display for BMap<'a, K, V, L>
    where
        K: Copy + fmt::Display + std::cmp::PartialOrd,
        V: Copy + fmt::Display,
        L: BlockLoader<V>
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl<'a, K, V, L> BMap<'a, K, V, L>
    where
        K: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        V: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        K: From<V> + Into<u64>,
        V: From<K> + NodeValue<V>,
        L: BlockLoader<V>
{
    async fn convert_and_insert(&mut self, data: Vec<u8>, meta_block_size: usize, last_seq: V, key: K, val: V) -> Result<()> {
        // create new btree map
        let mut v = Vec::with_capacity(data.len());
        v.extend(&data);
        let btree = BtreeMap {
            root: Rc::new(RefCell::new(BtreeNode::<K, V>::from_slice(&v))),
            data: v,
            nodes: RefCell::new(HashMap::new()),
            last_seq: RefCell::new(last_seq),
            dirty: RefCell::new(true),
            meta_block_size: meta_block_size,
            block_loader: self.block_loader.take().unwrap(),
        };

        let first_root_key;
        // create child node @level 1
        {

        let mut node = btree.get_new_node(last_seq).await?;
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
        root.init_root(2, true);
        root.insert(0, &first_root_key, &last_seq);

        }

        // modify inner
        self.inner = NodeType::Btree(btree);
        Ok(())
    }

    async fn convert_to_direct(&mut self, key: K, input: &Vec<(K, V)>,
            root_node_size: usize, last_seq: V, meta_block_size: usize, block_loader: L) -> Result<()> {
        let mut v = Vec::with_capacity(root_node_size);
        v.resize(root_node_size, 0);
        let direct = DirectMap {
            root: Rc::new(RefCell::new(BtreeNode::<K, V>::from_slice(&v))),
            data: v,
            nodes: RefCell::new(HashMap::new()),
            last_seq: RefCell::new(last_seq),
            dirty: RefCell::new(true),
            meta_block_size: meta_block_size,
        };

        let mut i = 0;
        for (k, v) in input {
            // skip the one not in sequential
            if i == (Into::<u64>::into(*k) as usize) {
                direct.root.borrow_mut().insert(i, k, v);
                i += 1;
            } else {
                // this is the one we need to skip
                assert!(key == *k);
            }
        }
        // we accept only 2 cases:
        // 1. input is in sequence
        // 2. we need skip one that not in sequence
        assert!(i == input.len() || i == input.len() - 1);

        self.block_loader = Some(block_loader);
        self.inner = NodeType::Direct(direct);
        Ok(())
    }
}

impl<'a, K, V, L> BMap<'a, K, V, L>
    where
        K: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        V: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        K: From<V> + Into<u64>,
        V: From<K> + NodeValue<V>,
        L: BlockLoader<V> + Clone,
{
    pub fn new(data: &[u8], meta_block_size: usize, block_loader: L) -> Self {
        // start from small
        Self {
            inner: NodeType::Direct(DirectMap::<K, V>::new(data, meta_block_size)),
            block_loader: Some(block_loader),
        }
    }

    pub fn new_direct(data: &[u8], meta_block_size: usize, block_loader: L) -> Self {
        Self {
            inner: NodeType::Direct(DirectMap::<K, V>::new(data, meta_block_size)),
            block_loader: Some(block_loader),
        }
    }

    pub fn new_btree(data: &[u8], meta_block_size: usize, block_loader: L) -> Self {
        Self {
            inner: NodeType::Btree(BtreeMap::<K, V, L>::new(data, meta_block_size, block_loader)),
            block_loader: None,
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        match &self.inner {
            NodeType::Direct(direct) => {
                return direct.as_slice();
            },
            NodeType::Btree(btree) => {
                return btree.as_slice();
            },
        }
    }

    pub async fn do_insert(&mut self, key: K, val: V) -> Result<()> {
        match &self.inner {
            NodeType::Direct(direct) => {
                if direct.is_key_exceed(key) {
                    // convert and insert
                    let data = direct.data.clone();
                    let last_seq = direct.last_seq.take();
                    let meta_block_size = direct.meta_block_size;
                    return self.convert_and_insert(data, meta_block_size, last_seq, key, val).await;
                }
                return direct.insert(key, val).await;
            },
            NodeType::Btree(btree) => {
                return btree.insert(key, val).await;
            },
        }
    }

    pub async fn insert(&mut self, key: K, val: V) -> Result<()> {
        self.do_insert(key, val).await
    }

    async fn do_delete(&mut self, key: K) -> Result<()> {
        match &self.inner {
            NodeType::Direct(direct) => {
                return direct.delete(key).await;
            },
            NodeType::Btree(btree) => {
                let mut v = Vec::<(K, V)>::new();
                if btree.delete_check_and_gather(key, &mut v).await? {
                    let _ = btree.delete(key).await?;
                    let _ = self.convert_to_direct(key, &v,
                        btree.data.len(), btree.last_seq.take(),
                        btree.meta_block_size, btree.block_loader.clone()).await?;
                    return Ok(());
                }
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
                return direct.lookup_dirty();
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

    pub async fn assign(&self, key: K, newval: V, node: Option<BtreeNodeRef<'_, K, V>>) -> Result<()> {
        match &self.inner {
            NodeType::Direct(direct) => {
                return direct.assign(key, newval, node).await;
            },
            NodeType::Btree(btree) => {
                return btree.assign(key, newval, node).await;
            },
        }
    }

    pub async fn mark(&self, key: K, level: usize) -> Result<()> {
        match &self.inner {
            NodeType::Direct(direct) => {
                return Ok(());
            },
            NodeType::Btree(btree) => {
                return btree.mark(key, level).await;
            },
        }
    }

    async fn do_truncate(&mut self, key: K) -> Result<()> {
        let mut last_key = self.last_key().await?;
        if key > last_key {
            return Ok(());
        }

        while key <= last_key {
            let _ = self.do_delete(last_key).await?;
            last_key = self.last_key().await?;
        }
        return Ok(());
    }

    pub async fn truncate(&mut self, key: K) -> Result<()> {
        self.do_truncate(key).await
    }

    // read in root node from extenal buffer
    pub fn read(buf: &[u8], meta_block_size: usize, block_loader: L) -> Self {
        let root = BtreeNode::<K, V>::from_slice(buf);
        if root.is_large() {
            return Self::new_btree(buf, meta_block_size, block_loader);
        }
        return Self::new_direct(buf, meta_block_size, block_loader);
    }

    // write out root node to external buffer
    pub fn write(&self, buf: &mut [u8]) {
        buf.copy_from_slice(self.as_slice())
    }
}
