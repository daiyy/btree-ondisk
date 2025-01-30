use std::fmt;
#[cfg(feature = "rc")]
use std::rc::Rc;
#[cfg(feature = "rc")]
use std::cell::RefCell;
#[cfg(feature = "arc")]
use std::sync::Arc;
#[cfg(feature = "arc")]
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
#[cfg(feature = "arc")]
use atomic_refcell::AtomicRefCell;
use std::collections::{HashMap, VecDeque};
use std::marker::PhantomData;
use std::io::{Result, ErrorKind};
use crate::VMap;
use crate::{NodeValue, BlockLoader};
use crate::direct::DirectMap;
use crate::btree::BtreeMap;
use crate::node::{BtreeNode, DirectNode};
use crate::btree::{BtreeNodeRef, BtreeNodeDirty};

#[derive(Default, Debug)]
pub struct BMapStat {
    pub btree: bool,
    pub level: usize,
    pub dirty: bool,
    pub meta_block_size: usize,
    pub nodes_total: usize,
    pub nodes_l1: usize,
}

pub enum NodeType<'a, K, V, L: BlockLoader<V>> {
    Direct(DirectMap<'a, K, V>),
    Btree(BtreeMap<'a, K, V, L>),
}

impl<'a, K, V, L> fmt::Display for NodeType<'a, K, V, L>
    where
        K: Copy + fmt::Display + std::cmp::PartialOrd,
        V: Copy + fmt::Display + NodeValue<V>,
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
    meta_block_size: usize,
    block_loader: Option<L>,
}

impl<'a, K, V, L> fmt::Display for BMap<'a, K, V, L>
    where
        K: Copy + fmt::Display + std::cmp::PartialOrd,
        V: Copy + fmt::Display + NodeValue<V>,
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
        K: From<V> + Into<u64> + From<u64>,
        V: From<K> + NodeValue<V> + From<u64> + Into<u64>,
        L: BlockLoader<V>,
{
    #[maybe_async::maybe_async]
    async fn convert_and_insert(&mut self, data: Vec<u8>, meta_block_size: usize, last_seq: V, limit: usize, key: K, val: V) -> Result<()> {
        // collect all valid value from old direct root
        let mut old_kv = Vec::new();
        let direct = DirectNode::<V>::from_slice(&data);
        for i in 0..direct.get_capacity() {
            let val = direct.get_val(i);
            if !val.is_invalid() {
                old_kv.push((From::<u64>::from(i as u64), val));
            }
        }

        // create new btree map
        let mut v = Vec::with_capacity(data.len());
        // root node to all zero
        v.resize(data.len(), 0);
        let btree = BtreeMap {
            #[cfg(feature = "rc")]
            root: Rc::new(Box::new(BtreeNode::<K, V>::from_slice(&v))),
            #[cfg(feature = "arc")]
            root: Arc::new(Box::new(BtreeNode::<K, V>::from_slice(&v))),
            data: v,
            #[cfg(feature = "rc")]
            nodes: RefCell::new(HashMap::new()),
            #[cfg(feature = "rc")]
            last_seq: RefCell::new(last_seq),
            #[cfg(feature = "rc")]
            dirty: RefCell::new(true),
            #[cfg(feature = "arc")]
            nodes: AtomicRefCell::new(HashMap::new()),
            #[cfg(feature = "arc")]
            last_seq: Arc::new(AtomicU64::new(Into::<u64>::into(last_seq))),
            #[cfg(feature = "arc")]
            dirty: Arc::new(AtomicBool::new(true)),
            meta_block_size: meta_block_size,
            #[cfg(feature = "rc")]
            cache_limit: RefCell::new(limit),
            #[cfg(feature = "arc")]
            cache_limit: Arc::new(AtomicUsize::new(limit)),
            block_loader: self.block_loader.take().unwrap(),
        };

        // all values in old root node plus one for new k,v to be insert
        if old_kv.len() + 1 <= btree.root.get_capacity() {
            // create root node @level 1
            btree.root.set_nchild(0);
            btree.root.init_root(1, true);
            let mut index = 0;
            for (k, v) in old_kv {
                btree.root.insert(index, &k, &v);
                index += 1;
            }
            btree.root.insert(index, &key, &val);

            // modify inner
            self.inner = NodeType::Btree(btree);
            return Ok(());
        }

        let first_root_key;
        // create child node @level 1
        {

        let node = btree.get_new_node(last_seq)?;
        node.set_flags(0);
        node.set_nchild(0);
        node.set_level(1);
        // save first key
        first_root_key = old_kv[0].0;
        let mut index = 0;
        for (k, v) in old_kv {
            node.insert(index, &k, &v);
            index += 1;
        }
        node.insert(index, &key, &val);
        node.mark_dirty();
        btree.nodes.borrow_mut().insert(last_seq, node);

        }

        // create root node @level 2
        {

        btree.root.set_nchild(0);
        btree.root.init_root(2, true);
        btree.root.insert(0, &first_root_key, &last_seq);

        }

        // modify inner
        self.inner = NodeType::Btree(btree);
        Ok(())
    }

    #[maybe_async::maybe_async]
    async fn convert_to_direct(&mut self, _key: &K, input: &Vec<(K, V)>,
            root_node_size: usize, last_seq: V, limit: usize, block_loader: L) -> Result<()> {
        let mut v = Vec::with_capacity(root_node_size);
        v.resize(root_node_size, 0);
        let direct = DirectMap {
            #[cfg(feature = "rc")]
            root: Rc::new(Box::new(DirectNode::<V>::from_slice(&v))),
            #[cfg(feature = "arc")]
            root: Arc::new(Box::new(DirectNode::<V>::from_slice(&v))),
            data: v,
            #[cfg(feature = "rc")]
            last_seq: RefCell::new(last_seq),
            #[cfg(feature = "rc")]
            dirty: RefCell::new(true),
            #[cfg(feature = "arc")]
            last_seq: Arc::new(AtomicU64::new(Into::<u64>::into(last_seq))),
            #[cfg(feature = "arc")]
            dirty: Arc::new(AtomicBool::new(true)),
            #[cfg(feature = "rc")]
            cache_limit: RefCell::new(limit),
            #[cfg(feature = "arc")]
            cache_limit: Arc::new(AtomicUsize::new(limit)),
            marker: PhantomData,
        };

        for (k, v) in input {
            // convert key to u64 then to usize,
            // use key as index of direct node
            let i = Into::<u64>::into(*k) as usize;
            direct.root.set_val(i, v);
        }

        self.block_loader = Some(block_loader);
        self.inner = NodeType::Direct(direct);
        Ok(())
    }
}

impl<'a, K, V, L> BMap<'a, K, V, L>
    where
        K: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        V: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        K: From<V> + Into<u64> + From<u64>,
        V: From<K> + NodeValue<V> + From<u64> + Into<u64>,
        L: BlockLoader<V> + Clone,
{
    /// Constructs a map start from empty direct node.
    // start from a direct node at level 1 with no entries
    pub fn new(root_node_size: usize, meta_block_size: usize, block_loader: L) -> Self {
        if root_node_size > (meta_block_size / 2) {
            panic!("root node size {} is too large, reduce to {} at least, which is half of meta block size",
                root_node_size, meta_block_size / 2);
        }
        // allocate temp space to init a root node as direct
        let mut data = Vec::with_capacity(root_node_size);
        data.resize(root_node_size, 0);
        // init direct root node at level 1
        let root = DirectNode::<V>::from_slice(&data);
        // flags = 0, level = 1, nchild = 0;
        root.init(0, 1, 0);

        Self {
            inner: NodeType::Direct(DirectMap::<K, V>::new(&data)),
            meta_block_size: meta_block_size,
            block_loader: Some(block_loader),
        }
    }

    /// Constructs a new direct map from data slice.
    ///
    /// Data will be copied into internal buffer.
    pub fn new_direct(data: &[u8], meta_block_size: usize, block_loader: L) -> Self {
        Self {
            inner: NodeType::Direct(DirectMap::<K, V>::new(data)),
            meta_block_size: meta_block_size,
            block_loader: Some(block_loader),
        }
    }

    /// Constructs a new btree map from data slice.
    ///
    /// Data will be copied into internal buffer.
    pub fn new_btree(data: &[u8], meta_block_size: usize, block_loader: L) -> Self {
        Self {
            inner: NodeType::Btree(BtreeMap::<K, V, L>::new(data, meta_block_size, block_loader)),
            meta_block_size: meta_block_size,
            block_loader: None,
        }
    }

    /// Expose root node data buffer as a slice.
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

    /// Set map dirty flag.
    pub fn dirty(&self) -> bool {
        match &self.inner {
            NodeType::Direct(direct) => {
                return direct.dirty();
            },
            NodeType::Btree(btree) => {
                return btree.dirty();
            }
        }
    }

    /// Clear map dirty flag. (don't affect nodes dirty state)
    pub fn clear_dirty(&mut self) {
        match &self.inner {
            NodeType::Direct(direct) => {
                return direct.clear_dirty();
            },
            NodeType::Btree(btree) => {
                return btree.clear_dirty();
            }
        }
    }

    #[maybe_async::maybe_async]
    async fn do_try_insert(&mut self, key: K, val: V) -> Result<()> {
        match &self.inner {
            NodeType::Direct(direct) => {
                if direct.is_key_exceed(key) {
                    // convert and insert
                    let data = direct.data.clone();
                    #[cfg(feature = "rc")]
                    let last_seq = direct.last_seq.take();
                    #[cfg(feature = "arc")]
                    let last_seq = direct.last_seq.load(Ordering::SeqCst).into();
                    let limit = direct.get_cache_limit();
                    return self.convert_and_insert(data, self.meta_block_size, last_seq, limit, key, val).await;
                }
                return direct.insert(key, val).await;
            },
            NodeType::Btree(btree) => {
                return btree.insert(key, val).await;
            },
        }
    }

    /// Insert a key/value pair.
    ///
    /// # Errors
    ///
    /// * NotFound - key not found, if key inserted out of node's capacity. **direct node ONLY**
    /// * AlreadyExists - key already exists, new value will not be updated into map.
    /// * OutOfMemory - insufficient memory.
    #[maybe_async::maybe_async]
    pub async fn try_insert(&mut self, key: K, val: V) -> Result<()> {
        self.do_try_insert(key, val).await
    }

    #[maybe_async::maybe_async]
    async fn do_insert_or_update(&mut self, key: K, val: V) -> Result<Option<V>> {
        match &self.inner {
            NodeType::Direct(direct) => {
                if direct.is_key_exceed(key) {
                    // convert and insert
                    let data = direct.data.clone();
                    #[cfg(feature = "rc")]
                    let last_seq = direct.last_seq.take();
                    #[cfg(feature = "arc")]
                    let last_seq = direct.last_seq.load(Ordering::SeqCst).into();
                    let limit = direct.get_cache_limit();
                    let _ = self.convert_and_insert(data, self.meta_block_size, last_seq, limit, key, val).await?;
                    return Ok(None);
                }
                return direct.insert_or_update(key, val).await;
            },
            NodeType::Btree(btree) => {
                return btree.insert_or_update(key, val).await;
            },
        }
    }

    /// Insert or Update a key/value pair,
    /// Return old value in Option if key is already exists.
    ///
    /// # Errors
    ///
    /// * NotFound - key not found, if key inserted out of node's capacity. **direct node ONLY**
    /// * OutOfMemory - insufficient memory.
    #[maybe_async::maybe_async]
    pub async fn insert(&mut self, key: K, val: V) -> Result<Option<V>> {
        self.do_insert_or_update(key, val).await
    }

    #[maybe_async::maybe_async]
    async fn do_delete(&mut self, key: &K) -> Result<()> {
        match &self.inner {
            NodeType::Direct(direct) => {
                return direct.delete(key).await;
            },
            NodeType::Btree(btree) => {
                let mut v = Vec::<(K, V)>::new();
                if btree.delete_check_and_gather(key, &mut v).await? {
                    let _ = btree.delete(key).await?;
                    // re-visit vec we got, remove above last key we need to delete
                    v.retain(|(_k, _)| _k != key);
                    #[cfg(feature = "rc")]
                    let _ = self.convert_to_direct(key, &v,
                        btree.data.len(), btree.last_seq.take(), btree.get_cache_limit(), btree.block_loader.clone()).await?;
                    #[cfg(feature = "arc")]
                    let _ = self.convert_to_direct(key, &v,
                        btree.data.len(), btree.last_seq.load(Ordering::SeqCst).into(), btree.get_cache_limit(), btree.block_loader.clone()).await?;
                    return Ok(());
                }
                return btree.delete(key).await;
            },
        }
    }

    /// Delete a key/value pair from map.
    ///
    /// When btree shrink to height 2 or 3, start to check if btree can be converted to direct.
    ///
    /// # Errors
    ///
    /// * NotFound - key not found.
    /// * OutOfMemory - insufficient memory.
    #[maybe_async::maybe_async]
    pub async fn delete(&mut self, key: &K) -> Result<()> {
        self.do_delete(key).await
    }

    /// Lookup key at specific level, return it's value.
    ///
    /// # Errors
    ///
    /// * NotFound - key not found.
    /// * OutOfMemory - insufficient memory.
    #[maybe_async::maybe_async]
    pub async fn lookup_at_level(&self, key: &K, level: usize) -> Result<V> {
        match &self.inner {
            NodeType::Direct(direct) => {
                return direct.lookup(key, level).await;
            },
            NodeType::Btree(btree) => {
                return btree.lookup(key, level).await;
            },
        }
    }

    /// Lookup key at level 1, return it's value.
    ///
    /// # Errors
    ///
    /// * NotFound - key not found.
    /// * OutOfMemory - insufficient memory.
    #[maybe_async::maybe_async]
    pub async fn lookup(&self, key: &K) -> Result<V> {
        self.lookup_at_level(key, 1).await
    }

    /// Lookup max continues key space.
    ///   - key: start key
    ///   - maxblocks: max expected continues
    ///
    /// # Return
    ///
    /// value, max continue count we acctually found
    ///
    /// # Errors
    ///
    /// * NotFound - key not found.
    /// * OutOfMemory - insufficient memory.
    #[maybe_async::maybe_async]
    pub async fn lookup_contig(&self, key: &K, maxblocks: usize) -> Result<(V, usize)> {
        match &self.inner {
            NodeType::Direct(direct) => {
                return direct.lookup_contig(key, maxblocks).await;
            },
            NodeType::Btree(btree) => {
                return btree.lookup_contig(key, maxblocks).await;
            },
        }
    }

    /// Collect all dirty nodes into a Vec.
    pub fn lookup_dirty(&self) -> Vec<BtreeNodeDirty<'a, K, V>> {
        match &self.inner {
            NodeType::Direct(_) => {
                return Vec::new();
            },
            NodeType::Btree(btree) => {
                return btree.lookup_dirty().into_iter().map(|n| BtreeNodeDirty(n)).collect();
            },
        }
    }

    /// Seek a valid entry and return it's key starting from start key.
    ///
    /// # Errors
    ///
    /// * NotFound - no valid entry found.
    /// * OutOfMemory - insufficient memory.
    #[maybe_async::maybe_async]
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

    /// Get last key.
    ///
    /// # Errors
    ///
    /// * NotFound - key not found.
    /// * OutOfMemory - insufficient memory.
    #[maybe_async::maybe_async]
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

    /// Assign a new value by key.
    ///
    /// **data node:** use key to find value entry.
    ///
    /// **meta node:** use node to find value entry, key is unused.
    ///
    /// use [`BMap::assign_meta_node`] and [`BMap::assign_data_node`] for more explict semantics
    ///
    /// # Errors
    ///
    /// * NotFound - key not found.
    /// * OutOfMemory - insufficient memory.
    #[maybe_async::maybe_async]
    pub async fn assign(&self, key: K, newval: V, node: Option<BtreeNodeDirty<'_, K, V>>) -> Result<()> {
        #[cfg(feature = "value-check")]
        if !newval.is_valid_extern_assign() {
            // potiential conflict with seq value internal used
            panic!("assign value is not in a valid format");
        }
        match &self.inner {
            NodeType::Direct(direct) => {
                return direct.assign(key, newval).await;
            },
            NodeType::Btree(btree) => {
                return btree.assign(key, newval, node.map(|n| n.clone_node_ref())).await;
            },
        }
    }

    /// Assign extneral value to meta node.
    ///
    /// # Errors
    ///
    /// * NotFound - key not found.
    /// * OutOfMemory - insufficient memory.
    #[maybe_async::maybe_async]
    pub async fn assign_meta_node(&self, newval: V, node: BtreeNodeDirty<'_, K, V>) -> Result<()> {
        #[cfg(feature = "value-check")]
        if !newval.is_valid_extern_assign() {
            // potiential conflict with seq value internal used
            panic!("assign value is not in a valid format");
        }
        match &self.inner {
            NodeType::Direct(_direct) => {
                return Ok(());
            },
            NodeType::Btree(btree) => {
                // key is unused, so use 0
                return btree.assign(0.into(), newval, Some(node.clone_node_ref())).await;
            },
        }
    }

    /// Assign external value to data node.
    ///
    /// # Errors
    ///
    /// * NotFound - key not found.
    /// * OutOfMemory - insufficient memory.
    #[maybe_async::maybe_async]
    pub async fn assign_data_node(&self, key: K, newval: V) -> Result<()> {
        #[cfg(feature = "value-check")]
        if !newval.is_valid_extern_assign() {
            // potiential conflict with seq value internal used
            panic!("assign value is not in a valid format");
        }
        match &self.inner {
            NodeType::Direct(direct) => {
                return direct.assign(key, newval).await;
            },
            NodeType::Btree(btree) => {
                return btree.assign(key, newval, None).await;
            },
        }
    }

    /// Propagate changes of key/node from specific level up to root, marks all nodes dirty in the path.
    ///
    /// **data node:** use key to find target.
    ///
    /// **meta node:** use node to find target, key is unused.
    ///
    /// explicit invoke of this function is not required,
    /// propagation will be implicit invoked by [`BMap::insert`] and [`BMap::delete`].
    ///
    /// # Errors
    ///
    /// * NotFound - key not found.
    /// * OutOfMemory - insufficient memory.
    #[maybe_async::maybe_async]
    pub async fn propagate(&self, key: K, node: Option<BtreeNodeRef<'_, K, V>>) -> Result<()> {
        match &self.inner {
            NodeType::Direct(direct) => {
                return direct.propagate(key).await;
            },
            NodeType::Btree(btree) => {
                return btree.propagate(key, node).await;
            },
        }
    }

    /// Mark the block specified by key at level as dirty.
    ///
    /// # Errors
    ///
    /// * NotFound - key not found.
    /// * OutOfMemory - insufficient memory.
    #[maybe_async::maybe_async]
    pub async fn mark(&self, key: &K, level: usize) -> Result<()> {
        match &self.inner {
            NodeType::Direct(_) => {
                return Ok(());
            },
            NodeType::Btree(btree) => {
                return btree.mark(key, level).await;
            },
        }
    }

    #[maybe_async::maybe_async]
    async fn do_truncate(&mut self, key: &K) -> Result<()> {
        let mut last_key = self.last_key().await?;
        if key > &last_key {
            return Ok(());
        }

        while key <= &last_key {
            let _ = self.do_delete(&last_key).await?;
            match self.last_key().await {
                Ok(key) => {
                    last_key = key;
                },
                Err(e) => {
                    if e.kind() == ErrorKind::NotFound {
                        return Ok(());
                    }
                    return Err(e);
                }
            }
        }
        return Ok(());
    }

    /// Truncate index to key value (including key value supplied).
    ///
    /// # Errors
    ///
    /// * OutOfMemory - insufficient memory.
    #[maybe_async::maybe_async]
    pub async fn truncate(&mut self, key: &K) -> Result<()> {
        self.do_truncate(key).await
    }

    /// Read in root node from extenal buffer.
    pub fn read(buf: &[u8], meta_block_size: usize, block_loader: L) -> Self {
        let root = BtreeNode::<K, V>::from_slice(buf);
        if root.is_large() {
            return Self::new_btree(buf, meta_block_size, block_loader);
        }
        return Self::new_direct(buf, meta_block_size, block_loader);
    }

    /// Write out root node to external buffer.
    pub fn write(&self, buf: &mut [u8]) {
        buf.copy_from_slice(self.as_slice())
    }

    /// Get statistics from map internal.
    ///
    /// For root node in direct, return all zero.
    pub fn get_stat(&self) -> BMapStat {
        match &self.inner {
            NodeType::Direct(_) => {
                return BMapStat::default();
            },
            NodeType::Btree(btree) => {
                return btree.get_stat();
            },
        }
    }

    /// Get user data from root node
    pub fn get_userdata(&self) -> u32 {
        match &self.inner {
            NodeType::Direct(direct) => {
                return direct.get_userdata();
            },
            NodeType::Btree(btree) => {
                return btree.get_userdata();
            },
        }
    }

    /// Set user data into root node
    pub fn set_userdata(&self, data: u32) {
        match &self.inner {
            NodeType::Direct(direct) => {
                return direct.set_userdata(data);
            },
            NodeType::Btree(btree) => {
                return btree.set_userdata(data);
            },
        }
    }

    /// Get limit of max nodes count cached in memory for btree
    pub fn get_cache_limit(&self) -> usize {
        match &self.inner {
            NodeType::Direct(direct) => {
                return direct.get_cache_limit();
            },
            NodeType::Btree(btree) => {
                return btree.get_cache_limit();
            },
        }
    }

    /// Set limit of max nodes count cached in memory for btree
    pub fn set_cache_limit(&self, limit: usize) {
        match &self.inner {
            NodeType::Direct(direct) => {
                return direct.set_cache_limit(limit);
            },
            NodeType::Btree(btree) => {
                return btree.set_cache_limit(limit);
            },
        }
    }

    /// Return iterator for all non-leaf node which including root node
    pub fn nonleafnode_iter<'b>(&'b self) -> NonLeafNodeIter<'a, 'b, K, V, L> {
        NonLeafNodeIter::new(self)
    }

    /// Release bmap and get back block loader
    pub fn get_block_loader(&self) -> L {
        match &self.inner {
            NodeType::Direct(_) => {
                assert!(self.block_loader.is_some());
                return self.block_loader.as_ref().unwrap().clone();
            },
            NodeType::Btree(btree) => {
                return btree.block_loader.clone();
            },
        }
    }
}

pub struct NonLeafNodeIter<'a, 'b, K, V, L: BlockLoader<V>> {
    bmap: &'b BMap<'a, K, V, L>,
    last_root_node_index: usize,
    root_node_cap_or_nchild: usize,
    last_btree_node_index: usize,
    last_btree_node: Option<BtreeNodeRef<'a, K, V>>,
    btree_node_backlog: VecDeque<BtreeNodeRef<'a, K, V>>,
}

impl<'a, 'b, K, V, L> NonLeafNodeIter<'a, 'b, K, V, L>
    where
        K: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        V: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        K: From<V> + Into<u64> + From<u64>,
        V: From<K> + NodeValue<V> + From<u64> + Into<u64>,
        L: BlockLoader<V> + Clone,
{
    fn new(bmap: &'b BMap<'a, K, V, L>) -> Self {
        let root = BtreeNode::<K, V>::from_slice(bmap.as_slice());
        let root_node_cap_or_nchild = if root.is_large() {
            root.get_nchild()
        } else {
            let root = DirectNode::<V>::from_slice(bmap.as_slice());
            root.get_capacity()
        };

        Self {
            bmap: bmap,
            last_root_node_index: 0,
            root_node_cap_or_nchild: root_node_cap_or_nchild,
            last_btree_node_index: 0,
            last_btree_node: None,
            btree_node_backlog: VecDeque::new(),
        }
    }
}

/// Iterate all values of intermediate nodes from highest level to level 1
impl<'a, 'b, K, V, L> Iterator for NonLeafNodeIter<'a, 'b, K, V, L>
    where
        K: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        V: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        K: From<V> + Into<u64> + From<u64>,
        V: From<K> + NodeValue<V> + From<u64> + Into<u64>,
        L: BlockLoader<V> + Clone,
{
    type Item = V;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match &self.bmap.inner {
            NodeType::Direct(_) => {
                // try working on root node only for direct node
                for idx in self.last_root_node_index..self.root_node_cap_or_nchild {
                    let node = DirectNode::<V>::from_slice(self.bmap.as_slice());
                    let v = node.get_val(idx);
                    self.last_root_node_index += 1;
                    if !v.is_invalid() {
                        return Some(v);
                    }
                }
                return None;
            },
            NodeType::Btree(btree) => {
                // try working on root node
                for idx in self.last_root_node_index..self.root_node_cap_or_nchild {
                    let node = BtreeNode::<K, V>::from_slice(self.bmap.as_slice());
                    let v = node.get_val(idx);
                    assert!(!v.is_invalid());
                    if node.get_level() >= 2 {
                        // for L1 node, we don't need to fetch actual data block
                        // so we limit condition for fetch next level node here to >= 2
                        #[cfg(not(feature = "sync-api"))]
                        let node_ref = futures::executor::block_on(async {
                            btree.get_from_nodes(v).await.unwrap_or_else(|_| panic!("failed to fetch node {v}"))
                        });
                        #[cfg(feature = "sync-api")]
                        let node_ref = btree.get_from_nodes(v).unwrap_or_else(|_| panic!("failed to fetch node {v}"));
                        self.btree_node_backlog.push_back(node_ref);
                    }
                    self.last_root_node_index += 1;
                    return Some(v);
                }

                // try find the node we currently working on
                let node = if let Some(node) = &self.last_btree_node {
                    node
                } else {
                    let Some(node) = self.btree_node_backlog.pop_front() else {
                        // if no node currently working and no more nodes in backlog
                        // we reached the end
                        return None;
                    };
                    self.last_btree_node_index = 0;
                    self.last_btree_node = Some(node);
                    self.last_btree_node.as_ref().expect("unable to get last btree node")
                };

                // try working on one intermediate node
                let v = node.get_val(self.last_btree_node_index);
                assert!(!v.is_invalid());
                if node.get_level() >= 2 {
                    // for L1 node, we don't need to fetch actual data block
                    // so we limit condition for fetch next level node here to >= 2
                    #[cfg(not(feature = "sync-api"))]
                    let node_ref = futures::executor::block_on(async {
                        btree.get_from_nodes(v).await.unwrap_or_else(|_| panic!("failed to fetch node {v}"))
                    });
                    #[cfg(feature = "sync-api")]
                    let node_ref = btree.get_from_nodes(v).unwrap_or_else(|_| panic!("failed to fetch node {v}"));
                    self.btree_node_backlog.push_back(node_ref);
                }
                self.last_btree_node_index += 1;
                if self.last_btree_node_index >= node.get_nchild() {
                    // reach the end of this node,
                    // reset last btree node,
                    // will fetch next available node from backlog next time
                    self.last_btree_node = None;
                }
                return Some(v);
            },
        }
    }
}
