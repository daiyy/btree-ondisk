use std::fmt;
use std::collections::HashMap;
#[cfg(feature = "rc")]
use std::rc::Rc;
#[cfg(feature = "rc")]
use std::cell::RefCell;
#[cfg(feature = "arc")]
use std::sync::Arc;
#[cfg(feature = "arc")]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(feature = "arc")]
use atomic_refcell::AtomicRefCell;
use std::io::{Error, ErrorKind, Result};
use log::{warn, debug};
use crate::node::*;
use crate::VMap;
use crate::bmap::BMapStat;
use crate::{NodeValue, BlockLoader};

pub(crate) type BtreeLevel = usize;
#[cfg(feature = "rc")]
pub type BtreeNodeRef<'a, K, V> = Rc<Box<BtreeNode<'a, K, V>>>;
#[cfg(feature = "arc")]
pub type BtreeNodeRef<'a, K, V> = Arc<Box<BtreeNode<'a, K, V>>>;

#[derive(Clone, Copy, Debug, Default)]
pub enum BtreeMapOp {
    #[default]
    Nop,
    Insert,
    Grow,
    Split,
    CarryLeft,
    CarryRight,
    ConcatLeft,
    ConcatRight,
    BorrowLeft,
    BorrowRight,
    Delete,
    Shrink,
}

struct BtreePathLevel<'a, K, V> {
    node: Option<BtreeNodeRef<'a, K, V>>,
    sib_node: Option<BtreeNodeRef<'a, K, V>>,
    index: usize,
    oldseq: V,
    newseq: V,
    op: BtreeMapOp,
}

pub struct BtreePath<'a, K, V> {
    #[cfg(feature = "rc")]
    levels: Vec<RefCell<BtreePathLevel<'a, K, V>>>,
    #[cfg(feature = "arc")]
    levels: Vec<AtomicRefCell<BtreePathLevel<'a, K, V>>>,
}

impl<'a, K, V: Copy + Default + NodeValue<V>> BtreePath<'a, K, V>
{
    pub fn new() -> Self {
        let mut l = Vec::with_capacity(BTREE_NODE_LEVEL_MAX);
        for _ in 0..BTREE_NODE_LEVEL_MAX {
            #[cfg(feature = "rc")]
            l.push(RefCell::new(BtreePathLevel {
                node: None,
                sib_node: None,
                index: 0,
                oldseq: V::invalid_value(),
                newseq: V::invalid_value(),
                op: BtreeMapOp::Nop,
            }));
            #[cfg(feature = "arc")]
            l.push(AtomicRefCell::new(BtreePathLevel {
                node: None,
                sib_node: None,
                index: 0,
                oldseq: V::invalid_value(),
                newseq: V::invalid_value(),
                op: BtreeMapOp::Nop,
            }));
        }

        Self {
            levels: l,
        }
    }

    #[inline]
    pub fn get_index(&self, level: usize) -> usize {
        self.levels[level].borrow().index
    }

    #[inline]
    pub fn set_index(&self, level: usize, index: usize) {
        self.levels[level].borrow_mut().index = index;
    }

    #[inline]
    pub fn get_nonroot_node(&self, level: usize) -> BtreeNodeRef<'a, K, V> {
        self.levels[level].borrow().node.as_ref().unwrap().clone()
    }

    #[inline]
    pub fn set_nonroot_node(&self, level: usize, node: BtreeNodeRef<'a, K, V>) {
        self.levels[level].borrow_mut().node = Some(node);
    }

    #[inline]
    pub fn set_nonroot_node_none(&self, level: usize) {
        self.levels[level].borrow_mut().node = None;
    }

    #[inline]
    pub fn get_sib_node(&self, level: usize) -> BtreeNodeRef<'a, K, V> {
        self.levels[level].borrow().sib_node.as_ref().unwrap().clone()
    }

    #[inline]
    pub fn set_sib_node(&self, level: usize, node: BtreeNodeRef<'a, K, V>) {
        self.levels[level].borrow_mut().sib_node = Some(node);
    }

    #[inline]
    pub fn set_sib_node_none(&self, level: usize) {
        self.levels[level].borrow_mut().sib_node = None;
    }

    #[inline]
    pub fn get_new_seq(&self, level: usize) -> V {
        self.levels[level].borrow().newseq
    }

    #[inline]
    pub fn set_new_seq(&self, level: usize, seq: V) {
        self.levels[level].borrow_mut().newseq = seq;
    }

    #[inline]
    pub fn get_old_seq(&self, level: usize) -> V {
        self.levels[level].borrow().oldseq
    }

    #[inline]
    pub fn set_old_seq(&self, level: usize, seq: V) {
        self.levels[level].borrow_mut().oldseq = seq;
    }

    #[inline]
    pub fn get_op(&self, level: usize) -> BtreeMapOp {
        self.levels[level].borrow().op
    }

    #[inline]
    pub fn set_op(&self, level: usize, op: BtreeMapOp) {
        self.levels[level].borrow_mut().op = op;
    }
}

#[cfg(feature = "rc")]
pub struct BtreeMap<'a, K, V, L: BlockLoader<V>> {
    pub data: Vec<u8>,
    pub root: BtreeNodeRef<'a, K, V>,
    pub nodes: RefCell<HashMap<V, BtreeNodeRef<'a, K, V>>>, // list of btree node in memory
    pub last_seq: RefCell<V>,
    pub dirty: RefCell<bool>,
    pub meta_block_size: usize,
    pub block_loader: L,
}

#[cfg(feature = "arc")]
pub struct BtreeMap<'a, K, V, L: BlockLoader<V>> {
    pub data: Vec<u8>,
    pub root: BtreeNodeRef<'a, K, V>,
    pub nodes: AtomicRefCell<HashMap<V, BtreeNodeRef<'a, K, V>>>,
    pub last_seq: Arc<AtomicU64>,
    pub dirty: Arc<AtomicBool>,
    pub meta_block_size: usize,
    pub block_loader: L,
}

impl<'a, K, V, L> fmt::Display for BtreeMap<'a, K, V, L>
    where
        K: Copy + fmt::Display + std::cmp::PartialOrd,
        V: Copy + fmt::Display + NodeValue<V>,
        L: BlockLoader<V>,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.root)?;
        for (_, node) in self.nodes.borrow().iter() {
            let n: BtreeNodeRef<'a, K, V> = node.clone();
            write!(f, "{}", n)?;
        }
        write!(f, "")
    }
}

impl<'a, K, V, L> BtreeMap<'a, K, V, L>
    where
        K: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        V: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        K: From<V> + Into<u64>,
        V: From<K> + NodeValue<V> + From<u64> + Into<u64>,
        L: BlockLoader<V>,
{
    #[inline]
    fn get_root_node(&self) -> BtreeNodeRef<'a, K, V> {
        self.root.clone()
    }

    #[inline]
    fn get_root_level(&self) -> usize {
        self.root.get_level()
    }

    #[inline]
    fn get_height(&self) -> usize {
        self.root.get_level() + 1
    }

    #[inline]
    fn get_node(&self, path: &BtreePath<'a, K, V>, level: usize) -> BtreeNodeRef<'a, K, V> {
        if level == self.get_height() - 1 {
            return self.get_root_node();
        }
        path.get_nonroot_node(level)
    }

    #[inline]
    fn is_root_level(&self, level: usize) -> bool {
        level == self.get_height() - 1
    }

    #[inline]
    fn is_nonroot_level(&self, level: usize) -> bool {
        level < self.get_height() - 1
    }

    #[cfg(feature = "rc")]
    #[inline]
    fn get_next_seq(&self) -> V {
        *self.last_seq.borrow_mut() += 1;
        *self.last_seq.borrow()
    }

    #[cfg(feature = "arc")]
    #[inline]
    fn get_next_seq(&self) -> V {
        let old_value = self.last_seq.fetch_add(1, Ordering::SeqCst);
        From::<u64>::from(old_value + 1)
    }

    #[inline]
    fn prepare_seq(&self, path: &BtreePath<K, V>, level: usize) -> V {
        let seq = self.get_next_seq();
        path.set_new_seq(level, seq);
        seq
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
    async fn meta_block_loader(&self, v: V, buf: &mut [u8]) -> Result<Vec<(V, Vec<u8>)>> {
        self.block_loader.read(v, buf).await
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn get_from_nodes(&self, val: V) -> Result<BtreeNodeRef<'a, K, V>> {
        let list = self.nodes.borrow();
        if let Some(node) = list.get(&val) {
            return Ok(node.clone());
        }
        drop(list);

        if let Some(node) = BtreeNode::<K, V>::new_with_id(self.meta_block_size, val) {
            #[cfg(not(feature = "sync-api"))]
            let more = self.meta_block_loader(val, node.as_mut()).await?;
            #[cfg(feature = "sync-api")]
            let more = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    self.meta_block_loader(val, node.as_mut()).await
                })
            })?;
            #[cfg(feature = "rc")]
            let n = Rc::new(Box::new(node));
            #[cfg(feature = "arc")]
            let n = Arc::new(Box::new(node));
            let mut list = self.nodes.borrow_mut();
            list.insert(val, n.clone());
            drop(list);

            // if we have more to load
            for (v, data) in more.into_iter() {
                assert!(data.len() == self.meta_block_size);
                if val == v {
                    // in case we go duplicated meta block
                    // skip it
                    continue;
                }
                if let Some(node) = BtreeNode::<K, V>::copy_from_slice(v, &data) {
                    #[cfg(feature = "rc")]
                    let n = Rc::new(Box::new(node));
                    #[cfg(feature = "arc")]
                    let n = Arc::new(Box::new(node));
                    let mut list = self.nodes.borrow_mut();
                    list.insert(v, n.clone());
                    drop(list);
                } else {
                    return Err(Error::new(ErrorKind::OutOfMemory, ""));
                }
            }
            return Ok(n);
        }
        return Err(Error::new(ErrorKind::OutOfMemory, ""));
    }

    pub(crate) fn get_new_node(&self, val: V) -> Result<BtreeNodeRef<'a, K, V>> {
        let mut list = self.nodes.borrow_mut();
        if let Some(node) = BtreeNode::<K, V>::new_with_id(self.meta_block_size, val) {
            #[cfg(feature = "rc")]
            let n = Rc::new(Box::new(node));
            #[cfg(feature = "arc")]
            let n = Arc::new(Box::new(node));
            if let Some(_oldnode) = list.insert(val, n.clone()) {
                panic!("value {} is already in nodes list", val);
            }
            return Ok(n);
        }
        return Err(Error::new(ErrorKind::OutOfMemory, ""));
    }

    fn remove_from_nodes(&self, node: BtreeNodeRef<K, V>) -> Result<()> {
        let n = self.nodes.borrow_mut().remove(node.id());
        assert!(n.is_some());
        Ok(())
    }

    fn do_op(&self, path: &BtreePath<K, V>, level: BtreeLevel, key: &mut K, val: &mut V) {
        match path.get_op(level) {
            BtreeMapOp::Insert => self.op_insert(path, level, key, val),
            BtreeMapOp::Grow => self.op_grow(path, level, key, val),
            BtreeMapOp::Split => self.op_split(path, level, key, val),
            BtreeMapOp::CarryLeft => self.op_carry_left(path, level, key, val),
            BtreeMapOp::CarryRight => self.op_carry_right(path, level, key, val),
            BtreeMapOp::ConcatLeft => self.op_concat_left(path, level, key, val),
            BtreeMapOp::ConcatRight => self.op_concat_right(path, level, key, val),
            BtreeMapOp::BorrowLeft => self.op_borrow_left(path, level, key, val),
            BtreeMapOp::BorrowRight => self.op_borrow_right(path, level, key, val),
            BtreeMapOp::Delete => self.op_delete(path, level, key, val),
            BtreeMapOp::Shrink => self.op_shrink(path, level, key, val),
            BtreeMapOp::Nop => self.op_nop(path, level, key, val),
        }
    }

    #[maybe_async::maybe_async]
    async fn do_lookup(&self, path: &BtreePath<'a, K, V>, key: &K, minlevel: usize) -> Result<V> {
        let root = self.get_root_node();
        let mut level = root.get_level();
        if level < minlevel || root.get_nchild() <= 0 {
            return Err(Error::new(ErrorKind::NotFound, ""));
        }

        let (mut found, mut index) = root.lookup(key);
        let mut value = root.get_val(index);

        path.set_index(level, index);

        level -= 1;
        while level >= minlevel {
            let node = self.get_from_nodes(value).await?;
            if !found {
                (found, index) = node.lookup(key);
            } else {
                index = 0;
            }

            if index < node.get_nchild() {
                value = node.get_val(index);
            } else {
                if found || level != BTREE_NODE_LEVEL_MIN {
                    warn!("index {} - level {} - found {}", index, level, found);
                }
                value = V::invalid_value();
            }

            path.set_nonroot_node(level, node);
            path.set_index(level, index);
            level -= 1;
        }

        if !found {
            return Err(Error::new(ErrorKind::NotFound, ""));
        }

        Ok(value)
    }

    #[maybe_async::maybe_async]
    async fn do_lookup_last(&self, path: &BtreePath<'a, K, V>) -> Result<K> {
        let mut node = self.get_root_node();
        let nchild = node.get_nchild();
        if nchild == 0 {
            return Err(Error::new(ErrorKind::NotFound, ""));
        }
        let mut index = nchild - 1;
        let mut level = node.get_level();
        let mut value = node.get_val(index);
        path.set_nonroot_node_none(level);
        path.set_index(level, index);

        level -= 1;
        while level > 0 {
            node = self.get_from_nodes(value).await?;
            index = node.get_nchild() - 1;
            value = node.get_val(index);
            path.set_nonroot_node(level, node.clone());
            path.set_index(level, index);
            level -= 1;
        }
        let key = node.get_key(index);
        Ok(key)
    }

    #[maybe_async::maybe_async]
    async fn prepare_insert(&self, path: &BtreePath<'a, K, V>) -> Result<BtreeLevel> {

        let mut level = BTREE_NODE_LEVEL_MIN;
        // go through all non leap levels
        for _ in BTREE_NODE_LEVEL_MIN..self.get_root_level() {
            let node = path.get_nonroot_node(level);
            if node.has_free_slots() {
                path.set_op(level, BtreeMapOp::Insert);
                return Ok(level);
            }

            // if no free slots found at this level, we check parent
            let parent = self.get_node(path, level + 1);
            let pindex = path.get_index(level + 1);

            // left sibling
            if pindex > 0 {
                let sib_val = parent.get_val(pindex - 1);
                #[cfg(not(feature = "sync-api"))]
                let sib_node = self.get_from_nodes(sib_val).await?;
                #[cfg(feature = "sync-api")]
                let sib_node = self.get_from_nodes(sib_val)?;
                if sib_node.has_free_slots() {
                    path.set_sib_node(level, sib_node);
                    path.set_op(level, BtreeMapOp::CarryLeft);
                    return Ok(level);
                }
            }

            // right sibling
            if pindex < parent.get_nchild() - 1 {
                let sib_val = parent.get_val(pindex + 1);
                #[cfg(not(feature = "sync-api"))]
                let sib_node = self.get_from_nodes(sib_val).await?;
                #[cfg(feature = "sync-api")]
                let sib_node = self.get_from_nodes(sib_val)?;
                if sib_node.has_free_slots() {
                    path.set_sib_node(level, sib_node);
                    path.set_op(level, BtreeMapOp::CarryRight);
                    return Ok(level);
                }
            }

            // split
            // both left and right sibling has no free slots
            // prepare a new node and split myself
            let seq = self.prepare_seq(path, level);
            let node = self.get_new_node(seq)?;
            node.init(0, level, 0);
            path.set_sib_node(level, node);
            path.set_op(level, BtreeMapOp::Split);

            level += 1;
        }

        // root node
        let root = self.get_root_node();
        if root.has_free_slots() {
            path.set_op(level, BtreeMapOp::Insert);
            return Ok(level);
        }

        // grow
        let seq = self.prepare_seq(path, level);
        let node = self.get_new_node(seq)?;
        node.init(0, level, 0);
        path.set_sib_node(level, node);
        path.set_op(level, BtreeMapOp::Grow);

        level += 1;
        path.set_op(level, BtreeMapOp::Insert);
        Ok(level)
    }

    fn commit_insert(&self, path: &BtreePath<'_, K, V>, key: K, val: V, maxlevel: usize) {

        let mut _key = key;
        let mut _val = val;

        for level in BTREE_NODE_LEVEL_MIN..=maxlevel {
            self.do_op(path, level, &mut _key, &mut _val);
        }

        self.set_dirty();
    }

    #[maybe_async::maybe_async]
    async fn prepare_delete(&self, path: &BtreePath<'a, K, V>) -> Result<BtreeLevel> {
        let mut level = BTREE_NODE_LEVEL_MIN;
        let mut dindex = path.get_index(level);
        for _ in BTREE_NODE_LEVEL_MIN..self.get_root_level() {
            let node = path.get_nonroot_node(level);
            path.set_old_seq(level, node.get_val(dindex));

            if node.is_overflowing() {
                path.set_op(level, BtreeMapOp::Delete);
                return Ok(level);
            }

            let parent = self.get_node(path, level + 1);
            let pindex = path.get_index(level + 1);
            dindex = pindex;

            // left sibling
            if pindex > 0 {
                let sib_val = parent.get_val(pindex - 1);
                #[cfg(not(feature = "sync-api"))]
                let sib_node = self.get_from_nodes(sib_val).await?;
                #[cfg(feature = "sync-api")]
                let sib_node = self.get_from_nodes(sib_val)?;
                if sib_node.is_overflowing() {
                    path.set_sib_node(level, sib_node);
                    path.set_op(level, BtreeMapOp::BorrowLeft);
                    return Ok(level);
                } else {
                    path.set_sib_node(level, sib_node);
                    path.set_op(level, BtreeMapOp::ConcatLeft);
                }
            } else if pindex < parent.get_nchild() - 1 {
                // right sibling
                let sib_val = parent.get_val(pindex + 1);
                #[cfg(not(feature = "sync-api"))]
                let sib_node = self.get_from_nodes(sib_val).await?;
                #[cfg(feature = "sync-api")]
                let sib_node = self.get_from_nodes(sib_val)?;
                if sib_node.is_overflowing() {
                    path.set_sib_node(level, sib_node);
                    path.set_op(level, BtreeMapOp::BorrowRight);
                    return Ok(level);
                } else {
                    path.set_sib_node(level, sib_node);
                    path.set_op(level, BtreeMapOp::ConcatRight);
                    dindex = pindex + 1;
                }
            } else {
                // no siblings, the only child of the root node
                if node.get_nchild() - 1 <= self.get_root_node().get_capacity() {
                    path.set_op(level, BtreeMapOp::Shrink);
                    level += 1;
                    path.set_op(level, BtreeMapOp::Nop);
                    // shrink root child
                    let root = self.get_root_node();
                    path.set_old_seq(level, root.get_val(dindex));
                    return Ok(level);
                } else {
                    path.set_op(level, BtreeMapOp::Delete);
                    return Ok(level);
                }
            }
            level += 1;
        }
        // child of the root node is deleted
        path.set_op(level, BtreeMapOp::Delete);

        // shrink root child
        let root = self.get_root_node();
        path.set_old_seq(level, root.get_val(dindex));
        Ok(level)
    }

    fn commit_delete(&self, path: &BtreePath<'_, K, V>, maxlevel: BtreeLevel) {
        let mut key: K = K::default();
        let mut val: V = V::default();
        for level in BTREE_NODE_LEVEL_MIN..=maxlevel {
            self.do_op(path, level, &mut key, &mut val);
        }

        self.set_dirty();
    }

    fn promote_key(&self, path: &BtreePath<'_, K, V>, lvl: usize, key: &K) {
        let mut level = lvl;
        if self.is_nonroot_level(level) {
            loop {
                let index = path.get_index(level);
                // get node @ level
                let node = path.get_nonroot_node(level);
                node.set_key(index, key);
                node.mark_dirty();

                if index != 0 {
                    // break if it is not first one in the node
                    // we don't need to promote upper level any more
                    break;
                }
                level += 1;
                if self.is_root_level(level) {
                    break;
                }
            }
        }

        if self.is_root_level(level) {
            let root = self.get_root_node();
            root.set_key(path.get_index(level), key);
        }
    }

    fn get_next_key(&self, path: &BtreePath<'a, K, V>, minlevel: BtreeLevel) -> Result<K> {
        let mut next_adj = 0;
        let maxlevel = self.get_height() - 1;
        for level in minlevel..=maxlevel {
            let node = if level == maxlevel {
                self.get_root_node()
            } else {
                path.get_nonroot_node(level)
            };
            let index = path.get_index(level) + next_adj;
            if index < node.get_nchild() {
                return Ok(node.get_key(index));
            }
            next_adj = 1;
        }
        Err(Error::new(ErrorKind::NotFound, ""))
    }

    // test if map is dirty, expose to crate
    pub(crate) fn dirty(&self) -> bool {
        self.is_dirty()
    }

    pub(crate) fn lookup_dirty(&self) -> Vec<BtreeNodeRef<'a, K, V>> {
        let mut v = Vec::new();
        for (_, n) in self.nodes.borrow().iter() {
            if n.is_dirty() {
                v.push(n.clone());
            }
        }
        v
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn assign(&self, key: K, newval: V, meta_node: Option<BtreeNodeRef<'_, K, V>>) -> Result<()> {

        let (search_key, level, is_meta) = if let Some(node) = meta_node {
            // if this is meta node, we use node key as search key to search in parent node
            let node_key = node.get_key(0);
            let node_level = node.get_level();
            (node_key, node_level, true)
        } else {
            (key, BTREE_NODE_LEVEL_DATA, false)
        };
        debug!("assign - key: {key}, newval: {newval}, is_meta: {is_meta}");
        debug!("search at parent level: {}, search_key: {search_key}", level + 1);

        let path = BtreePath::new();
        let _ = self.do_lookup(&path, &search_key, level + 1).await?;
        let parent = self.get_node(&path, level + 1);
        let pindex = path.get_index(level + 1);

        if is_meta {
            // get back oldkey
            let oldval = parent.get_val(pindex);
            debug!("assign - get back meta node oldval {oldval}");

            let mut list = self.nodes.borrow_mut();
            // remove node from list via old temp val
            if let Some(node) = list.remove(&oldval) {
                // update node id
                node.set_id(newval);
                // insert back with new val
                list.insert(newval, node);
            } else {
                panic!("old value {} is not on nodes list, THIS SHOULD NOT HAPPEN", oldval);
            }
        }

        parent.set_val(pindex, &newval);
        Ok(())
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn propagate(&self, key: K, meta_node: Option<BtreeNodeRef<'_, K, V>>) -> Result<()> {

        let (search_key, level) = if let Some(node) = meta_node {
            // if this is meta node, we use node key as search key to search in parent node
            let node_key = node.get_key(0);
            let node_level = node.get_level();
            (node_key, node_level)
        } else {
            (key, BTREE_NODE_LEVEL_DATA)
        };
        debug!("propagate - key: {key}, search at parent level: {}, search_key: {search_key}", level + 1);

        let path = BtreePath::new();
        let _ = self.do_lookup(&path, &search_key, level + 1).await?;

        let mut level = level + 1;
        while level < self.get_height() - 1 {
            let node = self.get_node(&path, level);
            node.mark_dirty();
            level += 1;
        }
        Ok(())
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn mark(&self, key: K, level: usize) -> Result<()> {
        let path = BtreePath::new();

        let val = self.do_lookup(&path, &key, level + 1).await?;

        let node = self.get_from_nodes(val).await?;

        node.mark_dirty();
        self.set_dirty();
        Ok(())
    }

    pub(crate) fn new(data: &[u8], meta_block_size: usize, block_loader: L) -> Self {
        let mut v = Vec::with_capacity(data.len());
        v.extend_from_slice(data);
        Self {
            #[cfg(feature = "rc")]
            root: Rc::new(Box::new(BtreeNode::<K, V>::from_slice(&v))),
            #[cfg(feature = "arc")]
            root: Arc::new(Box::new(BtreeNode::<K, V>::from_slice(&v))),
            data: v,
            #[cfg(feature = "rc")]
            nodes: RefCell::new(HashMap::new()),
            #[cfg(feature = "rc")]
            last_seq: RefCell::new(V::invalid_value()),
            #[cfg(feature = "rc")]
            dirty: RefCell::new(false),
            #[cfg(feature = "arc")]
            nodes: AtomicRefCell::new(HashMap::new()),
            #[cfg(feature = "arc")]
            last_seq: Arc::new(AtomicU64::new(Into::<u64>::into(V::invalid_value()))),
            #[cfg(feature = "arc")]
            dirty: Arc::new(AtomicBool::new(false)),
            meta_block_size: meta_block_size,
            block_loader: block_loader,
        }
    }

    pub(crate) fn get_stat(&self) -> BMapStat {
        let mut l1 = 0;
        for (_, n) in self.nodes.borrow().iter() {
            // count all level 1
            if n.get_level() == 1 {
                l1 += 1;
            }
        }

        BMapStat {
            btree: true,
            level: self.root.get_level(),
            #[cfg(feature = "rc")]
            dirty: *self.dirty.borrow(),
            #[cfg(feature = "arc")]
            dirty: self.dirty.load(Ordering::SeqCst),
            meta_block_size: self.meta_block_size,
            nodes_total: self.nodes.borrow().len() + 1, // plus root node
            nodes_l1: l1,
        }
    }

    #[maybe_async::maybe_async]
    // for delete
    pub(crate) async fn delete_check_and_gather(&self, key: K, v: &mut Vec<(K, V)>) -> Result<bool> {
        let node;
        let root = self.get_root_node();
        let root_capacity = root.get_capacity();
        match self.get_height() {
            2 => {
                // if height is 2, we check root node
                node = root;
            },
            3 => {
                let nchild = root.get_nchild();
                if nchild > 1 {
                    return Ok(false);
                }
                // get back only child node, wee need to check it
                let val = root.get_val(nchild - 1);
                node = self.get_from_nodes(val).await?;
            },
            _ => {
                return Ok(false);
            },
        }

        // convert all to u64 to compare
        let nchild = node.get_nchild();
        let maxkey = node.get_key(nchild - 1).into();
        let next_maxkey = if nchild > 1 {
            node.get_key(nchild - 2).into()
        } else {
            0
        };

        // gather data to output vec
        if (maxkey == key.into()) && (next_maxkey < root_capacity as u64) {
            // because we shrink to root node, we copy only size min than root capacity
            for i in 0..std::cmp::min(root_capacity, nchild) {
                let key = node.get_key(i);
                let val = node.get_val(i);
                v.push((key, val));
            }
            return Ok(true);
        }
        return Ok(false);
    }
}

// all op_* functions
impl<'a, K, V, L> BtreeMap<'a, K, V, L>
    where
        K: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        V: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        K: From<V> + Into<u64>,
        V: From<K> + NodeValue<V> + From<u64> + Into<u64>,
        L: BlockLoader<V>,
{
    fn op_insert(&self, path: &BtreePath<'_, K, V>, level: BtreeLevel, key: &mut K, val: &mut V) {
        if self.is_nonroot_level(level) {
            // non root node
            let node = path.get_nonroot_node(level);

            node.insert(path.get_index(level), key, val);
            node.mark_dirty();

            if path.get_index(level) == 0 {
                let node_key = node.get_key(0);
                self.promote_key(path, level + 1, &node_key);
            }
        } else {
            // root node
            let root = self.get_root_node();
            root.insert(path.get_index(level), key, val);
        }
    }

    fn op_grow(&self, path: &BtreePath<K, V>, level: BtreeLevel, key: &mut K, val: &mut V) {
        let child = path.get_sib_node(level);
        let root = self.get_root_node();

        let n = root.get_nchild();

        {

        let root_ref = &root.clone();
        let child_ref = &child.clone();
        BtreeNode::move_right(root_ref, child_ref, n);

        }

        child.mark_dirty();

        root.set_level(level + 1);

        let sib_node = path.get_sib_node(level);
        path.set_nonroot_node(level, sib_node);
        path.set_sib_node_none(level);

        self.op_insert(path, level, key, val);

        *key = child.get_key(0);
        *val = path.get_new_seq(level);
    }

    fn op_split(&self, path: &BtreePath<K, V>, level: BtreeLevel, key: &mut K, val: &mut V) {
        let node = path.get_nonroot_node(level);
        let right = path.get_sib_node(level);
        let nchild = node.get_nchild();
        let mut mv = false;

        let mut n = (nchild + 1) / 2;
        if n > nchild - path.get_index(level) {
            n -= 1;
            mv = true;
        }

        {

        let node_ref = &node.clone();
        let right_ref = &right.clone();
        BtreeNode::move_right(node_ref, right_ref, n);

        }

        node.mark_dirty();
        right.mark_dirty();

        if mv {
            let idx = path.get_index(level) - node.get_nchild();
            path.set_index(level, idx);

            right.insert(idx, key, val);

            *key = right.get_key(0);
            *val = path.get_new_seq(level);

            let sib_node = path.get_sib_node(level);
            path.set_nonroot_node(level, sib_node);
            path.set_sib_node_none(level);
        } else {
            self.op_insert(path, level, key, val);

            *key = right.get_key(0);
            *val = path.get_new_seq(level);

            path.set_sib_node_none(level);
        }

        path.set_index(level + 1, path.get_index(level + 1) + 1);
    }

    fn op_carry_left(&self, path: &BtreePath<K, V>, level: BtreeLevel, key: &mut K, val: &mut V) {
        let node = path.get_nonroot_node(level);
        let left = path.get_sib_node(level);
        let nchild = node.get_nchild();
        let lnchild = left.get_nchild();
        let mut mv = false;

        let mut n = (nchild + lnchild + 1) / 2 - lnchild;
        if n > path.get_index(level) {
            n -= 1;
            mv = true;
        }

        {

        let left_ref = &left.clone();
        let node_ref = &node.clone();
        BtreeNode::move_left(left_ref, node_ref, n);

        }

        left.mark_dirty();
        node.mark_dirty();

        let node_key = node.get_key(0);
        self.promote_key(path, level + 1, &node_key);

        if mv {
            let sib_node = path.get_sib_node(level);
            path.set_nonroot_node(level, sib_node);
            path.set_sib_node_none(level);
            path.set_index(level, path.get_index(level) + lnchild);
            path.set_index(level + 1, path.get_index(level + 1) - 1);
        } else {
            path.set_sib_node_none(level);
            path.set_index(level, path.get_index(level) - n);
        }

        self.op_insert(path, level, key, val);
    }

    fn op_carry_right(&self, path: &BtreePath<K, V>, level: BtreeLevel, key: &mut K, val: &mut V) {
        let node = path.get_nonroot_node(level);
        let right = path.get_sib_node(level);
        let nchild = node.get_nchild();
        let rnchild = right.get_nchild();
        let mut mv = false;

        let mut n = (nchild + rnchild + 1) / 2 - rnchild;
        if n > nchild - path.get_index(level) {
            n -= 1;
            mv = true;
        }

        {

        let node_ref = &node.clone();
        let right_ref = &right.clone();
        BtreeNode::move_right(node_ref, right_ref, n);

        }

        node.mark_dirty();
        right.mark_dirty();

        path.set_index(level + 1, path.get_index(level + 1) + 1);
        let node_key = right.get_key(0);
        self.promote_key(path, level + 1, &node_key);
        path.set_index(level + 1, path.get_index(level + 1) - 1);

        if mv {
            let sib_node = path.get_sib_node(level);
            path.set_sib_node(level, sib_node);
            path.set_sib_node_none(level);
            let nchild = node.get_nchild();
            path.set_index(level, path.get_index(level) - nchild);
            path.set_index(level + 1, path.get_index(level + 1) + 1);
        } else {
            path.set_sib_node_none(level);
        }

        self.op_insert(path, level, key, val);
    }

    fn op_concat_left(&self, path: &BtreePath<K, V>, level: BtreeLevel, key: &mut K, val: &mut V) {
        self.op_delete(path, level, key, val);

        let node = path.get_nonroot_node(level);
        let left = path.get_sib_node(level);

        let n = node.get_nchild();

        {

        let left_ref = &left.clone();
        let node_ref = &node.clone();
        BtreeNode::move_left(left_ref, node_ref, n);

        }

        left.mark_dirty();

        let _ = self.remove_from_nodes(node);
        let sib_node = path.get_sib_node(level);
        path.set_nonroot_node(level, sib_node);
        path.set_sib_node_none(level);
        path.set_index(level, path.get_index(level) + left.get_nchild());
    }

    fn op_concat_right(&self, path: &BtreePath<K, V>, level: BtreeLevel, key: &mut K, val: &mut V) {
        self.op_delete(path, level, key, val);

        let node = path.get_nonroot_node(level);
        let right = path.get_sib_node(level);

        let n = right.get_nchild();

        {

        let node_ref = &node.clone();
        let right_ref = &right.clone();
        BtreeNode::move_left(node_ref, right_ref, n);

        }

        node.mark_dirty();

        let _ = self.remove_from_nodes(path.get_sib_node(level));
        path.set_sib_node_none(level);
        path.set_index(level + 1, path.get_index(level + 1) + 1);
    }

    fn op_borrow_left(&self, path: &BtreePath<K, V>, level: BtreeLevel, key: &mut K, val: &mut V) {
        self.op_delete(path, level, key, val);

        let node = path.get_nonroot_node(level);
        let left = path.get_sib_node(level);
        let nchild = node.get_nchild();
        let lnchild = left.get_nchild();

        let n = (nchild + lnchild) / 2 - nchild;

        {

        let left_ref = &left.clone();
        let node_ref = &node.clone();
        BtreeNode::move_right(left_ref, node_ref, n);

        }

        node.mark_dirty();
        left.mark_dirty();

        let node_key = node.get_key(0);
        self.promote_key(path, level + 1, &node_key);

        path.set_sib_node_none(level);
        path.set_index(level, path.get_index(level) + n);
    }

    fn op_borrow_right(&self, path: &BtreePath<K, V>, level: BtreeLevel, key: &mut K, val: &mut V) {
        self.op_delete(path, level, key, val);

        let node = path.get_nonroot_node(level);
        let right = path.get_sib_node(level);
        let nchild = node.get_nchild();
        let rnchild = right.get_nchild();

        let n = (nchild + rnchild) / 2 - nchild;

        {

        let node_ref = &node.clone();
        let right_ref = &right.clone();
        BtreeNode::move_left(node_ref, right_ref, n);

        }

        node.mark_dirty();
        right.mark_dirty();

        path.set_index(level + 1, path.get_index(level + 1) + 1);
        let node_key = right.get_key(0);
        self.promote_key(path, level + 1, &node_key);
        path.set_index(level + 1, path.get_index(level + 1) - 1);

        path.set_sib_node_none(level);
    }

    fn op_delete(&self, path: &BtreePath<K, V>, level: BtreeLevel, key: &mut K, val: &mut V) {
        if self.is_nonroot_level(level) {
            let node = path.get_nonroot_node(level);
            node.delete(path.get_index(level), key, val);
            node.mark_dirty();
            if path.get_index(level) == 0 {
                let node_key = node.get_key(0);
                self.promote_key(path, level + 1, &node_key);
            }
        } else {
            let root = self.get_root_node();
            root.delete(path.get_index(level), key, val);
        }
    }

    fn op_shrink(&self, path: &BtreePath<K, V>, level: BtreeLevel, key: &mut K, val: &mut V) {
        self.op_delete(path, level, key, val);

        let root = self.get_root_node();
        let child = path.get_nonroot_node(level);

        let mut _key = K::default();
        let mut _val = V::default();
        root.delete(0, &mut _key, &mut _val);
        root.set_level(level);
        let n = child.get_nchild();

        {

        let root_ref = &root.clone();
        let child_ref = &child.clone();
        BtreeNode::move_left(root_ref, child_ref, n);

        }

        let _ = self.remove_from_nodes(path.get_nonroot_node(level));
        path.set_nonroot_node_none(level);
    }

    fn op_nop(&self, _path: &BtreePath<K, V>, _level: BtreeLevel, _key: &mut K, _val: &mut V) {
        // do nothing
    }
}

// all up level api
impl<'a, K, V, L> VMap<K, V> for BtreeMap<'a, K, V, L>
    where
        K: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        V: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        K: From<V> + Into<u64>,
        V: From<K> + NodeValue<V> + From<u64> + Into<u64>,
        L: BlockLoader<V>,
{
    #[maybe_async::maybe_async]
    async fn lookup(&self, key: K, level: usize) -> Result<V> {
        let path = BtreePath::new();
        let val = self.do_lookup(&path, &key, level).await?;
        Ok(val)
    }

    #[maybe_async::maybe_async]
    async fn lookup_contig(&self, key: K, maxblocks: usize) -> Result<(V, usize)> {
        let level = BTREE_NODE_LEVEL_MIN;
        let path = BtreePath::new();
        let value = self.do_lookup(&path, &key, level).await?;

        if maxblocks == 1 {
            return Ok((value, 1));
        }

        let mut count = 1;
        let maxlevel = self.get_height() - 1;
        let mut node = path.get_nonroot_node(level);
        let mut index = path.get_index(level) + 1;
        loop {
            while index < node.get_nchild() {
                if count == maxblocks {
                    return Ok((value, count))
                }

                let mut _key = key; _key += count as u64;
                if node.get_key(index) != _key {
                    // early break at first non continue key
                    return Ok((value, count))
                }

                count += 1;
                index += 1;
            }

            if level == maxlevel {
                break;
            }

            // lookup right sibling node
            // get parent node
            let p_node = self.get_node(&path, level + 1);
            let p_index = path.get_index(level + 1) + 1;
            let mut _key = key; _key += count as u64;
            if p_index >= p_node.get_nchild() || p_node.get_key(p_index) != _key {
                return Ok((value, count));
            }
            let v = p_node.get_val(p_index);
            path.set_index(level + 1, p_index);
            path.set_nonroot_node_none(level);

            // get sibling node for next looop
            node = self.get_from_nodes(v).await?;
            path.set_nonroot_node(level, node.clone());
            index = 0;
            path.set_index(level, index);
        }
        Ok((value, count))
    }

    #[maybe_async::maybe_async]
    async fn insert(&self, key: K, val: V) -> Result<()> {
        let path = BtreePath::new();
        let res = self.do_lookup(&path, &key, BTREE_NODE_LEVEL_MIN).await;
        match res {
            Ok(_) => {
                return Err(Error::new(ErrorKind::AlreadyExists, ""));
            },
            Err(e) => {
                if e.kind() != ErrorKind::NotFound {
                    return Err(e);
                }
            },
        }

        // key not found
        let level = self.prepare_insert(&path).await?;
        self.commit_insert(&path, key, val, level);
        Ok(())
    }

    #[maybe_async::maybe_async]
    async fn delete(&self, key: K) -> Result<()> {
        let path = BtreePath::new();
        let res = self.do_lookup(&path, &key, BTREE_NODE_LEVEL_MIN).await;
        match res {
            Ok(_) => {}, // do nothing if found
            Err(e) => { return Err(e); }, // return any errors
        }

        let level = self.prepare_delete(&path).await?;
        self.commit_delete(&path, level);
        Ok(())
    }

    #[maybe_async::maybe_async]
    async fn seek_key(&self, start: K) -> Result<K> {
        let path = BtreePath::new();
        let res = self.do_lookup(&path, &start, BTREE_NODE_LEVEL_MIN).await;
        match res {
            Ok(_) => {
                return Ok(start);
            },
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                   return self.get_next_key(&path, BTREE_NODE_LEVEL_MIN);
                }
                return Err(e);
            }
        }
    }

    #[maybe_async::maybe_async]
    async fn last_key(&self) -> Result<K> {
        let path = BtreePath::new();
        self.do_lookup_last(&path).await
    }
}
