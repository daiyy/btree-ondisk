use std::fmt;
use std::collections::HashMap;
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
use std::io::{Error, ErrorKind, Result};
use log::{warn, debug};
use crate::node::*;
use crate::VMap;
use crate::bmap::BMapStat;
use crate::{NodeValue, BlockLoader};
use crate::DEFAULT_CACHE_UNLIMITED;

pub(crate) type BtreeLevel = usize;
#[cfg(feature = "rc")]
pub(crate) type BtreeNodeRef<'a, K, V, P> = Rc<Box<BtreeNode<'a, K, V, P>>>;
#[cfg(feature = "arc")]
pub(crate) type BtreeNodeRef<'a, K, V, P> = Arc<Box<BtreeNode<'a, K, V, P>>>;

#[derive(Clone)]
pub struct BtreeNodeDirty<'a, K, V, P>(pub(crate) BtreeNodeRef<'a, K, V, P>);

impl<'a, K, V, P> BtreeNodeDirty<'a, K, V, P>
    where
        K: Copy + fmt::Display + std::cmp::PartialOrd,
        V: Copy + fmt::Display + NodeValue,
        P: Copy + fmt::Display + NodeValue,
{
    pub(crate) fn clone_node_ref(&self) -> BtreeNodeRef<'a, K, V, P> {
        self.0.clone()
    }

    pub fn size(&self) -> usize {
        // rc/arc -> box -> inner slice -> len
        self.0.as_ref().as_ref().as_ref().len()
    }

    pub fn as_slice(&self) -> &[u8] {
        // rc/arc -> box -> inner slice
        self.0.as_ref().as_ref().as_ref()
    }

    pub fn clear_dirty(&self) {
        self.0.clear_dirty()
    }
}

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

struct BtreePathLevel<'a, K, V, P> {
    node: Option<BtreeNodeRef<'a, K, V, P>>,
    sib_node: Option<BtreeNodeRef<'a, K, V, P>>,
    index: usize,
    oldseq: P,
    newseq: P,
    op: BtreeMapOp,
}

pub struct BtreePath<'a, K, V, P> {
    #[cfg(feature = "rc")]
    levels: Vec<RefCell<BtreePathLevel<'a, K, V, P>>>,
    #[cfg(feature = "arc")]
    levels: Vec<AtomicRefCell<BtreePathLevel<'a, K, V, P>>>,
}

impl<'a, K, V, P> BtreePath<'a, K, V, P>
    where
        V: Copy + NodeValue,
        P: Copy + NodeValue,
{
    pub fn new() -> Self {
        let mut l = Vec::with_capacity(BTREE_NODE_LEVEL_MAX);
        for _ in 0..BTREE_NODE_LEVEL_MAX {
            #[cfg(feature = "rc")]
            l.push(RefCell::new(BtreePathLevel {
                node: None,
                sib_node: None,
                index: 0,
                oldseq: P::invalid_value(),
                newseq: P::invalid_value(),
                op: BtreeMapOp::Nop,
            }));
            #[cfg(feature = "arc")]
            l.push(AtomicRefCell::new(BtreePathLevel {
                node: None,
                sib_node: None,
                index: 0,
                oldseq: P::invalid_value(),
                newseq: P::invalid_value(),
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
    pub fn get_nonroot_node(&self, level: usize) -> BtreeNodeRef<'a, K, V, P> {
        self.levels[level].borrow().node.as_ref().unwrap().clone()
    }

    #[inline]
    pub fn set_nonroot_node(&self, level: usize, node: BtreeNodeRef<'a, K, V, P>) {
        self.levels[level].borrow_mut().node = Some(node);
    }

    #[inline]
    pub fn set_nonroot_node_none(&self, level: usize) {
        self.levels[level].borrow_mut().node = None;
    }

    #[inline]
    pub fn get_sib_node(&self, level: usize) -> BtreeNodeRef<'a, K, V, P> {
        self.levels[level].borrow().sib_node.as_ref().unwrap().clone()
    }

    #[inline]
    pub fn set_sib_node(&self, level: usize, node: BtreeNodeRef<'a, K, V, P>) {
        self.levels[level].borrow_mut().sib_node = Some(node);
    }

    #[inline]
    pub fn set_sib_node_none(&self, level: usize) {
        self.levels[level].borrow_mut().sib_node = None;
    }

    #[inline]
    pub fn get_new_seq(&self, level: usize) -> P {
        self.levels[level].borrow().newseq
    }

    #[inline]
    pub fn set_new_seq(&self, level: usize, seq: P) {
        self.levels[level].borrow_mut().newseq = seq;
    }

    #[inline]
    pub fn get_old_seq(&self, level: usize) -> P {
        self.levels[level].borrow().oldseq
    }

    #[inline]
    pub fn set_old_seq(&self, level: usize, seq: P) {
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
pub struct BtreeMap<'a, K, V, P, L: BlockLoader<P>> {
    pub data: Vec<u8>,
    pub root: BtreeNodeRef<'a, K, V, P>,
    pub nodes: RefCell<HashMap<P, BtreeNodeRef<'a, K, V, P>>>, // list of btree node in memory
    pub last_seq: RefCell<P>,
    pub dirty: RefCell<bool>,
    pub meta_block_size: usize,
    pub cache_limit: RefCell<usize>,
    pub block_loader: L,
}

#[cfg(feature = "arc")]
pub struct BtreeMap<'a, K, V, P, L: BlockLoader<P>> {
    pub data: Vec<u8>,
    pub root: BtreeNodeRef<'a, K, V, P>,
    pub nodes: AtomicRefCell<HashMap<P, BtreeNodeRef<'a, K, V, P>>>,
    pub last_seq: Arc<AtomicU64>,
    pub dirty: Arc<AtomicBool>,
    pub meta_block_size: usize,
    pub cache_limit: Arc<AtomicUsize>,
    pub block_loader: L,
}

impl<'a, K, V, P, L> fmt::Display for BtreeMap<'a, K, V, P, L>
    where
        K: Copy + fmt::Display + std::cmp::PartialOrd,
        V: Copy + fmt::Display + NodeValue,
        P: Copy + fmt::Display + NodeValue,
        L: BlockLoader<P>,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.root)?;
        for (_, node) in self.nodes.borrow().iter() {
            let n: BtreeNodeRef<'a, K, V, P> = node.clone();
            write!(f, "{}", n)?;
        }
        write!(f, "")
    }
}

impl<'a, K, V, P, L> BtreeMap<'a, K, V, P, L>
    where
        K: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        V: Copy + Default + std::fmt::Display + NodeValue,
        K: Into<u64>,
        P: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        P: NodeValue + From<u64> + Into<u64>,
        L: BlockLoader<P>,
{
    #[inline]
    fn get_root_node(&self) -> BtreeNodeRef<'a, K, V, P> {
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
    fn get_node(&self, path: &BtreePath<'a, K, V, P>, level: usize) -> BtreeNodeRef<'a, K, V, P> {
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
    fn get_next_seq(&self) -> P {
        *self.last_seq.borrow_mut() += 1;
        *self.last_seq.borrow()
    }

    #[cfg(feature = "arc")]
    #[inline]
    fn get_next_seq(&self) -> P {
        let old_value = self.last_seq.fetch_add(1, Ordering::SeqCst);
        From::<u64>::from(old_value + 1)
    }

    #[inline]
    fn prepare_seq(&self, path: &BtreePath<K, V, P>, level: usize) -> P {
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
        self.evict();
    }

    #[cfg(feature = "arc")]
    #[inline]
    pub(crate) fn clear_dirty(&self) {
        self.dirty.store(false, Ordering::SeqCst);
        self.evict();
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
        self.evict();
    }

    #[cfg(feature = "arc")]
    #[inline]
    pub(crate) fn set_cache_limit(&self, limit: usize) {
        self.cache_limit.store(limit, Ordering::SeqCst);
        self.evict();
    }

    #[inline]
    async fn meta_block_loader(&self, id: P, buf: &mut [u8]) -> Result<Vec<(P, Vec<u8>)>> {
        self.block_loader.read(id, buf, self.get_userdata()).await
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn get_from_nodes(&self, id: &P) -> Result<BtreeNodeRef<'a, K, V, P>> {
        let list = self.nodes.borrow();
        if let Some(node) = list.get(id) {
            return Ok(node.clone());
        }
        drop(list);

        if let Some(mut node) = BtreeNode::<K, V, P>::new_with_id(self.meta_block_size, id) {
            #[cfg(not(feature = "sync-api"))]
            let more = self.meta_block_loader(*id, node.as_mut()).await?;
            #[cfg(all(feature = "sync-api", feature = "futures-runtime"))]
            let more = futures::executor::block_on(async {
                self.meta_block_loader(*id, node.as_mut()).await
            })?;
            #[cfg(all(feature = "sync-api", feature = "tokio-runtime"))]
            let more = tokio::runtime::Handle::current().block_on(async {
                self.meta_block_loader(*id, node.as_mut()).await
            })?;
            node.do_update();
            #[cfg(feature = "rc")]
            let n = Rc::new(Box::new(node));
            #[cfg(feature = "arc")]
            let n = Arc::new(Box::new(node));
            let mut list = self.nodes.borrow_mut();
            list.insert(*id, n.clone());
            drop(list);

            // if we have more to load
            for (i, data) in more.into_iter() {
                assert!(data.len() == self.meta_block_size);
                if *id == i {
                    // in case we go duplicated meta block
                    // skip it
                    continue;
                }
                if let Some(node) = BtreeNode::<K, V, P>::copy_from_slice(i, &data) {
                    #[cfg(feature = "rc")]
                    let n = Rc::new(Box::new(node));
                    #[cfg(feature = "arc")]
                    let n = Arc::new(Box::new(node));
                    let mut list = self.nodes.borrow_mut();
                    list.insert(i, n.clone());
                    drop(list);
                } else {
                    return Err(Error::new(ErrorKind::OutOfMemory, "failed to allocate memory for btree node"));
                }
            }
            return Ok(n);
        }
        return Err(Error::new(ErrorKind::OutOfMemory, "failed to allocate memory for btree node"));
    }

    pub(crate) fn get_new_node(&self, id: &P, level: usize) -> Result<BtreeNodeRef<'a, K, V, P>> {
        let mut list = self.nodes.borrow_mut();
        if let Some(mut node) = BtreeNode::<K, V, P>::new_with_id(self.meta_block_size, id) {
            if level == BTREE_NODE_LEVEL_LEAF {
                node.do_reinit::<V>();
            } else {
                node.do_reinit::<P>();
            }
            #[cfg(feature = "rc")]
            let n = Rc::new(Box::new(node));
            #[cfg(feature = "arc")]
            let n = Arc::new(Box::new(node));
            if let Some(_oldnode) = list.insert(*id, n.clone()) {
                panic!("value {} is already in nodes list", id);
            }
            return Ok(n);
        }
        return Err(Error::new(ErrorKind::OutOfMemory, "failed to allocate memory for btree node"));
    }

    fn remove_from_nodes(&self, node: BtreeNodeRef<K, V, P>) -> Result<()> {
        let n = self.nodes.borrow_mut().remove(node.id());
        assert!(n.is_some());
        Ok(())
    }

    fn do_op(&self, path: &BtreePath<K, V, P>, level: BtreeLevel, key: &mut K, val: &mut V, pid: &mut P) {
        match path.get_op(level) {
            BtreeMapOp::Insert => self.op_insert(path, level, key, val, pid),
            BtreeMapOp::Grow => self.op_grow(path, level, key, val, pid),
            BtreeMapOp::Split => self.op_split(path, level, key, val, pid),
            BtreeMapOp::CarryLeft => self.op_carry_left(path, level, key, val, pid),
            BtreeMapOp::CarryRight => self.op_carry_right(path, level, key, val, pid),
            BtreeMapOp::ConcatLeft => self.op_concat_left(path, level, key, val, pid),
            BtreeMapOp::ConcatRight => self.op_concat_right(path, level, key, val, pid),
            BtreeMapOp::BorrowLeft => self.op_borrow_left(path, level, key, val, pid),
            BtreeMapOp::BorrowRight => self.op_borrow_right(path, level, key, val, pid),
            BtreeMapOp::Delete => self.op_delete(path, level, key, val, pid),
            BtreeMapOp::Shrink => self.op_shrink(path, level, key, val, pid),
            BtreeMapOp::Nop => self.op_nop(path, level, key, val, pid),
        }
    }

    #[maybe_async::maybe_async]
    async fn do_lookup(&self, path: &BtreePath<'a, K, V, P>, key: &K, minlevel: usize) -> Result<(V, P)> {
        let root = self.get_root_node();
        let mut level = root.get_level();
        if level < minlevel || root.get_nchild() <= 0 {
            return Err(Error::new(ErrorKind::NotFound, "btree root node not eligible for lookup"));
        }

        let mut value = V::invalid_value();
        let mut next_level_id = P::invalid_value();

        let (mut found, mut index) = root.lookup(key);
        // set invalid value when returned index out of root node capacity
        if index > root.get_capacity() - 1 {
            // do nothing, next_level_id is init with invalid value
        } else {
            if level == BTREE_NODE_LEVEL_MIN {
                value = *root.get_val::<V>(index);
            } else {
                next_level_id = *root.get_val::<P>(index);
            }
        }

        path.set_index(level, index);

        level -= 1;
        while level >= minlevel {
            let node = self.get_from_nodes(&next_level_id).await?;
            if !found {
                (found, index) = node.lookup(key);
            } else {
                index = 0;
            }

            if node.is_leaf() {
                if index < node.get_capacity() {
                    value = *node.get_val::<V>(index);
                } else {
                    if found || level != BTREE_NODE_LEVEL_MIN {
                        warn!("index {} - level {} - found {}", index, level, found);
                    }
                    value = V::invalid_value();
                }
            } else {
                if index < node.get_capacity() {
                    next_level_id = *node.get_val::<P>(index);
                } else {
                    if found || level != BTREE_NODE_LEVEL_MIN {
                        warn!("index {} - level {} - found {}", index, level, found);
                    }
                    next_level_id = P::invalid_value();
                }
            }

            path.set_nonroot_node(level, node);
            path.set_index(level, index);
            level -= 1;
        }

        if !found {
            return Err(Error::new(ErrorKind::NotFound, "key not found through btree node lookup"));
        }

        Ok((value, next_level_id))
    }

    #[maybe_async::maybe_async]
    async fn do_lookup_last(&self, path: &BtreePath<'a, K, V, P>) -> Result<K> {
        let mut node = self.get_root_node();
        let nchild = node.get_nchild();
        if nchild == 0 {
            return Err(Error::new(ErrorKind::NotFound, "btree root node has no children"));
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
        Ok(*key)
    }

    #[maybe_async::maybe_async]
    async fn prepare_insert(&self, path: &BtreePath<'a, K, V, P>) -> Result<BtreeLevel> {

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
            let node = self.get_new_node(&seq, level)?;
            node.init(level, 0);
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
        let node = self.get_new_node(&seq, level)?;
        node.init(level, 0);
        path.set_sib_node(level, node);
        path.set_op(level, BtreeMapOp::Grow);

        level += 1;
        path.set_op(level, BtreeMapOp::Insert);
        Ok(level)
    }

    fn commit_insert(&self, path: &BtreePath<'_, K, V, P>, key: K, val: V, maxlevel: usize) {

        let mut _key = key;
        let mut _val = val;
        let mut _pid = P::invalid_value();

        for level in BTREE_NODE_LEVEL_MIN..=maxlevel {
            self.do_op(path, level, &mut _key, &mut _val, &mut _pid);
        }

        self.set_dirty();
    }

    #[maybe_async::maybe_async]
    async fn prepare_delete(&self, path: &BtreePath<'a, K, V, P>) -> Result<BtreeLevel> {
        let mut level = BTREE_NODE_LEVEL_MIN;
        let mut dindex = path.get_index(level);
        for _ in BTREE_NODE_LEVEL_MIN..self.get_root_level() {
            let node = path.get_nonroot_node(level);
            path.set_old_seq(level, *node.get_val(dindex));

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
                let root = self.get_root_node();
                let root_capacity = if node.get_level() == BTREE_NODE_LEVEL_LEAF {
                    // if node is leaf and root level is leaf + 1
                    // use v capacity for root to check
                    // if we can shrink leaf node into root node
                    root.get_v_capacity()
                } else {
                    root.get_capacity()
                };
                if node.get_nchild() - 1 <= root_capacity {
                    path.set_op(level, BtreeMapOp::Shrink);
                    level += 1;
                    path.set_op(level, BtreeMapOp::Nop);
                    // shrink root child
                    let root = self.get_root_node();
                    path.set_old_seq(level, *root.get_val(dindex));
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
        path.set_old_seq(level, *root.get_val(dindex));
        Ok(level)
    }

    fn commit_delete(&self, path: &BtreePath<'_, K, V, P>, maxlevel: BtreeLevel) {
        let mut key: K = K::default();
        let mut val: V = V::default();
        let mut pid: P = P::default();
        for level in BTREE_NODE_LEVEL_MIN..=maxlevel {
            self.do_op(path, level, &mut key, &mut val, &mut pid);
        }

        self.set_dirty();
    }

    fn promote_key(&self, path: &BtreePath<'_, K, V, P>, lvl: usize, key: &K) {
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

    fn get_next_key(&self, path: &BtreePath<'a, K, V, P>, minlevel: BtreeLevel) -> Result<K> {
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
                return Ok(*node.get_key(index));
            }
            next_adj = 1;
        }
        Err(Error::new(ErrorKind::NotFound, "key not found through btree get next key"))
    }

    // test if map is dirty, expose to crate
    pub(crate) fn dirty(&self) -> bool {
        self.is_dirty()
    }

    pub(crate) fn lookup_dirty(&self) -> Vec<BtreeNodeRef<'a, K, V, P>> {
        let mut v = Vec::new();
        for (_, n) in self.nodes.borrow().iter() {
            if n.is_dirty() {
                v.push(n.clone());
            }
        }
        // sort by id in asc
        v.sort_by(|a, b|
            a.id().partial_cmp(b.id()).unwrap()
        );
        // then sort by level in desc
        v.sort_by(|a, b|
            b.get_level().partial_cmp(&a.get_level()).unwrap()
        );
        v
    }

    // a simple process to evict node in btree
    pub(crate) fn evict(&self) {
        let limit = self.get_cache_limit();
        if limit == DEFAULT_CACHE_UNLIMITED {
            return;
        }
        let mut nodes: Vec<BtreeNodeRef<'_, K, V, P>> = self.nodes.borrow()
                    .iter()
                    .filter_map(|(key, node)| {
                        let id = node.id();
                        assert!(key == id);
                        if id.is_valid_extern_assign() {
                            Some(node.clone())
                        } else {
                            None
                        }
                    })
                    .collect();

        // sort by id in asc
        nodes.sort_by(|a, b|
            a.id().partial_cmp(b.id()).unwrap()
        );
        // then sort by level in asc
        nodes.sort_by(|a, b|
            a.get_level().partial_cmp(&b.get_level()).unwrap()
        );

        let total = nodes.len();
        if limit >= total {
            return;
        }
        let mut count = total - limit;
        while count > 0 {
            let node = nodes.pop().expect("failed to get evict candidate");
            let id = node.id();
            let n = self.nodes.borrow_mut().remove(&id);
            assert!(n.is_some());
            count -= 1;
        }
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn assign(&self, key: &K, newid: P, meta_node: Option<BtreeNodeRef<'_, K, V, P>>) -> Result<()> {

        let (search_key, level, is_meta) = if let Some(node) = meta_node {
            // if this is meta node, we use node key as search key to search in parent node
            let node_key = node.get_key(0);
            let node_level = node.get_level();
            (*node_key, node_level, true)
        } else {
            (*key, BTREE_NODE_LEVEL_DATA, false)
        };
        debug!("assign - key: {key}, newid: {newid}, is_meta: {is_meta}");
        debug!("search at parent level: {}, search_key: {search_key}", level + 1);

        let path = BtreePath::new();
        let _ = self.do_lookup(&path, &search_key, level + 1).await?;
        let parent = self.get_node(&path, level + 1);
        let pindex = path.get_index(level + 1);

        if is_meta {
            // get back oldkey
            let oldid = parent.get_val::<P>(pindex);
            debug!("assign - get back meta node oldid {oldid}");

            let mut list = self.nodes.borrow_mut();
            // remove node from list via old temp id
            if let Some(node) = list.remove(&oldid) {
                // update node id
                node.set_id(newid);
                // insert back with new val
                list.insert(newid, node);
            } else {
                panic!("old id {} is not on nodes list, THIS SHOULD NOT HAPPEN", oldid);
            }
        }

        parent.set_val::<P>(pindex, &newid);
        Ok(())
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn propagate(&self, key: &K, meta_node: Option<BtreeNodeRef<'_, K, V, P>>) -> Result<()> {

        let (search_key, level) = if let Some(node) = meta_node {
            // if this is meta node, we use node key as search key to search in parent node
            let node_key = node.get_key(0);
            let node_level = node.get_level();
            (*node_key, node_level)
        } else {
            (*key, BTREE_NODE_LEVEL_DATA)
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

    // lite version of propagate only for inner insert and delete at BTREE_NODE_LEVEL_MIN
    pub(crate) fn propagate_lite(&self, path: &BtreePath<'a, K, V, P>) {
        // MIN level node has been marked dirty by insert or delete
        let mut level = BTREE_NODE_LEVEL_MIN + 1;
        while level < self.get_height() - 1 {
            let node = self.get_node(&path, level);
            node.mark_dirty();
            level += 1;
        }
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn mark(&self, key: &K, level: usize) -> Result<()> {
        let path = BtreePath::new();

        let (_, id) = self.do_lookup(&path, key, level + 1).await?;

        let node = self.get_from_nodes(&id).await?;

        node.mark_dirty();
        self.set_dirty();
        Ok(())
    }

    pub(crate) fn new(data: &[u8], meta_block_size: usize, block_loader: L) -> Self {
        let mut v = Vec::with_capacity(data.len());
        v.extend_from_slice(data);
        Self {
            #[cfg(feature = "rc")]
            root: Rc::new(Box::new(BtreeNode::<K, V, P>::from_slice(&v))),
            #[cfg(feature = "arc")]
            root: Arc::new(Box::new(BtreeNode::<K, V, P>::from_slice(&v))),
            data: v,
            #[cfg(feature = "rc")]
            nodes: RefCell::new(HashMap::new()),
            #[cfg(feature = "rc")]
            last_seq: RefCell::new(P::invalid_value()),
            #[cfg(feature = "rc")]
            dirty: RefCell::new(false),
            #[cfg(feature = "arc")]
            nodes: AtomicRefCell::new(HashMap::new()),
            #[cfg(feature = "arc")]
            last_seq: Arc::new(AtomicU64::new(Into::<u64>::into(P::invalid_value()))),
            #[cfg(feature = "arc")]
            dirty: Arc::new(AtomicBool::new(false)),
            meta_block_size: meta_block_size,
            #[cfg(feature = "rc")]
            cache_limit: RefCell::new(DEFAULT_CACHE_UNLIMITED),
            #[cfg(feature = "arc")]
            cache_limit: Arc::new(AtomicUsize::new(DEFAULT_CACHE_UNLIMITED)),
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
    pub(crate) async fn delete_check_and_gather(&self, key: &K, v: &mut Vec<(K, V)>) -> Result<bool> {
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
        let maxkey = (*node.get_key(nchild - 1)).into();
        let next_maxkey = if nchild > 1 {
            (*node.get_key(nchild - 2)).into()
        } else {
            0
        };

        // gather data to output vec
        if (maxkey == (*key).into()) && (next_maxkey < root_capacity as u64) {
            // because we shrink to root node, we copy only size min than root capacity
            for i in 0..std::cmp::min(root_capacity, nchild) {
                let key = node.get_key(i);
                let val = node.get_val(i);
                v.push((*key, *val));
            }
            return Ok(true);
        }
        return Ok(false);
    }
}

// all op_* functions
impl<'a, K, V, P, L> BtreeMap<'a, K, V, P, L>
    where
        K: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        V: Copy + Default + std::fmt::Display + NodeValue,
        K: Into<u64>,
        P: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        P: NodeValue + From<u64> + Into<u64>,
        L: BlockLoader<P>,
{
    fn op_insert(&self, path: &BtreePath<'_, K, V, P>, level: BtreeLevel, key: &mut K, val: &mut V, pid: &mut P) {
        if self.is_nonroot_level(level) {
            // non root node
            let node = path.get_nonroot_node(level);

            if level == BTREE_NODE_LEVEL_LEAF {
                node.insert(path.get_index(level), key, val);
            } else {
                node.insert(path.get_index(level), key, pid);
            }
            node.mark_dirty();

            if path.get_index(level) == 0 {
                let node_key = node.get_key(0);
                self.promote_key(path, level + 1, &node_key);
            }
        } else {
            // root node
            let root = self.get_root_node();
            if level == BTREE_NODE_LEVEL_LEAF {
                root.insert(path.get_index(level), key, val);
            } else {
                root.insert(path.get_index(level), key, pid);
            }
        }
    }

    fn op_grow(&self, path: &BtreePath<K, V, P>, level: BtreeLevel, key: &mut K, val: &mut V, pid: &mut P) {
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
        // if root no more leaf, clear flag
        root.clear_leaf();

        let sib_node = path.get_sib_node(level);
        path.set_nonroot_node(level, sib_node);
        path.set_sib_node_none(level);

        self.op_insert(path, level, key, val, pid);

        *key = *child.get_key(0);
        *pid = path.get_new_seq(level);
    }

    fn op_split(&self, path: &BtreePath<K, V, P>, level: BtreeLevel, key: &mut K, val: &mut V, pid: &mut P) {
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

            if right.is_leaf() {
                right.insert(idx, key, val);
            } else {
                right.insert(idx, key, pid);
            }

            *key = *right.get_key(0);
            *pid = path.get_new_seq(level);

            let sib_node = path.get_sib_node(level);
            path.set_nonroot_node(level, sib_node);
            path.set_sib_node_none(level);
        } else {
            self.op_insert(path, level, key, val, pid);

            *key = *right.get_key(0);
            *pid = path.get_new_seq(level);

            path.set_sib_node_none(level);
        }

        path.set_index(level + 1, path.get_index(level + 1) + 1);
    }

    fn op_carry_left(&self, path: &BtreePath<K, V, P>, level: BtreeLevel, key: &mut K, val: &mut V, pid: &mut P) {
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

        self.op_insert(path, level, key, val, pid);
    }

    fn op_carry_right(&self, path: &BtreePath<K, V, P>, level: BtreeLevel, key: &mut K, val: &mut V, pid: &mut P) {
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
            path.set_nonroot_node(level, sib_node);
            path.set_sib_node_none(level);
            let nchild = node.get_nchild();
            path.set_index(level, path.get_index(level) - nchild);
            path.set_index(level + 1, path.get_index(level + 1) + 1);
        } else {
            path.set_sib_node_none(level);
        }

        self.op_insert(path, level, key, val, pid);
    }

    fn op_concat_left(&self, path: &BtreePath<K, V, P>, level: BtreeLevel, key: &mut K, val: &mut V, pid: &mut P) {
        self.op_delete(path, level, key, val, pid);

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

    fn op_concat_right(&self, path: &BtreePath<K, V, P>, level: BtreeLevel, key: &mut K, val: &mut V, pid: &mut P) {
        self.op_delete(path, level, key, val, pid);

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

    fn op_borrow_left(&self, path: &BtreePath<K, V, P>, level: BtreeLevel, key: &mut K, val: &mut V, pid: &mut P) {
        self.op_delete(path, level, key, val, pid);

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

    fn op_borrow_right(&self, path: &BtreePath<K, V, P>, level: BtreeLevel, key: &mut K, val: &mut V, pid: &mut P) {
        self.op_delete(path, level, key, val, pid);

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

    fn op_delete(&self, path: &BtreePath<K, V, P>, level: BtreeLevel, key: &mut K, val: &mut V, pid: &mut P) {
        if self.is_nonroot_level(level) {
            let node = path.get_nonroot_node(level);
            if level == BTREE_NODE_LEVEL_LEAF {
                node.delete(path.get_index(level), key, val);
            } else {
                node.delete(path.get_index(level), key, pid);
            }
            node.mark_dirty();
            if path.get_index(level) == 0 {
                let node_key = node.get_key(0);
                self.promote_key(path, level + 1, &node_key);
            }
        } else {
            let root = self.get_root_node();
            if level == BTREE_NODE_LEVEL_LEAF {
                root.delete(path.get_index(level), key, val);
            } else {
                root.delete(path.get_index(level), key, pid);
            }
        }
    }

    fn op_shrink(&self, path: &BtreePath<K, V, P>, level: BtreeLevel, key: &mut K, val: &mut V, pid: &mut P) {
        self.op_delete(path, level, key, val, pid);

        let root = self.get_root_node();
        let child = path.get_nonroot_node(level);

        let mut _key = K::default();
        let mut _pid = P::default();
        root.delete(0, &mut _key, &mut _pid);
        root.set_level(level);
        if level == BTREE_NODE_LEVEL_LEAF {
            // if root become leaf
            root.set_leaf();
        }

        let n = child.get_nchild();

        {

        let root_ref = &root.clone();
        let child_ref = &child.clone();
        BtreeNode::move_left(root_ref, child_ref, n);

        }

        let _ = self.remove_from_nodes(path.get_nonroot_node(level));
        path.set_nonroot_node_none(level);
    }

    fn op_nop(&self, _path: &BtreePath<K, V, P>, _level: BtreeLevel, _key: &mut K, _val: &mut V, _pid: &mut P) {
        // do nothing
    }
}

// all up level api
impl<'a, K, V, P, L> VMap<K, V> for BtreeMap<'a, K, V, P, L>
    where
        K: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        V: Copy + Default + std::fmt::Display + NodeValue,
        K: Into<u64>,
        P: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        P: NodeValue + From<u64> + Into<u64>,
        L: BlockLoader<P>,
{
    #[maybe_async::maybe_async]
    async fn lookup(&self, key: &K, level: usize) -> Result<V> {
        let path = BtreePath::new();
        let (val, _) = self.do_lookup(&path, key, level).await?;
        Ok(val)
    }

    #[maybe_async::maybe_async]
    async fn lookup_contig(&self, key: &K, maxblocks: usize) -> Result<(V, usize)> {
        let level = BTREE_NODE_LEVEL_MIN;
        let path = BtreePath::new();
        let (value, _) = self.do_lookup(&path, key, level).await?;

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

                let mut _key = *key; _key += count as u64;
                if node.get_key(index) != &_key {
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
            let mut _key = *key; _key += count as u64;
            if p_index >= p_node.get_nchild() || p_node.get_key(p_index) != &_key {
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
                return Err(Error::new(ErrorKind::AlreadyExists, "key value already exists in btree"));
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
        self.propagate_lite(&path);
        Ok(())
    }

    #[maybe_async::maybe_async]
    async fn insert_or_update(&self, key: K, val: V) -> Result<Option<V>> {
        let path = BtreePath::new();
        let res = self.do_lookup(&path, &key, BTREE_NODE_LEVEL_MIN).await;
        match res {
            Ok((old_val, _)) => {
                let node = self.get_node(&path, BTREE_NODE_LEVEL_MIN);
                let index = path.get_index(BTREE_NODE_LEVEL_MIN);
                node.set_val(index, &val);
                node.mark_dirty();
                self.propagate_lite(&path);
                self.set_dirty();
                return Ok(Some(old_val));
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
        self.propagate_lite(&path);
        Ok(None)
    }

    #[maybe_async::maybe_async]
    async fn delete(&self, key: &K) -> Result<()> {
        let path = BtreePath::new();
        let res = self.do_lookup(&path, key, BTREE_NODE_LEVEL_MIN).await;
        match res {
            Ok(_) => {}, // do nothing if found
            Err(e) => { return Err(e); }, // return any errors
        }

        let level = self.prepare_delete(&path).await?;
        self.commit_delete(&path, level);
        self.propagate_lite(&path);
        Ok(())
    }

    #[maybe_async::maybe_async]
    async fn seek_key(&self, start: &K) -> Result<K> {
        let path = BtreePath::new();
        let res = self.do_lookup(&path, start, BTREE_NODE_LEVEL_MIN).await;
        match res {
            Ok(_) => {
                return Ok(*start);
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
