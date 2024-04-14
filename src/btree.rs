use std::fmt;
use std::collections::HashMap;
use std::cell::RefCell;
use std::rc::Rc;
use std::ops::{Deref, DerefMut};
use std::borrow::{Borrow, BorrowMut};
use log::{warn, debug};
use tokio::io::{Error, ErrorKind, Result};
use crate::node::*;
use crate::VMap;
use crate::{NodeValue, BlockLoader};

pub(crate) type BtreeLevel = usize;
pub(crate) type BtreeNodeRef<'a, K, V> = Rc<RefCell<BtreeNode<'a, K, V>>>;

macro_rules! r {
    ($node: expr) => {
        (*$node).borrow()
    }
}

macro_rules! w {
    ($node: expr) => {
        (*$node).borrow_mut()
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

//#[derive(Clone)]
struct BtreePathLevel<'a, K, V> {
    node: Option<BtreeNodeRef<'a, K, V>>,
    sib_node: Option<BtreeNodeRef<'a, K, V>>,
    index: usize,
    oldseq: V,
    newseq: V,
    op: BtreeMapOp,
}

pub struct BtreePath<'a, K, V> {
    levels: Vec<RefCell<BtreePathLevel<'a, K, V>>>,
}

impl<'a, K, V: Copy + Default + NodeValue<V>> BtreePath<'a, K, V>
{
    pub fn new() -> Self {
        let mut l = Vec::with_capacity(BTREE_NODE_LEVEL_MAX);
        for _ in 0..BTREE_NODE_LEVEL_MAX {
            l.push(RefCell::new(BtreePathLevel {
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

    pub fn get_index(&self, level: usize) -> usize {
        self.levels[level].borrow().index
    }

    pub fn set_index(&self, level: usize, index: usize) {
        self.levels[level].borrow_mut().index = index;
    }

    pub fn get_nonroot_node(&self, level: usize) -> BtreeNodeRef<'a, K, V> {
        self.levels[level].borrow().node.as_ref().unwrap().clone()
    }

    pub fn set_nonroot_node(&self, level: usize, node: BtreeNodeRef<'a, K, V>) {
        self.levels[level].borrow_mut().node = Some(node);
    }

    pub fn set_nonroot_node_none(&self, level: usize) {
        self.levels[level].borrow_mut().node = None;
    }

    pub fn get_sib_node(&self, level: usize) -> BtreeNodeRef<'a, K, V> {
        self.levels[level].borrow().sib_node.as_ref().unwrap().clone()
    }

    pub fn set_sib_node(&self, level: usize, node: BtreeNodeRef<'a, K, V>) {
        self.levels[level].borrow_mut().sib_node = Some(node);
    }

    pub fn set_sib_node_none(&self, level: usize) {
        self.levels[level].borrow_mut().sib_node = None;
    }

    pub fn get_new_seq(&self, level: usize) -> V {
        self.levels[level].borrow().newseq
    }

    pub fn set_new_seq(&self, level: usize, seq: V) {
        self.levels[level].borrow_mut().newseq = seq;
    }

    pub fn get_old_seq(&self, level: usize) -> V {
        self.levels[level].borrow().oldseq
    }

    pub fn set_old_seq(&self, level: usize, seq: V) {
        self.levels[level].borrow_mut().oldseq = seq;
    }

    pub fn get_op(&self, level: usize) -> BtreeMapOp {
        self.levels[level].borrow().op
    }

    pub fn set_op(&self, level: usize, op: BtreeMapOp) {
        self.levels[level].borrow_mut().op = op;
    }
}

pub struct BtreeMap<'a, K, V, L: BlockLoader<V>> {
    pub data: Vec<u8>,
    pub root: BtreeNodeRef<'a, K, V>,
    pub nodes: RefCell<HashMap<V, BtreeNodeRef<'a, K, V>>>, // list of btree node in memory
    pub last_seq: RefCell<V>,
    pub dirty: RefCell<bool>,
    pub meta_block_size: usize,
    pub block_loader: L,
}

impl<'a, K, V, L> fmt::Display for BtreeMap<'a, K, V, L>
    where
        K: Copy + fmt::Display + std::cmp::PartialOrd,
        V: Copy + fmt::Display,
        L: BlockLoader<V>,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", (*self.root).borrow());
        for (v, node) in self.nodes.borrow().iter() {
            let n: Rc<RefCell<BtreeNode<'a, K, V>>> = node.clone();
            write!(f, "{} - {}", v, (*n).borrow());
        }
        write!(f, "")
    }
}

impl<'a, K, V, L> BtreeMap<'a, K, V, L>
    where
        K: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        V: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        K: From<V>,
        V: From<K> + NodeValue<V>,
        L: BlockLoader<V>,
{
    #[inline]
    fn get_root_node(&self) -> BtreeNodeRef<'a, K, V> {
        self.root.clone()
    }

    #[inline]
    fn get_root_level(&self) -> usize {
        (*self.root).borrow().get_level()
    }

    #[inline]
    fn get_height(&self) -> usize {
        (*self.root).borrow().get_level() + 1
    }

    #[inline]
    fn get_node<'s>(&'s self, path: &'s BtreePath<'a, K, V>, level: usize) -> BtreeNodeRef<'a, K, V> {
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

    #[inline]
    fn get_next_seq(&self) -> V {
        *self.last_seq.borrow_mut() += 1;
        *self.last_seq.borrow()
    }

    #[inline]
    fn prepare_seq(&self, path: &BtreePath<K, V>, level: usize) -> V {
        let seq = self.get_next_seq();
        path.set_new_seq(level, seq);
        seq
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

    async fn meta_block_loader(&self, v: &V, buf: &mut [u8]) -> Result<()> {
        self.block_loader.read(v, buf).await
    }

    pub(crate) async fn get_from_nodes(&self, val: V) -> Result<BtreeNodeRef<'a, K, V>> {
        let mut list = self.nodes.borrow_mut();
        if let Some(node) = list.get(&val) {
            return Ok(node.clone());
        }

        if let Some(mut node) = BtreeNode::<K, V>::new(val, self.meta_block_size) {
            self.meta_block_loader(&val, node.as_mut()).await?;
            let n = Rc::new(RefCell::new(node));
            list.insert(val, n.clone());
            return Ok(n);
        }
        return Err(Error::new(ErrorKind::OutOfMemory, ""));
    }

    pub(crate) async fn get_new_node(&self, val: V) -> Result<BtreeNodeRef<'a, K, V>> {
        let mut list = self.nodes.borrow_mut();
        if let Some(node) = BtreeNode::<K, V>::new(val, self.meta_block_size) {
            let n = Rc::new(RefCell::new(node));
            if let Some(oldnode) = list.insert(val, n.clone()) {
                panic!("value {} is already in nodes list", val);
            }
            return Ok(n);
        }
        return Err(Error::new(ErrorKind::OutOfMemory, ""));
    }

    fn remove_from_nodes(&self, node: BtreeNodeRef<K, V>) -> Result<()> {
        if let Some(id) = r!(node).id() {
            let n = self.nodes.borrow_mut().remove(id);
            assert!(n.is_some());
        }
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
            _ => panic!("{:?} not yet implement", path.get_op(level)),
        }
    }

    async fn do_lookup<'s>(&'s self, path: &'s BtreePath<'a, K, V>, key: &K, minlevel: usize) -> Result<V> {
        let root = self.get_root_node();
        let mut level = (*root).borrow().get_level();
        if level < minlevel || (*root).borrow().get_nchild() <= 0 {
            return Err(Error::new(ErrorKind::NotFound, ""));
        }

        let (mut found, mut index) = (*root).borrow().lookup(key);
        let mut value = (*root).borrow().get_val(index);

        path.set_index(level, index);

        level -= 1;
        while level >= minlevel {
            let node = self.get_from_nodes(value).await?;

            if !found {
                (found, index) = (*node).borrow().lookup(key);
            } else {
                index = 0;
            }

            if index < (*node).borrow().get_nchild() {
                value = (*node).borrow().get_val(index);
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

    async fn do_lookup_last<'s>(&'s self, path: &'s BtreePath<'a, K, V>) -> Result<K> {
        let mut node = self.get_root_node();
        let mut index = r!(node).get_nchild() - 1;
        if index < 0 {
            return Err(Error::new(ErrorKind::NotFound, ""));
        }
        let mut level = r!(node).get_level();
        let mut value = r!(node).get_val(index);
        path.set_nonroot_node_none(level);
        path.set_index(level, index);

        level -= 1;
        while level > 0 {
            node = self.get_from_nodes(value).await?;
            index = r!(node).get_nchild() - 1;
            value = r!(node).get_val(index);
            path.set_nonroot_node(level, node.clone());
            path.set_index(level, index);
            level -= 1;
        }
        let key = r!(node).get_key(index);
        Ok(key)
    }

    async fn prepare_insert<'s>(&'s self, path: &'s BtreePath<'a, K, V>, key: &K) -> Result<BtreeLevel> {

        let mut level = BTREE_NODE_LEVEL_DATA;

        level = BTREE_NODE_LEVEL_MIN;
        // go through all non leap levels
        for level in BTREE_NODE_LEVEL_MIN..self.get_root_level() {
            let node = path.get_nonroot_node(level);
            if (*node).borrow().has_free_slots() {
                path.set_op(level, BtreeMapOp::Insert);
                return Ok(level);
            }

            // if no free slots found at this level, we check parent
            let parent = self.get_node(path, level + 1);
            let pindex = path.get_index(level + 1);

            // left sibling
            if pindex > 0 {
                let sib_val = (*parent).borrow().get_val(pindex - 1);
                let sib_node = self.get_from_nodes(sib_val).await?;
                if (*sib_node).borrow().has_free_slots() {
                    path.set_sib_node(level, sib_node);
                    path.set_op(level, BtreeMapOp::CarryLeft);
                    return Ok(level);
                }
            }

            // right sibling
            if pindex < (*parent).borrow().get_nchild() - 1 {
                let sib_val = (*parent).borrow().get_val(pindex + 1);
                let sib_node = self.get_from_nodes(sib_val).await?;
                if (*sib_node).borrow().has_free_slots() {
                    path.set_sib_node(level, sib_node);
                    path.set_op(level, BtreeMapOp::CarryRight);
                    return Ok(level);
                }
            }

            // split
            // both left and right sibling has no free slots
            // prepare a new node and split myself
            let seq = self.prepare_seq(path, level);
            let node = self.get_new_node(seq).await?;
            (*node).borrow_mut().init(0, level, 0);
            path.set_sib_node(level, node);
            path.set_op(level, BtreeMapOp::Split);
        }

        level += 1;

        // root node
        let root = self.get_root_node();
        if (*root).borrow().has_free_slots() {
            path.set_op(level, BtreeMapOp::Insert);
            return Ok(level);
        }

        // grow
        let seq = self.prepare_seq(path, level);
        let node = self.get_new_node(seq).await?;
        (*node).borrow_mut().init(0, level, 0);
        path.set_sib_node(level, node);
        path.set_op(level, BtreeMapOp::Grow);

        level += 1;
        path.set_op(level, BtreeMapOp::Insert);
        Ok(level)
    }

    fn commit_insert(&self, path: &BtreePath<'_, K, V>, key: K, val: V, maxlevel: usize) {

        let mut level: usize;
        let mut _key = key;
        let mut _val = val;

        for level in BTREE_NODE_LEVEL_MIN..=maxlevel {
            self.do_op(path, level, &mut _key, &mut _val);
        }

        self.set_dirty();
    }

    async fn prepare_delete<'s>(&'s self, path: &'s BtreePath<'a, K, V>) -> Result<BtreeLevel> {
        let mut level = BTREE_NODE_LEVEL_MIN;
        let mut dindex = path.get_index(level);
        for mut level in BTREE_NODE_LEVEL_MIN..self.get_root_level() {
            let node = path.get_nonroot_node(level);
            path.set_old_seq(level, r!(node).get_val(dindex).into());

            if r!(node).is_overflowing() {
                path.set_op(level, BtreeMapOp::Delete);
                return Ok(level);
            }

            let parent = self.get_node(path, level + 1);
            let pindex = path.get_index(level + 1);
            dindex = pindex;

            // left sibling
            if pindex > 0 {
                let sib_val = r!(parent).get_val(pindex - 1);
                let sib_node = self.get_from_nodes(sib_val.into()).await?;
                if r!(sib_node).is_overflowing() {
                    path.set_sib_node(level, sib_node);
                    path.set_op(level, BtreeMapOp::BorrowLeft);
                    return Ok(level);
                } else {
                    path.set_sib_node(level, sib_node);
                    path.set_op(level, BtreeMapOp::ConcatLeft);
                }
            } else if pindex < r!(parent).get_nchild() - 1 {
                // right sibling
                let sib_val = r!(parent).get_val(pindex + 1);
                let sib_node = self.get_from_nodes(sib_val.into()).await?;
                if r!(sib_node).is_overflowing() {
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
                if r!(node).get_nchild() - 1 <= r!(self.get_root_node()).get_capacity() {
                    path.set_op(level, BtreeMapOp::Shrink);
                    level += 1;
                    path.set_op(level, BtreeMapOp::Nop);
                    // shrink root child
                    let root = self.get_root_node();
                    path.set_old_seq(level, r!(root).get_val(dindex).into());
                    return Ok(level);
                } else {
                    path.set_op(level, BtreeMapOp::Delete);
                    return Ok(level);
                }
            }
        }
        level += 1;
        // child of the root node is deleted
        path.set_op(level, BtreeMapOp::Delete);

        // shrink root child
        let root = self.get_root_node();
        path.set_old_seq(level, r!(root).get_val(dindex).into());
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
                (*node).borrow_mut().set_key(index, key);
                w!(node).mark_dirty();

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
            (*root).borrow_mut().set_key(path.get_index(level), key);
        }
    }

    fn get_next_key(&self, path: &BtreePath<'a, K, V>, minlevel: BtreeLevel, start: &K) -> Result<K> {
        let mut next_adj = 0;
        let maxlevel = self.get_height() - 1;
        for level in minlevel..=maxlevel {
            let node = if level == maxlevel {
                self.get_root_node()
            } else {
                path.get_nonroot_node(level)
            };
            let index = path.get_index(level) + next_adj;
            if index < r!(node).get_nchild() {
                return Ok(r!(node).get_key(index));
            }
            next_adj = 1;
        }
        Err(Error::new(ErrorKind::NotFound, ""))
    }

    pub(crate) fn lookup_dirty(&self) -> Vec<BtreeNodeRef<'a, K, V>> {
        let mut v = Vec::new();
        for (_, n) in self.nodes.borrow().iter() {
            if r!(*n).is_dirty() {
                v.push(n.clone());
            }
        }
        v
    }

    pub(crate) async fn assign(&self, key: K, newval: V, meta_node: Option<BtreeNodeRef<'_, K, V>>) -> Result<()> {

        let (search_key, level, is_meta) = if let Some(node) = meta_node {
            // if this is meta node, we use node key as search key to search in parent node
            let node_key = r!(node).get_key(0);
            let node_level = r!(node).get_level();
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
            let oldval = r!(parent).get_val(pindex);
            debug!("assign - get back meta node oldval {oldval}");

            let mut list = self.nodes.borrow_mut();
            // remove node from list via old temp val
            if let Some(node) = list.remove(&oldval) {
                // update node id
                w!(node).set_id(newval);
                // insert back with new val
                list.insert(newval, node);
            } else {
                panic!("old value {} is not on nodes list, THIS SHOULD NOT HAPPEN", oldval);
            }
        }

        w!(parent).set_val(pindex, &newval);
        Ok(())
    }

    pub(crate) async fn mark(&self, key: K, level: usize) -> Result<()> {
        let path = BtreePath::new();
        let val = self.do_lookup(&path, &key, level + 1).await?;
        let node = self.get_from_nodes(val.into()).await?;
        w!(node).mark_dirty();
        self.set_dirty();
        Ok(())
    }

    pub(crate) fn new(data: &[u8], meta_block_size: usize, block_loader: L) -> Self {
        let mut v = Vec::with_capacity(data.len());
        v.extend_from_slice(data);
        Self {
            root: Rc::new(RefCell::new(BtreeNode::<K, V>::from_slice(&v))),
            data: v,
            nodes: RefCell::new(HashMap::new()),
            last_seq: RefCell::new(V::invalid_value()),
            dirty: RefCell::new(false),
            meta_block_size: meta_block_size,
            block_loader: block_loader,
        }
    }
}

// all op_* functions
impl<'a, K, V, L> BtreeMap<'a, K, V, L>
    where
        K: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        V: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        K: From<V>,
        V: From<K> + NodeValue<V>,
        L: BlockLoader<V>,
{
    fn op_insert(&self, path: &BtreePath<'_, K, V>, level: BtreeLevel, key: &mut K, val: &mut V) {
        if self.is_nonroot_level(level) {
            // non root node
            let node = path.get_nonroot_node(level);

            (*node).borrow_mut().insert(path.get_index(level), key, val);
            w!(node).mark_dirty();

            if path.get_index(level) == 0 {
                let node_key = (*node).borrow().get_key(0);
                self.promote_key(path, level + 1, &node_key);
            }
        } else {
            // root node
            let root = self.get_root_node();
            (*root).borrow_mut().insert(path.get_index(level), key, val);
        }
    }

    fn op_grow(&self, path: &BtreePath<K, V>, level: BtreeLevel, key: &mut K, val: &mut V) {
        let child = path.get_sib_node(level);
        let root = self.get_root_node();

        let n = (*root).borrow().get_nchild();

        {

        let root_ref = &(*root).borrow();
        let child_ref = &(*child).borrow();
        BtreeNode::move_right(root_ref, child_ref, n);

        }

        w!(child).mark_dirty();

        (*root).borrow_mut().set_level(level + 1);

        let sib_node = path.get_sib_node(level);
        path.set_nonroot_node(level, sib_node);
        path.set_sib_node_none(level);

        self.op_insert(path, level, key, val);

        *key = (*child).borrow().get_key(0);
        *val = path.get_new_seq(level).into();
    }

    fn op_split(&self, path: &BtreePath<K, V>, level: BtreeLevel, key: &mut K, val: &mut V) {
        let node = path.get_nonroot_node(level);
        let right = path.get_sib_node(level);
        let nchild = r!(node).get_nchild();
        let mut mv = false;

        let mut n = (nchild + 1) / 2;
        if n > nchild - path.get_index(level) {
            n -= 1;
            mv = true;
        }

        {

        let node_ref = &r!(node);
        let right_ref = &r!(right);
        BtreeNode::move_right(node_ref, right_ref, n);

        }

        w!(node).mark_dirty();
        w!(right).mark_dirty();

        if mv {
            let idx = path.get_index(level) - r!(node).get_nchild();
            path.set_index(level, idx);

            w!(right).insert(idx, key, val);

            *key = r!(right).get_key(0);
            *val = path.get_new_seq(level).into();

            let sib_node = path.get_sib_node(level);
            path.set_nonroot_node(level, sib_node);
            path.set_sib_node_none(level);
        } else {
            self.op_insert(path, level, key, val);

            *key = r!(right).get_key(0);
            *val = path.get_new_seq(level).into();

            path.set_sib_node_none(level);
        }

        path.set_index(level + 1, path.get_index(level + 1) + 1);
    }

    fn op_carry_left(&self, path: &BtreePath<K, V>, level: BtreeLevel, key: &mut K, val: &mut V) {
        let node = path.get_nonroot_node(level);
        let left = path.get_sib_node(level);
        let nchild = r!(node).get_nchild();
        let lnchild = r!(left).get_nchild();
        let mut mv = false;

        let mut n = (nchild + lnchild + 1) / 2 - lnchild;
        if n > path.get_index(level) {
            n -= 1;
            mv = true;
        }

        {

        let left_ref = &r!(left);
        let node_ref = &r!(node);
        BtreeNode::move_left(left_ref, node_ref, n);

        }

        w!(left).mark_dirty();
        w!(node).mark_dirty();

        let node_key = r!(node).get_key(0);
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
        let nchild = r!(node).get_nchild();
        let rnchild = r!(right).get_nchild();
        let mut mv = false;

        let mut n = (nchild + rnchild + 1) / 2 - rnchild;
        if n > nchild - path.get_index(level) {
            n -= 1;
            mv = true;
        }

        {

        let node_ref = &r!(node);
        let right_ref = &r!(right);
        BtreeNode::move_right(node_ref, right_ref, n);

        }

        w!(node).mark_dirty();
        w!(right).mark_dirty();

        path.set_index(level + 1, path.get_index(level + 1) + 1);
        let node_key = r!(right).get_key(0);
        self.promote_key(path, level + 1, &node_key);
        path.set_index(level + 1, path.get_index(level + 1) - 1);

        if mv {
            let sib_node = path.get_sib_node(level);
            path.set_sib_node(level, sib_node);
            path.set_sib_node_none(level);
            let nchild = r!(node).get_nchild();
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

        let n = r!(node).get_nchild();

        {

        let left_ref = &r!(left);
        let node_ref = &r!(node);
        BtreeNode::move_left(left_ref, node_ref, n);

        }

        w!(left).mark_dirty();

        self.remove_from_nodes(node);
        let sib_node = path.get_sib_node(level);
        path.set_nonroot_node(level, sib_node);
        path.set_sib_node_none(level);
        path.set_index(level, path.get_index(level) + r!(left).get_nchild());
    }

    fn op_concat_right(&self, path: &BtreePath<K, V>, level: BtreeLevel, key: &mut K, val: &mut V) {
        self.op_delete(path, level, key, val);

        let node = path.get_nonroot_node(level);
        let right = path.get_sib_node(level);

        let n = r!(right).get_nchild();

        {

        let node_ref = &r!(node);
        let right_ref = &r!(right);
        BtreeNode::move_left(node_ref, right_ref, n);

        }

        w!(node).mark_dirty();

        self.remove_from_nodes(path.get_sib_node(level));
        path.set_sib_node_none(level);
        path.set_index(level + 1, path.get_index(level + 1) + 1);
    }

    fn op_borrow_left(&self, path: &BtreePath<K, V>, level: BtreeLevel, key: &mut K, val: &mut V) {
        self.op_delete(path, level, key, val);

        let node = path.get_nonroot_node(level);
        let left = path.get_sib_node(level);
        let nchild = r!(node).get_nchild();
        let lnchild = r!(left).get_nchild();

        let mut n = (nchild + lnchild) / 2 - nchild;

        {

        let left_ref = &r!(left);
        let node_ref = &r!(node);
        BtreeNode::move_right(left_ref, node_ref, n);

        }

        w!(node).mark_dirty();
        w!(left).mark_dirty();

        let node_key = r!(node).get_key(0);
        self.promote_key(path, level + 1, &node_key);

        path.set_sib_node_none(level);
        path.set_index(level, path.get_index(level) + n);
    }

    fn op_borrow_right(&self, path: &BtreePath<K, V>, level: BtreeLevel, key: &mut K, val: &mut V) {
        self.op_delete(path, level, key, val);

        let node = path.get_nonroot_node(level);
        let right = path.get_sib_node(level);
        let nchild = r!(node).get_nchild();
        let rnchild = r!(right).get_nchild();

        let mut n = (nchild + rnchild) / 2 - nchild;

        {

        let node_ref = &r!(node);
        let right_ref = &r!(right);
        BtreeNode::move_left(node_ref, right_ref, n);

        }

        w!(node).mark_dirty();
        w!(right).mark_dirty();

        path.set_index(level + 1, path.get_index(level + 1) + 1);
        let node_key = r!(right).get_key(0);
        self.promote_key(path, level + 1, &node_key);
        path.set_index(level + 1, path.get_index(level + 1) - 1);

        path.set_sib_node_none(level);
    }

    fn op_delete(&self, path: &BtreePath<K, V>, level: BtreeLevel, key: &mut K, val: &mut V) {
        if self.is_nonroot_level(level) {
            let node = path.get_nonroot_node(level);
            w!(node).delete(path.get_index(level), key, val);
            w!(node).mark_dirty();
            if path.get_index(level) == 0 {
                let node_key = r!(node).get_key(0);
                self.promote_key(path, level + 1, &node_key);
            }
        } else {
            let root = self.get_root_node();
            w!(root).delete(path.get_index(level), key, val);
        }
    }

    fn op_shrink(&self, path: &BtreePath<K, V>, level: BtreeLevel, key: &mut K, val: &mut V) {
        self.op_delete(path, level, key, val);

        let root = self.get_root_node();
        let child = path.get_nonroot_node(level);

        let mut _key = K::default();
        let mut _val = V::default();
        w!(root).delete(0, &mut _key, &mut _val);
        w!(root).set_level(level);
        let n = r!(child).get_nchild();

        {

        let root_ref = &r!(root);
        let child_ref = &r!(child);
        BtreeNode::move_left(root_ref, child_ref, n);

        }

        self.remove_from_nodes(path.get_nonroot_node(level));
        path.set_nonroot_node_none(level);
    }

    fn op_nop(&self, path: &BtreePath<K, V>, level: BtreeLevel, key: &mut K, val: &mut V) {
    }
}

// all up level api
impl<'a, K, V, L> VMap<K, V> for BtreeMap<'a, K, V, L>
    where
        K: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        V: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        K: From<V>,
        V: From<K> + NodeValue<V>,
        L: BlockLoader<V>,
{
    async fn lookup(&self, key: K, level: usize) -> Result<V> {
        let path = BtreePath::new();
        let val = self.do_lookup(&path, &key, level).await?;
        Ok(val)
    }

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
            while index < r!(node).get_nchild() {
                if count == maxblocks {
                    return Ok((value, count))
                }

                let mut _key = key; _key += count as u64;
                if r!(node).get_key(index) != _key {
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
            if p_index >= r!(p_node).get_nchild() || r!(p_node).get_key(p_index) != _key {
                return Ok((value, count));
            }
            let v = r!(p_node).get_val(p_index);
            path.set_index(level + 1, p_index);
            path.set_nonroot_node_none(level);

            // get sibling node for next looop
            node = self.get_from_nodes(v.into()).await?;
            path.set_nonroot_node(level, node.clone());
            index = 0;
            path.set_index(level, index);
        }
        Ok((value, count))
    }

    async fn insert(&self, key: K, val: V) -> Result<()> {
        let path = BtreePath::new();
        match self.do_lookup(&path, &key, BTREE_NODE_LEVEL_MIN).await {
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
        let level = self.prepare_insert(&path, &key).await?;
        self.commit_insert(&path, key, val, level);
        Ok(())
    }

    async fn delete(&self, key: K) -> Result<()> {
        let path = BtreePath::new();
        match self.do_lookup(&path, &key, BTREE_NODE_LEVEL_MIN).await {
            Ok(_) => {}, // do nothing if found
            Err(e) => { return Err(e); }, // return any errors
        }

        let level = self.prepare_delete(&path).await?;
        self.commit_delete(&path, level);
        Ok(())
    }

    async fn seek_key(&self, start: K) -> Result<K> {
        let path = BtreePath::new();
        match self.do_lookup(&path, &start, BTREE_NODE_LEVEL_MIN).await {
            Ok(_) => {
                return Ok(start);
            },
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                   return self.get_next_key(&path, BTREE_NODE_LEVEL_MIN, &start);
                }
                return Err(e);
            }
        }
    }

    async fn last_key(&self) -> Result<K> {
        let path = BtreePath::new();
        self.do_lookup_last(&path).await
    }
}
