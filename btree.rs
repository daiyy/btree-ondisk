use std::fmt;
use std::collections::HashMap;
use std::cell::RefCell;
use std::rc::Rc;
use std::ops::{Deref, DerefMut};
use std::borrow::{Borrow, BorrowMut};
use tokio::io::{Error, ErrorKind, Result};
use crate::node::*;
use crate::VMap;

type BtreeLevel = usize;
type BtreeNodeRef<'a, K, V> = Rc<RefCell<BtreeNode<'a, K, V>>>;

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
    oldseq: K,
    newseq: K,
    op: BtreeMapOp,
}

pub struct BtreePath<'a, K, V> {
    levels: Vec<RefCell<BtreePathLevel<'a, K, V>>>,
}

impl<'a, K: Default + Copy, V> BtreePath<'a, K, V>
{
    pub fn new() -> Self {
        let mut l = Vec::with_capacity(BTREE_NODE_LEVEL_MAX);
        for _ in 0..BTREE_NODE_LEVEL_MAX {
            l.push(RefCell::new(BtreePathLevel {
                node: None,
                sib_node: None,
                index: 0,
                oldseq: K::default(),
                newseq: K::default(),
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

    pub fn get_new_seq(&self, level: usize) -> K {
        self.levels[level].borrow().newseq
    }

    pub fn set_new_seq(&self, level: usize, seq: K) {
        self.levels[level].borrow_mut().newseq = seq;
    }

    pub fn get_old_seq(&self, level: usize) -> K {
        self.levels[level].borrow().oldseq
    }

    pub fn set_old_seq(&self, level: usize, seq: K) {
        self.levels[level].borrow_mut().oldseq = seq;
    }

    pub fn get_op(&self, level: usize) -> BtreeMapOp {
        self.levels[level].borrow().op
    }

    pub fn set_op(&self, level: usize, op: BtreeMapOp) {
        self.levels[level].borrow_mut().op = op;
    }
}

pub struct BtreeMap<'a, K, V> {
    pub data: Vec<u8>,
    pub root: BtreeNodeRef<'a, K, V>,
    pub nodes: RefCell<HashMap<K, BtreeNodeRef<'a, K, V>>>, // list of btree node in memory
    pub last_seq: RefCell<K>,
    pub caches: RefCell<Vec<Rc<Box<Vec<u8>>>>>,
}

impl<'a, K, V> fmt::Display for BtreeMap<'a, K, V>
    where
        K: Copy + fmt::Display + std::cmp::PartialOrd,
        V: Copy + fmt::Display
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", (*self.root).borrow());
        for (k, node) in self.nodes.borrow().iter() {
            let n: Rc<RefCell<BtreeNode<'a, K, V>>> = node.clone();
            write!(f, "{} - {}", k, (*n).borrow());
        }
        write!(f, "")
    }
}

impl<'a, K, V> BtreeMap<'a, K, V>
    where
        K: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        V: Copy + Default + std::fmt::Display + From<K>,
        K: From<V>
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
        level >= self.get_height() - 1
    }

    #[inline]
    fn is_nonroot_level(&self, level: usize) -> bool {
        level < self.get_height() - 1
    }

    #[inline]
    fn get_next_seq(&self) -> K {
        *self.last_seq.borrow_mut() += 1;
        *self.last_seq.borrow()
    }

    #[inline]
    fn prepare_seq(&self, path: &BtreePath<K, V>, level: usize) -> K {
        let seq = self.get_next_seq();
        path.set_new_seq(level, seq);
        seq
    }

    pub async fn get_from_nodes(&self, key: K) -> Result<BtreeNodeRef<'a, K, V>> {
        let mut list = self.nodes.borrow_mut();
        if let Some(node) = list.get(&key) {
            return Ok(node.clone());
        }

        // FIXME: temp allocate a new node with 4096
        let mut v: Rc<Box<Vec<u8>>> = Rc::new(Box::new(Vec::with_capacity(4096)));
        let inner = Rc::make_mut(&mut v);
        for i in 0..4096 {
            inner.push(0);
        }

        let n = Rc::new(RefCell::new(BtreeNode::<K, V>::new(&v)));
        list.insert(key, n.clone());
        self.caches.borrow_mut().push(v);
        Ok(n)
    }

    async fn get_new_node(&self) -> Result<&BtreeNode<K, V>> {
        todo!();
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
            let node = self.get_from_nodes(value.into()).await?;

            if !found {
                (found, index) = (*node).borrow().lookup(key);
            } else {
                index = 0;
            }

            if index < (*node).borrow().get_nchild() {
                value = (*node).borrow().get_val(index);
            } else {
                value = V::default();
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
            let node = self.get_from_nodes(value.into()).await?;
            index = r!(node).get_nchild() - 1;
            value = r!(node).get_val(index);
            path.set_nonroot_node(level, node);
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
                let sib_node = self.get_from_nodes(sib_val.into()).await?;
                if (*sib_node).borrow().has_free_slots() {
                    path.set_sib_node(level, sib_node);
                    path.set_op(level, BtreeMapOp::CarryLeft);
                    return Ok(level);
                }
            }

            // right sibling
            if pindex < (*parent).borrow().get_nchild() - 1 {
                let sib_val = (*parent).borrow().get_val(pindex + 1);
                let sib_node = self.get_from_nodes(sib_val.into()).await?;
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
            let node = self.get_from_nodes(seq).await?;
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
        let node = self.get_from_nodes(seq).await?;
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
    }

    async fn prepare_delete<'s>(&'s self, path: &'s BtreePath<'a, K, V>) -> Result<BtreeLevel> {
        let mut level = BTREE_NODE_LEVEL_MIN;
        let mut dindex = path.get_index(level);
        for mut level in BTREE_NODE_LEVEL_MIN..self.get_root_level() {
            dindex = path.get_index(level);
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
    }

    fn promote_key(&self, path: &BtreePath<'_, K, V>, lvl: usize, key: &K) {
        let mut level = lvl;
        if self.is_nonroot_level(level) {
            loop {
                let index = path.get_index(level);
                // get node @ level
                let node = path.get_nonroot_node(level);
                (*node).borrow_mut().set_key(index, key);

                level += 1;
                if index != 0 || self.is_root_level(level) {
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
}

// all op_* functions
impl<'a, K, V> BtreeMap<'a, K, V>
    where
        K: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        V: Copy + Default + std::fmt::Display + From<K>,
        K: From<V>
{
    fn op_insert(&self, path: &BtreePath<'_, K, V>, level: BtreeLevel, key: &mut K, val: &mut V) {
        let index = path.get_index(level);
        if self.is_nonroot_level(level) {
            // non root node
            let node = path.get_nonroot_node(level);
            let node_key = (*node).borrow().get_key(0);

            (*node).borrow_mut().insert(index, key, val);

            if index == 0 {
                self.promote_key(path, level + 1, &node_key);
            }
        } else {
            // root node
            let root = self.get_root_node();
            (*root).borrow_mut().insert(index, key, val);
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

        path.set_index(level + 1, path.get_index(level + 1) + 1);
        let node_key = r!(right).get_key(0);
        self.promote_key(path, level + 1, &node_key);
        path.set_index(level + 1, path.get_index(level + 1) - 1);

        if mv {
            let sib_node = path.get_sib_node(level);
            path.set_sib_node(level, sib_node);
            path.set_sib_node_none(level);
            path.set_index(level, path.get_index(level) - nchild);
            path.set_index(level + 1, path.get_index(level + 1) - 1);
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
        BtreeNode::move_right(left_ref, node_ref, n);

        }

        let sib_node = path.get_sib_node(level);
        path.set_nonroot_node(level, sib_node);
        path.set_sib_node_none(level);
        path.set_index(level, path.get_index(level) + r!(left).get_nchild());
    }

    fn op_concat_right(&self, path: &BtreePath<K, V>, level: BtreeLevel, key: &mut K, val: &mut V) {
        self.op_delete(path, level, key, val);

        let node = path.get_nonroot_node(level);
        let right = path.get_sib_node(level);

        let n = r!(node).get_nchild();

        {

        let node_ref = &r!(node);
        let right_ref = &r!(right);
        BtreeNode::move_left(node_ref, right_ref, n);

        }

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

        path.set_nonroot_node_none(level);
    }

    fn op_nop(&self, path: &BtreePath<K, V>, level: BtreeLevel, key: &mut K, val: &mut V) {
    }
}

// all up level api
impl<'a, K, V> VMap<K, V> for BtreeMap<'a, K, V>
    where
        K: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        V: Copy + Default + std::fmt::Display + From<K>,
        K: From<V>
{
    fn new(data: Vec<u8>) -> Self {
        let root = BtreeNode::<K, V>::new(&data);
        let mut list = RefCell::new(HashMap::new());

        Self {
            data: data,
            root: Rc::new(RefCell::new(root)),
            nodes: list,
            last_seq: RefCell::new(K::default()),
            caches: RefCell::new(Vec::new()),
        }
    }

    async fn lookup(&self, key: K, level: usize) -> Result<V> {
        let path = BtreePath::new();
        let val = self.do_lookup(&path, &key, level).await?;
        Ok(val)
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
