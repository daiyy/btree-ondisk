use std::collections::HashMap;
use std::cell::RefCell;
use std::rc::Rc;
use std::ops::{Deref, DerefMut};
use std::borrow::{Borrow, BorrowMut};
use tokio::io::{Error, ErrorKind, Result};
use crate::node::*;

type BtreeLevel = usize;
type BtreeNodeRef<'a, K, V> = Rc<RefCell<BtreeNode<'a, K, V>>>;

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
        let mut l = Vec::new();
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
        self.levels[level].borrow_mut().node.as_mut().unwrap().clone()
    }

    pub fn set_nonroot_node(&self, level: usize, node: BtreeNodeRef<'a, K, V>) {
        self.levels[level].borrow_mut().node = Some(node);
    }

    pub fn get_sib_node(&self, level: usize) -> BtreeNodeRef<'a, K, V> {
        self.levels[level].borrow_mut().sib_node.as_ref().unwrap().clone()
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
    pub nodes: HashMap<K, BtreeNodeRef<'a, K, V>>, // list of btree node in memory
    pub nchild_max: usize,
    pub last_seq: RefCell<K>,
}

impl<'a, K, V> BtreeMap<'a, K, V>
    where
        K: Copy + Default + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        V: Copy + From<K>,
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

    // FIXME
    async fn get_from_nodes(&self, key: K) -> Result<BtreeNodeRef<'a, K, V>> {
        let mut clone = self.nodes.get(&key).unwrap().clone();
        Ok(clone)
        //Err(Error::new(ErrorKind::NotFound, ""))
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
            BtreeMapOp::BorrowLeft => self.op_borrow_left(path, level, key, val),
            BtreeMapOp::BorrowRight => self.op_borrow_right(path, level, key, val),
            BtreeMapOp::Delete => self.op_delete(path, level, key, val),
            BtreeMapOp::Nop => self.op_nop(path, level, key, val),
            _ => panic!("{:?} not yet implement", path.get_op(level)),
        }
    }

    async fn do_lookup<'s>(&'s self, path: &'s BtreePath<'a, K, V>, key: &K, minlevel: usize) -> Result<V> {
        let root = self.get_root_node();
        let mut level = (*root).borrow().get_level();
        if level < minlevel {
            return Err(Error::new(ErrorKind::NotFound, ""));
        }

        let (mut found, mut index) = (*root).borrow().lookup(key);
        assert!(found == false);
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

            value = (*node).borrow().get_val(index);
            path.set_nonroot_node(level, node);
            path.set_index(level, index);
            level -= 1;
        }

        if !found {
            return Err(Error::new(ErrorKind::NotFound, ""));
        }

        Ok(value)
    }

    async fn prepare_insert<'s>(&'s self, path: &'s BtreePath<'a, K, V>, key: &K) -> Result<BtreeLevel> {

        let mut level = BTREE_NODE_LEVEL_DATA;

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
}

// all op_* functions
impl<'a, K, V> BtreeMap<'a, K, V>
    where
        K: Copy + Default + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        V: Copy + From<K>,
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

        (*root).borrow_mut().set_level(level + 1);

        let sib_node = path.get_sib_node(level);
        path.set_nonroot_node(level, sib_node);
        path.set_sib_node_none(level);

        self.op_insert(path, level, key, val);

        let root_ref = &(*root).borrow();
        let child_ref = &(*child).borrow();
        BtreeNode::move_right(root_ref, child_ref, n);

        *key = (*child).borrow().get_key(0);
        *val = path.get_new_seq(level).into();
    }

    fn op_split(&self, path: &BtreePath<K, V>, level: BtreeLevel, key: &mut K, val: &mut V) {
    }

    fn op_carry_left(&self, path: &BtreePath<K, V>, level: BtreeLevel, key: &mut K, val: &mut V) {
    }

    fn op_carry_right(&self, path: &BtreePath<K, V>, level: BtreeLevel, key: &mut K, val: &mut V) {
    }

    fn op_borrow_left(&self, path: &BtreePath<K, V>, level: BtreeLevel, key: &mut K, val: &mut V) {
    }

    fn op_borrow_right(&self, path: &BtreePath<K, V>, level: BtreeLevel, key: &mut K, val: &mut V) {
    }

    fn op_delete(&self, path: &BtreePath<K, V>, level: BtreeLevel, key: &mut K, val: &mut V) {
    }

    fn op_nop(&self, path: &BtreePath<K, V>, level: BtreeLevel, key: &mut K, val: &mut V) {
    }
}

// all up level api
impl<'a, K, V> BtreeMap<'a, K, V>
    where
        K: Copy + Default + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        V: Copy + From<K>,
        K: From<V>
{
    pub fn new(data: Vec<u8>) -> Self {
        let root = BtreeNode::<K, V>::new(&data);
        let mut list = HashMap::new();

        Self {
            data: data,
            root: Rc::new(RefCell::new(root)),
            nodes: list,
            nchild_max: 32,
            last_seq: RefCell::new(K::default()),
        }
    }

    pub async fn insert(&self, key: K, val: V) -> Result<()> {
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
}
