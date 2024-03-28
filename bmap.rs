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
        // convert old root node to btree node
        let mut btree = BtreeMap {
            root: Rc::new(RefCell::new(BtreeNode::<K, V>::new(&data))),
            data: data,
            nodes: RefCell::new(HashMap::new()),
            last_seq: RefCell::new(last_seq),
        };
        // insert k, v
        let res = btree.insert(key, val).await?;
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
}
