use std::marker::PhantomData;
use tokio::io::Result;
use crate::VMap;
use crate::InvalidValue;
use crate::direct::DirectMap;
use crate::btree::BtreeMap;

pub struct BMap<T, K, V> {
    inner: T,
    k_mark: PhantomData<V>,
    v_mark: PhantomData<K>,
}

impl<T, K, V> BMap<T, K, V>
    where
        T: VMap<K, V>,
        K: Copy + Default + std::fmt::Display + PartialOrd + Eq + std::hash::Hash + std::ops::AddAssign<u64>,
        V: Copy + Default + std::fmt::Display + From<K> + InvalidValue<V>,
        K: From<V> + Into<u64>
{
    pub fn new(data: Vec<u8>) -> Self {
        Self {
            inner: T::new(data),
            k_mark: PhantomData,
            v_mark: PhantomData,
        }
    }

    pub async fn do_insert(&self, key: K, val: V) -> Result<()> {
        Ok(())
    }
}
