use std::io::{Error, ErrorKind, Result};
use std::collections::HashMap;
#[cfg(feature = "rc")]
use std::rc::Rc;
#[cfg(feature = "rc")]
use std::cell::RefCell;
#[cfg(feature = "arc")]
use atomic_refcell::AtomicRefCell;
#[cfg(feature = "arc")]
use std::sync::Arc;
use crate::BlockLoader;

// memory block loader for test purpose
#[derive(Clone)]
pub struct MemoryBlockLoader<V> {
    #[cfg(feature = "rc")]
    inner: Rc<RefCell<HashMap<V, Vec<u8>>>>,
    #[cfg(feature = "arc")]
    inner: Arc<AtomicRefCell<HashMap<V, Vec<u8>>>>,
    meta_node_size: usize,
}

// force impl Sync for life easy
unsafe impl<V> Sync for MemoryBlockLoader<V> {}

impl<V: Send + Sync + Eq + std::hash::Hash + std::fmt::Display> BlockLoader<V> for MemoryBlockLoader<V>
    where
        u64: From<V>,
{
    async fn read(&self, v: &V, buf: &mut [u8], _user_data: u32) -> Result<Vec<(V, Vec<u8>)>> {
        assert!(buf.len() == self.meta_node_size);
        if let Some(data) =  self.inner.borrow().get(&v) {
            buf.copy_from_slice(data);
            return Ok(Vec::new());
        }
        let msg = format!("requested key {v} not exists");
        return Err(Error::new(ErrorKind::NotFound, msg));
    }

    fn from_new_path(self, _: &str) -> Self {
        todo!()
    }
}

impl<V: Eq + Clone + std::hash::Hash + std::fmt::Display> MemoryBlockLoader<V> {
    pub fn new(meta_node_size: usize) -> Self {
        Self {
            #[cfg(feature = "rc")]
            inner: Rc::new(RefCell::new(HashMap::new())),
            #[cfg(feature = "arc")]
            inner: Arc::new(AtomicRefCell::new(HashMap::new())),
            meta_node_size,
        }
    }

    pub fn write(&self, v: V, buf: &[u8]) {
        assert!(buf.len() == self.meta_node_size);
        let data = buf.to_vec();
        let key = v.clone();
        if let Some(_) = self.inner.borrow_mut().insert(v, data) {
            panic!("key {key} already exists!");
        }
    }
}
