use std::io::Result;
use crate::BlockLoader;

// null block loader for test purpose
#[derive(Clone)]
pub struct NullBlockLoader;

impl<V: Send> BlockLoader<V> for NullBlockLoader {
    async fn read(&self, _v: V, _buf: &mut [u8]) -> Result<Vec<(V, Vec<u8>)>> {
        todo!()
    }

    fn from_new_path(self, _: &str) -> Self {
        todo!()
    }
}
