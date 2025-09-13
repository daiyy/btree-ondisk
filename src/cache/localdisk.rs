use std::io::{Error, ErrorKind, Result};
use std::path::Path;
use foyer::{
    HybridCacheBuilder, HybridCache,
    BlockEngineBuilder, FsDeviceBuilder, DeviceBuilder,
    HybridCachePolicy,
};
use crate::NodeCache;

#[derive(Clone)]
pub struct LocalDiskNodeCache {
    hybrid: HybridCache<u64, Vec<u8>>,
}

impl<P: Copy + Into<u64>> NodeCache<P> for LocalDiskNodeCache {
    fn push(&self, p: &P, data: &[u8]) {
        let key = (*p).into();
        self.hybrid.insert(key, data.to_vec());
    }

    async fn load(&self, p: &P, data: &mut [u8]) -> Result<bool> {
        let key = (*p).into();
        match self.hybrid.get(&key).await
                .map_err(|e| {
                    let err_msg = format!("{}", e);
                    Error::new(ErrorKind::Other, err_msg)
                })?
        {
            Some(entry) => {
                data.copy_from_slice(&entry.value());
                Ok(true)
            },
            None => {
                Ok(false)
            },
        }
    }
    
    fn invalid(&self, p: &P) {
        let key = (*p).into();
        self.hybrid.remove(&key);
    }

    fn evict(&self) {
        self.hybrid.memory().evict_all();
    }
}

impl LocalDiskNodeCache {
    pub async fn new(dir: impl AsRef<Path>, capacity: usize) -> Self {
        let cache_device = FsDeviceBuilder::new(dir)
            .with_capacity(capacity)
            .build()
            .expect("failed to build cache device");

        let hybrid: HybridCache<u64, Vec<u8>> = HybridCacheBuilder::new()
            .with_name("btncache")
            .with_policy(HybridCachePolicy::WriteOnInsertion)
            .memory(0)
            .storage()
            .with_engine_config(BlockEngineBuilder::new(cache_device))
            .build()
            .await
            .expect("failed to build cache");

        Self {
            hybrid,
        }
    }

    pub async fn close(&self) {
        self.hybrid.memory().flush().await;
        self.hybrid.close().await.expect("failed to close hybrid");
    }
}
