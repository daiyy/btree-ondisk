use std::io::{Error, ErrorKind, Result};
use std::sync::atomic::Ordering;
use std::path::Path;
use foyer::{
    HybridCacheBuilder, HybridCache,
    BlockEngineBuilder, FsDeviceBuilder, DeviceBuilder,
    HybridCachePolicy, HybridCacheProperties, Location,
};
use crate::NodeCache;
use super::NodeTieredCacheStats;

#[derive(Clone)]
pub struct LocalDiskNodeCache {
    hybrid: HybridCache<u64, Vec<u8>>,
    stats: NodeTieredCacheStats,
}

#[derive(PartialEq)]
pub enum LocalDiskNodeCacheOpenMode {
    ReuseCache,
    ReuseSpace,
    Recreate,
}

impl<P: Copy + Into<u64>> NodeCache<P> for LocalDiskNodeCache {
    fn push(&self, p: &P, data: &[u8]) {
        let key = (*p).into();
        self.stats.total_push.fetch_add(1, Ordering::SeqCst);
        self.hybrid.insert_with_properties(key, data.to_vec(),
            HybridCacheProperties::default()
                .with_ephemeral(false)
                .with_location(Location::OnDisk)
        );
    }

    async fn load(&self, p: &P, data: &mut [u8]) -> Result<bool> {
        let key = (*p).into();
        self.stats.total_load.fetch_add(1, Ordering::SeqCst);
        match self.hybrid.get(&key).await
                .map_err(|e| {
                    let err_msg = format!("{}", e);
                    Error::new(ErrorKind::Other, err_msg)
                })?
        {
            Some(entry) => {
                self.stats.total_hit.fetch_add(1, Ordering::SeqCst);
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
        self.stats.total_remove.fetch_add(1, Ordering::SeqCst);
        self.hybrid.remove(&key);
    }

    fn evict(&self) {
        self.hybrid.memory().evict_all();
    }

    fn get_stats(&self) -> NodeTieredCacheStats {
        self.stats.clone()
    }
}

impl LocalDiskNodeCache {
    pub async fn new(dir: impl AsRef<Path>, capacity: usize, mode: LocalDiskNodeCacheOpenMode) -> Self {
        if mode == LocalDiskNodeCacheOpenMode::Recreate {
            let _ = tokio::fs::remove_dir_all(&dir).await;
        }

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

        if mode == LocalDiskNodeCacheOpenMode::ReuseSpace {
            let _ = hybrid.storage().destroy().await.expect("failed to clear cache");
            let _ = hybrid.storage().wait().await;
        }

        Self {
            hybrid,
            stats: NodeTieredCacheStats::default(),
        }
    }

    pub async fn close(&self) {
        self.hybrid.memory().flush().await;
        self.hybrid.close().await.expect("failed to close hybrid");
    }
}
