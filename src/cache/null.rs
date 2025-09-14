use std::io::Result;
use crate::NodeCache;
use super::NodeTieredCacheStats;

#[derive(Clone)]
pub struct NullNodeCache;

impl<P> NodeCache<P> for NullNodeCache {
    fn push(&self, _p: &P, _data: &[u8]) {
    }

    async fn load(&self, _p: &P, _data: &mut [u8]) -> Result<bool> {
        Ok(false)
    }
    
    fn invalid(&self, _p: &P) {
    }

    fn evict(&self) {
    }

    fn get_stats(&self) -> NodeTieredCacheStats {
        NodeTieredCacheStats::default()
    }
}
