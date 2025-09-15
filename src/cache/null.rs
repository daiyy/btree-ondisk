use std::io::Result;
use std::sync::atomic::Ordering;
use crate::NodeCache;
use super::NodeTieredCacheStats;

#[derive(Clone)]
pub struct NullNodeCache;

impl<P: Send> NodeCache<P> for NullNodeCache {
    fn push(&self, _p: &P, _data: &[u8]) {
    }

    async fn load(&self, _p: P, _data: &mut [u8]) -> Result<bool> {
        Ok(false)
    }
    
    fn invalid(&self, _p: &P) {
    }

    fn evict(&self) {
    }

    fn get_stats(&self) -> NodeTieredCacheStats {
        let stats = NodeTieredCacheStats::default();
        let _ = stats.total_push.load(Ordering::SeqCst);
        let _ = stats.total_load.load(Ordering::SeqCst);
        let _ = stats.total_hit.load(Ordering::SeqCst);
        let _ = stats.total_remove.load(Ordering::SeqCst);
        stats
    }
}
