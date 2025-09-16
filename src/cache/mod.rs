pub mod null;

use std::sync::{Arc, atomic::AtomicUsize};

#[derive(Default, Debug, Clone)]
pub struct NodeTieredCacheStats {
    pub total_load: Arc<AtomicUsize>,
    pub total_hit: Arc<AtomicUsize>,
    pub total_push: Arc<AtomicUsize>,
    pub total_remove: Arc<AtomicUsize>,
}
