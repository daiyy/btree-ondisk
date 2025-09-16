pub mod null;

use std::sync::{Arc, atomic::AtomicUsize};

#[derive(Default, Debug, Clone)]
pub struct NodeTieredCacheStats {
    total_load: Arc<AtomicUsize>,
    total_hit: Arc<AtomicUsize>,
    total_push: Arc<AtomicUsize>,
    total_remove: Arc<AtomicUsize>,
}
