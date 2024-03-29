
macro_rules! dirty_u64 {
    ($x: expr) => {
        unsafe {
            *(&$x as *const _ as *const u64)
        }
    }
}
pub(crate) use dirty_u64;
