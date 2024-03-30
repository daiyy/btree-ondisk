
// dirty convert to u64
macro_rules! as_u64 {
    ($x: expr) => {
        unsafe {
            *(&$x as *const _ as *const u64)
        }
    }
}
pub(crate) use as_u64;
