
// dirty convert to u64
#[allow(unused_macros)]
macro_rules! as_u64 {
    ($x: expr) => {
        unsafe {
            *(&$x as *const _ as *const u64)
        }
    }
}

#[allow(unused_imports)]
pub(crate) use as_u64;
