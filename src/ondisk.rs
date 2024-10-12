
#[derive(Clone, Debug)]
#[repr(C, align(8))]
pub struct NodeHeader {
    pub flags: u8,
    pub level: u8,
    pub nchildren: u16,
    pub userdata: u32,
}
