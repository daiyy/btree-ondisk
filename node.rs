use std::ptr;
use std::fmt;
use crate::ondisk::BtreeNodeHeader;

pub const BTREE_NODE_ROOT: u8 = 0x01;
pub const BTREE_NODE_LEVEL_DATA: usize = 0x00;
pub const BTREE_NODE_LEVEL_MIN: usize = BTREE_NODE_LEVEL_DATA + 1;
pub const BTREE_NODE_LEVEL_MAX: usize = 14;

#[derive(Debug)]
#[repr(C, align(8))]
pub struct BtreeNode<'a, K, V> {
    header: &'a mut BtreeNodeHeader,
    keymap: &'a mut [K],
    valmap: &'a mut [V],
    capacity: usize,    // kv capacity of this btree node
}

impl<'a, K, V> BtreeNode<'a, K, V>
    where
        K: Copy + fmt::Display + std::cmp::PartialOrd,
        V: Copy + fmt::Display
{
    pub fn new(buf: &[u8]) -> Self {
        let len = buf.len();
        let hdr_size = std::mem::size_of::<BtreeNodeHeader>();
        if len < hdr_size {
            panic!("input buf size {} smaller than a valid btree node header size {}", len, hdr_size);
        }

        let ptr = buf.as_ptr() as *mut u8;
        let header = unsafe {
            ptr.cast::<BtreeNodeHeader>().as_mut().unwrap()
        };

        let key_size = std::mem::size_of::<K>();
        let val_size = std::mem::size_of::<V>();
        let capacity = (len - hdr_size) / (key_size + val_size);
        assert!(capacity >= header.nchildren as usize,
            "nchildren in header is large than it's capacity {} > {}", header.nchildren, capacity);

        let keymap = unsafe {
            std::slice::from_raw_parts_mut(ptr.add(hdr_size) as *mut K, capacity)
        };

        let valmap = unsafe {
            std::slice::from_raw_parts_mut(ptr.add(hdr_size + capacity * key_size) as *mut V, capacity)
        };

        Self {
            header: header,
            keymap: keymap,
            valmap: valmap,
            capacity: capacity,
        }
    }

    #[inline]
    pub fn is_root(&self) -> bool {
        if (self.header.flags & BTREE_NODE_ROOT) == BTREE_NODE_ROOT {
            return true;
        }
        false
    }

    #[inline]
    pub fn get_flags(&self) -> u8 {
        self.header.flags
    }

    #[inline]
    pub fn set_flags(&mut self, flags: usize) {
        self.header.flags = flags as u8;
    }

    #[inline]
    pub fn get_level(&self) -> usize {
        self.header.level as usize
    }

    #[inline]
    pub fn set_level(&mut self, level: usize) {
        self.header.level = level as u8
    }

    #[inline]
    pub fn get_key(&self, index: usize) -> K {
        self.keymap[index]
    }

    #[inline]
    pub fn set_key(&mut self, index: usize, key: &K) {
        self.keymap[index] = *key;
    }

    #[inline]
    pub fn get_val(&self, index: usize) -> V {
        self.valmap[index]
    }

    #[inline]
    pub fn set_val(&mut self, index: usize, val: &V) {
        self.valmap[index] = *val;
    }

    #[inline]
    pub fn get_nchild(&self) -> usize {
        self.header.nchildren as usize
    }

    #[inline]
    pub fn set_nchild(&mut self, c: usize) {
        self.header.nchildren = c as u16
    }

    #[inline]
    pub fn set_nchild_use_p(&self, c: usize) {
        let nchild_ptr: *mut u16 = &self.header.nchildren as *const _ as *mut u16;
        let nchild = c as u16;
        unsafe {
            std::ptr::copy::<u16>(ptr::addr_of!(nchild), nchild_ptr, 1);
        }
    }

    #[inline]
    pub fn get_capacity(&self) -> usize {
        self.capacity
    }

    #[inline]
    pub fn has_free_slots(&self) -> bool {
        self.get_nchild() < self.capacity
    }

    #[inline]
    pub fn get_nchild_min(&self) -> usize {
        (self.capacity - 1) / 2 + 1
    }

    #[inline]
    pub fn is_overflowing(&self) -> bool {
        self.get_nchild() > self.get_nchild_min()
    }

    #[inline]
    pub fn node_key(&self) -> &K {
        &self.keymap[0]
    }

    #[inline]
    pub fn init(&mut self, flags: usize, level: usize, nchild: usize) {
        self.set_flags(flags);
        self.set_level(level);
        self.set_nchild(nchild);
    }

    #[inline]
    pub fn init_root(&mut self, level: usize) {
        self.header.flags = BTREE_NODE_ROOT;
        self.set_level(level);
    }

    // move n k,v pairs from head of right append to left
    // and move rest of right to it's head
    pub fn move_left(left: &BtreeNode<K, V>, right: &BtreeNode<K, V>, n: usize) {
        let mut lnchild = left.get_nchild();
        let mut rnchild = right.get_nchild();

        let lkeymap_tail_ptr = &left.keymap[lnchild] as *const K as *mut K;
        let lvalmap_tail_ptr = &left.valmap[lnchild] as *const V as *mut V;

        let rkeymap_head_ptr = &right.keymap[0] as *const K as *mut K;
        let rvalmap_head_ptr = &right.valmap[0] as *const V as *mut V;

        let rkeymap_n_ptr = &right.keymap[n] as *const K as *mut K;
        let rvalmap_n_ptr = &right.valmap[n] as *const V as *mut V;

        unsafe {

        // append right to left
        ptr::copy::<K>(rkeymap_head_ptr, lkeymap_tail_ptr, n);
        ptr::copy::<V>(rvalmap_head_ptr, lvalmap_tail_ptr, n);

        // move rest of right to it's head
        ptr::copy::<K>(rkeymap_n_ptr, rkeymap_head_ptr, rnchild - n);
        ptr::copy::<V>(rvalmap_n_ptr, rvalmap_head_ptr, rnchild - n);

        }

        lnchild += n;
        rnchild -= n;

        left.set_nchild_use_p(lnchild);
        right.set_nchild_use_p(rnchild);
    }

    // reserve space at head of right for n slot
    // move n k,v pairs from tail of left to head of right
    pub fn move_right(left: &BtreeNode<K, V>, right: &BtreeNode<K, V>, n: usize) {
        let mut lnchild = left.get_nchild();
        let mut rnchild = right.get_nchild();

        let lkeymap_tailn_ptr = &left.keymap[lnchild - n] as *const K as *mut K;
        let lvalmap_tailn_ptr = &left.valmap[lnchild - n] as *const V as *mut V;

        let rkeymap_head_ptr = &right.keymap[0] as *const K as *mut K;
        let rvalmap_head_ptr = &right.valmap[0] as *const V as *mut V;

        let rkeymap_n_ptr = &right.keymap[n] as *const K as *mut K;
        let rvalmap_n_ptr = &right.valmap[n] as *const V as *mut V;

        unsafe {

        // reserve n slot by move all child from head to n
        std::ptr::copy::<K>(rkeymap_head_ptr, rkeymap_n_ptr, rnchild);
        std::ptr::copy::<V>(rvalmap_head_ptr, rvalmap_n_ptr, rnchild);

        // move n k,v pairs from tail of left to head of right
        std::ptr::copy::<K>(lkeymap_tailn_ptr, rkeymap_head_ptr, n);
        std::ptr::copy::<V>(lvalmap_tailn_ptr, rvalmap_head_ptr, n);

        }

        lnchild -= n;
        rnchild += n;

        left.set_nchild_use_p(lnchild);
        right.set_nchild_use_p(rnchild);
    }

    // lookup key
    // @return:
    //   - (found, index)
    //   - (notfound, index)
    pub fn lookup(&self, key: &K) -> (bool, usize) {
        let mut low: isize = 0;
        let mut high: isize = (self.header.nchildren - 1) as isize;
        let mut s = false;
        let mut index = 0;

        while low <= high {
            index = (low + high) / 2;
            let nkey = self.get_key(index as usize);
            if &nkey == key {
                return (true, index as usize);
            } else if &nkey < key {
                low = index + 1;
                s = false;
            } else {
                high = index - 1;
                s = true;
            }
        }

        if self.get_level() > BTREE_NODE_LEVEL_MIN {
            if s && index > 0 {
                index -= 1;
            }
        } else if s == false {
            index += 1;
        }

        return (false, index as usize);
    }

    // insert key val @ index
    pub fn insert(&mut self, index: usize, key: &K, val: &V) {
        let mut nchild = self.get_nchild();

        if index < nchild {
            unsafe {
                let ksrc: *const K = &self.keymap[index] as *const K;
                let vsrc: *const V = &self.valmap[index] as *const V;

                let kdst: *mut K = &mut self.keymap[index + 1] as *mut K;
                let vdst: *mut V = &mut self.valmap[index + 1] as *mut V;

                let count = nchild - index;

                std::ptr::copy::<K>(ksrc, kdst, count);
                std::ptr::copy::<V>(vsrc, vdst, count);
            }
        }

        self.set_key(index, key);
        self.set_val(index, val);
        nchild += 1;
        self.set_nchild(nchild);
    }

    // delete key val @ index
    pub fn delete(&mut self, index: usize, key: &mut K, val: &mut V) {
        let mut nchild = self.get_nchild();

        *key = self.get_key(index);
        *val = self.get_val(index);

        if index < nchild {
            unsafe {
                let ksrc: *const K = &self.keymap[index + 1] as *const K;
                let vsrc: *const V = &self.valmap[index + 1] as *const V;

                let kdst: *mut K = &mut self.keymap[index] as *mut K;
                let vdst: *mut V = &mut self.valmap[index] as *mut V;

                let count = nchild - index - 1;

                std::ptr::copy::<K>(ksrc, kdst, count);
                std::ptr::copy::<V>(vsrc, vdst, count);
            }
        }

        nchild -= 1;
        self.set_nchild(nchild);
    }
}

impl<'a, K, V> PartialEq for BtreeNode<'a, K, V> {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::addr_of!(self.header) == std::ptr::addr_of!(other.header)
    }
}

impl<'a, K, V> fmt::Display for BtreeNode<'a, K, V>
    where
        K: Copy + fmt::Display + std::cmp::PartialOrd,
        V: Copy + fmt::Display
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "===== dump btree node @{:?} ====\n", self.header as *const BtreeNodeHeader)?;
        write!(f, "  flags: {},  level: {}, nchildren: {}, capacity: {}\n",
            self.header.flags, self.header.level, self.header.nchildren, self.capacity)?;
        for idx in 0..self.header.nchildren.into() {
            write!(f, "{:3}   {:20}   {:20}\n", idx, self.get_key(idx), self.get_val(idx))?;
        }
        write!(f, "")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node() {
    }
}
