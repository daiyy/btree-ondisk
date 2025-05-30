use std::ptr;
use std::fmt;
use std::marker::PhantomPinned;
use std::marker::PhantomData;
use crate::ondisk::NodeHeader;
use crate::NodeValue;

pub const BTREE_NODE_FLAG_LARGE: u8 = 0b0000_0001;
pub const BTREE_NODE_FLAG_LEAF: u8 = 0b0000_0010;
pub const BTREE_NODE_LEVEL_DATA: usize = 0x00;
pub const BTREE_NODE_LEVEL_MIN: usize = BTREE_NODE_LEVEL_DATA + 1;
pub const BTREE_NODE_LEVEL_MAX: usize = 14;
pub const BTREE_NODE_LEVEL_LEAF: usize = BTREE_NODE_LEVEL_MIN;

const MIN_ALIGNED: usize = 8;

/// btree node descriptor for memory pointer, normally a page
///
/// SAFETY:
///   node operations mutable by unsafe code,
///   this works because all ops for same node are expected to be ran in a single thread
///
#[derive(Debug)]
#[repr(C, align(8))]
pub struct BtreeNode<'a, K, V, P> {
    header: &'a mut NodeHeader,
    keymap: &'a mut [K],
    valptr: *mut u8,
    capacity: usize,    // kv capacity of this btree node
    ptr: *const u8,
    size: usize,
    id: P,
    dirty: bool,
    _pin: PhantomPinned,
    phantom: PhantomData<V>,
}

#[cfg(feature = "arc")]
unsafe impl<'a, K, V, P> Send for BtreeNode<'a, K, V, P> {}
#[cfg(feature = "arc")]
unsafe impl<'a, K, V, P> Sync for BtreeNode<'a, K, V, P> {}

impl<'a, K, V, P> BtreeNode<'a, K, V, P>
    where
        K: Copy + fmt::Display + std::cmp::PartialOrd,
        V: Copy + fmt::Display + NodeValue,
        P: Copy + fmt::Display + NodeValue,
{
    pub fn from_slice(buf: &[u8]) -> Self {
        let len = buf.len();
        let hdr_size = std::mem::size_of::<NodeHeader>();
        if len < hdr_size {
            panic!("input buf size {} smaller than a valid btree node header size {}", len, hdr_size);
        }

        let ptr = buf.as_ptr() as *mut u8;
        let header = unsafe {
            ptr.cast::<NodeHeader>().as_mut().unwrap()
        };

        let key_size = std::mem::size_of::<K>();
        let val_size = if header.flags & BTREE_NODE_FLAG_LEAF > 0 {
            std::mem::size_of::<V>()
        } else {
            std::mem::size_of::<P>()
        };
        let capacity = (len - hdr_size) / (key_size + val_size);
        assert!(capacity >= header.nchildren as usize,
            "nchildren in header is large than it's capacity {} > {}", header.nchildren, capacity);

        let keymap = unsafe {
            std::slice::from_raw_parts_mut(ptr.add(hdr_size) as *mut K, capacity)
        };

        let valptr = unsafe {
            ptr.add(hdr_size + capacity * key_size)
        };

        Self {
            header: header,
            keymap: keymap,
            valptr: valptr,
            capacity: capacity,
            ptr: std::ptr::null(),
            size: len,
            id: P::invalid_value(),
            dirty: false,
            _pin: PhantomPinned,
            phantom: PhantomData,
        }
    }

    pub fn new(size: usize) -> Option<Self> {
        if let Ok(aligned_layout) = std::alloc::Layout::from_size_align(size, MIN_ALIGNED) {
            let ptr = unsafe { std::alloc::alloc_zeroed(aligned_layout) };
            if ptr.is_null() {
                return None;
            }

            let data = unsafe { std::slice::from_raw_parts(ptr, size) };
            let mut node = Self::from_slice(data);
            node.ptr = ptr;
            node.id = P::invalid_value();
            return Some(node);
        };
        None
    }

    pub fn new_with_id(size: usize, id: &P) -> Option<Self> {
        if let Some(node) = Self::new(size) {
            node.set_id(*id);
            return Some(node);
        }
        None
    }

    pub fn copy_from_slice(id: P, buf: &[u8]) -> Option<Self> {
        let size = buf.len();
        if let Ok(aligned_layout) = std::alloc::Layout::from_size_align(size, MIN_ALIGNED) {
            let ptr = unsafe { std::alloc::alloc_zeroed(aligned_layout) };
            if ptr.is_null() {
                return None;
            }

            let data = unsafe { std::slice::from_raw_parts_mut(ptr, size) };
            // copy data from buf to inner data
            data.copy_from_slice(buf);
            let mut node = Self::from_slice(data);
            node.ptr = ptr;
            node.id = id;
            return Some(node);
        };
        None
    }

    pub fn as_ref(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self.ptr as *const u8, self.size)
        }
    }

    pub fn as_mut(&self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(self.ptr as *mut u8, self.size)
        }
    }

    #[inline]
    pub fn is_leaf(&self) -> bool {
        if (self.header.flags & BTREE_NODE_FLAG_LEAF) == BTREE_NODE_FLAG_LEAF {
            return true;
        }
        false
    }

    #[inline]
    pub fn set_leaf(&self) {
        let ptr = ptr::addr_of!(self.header.flags) as *mut u8;
        unsafe {
            let mut flags = self.header.flags;
            flags |= BTREE_NODE_FLAG_LEAF;
            ptr::write_volatile(ptr, flags);
        }
    }

    #[inline]
    pub fn clear_leaf(&self) {
        let ptr = ptr::addr_of!(self.header.flags) as *mut u8;
        unsafe {
            let mut flags = self.header.flags;
            flags &= !BTREE_NODE_FLAG_LEAF;
            ptr::write_volatile(ptr, flags);
        }
    }

    #[inline]
    pub fn is_large(&self) -> bool {
        if (self.header.flags & BTREE_NODE_FLAG_LARGE) == BTREE_NODE_FLAG_LARGE {
            return true;
        }
        false
    }

    #[inline]
    pub fn set_large(&self) {
        let ptr = ptr::addr_of!(self.header.flags) as *mut u8;
        unsafe {
            let mut flags = self.header.flags;
            flags |= BTREE_NODE_FLAG_LARGE;
            ptr::write_volatile(ptr, flags);
        }
    }

    #[inline]
    pub fn clear_large(&self) {
        let ptr = ptr::addr_of!(self.header.flags) as *mut u8;
        unsafe {
            let mut flags = self.header.flags;
            flags &= !BTREE_NODE_FLAG_LARGE;
            ptr::write_volatile(ptr, flags);
        }
    }

    #[inline]
    pub fn get_flags(&self) -> u8 {
        self.header.flags
    }

    #[inline]
    pub fn set_flags(&self, flags: u8) {
        let ptr = ptr::addr_of!(self.header.flags) as *mut u8;
        unsafe {
            ptr::write_volatile(ptr, flags as u8);
        }
    }

    #[inline]
    pub fn get_level(&self) -> usize {
        self.header.level as usize
    }

    #[inline]
    pub fn set_level(&self, level: usize) {
        let ptr = ptr::addr_of!(self.header.level) as *mut u8;
        unsafe {
            ptr::write_volatile(ptr, level as u8);
        }
    }

    #[inline]
    pub fn get_key(&self, index: usize) -> &K {
        &self.keymap[index]
    }

    #[inline]
    pub fn set_key(&self, index: usize, key: &K) {
         unsafe {
            ptr::copy_nonoverlapping(
                ptr::addr_of!(*key),
                ptr::addr_of!(self.keymap[index]) as *mut K,
                1
            )
        }
    }

    #[inline]
    pub fn get_val<X>(&self, index: usize) -> &X {
        let slice = unsafe {
            std::slice::from_raw_parts(self.valptr as *const X, self.capacity)
        };
        &slice[index]
    }

    #[inline]
    pub fn set_val<X>(&self, index: usize, val: &X) {
        unsafe {
            let dst = (self.valptr as *mut X).add(index);
            ptr::copy_nonoverlapping(ptr::addr_of!(*val), dst, 1)
        }
    }

    #[inline]
    pub fn get_nchild(&self) -> usize {
        self.header.nchildren as usize
    }

    #[inline]
    pub fn set_nchild(&self, c: usize) {
        let ptr = ptr::addr_of!(self.header.nchildren) as *mut u16;
        unsafe {
            ptr::write_volatile(ptr, c as u16);
        }
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
    pub fn get_userdata(&self) -> u32 {
        self.header.userdata
    }

    #[inline]
    pub fn set_userdata(&self, data: u32) {
        let ptr = ptr::addr_of!(self.header.userdata) as *mut u32;
        unsafe {
            ptr::write_volatile(ptr, data);
        }
    }

    #[inline]
    pub fn get_capacity(&self) -> usize {
        self.capacity
    }

    #[inline]
    // dynamic calc V capacity
    pub fn get_v_capacity(&self) -> usize {
        let hdr_size = std::mem::size_of::<NodeHeader>();
        let key_size = std::mem::size_of::<K>();
        let val_size = std::mem::size_of::<V>();
        (self.size - hdr_size) / (key_size + val_size)
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
    pub fn id(&self) -> &P {
        &self.id
    }

    #[inline]
    pub fn set_id(&self, id: P) {
        let ptr = ptr::addr_of!(self.id) as *mut P;
        unsafe {
            ptr::write_volatile(ptr, id);
        }
    }

    #[inline]
    // re-calc capacity and valptr by flags
    pub(crate) fn do_update(&mut self) {
        let len = self.size;
        let hdr_size = std::mem::size_of::<NodeHeader>();
        if len < hdr_size {
            panic!("input buf size {} smaller than a valid btree node header size {}", len, hdr_size);
        }

        let ptr = ptr::addr_of!(self.header.flags) as *mut u8;

        let key_size = std::mem::size_of::<K>();
        let val_size = if self.get_level() == BTREE_NODE_LEVEL_LEAF {
            assert!(self.get_flags() & BTREE_NODE_FLAG_LEAF == BTREE_NODE_FLAG_LEAF);
            std::mem::size_of::<V>()
        } else {
            assert!(self.get_flags() & BTREE_NODE_FLAG_LEAF != BTREE_NODE_FLAG_LEAF);
            std::mem::size_of::<P>()
        };
        let capacity = (len - hdr_size) / (key_size + val_size);

        self.keymap = unsafe {
            std::slice::from_raw_parts_mut(ptr.add(hdr_size) as *mut K, capacity)
        };
        self.valptr = unsafe {
            ptr.add(hdr_size + capacity * key_size)
        };
        self.capacity = capacity;
    }

    #[inline]
    // re-calc capacity and valptr based on X
    pub(crate) fn do_reinit<X>(&mut self) {
        let len = self.size;
        let hdr_size = std::mem::size_of::<NodeHeader>();
        if len < hdr_size {
            panic!("input buf size {} smaller than a valid btree node header size {}", len, hdr_size);
        }

        let ptr = ptr::addr_of!(self.header.flags) as *mut u8;

        let key_size = std::mem::size_of::<K>();
        let val_size = std::mem::size_of::<X>();
        let capacity = (len - hdr_size) / (key_size + val_size);

        self.keymap = unsafe {
            std::slice::from_raw_parts_mut(ptr.add(hdr_size) as *mut K, capacity)
        };
        self.valptr = unsafe {
            ptr.add(hdr_size + capacity * key_size)
        };
        self.capacity = capacity;
    }

    #[inline]
    pub fn init(&self, level: usize, nchild: usize) {
        if level == BTREE_NODE_LEVEL_LEAF {
            self.set_leaf();
        }
        self.set_level(level);
        self.set_nchild(nchild);
    }

    #[inline]
    pub fn init_root(&self, level: usize, is_large: bool) {
        if level == BTREE_NODE_LEVEL_LEAF {
            self.set_leaf();
        }
        if is_large {
            self.set_large();
        }
        self.set_level(level);
    }

    #[inline]
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    #[inline]
    pub fn mark_dirty(&self) {
        let ptr = ptr::addr_of!(self.dirty) as *mut bool;
        unsafe {
            ptr::write_volatile(ptr, true);
        }
    }

    #[inline]
    pub fn clear_dirty(&self) {
        let ptr = ptr::addr_of!(self.dirty) as *mut bool;
        unsafe {
            ptr::write_volatile(ptr, false);
        }
    }

    #[inline]
    // move n k,v pairs from head of right append to left
    // and move rest of right to it's head
    fn do_move_left<X>(left: &BtreeNode<K, V, P>, right: &BtreeNode<K, V, P>, n: usize) {

        // input param protection
        if n == 0 { return; }

        assert!(left.is_leaf() == right.is_leaf());

        let mut lnchild = left.get_nchild();
        let mut rnchild = right.get_nchild();

        unsafe {

        let lkeymap_tail_ptr = &left.keymap[lnchild] as *const K as *mut K;
        let lvalmap_tail_ptr = (left.valptr as *mut X).add(lnchild);

        let rkeymap_head_ptr = &right.keymap[0] as *const K as *mut K;
        let rvalmap_head_ptr = right.valptr as *mut X;

        let rkeymap_n_ptr = &right.keymap[n] as *const K as *mut K;
        let rvalmap_n_ptr = (right.valptr as *mut X).add(n);


        // append right to left
        ptr::copy::<K>(rkeymap_head_ptr, lkeymap_tail_ptr, n);
        ptr::copy::<X>(rvalmap_head_ptr, lvalmap_tail_ptr, n);

        // move rest of right to it's head
        ptr::copy::<K>(rkeymap_n_ptr, rkeymap_head_ptr, rnchild - n);
        ptr::copy::<X>(rvalmap_n_ptr, rvalmap_head_ptr, rnchild - n);

        }

        lnchild += n;
        rnchild -= n;

        left.set_nchild_use_p(lnchild);
        right.set_nchild_use_p(rnchild);
    }

    pub fn move_left(left: &BtreeNode<K, V, P>, right: &BtreeNode<K, V, P>, n: usize) {
        if left.is_leaf() && right.is_leaf() {
            Self::do_move_left::<V>(left, right, n);
        } else if !left.is_leaf() && !right.is_leaf() {
            Self::do_move_left::<P>(left, right, n);
        } else {
            panic!("left node is leaf {}, right node is leaf {}, not consistent", left.is_leaf(), right.is_leaf());
        }
    }

    #[inline]
    // reserve space at head of right for n slot
    // move n k,v pairs from tail of left to head of right
    fn do_move_right<X>(left: &BtreeNode<K, V, P>, right: &BtreeNode<K, V, P>, n: usize) {

        // input param protection
        if n == 0 { return; }

        let mut lnchild = left.get_nchild();
        let mut rnchild = right.get_nchild();

        unsafe {

        let lkeymap_tailn_ptr = &left.keymap[lnchild - n] as *const K as *mut K;
        let lvalmap_tailn_ptr = (left.valptr as *mut X).add(lnchild - n);

        let rkeymap_head_ptr = &right.keymap[0] as *const K as *mut K;
        let rvalmap_head_ptr = right.valptr as *mut X;

        let rkeymap_n_ptr = &right.keymap[n] as *const K as *mut K;
        let rvalmap_n_ptr = (right.valptr as *mut X).add(n);


        // reserve n slot by move all child from head to n
        std::ptr::copy::<K>(rkeymap_head_ptr, rkeymap_n_ptr, rnchild);
        std::ptr::copy::<X>(rvalmap_head_ptr, rvalmap_n_ptr, rnchild);

        // move n k,v pairs from tail of left to head of right
        std::ptr::copy::<K>(lkeymap_tailn_ptr, rkeymap_head_ptr, n);
        std::ptr::copy::<X>(lvalmap_tailn_ptr, rvalmap_head_ptr, n);

        }

        lnchild -= n;
        rnchild += n;

        left.set_nchild_use_p(lnchild);
        right.set_nchild_use_p(rnchild);
    }

    pub fn move_right(left: &BtreeNode<K, V, P>, right: &BtreeNode<K, V, P>, n: usize) {
        if left.is_leaf() && right.is_leaf() {
            Self::do_move_right::<V>(left, right, n);
        } else if !left.is_leaf() && !right.is_leaf() {
            Self::do_move_right::<P>(left, right, n);
        } else {
            panic!("left node is leaf {}, right node is leaf {}, not consistent", left.is_leaf(), right.is_leaf());
        }
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
            if nkey == key {
                return (true, index as usize);
            } else if nkey < key {
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
    pub fn insert<X>(&self, index: usize, key: &K, val: &X) {
        let mut nchild = self.get_nchild();

        if index < nchild {
            unsafe {
                let ksrc: *const K = &self.keymap[index] as *const K;
                let vsrc: *const X = (self.valptr as *const X).add(index);

                let kdst: *mut K = ptr::addr_of!(self.keymap[index + 1]) as *mut K;
                let vdst: *mut X = (self.valptr as *mut X).add(index + 1);

                let count = nchild - index;

                std::ptr::copy::<K>(ksrc, kdst, count);
                std::ptr::copy::<X>(vsrc, vdst, count);
            }
        }

        self.set_key(index, key);
        self.set_val(index, val);
        nchild += 1;
        self.set_nchild(nchild);
    }

    // delete key val @ index
    pub fn delete<X: Copy>(&self, index: usize, key: &mut K, val: &mut X) {
        let mut nchild = self.get_nchild();

        *key = *self.get_key(index);
        *val = *self.get_val(index);

        if index < nchild - 1 {
            unsafe {
                let ksrc: *const K = &self.keymap[index + 1] as *const K;
                let vsrc: *const X = (self.valptr as *const X).add(index + 1);

                let kdst: *mut K = ptr::addr_of!(self.keymap[index]) as *mut K;
                let vdst: *mut X = (self.valptr as *mut X).add(index);

                let count = nchild - index - 1;

                std::ptr::copy::<K>(ksrc, kdst, count);
                std::ptr::copy::<X>(vsrc, vdst, count);
            }
        }

        nchild -= 1;
        self.set_nchild(nchild);
    }
}

impl<'a, K, V, P> Drop for BtreeNode<'a, K, V, P> {
    fn drop(&mut self) {
        if self.ptr.is_null() {
            return;
        }
        if let Ok(layout) = std::alloc::Layout::from_size_align(self.size, MIN_ALIGNED) {
            unsafe { std::alloc::dealloc(self.ptr as *mut u8, layout) };
        }
    }
}

impl<'a, K, V, P> PartialEq for BtreeNode<'a, K, V, P> {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::addr_of!(self.header) == std::ptr::addr_of!(other.header)
    }
}

impl<'a, K, V, P> fmt::Display for BtreeNode<'a, K, V, P>
    where
        K: Copy + fmt::Display + std::cmp::PartialOrd,
        V: Copy + fmt::Display + NodeValue,
        P: Copy + fmt::Display + NodeValue,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.is_large() {
            write!(f, "===== dump btree node @{:?} ROOT ====\n", self.header as *const NodeHeader)?;
        } else {
            write!(f, "===== dump btree node @{:?} id {} ====\n", self.header as *const NodeHeader, self.id())?;
        }
        write!(f, "  flags: {},  level: {}, nchildren: {}, capacity: {}, is leaf: {}\n",
            self.header.flags, self.header.level, self.header.nchildren, self.capacity, self.is_leaf())?;
        for idx in 0..self.header.nchildren.into() {
            if self.is_leaf() {
                write!(f, "{:3}   {:20}   {:20}\n", idx, self.get_key(idx), self.get_val::<K>(idx))?;
            } else {
                write!(f, "{:3}   {:20}   {:20}\n", idx, self.get_key(idx), self.get_val::<P>(idx))?;
            }
        }
        write!(f, "")
    }
}

/// direct node descriptor for memory pointer, normally a tiny memory buffer
///
/// SAFETY:
///   node operations in immutable by unsafe code,
///   this works because all ops for same node are expected to be ran in a single thread
///
#[derive(Debug)]
#[repr(C, align(8))]
pub struct DirectNode<'a, V> {
    header: &'a mut NodeHeader,
    valmap: &'a mut [V],
    capacity: usize,
    ptr: *const u8,
    size: usize,
    dirty: bool,
    _pin: PhantomPinned,
}

#[cfg(feature = "arc")]
unsafe impl<'a, V> Send for DirectNode<'a, V> {}
#[cfg(feature = "arc")]
unsafe impl<'a, V> Sync for DirectNode<'a, V> {}

impl<'a, V> DirectNode<'a, V>
    where
        V: Copy + fmt::Display
{
    pub fn from_slice(buf: &[u8]) -> Self {
        let len = buf.len();
        let hdr_size = std::mem::size_of::<NodeHeader>();
        if len < hdr_size {
            panic!("input buf size {} smaller than a valid btree node header size {}", len, hdr_size);
        }

        let ptr = buf.as_ptr() as *mut u8;
        let header = unsafe {
            ptr.cast::<NodeHeader>().as_mut().unwrap()
        };

        let val_size = std::mem::size_of::<V>();
        let capacity = (len - hdr_size) / val_size;
        assert!(capacity >= header.nchildren as usize,
            "nchildren in header is large than it's capacity {} > {}", header.nchildren, capacity);

        let valmap = unsafe {
            std::slice::from_raw_parts_mut(ptr.add(hdr_size) as *mut V, capacity)
        };

        Self {
            header: header,
            valmap: valmap,
            capacity: capacity,
            ptr: std::ptr::null(),
            size: len,
            dirty: false,
            _pin: PhantomPinned,
        }
    }

    pub fn new(size: usize) -> Option<Self> {
        if let Ok(aligned_layout) = std::alloc::Layout::from_size_align(size, MIN_ALIGNED) {
            let ptr = unsafe { std::alloc::alloc_zeroed(aligned_layout) };
            if ptr.is_null() {
                return None;
            }

            let data = unsafe { std::slice::from_raw_parts(ptr, size) };
            let mut node = Self::from_slice(data);
            node.ptr = ptr;
            return Some(node);
        };
        None
    }

    pub fn copy_from_slice(buf: &[u8]) -> Option<Self> {
        let size = buf.len();
        if let Some(n) = Self::new(size) {
            // copy data from buf to inner data
            let data = n.as_mut();
            data.copy_from_slice(buf);
            return Some(n);
        }
        None
    }

    #[inline]
    pub fn init(&self, flags: usize, level: usize, nchild: usize) {
        unsafe {
            let ptr = ptr::addr_of!(self.header.flags) as *mut u8;
            ptr::write_volatile(ptr, flags as u8);
            let ptr = ptr::addr_of!(self.header.level) as *mut u8;
            ptr::write_volatile(ptr, level as u8);
            let ptr = ptr::addr_of!(self.header.nchildren) as *mut u16;
            ptr::write_volatile(ptr, nchild as u16);
        }
    }

    #[inline]
    pub fn get_val(&self, index: usize) -> &V {
        &self.valmap[index]
    }

    #[inline]
    pub fn set_val(&self, index: usize, val: &V) {
        unsafe {
            ptr::copy_nonoverlapping(
                ptr::addr_of!(*val),
                ptr::addr_of!(self.valmap[index]) as *mut V,
                1
            )
        }
    }

    #[inline]
    pub fn get_userdata(&self) -> u32 {
        self.header.userdata
    }

    #[inline]
    pub fn set_userdata(&self, data: u32) {
        let ptr = ptr::addr_of!(self.header.userdata) as *mut u32;
        unsafe {
            ptr::write_volatile(ptr, data);
        }
    }

    #[inline]
    pub fn get_capacity(&self) -> usize {
        self.capacity
    }

    pub fn as_ref(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self.ptr as *const u8, self.size)
        }
    }

    pub fn as_mut(&self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(self.ptr as *mut u8, self.size)
        }
    }
}

impl<'a, V> Drop for DirectNode<'a, V> {
    fn drop(&mut self) {
        if self.ptr.is_null() {
            return;
        }
        if let Ok(layout) = std::alloc::Layout::from_size_align(self.size, MIN_ALIGNED) {
            unsafe { std::alloc::dealloc(self.ptr as *mut u8, layout) };
        }
    }
}

impl<'a, V> fmt::Display for DirectNode<'a, V>
    where
        V: Copy + fmt::Display
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "===== dump direct node @{:?} ====\n", self.header as *const NodeHeader)?;
        write!(f, "  flags: {},  level: {}, nchildren: {}, capacity: {}\n",
            self.header.flags, self.header.level, self.header.nchildren, self.capacity)?;
        for idx in 0..self.capacity {
            write!(f, "{:3}   {:20}   {:20}\n", idx, idx, self.get_val(idx))?;
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
