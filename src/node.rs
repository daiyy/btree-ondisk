use std::ptr;
use std::fmt;
use std::cell::Cell;
use std::io::{Error, ErrorKind, Result};
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

/// Owned aligned memory buffer for node storage.
///
/// Handles allocation and deallocation of aligned memory.
/// When `ptr` is null, the buffer is a non-owning view (e.g. from `from_slice`).
#[derive(Debug)]
pub struct AlignedBuffer {
    ptr: *mut u8,
    size: usize,
}

impl AlignedBuffer {
    /// Allocate a new zeroed buffer of the given size.
    pub fn new(size: usize) -> Option<Self> {
        let layout = std::alloc::Layout::from_size_align(size, MIN_ALIGNED).ok()?;
        // SAFETY: `layout` has non-zero size when `size > 0`; when size is 0
        // the layout's size is 0 and alloc_zeroed is UB — callers never
        // construct zero-sized nodes in practice, but we still check for a
        // null return below, which covers both allocation failure and the
        // zero-size edge case if it ever occurs.
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            return None;
        }
        Some(Self { ptr, size })
    }

    /// Allocate and copy from an existing byte slice. Returns an aligned copy.
    pub fn from_slice_copy(src: &[u8]) -> Self {
        let ab = Self::new(src.len()).expect("failed to allocate aligned buffer");
        // SAFETY: `ab.ptr` is freshly allocated with `src.len()` bytes; `src`
        // is a separate allocation.
        unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), ab.ptr, src.len()); }
        ab
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    pub fn as_slice(&self) -> &[u8] {
        self.as_ref()
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.as_mut()
    }

    /// Create a non-owning view that records the size of an externally-owned
    /// buffer. `ptr` stays null so `Drop` is a no-op; `size` simply carries the
    /// length of the data region the enclosing node (`BtreeNode`/`DirectNode`)
    /// was constructed from via `from_slice` / `from_slice_ref`, so callers
    /// such as `do_update` / `do_reinit` can recover the original length.
    fn non_owning(size: usize) -> Self {
        Self { ptr: std::ptr::null_mut(), size }
    }

    fn as_ref(&self) -> &[u8] {
        // Non-owning view: no owned storage to expose, even though `size`
        // records the external buffer's length for other bookkeeping.
        if self.ptr.is_null() {
            return &[];
        }
        // SAFETY: `ptr` and `size` came from a successful allocation in `new`.
        unsafe { std::slice::from_raw_parts(self.ptr, self.size) }
    }

    fn as_mut(&mut self) -> &mut [u8] {
        if self.ptr.is_null() {
            return &mut [];
        }
        // SAFETY: same as as_ref. Unique access is guaranteed by &mut self.
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.size) }
    }
}

impl Clone for AlignedBuffer {
    fn clone(&self) -> Self {
        if self.ptr.is_null() {
            return Self::non_owning(self.size);
        }
        Self::from_slice_copy(self.as_ref())
    }
}

// SAFETY: AlignedBuffer owns its allocation exclusively; the raw pointer is
// only ever accessed via &self / &mut self, so Send/Sync mirror the byte
// slice that it wraps.
unsafe impl Send for AlignedBuffer {}
unsafe impl Sync for AlignedBuffer {}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        if self.ptr.is_null() {
            return;
        }
        if let Ok(layout) = std::alloc::Layout::from_size_align(self.size, MIN_ALIGNED) {
            // SAFETY: `ptr` was allocated with the identical `layout` in `new`
            // and has not been freed yet (Drop runs once).
            unsafe { std::alloc::dealloc(self.ptr, layout) };
        }
    }
}

/// btree node descriptor for memory pointer, normally a page
///
/// # Safety invariants
///
/// The node reinterprets a byte buffer as a fixed layout:
///   `[NodeHeader][keymap: [K; capacity]][valmap: [V or P; capacity]]`
/// where the choice between V and P depends on `header.flags & BTREE_NODE_FLAG_LEAF`.
///
/// All mutating methods take `&self` and perform writes through raw pointers
/// (interior mutability). This is sound under the following invariants:
///
/// 1. The backing buffer (`buf`) outlives all internal pointers, or the node
///    was constructed from an externally-owned buffer via `from_slice` and
///    will not outlive that buffer.
/// 2. The fields of `NodeHeader` (`flags`, `level`, `nchildren`, `userdata`)
///    are plain `Copy` types with no interior references; writes through
///    `ptr::write` are torn-read safe because each write targets a single
///    primitive (`u8`/`u16`/`u32`).
/// 3. On a single node, concurrent writes are forbidden. Under `rc` this is
///    guaranteed by single-threaded execution; under `arc` the library
///    serializes writes at higher levels (each node is held behind
///    `Arc<...>` and external containers wrap it in `AtomicRefCell`).
/// 4. `get_val<X>` / `set_val<X>` require the caller to pick `X == V` for
///    leaf nodes and `X == P` for internal nodes. Mismatched `X` yields UB.
/// 5. `header`/`keymap` are stored as raw pointers (not `&mut` references) so
///    that short-lived `&mut [u8]` views obtained via `as_u8_mut` do not
///    conflict with long-lived aliasing borrows under Stacked/Tree Borrows.
#[repr(C, align(8))]
pub struct BtreeNode<'a, K, V, P> {
    header: *mut NodeHeader,
    keymap: *mut K,
    valptr: *mut u8,
    capacity: usize,    // kv capacity of this btree node
    buf: AlignedBuffer,
    id: Cell<P>,
    dirty: Cell<bool>,
    _pin: PhantomPinned,
    phantom: PhantomData<V>,
    _lifetime: PhantomData<&'a mut u8>,
}

// SAFETY:
// A BtreeNode internally holds raw pointers (keymap slice, valptr) that alias
// into `buf`. These raw pointers are logically owned by this struct. When the
// generic parameters themselves are thread-safe (Send + Sync), sharing the
// node across threads is sound provided the caller serializes mutation on
// any given node (the library enforces this by holding each node behind
// Arc<...> and using AtomicRefCell for its owning map).
#[cfg(feature = "arc")]
unsafe impl<'a, K: Send, V: Send, P: Send> Send for BtreeNode<'a, K, V, P> {}
#[cfg(feature = "arc")]
unsafe impl<'a, K: Sync, V: Sync, P: Sync> Sync for BtreeNode<'a, K, V, P> {}

impl<'a, K, V, P> BtreeNode<'a, K, V, P>
    where
        K: Copy + fmt::Display + std::cmp::PartialOrd,
        V: Copy + fmt::Display + NodeValue,
        P: Copy + fmt::Display + NodeValue,
{
    /// Reinterpret `ptr[..len]` as a BtreeNode.
    ///
    /// # Safety
    ///
    /// - `ptr` must be non-null and valid for reads and writes of `len` bytes.
    /// - The memory region must remain valid and exclusively owned by the
    ///   returned node for its lifetime.
    /// - Alignment and minimum length are checked at runtime; callers don't
    ///   have to pre-check, but if these checks fail an `Err` is returned.
    unsafe fn from_raw_ptr(ptr: *mut u8, len: usize) -> Result<Self> {
        let hdr_size = std::mem::size_of::<NodeHeader>();
        if len < hdr_size {
            return Err(Error::new(ErrorKind::InvalidInput,
                format!("input buf size {} smaller than a valid btree node header size {}", len, hdr_size)));
        }
        if !(ptr as usize).is_multiple_of(std::mem::align_of::<NodeHeader>()) {
            return Err(Error::new(ErrorKind::InvalidInput,
                format!("buffer pointer {:p} is not aligned to {}", ptr, std::mem::align_of::<NodeHeader>())));
        }

        // SAFETY: ptr is non-null, len >= hdr_size, alignment verified above.
        let header = ptr.cast::<NodeHeader>();

        let key_size = std::mem::size_of::<K>();
        let val_size = if (*header).flags & BTREE_NODE_FLAG_LEAF > 0 {
            std::mem::size_of::<V>()
        } else {
            std::mem::size_of::<P>()
        };
        let capacity = (len - hdr_size) / (key_size + val_size);
        if capacity == 0 {
            return Err(Error::new(ErrorKind::InvalidInput,
                format!("buffer size {} too small to hold a btree node header plus one {}-byte slot",
                    len, key_size + val_size)));
        }
        if capacity < (*header).nchildren as usize {
            return Err(Error::new(ErrorKind::InvalidData,
                format!("nchildren in header is larger than its capacity {} > {}", (*header).nchildren, capacity)));
        }

        let keymap = ptr.add(hdr_size) as *mut K;
        let valptr = ptr.add(hdr_size + capacity * key_size);

        Ok(Self {
            header,
            keymap,
            valptr,
            capacity,
            buf: AlignedBuffer::non_owning(len),
            id: Cell::new(P::invalid_value()),
            dirty: Cell::new(false),
            _pin: PhantomPinned,
            phantom: PhantomData,
            _lifetime: PhantomData,
        })
    }

    pub fn from_slice(buf: &mut [u8]) -> Result<Self> {
        // SAFETY: `buf` is a valid mutable slice, so its pointer is non-null,
        // aligned to at least u8, and covers `buf.len()` bytes. from_raw_ptr
        // performs additional alignment/length checks before reinterpreting.
        unsafe { Self::from_raw_ptr(buf.as_mut_ptr(), buf.len()) }
    }

    /// Construct a read-only view from an immutable slice.
    ///
    /// # Caller contract (not enforced by the type system)
    ///
    /// The returned node aliases `buf` via a mutable-looking raw pointer.
    /// **Only read-only methods** (e.g. `is_large`, `get_level`,
    /// `get_nchild`, `get_key`, `get_val`) may be called on it. Calling any
    /// setter (`set_*`, `init*`, `insert`, `delete`, `mark_dirty`,
    /// `move_*`, `as_u8_mut`, `do_update`, `do_reinit`) is **undefined
    /// behaviour** because `buf` is not exclusively owned.
    pub fn from_slice_ref(buf: &[u8]) -> Result<Self> {
        // SAFETY: caller promises read-only use per the contract above.
        unsafe { Self::from_raw_ptr(buf.as_ptr() as *mut u8, buf.len()) }
    }

    pub fn new(size: usize) -> Option<Self> {
        let ab = AlignedBuffer::new(size)?;
        // SAFETY: ab just came from a successful allocation with `size` bytes.
        let data = unsafe { std::slice::from_raw_parts_mut(ab.ptr, ab.size) };
        let mut node = Self::from_slice(data).ok()?;
        node.buf = ab;
        node.id.set(P::invalid_value());
        Some(node)
    }

    pub fn new_with_id(size: usize, id: &P) -> Option<Self> {
        let node = Self::new(size)?;
        node.set_id(*id);
        Some(node)
    }

    pub fn copy_from_slice(id: P, buf: &[u8]) -> Option<Self> {
        let ab = AlignedBuffer::new(buf.len())?;
        // Copy via raw pointers, then build the node. This avoids
        // constructing an intermediate `&mut [u8]` that would alias the
        // internal `header`/`keymap` borrows the node is about to hold.
        // SAFETY: `ab` is a fresh exclusive allocation of `buf.len()` bytes;
        // `buf` is a disjoint read-only slice.
        unsafe {
            std::ptr::copy_nonoverlapping(buf.as_ptr(), ab.ptr, buf.len());
        }
        // SAFETY: ab came from a successful allocation of `buf.len()` bytes;
        // no other references into it exist at this point.
        let data = unsafe { std::slice::from_raw_parts_mut(ab.ptr, ab.size) };
        let mut node = Self::from_slice(data).ok()?;
        node.buf = ab;
        node.id.set(id);
        Some(node)
    }

    pub fn as_u8_ref(&self) -> &[u8] {
        // Share provenance with header/keymap by rooting at `self.header`.
        // SAFETY: `self.header` is the start of `self.buf`.
        unsafe { std::slice::from_raw_parts(self.header as *const u8, self.buf.size) }
    }

    pub fn as_u8_mut(&mut self) -> &mut [u8] {
        // Reborrow from the same raw base that `header`/`keymap` are
        // derived from, so the resulting slice does not invalidate them
        // under Tree/Stacked Borrows.
        // SAFETY: `self.header` points at the start of `self.buf`, which
        // spans `self.buf.size` initialized bytes owned exclusively by
        // this node.
        unsafe { std::slice::from_raw_parts_mut(self.header as *mut u8, self.buf.size) }
    }

    /// Short-lived reborrow of the header. The reference is only valid for
    /// the duration of the caller's expression and must not coexist with any
    /// other borrow of the same bytes.
    #[inline]
    fn header(&self) -> &NodeHeader {
        // SAFETY: `self.header` points into `self.buf`; see type invariants.
        unsafe { &*self.header }
    }

    #[inline]
    pub fn is_leaf(&self) -> bool {
        if (self.header().flags & BTREE_NODE_FLAG_LEAF) == BTREE_NODE_FLAG_LEAF {
            return true;
        }
        false
    }

    #[inline]
    pub fn set_leaf(&self) {
        let ptr = unsafe { &raw mut (*self.header).flags };
        // SAFETY: `ptr` points to an initialized `u8` field inside the header
        // owned by this node; see the type-level "Safety invariants".
        unsafe {
            let mut flags = self.header().flags;
            flags |= BTREE_NODE_FLAG_LEAF;
            ptr::write(ptr, flags);
        }
    }

    #[inline]
    pub fn clear_leaf(&self) {
        let ptr = unsafe { &raw mut (*self.header).flags };
        // SAFETY: header.flags is a valid, initialized u8 in this node's buffer.
        unsafe {
            let mut flags = self.header().flags;
            flags &= !BTREE_NODE_FLAG_LEAF;
            ptr::write(ptr, flags);
        }
    }

    #[inline]
    pub fn is_large(&self) -> bool {
        if (self.header().flags & BTREE_NODE_FLAG_LARGE) == BTREE_NODE_FLAG_LARGE {
            return true;
        }
        false
    }

    #[inline]
    pub fn set_large(&self) {
        let ptr = unsafe { &raw mut (*self.header).flags };
        // SAFETY: header.flags is a valid, initialized u8 in this node's buffer.
        unsafe {
            let mut flags = self.header().flags;
            flags |= BTREE_NODE_FLAG_LARGE;
            ptr::write(ptr, flags);
        }
    }

    #[inline]
    pub fn clear_large(&self) {
        let ptr = unsafe { &raw mut (*self.header).flags };
        // SAFETY: header.flags is a valid, initialized u8 in this node's buffer.
        unsafe {
            let mut flags = self.header().flags;
            flags &= !BTREE_NODE_FLAG_LARGE;
            ptr::write(ptr, flags);
        }
    }

    #[inline]
    pub fn get_flags(&self) -> u8 {
        self.header().flags
    }

    #[inline]
    pub fn set_flags(&self, flags: u8) {
        let ptr = unsafe { &raw mut (*self.header).flags };
        // SAFETY: header.flags is a valid, initialized u8 in this node's buffer.
        unsafe {
            ptr::write(ptr, flags);
        }
    }

    #[inline]
    pub fn get_level(&self) -> usize {
        self.header().level as usize
    }

    #[inline]
    pub fn set_level(&self, level: usize) {
        let ptr = unsafe { &raw mut (*self.header).level };
        // SAFETY: header.level is a valid, initialized u8 in this node's buffer.
        unsafe {
            ptr::write(ptr, level as u8);
        }
    }

    #[inline]
    pub fn get_key(&self, index: usize) -> &K {
        // SAFETY: `self.keymap` spans `self.capacity` K values; indexing is
        // the caller's responsibility (library always passes index < capacity).
        unsafe { &*self.keymap.add(index) }
    }

    #[inline]
    pub fn set_key(&self, index: usize, key: &K) {
         // SAFETY: `self.keymap[index]` is checked by indexing; the cast to
         // *mut K is legal because the backing buffer is mutable (see type
         // invariants) and K: Copy.
         unsafe {
            ptr::copy_nonoverlapping(
                ptr::addr_of!(*key),
                self.keymap.add(index),
                1
            )
        }
    }

    /// Read a value slot as type `X`.
    ///
    /// # Caller contract (not enforced by the type system)
    ///
    /// - `X` must match the node's storage: `V` for leaves
    ///   (`is_leaf() == true`), `P` for internal nodes.
    /// - `index` must be `< self.get_capacity()`.
    ///
    /// Violating either is **undefined behaviour** — bytes are
    /// reinterpreted through the wrong type or read past the end of the
    /// buffer. The function stays `safe` for ergonomic reasons; debug
    /// builds add a `debug_assert!` on `size_of::<X>()` to catch the most
    /// common mistakes, but size-compatible mismatches (e.g. `u64` vs
    /// `i64`) slip through. Prefer the higher-level `BtreeMap` API.
    #[inline]
    pub fn get_val<X>(&self, index: usize) -> &X {
        debug_assert_eq!(
            std::mem::size_of::<X>(),
            if self.is_leaf() { std::mem::size_of::<V>() } else { std::mem::size_of::<P>() },
            "get_val<X>: X size does not match the node's slot type",
        );
        // SAFETY: caller contract above; valptr spans `capacity` elements
        // of type X, index is bounds-checked by the slice indexing.
        let slice = unsafe {
            std::slice::from_raw_parts(self.valptr as *const X, self.capacity)
        };
        &slice[index]
    }

    /// Write a value slot of type `X`. Same contract as [`Self::get_val`].
    #[inline]
    pub fn set_val<X>(&self, index: usize, val: &X) {
        debug_assert_eq!(
            std::mem::size_of::<X>(),
            if self.is_leaf() { std::mem::size_of::<V>() } else { std::mem::size_of::<P>() },
            "set_val<X>: X size does not match the node's slot type",
        );
        debug_assert!(index < self.capacity, "set_val<X>: index {} >= capacity {}", index, self.capacity);
        // SAFETY: see `get_val`.
        unsafe {
            let dst = (self.valptr as *mut X).add(index);
            ptr::copy_nonoverlapping(ptr::addr_of!(*val), dst, 1)
        }
    }

    #[inline]
    pub fn get_nchild(&self) -> usize {
        self.header().nchildren as usize
    }

    #[inline]
    pub fn set_nchild(&self, c: usize) {
        let ptr = unsafe { &raw mut (*self.header).nchildren };
        // SAFETY: header.nchildren is a valid, initialized u16 in this node's buffer.
        unsafe {
            ptr::write(ptr, c as u16);
        }
    }

    #[inline]
    pub fn get_userdata(&self) -> u32 {
        self.header().userdata
    }

    #[inline]
    pub fn set_userdata(&self, data: u32) {
        let ptr = unsafe { &raw mut (*self.header).userdata };
        // SAFETY: header.userdata is a valid, initialized u32 in this node's buffer.
        unsafe {
            ptr::write(ptr, data);
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
        (self.buf.size - hdr_size) / (key_size + val_size)
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
        // SAFETY: capacity >= 1 for all valid nodes (enforced by from_raw_ptr).
        unsafe { &*self.keymap }
    }

    #[inline]
    pub fn id(&self) -> &P {
        // SAFETY: Cell::as_ptr returns a valid pointer to self.id; no
        // concurrent mutation on the same node (see type invariants).
        unsafe { &*self.id.as_ptr() }
    }

    #[inline]
    pub fn set_id(&self, id: P) {
        self.id.set(id);
    }

    #[inline]
    // re-calc capacity and valptr by flags
    pub(crate) fn do_update(&mut self) {
        let len = self.buf.size;
        let hdr_size = std::mem::size_of::<NodeHeader>();
        if len < hdr_size {
            panic!("input buf size {} smaller than a valid btree node header size {}", len, hdr_size);
        }

        let base = self.header as *mut u8;

        let key_size = std::mem::size_of::<K>();
        let val_size = if self.get_level() == BTREE_NODE_LEVEL_LEAF {
            assert!(self.get_flags() & BTREE_NODE_FLAG_LEAF == BTREE_NODE_FLAG_LEAF);
            std::mem::size_of::<V>()
        } else {
            assert!(self.get_flags() & BTREE_NODE_FLAG_LEAF != BTREE_NODE_FLAG_LEAF);
            std::mem::size_of::<P>()
        };
        let capacity = (len - hdr_size) / (key_size + val_size);

        // SAFETY: `base` points at the start of the header; offsetting by
        // `hdr_size` reaches the keymap region, followed by the valmap. All
        // arithmetic stays within `self.buf` (len == buf.size).
        self.keymap = unsafe { base.add(hdr_size) } as *mut K;
        self.valptr = unsafe { base.add(hdr_size + capacity * key_size) };
        self.capacity = capacity;
    }

    #[inline]
    // re-calc capacity and valptr based on X
    pub(crate) fn do_reinit<X>(&mut self) {
        let len = self.buf.size;
        let hdr_size = std::mem::size_of::<NodeHeader>();
        if len < hdr_size {
            panic!("input buf size {} smaller than a valid btree node header size {}", len, hdr_size);
        }

        let base = self.header as *mut u8;

        let key_size = std::mem::size_of::<K>();
        let val_size = std::mem::size_of::<X>();
        let capacity = (len - hdr_size) / (key_size + val_size);

        // SAFETY: same reasoning as `do_update`.
        self.keymap = unsafe { base.add(hdr_size) } as *mut K;
        self.valptr = unsafe { base.add(hdr_size + capacity * key_size) };
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
        self.dirty.get()
    }

    #[inline]
    pub fn mark_dirty(&self) {
        self.dirty.set(true)
    }

    #[inline]
    pub fn clear_dirty(&self) {
        self.dirty.set(false)
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

        // SAFETY: `left` and `right` are distinct sibling nodes (the caller
        // passes two different BtreeNode references); their backing buffers
        // do not alias. Pointer arithmetic stays within each node's keymap/
        // valmap because the caller bounds n: `n <= rnchild` and
        // `lnchild + n <= capacity`. ptr::copy handles potential overlap
        // within a single node correctly. The X type must match the node
        // layout (V for leaves, P for internals); `move_left` dispatches
        // accordingly.
        unsafe {

        let lkeymap_tail_ptr = left.keymap.add(lnchild) as *const K as *mut K;
        let lvalmap_tail_ptr = (left.valptr as *mut X).add(lnchild);

        let rkeymap_head_ptr = right.keymap.add(0) as *const K as *mut K;
        let rvalmap_head_ptr = right.valptr as *mut X;

        let rkeymap_n_ptr = right.keymap.add(n) as *const K as *mut K;
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

        left.set_nchild(lnchild);
        right.set_nchild(rnchild);
    }

    pub(crate) fn move_left(left: &BtreeNode<K, V, P>, right: &BtreeNode<K, V, P>, n: usize) {
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

        // SAFETY: same reasoning as `do_move_left`. The caller ensures
        // `n <= lnchild` and `rnchild + n <= capacity` so all offsets stay
        // within each node's allocation. Left and right are distinct nodes,
        // so their buffers never alias.
        unsafe {

        let lkeymap_tailn_ptr = left.keymap.add(lnchild - n) as *const K as *mut K;
        let lvalmap_tailn_ptr = (left.valptr as *mut X).add(lnchild - n);

        let rkeymap_head_ptr = right.keymap.add(0) as *const K as *mut K;
        let rvalmap_head_ptr = right.valptr as *mut X;

        let rkeymap_n_ptr = right.keymap.add(n) as *const K as *mut K;
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

        left.set_nchild(lnchild);
        right.set_nchild(rnchild);
    }

    pub(crate) fn move_right(left: &BtreeNode<K, V, P>, right: &BtreeNode<K, V, P>, n: usize) {
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
        if self.header().nchildren == 0 {
            return (false, 0);
        }
        let mut low: isize = 0;
        let mut high: isize = (self.header().nchildren - 1) as isize;
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
        } else if !s {
            index += 1;
        }

        (false, index as usize)
    }

    /// Insert `(key, val)` at `index`, shifting later entries right.
    ///
    /// # Caller contract (not enforced by the type system)
    ///
    /// - `X` must match the node's slot type (`V` for leaves, `P` for
    ///   internal nodes).
    /// - `nchild < capacity` (a free slot exists).
    ///
    /// Violating either is **undefined behaviour**.
    pub fn insert<X>(&self, index: usize, key: &K, val: &X) {
        debug_assert_eq!(
            std::mem::size_of::<X>(),
            if self.is_leaf() { std::mem::size_of::<V>() } else { std::mem::size_of::<P>() },
            "insert<X>: X size does not match the node's slot type",
        );
        let mut nchild = self.get_nchild();
        debug_assert!(nchild < self.capacity, "insert: node is full");

        if index < nchild {
            // SAFETY: shifting `nchild - index` elements from `index` to
            // `index + 1` inside keymap and valmap. Requires nchild < capacity
            // (the caller of insert must have ensured there is a free slot,
            // which the btree layer enforces by splitting full nodes before
            // insert). X must match the node layout (V for leaves, P for
            // internals). ptr::copy handles the overlapping forward shift.
            unsafe {
                let ksrc: *const K = self.keymap.add(index) as *const K;
                let vsrc: *const X = (self.valptr as *const X).add(index);

                let kdst: *mut K = self.keymap.add(index + 1);
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

    /// Delete entry at `index`, shifting later entries left.
    ///
    /// # Caller contract (not enforced by the type system)
    ///
    /// - `X` must match the node's slot type (`V` for leaves, `P` for
    ///   internal nodes).
    /// - `index < nchild`.
    ///
    /// Violating either is **undefined behaviour**.
    pub fn delete<X: Copy>(&self, index: usize, key: &mut K, val: &mut X) {
        debug_assert_eq!(
            std::mem::size_of::<X>(),
            if self.is_leaf() { std::mem::size_of::<V>() } else { std::mem::size_of::<P>() },
            "delete<X>: X size does not match the node's slot type",
        );
        let mut nchild = self.get_nchild();
        debug_assert!(index < nchild, "delete: index {} >= nchild {}", index, nchild);

        *key = *self.get_key(index);
        *val = *self.get_val(index);

        if index < nchild - 1 {
            // SAFETY: shifting `nchild - index - 1` elements from `index + 1`
            // down to `index`, all within [0, nchild) <= [0, capacity). Same X
            // requirement as `insert`. ptr::copy handles the overlapping
            // backward shift.
            unsafe {
                let ksrc: *const K = self.keymap.add(index + 1) as *const K;
                let vsrc: *const X = (self.valptr as *const X).add(index + 1);

                let kdst: *mut K = self.keymap.add(index);
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


impl<'a, K, V, P> PartialEq for BtreeNode<'a, K, V, P> {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self.header, other.header)
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
            writeln!(f, "===== dump btree node @{:?} ROOT ====", self.header as *const NodeHeader)?;
        } else {
            writeln!(f, "===== dump btree node @{:?} id {} ====", self.header as *const NodeHeader, self.id())?;
        }
        writeln!(f, "  flags: {},  level: {}, nchildren: {}, capacity: {}, is leaf: {}",
            self.header().flags, self.header().level, self.header().nchildren, self.capacity, self.is_leaf())?;
        for idx in 0..self.header().nchildren.into() {
            if self.is_leaf() {
                writeln!(f, "{:3}   {:20}   {:20}", idx, self.get_key(idx), self.get_val::<V>(idx))?;
            } else {
                writeln!(f, "{:3}   {:20}   {:20}", idx, self.get_key(idx), self.get_val::<P>(idx))?;
            }
        }
        write!(f, "")
    }
}

/// direct node descriptor for memory pointer, normally a tiny memory buffer
///
/// # Safety invariants
///
/// Same model as [`BtreeNode`]: a flat buffer reinterpreted as
/// `[NodeHeader][valmap: [V; capacity]]`. All `set_*` methods use interior
/// mutability through raw pointers; concurrent writes to the same node are
/// forbidden and the header fields are primitive `Copy` types whose word-sized
/// writes are atomic with respect to the Rust abstract machine for our uses.
#[derive(Debug)]
#[repr(C, align(8))]
pub struct DirectNode<'a, V> {
    header: *mut NodeHeader,
    valmap: *mut V,
    capacity: usize,
    buf: AlignedBuffer,
    dirty: Cell<bool>,
    _pin: PhantomPinned,
    _lifetime: PhantomData<&'a mut V>,
}

// SAFETY: see BtreeNode Send/Sync impls above. DirectNode carries similar raw
// pointers; sharing across threads requires V: Send/Sync.
#[cfg(feature = "arc")]
unsafe impl<'a, V: Send> Send for DirectNode<'a, V> {}
#[cfg(feature = "arc")]
unsafe impl<'a, V: Sync> Sync for DirectNode<'a, V> {}

impl<'a, V> DirectNode<'a, V>
    where
        V: Copy + fmt::Display
{
    /// Short-lived reborrow of the header.
    #[inline]
    fn header(&self) -> &NodeHeader {
        // SAFETY: `self.header` points into `self.buf`; see type invariants.
        unsafe { &*self.header }
    }

    /// Reinterpret `ptr[..len]` as a BtreeNode.
    ///
    /// # Safety
    ///
    /// - `ptr` must be non-null and valid for reads and writes of `len` bytes.
    /// - The memory region must remain valid and exclusively owned by the
    ///   returned node for its lifetime.
    /// - Alignment and minimum length are checked at runtime; callers don't
    ///   have to pre-check, but if these checks fail an `Err` is returned.
    unsafe fn from_raw_ptr(ptr: *mut u8, len: usize) -> Result<Self> {
        let hdr_size = std::mem::size_of::<NodeHeader>();
        if len < hdr_size {
            return Err(Error::new(ErrorKind::InvalidInput,
                format!("input buf size {} smaller than a valid btree node header size {}", len, hdr_size)));
        }
        if !(ptr as usize).is_multiple_of(std::mem::align_of::<NodeHeader>()) {
            return Err(Error::new(ErrorKind::InvalidInput,
                format!("buffer pointer {:p} is not aligned to {}", ptr, std::mem::align_of::<NodeHeader>())));
        }

        // SAFETY: ptr is non-null, len >= hdr_size, alignment verified above.
        let header = ptr.cast::<NodeHeader>();

        let val_size = std::mem::size_of::<V>();
        let capacity = (len - hdr_size) / val_size;
        if capacity < (*header).nchildren as usize {
            return Err(Error::new(ErrorKind::InvalidData,
                format!("nchildren in header is larger than its capacity {} > {}", (*header).nchildren, capacity)));
        }

        // SAFETY: `ptr` points at the start of the header (offset 0), so
        // `ptr.add(hdr_size)` lies at the start of the valmap, which spans
        // `capacity` elements of V within `self.buf`.
        let valmap = ptr.add(hdr_size) as *mut V;

        Ok(Self {
            header,
            valmap,
            capacity,
            buf: AlignedBuffer::non_owning(len),
            dirty: Cell::new(false),
            _pin: PhantomPinned,
            _lifetime: PhantomData,
        })
    }

    pub fn from_slice(buf: &mut [u8]) -> Result<Self> {
        // SAFETY: `buf` is a valid mutable slice; from_raw_ptr performs
        // alignment and length checks before reinterpreting.
        unsafe { Self::from_raw_ptr(buf.as_mut_ptr(), buf.len()) }
    }

    /// Construct a read-only view from an immutable slice.
    ///
    /// # Caller contract (not enforced by the type system)
    ///
    /// Same contract as [`BtreeNode::from_slice_ref`]: only read-only
    /// methods may be invoked on the returned node. Calling any setter
    /// is **undefined behaviour**.
    pub fn from_slice_ref(buf: &[u8]) -> Result<Self> {
        // SAFETY: caller promises read-only use per the contract above.
        unsafe { Self::from_raw_ptr(buf.as_ptr() as *mut u8, buf.len()) }
    }

    pub fn new(size: usize) -> Option<Self> {
        let ab = AlignedBuffer::new(size)?;
        // SAFETY: ab came from a successful allocation of `size` bytes.
        let data = unsafe { std::slice::from_raw_parts_mut(ab.ptr, ab.size) };
        let mut node = Self::from_slice(data).ok()?;
        node.buf = ab;
        Some(node)
    }

    pub fn copy_from_slice(buf: &[u8]) -> Option<Self> {
        let ab = AlignedBuffer::new(buf.len())?;
        // Copy via raw pointer, then build the node — don't create a
        // `&mut [u8]` view that would alias the node's internal borrows.
        // SAFETY: `ab` is a fresh exclusive allocation; `buf` is disjoint.
        unsafe {
            std::ptr::copy_nonoverlapping(buf.as_ptr(), ab.ptr, buf.len());
        }
        // SAFETY: ab came from a successful allocation of `buf.len()` bytes;
        // no other references into it exist at this point.
        let data = unsafe { std::slice::from_raw_parts_mut(ab.ptr, ab.size) };
        let mut node = Self::from_slice(data).ok()?;
        node.buf = ab;
        Some(node)
    }

    #[inline]
    pub fn init(&self, flags: usize, level: usize, nchild: usize) {
        // SAFETY: `flags`, `level`, `nchildren` are primitive fields inside
        // the header owned by this node; see the type-level "Safety invariants".
        unsafe {
            let h = self.header;
            ptr::write(&raw mut (*h).flags, flags as u8);
            ptr::write(&raw mut (*h).level, level as u8);
            ptr::write(&raw mut (*h).nchildren, nchild as u16);
        }
    }

    #[inline]
    pub fn get_val(&self, index: usize) -> &V {
        // SAFETY: valmap spans `self.capacity` elements; caller ensures index bound.
        unsafe { &*self.valmap.add(index) }
    }

    #[inline]
    pub fn set_val(&self, index: usize, val: &V) {
        // SAFETY: `self.valmap[index]` is bounds-checked by the indexing;
        // V: Copy (enforced by impl bound). Writing through &self is part of
        // the type's interior-mutability contract.
        unsafe {
            ptr::copy_nonoverlapping(
                ptr::addr_of!(*val),
                self.valmap.add(index),
                1
            )
        }
    }

    #[inline]
    pub fn get_userdata(&self) -> u32 {
        self.header().userdata
    }

    #[inline]
    pub fn set_userdata(&self, data: u32) {
        let ptr = unsafe { &raw mut (*self.header).userdata };
        // SAFETY: header.userdata is a valid, initialized u32 in this node's buffer.
        unsafe {
            ptr::write(ptr, data);
        }
    }

    #[inline]
    pub fn get_capacity(&self) -> usize {
        self.capacity
    }

    pub fn as_u8_ref(&self) -> &[u8] {
        // Share provenance with header/keymap by rooting at `self.header`.
        // SAFETY: `self.header` is the start of `self.buf`.
        unsafe { std::slice::from_raw_parts(self.header as *const u8, self.buf.size) }
    }

    pub fn as_u8_mut(&mut self) -> &mut [u8] {
        // Same reasoning as BtreeNode::as_u8_mut.
        // SAFETY: `self.header` is the start of the node's buffer.
        unsafe { std::slice::from_raw_parts_mut(self.header as *mut u8, self.buf.size) }
    }
}

impl<'a, V> fmt::Display for DirectNode<'a, V>
    where
        V: Copy + fmt::Display
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "===== dump direct node @{:?} ====", self.header as *const NodeHeader)?;
        writeln!(f, "  flags: {},  level: {}, nchildren: {}, capacity: {}",
            self.header().flags, self.header().level, self.header().nchildren, self.capacity)?;
        for idx in 0..self.capacity {
            writeln!(f, "{:3}   {:20}   {:20}", idx, idx, self.get_val(idx))?;
        }
        write!(f, "")
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn node() {
    }
}
