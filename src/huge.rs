//! Anonymous memory-mapped backing buffers for large zeroed tables and uninit slabs.

use std::alloc::{alloc, alloc_zeroed, dealloc, handle_alloc_error, Layout};
use std::mem::{align_of, needs_drop, size_of, MaybeUninit};
use std::ops::Deref;
use std::ptr::NonNull;

use memmap2::{MmapMut, MmapOptions};

const HUGE_PAGE_THRESHOLD: usize = 2 * 1024 * 1024;

enum BackingOwner {
    Empty,
    Alloc { ptr: *mut u8, layout: Layout },
    #[allow(dead_code)]
    Mmap(MmapMut),
}

impl Drop for BackingOwner {
    fn drop(&mut self) {
        if let BackingOwner::Alloc { ptr, layout } = *self {
            if layout.size() > 0 {
                unsafe { dealloc(ptr, layout) };
            }
        }
    }
}

/// Fixed-length zero-initialized buffer using `mmap(MAP_ANONYMOUS)` for allocations >= 2 MB
/// on Linux, falling back to aligned heap allocation otherwise.
pub struct HugeZeroed<T> {
    ptr: *mut T,
    len: usize,
    owner: BackingOwner,
}

unsafe impl<T: Send> Send for HugeZeroed<T> {}
unsafe impl<T: Sync> Sync for HugeZeroed<T> {}

impl<T> HugeZeroed<T> {
    pub fn new(len: usize) -> Self {
        const {
            assert!(!needs_drop::<T>());
            assert!(align_of::<T>() <= 4096);
        }
        let elem_size = size_of::<T>();
        let byte_len = len.checked_mul(elem_size).expect("HugeZeroed byte_len overflow");
        if byte_len == 0 {
            return Self {
                ptr: NonNull::<T>::dangling().as_ptr(),
                len,
                owner: BackingOwner::Empty,
            };
        }

        if cfg!(target_os = "linux") && byte_len >= HUGE_PAGE_THRESHOLD {
            if let Ok(mut m) = MmapOptions::new().len(byte_len).map_anon() {
                let ptr = m.as_mut_ptr() as *mut T;
                return Self {
                    ptr,
                    len,
                    owner: BackingOwner::Mmap(m),
                };
            }
        }

        let layout = Layout::array::<T>(len).expect("HugeZeroed layout");
        let ptr = unsafe { alloc_zeroed(layout) };
        if ptr.is_null() {
            handle_alloc_error(layout);
        }
        Self {
            ptr: ptr as *mut T,
            len,
            owner: BackingOwner::Alloc { ptr, layout },
        }
    }

    /// Zero-copy reinterpretation between layout-compatible types (e.g. `AtomicU64` -> `u64`).
    pub fn reinterpret<U>(self) -> HugeZeroed<U> {
        const {
            assert!(!needs_drop::<U>());
            assert!(size_of::<T>() == size_of::<U>());
            assert!(align_of::<T>() == align_of::<U>());
        }
        HugeZeroed {
            ptr: self.ptr as *mut U,
            len: self.len,
            owner: self.owner,
        }
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline(always)]
    pub fn as_ptr(&self) -> *const T {
        self.ptr as *const T
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[T] {
        if self.len == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(self.ptr as *const T, self.len) }
        }
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        if self.len == 0 {
            &mut []
        } else {
            unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
        }
    }
}

impl<T> Deref for HugeZeroed<T> {
    type Target = [T];

    #[inline(always)]
    fn deref(&self) -> &[T] {
        self.as_slice()
    }
}

/// Append-only uninitialized slab backed by `mmap(MAP_ANONYMOUS)` for allocations >= 2 MB
/// on Linux, enabling fast zero-branch pointer access.
pub struct HugeSlab<T> {
    ptr: *mut MaybeUninit<T>,
    len: usize,
    cap: usize,
    _owner: BackingOwner,
}

unsafe impl<T: Send> Send for HugeSlab<T> {}
unsafe impl<T: Sync> Sync for HugeSlab<T> {}

impl<T> HugeSlab<T> {
    pub fn with_capacity(cap: usize) -> Self {
        const {
            assert!(!needs_drop::<T>());
            assert!(align_of::<MaybeUninit<T>>() <= 4096);
        }
        let elem_size = size_of::<MaybeUninit<T>>();
        let byte_len = cap.checked_mul(elem_size).expect("HugeSlab byte_len overflow");
        if byte_len == 0 {
            return Self {
                ptr: NonNull::<MaybeUninit<T>>::dangling().as_ptr(),
                len: 0,
                cap,
                _owner: BackingOwner::Empty,
            };
        }

        if cfg!(target_os = "linux") && byte_len >= HUGE_PAGE_THRESHOLD {
            if let Ok(mut m) = MmapOptions::new().len(byte_len).map_anon() {
                let ptr = m.as_mut_ptr() as *mut MaybeUninit<T>;
                return Self {
                    ptr,
                    len: 0,
                    cap,
                    _owner: BackingOwner::Mmap(m),
                };
            }
        }

        let align = align_of::<MaybeUninit<T>>().max(64);
        let padded_size = (byte_len + align - 1) & !(align - 1);
        let layout = Layout::from_size_align(padded_size, align).expect("HugeSlab layout");
        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            handle_alloc_error(layout);
        }
        Self {
            ptr: ptr as *mut MaybeUninit<T>,
            len: 0,
            cap,
            _owner: BackingOwner::Alloc { ptr, layout },
        }
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.cap
    }

    #[inline(always)]
    pub unsafe fn set_len(&mut self, new_len: usize) {
        debug_assert!(new_len <= self.cap);
        self.len = new_len;
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub fn as_ptr(&self) -> *const MaybeUninit<T> {
        self.ptr as *const MaybeUninit<T>
    }

    #[inline(always)]
    pub fn as_mut_ptr(&mut self) -> *mut MaybeUninit<T> {
        self.ptr
    }

    #[inline(always)]
    pub unsafe fn get_unchecked(&self, start: usize, len: usize) -> &[MaybeUninit<T>] {
        debug_assert!(start + len <= self.cap);
        std::slice::from_raw_parts((self.ptr as *const MaybeUninit<T>).add(start), len)
    }
}
