//! The global allocator. Small allocations go to the system allocator. Large
//! ones are carved from one big anonymous mapping made at startup and are never
//! given back: mapping and unmapping memory while other threads take page
//! faults stalls them all, and this process only lives for a second or two.

use std::alloc::{GlobalAlloc, Layout, System};
use std::ffi::{c_int, c_void};
use std::sync::atomic::{AtomicUsize, Ordering};

extern "C" {
    fn mmap(addr: *mut c_void, len: usize, prot: c_int, flags: c_int, fd: c_int, offset: i64) -> *mut c_void;
}

const LARGE: usize = 64 << 10;
const PAGE: usize = 4096;

static BASE: AtomicUsize = AtomicUsize::new(0);
static CURSOR: AtomicUsize = AtomicUsize::new(0);
static END: AtomicUsize = AtomicUsize::new(0);

pub struct Bump;

/// Reserves up to `bytes` of address space for large allocations. Only pages
/// that are touched use memory. Call once, before any thread starts.
pub fn reserve(mut bytes: usize) {
    while bytes >= 1 << 30 {
        // PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE
        // SAFETY: a fresh anonymous mapping.
        let base = unsafe { mmap(std::ptr::null_mut(), bytes, 1 | 2, 0x02 | 0x20 | 0x4000, -1, 0) };
        if base as isize != -1 {
            // Stretches start on 2 MB boundaries: one page-table page each.
            CURSOR.store((base as usize + (2 << 20) - 1) & !((2 << 20) - 1), Ordering::Relaxed);
            END.store(base as usize + bytes, Ordering::Relaxed);
            BASE.store(base as usize, Ordering::Release);
            return;
        }
        bytes /= 2;
    }
}

#[inline(always)]
fn inside(pointer: *mut u8) -> bool {
    let base = BASE.load(Ordering::Relaxed);
    base != 0 && (base..END.load(Ordering::Relaxed)).contains(&(pointer as usize))
}

/// Each thread carves from its own stretch of the region, so that threads
/// taking page faults at the same time never share a page-table page.
const STRETCH: usize = 64 << 20;

thread_local! {
    static OWN: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
}

/// A zeroed, page-aligned piece of the region, or null when it is full or the layout does not fit.
#[inline(always)]
fn carve(layout: Layout) -> *mut u8 {
    if layout.size() < LARGE || layout.align() > PAGE || BASE.load(Ordering::Acquire) == 0 {
        return std::ptr::null_mut();
    }
    let size = (layout.size() + PAGE - 1) & !(PAGE - 1);
    let (cursor, end) = OWN.get();
    if cursor + size <= end {
        OWN.set((cursor + size, end));
        return cursor as *mut u8;
    }
    // A new stretch for this thread, or for a huge request a stretch of its own.
    let stretch = if size > STRETCH / 4 { (size + STRETCH - 1) & !(STRETCH - 1) } else { STRETCH };
    let at = CURSOR.fetch_add(stretch, Ordering::Relaxed);
    if at + stretch > END.load(Ordering::Relaxed) {
        return std::ptr::null_mut();
    }
    if size <= STRETCH / 4 {
        OWN.set((at + size, at + stretch));
    }
    at as *mut u8
}

// SAFETY: every pointer returned is either the system allocator's or an
// exclusive, suitably aligned, never-reused piece of the region.
unsafe impl GlobalAlloc for Bump {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let carved = carve(layout);
        if carved.is_null() { System.alloc(layout) } else { carved }
    }
    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // The region is fresh from the kernel and never reused, so it is zero already.
        let carved = carve(layout);
        if carved.is_null() { System.alloc_zeroed(layout) } else { carved }
    }
    #[inline]
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        if !inside(pointer) {
            System.dealloc(pointer, layout);
        }
    }
    #[inline]
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let was_inside = inside(pointer);
        if !was_inside && new_size < LARGE {
            return System.realloc(pointer, layout, new_size);
        }
        if was_inside && new_size <= layout.size() {
            return pointer;
        }
        let new = self.alloc(Layout::from_size_align_unchecked(new_size, layout.align()));
        if !new.is_null() {
            std::ptr::copy_nonoverlapping(pointer, new, layout.size().min(new_size));
            self.dealloc(pointer, layout);
        }
        new
    }
}
