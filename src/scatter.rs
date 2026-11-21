//! Partitioned output buffer for one worker: one large allocation split into
//! fixed-capacity regions, one per partition, plus overflow vectors. The
//! memory is not initialized up front, so the kernel faults pages in lazily on
//! the worker's own thread as it writes.

use std::mem::MaybeUninit;

pub struct Scatter<T: Copy> {
    data: Vec<MaybeUninit<T>>,
    cap: usize,
    counts: Vec<u32>,
    overflow: Vec<Vec<T>>,
}

impl<T: Copy> Scatter<T> {
    pub fn new(parts: usize, cap: usize) -> Self {
        let mut data = Vec::with_capacity(parts * cap);
        unsafe { data.set_len(parts * cap) };
        Scatter { data, cap, counts: vec![0; parts], overflow: (0..parts).map(|_| Vec::new()).collect() }
    }

    #[inline(always)]
    pub fn push(&mut self, p: usize, v: T) {
        let c = unsafe { *self.counts.get_unchecked(p) } as usize;
        if c < self.cap {
            unsafe {
                self.data.get_unchecked_mut(p * self.cap + c).write(v);
                *self.counts.get_unchecked_mut(p) += 1;
            }
        } else {
            self.overflow[p].push(v);
        }
    }

    /// `push`, returning the row's index in partition `p` (as `at` takes it).
    #[inline(always)]
    pub fn push_idx(&mut self, p: usize, v: T) -> usize {
        let c = unsafe { *self.counts.get_unchecked(p) } as usize;
        if c < self.cap {
            unsafe {
                self.data.get_unchecked_mut(p * self.cap + c).write(v);
                *self.counts.get_unchecked_mut(p) += 1;
            }
            c
        } else {
            self.overflow[p].push(v);
            self.cap + self.overflow[p].len() - 1
        }
    }

    /// The rows of partition `p`: the region, then the overflow.
    #[inline(always)]
    pub fn part(&self, p: usize) -> [&[T]; 2] {
        let n = self.counts[p] as usize;
        let region = unsafe { std::slice::from_raw_parts(self.data.as_ptr().add(p * self.cap) as *const T, n) };
        [region, &self.overflow[p]]
    }

    pub fn part_len(&self, p: usize) -> usize {
        self.counts[p] as usize + self.overflow[p].len()
    }

    /// Raw write access to the rows, for updating rows in place from several
    /// threads (each row by at most one).
    pub fn raw(&mut self) -> Raw<T> {
        Raw {
            data: self.data.as_mut_ptr() as *mut T,
            cap: self.cap,
            overflow: self.overflow.iter_mut().map(|v| v.as_mut_ptr()).collect(),
        }
    }
}

pub struct Raw<T> {
    data: *mut T,
    cap: usize,
    overflow: Vec<*mut T>,
}

unsafe impl<T> Send for Raw<T> {}
unsafe impl<T> Sync for Raw<T> {}

impl<T> Raw<T> {
    /// Row `i` of partition `p` (as returned by push_idx).
    ///
    /// # Safety
    /// The row must exist, the Scatter must be alive and not otherwise
    /// borrowed, and no other thread may access the same row meanwhile.
    #[inline(always)]
    pub unsafe fn at(&self, p: usize, i: usize) -> *mut T {
        if i < self.cap {
            self.data.add(p * self.cap + i)
        } else {
            (*self.overflow.get_unchecked(p)).add(i - self.cap)
        }
    }
}
