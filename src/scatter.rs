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
}
