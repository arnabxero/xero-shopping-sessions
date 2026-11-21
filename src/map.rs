//! A fixed-capacity open-addressing hash map from u64 keys (never u64::MAX).

pub const EMPTY: u64 = u64::MAX;

/// murmur3's 64-bit finalizer.
#[inline(always)]
pub fn mix(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    k ^= k >> 33;
    k
}

pub struct U64Map<V: Copy + Default> {
    slots: Vec<(u64, V)>,
    mask: usize,
}

impl<V: Copy + Default> U64Map<V> {
    /// Room for at least `n` keys at a load factor of at most 3/4.
    pub fn with_capacity(n: usize) -> Self {
        let cap = (n.max(12) * 4 / 3 + 1).next_power_of_two();
        U64Map { slots: vec![(EMPTY, V::default()); cap], mask: cap - 1 }
    }

    pub fn capacity(&self) -> usize {
        self.slots.len()
    }

    /// Hint the cache line holding the first probe slot of `k`.
    #[inline(always)]
    pub fn prefetch(&self, k: u64) {
        let i = mix(k) as usize & self.mask;
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            _mm_prefetch(self.slots.as_ptr().add(i) as *const i8, _MM_HINT_T0);
        }
    }

    #[inline(always)]
    pub fn get(&self, k: u64) -> Option<V> {
        let mut i = mix(k) as usize & self.mask;
        loop {
            let s = unsafe { self.slots.get_unchecked(i) };
            if s.0 == k {
                return Some(s.1);
            }
            if s.0 == EMPTY {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }

    /// The slot of `k`, inserting it (with the default value) if absent.
    #[inline(always)]
    pub fn insert_slot(&mut self, k: u64) -> usize {
        let mut i = mix(k) as usize & self.mask;
        loop {
            let s = &mut self.slots[i];
            if s.0 == k {
                return i;
            }
            if s.0 == EMPTY {
                s.0 = k;
                return i;
            }
            i = (i + 1) & self.mask;
        }
    }

    pub fn insert(&mut self, k: u64, v: V) {
        let i = self.insert_slot(k);
        self.slots[i].1 = v;
    }

    #[inline(always)]
    pub fn key_at(&self, i: usize) -> u64 {
        self.slots[i].0
    }

    #[inline(always)]
    pub fn val_at(&self, i: usize) -> V {
        self.slots[i].1
    }

    #[inline(always)]
    pub fn set_val_at(&mut self, i: usize, v: V) {
        self.slots[i].1 = v;
    }
}
