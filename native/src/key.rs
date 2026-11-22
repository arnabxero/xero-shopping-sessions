//! IDs as 128-bit keys. An ID of up to 15 bytes is packed exactly: its bytes
//! big-endian in the top 15 bytes and its length in the low byte, so comparing
//! two short keys as integers is comparing the IDs byte-wise. Longer IDs (the
//! generator writes none) are interned in a global table and carry 0xFF in the
//! low byte. Key 0 is the empty field, i.e. null.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Mutex;

pub type Key = u128;

#[derive(Default)]
struct Long {
    ids: HashMap<Vec<u8>, u32>,
    list: Vec<Vec<u8>>,
}

static LONG: Mutex<Option<Long>> = Mutex::new(None);

#[cold]
fn long_key(bytes: &[u8]) -> Key {
    let mut guard = LONG.lock().unwrap();
    let long = guard.get_or_insert_with(Long::default);
    let index = match long.ids.get(bytes) {
        Some(&index) => index,
        None => {
            let index = long.list.len() as u32;
            long.list.push(bytes.to_vec());
            long.ids.insert(bytes.to_vec(), index);
            index
        }
    };
    (u128::from(index) << 8) | 0xFF
}

/// For each length up to 15: the bytes to keep of the high and low halves.
static MASKS: [(u64, u64); 16] = {
    let mut masks = [(0u64, 0u64); 16];
    let mut len = 1;
    while len < 16 {
        masks[len] = if len < 8 { (!0u64 << (8 * (8 - len)), 0) } else if len == 8 { (!0, 0) } else { (!0, !0u64 << (8 * (16 - len))) };
        len += 1;
    }
    masks
};

/// The key of the field `data[start..end]`.
#[inline(always)]
pub fn key(data: &[u8], start: usize, end: usize) -> Key {
    let len = end - start;
    if len.wrapping_sub(1) < 15 && start + 16 <= data.len() {
        // SAFETY: the 16 bytes are inside `data`.
        let (high, low) = unsafe {
            let at = data.as_ptr().add(start);
            (u64::from_be(at.cast::<u64>().read_unaligned()), u64::from_be(at.add(8).cast::<u64>().read_unaligned()))
        };
        let mask = MASKS[len];
        return u128::from(high & mask.0) << 64 | u128::from(low & mask.1 | len as u64);
    }
    key_slow(&data[start..end])
}

#[inline(never)]
fn key_slow(bytes: &[u8]) -> Key {
    let len = bytes.len();
    if len == 0 {
        return 0;
    }
    if len > 15 {
        return long_key(bytes);
    }
    let mut buffer = [0u8; 16];
    buffer[..len].copy_from_slice(bytes);
    (u128::from_be_bytes(buffer) & (!0u128 << (8 * (16 - len)))) | len as u128
}

#[inline(always)]
pub fn is_long(key: Key) -> bool {
    key as u8 == 0xFF
}

/// The ID's bytes, appended to `out`.
#[inline(always)]
pub fn write_bytes(key: Key, out: &mut Vec<u8>) {
    if is_long(key) {
        let guard = LONG.lock().unwrap();
        out.extend_from_slice(&guard.as_ref().unwrap().list[(key >> 8) as usize]);
    } else {
        let len = (key as u8) as usize;
        out.extend_from_slice(&key.to_be_bytes()[..len]);
    }
}

/// Byte-wise order of the two IDs.
#[inline(always)]
pub fn compare(a: Key, b: Key) -> Ordering {
    if !is_long(a) && !is_long(b) {
        return a.cmp(&b);
    }
    let (mut x, mut y) = (Vec::new(), Vec::new());
    write_bytes(a, &mut x);
    write_bytes(b, &mut y);
    x.cmp(&y)
}

#[inline(always)]
pub fn hash(key: Key) -> u64 {
    let low = key as u64 ^ 0x9E37_79B9_7F4A_7C15;
    let high = (key >> 64) as u64 ^ 0xD6E8_FEB8_6659_FD93;
    let product = u128::from(low) * u128::from(high);
    let folded = (product as u64) ^ ((product >> 64) as u64);
    folded ^ (folded >> 29)
}

/// An open-addressing map from keys to a small value, built by one thread, then read by all.
pub struct KeyMap<V: Copy> {
    slots: Vec<(Key, V)>,
    mask: usize,
    len: usize,
}

impl<V: Copy + Default> KeyMap<V> {
    pub fn with_capacity(entries: usize) -> Self {
        let size = (entries * 2).max(16).next_power_of_two();
        Self { slots: Self::empty(size), mask: size - 1, len: 0 }
    }

    /// `size` empty slots, written rather than left to the kernel's lazy zero
    /// pages: reading such a page and then writing it costs two page faults and,
    /// with other threads running, a TLB shootdown.
    fn empty(size: usize) -> Vec<(Key, V)> {
        let mut slots = Vec::with_capacity(size);
        slots.resize(size, (0, V::default()));
        slots
    }


    #[cold]
    fn grow(&mut self) {
        let old = std::mem::replace(&mut self.slots, Self::empty((self.mask + 1) * 2));
        self.mask = self.slots.len() - 1;
        for (key, value) in old {
            if key != 0 {
                let mut at = hash(key) as usize & self.mask;
                while self.slots[at].0 != 0 {
                    at = (at + 1) & self.mask;
                }
                self.slots[at] = (key, value);
            }
        }
    }

    /// Inserts unless the key (never 0) is there already; returns the value now stored.
    #[inline(always)]
    pub fn insert(&mut self, key: Key, value: V) -> V {
        self.insert_hashed(key, hash(key), value)
    }

    #[inline(always)]
    pub fn insert_hashed(&mut self, key: Key, hash: u64, value: V) -> V {
        if self.len * 10 >= self.slots.len() * 6 {
            self.grow();
        }
        let mut at = hash as usize & self.mask;
        loop {
            // SAFETY: `at` is masked to the table size.
            let slot = unsafe { self.slots.get_unchecked_mut(at) };
            if slot.0 == key {
                return slot.1;
            }
            if slot.0 == 0 {
                *slot = (key, value);
                self.len += 1;
                return value;
            }
            at = (at + 1) & self.mask;
        }
    }

    #[inline(always)]
    pub fn get(&self, key: Key) -> Option<V> {
        self.get_hashed(key, hash(key))
    }

    /// `get` for a caller that already has `hash(key)`.
    #[inline(always)]
    pub fn get_hashed(&self, key: Key, hash: u64) -> Option<V> {
        if key == 0 {
            return None;
        }
        let mut at = hash as usize & self.mask;
        loop {
            // SAFETY: `at` is masked to the table size.
            let slot = unsafe { self.slots.get_unchecked(at) };
            if slot.0 == key {
                return Some(slot.1);
            }
            if slot.0 == 0 {
                return None;
            }
            at = (at + 1) & self.mask;
        }
    }

    #[inline(always)]
    pub fn prefetch_hashed(&self, hash: u64) {
        let at = hash as usize & self.mask;
        #[cfg(target_arch = "x86_64")]
        // SAFETY: prefetching never faults.
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            _mm_prefetch(self.slots.as_ptr().add(at).cast(), _MM_HINT_T0);
        }
        let _ = at;
    }

    pub fn slots_mut(&mut self) -> &mut [(Key, V)] {
        &mut self.slots
    }
}
