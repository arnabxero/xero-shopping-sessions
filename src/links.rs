//! Direct 2-pass lock-free DSU link resolver (with 2-bit register-blocked Bloom filter
//! and flat open-addressed redirect table) and cold bucketed fallback.

use std::cmp::Ordering;
use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::{
    AtomicBool, AtomicU16, AtomicU32, AtomicU64, AtomicUsize, Ordering as MemOrdering,
};

use crate::csv::{
    fadvise_sequential, parse_csv_line, pread_csv_chunk, scan_csv_mmap_chunk,
    scan_csv_mmap_chunk_sink, CsvRowSink,
};
use crate::huge::HugeZeroed;
use crate::pack::{
    decode_canon_val, encode_canon_val, link_hash, mix64, pack_b37_pair, pack_b37_pair_89,
    OverflowInterner, B37_LIMIT, EMPTY_KEY,
};

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RedirectSlot {
    pub key: u64,
    pub val: u64,
}

#[repr(C)]
struct AtomicRedirectSlot {
    key: AtomicU64,
    val: AtomicU64,
}

const _: () = {
    assert!(std::mem::size_of::<AtomicRedirectSlot>() == std::mem::size_of::<RedirectSlot>());
    assert!(std::mem::align_of::<AtomicRedirectSlot>() == std::mem::align_of::<RedirectSlot>());
    assert!(std::mem::size_of::<AtomicRedirectSlot>() == 16);
    assert!(std::mem::align_of::<AtomicRedirectSlot>() == 8);
};

const LINK_BLOOM_MAX_SHIFT: u32 = 17; // 1<<17 words = 1 MB
const BUCKET_PROBED_NEXT: u32 = 0x4000_0000;
const BUCKET_HAS_OVERFLOW: u32 = 0x8000_0000;
const BUCKET_FLAG_MASK: u32 = BUCKET_PROBED_NEXT | BUCKET_HAS_OVERFLOW;
const BUCKET_CNT_MASK: u32 = !BUCKET_FLAG_MASK;
const OV_SLOT_TAG: usize = 1usize << 63;
const BUCKET6_MIN_REDIRECTS: usize = 16_384;
const BUCKET6_MAX_PROBE: usize = 8;
const BUCKET6_MIN_BUCKETS: usize = 20_480;

const _: () = assert!(BUCKET6_MIN_BUCKETS >= 2048 * (BUCKET6_MAX_PROBE + 1));

#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct Bucket6 {
    tags: [u32; 6],    // 24 B: ((h as u32) & 0x7FFF_FFFF) | 0x8000_0000; 0 = empty
    vals_lo: [u32; 6], // 24 B: low 32 bits of encoded_canon (< B37_LIMIT)
    vals_hi: [u16; 6], // 12 B: high 10 bits of encoded_canon
    meta: u32,         //  4 B: slot fill count; > 6 or BUCKET_HAS_OVERFLOW => check overflow_slots
}

#[repr(C, align(64))]
struct AtomicBucket6 {
    tags: [AtomicU32; 6],
    vals_lo: [AtomicU32; 6],
    vals_hi: [AtomicU16; 6],
    meta: AtomicU32,
}

const _: () = {
    assert!(std::mem::size_of::<Bucket6>() == 64);
    assert!(std::mem::align_of::<Bucket6>() == 64);
    assert!(std::mem::size_of::<AtomicBucket6>() == 64);
    assert!(std::mem::align_of::<AtomicBucket6>() == 64);
};

pub struct LinkRedirect {
    bitset: HugeZeroed<u64>,
    wmask: usize,
    buckets: HugeZeroed<Bucket6>,
    num_buckets: usize,
    overflow_slots: HugeZeroed<RedirectSlot>,
    ov_mask: usize,
}

#[inline(always)]
fn bloom_word_and_mask(h: u64, wmask: usize) -> (usize, u64) {
    let w = (h as usize) & wmask;
    let m = (1u64 << ((h >> 24) & 63)) | (1u64 << ((h >> 30) & 63));
    (w, m)
}

#[inline(always)]
fn bucket6_index(h: u64, num_buckets: usize) -> usize {
    (((h as u128) * (num_buckets as u128)) >> 42) as usize
}

#[inline(always)]
fn bucket6_tag(h: u64) -> u32 {
    ((h as u32) & 0x7FFF_FFFF) | 0x8000_0000
}

#[inline(always)]
fn prefetch_addr<T>(ptr: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch(
            ptr as *const i8,
            std::arch::x86_64::_MM_HINT_T0,
        );
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = ptr;
}

#[inline(always)]
fn match_bucket6_tag(b: &Bucket6, tag: u32) -> Option<u64> {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        use std::arch::x86_64::*;
        unsafe {
            let v = _mm256_load_si256(b as *const Bucket6 as *const __m256i);
            let cmp = _mm256_cmpeq_epi32(v, _mm256_set1_epi32(tag as i32));
            let m = (_mm256_movemask_ps(_mm256_castsi256_ps(cmp)) as u32) & 0x3F;
            if m != 0 {
                let lane = m.trailing_zeros() as usize;
                return Some(
                    (*b.vals_lo.get_unchecked(lane) as u64)
                        | ((*b.vals_hi.get_unchecked(lane) as u64) << 32),
                );
            }
        }
        return None;
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    {
        for lane in 0..6 {
            if b.tags[lane] == tag {
                return Some((b.vals_lo[lane] as u64) | ((b.vals_hi[lane] as u64) << 32));
            }
        }
        None
    }
}

impl LinkRedirect {
    pub fn empty() -> Self {
        Self {
            bitset: HugeZeroed::new(0),
            wmask: 0,
            buckets: HugeZeroed::new(0),
            num_buckets: 0,
            overflow_slots: HugeZeroed::new(0),
            ov_mask: 0,
        }
    }

    #[inline(always)]
    pub fn has_bucket6(&self) -> bool {
        self.num_buckets > 0
    }

    #[inline(always)]
    pub fn prefetch_bloom_b37(&self, k: u64) -> u64 {
        let h = crate::pack::mix42(k);
        let w = (h as usize) & self.wmask;
        prefetch_addr(unsafe { self.bitset.as_ptr().add(w) });
        h
    }

    #[inline(always)]
    pub fn bloom_test_b37(&self, h: u64) -> bool {
        let (w, m) = bloom_word_and_mask(h, self.wmask);
        let word = unsafe { *self.bitset.get_unchecked(w) };
        (word & m) == m
    }

    #[inline(always)]
    pub fn bucket6_slot_and_prefetch(&self, h: u64) -> usize {
        let idx = bucket6_index(h, self.num_buckets);
        prefetch_addr(unsafe { self.buckets.as_ptr().add(idx) });
        idx
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.num_buckets == 0 && self.overflow_slots.is_empty()
    }

    #[inline(always)]
    pub fn prefetch_bloom_for(&self, k: u64) -> u64 {
        let h = link_hash(k);
        if !self.bitset.is_empty() {
            let w = (h as usize) & self.wmask;
            prefetch_addr(unsafe { self.bitset.as_ptr().add(w) });
        }
        h
    }

    #[inline(always)]
    pub fn bloom_hit_slot_with_hash(&self, k: u64, h: u64) -> Option<usize> {
        if self.is_empty() {
            return None;
        }
        let (w, m) = bloom_word_and_mask(h, self.wmask);
        let word = unsafe { *self.bitset.get_unchecked(w) };
        if (word & m) != m {
            return None;
        }
        if k < B37_LIMIT && self.num_buckets > 0 {
            Some(bucket6_index(h, self.num_buckets))
        } else if !self.overflow_slots.is_empty() {
            Some(OV_SLOT_TAG | (((h >> 16) as usize) & self.ov_mask))
        } else {
            None
        }
    }

    #[inline(always)]
    pub fn prefetch_slot(&self, idx: usize) {
        if (idx & OV_SLOT_TAG) == 0 {
            prefetch_addr(unsafe { self.buckets.as_ptr().add(idx) });
        } else {
            prefetch_addr(unsafe { self.overflow_slots.as_ptr().add(idx & !OV_SLOT_TAG) });
        }
    }

    #[inline(always)]
    fn resolve_overflow(&self, k: u64, h: u64, mut idx: usize) -> u64 {
        if self.overflow_slots.is_empty() {
            return if k < B37_LIMIT { h } else { k };
        }
        loop {
            let s = unsafe { *self.overflow_slots.get_unchecked(idx) };
            if s.key == k {
                return s.val;
            }
            if s.key == EMPTY_KEY {
                return if k < B37_LIMIT { h } else { k };
            }
            idx = (idx + 1) & self.ov_mask;
        }
    }

    /// Resolve a user key `k` whose `link_hash(k) == h` using pre-computed slot `sidx`.
    /// Returns `mix42(person)` when `person < B37_LIMIT`, or raw `person` otherwise.
    #[inline(always)]
    pub fn resolve_from_slot_hashed(&self, k: u64, h: u64, sidx: usize) -> u64 {
        if (sidx & OV_SLOT_TAG) == 0 {
            let b = unsafe { self.buckets.get_unchecked(sidx) };
            let tag = bucket6_tag(h);
            if let Some(val) = match_bucket6_tag(b, tag) {
                return val;
            }
            let home_meta = b.meta;
            if (home_meta & BUCKET_FLAG_MASK) == 0 {
                return h;
            }
            if (home_meta & BUCKET_PROBED_NEXT) != 0 {
                let mut cur = sidx;
                for _ in 1..BUCKET6_MAX_PROBE {
                    cur += 1;
                    if cur >= self.num_buckets {
                        cur = 0;
                    }
                    let nb = unsafe { self.buckets.get_unchecked(cur) };
                    if let Some(val) = match_bucket6_tag(nb, tag) {
                        return val;
                    }
                    if (nb.meta & BUCKET_PROBED_NEXT) == 0 {
                        break;
                    }
                }
            }
            if (home_meta & BUCKET_HAS_OVERFLOW) != 0 {
                return self.resolve_overflow(k, h, ((h >> 16) as usize) & self.ov_mask);
            }
            return h;
        }
        self.resolve_overflow(k, h, sidx & !OV_SLOT_TAG)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    #[inline(always)]
    pub fn resolve(&self, k: u64) -> u64 {
        if self.is_empty() {
            return k;
        }
        let h = link_hash(k);
        let Some(idx) = self.bloom_hit_slot_with_hash(k, h) else {
            return k;
        };
        decode_canon_val(self.resolve_from_slot_hashed(k, h, idx))
    }
}

#[inline(always)]
fn intern_node(
    keys: &[AtomicU64],
    mask: usize,
    k: u64,
    local_added: &mut usize,
    global_nodes: &AtomicUsize,
    max_nodes: usize,
    overflowed: &AtomicBool,
) -> Option<u32> {
    let mut pos = (mix64(k) as usize) & mask;
    let mut probes = 0usize;
    loop {
        let cur = unsafe { keys.get_unchecked(pos) }.load(MemOrdering::Acquire);
        if cur == k {
            return Some(pos as u32);
        }
        if cur == EMPTY_KEY {
            match unsafe { keys.get_unchecked(pos) }.compare_exchange(
                EMPTY_KEY,
                k,
                MemOrdering::AcqRel,
                MemOrdering::Acquire,
            ) {
                Ok(_) => {
                    *local_added += 1;
                    if *local_added >= 16 {
                        let prev = global_nodes.fetch_add(*local_added, MemOrdering::Relaxed);
                        let added = *local_added;
                        *local_added = 0;
                        if prev + added > max_nodes {
                            overflowed.store(true, MemOrdering::Relaxed);
                            return None;
                        }
                    }
                    return Some(pos as u32);
                }
                Err(actual) => {
                    if actual == k {
                        return Some(pos as u32);
                    }
                }
            }
        }
        probes += 1;
        if probes > 1024 {
            if overflowed.load(MemOrdering::Relaxed) || probes > mask {
                overflowed.store(true, MemOrdering::Relaxed);
                return None;
            }
        }
        pos = (pos + 1) & mask;
    }
}

#[inline(always)]
fn find_root_zero(parent: &[AtomicU32], mut x: u32) -> u32 {
    loop {
        let p_raw = unsafe { parent.get_unchecked(x as usize) }.load(MemOrdering::Relaxed);
        if p_raw == 0 {
            let p2 = unsafe { parent.get_unchecked(x as usize) }.load(MemOrdering::Acquire);
            if p2 == 0 {
                return x;
            }
            x = p2 - 1;
            continue;
        }
        let p = p_raw - 1;
        let gp_raw = unsafe { parent.get_unchecked(p as usize) }.load(MemOrdering::Relaxed);
        if gp_raw != 0 {
            let _ = unsafe { parent.get_unchecked(x as usize) }.compare_exchange_weak(
                p_raw,
                gp_raw,
                MemOrdering::Relaxed,
                MemOrdering::Relaxed,
            );
        }
        x = p;
    }
}

#[inline(always)]
fn union_min_zero(
    parent: &[AtomicU32],
    keys: &[AtomicU64],
    overflow: &OverflowInterner,
    a: u32,
    b: u32,
) {
    loop {
        let mut u = find_root_zero(parent, a);
        let mut v = find_root_zero(parent, b);
        if u == v {
            return;
        }
        let ku = unsafe { keys.get_unchecked(u as usize) }.load(MemOrdering::Acquire);
        let kv = unsafe { keys.get_unchecked(v as usize) }.load(MemOrdering::Acquire);
        if overflow.cmp_user_key(kv, ku) == Ordering::Less {
            std::mem::swap(&mut u, &mut v);
        }
        if unsafe { parent.get_unchecked(v as usize) }
            .compare_exchange(0, u + 1, MemOrdering::AcqRel, MemOrdering::Acquire)
            .is_ok()
        {
            return;
        }
    }
}

#[inline(always)]
fn redirect_table_dims(num_redirects: usize) -> (usize, usize, usize) {
    let num_buckets = if num_redirects >= BUCKET6_MIN_REDIRECTS {
        ((num_redirects * 10) / 48).max(BUCKET6_MIN_BUCKETS)
    } else {
        0
    };
    let num_words = (num_redirects / 4)
        .next_power_of_two()
        .clamp(64, 1 << LINK_BLOOM_MAX_SHIFT);
    (num_buckets, num_words, num_words - 1)
}

#[inline(always)]
fn overflow_table_cap(ov_total: usize) -> usize {
    (ov_total * 10 / 7)
        .max(ov_total + 64)
        .next_power_of_two()
        .max(64)
}

fn finalize_redirect_table<F>(num_redirects: usize, num_threads: usize, fill: F) -> LinkRedirect
where
    F: FnOnce(&[AtomicBucket6], &[AtomicU64], usize, usize) -> Vec<Vec<(u64, u64, u64)>>,
{
    if num_redirects == 0 {
        return LinkRedirect::empty();
    }
    let (num_buckets, num_words, wmask) = redirect_table_dims(num_redirects);
    let atomic_buckets: HugeZeroed<AtomicBucket6> = HugeZeroed::new(num_buckets);
    let atomic_bitset: HugeZeroed<AtomicU64> = HugeZeroed::new(num_words);

    let ov_lists = fill(&atomic_buckets, &atomic_bitset, num_buckets, wmask);
    let ov_total: usize = ov_lists.iter().map(|v| v.len()).sum();

    let (overflow_slots, ov_mask) = if ov_total > 0 {
        let ov_cap = overflow_table_cap(ov_total);
        let mask = ov_cap - 1;
        let atomic_ov: HugeZeroed<AtomicRedirectSlot> = HugeZeroed::new(ov_cap);
        if ov_total < 4096 || num_threads <= 1 {
            for list in &ov_lists {
                for &(k, ev, h) in list {
                    insert_overflow_slot_with_hash(&atomic_ov, mask, k, ev, h);
                }
            }
        } else {
            let asl: &[AtomicRedirectSlot] = &atomic_ov;
            let lists_ref = &ov_lists;
            std::thread::scope(|s| {
                let mut hs = Vec::with_capacity(lists_ref.len());
                for list in lists_ref {
                    if list.is_empty() {
                        continue;
                    }
                    hs.push(s.spawn(move || {
                        for &(k, ev, h) in list {
                            insert_overflow_slot_with_hash(asl, mask, k, ev, h);
                        }
                    }));
                }
                for h in hs {
                    h.join().unwrap();
                }
            });
        }
        (atomic_ov.reinterpret::<RedirectSlot>(), mask)
    } else {
        (HugeZeroed::new(0), 0)
    };

    LinkRedirect {
        bitset: atomic_bitset.reinterpret::<u64>(),
        wmask,
        buckets: atomic_buckets.reinterpret::<Bucket6>(),
        num_buckets,
        overflow_slots,
        ov_mask,
    }
}

#[inline(always)]
fn insert_overflow_slot_with_hash(
    asl: &[AtomicRedirectSlot],
    mask: usize,
    key: u64,
    encoded_val: u64,
    h: u64,
) {
    let mut idx = ((h >> 16) as usize) & mask;
    loop {
        let s_ref = unsafe { asl.get_unchecked(idx) };
        if s_ref
            .key
            .compare_exchange(EMPTY_KEY, key, MemOrdering::Relaxed, MemOrdering::Relaxed)
            .is_ok()
        {
            s_ref.val.store(encoded_val, MemOrdering::Relaxed);
            break;
        }
        idx = (idx + 1) & mask;
    }
}

#[inline(always)]
fn insert_bucket6_with_hash(
    buckets: &[AtomicBucket6],
    abs: &[AtomicU64],
    num_buckets: usize,
    wmask: usize,
    key: u64,
    encoded_val: u64,
    h: u64,
    local_ov: &mut Vec<(u64, u64, u64)>,
) {
    let (w, m) = bloom_word_and_mask(h, wmask);
    unsafe { abs.get_unchecked(w) }.fetch_or(m, MemOrdering::Relaxed);

    if num_buckets > 0 && key < B37_LIMIT {
        let home = bucket6_index(h, num_buckets);
        if encoded_val < B37_LIMIT {
            let tag = bucket6_tag(h);
            let mut bidx = home;
            for step in 0..BUCKET6_MAX_PROBE {
                let b = unsafe { buckets.get_unchecked(bidx) };
                if (b.meta.load(MemOrdering::Relaxed) & BUCKET_CNT_MASK) < 6 {
                    let slot = (b.meta.fetch_add(1, MemOrdering::Relaxed) & BUCKET_CNT_MASK) as usize;
                    if slot < 6 {
                        unsafe {
                            b.tags
                                .get_unchecked(slot)
                                .store(tag, MemOrdering::Relaxed);
                            b.vals_lo
                                .get_unchecked(slot)
                                .store(encoded_val as u32, MemOrdering::Relaxed);
                            b.vals_hi
                                .get_unchecked(slot)
                                .store((encoded_val >> 32) as u16, MemOrdering::Relaxed);
                        }
                        return;
                    }
                }
                if step + 1 < BUCKET6_MAX_PROBE {
                    if (b.meta.load(MemOrdering::Relaxed) & BUCKET_PROBED_NEXT) == 0 {
                        b.meta.fetch_or(BUCKET_PROBED_NEXT, MemOrdering::Relaxed);
                    }
                    bidx += 1;
                    if bidx >= num_buckets {
                        bidx = 0;
                    }
                }
            }
        }
        let hb = unsafe { buckets.get_unchecked(home) };
        if (hb.meta.load(MemOrdering::Relaxed) & BUCKET_HAS_OVERFLOW) == 0 {
            hb.meta.fetch_or(BUCKET_HAS_OVERFLOW, MemOrdering::Relaxed);
        }
    }
    local_ov.push((key, encoded_val, h));
}

#[inline(always)]
fn insert_bucket6_canon(
    buckets: &[AtomicBucket6],
    abs: &[AtomicU64],
    num_buckets: usize,
    wmask: usize,
    key: u64,
    canon: u64,
    local_ov: &mut Vec<(u64, u64, u64)>,
) {
    insert_bucket6_with_hash(
        buckets,
        abs,
        num_buckets,
        wmask,
        key,
        encode_canon_val(canon),
        link_hash(key),
        local_ov,
    );
}

#[inline]
fn flush_edge_batch(
    batch: &[(u64, u64)],
    keys_ref: &[AtomicU64],
    parent_ref: &[AtomicU32],
    mask: usize,
    overflow: &OverflowInterner,
    local_added: &mut usize,
    global_nodes_ref: &AtomicUsize,
    max_nodes: usize,
    overflowed_ref: &AtomicBool,
) {
    for &(ka, kb) in batch {
        prefetch_addr(unsafe { keys_ref.as_ptr().add((mix64(ka) as usize) & mask) });
        prefetch_addr(unsafe { keys_ref.as_ptr().add((mix64(kb) as usize) & mask) });
    }
    for &(ka, kb) in batch {
        if overflowed_ref.load(MemOrdering::Relaxed) {
            return;
        }
        let Some(ia) = intern_node(
            keys_ref,
            mask,
            ka,
            local_added,
            global_nodes_ref,
            max_nodes,
            overflowed_ref,
        ) else {
            return;
        };
        let Some(ib) = intern_node(
            keys_ref,
            mask,
            kb,
            local_added,
            global_nodes_ref,
            max_nodes,
            overflowed_ref,
        ) else {
            return;
        };
        union_min_zero(parent_ref, keys_ref, overflow, ia, ib);
    }
}

struct LinkSink<'a> {
    keys_ref: &'a [AtomicU64],
    parent_ref: &'a [AtomicU32],
    mask: usize,
    overflow: &'a OverflowInterner,
    local_added: usize,
    local_edges: bool,
    global_nodes_ref: &'a AtomicUsize,
    max_nodes: usize,
    overflowed_ref: &'a AtomicBool,
    edge_batch: [(u64, u64); 32],
    batch_len: usize,
}

impl<'a> LinkSink<'a> {
    #[inline(always)]
    fn push_pair(&mut self, a: &[u8], b: &[u8]) {
        let (ka, kb) = pack_b37_pair(a, b'u', b, b'u')
            .unwrap_or_else(|| (self.overflow.encode_user(a), self.overflow.encode_user(b)));
        if ka != kb {
            self.local_edges = true;
            self.edge_batch[self.batch_len] = (ka, kb);
            self.batch_len += 1;
            if self.batch_len == 32 {
                flush_edge_batch(
                    &self.edge_batch,
                    self.keys_ref,
                    self.parent_ref,
                    self.mask,
                    self.overflow,
                    &mut self.local_added,
                    self.global_nodes_ref,
                    self.max_nodes,
                    self.overflowed_ref,
                );
                self.batch_len = 0;
            }
        }
    }

    #[inline(always)]
    fn flush_tail(&mut self) {
        if self.batch_len > 0 && !self.overflowed_ref.load(MemOrdering::Relaxed) {
            flush_edge_batch(
                &self.edge_batch[..self.batch_len],
                self.keys_ref,
                self.parent_ref,
                self.mask,
                self.overflow,
                &mut self.local_added,
                self.global_nodes_ref,
                self.max_nodes,
                self.overflowed_ref,
            );
        }
        self.batch_len = 0;
    }
}

impl CsvRowSink<2> for LinkSink<'_> {
    #[inline(always)]
    unsafe fn on_pos(&mut self, ptr: *const u8, s0: usize, rpos: *const u16) {
        if self.overflowed_ref.load(MemOrdering::Relaxed) {
            return;
        }
        let e0 = *rpos as usize;
        let q0 = usize::from(*ptr.add(s0) == b'"');
        let a_len = (e0 - s0) - (q0 << 1);
        if a_len == 0 {
            return;
        }
        let e1 = *rpos.add(1) as usize;
        let s1 = e0 + 1;
        let q1 = usize::from(*ptr.add(s1) == b'"');
        let b_len = (e1 - s1) - (q1 << 1);
        if b_len == 0 {
            return;
        }
        let a = std::slice::from_raw_parts(ptr.add(s0 + q0), a_len);
        let b = std::slice::from_raw_parts(ptr.add(s1 + q1), b_len);
        self.push_pair(a, b);
    }

    #[inline(always)]
    fn on_row(&mut self, &[a, b]: &[&[u8]; 2]) {
        if self.overflowed_ref.load(MemOrdering::Relaxed) {
            return;
        }
        if !a.is_empty() && !b.is_empty() {
            self.push_pair(a, b);
        }
    }
}

#[inline(always)]
fn intern_node_st(
    keys: &mut [u64],
    mask: usize,
    k: u64,
    num_nodes: &mut usize,
    max_nodes: usize,
) -> Option<u32> {
    let mut pos = (mix64(k) as usize) & mask;
    let mut probes = 0usize;
    loop {
        let cur = unsafe { *keys.get_unchecked(pos) };
        if cur == k {
            return Some(pos as u32);
        }
        if cur == EMPTY_KEY {
            *num_nodes += 1;
            if *num_nodes > max_nodes {
                return None;
            }
            unsafe { *keys.get_unchecked_mut(pos) = k };
            return Some(pos as u32);
        }
        probes += 1;
        if probes > 1024 && probes > mask {
            return None;
        }
        pos = (pos + 1) & mask;
    }
}

#[inline(always)]
fn find_root_zero_st(parent: &mut [u32], mut x: u32) -> u32 {
    loop {
        let p_raw = unsafe { *parent.get_unchecked(x as usize) };
        if p_raw == 0 {
            return x;
        }
        let p = p_raw - 1;
        let gp_raw = unsafe { *parent.get_unchecked(p as usize) };
        if gp_raw != 0 {
            unsafe { *parent.get_unchecked_mut(x as usize) = gp_raw };
        }
        x = p;
    }
}

#[inline(always)]
fn union_min_zero_st(
    parent: &mut [u32],
    keys: &[u64],
    overflow: &OverflowInterner,
    a: u32,
    b: u32,
) {
    let mut u = find_root_zero_st(parent, a);
    let mut v = find_root_zero_st(parent, b);
    if u == v {
        return;
    }
    let ku = unsafe { *keys.get_unchecked(u as usize) };
    let kv = unsafe { *keys.get_unchecked(v as usize) };
    if overflow.cmp_user_key(kv, ku) == Ordering::Less {
        std::mem::swap(&mut u, &mut v);
    }
    unsafe { *parent.get_unchecked_mut(v as usize) = u + 1 };
}

struct LinkSinkSt<'a> {
    keys: &'a mut [u64],
    parent: &'a mut [u32],
    mask: usize,
    overflow: &'a OverflowInterner,
    num_nodes: usize,
    max_nodes: usize,
    overflowed: bool,
    has_edges: bool,
    edge_batch: [(u64, u64); 32],
    batch_len: usize,
}

impl<'a> LinkSinkSt<'a> {
    #[inline]
    fn flush_batch(&mut self) {
        for &(ka, kb) in &self.edge_batch[..self.batch_len] {
            prefetch_addr(unsafe { self.keys.as_ptr().add((mix64(ka) as usize) & self.mask) });
            prefetch_addr(unsafe { self.keys.as_ptr().add((mix64(kb) as usize) & self.mask) });
        }
        for &(ka, kb) in &self.edge_batch[..self.batch_len] {
            if self.overflowed {
                return;
            }
            let Some(ia) = intern_node_st(
                self.keys,
                self.mask,
                ka,
                &mut self.num_nodes,
                self.max_nodes,
            ) else {
                self.overflowed = true;
                return;
            };
            let Some(ib) = intern_node_st(
                self.keys,
                self.mask,
                kb,
                &mut self.num_nodes,
                self.max_nodes,
            ) else {
                self.overflowed = true;
                return;
            };
            union_min_zero_st(self.parent, self.keys, self.overflow, ia, ib);
        }
    }

    #[inline(always)]
    fn push_pair(&mut self, a: &[u8], b: &[u8]) {
        let (ka, kb) = pack_b37_pair(a, b'u', b, b'u')
            .unwrap_or_else(|| (self.overflow.encode_user(a), self.overflow.encode_user(b)));
        if ka != kb {
            self.has_edges = true;
            self.edge_batch[self.batch_len] = (ka, kb);
            self.batch_len += 1;
            if self.batch_len == 32 {
                self.flush_batch();
                self.batch_len = 0;
            }
        }
    }

    #[inline(always)]
    fn flush_tail(&mut self) {
        if self.batch_len > 0 && !self.overflowed {
            self.flush_batch();
        }
        self.batch_len = 0;
    }
}

impl CsvRowSink<2> for LinkSinkSt<'_> {
    #[inline(always)]
    unsafe fn on_pos(&mut self, ptr: *const u8, s0: usize, rpos: *const u16) {
        if self.overflowed {
            return;
        }
        let e0 = *rpos as usize;
        let q0 = usize::from(*ptr.add(s0) == b'"');
        let a_len = (e0 - s0) - (q0 << 1);
        if a_len == 0 {
            return;
        }
        let e1 = *rpos.add(1) as usize;
        let s1 = e0 + 1;
        let q1 = usize::from(*ptr.add(s1) == b'"');
        let b_len = (e1 - s1) - (q1 << 1);
        if b_len == 0 {
            return;
        }
        let a = std::slice::from_raw_parts(ptr.add(s0 + q0), a_len);
        let b = std::slice::from_raw_parts(ptr.add(s1 + q1), b_len);
        self.push_pair(a, b);
    }

    #[inline(always)]
    fn on_row(&mut self, &[a, b]: &[&[u8]; 2]) {
        if self.overflowed {
            return;
        }
        if !a.is_empty() && !b.is_empty() {
            self.push_pair(a, b);
        }
    }
}

#[inline(always)]
fn insert_bucket6_st(
    buckets: &mut [Bucket6],
    bitset: &mut [u64],
    num_buckets: usize,
    wmask: usize,
    key: u64,
    encoded_val: u64,
    h: u64,
    local_ov: &mut Vec<(u64, u64, u64)>,
) {
    let (w, m) = bloom_word_and_mask(h, wmask);
    unsafe { *bitset.get_unchecked_mut(w) |= m };

    if num_buckets > 0 && key < B37_LIMIT {
        let home = bucket6_index(h, num_buckets);
        if encoded_val < B37_LIMIT {
            let tag = bucket6_tag(h);
            let mut bidx = home;
            for step in 0..BUCKET6_MAX_PROBE {
                let b = unsafe { buckets.get_unchecked_mut(bidx) };
                let slot = (b.meta & BUCKET_CNT_MASK) as usize;
                if slot < 6 {
                    b.tags[slot] = tag;
                    b.vals_lo[slot] = encoded_val as u32;
                    b.vals_hi[slot] = (encoded_val >> 32) as u16;
                    b.meta += 1;
                    return;
                }
                if step + 1 < BUCKET6_MAX_PROBE {
                    b.meta |= BUCKET_PROBED_NEXT;
                    bidx += 1;
                    if bidx >= num_buckets {
                        bidx = 0;
                    }
                }
            }
        }
        unsafe { buckets.get_unchecked_mut(home) }.meta |= BUCKET_HAS_OVERFLOW;
    }
    local_ov.push((key, encoded_val, h));
}

fn build_link_redirect_st(
    files: Vec<(File, usize)>,
    jobs: Vec<(usize, usize, usize)>,
    total_link_bytes: usize,
    cap: usize,
    mask: usize,
    max_nodes: usize,
    overflow: &OverflowInterner,
) -> LinkRedirect {
    let mut keys: HugeZeroed<u64> = HugeZeroed::new(cap);
    let mut parent: HugeZeroed<u32> = HugeZeroed::new(cap);
    let mut io_buf: Vec<u8> = vec![0u8; 256 * 1024 + 32_768];

    let (overflowed, has_edges) = {
        let mut sink = LinkSinkSt {
            keys: keys.as_mut_slice(),
            parent: parent.as_mut_slice(),
            mask,
            overflow,
            num_nodes: 0,
            max_nodes,
            overflowed: false,
            has_edges: false,
            edge_batch: [(0u64, 0u64); 32],
            batch_len: 0,
        };
        for &(fidx, start, end) in &jobs {
            if sink.overflowed {
                break;
            }
            let (ref f, flen) = files[fidx];
            let (got, ls, le) = pread_csv_chunk(f, flen, start, end, &mut io_buf);
            if got > 0 {
                scan_csv_mmap_chunk_sink::<2, _>(&io_buf[..got], ls, le, &mut sink);
                sink.flush_tail();
            }
        }
        (sink.overflowed, sink.has_edges)
    };

    if overflowed {
        drop(keys);
        drop(parent);
        return build_link_redirect_fallback(&files, &jobs, 1, overflow);
    }

    if !has_edges {
        if total_link_bytes < 256 * 1024 * 1024 {
            std::mem::forget(keys);
            std::mem::forget(parent);
        }
        return LinkRedirect::empty();
    }

    let mut num_redirects = 0usize;
    {
        let p_sl = parent.as_mut_slice();
        for i in 0..cap {
            let p_raw = unsafe { *p_sl.get_unchecked(i) };
            if p_raw != 0 {
                let mut r = (p_raw - 1) as usize;
                loop {
                    let pr = unsafe { *p_sl.get_unchecked(r) };
                    if pr == 0 {
                        break;
                    }
                    r = (pr - 1) as usize;
                }
                if r + 1 != p_raw as usize {
                    unsafe { *p_sl.get_unchecked_mut(i) = (r + 1) as u32 };
                }
                num_redirects += 1;
            }
        }
    }

    let (num_buckets, num_words, wmask) = redirect_table_dims(num_redirects);
    let mut buckets: HugeZeroed<Bucket6> = HugeZeroed::new(num_buckets);
    let mut bitset: HugeZeroed<u64> = HugeZeroed::new(num_words);
    let mut local_ov: Vec<(u64, u64, u64)> = Vec::new();

    {
        let p_sl = parent.as_slice();
        let k_sl = keys.as_slice();
        let b_sl = buckets.as_mut_slice();
        let bs_sl = bitset.as_mut_slice();
        let mut batch = [(0u64, 0usize, 0u64); 32];
        let mut blen = 0usize;
        for i in 0..cap {
            let p_raw = unsafe { *p_sl.get_unchecked(i) };
            if p_raw != 0 {
                let key = unsafe { *k_sl.get_unchecked(i) };
                let h = link_hash(key);
                let root_idx = (p_raw - 1) as usize;
                prefetch_addr(unsafe { k_sl.as_ptr().add(root_idx) });
                prefetch_addr(unsafe { bs_sl.as_ptr().add((h as usize) & wmask) });
                if num_buckets > 0 && key < B37_LIMIT {
                    prefetch_addr(unsafe { b_sl.as_ptr().add(bucket6_index(h, num_buckets)) });
                }
                batch[blen] = (key, root_idx, h);
                blen += 1;
                if blen == 32 {
                    for &(k, ridx, kh) in &batch {
                        let canon = unsafe { *k_sl.get_unchecked(ridx) };
                        insert_bucket6_st(
                            b_sl,
                            bs_sl,
                            num_buckets,
                            wmask,
                            k,
                            encode_canon_val(canon),
                            kh,
                            &mut local_ov,
                        );
                    }
                    blen = 0;
                }
            }
        }
        for &(k, ridx, kh) in &batch[..blen] {
            let canon = unsafe { *k_sl.get_unchecked(ridx) };
            insert_bucket6_st(
                b_sl,
                bs_sl,
                num_buckets,
                wmask,
                k,
                encode_canon_val(canon),
                kh,
                &mut local_ov,
            );
        }
    }

    let (overflow_slots, ov_mask) = if !local_ov.is_empty() {
        let ov_cap = overflow_table_cap(local_ov.len());
        let ov_mask = ov_cap - 1;
        let mut ov_slots: HugeZeroed<RedirectSlot> = HugeZeroed::new(ov_cap);
        let ov_sl = ov_slots.as_mut_slice();
        for &(k, ev, h) in &local_ov {
            let mut idx = ((h >> 16) as usize) & ov_mask;
            while unsafe { ov_sl.get_unchecked(idx) }.key != EMPTY_KEY {
                idx = (idx + 1) & ov_mask;
            }
            unsafe {
                *ov_sl.get_unchecked_mut(idx) = RedirectSlot { key: k, val: ev };
            }
        }
        (ov_slots, ov_mask)
    } else {
        (HugeZeroed::new(0), 0)
    };

    if total_link_bytes < 256 * 1024 * 1024 {
        std::mem::forget(keys);
        std::mem::forget(parent);
    }

    LinkRedirect {
        bitset,
        wmask,
        buckets,
        num_buckets,
        overflow_slots,
        ov_mask,
    }
}

#[cold]
#[inline(never)]
fn fallback_link_line(
    data: &[u8],
    line_start: usize,
    stop_excl: usize,
    sink: &mut LinkSink<'_>,
) -> usize {
    let rem = &data[line_start..stop_excl];
    if let Some(rel) = memchr::memchr(b'\n', rem) {
        if let Some(row) = parse_csv_line::<2>(&rem[..rel]) {
            sink.on_row(&row);
        }
        line_start + rel + 1
    } else {
        if !rem.is_empty() {
            if let Some(row) = parse_csv_line::<2>(rem) {
                sink.on_row(&row);
            }
        }
        stop_excl
    }
}

/// Direct single-pass links CSV scanner bypassing 4 KB intermediate delimiter buffers
/// on 8/9-byte `'u'`-prefixed lines while falling back line-by-line cleanly.
#[inline(never)]
unsafe fn scan_links_direct(data: &[u8], start: usize, end: usize, sink: &mut LinkSink<'_>) {
    if start >= end || start >= data.len() {
        return;
    }
    let end = end.min(data.len());
    let start_pos = if start == 0 {
        let Some(nl) = memchr::memchr(b'\n', data) else {
            return;
        };
        nl + 1
    } else {
        let Some(rel) = memchr::memchr(b'\n', &data[start - 1..]) else {
            return;
        };
        start + rel
    };
    if start_pos >= end {
        return;
    }
    let stop_excl = if let Some(rel) = memchr::memchr(b'\n', &data[end - 1..]) {
        end + rel
    } else {
        data.len()
    };

    let base = data.as_ptr();
    let mut cur = start_pos;
    let safe_limit = stop_excl.saturating_sub(32);

    'outer: while cur < safe_limit {
        if sink.overflowed_ref.load(MemOrdering::Relaxed) {
            return;
        }
        let line_start = cur;
        let mut p = base.add(cur);
        'fast: {
            let q0 = usize::from(*p == b'"');
            p = p.add(q0);
            let a_p = p;
            let is9_0 = usize::from(*p.add(8) >= b'0');
            let len0 = 8 + is9_0;
            let ok0 = (*p == b'u') & (*p.add(len0 + q0) == b',') & ((q0 == 0) | (*p.add(len0) == b'"'));
            p = p.add(len0 + q0 + 1);

            let q1 = usize::from(*p == b'"');
            p = p.add(q1);
            let b_p = p;
            let is9_1 = usize::from(*p.add(8) >= b'0');
            let len1 = 8 + is9_1;
            let ok1 = (*p == b'u') & (*p.add(len1 + q1) == b'\n') & ((q1 == 0) | (*p.add(len1) == b'"'));
            p = p.add(len1 + q1 + 1);

            if !(ok0 & ok1) {
                break 'fast;
            }
            let Some((ka, kb)) = pack_b37_pair_89(a_p, is9_0, b_p, is9_1) else {
                break 'fast;
            };
            if ka != kb {
                sink.local_edges = true;
                sink.edge_batch[sink.batch_len] = (ka, kb);
                sink.batch_len += 1;
                if sink.batch_len == 32 {
                    flush_edge_batch(
                        &sink.edge_batch,
                        sink.keys_ref,
                        sink.parent_ref,
                        sink.mask,
                        sink.overflow,
                        &mut sink.local_added,
                        sink.global_nodes_ref,
                        sink.max_nodes,
                        sink.overflowed_ref,
                    );
                    sink.batch_len = 0;
                }
            }
            cur = p.offset_from(base) as usize;
            continue 'outer;
        }
        cur = fallback_link_line(data, line_start, stop_excl, sink);
    }
    while cur < stop_excl {
        if sink.overflowed_ref.load(MemOrdering::Relaxed) {
            return;
        }
        cur = fallback_link_line(data, cur, stop_excl, sink);
    }
}

pub fn build_link_redirect(
    link_files: &[PathBuf],
    num_threads: usize,
    overflow: &OverflowInterner,
) -> LinkRedirect {
    if link_files.is_empty() {
        return LinkRedirect::empty();
    }

    let files: Vec<(File, usize)> = link_files
        .iter()
        .filter_map(|p| {
            let f = File::open(p).ok()?;
            let len = f.metadata().ok()?.len() as usize;
            if len == 0 {
                return None;
            }
            fadvise_sequential(&f);
            Some((f, len))
        })
        .collect();

    let mut total_link_bytes = 0usize;
    let mut jobs: Vec<(usize, usize, usize)> = Vec::new();
    const CHUNK_BYTES: usize = 256 * 1024;
    for (fidx, &(_, len)) in files.iter().enumerate() {
        total_link_bytes += len;
        let mut pos = 0usize;
        while pos < len {
            let end = (pos + CHUNK_BYTES).min(len);
            jobs.push((fidx, pos, end));
            pos = end;
        }
    }

    if jobs.is_empty() {
        return LinkRedirect::empty();
    }

    let nt = num_threads.max(1);
    let cap = ((total_link_bytes / 12).max(4096)).next_power_of_two();
    let mask = cap - 1;
    let max_nodes = (cap * 3) / 4;

    if nt == 1 {
        return build_link_redirect_st(
            files,
            jobs,
            total_link_bytes,
            cap,
            mask,
            max_nodes,
            overflow,
        );
    }

    let keys: HugeZeroed<AtomicU64> = HugeZeroed::new(cap);
    let parent: HugeZeroed<AtomicU32> = HugeZeroed::new(cap);
    let global_nodes = AtomicUsize::new(0);
    let overflowed = AtomicBool::new(false);
    let has_edges = AtomicBool::new(false);
    let next_job = AtomicUsize::new(0);

    // Pass 1: Read CSV chunks and execute batched-prefetch lock-free DSU directly inside flat hash table
    std::thread::scope(|s| {
        let read_nt = nt.min(jobs.len());
        let mut handles = Vec::with_capacity(read_nt);
        let files_ref = &files;
        let jobs_ref = &jobs;
        let next_job_ref = &next_job;
        let keys_ref: &[AtomicU64] = &keys;
        let parent_ref: &[AtomicU32] = &parent;
        let global_nodes_ref = &global_nodes;
        let overflowed_ref = &overflowed;
        let has_edges_ref = &has_edges;

        for _ in 0..read_nt {
            handles.push(s.spawn(move || {
                let mut io_buf: Vec<u8> = vec![0u8; CHUNK_BYTES + 32_768];
                let mut sink = LinkSink {
                    keys_ref,
                    parent_ref,
                    mask,
                    overflow,
                    local_added: 0,
                    local_edges: false,
                    global_nodes_ref,
                    max_nodes,
                    overflowed_ref,
                    edge_batch: [(0u64, 0u64); 32],
                    batch_len: 0,
                };
                loop {
                    if overflowed_ref.load(MemOrdering::Relaxed) {
                        break;
                    }
                    let jid = next_job_ref.fetch_add(1, MemOrdering::Relaxed);
                    let Some(&(fidx, start, end)) = jobs_ref.get(jid) else {
                        break;
                    };
                    let (ref f, flen) = files_ref[fidx];
                    let (got, ls, le) = pread_csv_chunk(f, flen, start, end, &mut io_buf);
                    if got > 0 {
                        unsafe {
                            scan_links_direct(&io_buf[..got], ls, le, &mut sink);
                        }
                        sink.flush_tail();
                    }
                }
                if sink.local_edges {
                    has_edges_ref.store(true, MemOrdering::Relaxed);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    });

    if overflowed.load(MemOrdering::Relaxed) {
        drop(keys);
        drop(parent);
        return build_link_redirect_fallback(&files, &jobs, nt, overflow);
    }

    if !has_edges.load(MemOrdering::Relaxed) {
        if total_link_bytes < 256 * 1024 * 1024 {
            std::mem::forget(keys);
            std::mem::forget(parent);
        }
        return LinkRedirect::empty();
    }

    // Pass 2a: Parallel path compression over non-root nodes in 0..cap
    let chunk_n = (cap + nt - 1) / nt;
    let redirect_count = AtomicUsize::new(0);
    std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(nt);
        for t in 0..nt {
            let start = t * chunk_n;
            if start >= cap {
                break;
            }
            let end = (start + chunk_n).min(cap);
            let p_ref: &[AtomicU32] = &parent;
            let rc_ref = &redirect_count;
            handles.push(s.spawn(move || {
                let mut local_redir = 0usize;
                for i in start..end {
                    let p_raw = unsafe { p_ref.get_unchecked(i) }.load(MemOrdering::Relaxed);
                    if p_raw != 0 {
                        let mut r = (p_raw - 1) as usize;
                        loop {
                            let pr = unsafe { p_ref.get_unchecked(r) }.load(MemOrdering::Relaxed);
                            if pr == 0 {
                                break;
                            }
                            r = (pr - 1) as usize;
                        }
                        if r + 1 != p_raw as usize {
                            unsafe { p_ref.get_unchecked(i) }
                                .store((r + 1) as u32, MemOrdering::Relaxed);
                        }
                        local_redir += 1;
                    }
                }
                rc_ref.fetch_add(local_redir, MemOrdering::Relaxed);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    });

    let num_redirects = redirect_count.into_inner();
    let res = finalize_redirect_table(num_redirects, nt, |buckets, abs, num_buckets, wmask| {
        // Pass 2b: Batched-prefetch scatter (key -> encoded_canon) into 6-slot AVX2 buckets
        std::thread::scope(|s| {
            let mut handles = Vec::with_capacity(nt);
            for t in 0..nt {
                let start = t * chunk_n;
                if start >= cap {
                    break;
                }
                let end = (start + chunk_n).min(cap);
                let p_ref: &[AtomicU32] = &parent;
                let k_ref: &[AtomicU64] = &keys;
                handles.push(s.spawn(move || {
                    let mut local_ov: Vec<(u64, u64, u64)> = Vec::new();
                    let mut batch = [(0u64, 0usize, 0u64); 32];
                    let mut blen = 0usize;
                    for i in start..end {
                        let p_raw = unsafe { p_ref.get_unchecked(i) }.load(MemOrdering::Relaxed);
                        if p_raw != 0 {
                            let key = unsafe { k_ref.get_unchecked(i) }.load(MemOrdering::Relaxed);
                            let h = link_hash(key);
                            let root_idx = (p_raw - 1) as usize;
                            prefetch_addr(unsafe { k_ref.as_ptr().add(root_idx) });
                            prefetch_addr(unsafe { abs.as_ptr().add((h as usize) & wmask) });
                            if num_buckets > 0 && key < B37_LIMIT {
                                prefetch_addr(unsafe {
                                    buckets.as_ptr().add(bucket6_index(h, num_buckets))
                                });
                            }
                            batch[blen] = (key, root_idx, h);
                            blen += 1;
                            if blen == 32 {
                                for &(k, ridx, kh) in &batch {
                                    let canon = unsafe { k_ref.get_unchecked(ridx) }
                                        .load(MemOrdering::Relaxed);
                                    insert_bucket6_with_hash(
                                        buckets,
                                        abs,
                                        num_buckets,
                                        wmask,
                                        k,
                                        encode_canon_val(canon),
                                        kh,
                                        &mut local_ov,
                                    );
                                }
                                blen = 0;
                            }
                        }
                    }
                    if blen > 0 {
                        for &(k, ridx, kh) in &batch[..blen] {
                            let canon = unsafe { k_ref.get_unchecked(ridx) }
                                .load(MemOrdering::Relaxed);
                            insert_bucket6_with_hash(
                                buckets,
                                abs,
                                num_buckets,
                                wmask,
                                k,
                                encode_canon_val(canon),
                                kh,
                                &mut local_ov,
                            );
                        }
                    }
                    local_ov
                }));
            }
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        })
    });

    if total_link_bytes < 256 * 1024 * 1024 {
        std::mem::forget(keys);
        std::mem::forget(parent);
    }
    res
}

// =========================================================================
// Cold Fallback: Bucketed DSU for Ultra-Dense / Short-ID Adversarial Links
// =========================================================================

struct BucketDict {
    slots: Vec<(u64, u32)>,
    mask: usize,
    base_id: u32,
}

impl BucketDict {
    #[inline(always)]
    fn lookup_global(&self, k: u64) -> u32 {
        let mut idx = ((mix64(k) >> 16) as usize) & self.mask;
        loop {
            let (sk, local_id) = unsafe { *self.slots.get_unchecked(idx) };
            if sk == k {
                return self.base_id + local_id;
            }
            idx = (idx + 1) & self.mask;
        }
    }
}

#[inline(always)]
fn find_root_dense(parent: &[AtomicU32], mut x: u32) -> u32 {
    loop {
        let p = parent[x as usize].load(MemOrdering::Relaxed);
        if p == x {
            let p2 = parent[x as usize].load(MemOrdering::Acquire);
            if p2 == x {
                return x;
            }
            x = p2;
            continue;
        }
        let gp = parent[p as usize].load(MemOrdering::Relaxed);
        if gp != p {
            let _ = parent[x as usize].compare_exchange_weak(
                p,
                gp,
                MemOrdering::Relaxed,
                MemOrdering::Relaxed,
            );
        }
        x = p;
    }
}

#[inline]
fn union_min_dense(
    parent: &[AtomicU32],
    keys: &[u64],
    overflow: &OverflowInterner,
    a: u32,
    b: u32,
) {
    loop {
        let mut u = find_root_dense(parent, a);
        let mut v = find_root_dense(parent, b);
        if u == v {
            return;
        }
        if overflow.cmp_user_key(keys[v as usize], keys[u as usize]) == Ordering::Less {
            std::mem::swap(&mut u, &mut v);
        }
        if parent[v as usize]
            .compare_exchange(v, u, MemOrdering::AcqRel, MemOrdering::Acquire)
            .is_ok()
        {
            return;
        }
    }
}

struct ThreadLinkData {
    edges: Vec<(u64, u64)>,
    key_buckets: Vec<Vec<u64>>,
}

struct BucketDedupResult {
    unique_keys: Vec<u64>,
    slots: Vec<(u64, u32)>,
    mask: usize,
}

#[cold]
fn build_link_redirect_fallback(
    files: &[(File, usize)],
    jobs: &[(usize, usize, usize)],
    num_threads: usize,
    overflow: &OverflowInterner,
) -> LinkRedirect {
    let nt = num_threads.max(1).min(jobs.len());
    let next_job = AtomicUsize::new(0);

    let thread_data: Vec<ThreadLinkData> = std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(nt);
        let next_job_ref = &next_job;
        for _ in 0..nt {
            handles.push(s.spawn(move || {
                let mut io_buf: Vec<u8> = vec![0u8; 256 * 1024 + 32_768];
                let mut edges: Vec<(u64, u64)> = Vec::new();
                let mut key_buckets: Vec<Vec<u64>> = (0..64).map(|_| Vec::new()).collect();
                loop {
                    let jid = next_job_ref.fetch_add(1, MemOrdering::Relaxed);
                    let Some(&(fidx, start, end)) = jobs.get(jid) else {
                        break;
                    };
                    let (ref f, flen) = files[fidx];
                    let (got, ls, le) = pread_csv_chunk(f, flen, start, end, &mut io_buf);
                    if got == 0 {
                        continue;
                    }
                    scan_csv_mmap_chunk::<2, _>(&io_buf[..got], ls, le, |&[a, b]| {
                        if !a.is_empty() && !b.is_empty() {
                            let ka = overflow.encode_user(a);
                            let kb = overflow.encode_user(b);
                            if ka != kb {
                                edges.push((ka, kb));
                                key_buckets[(mix64(ka) as usize) & 63].push(ka);
                                key_buckets[(mix64(kb) as usize) & 63].push(kb);
                            }
                        }
                    });
                }
                ThreadLinkData { edges, key_buckets }
            }));
        }
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let total_edges: usize = thread_data.iter().map(|t| t.edges.len()).sum();
    if total_edges == 0 {
        return LinkRedirect::empty();
    }

    let mut bucket_inputs: Vec<Vec<Vec<u64>>> = (0..64).map(|_| Vec::with_capacity(nt)).collect();
    let mut thread_edges: Vec<Vec<(u64, u64)>> = Vec::with_capacity(nt);
    for td in thread_data {
        thread_edges.push(td.edges);
        for (b, kb) in td.key_buckets.into_iter().enumerate() {
            if !kb.is_empty() {
                bucket_inputs[b].push(kb);
            }
        }
    }

    let next_bucket = AtomicUsize::new(0);
    let cell_refs: Vec<std::sync::Mutex<Option<BucketDedupResult>>> =
        (0..64).map(|_| std::sync::Mutex::new(None)).collect();

    std::thread::scope(|s| {
        let dedup_threads = num_threads.max(1).min(64);
        let mut handles = Vec::with_capacity(dedup_threads);
        for _ in 0..dedup_threads {
            handles.push(s.spawn(|| loop {
                let b = next_bucket.fetch_add(1, MemOrdering::Relaxed);
                if b >= 64 {
                    break;
                }
                let raw_vecs = &bucket_inputs[b];
                if raw_vecs.is_empty() {
                    *cell_refs[b].lock().unwrap() = Some(BucketDedupResult {
                        unique_keys: Vec::new(),
                        slots: Vec::new(),
                        mask: 0,
                    });
                    continue;
                }
                let raw_cnt: usize = raw_vecs.iter().map(|v| v.len()).sum();
                let mut cap = 64usize.max(((raw_cnt * 3) / 2).next_power_of_two());
                let mut slots = vec![(EMPTY_KEY, 0u32); cap];
                let mut mask = cap - 1;
                let mut unique_keys: Vec<u64> = Vec::with_capacity(raw_cnt / 2);

                for v in raw_vecs {
                    for &k in v {
                        let mut idx = ((mix64(k) >> 16) as usize) & mask;
                        let mut found = false;
                        loop {
                            let (sk, _) = slots[idx];
                            if sk == k {
                                found = true;
                                break;
                            }
                            if sk == EMPTY_KEY {
                                break;
                            }
                            idx = (idx + 1) & mask;
                        }
                        if !found {
                            let lid = unique_keys.len() as u32;
                            unique_keys.push(k);
                            slots[idx] = (k, lid);
                            if unique_keys.len() * 2 >= cap {
                                let new_cap = cap * 2;
                                let new_mask = new_cap - 1;
                                let mut new_slots = vec![(EMPTY_KEY, 0u32); new_cap];
                                for &(ok, olid) in &slots {
                                    if ok != EMPTY_KEY {
                                        let mut nidx = ((mix64(ok) >> 16) as usize) & new_mask;
                                        while new_slots[nidx].0 != EMPTY_KEY {
                                            nidx = (nidx + 1) & new_mask;
                                        }
                                        new_slots[nidx] = (ok, olid);
                                    }
                                }
                                slots = new_slots;
                                cap = new_cap;
                                mask = new_mask;
                            }
                        }
                    }
                }

                *cell_refs[b].lock().unwrap() = Some(BucketDedupResult {
                    unique_keys,
                    slots,
                    mask,
                });
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    });
    drop(bucket_inputs);

    let mut num_nodes = 0usize;
    let mut dicts: Vec<BucketDict> = Vec::with_capacity(64);
    let mut unique_per_bucket: Vec<Vec<u64>> = Vec::with_capacity(64);
    for m in cell_refs {
        let res = m.into_inner().unwrap().unwrap();
        let cnt = res.unique_keys.len();
        dicts.push(BucketDict {
            slots: res.slots,
            mask: res.mask,
            base_id: num_nodes as u32,
        });
        num_nodes += cnt;
        unique_per_bucket.push(res.unique_keys);
    }

    let mut all_keys: Vec<u64> = Vec::with_capacity(num_nodes);
    for uk in &unique_per_bucket {
        all_keys.extend_from_slice(uk);
    }
    drop(unique_per_bucket);

    let parent: Vec<AtomicU32> = (0..num_nodes as u32).map(AtomicU32::new).collect();
    std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(thread_edges.len());
        let dicts_ref = &dicts;
        let parent_ref = &parent;
        let keys_ref = &all_keys;
        for edges in &thread_edges {
            handles.push(s.spawn(move || {
                for &(ka, kb) in edges {
                    let ia = dicts_ref[(mix64(ka) as usize) & 63].lookup_global(ka);
                    let ib = dicts_ref[(mix64(kb) as usize) & 63].lookup_global(kb);
                    union_min_dense(parent_ref, keys_ref, overflow, ia, ib);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    });
    drop(thread_edges);
    drop(dicts);

    let mut roots: Vec<u32> = vec![0u32; num_nodes];
    let redirect_count = AtomicUsize::new(0);
    let flat_threads = num_threads.max(1).min((num_nodes / 1024).max(1));
    let chunk_n = (num_nodes + flat_threads - 1) / flat_threads;

    std::thread::scope(|s| {
        let mut offset = 0usize;
        let mut handles = Vec::new();
        for slice in roots.chunks_mut(chunk_n) {
            let start_i = offset;
            offset += slice.len();
            let p_ref = &parent;
            let rc_ref = &redirect_count;
            handles.push(s.spawn(move || {
                let mut local_redir = 0usize;
                for (j, slot) in slice.iter_mut().enumerate() {
                    let i = start_i + j;
                    let mut r = p_ref[i].load(MemOrdering::Relaxed) as usize;
                    loop {
                        let pr = p_ref[r].load(MemOrdering::Relaxed) as usize;
                        if pr == r {
                            break;
                        }
                        r = pr;
                    }
                    *slot = r as u32;
                    if r != i {
                        local_redir += 1;
                    }
                }
                rc_ref.fetch_add(local_redir, MemOrdering::Relaxed);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    });
    drop(parent);

    let num_redirects = redirect_count.into_inner();
    finalize_redirect_table(num_redirects, flat_threads, |buckets, abs, num_buckets, wmask| {
        std::thread::scope(|s| {
            let mut handles = Vec::new();
            let mut offset = 0usize;
            for chunk in roots.chunks(chunk_n) {
                let start_i = offset;
                offset += chunk.len();
                let ak = &all_keys;
                handles.push(s.spawn(move || {
                    let mut local_ov: Vec<(u64, u64, u64)> = Vec::new();
                    for (j, &r) in chunk.iter().enumerate() {
                        let i = start_i + j;
                        if (r as usize) != i {
                            insert_bucket6_canon(
                                buckets,
                                abs,
                                num_buckets,
                                wmask,
                                ak[i],
                                ak[r as usize],
                                &mut local_ov,
                            );
                        }
                    }
                    local_ov
                }));
            }
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn link_redirect_chains_kiosk_mixed_and_self_links() {
        let dir = std::env::temp_dir().join("shopping_links_test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let f1 = dir.join("part-00000.csv");
        let f2 = dir.join("part-00001.csv");
        let f3 = dir.join("part-00002.csv");

        {
            let mut w1 = File::create(&f1).unwrap();
            writeln!(w1, "user_id_a,user_id_b").unwrap();
            for i in (0..399).rev() {
                writeln!(w1, "c{i:03},c{:03}", i + 1).unwrap();
            }
            writeln!(w1, "solo,solo").unwrap();
            writeln!(w1, "u1,").unwrap();
            writeln!(w1, ",u2").unwrap();
            writeln!(w1, "u3,u4,u5").unwrap();
            writeln!(w1, "\"a1\",\"a2\"").unwrap();
            writeln!(w1, "a2,a1").unwrap();
        }
        {
            let mut w2 = File::create(&f2).unwrap();
            writeln!(w2, "user_id_a,user_id_b").unwrap();
            for i in 0..200 {
                writeln!(w2, "very_long_kiosk_member_{i:04},kiosk_shared").unwrap();
            }
            writeln!(w2, "kiosk_shared,0_hub").unwrap();
        }
        {
            let mut w3 = File::create(&f3).unwrap();
            writeln!(w3, "user_id_a,user_id_b").unwrap();
        }

        let overflow = OverflowInterner::new();
        let lr = build_link_redirect(&[f1.clone(), f2.clone(), f3.clone()], 4, &overflow);

        // Also validate cold fallback produces identical redirects
        let files: Vec<(File, usize)> = [f1, f2, f3]
            .iter()
            .map(|p| {
                let f = File::open(p).unwrap();
                let l = f.metadata().unwrap().len() as usize;
                (f, l)
            })
            .collect();
        let jobs: Vec<(usize, usize, usize)> = files
            .iter()
            .enumerate()
            .map(|(i, &(_, l))| (i, 0, l))
            .collect();
        let lr_fb = build_link_redirect_fallback(&files, &jobs, 4, &overflow);

        for checker in [&lr, &lr_fb] {
            let canon_c = overflow.encode_user(b"c000");
            for i in 0..400 {
                let k = overflow.encode_user(format!("c{i:03}").as_bytes());
                assert_eq!(checker.resolve(k), canon_c, "chain idx {i}");
            }
            let solo = overflow.encode_user(b"solo");
            assert_eq!(checker.resolve(solo), solo);
            let u1 = overflow.encode_user(b"u1");
            assert_eq!(checker.resolve(u1), u1);
            assert_eq!(
                checker.resolve(overflow.encode_user(b"a2")),
                overflow.encode_user(b"a1")
            );
            let hub = overflow.encode_user(b"0_hub");
            assert_eq!(checker.resolve(overflow.encode_user(b"kiosk_shared")), hub);
            for i in 0..200 {
                let mk = overflow.encode_user(format!("very_long_kiosk_member_{i:04}").as_bytes());
                assert_eq!(checker.resolve(mk), hub, "kiosk member {i}");
            }
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn link_redirect_dense_mixed_keys_and_slotted_parity() {
        let dir = std::env::temp_dir().join("shopping_links_dense_test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let f = dir.join("dense.csv");
        {
            let mut w = File::create(&f).unwrap();
            writeln!(w, "user_id_a,user_id_b").unwrap();
            for i in 0..1600usize {
                writeln!(w, "u{i},x{i}").unwrap();
            }
            writeln!(w, "u0,U0").unwrap();
            writeln!(w, "x0,u1").unwrap();
        }
        let overflow = OverflowInterner::new();
        let checker = build_link_redirect(&[f], 4, &overflow);

        let root = overflow.encode_user(b"U0");
        assert_eq!(checker.resolve(overflow.encode_user(b"u0")), root);
        assert_eq!(checker.resolve(overflow.encode_user(b"x0")), root);
        assert_eq!(checker.resolve(overflow.encode_user(b"u1")), root);
        assert_eq!(checker.resolve(overflow.encode_user(b"x1")), root);
        assert_eq!(
            checker.resolve(overflow.encode_user(b"x1599")),
            overflow.encode_user(b"u1599")
        );
        let unlinked = overflow.encode_user(b"u99999999");
        assert_eq!(checker.resolve(unlinked), unlinked);

        // Verify resolve_from_slot_hashed matches resolve across linked and unlinked keys
        for s in [
            b"u0".as_slice(),
            b"x0",
            b"x1599",
            b"u1599",
            b"u99999999",
            b"unlinked_very_long_key_12345",
        ] {
            let k = overflow.encode_user(s);
            let h = checker.prefetch_bloom_for(k);
            let slotted = match checker.bloom_hit_slot_with_hash(k, h) {
                Some(idx) => decode_canon_val(checker.resolve_from_slot_hashed(k, h, idx)),
                None => k,
            };
            assert_eq!(slotted, checker.resolve(k), "slot parity failed for {s:?}");
        }

        // Empty LinkRedirect safety
        let empty = LinkRedirect::empty();
        let k0 = overflow.encode_user(b"u1234567");
        assert!(empty.is_empty());
        assert_eq!(
            empty.bloom_hit_slot_with_hash(k0, empty.prefetch_bloom_for(k0)),
            None
        );
        assert_eq!(empty.resolve(k0), k0);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn link_redirect_bucket6_large_and_overflow_parity() {
        let dir = std::env::temp_dir().join("shopping_links_bucket6_test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let f = dir.join("large.csv");
        let mut keys_raw: Vec<String> = Vec::with_capacity(26_000);
        {
            let mut w = File::create(&f).unwrap();
            writeln!(w, "user_id_a,user_id_b").unwrap();
            // 25_000 redirects triggering Bucket6 (>= BUCKET6_MIN_REDIRECTS) plus uppercase/long overflow keys
            for i in 0..25_000usize {
                let a = format!("u{:07}", i * 2 + 1);
                let b = if i % 50 == 0 {
                    // Uppercase B37_LIMIT+ canon triggers canon >= B37_LIMIT overflow path inside Bucket6
                    format!("U{:07}", i * 2)
                } else {
                    format!("u{:07}", i * 2)
                };
                writeln!(w, "{a},{b}").unwrap();
                keys_raw.push(a);
                keys_raw.push(b);
            }
            // Chains and >9 byte keys mixed in
            for i in 0..200usize {
                let a = format!("long_member_{i:04}");
                let b = format!("u{:07}", i * 2);
                writeln!(w, "{a},{b}").unwrap();
                keys_raw.push(a);
            }
            // Extra uppercase chain onto one existing root
            writeln!(w, "u0000010,Upper000000").unwrap();
            keys_raw.push("Upper000000".to_string());
        }

        let overflow = OverflowInterner::new();
        let lr = build_link_redirect(&[f.clone()], 4, &overflow);
        let lr_st = build_link_redirect(&[f.clone()], 1, &overflow);
        assert!(lr.num_buckets >= BUCKET6_MIN_BUCKETS, "expected Bucket6 path active");
        assert_eq!(lr_st.num_buckets, lr.num_buckets, "ST Bucket6 geometry mismatch");

        let lf = File::open(&f).unwrap();
        let llen = lf.metadata().unwrap().len() as usize;
        let files: Vec<(File, usize)> = vec![(lf, llen)];
        let jobs = vec![(0, 0, llen)];
        let lr_fb = build_link_redirect_fallback(&files, &jobs, 4, &overflow);

        // Verify parity across all linked keys + unlinked probes between MT slotted, ST slotted, resolve(), and fallback
        for i in 0..500usize {
            keys_raw.push(format!("unlinked_{i:04}"));
            keys_raw.push(format!("u9{:07}", i));
        }
        for s in &keys_raw {
            let k = overflow.encode_user(s.as_bytes());
            let h = lr.prefetch_bloom_for(k);
            let slotted = match lr.bloom_hit_slot_with_hash(k, h) {
                Some(idx) => {
                    lr.prefetch_slot(idx);
                    decode_canon_val(lr.resolve_from_slot_hashed(k, h, idx))
                }
                None => k,
            };
            let slotted_st = match lr_st.bloom_hit_slot_with_hash(k, h) {
                Some(idx) => decode_canon_val(lr_st.resolve_from_slot_hashed(k, h, idx)),
                None => k,
            };
            let resolved = lr.resolve(k);
            let resolved_st = lr_st.resolve(k);
            let fb_resolved = lr_fb.resolve(k);
            assert_eq!(slotted, resolved, "slotted != resolve for {s}");
            assert_eq!(slotted_st, resolved_st, "st slotted != st resolve for {s}");
            assert_eq!(resolved_st, fb_resolved, "st bucket6 != fallback for {s}");
            assert_eq!(resolved, fb_resolved, "bucket6 != fallback for {s}");
        }

        let _ = std::fs::remove_dir_all(&dir);
    }
}
