use std::cmp::Ordering;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering as MemOrdering};
use std::sync::{Condvar, Mutex};
use std::time::Instant;

use ahash::RandomState;
use hashbrown::{HashMap, HashTable};

use crate::csv::{
    fadvise_sequential, for_each_data_line, parse_csv_line, parse_d2, parse_f3_13_ptr,
    parse_hms_swar, parse_ingest_fast_sec, parse_ingest_rel_sec, parse_price_cents, parse_ts,
    parse_ts_rel_ms, parse_ts_rel_ms_and_sec, pread_csv_chunk, resolve_iso_day_rel_sec,
    scan_csv_mmap_chunk_sink, CsvRowSink, FastDateCache, CLICK_REACH_MS, DAY_MS, LATE_LIMIT_MS,
    SESSION_GAP_MS,
};

const IO_CHUNK_BYTES: usize = 1024 * 1024;
use crate::huge::HugeSlab;
use crate::links::{build_link_redirect, LinkRedirect};
use crate::pack::{
    decode_canon_val, mix42, mix64, pack_b37, pack_b37_pair, pack_b37_pair_89, unmix42,
    OverflowInterner, B37_LIMIT, MASK42, OVERFLOW_BIT,
};
use crate::parquet_writer::{
    write_campaign_daily_parquet, write_empty_user_daily, UserDailyParquetWriter,
};

const PERSON_BITS: u32 = 9;
const PERSON_BUCKETS: usize = 1 << PERSON_BITS; // 512
const PERSON_MASK: usize = PERSON_BUCKETS - 1;
const EID_BITS: u32 = 10;
const EID_BUCKETS: usize = 1 << EID_BITS; // 1024
const EID_MASK: usize = EID_BUCKETS - 1;

const VIEW_U32: u32 = u32::from_ne_bytes(*b"view");
const CART_U32: u32 = u32::from_ne_bytes(*b"cart");
const PURCH_U64: u64 = u64::from_ne_bytes(*b"purchase");

#[inline(always)]
fn prefetch_ptr<T>(ptr: *const T) {
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

// =========================================================================
// Fast Direct Lookup Indexes with General Hash Fallbacks
// =========================================================================

#[inline(always)]
fn parse_p_num(b: &[u8]) -> Option<usize> {
    if b.len() < 2 || b.len() > 8 || b[0] != b'p' || b[1] == b'0' {
        return None;
    }
    let mut val = 0usize;
    for &c in &b[1..] {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        val = val * 10 + d as usize;
    }
    if val > 2_000_000 {
        None
    } else {
        Some(val)
    }
}

#[inline(always)]
fn parse_c_num(b: &[u8]) -> Option<usize> {
    if b.len() < 3 || b.len() > 8 || b[0] != b'c' || b[1] != b'_' || b[2] == b'0' {
        return None;
    }
    let mut val = 0usize;
    for &c in &b[2..] {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        val = val * 10 + d as usize;
    }
    if val > 100_000 {
        None
    } else {
        Some(val)
    }
}

struct ProductIndex {
    direct: Vec<u16>,
    has_fallback: bool,
    hasher: RandomState,
    table: HashTable<u32>,
    arena: Vec<u8>,
    spans: Vec<(u32, u32)>,
    prod_cat: Vec<u32>,
    pub categories: Vec<String>,
    pub unknown_cat: u32,
}

impl ProductIndex {
    fn load(path: &Path) -> Self {
        let data = fs::read(path).expect("read products.csv");
        let mut raw_prods: Vec<(&[u8], &[u8])> = Vec::with_capacity(65_536);
        let mut cats_set: Vec<&[u8]> = vec![b"unknown".as_slice()];
        let mut max_p = 0usize;

        for_each_data_line(&data, true, |line| {
            if let Some([pid, cat]) = parse_csv_line::<2>(line) {
                if !pid.is_empty() && !cat.is_empty() {
                    if let Some(n) = parse_p_num(pid) {
                        if n > max_p {
                            max_p = n;
                        }
                    }
                    raw_prods.push((pid, cat));
                    if !cats_set.iter().any(|&c| c == cat) {
                        cats_set.push(cat);
                    }
                }
            }
        });
        cats_set.sort_unstable();

        let unknown_cat = cats_set
            .binary_search(&b"unknown".as_slice())
            .expect("unknown present") as u32;
        let categories: Vec<String> = cats_set
            .iter()
            .map(|c| String::from_utf8_lossy(c).into_owned())
            .collect();

        let use_direct = categories.len() <= u16::MAX as usize && max_p > 0;
        let mut direct = if use_direct {
            vec![unknown_cat as u16; max_p + 1]
        } else {
            Vec::new()
        };

        let hasher = RandomState::with_seeds(11, 22, 33, 44);
        let mut table = HashTable::new();
        let mut arena = Vec::new();
        let mut spans: Vec<(u32, u32)> = Vec::new();
        let mut prod_cat: Vec<u32> = Vec::new();

        for (pid, cat) in raw_prods {
            let cid = if cats_set.len() <= 64 {
                cats_set.iter().position(|&c| c == cat).unwrap() as u32
            } else {
                cats_set.binary_search(&cat).unwrap() as u32
            };
            if use_direct {
                if let Some(n) = parse_p_num(pid) {
                    if n < direct.len() {
                        direct[n] = cid as u16;
                        continue;
                    }
                }
            }
            let idx = spans.len() as u32;
            let off = arena.len() as u32;
            arena.extend_from_slice(pid);
            spans.push((off, pid.len() as u32));
            prod_cat.push(cid);
            let h = hasher.hash_one(pid);
            table.insert_unique(h, idx, |&id| {
                let (o, l) = spans[id as usize];
                hasher.hash_one(&arena[o as usize..(o + l) as usize])
            });
        }

        let has_fallback = !spans.is_empty();
        Self {
            direct,
            has_fallback,
            hasher,
            table,
            arena,
            spans,
            prod_cat,
            categories,
            unknown_cat,
        }
    }

    #[inline(always)]
    fn lookup(&self, product_id: &[u8]) -> u32 {
        if let Some(n) = parse_p_num(product_id) {
            if n < self.direct.len() {
                return unsafe { *self.direct.get_unchecked(n) } as u32;
            }
        }
        if !self.has_fallback {
            return self.unknown_cat;
        }
        self.lookup_slow(product_id)
    }

    #[cold]
    fn lookup_slow(&self, product_id: &[u8]) -> u32 {
        let h = self.hasher.hash_one(product_id);
        self.table
            .find(h, |&id| {
                let (o, l) = self.spans[id as usize];
                &self.arena[o as usize..(o + l) as usize] == product_id
            })
            .map_or(self.unknown_cat, |&id| self.prod_cat[id as usize])
    }
}

struct CampaignIndex {
    direct: Vec<u32>,
    has_fallback: bool,
    hasher: RandomState,
    table: HashTable<u32>,
    pub ids: Vec<String>,
    pub channels: Vec<String>,
    pub organic_id: u32,
}

impl CampaignIndex {
    fn load(path: &Path) -> Self {
        let data = fs::read(path).expect("read campaigns.csv");
        let mut raw: Vec<(&[u8], &[u8])> = Vec::with_capacity(2048);
        let mut max_c = 0usize;

        for_each_data_line(&data, true, |line| {
            if let Some([cid, ch]) = parse_csv_line::<2>(line) {
                if !cid.is_empty() {
                    if let Some(n) = parse_c_num(cid) {
                        if n > max_c {
                            max_c = n;
                        }
                    }
                    raw.push((cid, ch));
                }
            }
        });

        let mut direct = if max_c > 0 {
            vec![u32::MAX; max_c + 1]
        } else {
            Vec::new()
        };

        let hasher = RandomState::with_seeds(55, 66, 77, 88);
        let mut table = HashTable::new();
        let mut ids = Vec::with_capacity(raw.len() + 1);
        let mut channels = Vec::with_capacity(raw.len() + 1);
        let mut has_fallback = false;

        for (i, (cid, ch)) in raw.into_iter().enumerate() {
            let camp_idx = i as u32;
            ids.push(String::from_utf8_lossy(cid).into_owned());
            channels.push(String::from_utf8_lossy(ch).into_owned());
            if let Some(n) = parse_c_num(cid) {
                if n < direct.len() {
                    direct[n] = camp_idx;
                    continue;
                }
            }
            has_fallback = true;
            let h = hasher.hash_one(cid);
            table.insert_unique(h, camp_idx, |&id| {
                hasher.hash_one(ids[id as usize].as_bytes())
            });
        }

        let organic_id = ids.len() as u32;
        ids.push("organic".to_string());
        channels.push("organic".to_string());

        Self {
            direct,
            has_fallback,
            hasher,
            table,
            ids,
            channels,
            organic_id,
        }
    }

    #[inline(always)]
    fn lookup(&self, camp_id: &[u8]) -> Option<u32> {
        if let Some(n) = parse_c_num(camp_id) {
            if n < self.direct.len() {
                let v = unsafe { *self.direct.get_unchecked(n) };
                return if v == u32::MAX { None } else { Some(v) };
            }
        }
        if !self.has_fallback {
            return None;
        }
        self.lookup_slow(camp_id)
    }

    #[cold]
    fn lookup_slow(&self, camp_id: &[u8]) -> Option<u32> {
        let h = self.hasher.hash_one(camp_id);
        self.table
            .find(h, |&id| self.ids[id as usize].as_bytes() == camp_id)
            .copied()
    }
}

fn parse_manifest(path: &Path) -> (i64, i64) {
    let text = fs::read_to_string(path).expect("read manifest.json");
    let extract = |key: &str| -> i64 {
        let pat = format!("\"{key}\"");
        let kpos = text.find(&pat).expect("manifest key");
        let after_colon = text[kpos + pat.len()..].find(':').expect("colon") + kpos + pat.len() + 1;
        let q1 = text[after_colon..].find('"').expect("q1") + after_colon + 1;
        let q2 = text[q1..].find('"').expect("q2") + q1;
        parse_ts(text[q1..q2].as_bytes()).expect("valid manifest ts")
    };
    (extract("window_start"), extract("window_end"))
}

fn list_csvs(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut paths: Vec<PathBuf> = entries
        .filter_map(|e| e.ok().map(|x| x.path()))
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("csv"))
        .collect();
    paths.sort();
    paths
}

// =========================================================================
// Zero-Realloc Contiguous Slab Buckets for Low-DTLB Routing
// =========================================================================

struct SlabBucketsOut<T> {
    slabs: Vec<HugeSlab<T>>,
    bucket_runs: Vec<Vec<(u16, u32, u16)>>,
}

impl<T> SlabBucketsOut<T> {
    #[inline(always)]
    unsafe fn get_slice<'a>(
        all_slabs: &'a [Vec<HugeSlab<T>>],
        tid: u8,
        sidx: u16,
        start: u32,
        len: u16,
    ) -> &'a [T] {
        let slab = all_slabs
            .get_unchecked(tid as usize)
            .get_unchecked(sidx as usize);
        let raw = slab.get_unchecked(start as usize, len as usize);
        std::slice::from_raw_parts(raw.as_ptr() as *const T, len as usize)
    }
}

struct DirectSlabBuckets<T: Copy, const BLOCK: usize> {
    num_buckets: usize,
    extra_slab_elems: usize,
    cursor: Vec<u32>,
    active_meta: Vec<(u16, u32)>,
    slabs: Vec<HugeSlab<T>>,
    base_ptr: *mut T,
    bucket_runs: Vec<Vec<(u16, u32, u16)>>,
}

unsafe impl<T: Copy + Send, const BLOCK: usize> Send for DirectSlabBuckets<T, BLOCK> {}

impl<T: Copy, const BLOCK: usize> DirectSlabBuckets<T, BLOCK> {
    const _CHECK: () = {
        assert!(BLOCK >= 16 && BLOCK <= u16::MAX as usize);
        assert!(BLOCK.is_power_of_two());
    };

    fn new(num_buckets: usize, initial_slab_elems: usize, extra_slab_elems: usize) -> Self {
        let _ = Self::_CHECK;
        let init_cap = (((initial_slab_elems.max(BLOCK * 2) + BLOCK - 1) / BLOCK) * BLOCK)
            .max(BLOCK * 2);
        let extra_cap =
            (((extra_slab_elems.max(BLOCK * 2) + BLOCK - 1) / BLOCK) * BLOCK).max(BLOCK * 2);
        let mut first_slab = HugeSlab::with_capacity(init_cap);
        unsafe { first_slab.set_len(BLOCK) };
        let base_ptr = first_slab.as_mut_ptr() as *mut T;
        Self {
            num_buckets,
            extra_slab_elems: extra_cap,
            cursor: vec![0u32; num_buckets],
            active_meta: vec![(u16::MAX, 0); num_buckets],
            slabs: vec![first_slab],
            base_ptr,
            bucket_runs: (0..num_buckets).map(|_| Vec::new()).collect(),
        }
    }

    #[cold]
    fn refill_block(&mut self, b: usize) -> u32 {
        let (prev_sidx, prev_start) = unsafe { *self.active_meta.get_unchecked(b) };
        if prev_sidx != u16::MAX {
            unsafe { self.bucket_runs.get_unchecked_mut(b) }.push((
                prev_sidx,
                prev_start,
                BLOCK as u16,
            ));
            unsafe { *self.active_meta.get_unchecked_mut(b) = (u16::MAX, 0) };
        }
        let cur_sidx = self.slabs.len() - 1;
        if self.slabs[cur_sidx].len() + BLOCK > self.slabs[cur_sidx].capacity() {
            for i in 0..self.num_buckets {
                let (asidx, astart) = self.active_meta[i];
                if asidx != u16::MAX {
                    let filled = (self.cursor[i] - astart) as u16;
                    if filled > 0 {
                        self.bucket_runs[i].push((asidx, astart, filled));
                    }
                    self.active_meta[i] = (u16::MAX, 0);
                }
                self.cursor[i] = 0;
            }
            let mut next_slab = HugeSlab::with_capacity(self.extra_slab_elems);
            unsafe { next_slab.set_len(BLOCK) };
            self.base_ptr = next_slab.as_mut_ptr() as *mut T;
            self.slabs.push(next_slab);
        }
        let sidx = self.slabs.len() - 1;
        let slab = &mut self.slabs[sidx];
        let start = slab.len();
        unsafe { slab.set_len(start + BLOCK) };
        self.active_meta[b] = (sidx as u16, start as u32);
        start as u32
    }

    #[inline(always)]
    fn push(&mut self, b: usize, item: T) {
        let mut idx = unsafe { *self.cursor.get_unchecked(b) };
        if (idx & ((BLOCK as u32) - 1)) == 0 {
            idx = self.refill_block(b);
        }
        unsafe {
            self.base_ptr.add(idx as usize).write(item);
            *self.cursor.get_unchecked_mut(b) = idx + 1;
        }
    }

    fn finish(mut self) -> SlabBucketsOut<T> {
        for b in 0..self.num_buckets {
            let (sidx, start) = self.active_meta[b];
            if sidx != u16::MAX {
                let filled = (self.cursor[b] - start) as u16;
                if filled > 0 {
                    self.bucket_runs[b].push((sidx, start, filled));
                }
            }
        }
        SlabBucketsOut {
            slabs: self.slabs,
            bucket_runs: self.bucket_runs,
        }
    }
}

#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct StageBuf<T: Copy, const K: usize>([T; K]);

/// L1-staged, 64-byte-aligned non-temporal (`_mm256_stream_si256`) slab bucket router
/// that eliminates RFO cache-line ownership traffic and DRAM thrashing across 128+ buckets.
struct NtSlabBuckets<T: Copy + Default, const K: usize, const BLOCK: usize> {
    num_buckets: usize,
    extra_slab_elems: usize,
    stage: Vec<StageBuf<T, K>>,
    cursor: Vec<u32>,
    active_meta: Vec<(u16, u32)>,
    slabs: Vec<HugeSlab<T>>,
    base_ptr: *mut T,
    bucket_runs: Vec<Vec<(u16, u32, u16)>>,
}

unsafe impl<T: Copy + Default + Send, const K: usize, const BLOCK: usize> Send
    for NtSlabBuckets<T, K, BLOCK>
{
}

impl<T: Copy + Default, const K: usize, const BLOCK: usize> NtSlabBuckets<T, K, BLOCK> {
    const _CHECK: () = {
        assert!(BLOCK >= K && BLOCK <= u16::MAX as usize);
        assert!(BLOCK.is_power_of_two());
        assert!(BLOCK % K == 0);
        assert!(K <= u8::MAX as usize);
        assert!((K * std::mem::size_of::<T>()) % 64 == 0);
        assert!((BLOCK * std::mem::size_of::<T>()) % 64 == 0);
    };

    fn new(num_buckets: usize, initial_slab_elems: usize, extra_slab_elems: usize) -> Self {
        let _ = Self::_CHECK;
        let init_cap = (((initial_slab_elems.max(BLOCK * 2) + BLOCK - 1) / BLOCK) * BLOCK)
            .max(BLOCK * 2);
        let extra_cap =
            (((extra_slab_elems.max(BLOCK * 2) + BLOCK - 1) / BLOCK) * BLOCK).max(BLOCK * 2);
        let mut first_slab = HugeSlab::with_capacity(init_cap);
        unsafe { first_slab.set_len(BLOCK) };
        let base_ptr = first_slab.as_mut_ptr() as *mut T;
        Self {
            num_buckets,
            extra_slab_elems: extra_cap,
            stage: vec![StageBuf([T::default(); K]); num_buckets],
            cursor: vec![0u32; num_buckets],
            active_meta: vec![(u16::MAX, 0); num_buckets],
            slabs: vec![first_slab],
            base_ptr,
            bucket_runs: (0..num_buckets).map(|_| Vec::new()).collect(),
        }
    }

    #[cold]
    fn refill_block(&mut self, b: usize) -> u32 {
        let (prev_sidx, prev_start) = unsafe { *self.active_meta.get_unchecked(b) };
        if prev_sidx != u16::MAX {
            unsafe { self.bucket_runs.get_unchecked_mut(b) }.push((
                prev_sidx,
                prev_start,
                BLOCK as u16,
            ));
            unsafe { *self.active_meta.get_unchecked_mut(b) = (u16::MAX, 0) };
        }
        let cur_sidx = self.slabs.len() - 1;
        if self.slabs[cur_sidx].len() + BLOCK > self.slabs[cur_sidx].capacity() {
            for i in 0..self.num_buckets {
                let rem = self.cursor[i] & ((K as u32) - 1);
                let (asidx, astart) = self.active_meta[i];
                if asidx != u16::MAX {
                    let filled = ((self.cursor[i] - rem) - astart) as u16;
                    if filled > 0 {
                        self.bucket_runs[i].push((asidx, astart, filled));
                    }
                    self.active_meta[i] = (u16::MAX, 0);
                }
                self.cursor[i] = rem;
            }
            let mut next_slab = HugeSlab::with_capacity(self.extra_slab_elems);
            unsafe { next_slab.set_len(BLOCK) };
            self.base_ptr = next_slab.as_mut_ptr() as *mut T;
            self.slabs.push(next_slab);
        }
        let sidx = self.slabs.len() - 1;
        let slab = &mut self.slabs[sidx];
        let start = slab.len();
        unsafe { slab.set_len(start + BLOCK) };
        self.active_meta[b] = (sidx as u16, start as u32);
        start as u32
    }

    #[inline(always)]
    fn push(&mut self, b: usize, item: T) {
        let mut cur = unsafe { *self.cursor.get_unchecked(b) };
        let l = (cur as usize) & (K - 1);
        unsafe {
            *self
                .stage
                .get_unchecked_mut(b)
                .0
                .get_unchecked_mut(l) = item;
        }
        cur += 1;
        if ((cur as usize) & (K - 1)) != 0 {
            unsafe { *self.cursor.get_unchecked_mut(b) = cur };
            return;
        }
        let mut base_idx = cur - (K as u32);
        if (base_idx & ((BLOCK as u32) - 1)) == 0 {
            base_idx = self.refill_block(b);
        }
        unsafe {
            let src = self.stage.get_unchecked(b).0.as_ptr() as *const u8;
            let dst = self.base_ptr.add(base_idx as usize) as *mut u8;
            let bytes = K * std::mem::size_of::<T>();
            #[cfg(all(target_arch = "x86_64", target_feature = "avx"))]
            {
                use std::arch::x86_64::*;
                let mut off = 0usize;
                while off < bytes {
                    let v = _mm256_load_si256(src.add(off) as *const __m256i);
                    _mm256_stream_si256(dst.add(off) as *mut __m256i, v);
                    off += 32;
                }
            }
            #[cfg(not(all(target_arch = "x86_64", target_feature = "avx")))]
            {
                std::ptr::copy_nonoverlapping(src, dst, bytes);
            }
            *self.cursor.get_unchecked_mut(b) = base_idx + (K as u32);
        }
    }

    fn finish(mut self) -> SlabBucketsOut<T> {
        for b in 0..self.num_buckets {
            let cur = self.cursor[b];
            let rem = (cur as usize) & (K - 1);
            if rem > 0 {
                let mut base_idx = cur - (rem as u32);
                if (base_idx & ((BLOCK as u32) - 1)) == 0 {
                    base_idx = self.refill_block(b);
                }
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        self.stage[b].0.as_ptr(),
                        self.base_ptr.add(base_idx as usize),
                        rem,
                    );
                }
                self.cursor[b] = base_idx + rem as u32;
            }
            let (sidx, start) = self.active_meta[b];
            if sidx != u16::MAX {
                let filled = (self.cursor[b] - start) as u16;
                if filled > 0 {
                    self.bucket_runs[b].push((sidx, start, filled));
                }
                self.active_meta[b] = (u16::MAX, 0);
                self.cursor[b] = 0;
            }
        }
        #[cfg(all(target_arch = "x86_64", target_feature = "avx"))]
        unsafe {
            std::arch::x86_64::_mm_sfence();
        }
        SlabBucketsOut {
            slabs: self.slabs,
            bucket_runs: self.bucket_runs,
        }
    }
}

// =========================================================================
// Compact Event & Click Representations + Inline Ingest Packing
// =========================================================================

const ING_DELTA_BIAS: i64 = 1 << 18;
const ING_DELTA_MAX: i64 = (1 << 19) - 1;
const TOMB_ING_BIAS: i64 = 1 << 30;
const TOMB_ING_MAX: i64 = (1 << 31) - 1;
const RAW_SPECIAL_BIT: u64 = 1u64 << 31;
const RAW_EXTRA_EID_BIT: u64 = 1u64 << 44;

/// 16-byte deduplication & routing record for Stage 2 -> Stage 3 (`50%` less slab traffic).
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct RawEvent16 {
    /// - bit 31 (`RAW_SPECIAL_BIT`): `0` = inline valid `view`/`cart` (`bits 0..30` = `ts_rel_ms`);
    ///   `1` = tombstone or `ExtraEvent` (`bits 0..30` = `tomb_ing_biased`, or `0` to read `ExtraEvent.ingest_ts`)
    /// - bits 32..63: `(h >> 10) as u32` (`42 - 10 = 32` remaining hash bits when `EID_BUCKETS == 1024`)
    lo: u64,
    /// - bits 45..63: `ing_delta_b = (ing_rel_sec - ts_rel_sec) + ING_DELTA_BIAS` in `1..=ING_DELTA_MAX`
    /// - bit 44 (`RAW_EXTRA_EID_BIT`): `0` = Base-37 `eid`, `1` = non-Base-37 hash tag
    /// - bits 0..41: `uid_42` (when `special == 0`), `0` (tombstone when `special == 1`), or `eref + 1` (when `special == 1`)
    hi: u64,
}

const _: () = assert!(std::mem::size_of::<RawEvent16>() == 16);

#[derive(Clone, Copy)]
struct ExtraEvent {
    eid_k: u64,
    uid_k: u64,
    ts: i64,
    ingest_ts: i64,
    price_cents: u32,
    cat_id: u32,
    valid: bool,
}

#[derive(Clone, Copy, Default)]
struct ExtraSurv {
    person: u64,
    ts_rel: u32,
    price_cat: u32,
}

#[inline(always)]
fn person_bucket(person: u64) -> usize {
    if person < B37_LIMIT {
        (mix42(person) as usize) & PERSON_MASK
    } else {
        (mix64(person) as usize) & PERSON_MASK
    }
}

#[inline(always)]
fn inline_tomb_ingest_biased(ingest_ts: i64, win_start: i64) -> u64 {
    let Some(diff) = ingest_ts.checked_sub(win_start) else {
        return 0;
    };
    if diff % 1_000 != 0 {
        return 0;
    }
    let biased = diff / 1_000 + TOMB_ING_BIAS;
    if biased >= 1 && biased <= TOMB_ING_MAX {
        biased as u64
    } else {
        0
    }
}

#[inline(always)]
fn inline_ing_rel_sec(r: &RawEvent16) -> Option<i64> {
    if (r.lo & RAW_SPECIAL_BIT) == 0 {
        let ts_sec = ((r.lo & 0x7FFF_FFFF) as u32 / 1_000) as i64;
        let delta = (r.hi >> 45) as i64 - ING_DELTA_BIAS;
        Some(ts_sec + delta)
    } else {
        let tb = (r.lo & 0x7FFF_FFFF) as i64;
        if tb != 0 {
            Some(tb - TOMB_ING_BIAS)
        } else {
            None
        }
    }
}

#[inline(always)]
fn cmp_ingest16(
    a: &RawEvent16,
    b: &RawEvent16,
    extras: &[Vec<ExtraEvent>],
    win_start: i64,
    idx_bits: u32,
    idx_mask: u64,
) -> Ordering {
    let sa = inline_ing_rel_sec(a);
    let sb = inline_ing_rel_sec(b);
    if let (Some(va), Some(vb)) = (sa, sb) {
        return va.cmp(&vb);
    }
    let get_exact = |r: &RawEvent16, s: Option<i64>| -> i64 {
        if let Some(sec) = s {
            win_start + sec * 1_000
        } else {
            let eref = (r.hi & MASK42) - 1;
            let tid = (eref >> idx_bits) as usize;
            let eidx = (eref & idx_mask) as usize;
            extras[tid][eidx].ingest_ts
        }
    };
    get_exact(a, sa).cmp(&get_exact(b, sb))
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct SurvClick16 {
    lo: u64,
    hi: u64,
}

const _: () = assert!(std::mem::size_of::<SurvClick16>() == 16);

#[derive(Clone, Copy, Default)]
struct ExtraClick {
    person: u64,
    cid: u64,
    ts_rel: u32,
    camp_idx: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct CompactClick {
    cid: u64,
    ts_rel: u32,
    camp_idx: u32,
}

#[derive(Clone, Copy, Default)]
struct SurvEventWide {
    person: u64,
    ts: i64,
    price_cents: u32,
    cat_id: u32,
}

#[derive(Clone, Copy, Default)]
struct SurvClickWide {
    person: u64,
    cid: u64,
    ts: i64,
    camp_idx: u32,
}

#[derive(Clone, Copy, Default)]
struct CampAgg {
    sessions: i32,
    purchasing_sessions: i32,
    revenue_cents: i64,
    assisted_sessions: i32,
}

enum CampStore {
    Flat {
        sess_ass: Vec<(i32, i32)>,
        purch_rev: Vec<(i32, i64)>,
        organic_sess: [i32; 128],
        organic_id: u32,
        min_day: i32,
        day_span: usize,
    },
    Map(HashMap<(u32, i32), CampAgg, RandomState>),
}

impl CampStore {
    fn new(
        use_flat: bool,
        num_camps: usize,
        organic_id: u32,
        min_day: i32,
        day_span: usize,
    ) -> Self {
        if use_flat {
            let n = num_camps * day_span;
            CampStore::Flat {
                sess_ass: vec![(0i32, 0i32); n],
                purch_rev: vec![(0i32, 0i64); n],
                organic_sess: [0i32; 128],
                organic_id,
                min_day,
                day_span,
            }
        } else {
            CampStore::Map(HashMap::with_hasher(RandomState::with_seeds(3, 5, 7, 9)))
        }
    }

    #[inline(always)]
    fn add_credited(&mut self, camp_idx: u32, day: i32, sess_rev: i64) {
        match self {
            CampStore::Flat {
                sess_ass,
                purch_rev,
                organic_sess,
                organic_id,
                min_day,
                day_span,
            } => {
                let d = (day - *min_day) as usize;
                if sess_rev == 0 && camp_idx == *organic_id {
                    unsafe {
                        *organic_sess.get_unchecked_mut(d) += 1;
                    }
                    return;
                }
                let slot = (camp_idx as usize) * (*day_span) + d;
                unsafe {
                    sess_ass.get_unchecked_mut(slot).0 += 1;
                    if sess_rev > 0 {
                        let pr = purch_rev.get_unchecked_mut(slot);
                        pr.0 += 1;
                        pr.1 += sess_rev;
                    }
                }
            }
            CampStore::Map(m) => {
                let agg = m.entry((camp_idx, day)).or_default();
                agg.sessions += 1;
                if sess_rev > 0 {
                    agg.purchasing_sessions += 1;
                    agg.revenue_cents += sess_rev;
                }
            }
        }
    }

    #[inline(always)]
    fn add_credited_zero_rev(&mut self, camp_idx: u32, day: i32) {
        match self {
            CampStore::Flat {
                sess_ass,
                organic_sess,
                organic_id,
                min_day,
                day_span,
                ..
            } => {
                let d = (day - *min_day) as usize;
                if camp_idx == *organic_id {
                    unsafe {
                        *organic_sess.get_unchecked_mut(d) += 1;
                    }
                    return;
                }
                let slot = (camp_idx as usize) * (*day_span) + d;
                unsafe {
                    sess_ass.get_unchecked_mut(slot).0 += 1;
                }
            }
            CampStore::Map(m) => {
                m.entry((camp_idx, day)).or_default().sessions += 1;
            }
        }
    }

    #[inline(always)]
    fn add_assisted(&mut self, camp_idx: u32, day: i32, delta: i32) {
        match self {
            CampStore::Flat {
                sess_ass,
                min_day,
                day_span,
                ..
            } => {
                let d = (day - *min_day) as usize;
                let slot = (camp_idx as usize) * (*day_span) + d;
                unsafe {
                    sess_ass.get_unchecked_mut(slot).1 += delta;
                }
            }
            CampStore::Map(m) => {
                m.entry((camp_idx, day)).or_default().assisted_sessions += delta;
            }
        }
    }
}

// =========================================================================
// Stage 4a: Ultra-Fast Compact Path (Hash-Grouping + 8B Events + 8-Bit LSD Radix + Zero-Copy Parquet)
// =========================================================================

#[inline(always)]
unsafe fn cswap32(p: *mut u32, i: usize, j: usize) {
    let a = *p.add(i);
    let b = *p.add(j);
    let m = a < b;
    *p.add(i) = if m { a } else { b };
    *p.add(j) = if m { b } else { a };
}

#[inline(always)]
unsafe fn cswap64(p: *mut u64, i: usize, j: usize) {
    let a = *p.add(i);
    let b = *p.add(j);
    let m = a < b;
    *p.add(i) = if m { a } else { b };
    *p.add(j) = if m { b } else { a };
}

macro_rules! gen_sort_net {
    ($fn_name:ident, $cswap:ident, $T:ty) => {
        #[inline(always)]
        unsafe fn $fn_name(p: *mut $T, n: usize) {
            match n {
                2 => {
                    $cswap(p, 0, 1);
                }
                3 => {
                    $cswap(p, 0, 1); $cswap(p, 0, 2); $cswap(p, 1, 2);
                }
                4 => {
                    $cswap(p, 0, 1); $cswap(p, 2, 3); $cswap(p, 0, 2); $cswap(p, 1, 3); $cswap(p, 1, 2);
                }
                5 => {
                    $cswap(p, 0, 1); $cswap(p, 3, 4); $cswap(p, 2, 4); $cswap(p, 2, 3);
                    $cswap(p, 0, 3); $cswap(p, 0, 2); $cswap(p, 1, 4); $cswap(p, 1, 3); $cswap(p, 1, 2);
                }
                6 => {
                    $cswap(p, 1, 2); $cswap(p, 4, 5); $cswap(p, 0, 2); $cswap(p, 3, 5);
                    $cswap(p, 0, 1); $cswap(p, 3, 4); $cswap(p, 2, 5); $cswap(p, 0, 3);
                    $cswap(p, 1, 4); $cswap(p, 2, 4); $cswap(p, 1, 3); $cswap(p, 2, 3);
                }
                7 => {
                    $cswap(p, 1, 2); $cswap(p, 3, 4); $cswap(p, 5, 6); $cswap(p, 0, 2);
                    $cswap(p, 3, 5); $cswap(p, 4, 6); $cswap(p, 0, 1); $cswap(p, 4, 5);
                    $cswap(p, 2, 6); $cswap(p, 0, 4); $cswap(p, 1, 5); $cswap(p, 0, 3);
                    $cswap(p, 2, 5); $cswap(p, 1, 3); $cswap(p, 2, 4); $cswap(p, 2, 3);
                }
                8 => {
                    $cswap(p, 0, 1); $cswap(p, 2, 3); $cswap(p, 4, 5); $cswap(p, 6, 7);
                    $cswap(p, 0, 2); $cswap(p, 1, 3); $cswap(p, 4, 6); $cswap(p, 5, 7);
                    $cswap(p, 1, 2); $cswap(p, 5, 6); $cswap(p, 0, 4); $cswap(p, 1, 5);
                    $cswap(p, 2, 6); $cswap(p, 3, 7); $cswap(p, 2, 4); $cswap(p, 3, 5);
                    $cswap(p, 1, 2); $cswap(p, 3, 4); $cswap(p, 5, 6);
                }
                9 => {
                    $cswap(p, 0, 1); $cswap(p, 2, 3); $cswap(p, 0, 2); $cswap(p, 1, 3); $cswap(p, 1, 2);
                    $cswap(p, 4, 5); $cswap(p, 6, 7); $cswap(p, 4, 6); $cswap(p, 5, 7); $cswap(p, 5, 6);
                    $cswap(p, 0, 4); $cswap(p, 2, 6); $cswap(p, 2, 4); $cswap(p, 1, 5); $cswap(p, 3, 7);
                    $cswap(p, 3, 5); $cswap(p, 1, 2); $cswap(p, 3, 4); $cswap(p, 5, 6);
                    $cswap(p, 0, 8); $cswap(p, 4, 8); $cswap(p, 2, 4); $cswap(p, 6, 8);
                    $cswap(p, 1, 2); $cswap(p, 3, 4); $cswap(p, 5, 6); $cswap(p, 7, 8);
                }
                10 => {
                    $cswap(p, 0, 1); $cswap(p, 2, 3); $cswap(p, 0, 2); $cswap(p, 1, 3); $cswap(p, 1, 2);
                    $cswap(p, 4, 5); $cswap(p, 6, 7); $cswap(p, 4, 6); $cswap(p, 5, 7); $cswap(p, 5, 6);
                    $cswap(p, 0, 4); $cswap(p, 2, 6); $cswap(p, 2, 4); $cswap(p, 1, 5); $cswap(p, 3, 7);
                    $cswap(p, 3, 5); $cswap(p, 1, 2); $cswap(p, 3, 4); $cswap(p, 5, 6);
                    $cswap(p, 8, 9); $cswap(p, 0, 8); $cswap(p, 4, 8); $cswap(p, 2, 4); $cswap(p, 6, 8);
                    $cswap(p, 1, 9); $cswap(p, 5, 9); $cswap(p, 3, 5); $cswap(p, 7, 9);
                    $cswap(p, 1, 2); $cswap(p, 3, 4); $cswap(p, 5, 6); $cswap(p, 7, 8);
                }
                11 => {
                    $cswap(p, 0, 1); $cswap(p, 2, 3); $cswap(p, 0, 2); $cswap(p, 1, 3); $cswap(p, 1, 2);
                    $cswap(p, 4, 5); $cswap(p, 6, 7); $cswap(p, 4, 6); $cswap(p, 5, 7); $cswap(p, 5, 6);
                    $cswap(p, 0, 4); $cswap(p, 2, 6); $cswap(p, 2, 4); $cswap(p, 1, 5); $cswap(p, 3, 7);
                    $cswap(p, 3, 5); $cswap(p, 1, 2); $cswap(p, 3, 4); $cswap(p, 5, 6);
                    $cswap(p, 8, 9); $cswap(p, 8, 10); $cswap(p, 9, 10);
                    $cswap(p, 0, 8); $cswap(p, 4, 8); $cswap(p, 2, 10); $cswap(p, 6, 10); $cswap(p, 2, 4);
                    $cswap(p, 6, 8); $cswap(p, 1, 9); $cswap(p, 5, 9); $cswap(p, 3, 5); $cswap(p, 7, 9);
                    $cswap(p, 1, 2); $cswap(p, 3, 4); $cswap(p, 5, 6); $cswap(p, 7, 8); $cswap(p, 9, 10);
                }
                12 => {
                    $cswap(p, 0, 1); $cswap(p, 2, 3); $cswap(p, 0, 2); $cswap(p, 1, 3); $cswap(p, 1, 2);
                    $cswap(p, 4, 5); $cswap(p, 6, 7); $cswap(p, 4, 6); $cswap(p, 5, 7); $cswap(p, 5, 6);
                    $cswap(p, 0, 4); $cswap(p, 2, 6); $cswap(p, 2, 4); $cswap(p, 1, 5); $cswap(p, 3, 7);
                    $cswap(p, 3, 5); $cswap(p, 1, 2); $cswap(p, 3, 4); $cswap(p, 5, 6);
                    $cswap(p, 8, 9); $cswap(p, 10, 11); $cswap(p, 8, 10); $cswap(p, 9, 11); $cswap(p, 9, 10);
                    $cswap(p, 0, 8); $cswap(p, 4, 8); $cswap(p, 2, 10); $cswap(p, 6, 10); $cswap(p, 2, 4);
                    $cswap(p, 6, 8); $cswap(p, 1, 9); $cswap(p, 5, 9); $cswap(p, 3, 11); $cswap(p, 7, 11);
                    $cswap(p, 3, 5); $cswap(p, 7, 9); $cswap(p, 1, 2); $cswap(p, 3, 4); $cswap(p, 5, 6);
                    $cswap(p, 7, 8); $cswap(p, 9, 10);
                }
                13 => {
                    $cswap(p, 0, 1); $cswap(p, 2, 3); $cswap(p, 0, 2); $cswap(p, 1, 3); $cswap(p, 1, 2);
                    $cswap(p, 4, 5); $cswap(p, 6, 7); $cswap(p, 4, 6); $cswap(p, 5, 7); $cswap(p, 5, 6);
                    $cswap(p, 0, 4); $cswap(p, 2, 6); $cswap(p, 2, 4); $cswap(p, 1, 5); $cswap(p, 3, 7);
                    $cswap(p, 3, 5); $cswap(p, 1, 2); $cswap(p, 3, 4); $cswap(p, 5, 6);
                    $cswap(p, 8, 9); $cswap(p, 10, 11); $cswap(p, 8, 10); $cswap(p, 9, 11); $cswap(p, 9, 10);
                    $cswap(p, 8, 12); $cswap(p, 10, 12); $cswap(p, 9, 10); $cswap(p, 11, 12);
                    $cswap(p, 0, 8); $cswap(p, 4, 12); $cswap(p, 4, 8); $cswap(p, 2, 10); $cswap(p, 6, 10);
                    $cswap(p, 2, 4); $cswap(p, 6, 8); $cswap(p, 10, 12); $cswap(p, 1, 9); $cswap(p, 5, 9);
                    $cswap(p, 3, 11); $cswap(p, 7, 11); $cswap(p, 3, 5); $cswap(p, 7, 9);
                    $cswap(p, 1, 2); $cswap(p, 3, 4); $cswap(p, 5, 6); $cswap(p, 7, 8); $cswap(p, 9, 10); $cswap(p, 11, 12);
                }
                14 => {
                    $cswap(p, 0, 1); $cswap(p, 2, 3); $cswap(p, 0, 2); $cswap(p, 1, 3); $cswap(p, 1, 2);
                    $cswap(p, 4, 5); $cswap(p, 6, 7); $cswap(p, 4, 6); $cswap(p, 5, 7); $cswap(p, 5, 6);
                    $cswap(p, 0, 4); $cswap(p, 2, 6); $cswap(p, 2, 4); $cswap(p, 1, 5); $cswap(p, 3, 7);
                    $cswap(p, 3, 5); $cswap(p, 1, 2); $cswap(p, 3, 4); $cswap(p, 5, 6);
                    $cswap(p, 8, 9); $cswap(p, 10, 11); $cswap(p, 8, 10); $cswap(p, 9, 11); $cswap(p, 9, 10);
                    $cswap(p, 12, 13); $cswap(p, 8, 12); $cswap(p, 10, 12); $cswap(p, 9, 13); $cswap(p, 11, 13);
                    $cswap(p, 9, 10); $cswap(p, 11, 12);
                    $cswap(p, 0, 8); $cswap(p, 4, 12); $cswap(p, 4, 8); $cswap(p, 2, 10); $cswap(p, 6, 10);
                    $cswap(p, 2, 4); $cswap(p, 6, 8); $cswap(p, 10, 12); $cswap(p, 1, 9); $cswap(p, 5, 13);
                    $cswap(p, 5, 9); $cswap(p, 3, 11); $cswap(p, 7, 11); $cswap(p, 3, 5); $cswap(p, 7, 9);
                    $cswap(p, 11, 13); $cswap(p, 1, 2); $cswap(p, 3, 4); $cswap(p, 5, 6); $cswap(p, 7, 8);
                    $cswap(p, 9, 10); $cswap(p, 11, 12);
                }
                15 => {
                    $cswap(p, 0, 1); $cswap(p, 2, 3); $cswap(p, 0, 2); $cswap(p, 1, 3); $cswap(p, 1, 2);
                    $cswap(p, 4, 5); $cswap(p, 6, 7); $cswap(p, 4, 6); $cswap(p, 5, 7); $cswap(p, 5, 6);
                    $cswap(p, 0, 4); $cswap(p, 2, 6); $cswap(p, 2, 4); $cswap(p, 1, 5); $cswap(p, 3, 7);
                    $cswap(p, 3, 5); $cswap(p, 1, 2); $cswap(p, 3, 4); $cswap(p, 5, 6);
                    $cswap(p, 8, 9); $cswap(p, 10, 11); $cswap(p, 8, 10); $cswap(p, 9, 11); $cswap(p, 9, 10);
                    $cswap(p, 12, 13); $cswap(p, 12, 14); $cswap(p, 13, 14);
                    $cswap(p, 8, 12); $cswap(p, 10, 14); $cswap(p, 10, 12); $cswap(p, 9, 13); $cswap(p, 11, 13);
                    $cswap(p, 9, 10); $cswap(p, 11, 12); $cswap(p, 13, 14);
                    $cswap(p, 0, 8); $cswap(p, 4, 12); $cswap(p, 4, 8); $cswap(p, 2, 10); $cswap(p, 6, 14);
                    $cswap(p, 6, 10); $cswap(p, 2, 4); $cswap(p, 6, 8); $cswap(p, 10, 12);
                    $cswap(p, 1, 9); $cswap(p, 5, 13); $cswap(p, 5, 9); $cswap(p, 3, 11); $cswap(p, 7, 11);
                    $cswap(p, 3, 5); $cswap(p, 7, 9); $cswap(p, 11, 13);
                    $cswap(p, 1, 2); $cswap(p, 3, 4); $cswap(p, 5, 6); $cswap(p, 7, 8);
                    $cswap(p, 9, 10); $cswap(p, 11, 12); $cswap(p, 13, 14);
                }
                16 => {
                    $cswap(p, 0, 1); $cswap(p, 2, 3); $cswap(p, 0, 2); $cswap(p, 1, 3); $cswap(p, 1, 2);
                    $cswap(p, 4, 5); $cswap(p, 6, 7); $cswap(p, 4, 6); $cswap(p, 5, 7); $cswap(p, 5, 6);
                    $cswap(p, 0, 4); $cswap(p, 2, 6); $cswap(p, 2, 4); $cswap(p, 1, 5); $cswap(p, 3, 7);
                    $cswap(p, 3, 5); $cswap(p, 1, 2); $cswap(p, 3, 4); $cswap(p, 5, 6);
                    $cswap(p, 8, 9); $cswap(p, 10, 11); $cswap(p, 8, 10); $cswap(p, 9, 11); $cswap(p, 9, 10);
                    $cswap(p, 12, 13); $cswap(p, 14, 15); $cswap(p, 12, 14); $cswap(p, 13, 15); $cswap(p, 13, 14);
                    $cswap(p, 8, 12); $cswap(p, 10, 14); $cswap(p, 10, 12); $cswap(p, 9, 13); $cswap(p, 11, 15);
                    $cswap(p, 11, 13); $cswap(p, 9, 10); $cswap(p, 11, 12); $cswap(p, 13, 14);
                    $cswap(p, 0, 8); $cswap(p, 4, 12); $cswap(p, 4, 8); $cswap(p, 2, 10); $cswap(p, 6, 14);
                    $cswap(p, 6, 10); $cswap(p, 2, 4); $cswap(p, 6, 8); $cswap(p, 10, 12);
                    $cswap(p, 1, 9); $cswap(p, 5, 13); $cswap(p, 5, 9); $cswap(p, 3, 11); $cswap(p, 7, 15);
                    $cswap(p, 7, 11); $cswap(p, 3, 5); $cswap(p, 7, 9); $cswap(p, 11, 13);
                    $cswap(p, 1, 2); $cswap(p, 3, 4); $cswap(p, 5, 6); $cswap(p, 7, 8);
                    $cswap(p, 9, 10); $cswap(p, 11, 12); $cswap(p, 13, 14);
                }
                _ => {}
            }
        }
    };
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
unsafe fn bitonic_merge8_asc_avx2(v: std::arch::x86_64::__m256i) -> std::arch::x86_64::__m256i {
    use std::arch::x86_64::*;
    let p4 = _mm256_permute2x128_si256::<0x01>(v, v);
    let m4 = _mm256_blend_epi32::<0xF0>(_mm256_min_epu32(v, p4), _mm256_max_epu32(v, p4));
    let p2 = _mm256_shuffle_epi32::<0x4E>(m4);
    let m2 = _mm256_blend_epi32::<0xCC>(_mm256_min_epu32(m4, p2), _mm256_max_epu32(m4, p2));
    let p1 = _mm256_shuffle_epi32::<0xB1>(m2);
    _mm256_blend_epi32::<0xAA>(_mm256_min_epu32(m2, p1), _mm256_max_epu32(m2, p1))
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
unsafe fn sort8_avx2(v: std::arch::x86_64::__m256i) -> std::arch::x86_64::__m256i {
    use std::arch::x86_64::*;
    let p1 = _mm256_shuffle_epi32::<0xB1>(v);
    let s1 = _mm256_blend_epi32::<0x66>(_mm256_min_epu32(v, p1), _mm256_max_epu32(v, p1));
    let p2a = _mm256_shuffle_epi32::<0x4E>(s1);
    let s2a = _mm256_blend_epi32::<0x3C>(_mm256_min_epu32(s1, p2a), _mm256_max_epu32(s1, p2a));
    let p2b = _mm256_shuffle_epi32::<0xB1>(s2a);
    let s2b = _mm256_blend_epi32::<0x5A>(_mm256_min_epu32(s2a, p2b), _mm256_max_epu32(s2a, p2b));
    bitonic_merge8_asc_avx2(s2b)
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
unsafe fn sort16_avx2(
    a: std::arch::x86_64::__m256i,
    b: std::arch::x86_64::__m256i,
) -> (std::arch::x86_64::__m256i, std::arch::x86_64::__m256i) {
    use std::arch::x86_64::*;
    let sa = sort8_avx2(a);
    let sb = sort8_avx2(b);
    let sb_rev = _mm256_permutevar8x32_epi32(sb, _mm256_setr_epi32(7, 6, 5, 4, 3, 2, 1, 0));
    let lo = _mm256_min_epu32(sa, sb_rev);
    let hi = _mm256_max_epu32(sa, sb_rev);
    (bitonic_merge8_asc_avx2(lo), bitonic_merge8_asc_avx2(hi))
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
gen_sort_net!(sort_net_u32_2_16, cswap32, u32);
gen_sort_net!(sort_net_u64_2_16, cswap64, u64);

#[inline(always)]
fn sort_u32_small_2_16(arr: &mut [u32]) {
    let n = arr.len();
    let p = arr.as_mut_ptr();
    unsafe {
        if n <= 4 {
            match n {
                2 => {
                    cswap32(p, 0, 1);
                }
                3 => {
                    cswap32(p, 0, 1);
                    cswap32(p, 0, 2);
                    cswap32(p, 1, 2);
                }
                4 => {
                    cswap32(p, 0, 1);
                    cswap32(p, 2, 3);
                    cswap32(p, 0, 2);
                    cswap32(p, 1, 3);
                    cswap32(p, 1, 2);
                }
                _ => {}
            }
            return;
        }
        #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
        {
            use std::arch::x86_64::*;
            if n == 8 {
                let v = _mm256_loadu_si256(p as *const __m256i);
                _mm256_storeu_si256(p as *mut __m256i, sort8_avx2(v));
                return;
            }
            if n < 8 {
                let mut tmp = [u32::MAX; 8];
                std::ptr::copy_nonoverlapping(p, tmp.as_mut_ptr(), n);
                let v = _mm256_loadu_si256(tmp.as_ptr() as *const __m256i);
                _mm256_storeu_si256(tmp.as_mut_ptr() as *mut __m256i, sort8_avx2(v));
                std::ptr::copy_nonoverlapping(tmp.as_ptr(), p, n);
                return;
            }
            if n == 16 {
                let va = _mm256_loadu_si256(p as *const __m256i);
                let vb = _mm256_loadu_si256(p.add(8) as *const __m256i);
                let (la, ha) = sort16_avx2(va, vb);
                _mm256_storeu_si256(p as *mut __m256i, la);
                _mm256_storeu_si256(p.add(8) as *mut __m256i, ha);
                return;
            }
            let mut tmp = [u32::MAX; 16];
            std::ptr::copy_nonoverlapping(p, tmp.as_mut_ptr(), n);
            let va = _mm256_loadu_si256(tmp.as_ptr() as *const __m256i);
            let vb = _mm256_loadu_si256(tmp.as_ptr().add(8) as *const __m256i);
            let (la, ha) = sort16_avx2(va, vb);
            _mm256_storeu_si256(tmp.as_mut_ptr() as *mut __m256i, la);
            _mm256_storeu_si256(tmp.as_mut_ptr().add(8) as *mut __m256i, ha);
            std::ptr::copy_nonoverlapping(tmp.as_ptr(), p, n);
        }
        #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
        {
            sort_net_u32_2_16(p, n);
        }
    }
}

#[inline(always)]
fn sort_u32_small(arr: &mut [u32]) {
    let n = arr.len();
    if n <= 1 {
        return;
    }
    if n <= 16 {
        sort_u32_small_2_16(arr);
        return;
    }
    if n <= 32 {
        let mid = n >> 1;
        let p = arr.as_mut_ptr();
        sort_u32_small_2_16(&mut arr[..mid]);
        sort_u32_small_2_16(&mut arr[mid..]);
        let mut buf = [0u32; 32];
        let mut a = 0usize;
        let mut b = mid;
        let mut w = 0usize;
        while a < mid && b < n {
            let va = unsafe { *p.add(a) };
            let vb = unsafe { *p.add(b) };
            let take_a = va <= vb;
            buf[w] = if take_a { va } else { vb };
            w += 1;
            a += usize::from(take_a);
            b += usize::from(!take_a);
        }
        while a < mid {
            buf[w] = unsafe { *p.add(a) };
            w += 1;
            a += 1;
        }
        while b < n {
            buf[w] = unsafe { *p.add(b) };
            w += 1;
            b += 1;
        }
        unsafe { std::ptr::copy_nonoverlapping(buf.as_ptr(), p, n) };
        return;
    }
    arr.sort_unstable();
}

#[inline]
fn radix_sort_u32(arr: &mut [u32], tmp: &mut Vec<u32>) {
    let n = arr.len();
    if n <= 1 {
        return;
    }
    if tmp.len() < n {
        tmp.resize(n, 0);
    }
    let tmp_slice = &mut tmp[..n];

    let mut counts = [[0u32; 256]; 4];
    for &k in arr.iter() {
        counts[0][(k & 0xFF) as usize] += 1;
        counts[1][((k >> 8) & 0xFF) as usize] += 1;
        counts[2][((k >> 16) & 0xFF) as usize] += 1;
        counts[3][(k >> 24) as usize] += 1;
    }

    let n_u32 = n as u32;
    let first_k = arr[0];
    let mut in_tmp = false;

    for pass in 0..4usize {
        let shift = pass * 8;
        let b0 = ((first_k >> shift) & 0xFF) as usize;
        if counts[pass][b0] == n_u32 {
            continue;
        }
        let mut off = [0u32; 256];
        let mut sum = 0u32;
        for b in 0..256 {
            off[b] = sum;
            sum += counts[pass][b];
        }
        if !in_tmp {
            for &v in arr.iter() {
                let b = ((v >> shift) & 0xFF) as usize;
                let dst = off[b] as usize;
                off[b] += 1;
                unsafe { *tmp_slice.get_unchecked_mut(dst) = v };
            }
        } else {
            for &v in tmp_slice.iter() {
                let b = ((v >> shift) & 0xFF) as usize;
                let dst = off[b] as usize;
                off[b] += 1;
                unsafe { *arr.get_unchecked_mut(dst) = v };
            }
        }
        in_tmp = !in_tmp;
    }

    if in_tmp {
        arr.copy_from_slice(tmp_slice);
    }
}

#[inline]
fn msd8_sort_u32(arr: &mut [u32], tmp: &mut Vec<u32>) {
    let n = arr.len();
    if n <= 1 {
        return;
    }
    let mut counts = [0u32; 128];
    let mut max_b = 0usize;
    for &v in arr.iter() {
        let b = (v >> 24) as usize;
        if b >= 128 {
            radix_sort_u32(arr, tmp);
            return;
        }
        if b > max_b {
            max_b = b;
        }
        unsafe { *counts.get_unchecked_mut(b) += 1 };
    }
    if counts[max_b] == n as u32 {
        radix_sort_u32(arr, tmp);
        return;
    }
    if tmp.len() < n {
        tmp.resize(n, 0);
    }
    let tmp_slice = &mut tmp[..n];
    let mut off = [0u32; 128];
    let mut sum = 0u32;
    let mut max_c = 0u32;
    for b in 0..=max_b {
        let c = counts[b];
        off[b] = sum;
        sum += c;
        if c > max_c {
            max_c = c;
        }
    }
    let mut cur = off;
    for &v in arr.iter() {
        let b = (v >> 24) as usize;
        let dst = unsafe { *cur.get_unchecked(b) } as usize;
        unsafe {
            *cur.get_unchecked_mut(b) = (dst + 1) as u32;
            *tmp_slice.get_unchecked_mut(dst) = v;
        }
    }
    if max_c <= 160 {
        for b in 0..=max_b {
            let c = counts[b] as usize;
            if c == 0 {
                continue;
            }
            let s = off[b] as usize;
            if c == 1 {
                unsafe { *arr.get_unchecked_mut(s) = *tmp_slice.get_unchecked(s) };
            } else if c == 2 {
                unsafe {
                    let x0 = *tmp_slice.get_unchecked(s);
                    let x1 = *tmp_slice.get_unchecked(s + 1);
                    let lt = x0 < x1;
                    *arr.get_unchecked_mut(s) = if lt { x0 } else { x1 };
                    *arr.get_unchecked_mut(s + 1) = if lt { x1 } else { x0 };
                }
            } else {
                arr[s..s + c].copy_from_slice(&tmp_slice[s..s + c]);
                sort_u32_small(&mut arr[s..s + c]);
            }
        }
    } else {
        arr.copy_from_slice(tmp_slice);
        for b in 0..=max_b {
            let c = counts[b] as usize;
            if c <= 1 {
                continue;
            }
            let s = off[b] as usize;
            if c <= 160 {
                sort_u32_small(&mut arr[s..s + c]);
            } else {
                radix_sort_u32(&mut arr[s..s + c], tmp);
            }
        }
    }
}

#[inline]
unsafe fn radix_pass_11_u32(
    src: &[u32],
    dst: &mut [u32],
    counts: &[u32],
    shift_bits: u32,
    mask: u32,
) {
    let mut off = [0u32; 2048];
    let mut sum = 0u32;
    for (b, &c) in counts.iter().enumerate() {
        *off.get_unchecked_mut(b) = sum;
        sum += c;
    }
    for &k in src {
        let b = ((k >> shift_bits) & mask) as usize;
        let d = *off.get_unchecked(b) as usize;
        *off.get_unchecked_mut(b) = (d + 1) as u32;
        *dst.get_unchecked_mut(d) = k;
    }
}

#[inline]
fn radix_sort_u32_11bit(arr: &mut [u32], tmp: &mut Vec<u32>) {
    let n = arr.len();
    if n <= 1 {
        return;
    }
    if tmp.len() < n {
        tmp.resize(n, 0);
    }
    let tmp_slice = &mut tmp[..n];

    let mut c0 = [0u32; 2048];
    let mut c1 = [0u32; 2048];
    let mut c2 = [0u32; 1024];
    for &k in arr.iter() {
        unsafe {
            *c0.get_unchecked_mut((k & 0x7FF) as usize) += 1;
            *c1.get_unchecked_mut(((k >> 11) & 0x7FF) as usize) += 1;
            *c2.get_unchecked_mut((k >> 22) as usize) += 1;
        }
    }

    let n_u32 = n as u32;
    let first_k = arr[0];
    let mut in_tmp = false;

    for (counts, shift_bits, mask) in [
        (&c0[..], 0u32, 0x7FFu32),
        (&c1[..], 11u32, 0x7FFu32),
        (&c2[..], 22u32, 0x3FFu32),
    ] {
        let b0 = ((first_k >> shift_bits) & mask) as usize;
        if counts[b0] == n_u32 {
            continue;
        }
        unsafe {
            if !in_tmp {
                radix_pass_11_u32(arr, tmp_slice, counts, shift_bits, mask);
            } else {
                radix_pass_11_u32(tmp_slice, arr, counts, shift_bits, mask);
            }
        }
        in_tmp = !in_tmp;
    }

    if in_tmp {
        arr.copy_from_slice(tmp_slice);
    }
}

#[inline]
fn msd5_sort_u32(arr: &mut [u32], tmp: &mut Vec<u32>) {
    let n = arr.len();
    let mut counts = [0u16; 32];
    let mut min_b = 31usize;
    let mut max_b = 0usize;
    for &v in arr.iter() {
        let b = (v >> 26) as usize;
        if b >= 32 {
            msd8_sort_u32(arr, tmp);
            return;
        }
        if b < min_b {
            min_b = b;
        }
        if b > max_b {
            max_b = b;
        }
        unsafe { *counts.get_unchecked_mut(b) += 1 };
    }
    if min_b == max_b {
        msd8_sort_u32(arr, tmp);
        return;
    }
    if tmp.len() < n {
        tmp.resize(n, 0);
    }
    let mut off = [0u16; 32];
    let mut sum = 0u16;
    let mut max_c = 0u16;
    for b in min_b..=max_b {
        let c = unsafe { *counts.get_unchecked(b) };
        unsafe { *off.get_unchecked_mut(b) = sum };
        sum += c;
        if c > max_c {
            max_c = c;
        }
    }
    let mut cur = off;
    {
        let tmp_slice = &mut tmp[..n];
        for &v in arr.iter() {
            let b = (v >> 26) as usize;
            let dst = unsafe { *cur.get_unchecked(b) } as usize;
            unsafe {
                *cur.get_unchecked_mut(b) = (dst + 1) as u16;
                *tmp_slice.get_unchecked_mut(dst) = v;
            }
        }
        if max_c <= 160 {
            for b in min_b..=max_b {
                let c = unsafe { *counts.get_unchecked(b) } as usize;
                if c == 0 {
                    continue;
                }
                let s = unsafe { *off.get_unchecked(b) } as usize;
                if c == 1 {
                    unsafe { *arr.get_unchecked_mut(s) = *tmp_slice.get_unchecked(s) };
                } else if c == 2 {
                    unsafe {
                        let x0 = *tmp_slice.get_unchecked(s);
                        let x1 = *tmp_slice.get_unchecked(s + 1);
                        let lt = x0 < x1;
                        *arr.get_unchecked_mut(s) = if lt { x0 } else { x1 };
                        *arr.get_unchecked_mut(s + 1) = if lt { x1 } else { x0 };
                    }
                } else {
                    arr[s..s + c].copy_from_slice(&tmp_slice[s..s + c]);
                    sort_u32_small(&mut arr[s..s + c]);
                }
            }
            return;
        }
        arr.copy_from_slice(tmp_slice);
    }
    for b in min_b..=max_b {
        let c = unsafe { *counts.get_unchecked(b) } as usize;
        if c <= 1 {
            continue;
        }
        let s = unsafe { *off.get_unchecked(b) } as usize;
        if c <= 160 {
            sort_u32_small(&mut arr[s..s + c]);
        } else {
            msd8_sort_u32(&mut arr[s..s + c], tmp);
        }
    }
}

#[inline(always)]
fn sort_ts_u32(arr: &mut [u32], radix_tmp: &mut Vec<u32>) {
    if arr.len() <= 24 {
        sort_u32_small(arr);
    } else if arr.len() <= 4096 {
        msd5_sort_u32(arr, radix_tmp);
    } else {
        radix_sort_u32_11bit(arr, radix_tmp);
    }
}

#[inline(always)]
fn find_session_end_u32(p_ts: &[u32], i: usize) -> usize {
    let nev = p_ts.len();
    let ptr = p_ts.as_ptr();
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        if i + 16 < nev {
            use std::arch::x86_64::*;
            unsafe {
                let v_gap = _mm256_set1_epi32(SESSION_GAP_MS as i32);
                let mut k = i;
                while k + 9 <= nev {
                    let a = _mm256_loadu_si256(ptr.add(k) as *const __m256i);
                    let b = _mm256_loadu_si256(ptr.add(k + 1) as *const __m256i);
                    let diff = _mm256_sub_epi32(b, a);
                    let m = _mm256_movemask_epi8(_mm256_cmpgt_epi32(diff, v_gap)) as u32;
                    if m != 0 {
                        return k + 1 + ((m.trailing_zeros() >> 2) as usize);
                    }
                    k += 8;
                }
                let mut prev = *ptr.add(k);
                let mut j = k + 1;
                while j < nev {
                    let next = *ptr.add(j);
                    if next - prev > (SESSION_GAP_MS as u32) {
                        break;
                    }
                    prev = next;
                    j += 1;
                }
                return j;
            }
        }
    }
    let mut prev = unsafe { *ptr.add(i) };
    let mut j = i + 1;
    while j < nev {
        let next = unsafe { *ptr.add(j) };
        if next - prev > (SESSION_GAP_MS as u32) {
            break;
        }
        prev = next;
        j += 1;
    }
    j
}

#[repr(C)]
#[derive(Clone, Copy)]
struct CampTrack {
    in_win: u32,
    since: u16,
    in_active: u8,
    _pad: u8,
}

impl Default for CampTrack {
    fn default() -> Self {
        Self {
            in_win: 0,
            since: u16::MAX,
            in_active: 0,
            _pad: 0,
        }
    }
}

#[inline(always)]
fn advance_day(
    cur_day: &mut i32,
    cur_day_end: &mut u64,
    sess_start_rel: u64,
    min_day: i32,
    start_in_day: u64,
) {
    if *cur_day != i32::MIN && sess_start_rel < *cur_day_end + (DAY_MS as u64) {
        *cur_day += 1;
        *cur_day_end += DAY_MS as u64;
    } else {
        let day_idx = (((sess_start_rel + start_in_day) as u32) / (DAY_MS as u32)) as u64;
        *cur_day = min_day + (day_idx as i32);
        *cur_day_end = (day_idx + 1) * (DAY_MS as u64) - start_in_day;
    }
}

#[inline(always)]
fn flush_day_assisted(
    active_camps: &mut Vec<u32>,
    tracks: &mut [CampTrack],
    camp_store: &mut CampStore,
    cur_day: i32,
    day_sess_seq: u32,
) {
    let mut widx = 0usize;
    for ridx in 0..active_camps.len() {
        let c = unsafe { *active_camps.get_unchecked(ridx) } as usize;
        let tr = unsafe { tracks.get_unchecked_mut(c) };
        if tr.in_win > 0 {
            let delta = day_sess_seq - (tr.since as u32);
            if delta > 0 {
                camp_store.add_assisted(c as u32, cur_day, delta as i32);
            }
            tr.since = 0;
            unsafe { *active_camps.get_unchecked_mut(widx) = c as u32 };
            widx += 1;
        } else {
            tr.in_active = 0;
        }
    }
    active_camps.truncate(widx);
}

#[inline(always)]
fn finish_person_assisted(
    active_camps: &mut Vec<u32>,
    tracks: &mut [CampTrack],
    camp_store: &mut CampStore,
    cur_day: i32,
    day_sess_seq: u32,
) {
    for &c_u32 in active_camps.iter() {
        let tr = unsafe { tracks.get_unchecked_mut(c_u32 as usize) };
        if tr.in_win > 0 {
            let delta = day_sess_seq - (tr.since as u32);
            if delta > 0 {
                camp_store.add_assisted(c_u32, cur_day, delta as i32);
            }
            tr.in_win = 0;
            tr.since = u16::MAX;
        }
        tr.in_active = 0;
    }
    active_camps.clear();
}

trait ClickTimeSrc {
    fn len(&self) -> usize;
    unsafe fn ts_at(&self, i: usize) -> u64;
    unsafe fn camp_at(&self, i: usize) -> u32;
}

impl ClickTimeSrc for [u64] {
    #[inline(always)]
    fn len(&self) -> usize {
        self.len()
    }
    #[inline(always)]
    unsafe fn ts_at(&self, i: usize) -> u64 {
        (*self.get_unchecked(i)) >> 33
    }
    #[inline(always)]
    unsafe fn camp_at(&self, i: usize) -> u32 {
        ((*self.get_unchecked(i)) & 0xFFFF) as u32
    }
}

impl ClickTimeSrc for [CompactClick] {
    #[inline(always)]
    fn len(&self) -> usize {
        self.len()
    }
    #[inline(always)]
    unsafe fn ts_at(&self, i: usize) -> u64 {
        self.get_unchecked(i).ts_rel as u64
    }
    #[inline(always)]
    unsafe fn camp_at(&self, i: usize) -> u32 {
        self.get_unchecked(i).camp_idx
    }
}

#[inline(always)]
fn advance_clicks_window<S: ClickTimeSrc + ?Sized>(
    p_cks: &S,
    sess_start_rel: u64,
    sess_day: i32,
    day_sess_seq: u32,
    ck_left: &mut usize,
    ck_right: &mut usize,
    tracks: &mut [CampTrack],
    active_camps: &mut Vec<u32>,
    camp_store: &mut CampStore,
) {
    let ncks = p_cks.len();
    let max_ck_rel = sess_start_rel + (CLICK_REACH_MS as u64);
    while *ck_right < ncks && unsafe { p_cks.ts_at(*ck_right) } <= max_ck_rel {
        let c = unsafe { p_cks.camp_at(*ck_right) } as usize;
        let tr = unsafe { tracks.get_unchecked_mut(c) };
        if tr.in_win == 0 {
            debug_assert!(day_sess_seq < u16::MAX as u32);
            tr.since = day_sess_seq as u16;
            if tr.in_active == 0 {
                tr.in_active = 1;
                active_camps.push(c as u32);
            }
        }
        tr.in_win += 1;
        *ck_right += 1;
    }
    while *ck_left < *ck_right && unsafe { p_cks.ts_at(*ck_left) } < sess_start_rel {
        let c = unsafe { p_cks.camp_at(*ck_left) } as usize;
        let tr = unsafe { tracks.get_unchecked_mut(c) };
        tr.in_win -= 1;
        if tr.in_win == 0 {
            let delta = day_sess_seq - (tr.since as u32);
            if delta > 0 {
                camp_store.add_assisted(c as u32, sess_day, delta as i32);
            }
            tr.since = u16::MAX;
        }
        *ck_left += 1;
    }
}

#[inline(always)]
fn cmp_compact_click(a: &CompactClick, b: &CompactClick, overflow: &OverflowInterner) -> Ordering {
    a.ts_rel.cmp(&b.ts_rel).then_with(|| {
        if a.cid == b.cid {
            Ordering::Equal
        } else if (a.cid | b.cid) < B37_LIMIT {
            b.cid.cmp(&a.cid)
        } else {
            overflow.cmp_click_key(b.cid, a.cid)
        }
    })
}

#[cold]
#[inline(never)]
fn fix_click_ties_u64(
    slice: &mut [u64],
    matched_cids: &[u64],
    all_b37_cks: bool,
    overflow: &OverflowInterner,
) {
    let n = slice.len();
    let mut i = 0usize;
    while i + 1 < n {
        let t = slice[i] >> 33;
        let mut j = i + 1;
        while j < n && (slice[j] >> 33) == t {
            j += 1;
        }
        if j - i > 1 {
            if all_b37_cks {
                slice[i..j].sort_unstable_by(|&a, &b| {
                    let ca = unsafe { *matched_cids.get_unchecked(((a >> 16) & 0x1_FFFF) as usize) };
                    let cb = unsafe { *matched_cids.get_unchecked(((b >> 16) & 0x1_FFFF) as usize) };
                    cb.cmp(&ca)
                });
            } else {
                slice[i..j].sort_unstable_by(|&a, &b| {
                    let ca = unsafe { *matched_cids.get_unchecked(((a >> 16) & 0x1_FFFF) as usize) };
                    let cb = unsafe { *matched_cids.get_unchecked(((b >> 16) & 0x1_FFFF) as usize) };
                    if ca == cb {
                        Ordering::Equal
                    } else if (ca | cb) < B37_LIMIT {
                        cb.cmp(&ca)
                    } else {
                        overflow.cmp_click_key(cb, ca)
                    }
                });
            }
        }
        i = j;
    }
}

#[inline(always)]
fn sort_ck_u64(
    slice: &mut [u64],
    matched_cids: &[u64],
    all_b37_cks: bool,
    overflow: &OverflowInterner,
) {
    let n = slice.len();
    if n <= 1 {
        return;
    }
    if n <= 16 {
        unsafe { sort_net_u64_2_16(slice.as_mut_ptr(), n) };
    } else {
        slice.sort_unstable();
    }
    let mut prev_t = slice[0] >> 33;
    let mut has_tie = false;
    for &v in &slice[1..] {
        let t = v >> 33;
        has_tie |= t == prev_t;
        prev_t = t;
    }
    if has_tie {
        fix_click_ties_u64(slice, matched_cids, all_b37_cks, overflow);
    }
}

#[inline(always)]
fn run_session_loop_clicks<C: ClickTimeSrc + ?Sized, F>(
    p_ts: &[u32],
    p_cks: &C,
    p_purch: &[(u32, u32)],
    tracks: &mut [CampTrack],
    active_camps: &mut Vec<u32>,
    camp_store: &mut CampStore,
    pq_writer: &mut UserDailyParquetWriter,
    pname_bytes: &[u8],
    organic_id: u32,
    min_day: i32,
    start_in_day: u64,
    cat_rev: &mut [i64],
    cat_dirty: &mut Vec<usize>,
    get_ck: F,
) where
    F: Fn(&C, usize) -> u32,
{
    let mut emitted_user = false;
    if p_purch.is_empty() && p_cks.len() == 0 {
        let mut cur_day = i32::MIN;
        let mut cur_day_end = 0u64;
        let mut d_sessions = 0i32;
        let mut d_events = 0i32;
        let mut i = 0usize;
        let nev = p_ts.len();
        while i < nev {
            let sess_start_rel = unsafe { *p_ts.get_unchecked(i) } as u64;
            if sess_start_rel >= cur_day_end {
                if cur_day != i32::MIN {
                    pq_writer.push_user_day_zero_rev(
                        pname_bytes,
                        emitted_user,
                        cur_day,
                        d_sessions,
                        d_events,
                    );
                    emitted_user = true;
                }
                advance_day(
                    &mut cur_day,
                    &mut cur_day_end,
                    sess_start_rel,
                    min_day,
                    start_in_day,
                );
                d_sessions = 0;
                d_events = 0;
            }
            let j = find_session_end_u32(p_ts, i);
            d_sessions += 1;
            d_events += (j - i) as i32;
            camp_store.add_credited_zero_rev(organic_id, cur_day);
            i = j;
        }
        if cur_day != i32::MIN {
            pq_writer.push_user_day_zero_rev(
                pname_bytes,
                emitted_user,
                cur_day,
                d_sessions,
                d_events,
            );
        }
        return;
    }

    if p_purch.is_empty() {
        let mut ck_left = 0usize;
        let mut ck_right = 0usize;
        let mut cur_day = i32::MIN;
        let mut cur_day_end = 0u64;
        let mut day_sess_seq = 0u32;
        let mut d_sessions = 0i32;
        let mut d_events = 0i32;
        let mut i = 0usize;
        let nev = p_ts.len();
        while i < nev {
            let sess_start_rel = unsafe { *p_ts.get_unchecked(i) } as u64;
            if sess_start_rel >= cur_day_end {
                if cur_day != i32::MIN {
                    flush_day_assisted(active_camps, tracks, camp_store, cur_day, day_sess_seq);
                    day_sess_seq = 0;
                    pq_writer.push_user_day_zero_rev(
                        pname_bytes,
                        emitted_user,
                        cur_day,
                        d_sessions,
                        d_events,
                    );
                    emitted_user = true;
                }
                advance_day(
                    &mut cur_day,
                    &mut cur_day_end,
                    sess_start_rel,
                    min_day,
                    start_in_day,
                );
                d_sessions = 0;
                d_events = 0;
            }
            let sess_day = cur_day;
            let j = find_session_end_u32(p_ts, i);
            d_sessions += 1;
            d_events += (j - i) as i32;
            i = j;

            advance_clicks_window(
                p_cks,
                sess_start_rel,
                sess_day,
                day_sess_seq,
                &mut ck_left,
                &mut ck_right,
                tracks,
                active_camps,
                camp_store,
            );
            let win_camp = if ck_left == ck_right {
                organic_id
            } else {
                get_ck(p_cks, ck_right - 1)
            };
            camp_store.add_credited_zero_rev(win_camp, sess_day);
            day_sess_seq += 1;
        }
        if cur_day != i32::MIN {
            finish_person_assisted(active_camps, tracks, camp_store, cur_day, day_sess_seq);
            pq_writer.push_user_day_zero_rev(
                pname_bytes,
                emitted_user,
                cur_day,
                d_sessions,
                d_events,
            );
        } else {
            for &c in active_camps.iter() {
                tracks[c as usize] = CampTrack::default();
            }
            active_camps.clear();
        }
        return;
    }

    let mut ck_left = 0usize;
    let mut ck_right = 0usize;
    let mut pi = 0usize;
    let npurch = p_purch.len();
    let mut cur_day = i32::MIN;
    let mut cur_day_end = 0u64;
    let mut day_sess_seq = 0u32;
    let mut d_sessions = 0i32;
    let mut d_events = 0i32;
    let mut d_rev = 0i64;
    cat_dirty.clear();

    let mut i = 0usize;
    let nev = p_ts.len();
    while i < nev {
        let sess_start_rel = unsafe { *p_ts.get_unchecked(i) } as u64;
        if sess_start_rel >= cur_day_end {
            if cur_day != i32::MIN {
                flush_day_assisted(active_camps, tracks, camp_store, cur_day, day_sess_seq);
                day_sess_seq = 0;
                let top_cat = if cat_dirty.is_empty() {
                    None
                } else {
                    let mut best_cid = usize::MAX;
                    let mut best_rev = -1i64;
                    for &cid in cat_dirty.iter() {
                        let r = cat_rev[cid];
                        cat_rev[cid] = 0;
                        if r > best_rev || (r == best_rev && cid < best_cid) {
                            best_rev = r;
                            best_cid = cid;
                        }
                    }
                    cat_dirty.clear();
                    Some(best_cid)
                };
                pq_writer.push_user_day(
                    pname_bytes,
                    emitted_user,
                    cur_day,
                    d_sessions,
                    d_events,
                    d_rev,
                    top_cat,
                );
                emitted_user = true;
            }
            advance_day(
                &mut cur_day,
                &mut cur_day_end,
                sess_start_rel,
                min_day,
                start_in_day,
            );
            d_sessions = 0;
            d_events = 0;
            d_rev = 0;
        }
        let sess_day = cur_day;

        let j = find_session_end_u32(p_ts, i);
        let sess_events = (j - i) as i32;
        i = j;

        let mut sess_rev = 0i64;
        let prev_u32 = unsafe { *p_ts.get_unchecked(j - 1) };
        while pi < npurch && unsafe { p_purch.get_unchecked(pi) }.0 <= prev_u32 {
            let pc = unsafe { p_purch.get_unchecked(pi) }.1;
            let p = (pc & 0x00FF_FFFF) as i64;
            let c = (pc >> 24) as usize;
            sess_rev += p;
            if unsafe { *cat_rev.get_unchecked(c) } == 0 {
                cat_dirty.push(c);
            }
            unsafe { *cat_rev.get_unchecked_mut(c) += p };
            pi += 1;
        }

        d_sessions += 1;
        d_events += sess_events;
        d_rev += sess_rev;

        if p_cks.len() > 0 {
            advance_clicks_window(
                p_cks,
                sess_start_rel,
                sess_day,
                day_sess_seq,
                &mut ck_left,
                &mut ck_right,
                tracks,
                active_camps,
                camp_store,
            );
        }
        let win_camp = if ck_left == ck_right {
            organic_id
        } else {
            get_ck(p_cks, ck_right - 1)
        };
        camp_store.add_credited(win_camp, sess_day, sess_rev);
        day_sess_seq += 1;
    }

    if cur_day != i32::MIN {
        finish_person_assisted(active_camps, tracks, camp_store, cur_day, day_sess_seq);
        let top_cat = if cat_dirty.is_empty() {
            None
        } else {
            let mut best_cid = usize::MAX;
            let mut best_rev = -1i64;
            for &cid in cat_dirty.iter() {
                let r = cat_rev[cid];
                cat_rev[cid] = 0;
                if r > best_rev || (r == best_rev && cid < best_cid) {
                    best_rev = r;
                    best_cid = cid;
                }
            }
            cat_dirty.clear();
            Some(best_cid)
        };
        pq_writer.push_user_day(
            pname_bytes,
            emitted_user,
            cur_day,
            d_sessions,
            d_events,
            d_rev,
            top_cat,
        );
    } else {
        for &c in active_camps.iter() {
            tracks[c as usize] = CampTrack::default();
        }
        active_camps.clear();
    }
}

#[cold]
fn rehash_person_ht(ht: &mut Vec<u64>, cur_ht_cap: &mut usize, person_keys: &[u64]) -> usize {
    let new_cap = *cur_ht_cap * 2;
    let new_mask = new_cap - 1;
    if new_cap > ht.len() {
        ht.resize(new_cap, 0);
    }
    ht[..new_cap].fill(0);
    for (i, &opk) in person_keys.iter().enumerate() {
        if opk < B37_LIMIT {
            let oprem = mix42(opk) >> PERSON_BITS;
            let mut npos = (oprem as usize) & new_mask;
            while unsafe { *ht.get_unchecked(npos) } != 0 {
                npos = (npos + 1) & new_mask;
            }
            unsafe {
                *ht.get_unchecked_mut(npos) = (oprem << 31) | ((i as u64) + 1);
            }
        }
    }
    *cur_ht_cap = new_cap;
    new_mask
}

fn process_person_buckets_compact(
    num_threads: usize,
    all_ev_slabs: &[Vec<HugeSlab<u64>>],
    ev_runs: &[Vec<(u8, u16, u32, u16)>],
    all_ex_slabs: &[Vec<HugeSlab<ExtraSurv>>],
    ex_ev_runs: &[Vec<(u8, u16, u32, u16)>],
    all_ck_slabs: &[Vec<HugeSlab<SurvClick16>>],
    ck_runs: &[Vec<(u8, u16, u32, u16)>],
    ck_extra_per_bucket: &[Vec<ExtraClick>],
    overflow: &OverflowInterner,
    products: &ProductIndex,
    campaigns: &CampaignIndex,
    win_start: i64,
    min_day: i32,
    day_span: usize,
    user_daily_dir: &Path,
    drop_gate: &(Mutex<bool>, Condvar),
) -> (Vec<CampStore>, bool) {
    let nt = num_threads.max(1);
    let num_cats = products.categories.len();
    let num_camps = campaigns.ids.len();
    let use_flat = day_span <= 128 && num_camps <= 20_000;
    let start_in_day = win_start.rem_euclid(DAY_MS) as u64;

    let mut total_surv_ev = 0usize;
    let mut order: Vec<(usize, usize)> = (0..PERSON_BUCKETS)
        .filter_map(|pb| {
            let ev_n: usize = ev_runs[pb].iter().map(|r| r.3 as usize).sum::<usize>()
                + ex_ev_runs[pb].iter().map(|r| r.3 as usize).sum::<usize>();
            if ev_n == 0 {
                None
            } else {
                total_surv_ev += ev_n;
                let ck_n: usize = ck_runs[pb].iter().map(|r| r.3 as usize).sum::<usize>()
                    + ck_extra_per_bucket[pb].len();
                Some((ev_n + ck_n, pb))
            }
        })
        .collect();
    order.sort_unstable_by(|a, b| b.0.cmp(&a.0));
    let work_order: Vec<usize> = order.into_iter().map(|x| x.1).collect();

    if work_order.is_empty() {
        return (Vec::new(), false);
    }

    let est_rows_per_thread = ((total_surv_ev / 8 / nt).max(4_096)).min(393_216);
    let gate_idx = (work_order.len() * 3) / 8;
    let next_idx = AtomicUsize::new(0);

    let results: Vec<(CampStore, bool)> = std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(nt);
        let work_order_ref = &work_order;
        let next_idx_ref = &next_idx;

        for tid in 0..nt {
            handles.push(s.spawn(move || {
                let mut camp_store =
                    CampStore::new(use_flat, num_camps, campaigns.organic_id, min_day, day_span);
                let part_path = user_daily_dir.join(format!("part-{tid:05}.parquet"));
                let mut pq_writer = UserDailyParquetWriter::new(
                    part_path,
                    &products.categories,
                    min_day,
                    day_span,
                    est_rows_per_thread,
                );
                pq_writer.prefault();

                let mut ht: Vec<u64> = vec![0u64; 16_384];
                let mut non_b37_ht: HashMap<u64, u32, RandomState> =
                    HashMap::with_hasher(RandomState::with_seeds(17, 29, 43, 61));

                let mut person_keys: Vec<u64> = Vec::with_capacity(4096);
                let mut ev_cnt: Vec<u32> = Vec::with_capacity(4096);
                let mut ev_off: Vec<u32> = Vec::with_capacity(4096);
                let mut cur_ev_off: Vec<u32> = Vec::with_capacity(4096);
                let mut ev_pids_u16: Vec<u16> = Vec::with_capacity(65_536);
                let mut ev_pids_u32: Vec<u32> = Vec::new();
                let mut bucket_ts: Vec<u32> = Vec::with_capacity(65_536);
                let mut grouped_ts: Vec<u32> = Vec::with_capacity(65_536);
                let mut radix_tmp: Vec<u32> = Vec::new();

                let mut bucket_purch: Vec<(u32, u32, u32)> = Vec::with_capacity(4096);
                let mut purch_cnt: Vec<u32> = Vec::with_capacity(4096);
                let mut purch_off: Vec<u32> = Vec::with_capacity(4096);
                let mut cur_purch_off: Vec<u32> = Vec::with_capacity(4096);
                let mut grouped_purch: Vec<(u32, u32)> = Vec::with_capacity(4096);

                let mut ck_cnt: Vec<u32> = Vec::with_capacity(4096);
                let mut ck_off: Vec<u32> = Vec::with_capacity(4096);
                let mut cur_ck_off: Vec<u32> = Vec::with_capacity(4096);
                let mut matched_ck_u64: Vec<(u32, u64)> = Vec::with_capacity(16_384);
                let mut matched_cids: Vec<u64> = Vec::with_capacity(16_384);
                let mut grouped_ck_u64: Vec<u64> = Vec::with_capacity(16_384);
                let mut matched_cks: Vec<(u32, CompactClick)> = Vec::new();
                let mut grouped_cks: Vec<CompactClick> = Vec::new();

                let mut tracks: Vec<CampTrack> = vec![CampTrack::default(); num_camps];
                let mut active_camps: Vec<u32> = Vec::with_capacity(64);
                let mut cat_rev: Vec<i64> = vec![0i64; num_cats];
                let mut cat_dirty: Vec<usize> = Vec::with_capacity(16);

                let mut buf9 = [0u8; 9];
                let mut tmp_vec: Vec<u8> = Vec::with_capacity(64);

                loop {
                    let wi = next_idx_ref.fetch_add(1, MemOrdering::Relaxed);
                    if wi == gate_idx {
                        let mut g = drop_gate.0.lock().unwrap();
                        if !*g {
                            *g = true;
                            drop_gate.1.notify_all();
                        }
                    }
                    let Some(&pb) = work_order_ref.get(wi) else {
                        break;
                    };
                    let pb_ev_runs = &ev_runs[pb];
                    let pb_ex_runs = &ex_ev_runs[pb];
                    let total_ev: usize = pb_ev_runs.iter().map(|r| r.3 as usize).sum::<usize>()
                        + pb_ex_runs.iter().map(|r| r.3 as usize).sum::<usize>();
                    if total_ev == 0 {
                        continue;
                    }

                    let mut cur_ht_cap = (total_ev / 16).next_power_of_two().clamp(2048, 16384);
                    if cur_ht_cap > ht.len() {
                        ht.resize(cur_ht_cap, 0);
                    }
                    ht[..cur_ht_cap].fill(0);
                    let mut ht_mask = cur_ht_cap - 1;
                    let mut b37_count = 0usize;
                    if !non_b37_ht.is_empty() {
                        non_b37_ht.clear();
                    }
                    person_keys.clear();
                    ev_cnt.clear();
                    bucket_purch.clear();

                    ev_pids_u16.clear();
                    ev_pids_u16.reserve(total_ev);
                    bucket_ts.clear();
                    bucket_ts.reserve(total_ev);
                    let mut use_u16_pids = true;
                    let pids16_ptr = ev_pids_u16.as_mut_ptr();
                    let mut pids32_ptr = ev_pids_u32.as_mut_ptr();
                    let bts_ptr = bucket_ts.as_mut_ptr();
                    let mut b_idx = 0usize;

                    for &(rtid, sidx, start, len) in pb_ev_runs {
                        let chunk = unsafe {
                            SlabBucketsOut::get_slice(all_ev_slabs, rtid, sidx, start, len)
                        };
                        let clen = chunk.len();
                        let pre = clen.min(4);
                        for j in 0..pre {
                            let prem = unsafe { *chunk.get_unchecked(j) } >> 31;
                            prefetch_ptr(unsafe { ht.as_ptr().add((prem as usize) & ht_mask) });
                        }
                        for (j, &v) in chunk.iter().enumerate() {
                            if j + 4 < clen {
                                let nprem = unsafe { *chunk.get_unchecked(j + 4) } >> 31;
                                prefetch_ptr(unsafe {
                                    ht.as_ptr().add((nprem as usize) & ht_mask)
                                });
                            }
                            let prem = v >> 31;
                            let mut pos = (prem as usize) & ht_mask;
                            let pid = loop {
                                let slot = unsafe { *ht.get_unchecked(pos) };
                                if slot == 0 {
                                    let npid = person_keys.len() as u32;
                                    person_keys.push(unmix42((prem << PERSON_BITS) | (pb as u64)));
                                    ev_cnt.push(0);
                                    unsafe {
                                        *ht.get_unchecked_mut(pos) =
                                            (prem << 31) | ((npid as u64) + 1);
                                    }
                                    if use_u16_pids && npid >= 65_536 {
                                        use_u16_pids = false;
                                        ev_pids_u32.clear();
                                        ev_pids_u32.reserve(total_ev);
                                        unsafe {
                                            let dst32 = ev_pids_u32.as_mut_ptr();
                                            for k in 0..b_idx {
                                                *dst32.add(k) = *pids16_ptr.add(k) as u32;
                                            }
                                            pids32_ptr = dst32;
                                        }
                                    }
                                    b37_count += 1;
                                    if b37_count * 2 >= cur_ht_cap {
                                        ht_mask =
                                            rehash_person_ht(&mut ht, &mut cur_ht_cap, &person_keys);
                                    }
                                    break npid;
                                }
                                if (slot >> 31) == prem {
                                    break ((slot & 0x7FFF_FFFF) as u32) - 1;
                                }
                                pos = (pos + 1) & ht_mask;
                            };
                            unsafe {
                                *ev_cnt.get_unchecked_mut(pid as usize) += 1;
                                if use_u16_pids {
                                    *pids16_ptr.add(b_idx) = pid as u16;
                                } else {
                                    *pids32_ptr.add(b_idx) = pid;
                                }
                                *bts_ptr.add(b_idx) = (v & 0x7FFF_FFFF) as u32;
                            }
                            b_idx += 1;
                        }
                    }

                    for &(rtid, sidx, start, len) in pb_ex_runs {
                        let chunk = unsafe {
                            SlabBucketsOut::get_slice(all_ex_slabs, rtid, sidx, start, len)
                        };
                        for &ex in chunk {
                            let pid = if ex.person < B37_LIMIT {
                                let prem = mix42(ex.person) >> PERSON_BITS;
                                let mut pos = (prem as usize) & ht_mask;
                                loop {
                                    let slot = unsafe { *ht.get_unchecked(pos) };
                                    if slot == 0 {
                                        let npid = person_keys.len() as u32;
                                        person_keys.push(ex.person);
                                        ev_cnt.push(0);
                                        unsafe {
                                            *ht.get_unchecked_mut(pos) =
                                                (prem << 31) | ((npid as u64) + 1);
                                        }
                                        if use_u16_pids && npid >= 65_536 {
                                            use_u16_pids = false;
                                            ev_pids_u32.clear();
                                            ev_pids_u32.reserve(total_ev);
                                            unsafe {
                                                let dst32 = ev_pids_u32.as_mut_ptr();
                                                for k in 0..b_idx {
                                                    *dst32.add(k) = *pids16_ptr.add(k) as u32;
                                                }
                                                pids32_ptr = dst32;
                                            }
                                        }
                                        b37_count += 1;
                                        if b37_count * 2 >= cur_ht_cap {
                                            ht_mask = rehash_person_ht(
                                                &mut ht,
                                                &mut cur_ht_cap,
                                                &person_keys,
                                            );
                                        }
                                        break npid;
                                    }
                                    if (slot >> 31) == prem {
                                        break ((slot & 0x7FFF_FFFF) as u32) - 1;
                                    }
                                    pos = (pos + 1) & ht_mask;
                                }
                            } else {
                                let npid_cand = person_keys.len() as u32;
                                let p = *non_b37_ht.entry(ex.person).or_insert_with(|| {
                                    person_keys.push(ex.person);
                                    ev_cnt.push(0);
                                    npid_cand
                                });
                                if use_u16_pids && person_keys.len() > 65_536 {
                                    use_u16_pids = false;
                                    ev_pids_u32.clear();
                                    ev_pids_u32.reserve(total_ev);
                                    unsafe {
                                        let dst32 = ev_pids_u32.as_mut_ptr();
                                        for k in 0..b_idx {
                                            *dst32.add(k) = *pids16_ptr.add(k) as u32;
                                        }
                                        pids32_ptr = dst32;
                                    }
                                }
                                p
                            };
                            unsafe {
                                if ex.price_cat != 0 {
                                    bucket_purch.push((pid, ex.ts_rel, ex.price_cat));
                                }
                                *ev_cnt.get_unchecked_mut(pid as usize) += 1;
                                if use_u16_pids {
                                    *pids16_ptr.add(b_idx) = pid as u16;
                                } else {
                                    *pids32_ptr.add(b_idx) = pid;
                                }
                                *bts_ptr.add(b_idx) = ex.ts_rel;
                            }
                            b_idx += 1;
                        }
                    }

                    unsafe {
                        if use_u16_pids {
                            ev_pids_u16.set_len(b_idx);
                        } else {
                            ev_pids_u32.set_len(b_idx);
                        }
                        bucket_ts.set_len(b_idx);
                    }

                    let num_people = person_keys.len();
                    ev_off.clear();
                    cur_ev_off.clear();
                    let mut run = 0u32;
                    for &c in &ev_cnt {
                        ev_off.push(run);
                        cur_ev_off.push(run);
                        run += c;
                    }
                    ev_off.push(run);

                    if grouped_ts.len() < total_ev {
                        grouped_ts.resize(total_ev, 0);
                    }
                    if use_u16_pids {
                        for k in 0..total_ev {
                            unsafe {
                                let pid = *ev_pids_u16.get_unchecked(k) as usize;
                                let dst = *cur_ev_off.get_unchecked(pid) as usize;
                                *cur_ev_off.get_unchecked_mut(pid) = (dst + 1) as u32;
                                *grouped_ts.get_unchecked_mut(dst) = *bucket_ts.get_unchecked(k);
                            }
                        }
                    } else {
                        for k in 0..total_ev {
                            unsafe {
                                let pid = *ev_pids_u32.get_unchecked(k) as usize;
                                let dst = *cur_ev_off.get_unchecked(pid) as usize;
                                *cur_ev_off.get_unchecked_mut(pid) = (dst + 1) as u32;
                                *grouped_ts.get_unchecked_mut(dst) = *bucket_ts.get_unchecked(k);
                            }
                        }
                    }

                    let has_any_purch = !bucket_purch.is_empty();
                    if has_any_purch {
                        purch_cnt.clear();
                        purch_cnt.resize(num_people, 0);
                        for &(pid, _, _) in &bucket_purch {
                            unsafe { *purch_cnt.get_unchecked_mut(pid as usize) += 1 };
                        }
                        purch_off.clear();
                        cur_purch_off.clear();
                        let mut prun = 0u32;
                        for &c in &purch_cnt {
                            purch_off.push(prun);
                            cur_purch_off.push(prun);
                            prun += c;
                        }
                        purch_off.push(prun);
                        if grouped_purch.len() < bucket_purch.len() {
                            grouped_purch.resize(bucket_purch.len(), (0, 0));
                        }
                        for &(pid, ts_rel, pc) in &bucket_purch {
                            let dst = unsafe { *cur_purch_off.get_unchecked(pid as usize) } as usize;
                            unsafe {
                                *cur_purch_off.get_unchecked_mut(pid as usize) = (dst + 1) as u32;
                                *grouped_purch.get_unchecked_mut(dst) = (ts_rel, pc);
                            }
                        }
                    }

                    // Pass 3: Match clicks
                    let pb_ck_runs = &ck_runs[pb];
                    let pb_ck_ex = &ck_extra_per_bucket[pb];
                    let total_pb_cks: usize =
                        pb_ck_runs.iter().map(|r| r.3 as usize).sum::<usize>() + pb_ck_ex.len();
                    let use_ck_u64 = total_pb_cks <= 131_072 && num_camps <= 65_535;
                    ck_cnt.clear();
                    ck_cnt.resize(num_people, 0);
                    let total_matched_cks: usize;

                    if use_ck_u64 {
                        matched_ck_u64.clear();
                        matched_cids.clear();
                        for &(rtid, sidx, start, len) in pb_ck_runs {
                            let chunk = unsafe {
                                SlabBucketsOut::get_slice(all_ck_slabs, rtid, sidx, start, len)
                            };
                            for &ck in chunk {
                                let prem = ck.lo >> 31;
                                let mut pos = (prem as usize) & ht_mask;
                                loop {
                                    let slot = unsafe { *ht.get_unchecked(pos) };
                                    if slot == 0 {
                                        break;
                                    }
                                    if (slot >> 31) == prem {
                                        let pid = ((slot & 0x7FFF_FFFF) as u32) - 1;
                                        unsafe { *ck_cnt.get_unchecked_mut(pid as usize) += 1 };
                                        let ck_idx = matched_cids.len() as u64;
                                        matched_cids.push(ck.hi & MASK42);
                                        let ts_rel = ck.lo & 0x7FFF_FFFF;
                                        let camp_idx = ck.hi >> 42;
                                        matched_ck_u64.push((
                                            pid,
                                            (ts_rel << 33) | (ck_idx << 16) | camp_idx,
                                        ));
                                        break;
                                    }
                                    pos = (pos + 1) & ht_mask;
                                }
                            }
                        }
                        for &ex in pb_ck_ex {
                            let pk = ex.person;
                            if pk < B37_LIMIT {
                                let prem = mix42(pk) >> PERSON_BITS;
                                let mut pos = (prem as usize) & ht_mask;
                                loop {
                                    let slot = unsafe { *ht.get_unchecked(pos) };
                                    if slot == 0 {
                                        break;
                                    }
                                    if (slot >> 31) == prem {
                                        let pid = ((slot & 0x7FFF_FFFF) as u32) - 1;
                                        unsafe { *ck_cnt.get_unchecked_mut(pid as usize) += 1 };
                                        let ck_idx = matched_cids.len() as u64;
                                        matched_cids.push(ex.cid);
                                        matched_ck_u64.push((
                                            pid,
                                            ((ex.ts_rel as u64) << 33)
                                                | (ck_idx << 16)
                                                | (ex.camp_idx as u64),
                                        ));
                                        break;
                                    }
                                    pos = (pos + 1) & ht_mask;
                                }
                            } else if !non_b37_ht.is_empty() {
                                if let Some(&pid) = non_b37_ht.get(&pk) {
                                    unsafe { *ck_cnt.get_unchecked_mut(pid as usize) += 1 };
                                    let ck_idx = matched_cids.len() as u64;
                                    matched_cids.push(ex.cid);
                                    matched_ck_u64.push((
                                        pid,
                                        ((ex.ts_rel as u64) << 33)
                                            | (ck_idx << 16)
                                            | (ex.camp_idx as u64),
                                    ));
                                }
                            }
                        }
                        total_matched_cks = matched_ck_u64.len();
                        if total_matched_cks > 0 {
                            ck_off.clear();
                            cur_ck_off.clear();
                            let mut crun = 0u32;
                            for &c in &ck_cnt {
                                ck_off.push(crun);
                                cur_ck_off.push(crun);
                                crun += c;
                            }
                            ck_off.push(crun);
                            if grouped_ck_u64.len() < total_matched_cks {
                                grouped_ck_u64.resize(total_matched_cks, 0);
                            }
                            for &(pid, k64) in &matched_ck_u64 {
                                let dst =
                                    unsafe { *cur_ck_off.get_unchecked(pid as usize) } as usize;
                                unsafe {
                                    *cur_ck_off.get_unchecked_mut(pid as usize) = (dst + 1) as u32;
                                    *grouped_ck_u64.get_unchecked_mut(dst) = k64;
                                }
                            }
                        }
                    } else {
                        matched_cks.clear();
                        for &(rtid, sidx, start, len) in pb_ck_runs {
                            let chunk = unsafe {
                                SlabBucketsOut::get_slice(all_ck_slabs, rtid, sidx, start, len)
                            };
                            for &ck in chunk {
                                let prem = ck.lo >> 31;
                                let mut pos = (prem as usize) & ht_mask;
                                loop {
                                    let slot = unsafe { *ht.get_unchecked(pos) };
                                    if slot == 0 {
                                        break;
                                    }
                                    if (slot >> 31) == prem {
                                        let pid = ((slot & 0x7FFF_FFFF) as u32) - 1;
                                        unsafe { *ck_cnt.get_unchecked_mut(pid as usize) += 1 };
                                        matched_cks.push((
                                            pid,
                                            CompactClick {
                                                cid: ck.hi & MASK42,
                                                ts_rel: (ck.lo & 0x7FFF_FFFF) as u32,
                                                camp_idx: (ck.hi >> 42) as u32,
                                            },
                                        ));
                                        break;
                                    }
                                    pos = (pos + 1) & ht_mask;
                                }
                            }
                        }
                        for &ex in pb_ck_ex {
                            let pk = ex.person;
                            if pk < B37_LIMIT {
                                let prem = mix42(pk) >> PERSON_BITS;
                                let mut pos = (prem as usize) & ht_mask;
                                loop {
                                    let slot = unsafe { *ht.get_unchecked(pos) };
                                    if slot == 0 {
                                        break;
                                    }
                                    if (slot >> 31) == prem {
                                        let pid = ((slot & 0x7FFF_FFFF) as u32) - 1;
                                        unsafe { *ck_cnt.get_unchecked_mut(pid as usize) += 1 };
                                        matched_cks.push((
                                            pid,
                                            CompactClick {
                                                cid: ex.cid,
                                                ts_rel: ex.ts_rel,
                                                camp_idx: ex.camp_idx,
                                            },
                                        ));
                                        break;
                                    }
                                    pos = (pos + 1) & ht_mask;
                                }
                            } else if !non_b37_ht.is_empty() {
                                if let Some(&pid) = non_b37_ht.get(&pk) {
                                    unsafe { *ck_cnt.get_unchecked_mut(pid as usize) += 1 };
                                    matched_cks.push((
                                        pid,
                                        CompactClick {
                                            cid: ex.cid,
                                            ts_rel: ex.ts_rel,
                                            camp_idx: ex.camp_idx,
                                        },
                                    ));
                                }
                            }
                        }
                        total_matched_cks = matched_cks.len();
                        if total_matched_cks > 0 {
                            ck_off.clear();
                            cur_ck_off.clear();
                            let mut crun = 0u32;
                            for &c in &ck_cnt {
                                ck_off.push(crun);
                                cur_ck_off.push(crun);
                                crun += c;
                            }
                            ck_off.push(crun);
                            if grouped_cks.len() < total_matched_cks {
                                grouped_cks.resize(total_matched_cks, CompactClick::default());
                            }
                            for &(pid, ck) in &matched_cks {
                                let dst =
                                    unsafe { *cur_ck_off.get_unchecked(pid as usize) } as usize;
                                unsafe {
                                    *cur_ck_off.get_unchecked_mut(pid as usize) = (dst + 1) as u32;
                                    *grouped_cks.get_unchecked_mut(dst) = ck;
                                }
                            }
                        }
                    }

                    // Pass 4
                    let all_b37_cks = pb_ck_ex.is_empty();
                    for pid in 0..num_people {
                        let es = unsafe { *ev_off.get_unchecked(pid) } as usize;
                        let ee = unsafe { *ev_off.get_unchecked(pid + 1) } as usize;
                        let p_ts = &mut grouped_ts[es..ee];
                        sort_ts_u32(p_ts, &mut radix_tmp);

                        let p_purch: &mut [(u32, u32)] = if has_any_purch {
                            let ps = unsafe { *purch_off.get_unchecked(pid) } as usize;
                            let pe = unsafe { *purch_off.get_unchecked(pid + 1) } as usize;
                            let sl = &mut grouped_purch[ps..pe];
                            if sl.len() > 1 {
                                sl.sort_unstable_by(|a, b| a.0.cmp(&b.0));
                            }
                            sl
                        } else {
                            &mut []
                        };

                        let p_key = person_keys[pid];
                        let pname_raw = overflow.decode_user_with(p_key, &mut buf9, &mut tmp_vec);
                        let pname_cow;
                        let pname_bytes: &[u8] = if (p_key & OVERFLOW_BIT) == 0 {
                            pname_raw
                        } else {
                            pname_cow = String::from_utf8_lossy(pname_raw);
                            pname_cow.as_bytes()
                        };

                        if use_ck_u64 {
                            let p_cks: &[u64] = if total_matched_cks > 0 {
                                let cs = unsafe { *ck_off.get_unchecked(pid) } as usize;
                                let ce = unsafe { *ck_off.get_unchecked(pid + 1) } as usize;
                                let sl = &mut grouped_ck_u64[cs..ce];
                                sort_ck_u64(sl, &matched_cids, all_b37_cks, overflow);
                                sl
                            } else {
                                &[]
                            };
                            run_session_loop_clicks(
                                p_ts,
                                p_cks,
                                p_purch,
                                &mut tracks,
                                &mut active_camps,
                                &mut camp_store,
                                &mut pq_writer,
                                pname_bytes,
                                campaigns.organic_id,
                                min_day,
                                start_in_day,
                                &mut cat_rev,
                                &mut cat_dirty,
                                |s, idx| unsafe { (*s.get_unchecked(idx)) & 0xFFFF } as u32,
                            );
                        } else {
                            let p_cks: &[CompactClick] = if total_matched_cks > 0 {
                                let cs = unsafe { *ck_off.get_unchecked(pid) } as usize;
                                let ce = unsafe { *ck_off.get_unchecked(pid + 1) } as usize;
                                let sl = &mut grouped_cks[cs..ce];
                                if sl.len() > 1 {
                                    sl.sort_unstable_by(|a, b| cmp_compact_click(a, b, overflow));
                                }
                                sl
                            } else {
                                &[]
                            };
                            run_session_loop_clicks(
                                p_ts,
                                p_cks,
                                p_purch,
                                &mut tracks,
                                &mut active_camps,
                                &mut camp_store,
                                &mut pq_writer,
                                pname_bytes,
                                campaigns.organic_id,
                                min_day,
                                start_in_day,
                                &mut cat_rev,
                                &mut cat_dirty,
                                |s, idx| unsafe { s.get_unchecked(idx) }.camp_idx,
                            );
                        }
                    }
                }

                let wrote = pq_writer.close();
                (camp_store, wrote)
            }));
        }
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let mut wrote_any = false;
    let mut stores = Vec::with_capacity(results.len());
    for (st, w) in results {
        wrote_any |= w;
        stores.push(st);
    }
    (stores, wrote_any)
}

// =========================================================================
// Stage 4b: General Wide Path (> 25d window or > 256 categories)
// =========================================================================

fn process_person_buckets_wide(
    num_threads: usize,
    all_ev_slabs: &[Vec<HugeSlab<SurvEventWide>>],
    ev_runs: &[Vec<(u8, u16, u32, u16)>],
    all_ck_slabs: &[Vec<HugeSlab<SurvClickWide>>],
    ck_runs: &[Vec<(u8, u16, u32, u16)>],
    overflow: &OverflowInterner,
    products: &ProductIndex,
    campaigns: &CampaignIndex,
    min_day: i32,
    day_span: usize,
    user_daily_dir: &Path,
    drop_gate: &(Mutex<bool>, Condvar),
) -> (Vec<CampStore>, bool) {
    let nt = num_threads.max(1);
    let num_cats = products.categories.len();
    let num_camps = campaigns.ids.len();
    let use_flat = day_span <= 128 && num_camps <= 20_000;

    let mut total_surv_ev = 0usize;
    let mut order: Vec<(usize, usize)> = (0..PERSON_BUCKETS)
        .filter_map(|pb| {
            let ev_n: usize = ev_runs[pb].iter().map(|r| r.3 as usize).sum();
            if ev_n == 0 {
                None
            } else {
                total_surv_ev += ev_n;
                let ck_n: usize = ck_runs[pb].iter().map(|r| r.3 as usize).sum();
                Some((ev_n + ck_n, pb))
            }
        })
        .collect();
    order.sort_unstable_by(|a, b| b.0.cmp(&a.0));
    let work_order: Vec<usize> = order.into_iter().map(|x| x.1).collect();

    if work_order.is_empty() {
        return (Vec::new(), false);
    }

    let est_rows_per_thread = ((total_surv_ev / 8 / nt).max(4_096)).min(393_216);
    let gate_idx = (work_order.len() * 3) / 8;
    let next_idx = AtomicUsize::new(0);

    let results: Vec<(CampStore, bool)> = std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(nt);
        let work_order_ref = &work_order;
        let next_idx_ref = &next_idx;

        for tid in 0..nt {
            handles.push(s.spawn(move || {
                let mut camp_store =
                    CampStore::new(use_flat, num_camps, campaigns.organic_id, min_day, day_span);
                let part_path = user_daily_dir.join(format!("part-{tid:05}.parquet"));
                let mut pq_writer = UserDailyParquetWriter::new(
                    part_path,
                    &products.categories,
                    min_day,
                    day_span,
                    est_rows_per_thread,
                );
                pq_writer.prefault();

                let mut evs: Vec<SurvEventWide> = Vec::new();
                let mut cks: Vec<SurvClickWide> = Vec::new();
                let mut camp_in_win: Vec<u32> = vec![0u32; num_camps];
                let mut active_camps: Vec<u32> = Vec::with_capacity(32);
                let mut cat_rev: Vec<i64> = vec![0i64; num_cats];
                let mut cat_dirty: Vec<usize> = Vec::with_capacity(16);

                let mut buf9 = [0u8; 9];
                let mut tmp_vec: Vec<u8> = Vec::with_capacity(64);

                loop {
                    let wi = next_idx_ref.fetch_add(1, MemOrdering::Relaxed);
                    if wi == gate_idx {
                        let mut g = drop_gate.0.lock().unwrap();
                        if !*g {
                            *g = true;
                            drop_gate.1.notify_all();
                        }
                    }
                    let Some(&pb) = work_order_ref.get(wi) else {
                        break;
                    };
                    let total_ev: usize = ev_runs[pb].iter().map(|r| r.3 as usize).sum();
                    if total_ev == 0 {
                        continue;
                    }
                    evs.clear();
                    evs.reserve(total_ev);
                    for &(rtid, sidx, start, len) in &ev_runs[pb] {
                        let c = unsafe {
                            SlabBucketsOut::get_slice(all_ev_slabs, rtid, sidx, start, len)
                        };
                        evs.extend_from_slice(c);
                    }
                    let total_ck: usize = ck_runs[pb].iter().map(|r| r.3 as usize).sum();
                    cks.clear();
                    if total_ck > 0 {
                        cks.reserve(total_ck);
                        for &(rtid, sidx, start, len) in &ck_runs[pb] {
                            let c = unsafe {
                                SlabBucketsOut::get_slice(all_ck_slabs, rtid, sidx, start, len)
                            };
                            cks.extend_from_slice(c);
                        }
                    }

                    evs.sort_unstable_by(|a, b| a.person.cmp(&b.person).then_with(|| a.ts.cmp(&b.ts)));
                    if cks.len() > 1 {
                        cks.sort_unstable_by(|a, b| {
                            a.person
                                .cmp(&b.person)
                                .then_with(|| a.ts.cmp(&b.ts))
                                .then_with(|| overflow.cmp_click_key(b.cid, a.cid))
                        });
                    }

                    let mut ei = 0usize;
                    let mut ci = 0usize;
                    while ei < evs.len() {
                        let p_key = evs[ei].person;
                        let ev_s = ei;
                        ei += 1;
                        while ei < evs.len() && evs[ei].person == p_key {
                            ei += 1;
                        }
                        let p_evs = &evs[ev_s..ei];

                        while ci < cks.len() && cks[ci].person < p_key {
                            ci += 1;
                        }
                        let ck_s = ci;
                        while ci < cks.len() && cks[ci].person == p_key {
                            ci += 1;
                        }
                        let p_cks = &cks[ck_s..ci];

                        let pname_raw = overflow.decode_user_with(p_key, &mut buf9, &mut tmp_vec);
                        let pname_cow;
                        let pname_bytes: &[u8] = if (p_key & OVERFLOW_BIT) == 0 {
                            pname_raw
                        } else {
                            pname_cow = String::from_utf8_lossy(pname_raw);
                            pname_cow.as_bytes()
                        };
                        let mut emitted_user = false;

                        let mut ck_left = 0usize;
                        let mut ck_right = 0usize;
                        let mut cur_day = i32::MIN;
                        let mut d_sessions = 0i32;
                        let mut d_events = 0i32;
                        let mut d_rev = 0i64;
                        cat_dirty.clear();

                        let mut i = 0usize;
                        while i < p_evs.len() {
                            let sess_start = p_evs[i].ts;
                            let sess_day = sess_start.div_euclid(DAY_MS) as i32;

                            if sess_day != cur_day {
                                if cur_day != i32::MIN {
                                    let top_cat = if cat_dirty.is_empty() {
                                        None
                                    } else {
                                        let mut best_cid = usize::MAX;
                                        let mut best_rev = -1i64;
                                        for &cid in &cat_dirty {
                                            let r = cat_rev[cid];
                                            cat_rev[cid] = 0;
                                            if r > best_rev || (r == best_rev && cid < best_cid) {
                                                best_rev = r;
                                                best_cid = cid;
                                            }
                                        }
                                        cat_dirty.clear();
                                        Some(best_cid)
                                    };
                                    pq_writer.push_user_day(
                                        pname_bytes,
                                        emitted_user,
                                        cur_day,
                                        d_sessions,
                                        d_events,
                                        d_rev,
                                        top_cat,
                                    );
                                    emitted_user = true;
                                }
                                cur_day = sess_day;
                                d_sessions = 0;
                                d_events = 0;
                                d_rev = 0;
                            }

                            let mut sess_events = 1i32;
                            let mut sess_rev = 0i64;
                            let p0 = p_evs[i].price_cents as i64;
                            let c0 = p_evs[i].cat_id as usize;
                            if p0 > 0 {
                                sess_rev += p0;
                                if cat_rev[c0] == 0 {
                                    cat_dirty.push(c0);
                                }
                                cat_rev[c0] += p0;
                            }

                            let mut prev_ts = sess_start;
                            let mut j = i + 1;
                            while j < p_evs.len() {
                                let next_ts = p_evs[j].ts;
                                if next_ts - prev_ts > SESSION_GAP_MS {
                                    break;
                                }
                                prev_ts = next_ts;
                                sess_events += 1;
                                let pj = p_evs[j].price_cents as i64;
                                let cj = p_evs[j].cat_id as usize;
                                if pj > 0 {
                                    sess_rev += pj;
                                    if cat_rev[cj] == 0 {
                                        cat_dirty.push(cj);
                                    }
                                    cat_rev[cj] += pj;
                                }
                                j += 1;
                            }
                            i = j;

                            d_sessions += 1;
                            d_events += sess_events;
                            d_rev += sess_rev;

                            if !p_cks.is_empty() {
                                while ck_right < p_cks.len() && p_cks[ck_right].ts <= sess_start {
                                    let c = p_cks[ck_right].camp_idx;
                                    if camp_in_win[c as usize] == 0 {
                                        active_camps.push(c);
                                    }
                                    camp_in_win[c as usize] += 1;
                                    ck_right += 1;
                                }
                                let min_ck_ts = sess_start - CLICK_REACH_MS;
                                while ck_left < ck_right && p_cks[ck_left].ts < min_ck_ts {
                                    let c = p_cks[ck_left].camp_idx;
                                    camp_in_win[c as usize] -= 1;
                                    ck_left += 1;
                                }
                            }

                            if ck_left == ck_right {
                                active_camps.clear();
                                camp_store.add_credited(campaigns.organic_id, sess_day, sess_rev);
                            } else {
                                let win_camp = p_cks[ck_right - 1].camp_idx;
                                camp_store.add_credited(win_camp, sess_day, sess_rev);

                                let mut widx = 0usize;
                                for ridx in 0..active_camps.len() {
                                    let c = active_camps[ridx];
                                    if camp_in_win[c as usize] > 0 {
                                        active_camps[widx] = c;
                                        widx += 1;
                                        camp_store.add_assisted(c, sess_day, 1);
                                    }
                                }
                                active_camps.truncate(widx);
                            }
                        }

                        if cur_day != i32::MIN {
                            let top_cat = if cat_dirty.is_empty() {
                                None
                            } else {
                                let mut best_cid = usize::MAX;
                                let mut best_rev = -1i64;
                                for &cid in &cat_dirty {
                                    let r = cat_rev[cid];
                                    cat_rev[cid] = 0;
                                    if r > best_rev || (r == best_rev && cid < best_cid) {
                                        best_rev = r;
                                        best_cid = cid;
                                    }
                                }
                                cat_dirty.clear();
                                Some(best_cid)
                            };
                            pq_writer.push_user_day(
                                pname_bytes,
                                emitted_user,
                                cur_day,
                                d_sessions,
                                d_events,
                                d_rev,
                                top_cat,
                            );
                        }

                        for &c in &active_camps {
                            camp_in_win[c as usize] = 0;
                        }
                        active_camps.clear();
                    }
                }

                let wrote = pq_writer.close();
                (camp_store, wrote)
            }));
        }
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let mut wrote_any = false;
    let mut stores = Vec::with_capacity(results.len());
    for (st, w) in results {
        wrote_any |= w;
        stores.push(st);
    }
    (stores, wrote_any)
}

// =========================================================================
// Main Pipeline Orchestration
// =========================================================================

struct ChunkJob {
    fidx: usize,
    start: usize,
    end: usize,
}

#[derive(Clone, Copy, Default)]
struct PendingClick {
    uid_k: u64,
    cid_k: u64,
    ts_rel: i64,
    camp_idx: u32,
}

struct ReaderThreadOut {
    eid_out: SlabBucketsOut<RawEvent16>,
    extras: Vec<ExtraEvent>,
    extra_ck_buckets: Vec<Vec<ExtraClick>>,
    ck_compact_out: Option<SlabBucketsOut<SurvClick16>>,
    ck_wide_out: Option<SlabBucketsOut<SurvClickWide>>,
}

const NT_RAW_BLOCK: usize = 512;
const NT_SURV_BLOCK: usize = 256;
const NT_CLICK_BLOCK: usize = 256;
const EX_SURV_BLOCK: usize = 64;
const WIDE_BLOCK: usize = 256;

#[inline(always)]
fn tomb_ing_biased_from_rel_sec(ing_rel_sec: i64) -> u64 {
    let b = ing_rel_sec + TOMB_ING_BIAS;
    if b >= 1 && b <= TOMB_ING_MAX {
        b as u64
    } else {
        0
    }
}

#[inline(always)]
fn push_extra_click_bucketed(
    extra_ck_buckets: &mut Vec<Vec<ExtraClick>>,
    pb: usize,
    ck: ExtraClick,
) {
    if extra_ck_buckets.is_empty() {
        *extra_ck_buckets = (0..PERSON_BUCKETS).map(|_| Vec::new()).collect();
    }
    unsafe { extra_ck_buckets.get_unchecked_mut(pb) }.push(ck);
}

#[cold]
#[inline(never)]
fn push_raw_fallback(
    eid: &[u8],
    eid_b37: Option<u64>,
    uid_k: u64,
    valid: bool,
    ts_abs: i64,
    ingest_abs: i64,
    tomb_ing_b: u64,
    price_cents: u32,
    cat_id: u32,
    overflow: &OverflowInterner,
    tid: usize,
    idx_bits: u32,
    extras: &mut Vec<ExtraEvent>,
    eid_staged: &mut NtSlabBuckets<RawEvent16, 4, NT_RAW_BLOCK>,
) {
    let (eb, lo_eid, hi_eid, eid_k) = if let Some(eid_42) = eid_b37 {
        let h = mix42(eid_42);
        (
            (h as usize) & EID_MASK,
            (((h >> EID_BITS) as u32 as u64) << 32) | RAW_SPECIAL_BIT,
            0u64,
            eid_42,
        )
    } else {
        let ek = overflow.encode(eid);
        let h = mix64(ek);
        (
            (h as usize) & EID_MASK,
            (((h >> EID_BITS) as u32 as u64) << 32) | RAW_SPECIAL_BIT,
            RAW_EXTRA_EID_BIT,
            ek,
        )
    };

    if !valid && eid_b37.is_some() && tomb_ing_b != 0 {
        eid_staged.push(
            eb,
            RawEvent16 {
                lo: lo_eid | tomb_ing_b,
                hi: hi_eid,
            },
        );
        return;
    }

    let eidx = extras.len();
    extras.push(ExtraEvent {
        eid_k,
        uid_k,
        ts: ts_abs,
        ingest_ts: ingest_abs,
        price_cents,
        cat_id,
        valid,
    });
    let eref = ((tid as u64) << idx_bits) | (eidx as u64);
    eid_staged.push(
        eb,
        RawEvent16 {
            lo: lo_eid | tomb_ing_b,
            hi: hi_eid | (eref + 1),
        },
    );
}

#[inline(always)]
fn emit_surv_click(
    ph: u64,
    cid_k: u64,
    ts_rel: i64,
    camp_idx: u32,
    use_compact_ts: bool,
    ck_init_elems: usize,
    ck_extra_elems: usize,
    win_start: i64,
    extra_ck_buckets: &mut Vec<Vec<ExtraClick>>,
    ck_compact_staged: &mut Option<NtSlabBuckets<SurvClick16, 4, NT_CLICK_BLOCK>>,
    ck_wide_staged: &mut Option<DirectSlabBuckets<SurvClickWide, WIDE_BLOCK>>,
) {
    if use_compact_ts {
        let out = ck_compact_staged.get_or_insert_with(|| {
            NtSlabBuckets::new(PERSON_BUCKETS, ck_init_elems, ck_extra_elems)
        });
        let ts_rel_u32 = (ts_rel + CLICK_REACH_MS) as u32;
        if ph < B37_LIMIT && cid_k < B37_LIMIT && camp_idx < (1 << 22) {
            let pb = (ph as usize) & PERSON_MASK;
            out.push(
                pb,
                SurvClick16 {
                    lo: ((ph >> PERSON_BITS) << 31) | (ts_rel_u32 as u64),
                    hi: cid_k | ((camp_idx as u64) << 42),
                },
            );
        } else {
            let person = decode_canon_val(ph);
            let pb = if ph < B37_LIMIT {
                (ph as usize) & PERSON_MASK
            } else {
                (mix64(person) as usize) & PERSON_MASK
            };
            push_extra_click_bucketed(
                extra_ck_buckets,
                pb,
                ExtraClick {
                    person,
                    cid: cid_k,
                    ts_rel: ts_rel_u32,
                    camp_idx,
                },
            );
        }
    } else {
        let person = decode_canon_val(ph);
        let pb = if ph < B37_LIMIT {
            (ph as usize) & PERSON_MASK
        } else {
            (mix64(person) as usize) & PERSON_MASK
        };
        let out = ck_wide_staged.get_or_insert_with(|| {
            DirectSlabBuckets::new(PERSON_BUCKETS, ck_init_elems, ck_extra_elems)
        });
        out.push(
            pb,
            SurvClickWide {
                person,
                cid: cid_k,
                ts: win_start + ts_rel,
                camp_idx,
            },
        );
    }
}

struct ClickResolver<'a> {
    link_redirect: &'a LinkRedirect,
    use_compact_ts: bool,
    ck_init_elems: usize,
    ck_extra_elems: usize,
    win_start: i64,
    extra_ck_buckets: &'a mut Vec<Vec<ExtraClick>>,
    ck_compact_staged: &'a mut Option<NtSlabBuckets<SurvClick16, 4, NT_CLICK_BLOCK>>,
    ck_wide_staged: &'a mut Option<DirectSlabBuckets<SurvClickWide, WIDE_BLOCK>>,
    ck_bufs: [[PendingClick; 64]; 2],
    h_bufs: [[u64; 64]; 2],
    hits: [u8; 64],
    hit_bidx: [u32; 64],
    n_hits: usize,
    misses: [u8; 64],
    n_misses: usize,
    cur_buf: usize,
    cur_len: usize,
    has_pending: bool,
    has_b6_compact: bool,
}

impl<'a> ClickResolver<'a> {
    #[inline]
    fn new(
        link_redirect: &'a LinkRedirect,
        use_compact_ts: bool,
        ck_init_elems: usize,
        ck_extra_elems: usize,
        win_start: i64,
        extra_ck_buckets: &'a mut Vec<Vec<ExtraClick>>,
        ck_compact_staged: &'a mut Option<NtSlabBuckets<SurvClick16, 4, NT_CLICK_BLOCK>>,
        ck_wide_staged: &'a mut Option<DirectSlabBuckets<SurvClickWide, WIDE_BLOCK>>,
    ) -> Self {
        let has_b6_compact = use_compact_ts && link_redirect.has_bucket6();
        Self {
            link_redirect,
            use_compact_ts,
            ck_init_elems,
            ck_extra_elems,
            win_start,
            extra_ck_buckets,
            ck_compact_staged,
            ck_wide_staged,
            ck_bufs: [[PendingClick::default(); 64]; 2],
            h_bufs: [[0u64; 64]; 2],
            hits: [0u8; 64],
            hit_bidx: [0u32; 64],
            n_hits: 0,
            misses: [0u8; 64],
            n_misses: 0,
            cur_buf: 0,
            cur_len: 0,
            has_pending: false,
            has_b6_compact,
        }
    }

    #[inline(always)]
    fn push(&mut self, pk: PendingClick) {
        let h = if self.has_b6_compact && pk.uid_k < B37_LIMIT {
            self.link_redirect.prefetch_bloom_b37(pk.uid_k)
        } else {
            self.link_redirect.prefetch_bloom_for(pk.uid_k)
        };
        let b = self.cur_buf;
        let len = self.cur_len;
        unsafe {
            *self.ck_bufs.get_unchecked_mut(b).get_unchecked_mut(len) = pk;
            *self.h_bufs.get_unchecked_mut(b).get_unchecked_mut(len) = h;
        }
        self.cur_len = len + 1;
        if self.cur_len == 64 {
            self.rotate();
        }
    }

    #[inline]
    fn emit_pending_fast(&mut self, buf_idx: usize) {
        let out = self.ck_compact_staged.get_or_insert_with(|| {
            NtSlabBuckets::new(PERSON_BUCKETS, self.ck_init_elems, self.ck_extra_elems)
        });
        for i in 0..self.n_misses {
            let j = unsafe { *self.misses.get_unchecked(i) } as usize;
            let pk = unsafe { *self.ck_bufs.get_unchecked(buf_idx).get_unchecked(j) };
            let h = unsafe { *self.h_bufs.get_unchecked(buf_idx).get_unchecked(j) };
            let ts_rel_u32 = (pk.ts_rel + CLICK_REACH_MS) as u32;
            if pk.cid_k < B37_LIMIT && pk.camp_idx < (1 << 22) {
                out.push(
                    (h as usize) & PERSON_MASK,
                    SurvClick16 {
                        lo: ((h >> PERSON_BITS) << 31) | (ts_rel_u32 as u64),
                        hi: pk.cid_k | ((pk.camp_idx as u64) << 42),
                    },
                );
            } else {
                push_extra_click_bucketed(
                    self.extra_ck_buckets,
                    (h as usize) & PERSON_MASK,
                    ExtraClick {
                        person: decode_canon_val(h),
                        cid: pk.cid_k,
                        ts_rel: ts_rel_u32,
                        camp_idx: pk.camp_idx,
                    },
                );
            }
        }
        for i in 0..self.n_hits {
            let j = unsafe { *self.hits.get_unchecked(i) } as usize;
            let pk = unsafe { *self.ck_bufs.get_unchecked(buf_idx).get_unchecked(j) };
            let h = unsafe { *self.h_bufs.get_unchecked(buf_idx).get_unchecked(j) };
            let sidx = unsafe { *self.hit_bidx.get_unchecked(i) } as usize;
            let ph = self.link_redirect.resolve_from_slot_hashed(pk.uid_k, h, sidx);
            let ts_rel_u32 = (pk.ts_rel + CLICK_REACH_MS) as u32;
            if ph < B37_LIMIT && pk.cid_k < B37_LIMIT && pk.camp_idx < (1 << 22) {
                out.push(
                    (ph as usize) & PERSON_MASK,
                    SurvClick16 {
                        lo: ((ph >> PERSON_BITS) << 31) | (ts_rel_u32 as u64),
                        hi: pk.cid_k | ((pk.camp_idx as u64) << 42),
                    },
                );
            } else {
                let person = decode_canon_val(ph);
                let pb = if ph < B37_LIMIT {
                    (ph as usize) & PERSON_MASK
                } else {
                    (mix64(person) as usize) & PERSON_MASK
                };
                push_extra_click_bucketed(
                    self.extra_ck_buckets,
                    pb,
                    ExtraClick {
                        person,
                        cid: pk.cid_k,
                        ts_rel: ts_rel_u32,
                        camp_idx: pk.camp_idx,
                    },
                );
            }
        }
        self.n_hits = 0;
        self.n_misses = 0;
    }

    #[inline]
    fn prep_batch_fast(&mut self, buf_idx: usize, len: usize) {
        let mut nh = 0usize;
        let mut nm = 0usize;
        for j in 0..len {
            let pk = unsafe { *self.ck_bufs.get_unchecked(buf_idx).get_unchecked(j) };
            let h = unsafe { *self.h_bufs.get_unchecked(buf_idx).get_unchecked(j) };
            if pk.uid_k < B37_LIMIT {
                let hit = usize::from(self.link_redirect.bloom_test_b37(h));
                unsafe {
                    *self.hits.get_unchecked_mut(nh) = j as u8;
                    *self.misses.get_unchecked_mut(nm) = j as u8;
                }
                nh += hit;
                nm += 1 - hit;
            } else {
                let person = self.link_redirect.resolve(pk.uid_k);
                let ph = if person < B37_LIMIT {
                    mix42(person)
                } else {
                    person
                };
                emit_surv_click(
                    ph,
                    pk.cid_k,
                    pk.ts_rel,
                    pk.camp_idx,
                    self.use_compact_ts,
                    self.ck_init_elems,
                    self.ck_extra_elems,
                    self.win_start,
                    self.extra_ck_buckets,
                    self.ck_compact_staged,
                    self.ck_wide_staged,
                );
            }
        }
        for i in 0..nh {
            let j = unsafe { *self.hits.get_unchecked(i) } as usize;
            let h = unsafe { *self.h_bufs.get_unchecked(buf_idx).get_unchecked(j) };
            unsafe {
                *self.hit_bidx.get_unchecked_mut(i) =
                    self.link_redirect.bucket6_slot_and_prefetch(h) as u32;
            }
        }
        self.n_hits = nh;
        self.n_misses = nm;
    }

    #[inline(never)]
    fn rotate(&mut self) {
        if self.has_b6_compact {
            let prev = self.cur_buf ^ 1;
            if self.has_pending {
                self.emit_pending_fast(prev);
            }
            let cur = self.cur_buf;
            self.prep_batch_fast(cur, 64);
            self.has_pending = true;
            self.cur_buf = prev;
            self.cur_len = 0;
        } else {
            let cur = self.cur_buf;
            for j in 0..64 {
                let pk = self.ck_bufs[cur][j];
                let person = self.link_redirect.resolve(pk.uid_k);
                let ph = if person < B37_LIMIT {
                    mix42(person)
                } else {
                    person
                };
                emit_surv_click(
                    ph,
                    pk.cid_k,
                    pk.ts_rel,
                    pk.camp_idx,
                    self.use_compact_ts,
                    self.ck_init_elems,
                    self.ck_extra_elems,
                    self.win_start,
                    self.extra_ck_buckets,
                    self.ck_compact_staged,
                    self.ck_wide_staged,
                );
            }
            self.cur_len = 0;
        }
    }

    fn finish(mut self) {
        if self.has_b6_compact {
            if self.has_pending {
                self.emit_pending_fast(self.cur_buf ^ 1);
                self.has_pending = false;
            }
            if self.cur_len > 0 {
                let cur = self.cur_buf;
                let len = self.cur_len;
                self.prep_batch_fast(cur, len);
                self.emit_pending_fast(cur);
                self.cur_len = 0;
            }
        } else if self.cur_len > 0 {
            let cur = self.cur_buf;
            for j in 0..self.cur_len {
                let pk = self.ck_bufs[cur][j];
                let person = self.link_redirect.resolve(pk.uid_k);
                let ph = if person < B37_LIMIT {
                    mix42(person)
                } else {
                    person
                };
                emit_surv_click(
                    ph,
                    pk.cid_k,
                    pk.ts_rel,
                    pk.camp_idx,
                    self.use_compact_ts,
                    self.ck_init_elems,
                    self.ck_extra_elems,
                    self.win_start,
                    self.extra_ck_buckets,
                    self.ck_compact_staged,
                    self.ck_wide_staged,
                );
            }
            self.cur_len = 0;
        }
    }
}

#[cold]
#[inline(never)]
fn handle_event_row_rest<const COMPACT: bool>(
    eid: &[u8],
    eid_b37: Option<u64>,
    uid: &[u8],
    ts_b: &[u8],
    ing_val: i64,
    ing_is_rel_sec: bool,
    etype: &[u8],
    pid: &[u8],
    price_b: &[u8],
    date_cache: &FastDateCache,
    win_start: i64,
    win_len_u64: u64,
    products: &ProductIndex,
    overflow: &OverflowInterner,
    tid: usize,
    idx_bits: u32,
    extras: &mut Vec<ExtraEvent>,
    eid_staged: &mut NtSlabBuckets<RawEvent16, 4, NT_RAW_BLOCK>,
) {
    let ingest_rel_ms = if ing_is_rel_sec {
        ing_val * 1_000
    } else {
        ing_val - win_start
    };

    let mut valid = false;
    let mut valid_ts_rel = 0i64;
    let mut valid_ts_rel_sec = 0i64;
    let mut price_cents = 0u32;
    let mut cat_id = 0u32;

    if !uid.is_empty() && !pid.is_empty() {
        if etype.len() == 4 {
            let tag = unsafe { (etype.as_ptr() as *const u32).read_unaligned() };
            if (tag == VIEW_U32) | (tag == CART_U32) {
                if let Some((ts_rel, ts_rel_sec)) = parse_ts_rel_ms_and_sec(ts_b, date_cache) {
                    if (ts_rel as u64) < win_len_u64 && ingest_rel_ms - ts_rel <= LATE_LIMIT_MS {
                        valid = true;
                        valid_ts_rel = ts_rel;
                        valid_ts_rel_sec = ts_rel_sec;
                    }
                }
            }
        } else if etype.len() == 8
            && unsafe { (etype.as_ptr() as *const u64).read_unaligned() } == PURCH_U64
        {
            if let Some(p) = parse_price_cents(price_b) {
                if let Some((ts_rel, ts_rel_sec)) = parse_ts_rel_ms_and_sec(ts_b, date_cache) {
                    if (ts_rel as u64) < win_len_u64 && ingest_rel_ms - ts_rel <= LATE_LIMIT_MS {
                        valid = true;
                        valid_ts_rel = ts_rel;
                        valid_ts_rel_sec = ts_rel_sec;
                        price_cents = p;
                        cat_id = products.lookup(pid);
                    }
                }
            }
        }
    }

    let uid_k = if valid { overflow.encode_user(uid) } else { 0 };
    let (ingest_abs, tomb_ing_b, ing_rel_sec_opt) = if ing_is_rel_sec {
        let abs = win_start + ing_val * 1_000;
        let tb = tomb_ing_biased_from_rel_sec(ing_val);
        (abs, tb, Some(ing_val))
    } else {
        let tb = inline_tomb_ingest_biased(ing_val, win_start);
        let ir = if tb != 0 {
            Some((tb as i64) - TOMB_ING_BIAS)
        } else {
            None
        };
        (ing_val, tb, ir)
    };

    if COMPACT && valid && price_cents == 0 && uid_k < B37_LIMIT {
        if let (Some(eid_42), Some(ing_sec_v)) = (eid_b37, ing_rel_sec_opt) {
            let ing_delta_b = (ing_sec_v - valid_ts_rel_sec) + ING_DELTA_BIAS;
            if ing_delta_b >= 1 && ing_delta_b <= ING_DELTA_MAX {
                let h = mix42(eid_42);
                let eb = (h as usize) & EID_MASK;
                let lo = (((h >> EID_BITS) as u32 as u64) << 32) | (valid_ts_rel as u64);
                let hi = uid_k | ((ing_delta_b as u64) << 45);
                eid_staged.push(eb, RawEvent16 { lo, hi });
                return;
            }
        }
    }

    push_raw_fallback(
        eid,
        eid_b37,
        uid_k,
        valid,
        win_start + valid_ts_rel,
        ingest_abs,
        tomb_ing_b,
        price_cents,
        cat_id,
        overflow,
        tid,
        idx_bits,
        extras,
        eid_staged,
    );
}

#[cold]
#[inline(never)]
fn handle_event_row_cold<const COMPACT: bool>(
    eid: &[u8],
    uid: &[u8],
    ts_b: &[u8],
    ingest_b: &[u8],
    etype: &[u8],
    pid: &[u8],
    price_b: &[u8],
    date_cache: &FastDateCache,
    win_start: i64,
    win_len_u64: u64,
    products: &ProductIndex,
    overflow: &OverflowInterner,
    tid: usize,
    idx_bits: u32,
    extras: &mut Vec<ExtraEvent>,
    eid_staged: &mut NtSlabBuckets<RawEvent16, 4, NT_RAW_BLOCK>,
) {
    let Some((ing_val, ing_is_rel_sec)) = parse_ingest_rel_sec(ingest_b, date_cache) else {
        return;
    };
    let eid_b37 = pack_b37(eid, b'e');
    handle_event_row_rest::<COMPACT>(
        eid,
        eid_b37,
        uid,
        ts_b,
        ing_val,
        ing_is_rel_sec,
        etype,
        pid,
        price_b,
        date_cache,
        win_start,
        win_len_u64,
        products,
        overflow,
        tid,
        idx_bits,
        extras,
        eid_staged,
    );
}

#[inline(always)]
fn handle_event_row<const COMPACT: bool>(
    &[eid, uid, ts_b, ingest_b, etype, pid, price_b]: &[&[u8]; 7],
    date_cache: &FastDateCache,
    win_start: i64,
    win_len_u64: u64,
    products: &ProductIndex,
    overflow: &OverflowInterner,
    tid: usize,
    idx_bits: u32,
    extras: &mut Vec<ExtraEvent>,
    eid_staged: &mut NtSlabBuckets<RawEvent16, 4, NT_RAW_BLOCK>,
) {
    if eid.is_empty() {
        return;
    }
    let eid_b37 = pack_b37(eid, b'e');
    if let Some(ing_sec) = parse_ingest_fast_sec(ingest_b, date_cache) {
        if COMPACT {
            if let Some(eid_42) = eid_b37 {
                if !uid.is_empty() && !pid.is_empty() && etype.len() == 4 {
                    let tag = unsafe { (etype.as_ptr() as *const u32).read_unaligned() };
                    if (tag == VIEW_U32) | (tag == CART_U32) {
                        if let Some((ts_rel, ts_rel_sec)) = parse_ts_rel_ms_and_sec(ts_b, date_cache)
                        {
                            let ingest_rel_ms = (ing_sec as i64) * 1_000;
                            if (ts_rel as u64) < win_len_u64
                                && ingest_rel_ms - ts_rel <= LATE_LIMIT_MS
                            {
                                let ing_delta_b = (ing_sec as i64 - ts_rel_sec) + ING_DELTA_BIAS;
                                if ing_delta_b >= 1 && ing_delta_b <= ING_DELTA_MAX {
                                    if let Some(uid_42) = pack_b37(uid, b'u') {
                                        let h = mix42(eid_42);
                                        let eb = (h as usize) & EID_MASK;
                                        eid_staged.push(
                                            eb,
                                            RawEvent16 {
                                                lo: (((h >> EID_BITS) as u32 as u64) << 32)
                                                    | (ts_rel as u64),
                                                hi: uid_42 | ((ing_delta_b as u64) << 45),
                                            },
                                        );
                                        return;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        handle_event_row_rest::<COMPACT>(
            eid,
            eid_b37,
            uid,
            ts_b,
            ing_sec as i64,
            true,
            etype,
            pid,
            price_b,
            date_cache,
            win_start,
            win_len_u64,
            products,
            overflow,
            tid,
            idx_bits,
            extras,
            eid_staged,
        );
        return;
    }
    handle_event_row_cold::<COMPACT>(
        eid,
        uid,
        ts_b,
        ingest_b,
        etype,
        pid,
        price_b,
        date_cache,
        win_start,
        win_len_u64,
        products,
        overflow,
        tid,
        idx_bits,
        extras,
        eid_staged,
    );
}

#[inline(always)]
fn handle_click_row(
    &[cid, uid, camp_b, ts_b]: &[&[u8]; 4],
    campaigns: &CampaignIndex,
    date_cache: &FastDateCache,
    win_len_i64: i64,
    overflow: &OverflowInterner,
    links_empty: bool,
    resolver: &mut ClickResolver<'_>,
) {
    if cid.is_empty() || uid.is_empty() {
        return;
    }
    let Some(camp_idx) = campaigns.lookup(camp_b) else {
        return;
    };
    let Some(ts_rel) = parse_ts_rel_ms(ts_b, date_cache) else {
        return;
    };
    if ts_rel < -CLICK_REACH_MS || ts_rel >= win_len_i64 {
        return;
    }
    let uid_k = overflow.encode_user(uid);
    let cid_k = overflow.encode_click(cid);
    if links_empty {
        let ph = if uid_k < B37_LIMIT { mix42(uid_k) } else { uid_k };
        emit_surv_click(
            ph,
            cid_k,
            ts_rel,
            camp_idx,
            resolver.use_compact_ts,
            resolver.ck_init_elems,
            resolver.ck_extra_elems,
            resolver.win_start,
            resolver.extra_ck_buckets,
            resolver.ck_compact_staged,
            resolver.ck_wide_staged,
        );
    } else {
        resolver.push(PendingClick {
            uid_k,
            cid_k,
            ts_rel,
            camp_idx,
        });
    }
}

struct OpenCsvFile {
    len: usize,
    file: File,
}

struct OpenedInputs {
    event_files: Vec<OpenCsvFile>,
    click_files: Vec<OpenCsvFile>,
    event_jobs: Vec<ChunkJob>,
    click_jobs: Vec<ChunkJob>,
    total_event_bytes: u64,
    total_click_bytes: u64,
}

fn build_interleaved_file_jobs(paths: &[PathBuf]) -> (Vec<OpenCsvFile>, Vec<ChunkJob>, u64) {
    let mut pairs: Vec<(usize, OpenCsvFile)> = Vec::with_capacity(paths.len());
    let mut total_bytes = 0u64;
    for p in paths {
        let Ok(f) = File::open(p) else {
            continue;
        };
        let Ok(meta) = f.metadata() else {
            continue;
        };
        let len = meta.len() as usize;
        if len == 0 {
            continue;
        }
        fadvise_sequential(&f);
        let entry = OpenCsvFile { len, file: f };
        total_bytes += len as u64;
        pairs.push((len, entry));
    }
    pairs.sort_unstable_by(|a, b| b.0.cmp(&a.0));
    let files: Vec<OpenCsvFile> = pairs.into_iter().map(|x| x.1).collect();

    let mut jobs: Vec<ChunkJob> = Vec::new();
    for (fidx, f) in files.iter().enumerate() {
        let mut off = 0usize;
        while off < f.len {
            let end = (off + IO_CHUNK_BYTES).min(f.len);
            jobs.push(ChunkJob {
                fidx,
                start: off,
                end,
            });
            off = end;
        }
    }
    (files, jobs, total_bytes)
}

fn open_inputs_and_jobs(event_files: &[PathBuf], click_files: &[PathBuf]) -> OpenedInputs {
    let (ev_open, event_jobs, total_event_bytes) = build_interleaved_file_jobs(event_files);
    let (ck_open, click_jobs, total_click_bytes) = build_interleaved_file_jobs(click_files);
    OpenedInputs {
        event_files: ev_open,
        click_files: ck_open,
        event_jobs,
        click_jobs,
        total_event_bytes,
        total_click_bytes,
    }
}

#[inline(always)]
fn scan_open_file_sink<const N: usize, S: CsvRowSink<N>>(
    open_file: &OpenCsvFile,
    start: usize,
    end: usize,
    buf: &mut Vec<u8>,
    sink: &mut S,
) {
    let (got, ls, le) = pread_csv_chunk(&open_file.file, open_file.len, start, end, buf);
    if got > 0 {
        scan_csv_mmap_chunk_sink::<N, S>(&buf[..got], ls, le, sink);
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[cold]
#[inline(never)]
fn fallback_event_line_compact(
    data: &[u8],
    line_start: usize,
    stop_excl: usize,
    date_cache: &FastDateCache,
    win_start: i64,
    win_len_u64: u64,
    products: &ProductIndex,
    overflow: &OverflowInterner,
    tid: usize,
    idx_bits: u32,
    extras: &mut Vec<ExtraEvent>,
    eid_staged: &mut NtSlabBuckets<RawEvent16, 4, NT_RAW_BLOCK>,
) -> usize {
    let rem_slice = &data[line_start..stop_excl];
    if let Some(rel) = memchr::memchr(b'\n', rem_slice) {
        if let Some(row) = parse_csv_line::<7>(&rem_slice[..rel]) {
            handle_event_row::<true>(
                &row,
                date_cache,
                win_start,
                win_len_u64,
                products,
                overflow,
                tid,
                idx_bits,
                extras,
                eid_staged,
            );
        }
        line_start + rel + 1
    } else {
        if !rem_slice.is_empty() {
            if let Some(row) = parse_csv_line::<7>(rem_slice) {
                handle_event_row::<true>(
                    &row,
                    date_cache,
                    win_start,
                    win_len_u64,
                    products,
                    overflow,
                    tid,
                    idx_bits,
                    extras,
                    eid_staged,
                );
            }
        }
        stop_excl
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[derive(Clone, Copy)]
struct FastParsedEv {
    eid_p: *const u8,
    is9_0: usize,
    uid_p: *const u8,
    is9_1: usize,
    ts_rel: u64,
    ing_delta_b: u64,
    next_cur: usize,
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[derive(Clone, Copy)]
enum FastEvRes {
    Fast(FastParsedEv),
    InlineTomb {
        eid_p: *const u8,
        is9_0: usize,
        uid_p: *const u8,
        is9_1: usize,
        ing_sec: i64,
        next_cur: usize,
    },
    InlinePurch {
        eid_p: *const u8,
        is9_0: usize,
        uid_p: *const u8,
        is9_1: usize,
        ts_rel: i64,
        ing_sec: i64,
        price_cents: u32,
        cat_id: u32,
        next_cur: usize,
    },
    Fallback,
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
unsafe fn validate_tail_simd(
    p: *const u8,
    v_nl: std::arch::x86_64::__m256i,
    v_cm: std::arch::x86_64::__m256i,
    v_qt: std::arch::x86_64::__m256i,
) -> Option<(u32, u32, u32, u32)> {
    use std::arch::x86_64::*;
    let v16 = _mm_loadu_si128(p as *const __m128i);
    let m_nl16 = _mm_movemask_epi8(_mm_cmpeq_epi8(v16, _mm256_castsi256_si128(v_nl))) as u32;
    let (nl_pos, m_cm, m_qt) = if m_nl16 != 0 {
        let nl_pos = m_nl16.trailing_zeros();
        let line_mask = (1u32 << nl_pos) - 1;
        let cm =
            (_mm_movemask_epi8(_mm_cmpeq_epi8(v16, _mm256_castsi256_si128(v_cm))) as u32)
                & line_mask;
        let qt =
            (_mm_movemask_epi8(_mm_cmpeq_epi8(v16, _mm256_castsi256_si128(v_qt))) as u32)
                & line_mask;
        (nl_pos, cm, qt)
    } else {
        let v = _mm256_loadu_si256(p as *const __m256i);
        let m_nl = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, v_nl)) as u32;
        if m_nl == 0 {
            return None;
        }
        let nl_pos = m_nl.trailing_zeros();
        let line_mask = (1u32 << nl_pos) - 1;
        let cm = (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, v_cm)) as u32) & line_mask;
        let qt = (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, v_qt)) as u32) & line_mask;
        (nl_pos, cm, qt)
    };
    if m_cm == 0 || (m_cm & (m_cm - 1)) != 0 {
        return None;
    }
    let cm_pos = m_cm.trailing_zeros();
    if m_qt == 0 {
        if cm_pos < 1 {
            return None;
        }
        return Some((nl_pos, cm_pos, 0, 0));
    }
    let q5 = m_qt & ((1u32 << cm_pos) - 1);
    let exp5 = 1u32 | (1u32 << (cm_pos.wrapping_sub(1) & 31));
    let ok5 = (cm_pos >= 1) & ((q5 == 0) | ((cm_pos >= 3) & (q5 == exp5)));
    let q6 = m_qt >> (cm_pos + 1);
    let len6 = nl_pos - cm_pos - 1;
    let exp6 = 1u32 | (1u32 << (len6.wrapping_sub(1) & 31));
    let ok6 = (q6 == 0) | ((len6 >= 2) & (q6 == exp6));
    if !(ok5 & ok6) {
        return None;
    }
    Some((nl_pos, cm_pos, (q5 != 0) as u32, (q6 != 0) as u32))
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
unsafe fn parse_one_ev_res(
    base: *const u8,
    cur: usize,
    date_cache: &FastDateCache,
    win_len_u64: u64,
    products: &ProductIndex,
    v_nl: std::arch::x86_64::__m256i,
    v_cm: std::arch::x86_64::__m256i,
    v_qt: std::arch::x86_64::__m256i,
) -> FastEvRes {
    let mut p = base.add(cur);
    let q0 = usize::from(*p == b'"');
    p = p.add(q0);
    let eid_p = p;
    let is9_0 = usize::from(*p.add(8) >= b'0');
    let len0 = 8 + is9_0;
    let ok0 = (*p == b'e') & (*p.add(len0 + q0) == b',') & ((q0 == 0) | (*p.add(len0) == b'"'));
    p = p.add(len0 + q0 + 1);

    let q1 = usize::from(*p == b'"');
    p = p.add(q1);
    let uid_p = p;
    let is9_1 = usize::from(*p.add(8) >= b'0');
    let len1 = 8 + is9_1;
    let ok1 = (*p == b'u') & (*p.add(len1 + q1) == b',') & ((q1 == 0) | (*p.add(len1) == b'"'));
    p = p.add(len1 + q1 + 1);
    if !(ok0 & ok1) {
        return FastEvRes::Fallback;
    }

    let q2 = usize::from(*p == b'"');
    p = p.add(q2);
    let ts_rel: i64;
    let ts_rel_sec: i64;
    let ing_sec: i64;
    let mut same_f1_day = false;

    if *p.add(19) == b'Z' && *p.add(20 + q2) == b',' && (q2 == 0 || *p.add(20) == b'"') {
        let ts_w0 = (p as *const u64).read_unaligned().to_le();
        let ts_ddt = (p.add(8) as *const u32).read_unaligned().to_le() & 0x00FF_FFFF;
        let ts_whms = (p.add(11) as *const u64).read_unaligned().to_le();
        let Some(ts_hms) = parse_hms_swar(ts_whms) else {
            return FastEvRes::Fallback;
        };
        let Some(ts_day) = resolve_iso_day_rel_sec(ts_w0, ts_ddt, date_cache) else {
            return FastEvRes::Fallback;
        };
        p = p.add(21 + q2);

        let q3 = usize::from(*p == b'"');
        p = p.add(q3);
        if !((*p.add(19) == b'Z') & (*p.add(20 + q3) == b',') & ((q3 == 0) | (*p.add(20) == b'"')))
        {
            return FastEvRes::Fallback;
        }
        let ing_w0 = (p as *const u64).read_unaligned().to_le();
        let ing_ddt = (p.add(8) as *const u32).read_unaligned().to_le() & 0x00FF_FFFF;
        let ing_whms = (p.add(11) as *const u64).read_unaligned().to_le();
        let Some(ing_hms) = parse_hms_swar(ing_whms) else {
            return FastEvRes::Fallback;
        };
        let ing_day = if ing_w0 == ts_w0 && ing_ddt == ts_ddt {
            same_f1_day = true;
            ts_day
        } else {
            let Some(d) = resolve_iso_day_rel_sec(ing_w0, ing_ddt, date_cache) else {
                return FastEvRes::Fallback;
            };
            d
        };
        p = p.add(21 + q3);
        ts_rel_sec = ts_day + ts_hms;
        ts_rel = ts_rel_sec * 1_000;
        ing_sec = ing_day + ing_hms;
    } else if *p.add(13 + q2) == b',' && (q2 == 0 || *p.add(13) == b'"') {
        let Some((tr, trs)) = parse_f3_13_ptr(p, date_cache) else {
            return FastEvRes::Fallback;
        };
        p = p.add(14 + q2);

        let q3 = usize::from(*p == b'"');
        p = p.add(q3);
        if !((*p.add(19) == b'Z') & (*p.add(20 + q3) == b',') & ((q3 == 0) | (*p.add(20) == b'"')))
        {
            return FastEvRes::Fallback;
        }
        let ing_w0 = (p as *const u64).read_unaligned().to_le();
        let ing_ddt = (p.add(8) as *const u32).read_unaligned().to_le() & 0x00FF_FFFF;
        let ing_whms = (p.add(11) as *const u64).read_unaligned().to_le();
        let Some(ing_hms) = parse_hms_swar(ing_whms) else {
            return FastEvRes::Fallback;
        };
        let Some(ing_day) = resolve_iso_day_rel_sec(ing_w0, ing_ddt, date_cache) else {
            return FastEvRes::Fallback;
        };
        p = p.add(21 + q3);
        ts_rel = tr;
        ts_rel_sec = trs;
        ing_sec = ing_day + ing_hms;
    } else if (*p.add(19) == b'+' || *p.add(19) == b'-')
        && *p.add(22) == b':'
        && *p.add(25 + q2) == b','
        && (q2 == 0 || *p.add(25) == b'"')
    {
        let Some(oh) = parse_d2(*p.add(20), *p.add(21)) else {
            return FastEvRes::Fallback;
        };
        let Some(om) = parse_d2(*p.add(23), *p.add(24)) else {
            return FastEvRes::Fallback;
        };
        if oh > 23 || om > 59 {
            return FastEvRes::Fallback;
        }
        let sign = if *p.add(19) == b'+' { -1i64 } else { 1i64 };
        let off_sec = sign * (oh * 3_600 + om * 60);
        let ts_w0 = (p as *const u64).read_unaligned().to_le();
        let ts_ddt = (p.add(8) as *const u32).read_unaligned().to_le() & 0x00FF_FFFF;
        let ts_whms = (p.add(11) as *const u64).read_unaligned().to_le();
        let Some(ts_hms) = parse_hms_swar(ts_whms) else {
            return FastEvRes::Fallback;
        };
        let Some(ts_day) = resolve_iso_day_rel_sec(ts_w0, ts_ddt, date_cache) else {
            return FastEvRes::Fallback;
        };
        p = p.add(26 + q2);

        let q3 = usize::from(*p == b'"');
        p = p.add(q3);
        if !((*p.add(19) == b'Z') & (*p.add(20 + q3) == b',') & ((q3 == 0) | (*p.add(20) == b'"')))
        {
            return FastEvRes::Fallback;
        }
        let ing_w0 = (p as *const u64).read_unaligned().to_le();
        let ing_ddt = (p.add(8) as *const u32).read_unaligned().to_le() & 0x00FF_FFFF;
        let ing_whms = (p.add(11) as *const u64).read_unaligned().to_le();
        let Some(ing_hms) = parse_hms_swar(ing_whms) else {
            return FastEvRes::Fallback;
        };
        let ing_day = if ing_w0 == ts_w0 && ing_ddt == ts_ddt {
            ts_day
        } else {
            let Some(d) = resolve_iso_day_rel_sec(ing_w0, ing_ddt, date_cache) else {
                return FastEvRes::Fallback;
            };
            d
        };
        p = p.add(21 + q3);
        ts_rel_sec = ts_day + ts_hms + off_sec;
        ts_rel = ts_rel_sec * 1_000;
        ing_sec = ing_day + ing_hms;
    } else {
        return FastEvRes::Fallback;
    }

    let q4 = usize::from(*p == b'"');
    p = p.add(q4);
    let tag = (p as *const u32).read_unaligned();
    let is_vc = ((tag == VIEW_U32) | (tag == CART_U32))
        & (*p.add(4 + q4) == b',')
        & ((q4 == 0) | (*p.add(4) == b'"'));
    let is_purch = !is_vc
        && ((p as *const u64).read_unaligned() == PURCH_U64)
        && (*p.add(8 + q4) == b',')
        && ((q4 == 0) | (*p.add(8) == b'"'));
    if !is_vc && !is_purch {
        return FastEvRes::Fallback;
    }
    p = p.add(if is_vc { 5 + q4 } else { 9 + q4 });

    let Some((nl_pos, cm_pos, pq5, pq6)) = validate_tail_simd(p, v_nl, v_cm, v_qt) else {
        return FastEvRes::Fallback;
    };
    let next_cur = (p.offset_from(base) as usize) + (nl_pos as usize) + 1;

    let in_win = (ts_rel as u64) < win_len_u64;
    let timely = same_f1_day || (ing_sec * 1_000 - ts_rel <= LATE_LIMIT_MS);
    if !in_win || !timely {
        return FastEvRes::InlineTomb {
            eid_p,
            is9_0,
            uid_p,
            is9_1,
            ing_sec,
            next_cur,
        };
    }

    if is_vc {
        let ing_delta_b = (ing_sec - ts_rel_sec) + ING_DELTA_BIAS;
        if ing_delta_b >= 1 && ing_delta_b <= ING_DELTA_MAX {
            return FastEvRes::Fast(FastParsedEv {
                eid_p,
                is9_0,
                uid_p,
                is9_1,
                ts_rel: ts_rel as u64,
                ing_delta_b: ing_delta_b as u64,
                next_cur,
            });
        }
        return FastEvRes::Fallback;
    }

    let pid_slice = std::slice::from_raw_parts(p.add(pq5 as usize), (cm_pos - (pq5 << 1)) as usize);
    let len6 = nl_pos - cm_pos - 1;
    let price_slice = std::slice::from_raw_parts(
        p.add((cm_pos + 1 + pq6) as usize),
        (len6 - (pq6 << 1)) as usize,
    );
    let Some(price_cents) = parse_price_cents(price_slice) else {
        return FastEvRes::InlineTomb {
            eid_p,
            is9_0,
            uid_p,
            is9_1,
            ing_sec,
            next_cur,
        };
    };
    let cat_id = products.lookup(pid_slice);
    FastEvRes::InlinePurch {
        eid_p,
        is9_0,
        uid_p,
        is9_1,
        ts_rel,
        ing_sec,
        price_cents,
        cat_id,
        next_cur,
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
unsafe fn handle_single_fast_res(
    res: FastEvRes,
    data: &[u8],
    cur: usize,
    lim: usize,
    date_cache: &FastDateCache,
    win_start: i64,
    win_len_u64: u64,
    products: &ProductIndex,
    overflow: &OverflowInterner,
    tid: usize,
    idx_bits: u32,
    extras: &mut Vec<ExtraEvent>,
    eid_staged: &mut NtSlabBuckets<RawEvent16, 4, NT_RAW_BLOCK>,
) -> usize {
    match res {
        FastEvRes::Fast(ev) => {
            if let Some((e, u)) = pack_b37_pair_89(ev.eid_p, ev.is9_0, ev.uid_p, ev.is9_1) {
                let h = mix42(e);
                eid_staged.push(
                    (h as usize) & EID_MASK,
                    RawEvent16 {
                        lo: (((h >> EID_BITS) as u32 as u64) << 32) | ev.ts_rel,
                        hi: u | (ev.ing_delta_b << 45),
                    },
                );
                return ev.next_cur;
            }
        }
        FastEvRes::InlineTomb {
            eid_p,
            is9_0,
            uid_p,
            is9_1,
            ing_sec,
            next_cur,
        } => {
            let tomb_ing_b = tomb_ing_biased_from_rel_sec(ing_sec);
            if tomb_ing_b != 0 {
                if let Some((eid_42, _)) = pack_b37_pair_89(eid_p, is9_0, uid_p, is9_1) {
                    let h = mix42(eid_42);
                    eid_staged.push(
                        (h as usize) & EID_MASK,
                        RawEvent16 {
                            lo: (((h >> EID_BITS) as u32 as u64) << 32)
                                | RAW_SPECIAL_BIT
                                | tomb_ing_b,
                            hi: 0,
                        },
                    );
                    return next_cur;
                }
            }
        }
        FastEvRes::InlinePurch {
            eid_p,
            is9_0,
            uid_p,
            is9_1,
            ts_rel,
            ing_sec,
            price_cents,
            cat_id,
            next_cur,
        } => {
            let tomb_ing_b = tomb_ing_biased_from_rel_sec(ing_sec);
            if let Some((eid_42, uid_42)) = pack_b37_pair_89(eid_p, is9_0, uid_p, is9_1) {
                let h = mix42(eid_42);
                let eidx = extras.len();
                extras.push(ExtraEvent {
                    eid_k: eid_42,
                    uid_k: uid_42,
                    ts: win_start + ts_rel,
                    ingest_ts: win_start + ing_sec * 1_000,
                    price_cents,
                    cat_id,
                    valid: true,
                });
                let eref = ((tid as u64) << idx_bits) | (eidx as u64);
                eid_staged.push(
                    (h as usize) & EID_MASK,
                    RawEvent16 {
                        lo: (((h >> EID_BITS) as u32 as u64) << 32)
                            | RAW_SPECIAL_BIT
                            | tomb_ing_b,
                        hi: eref + 1,
                    },
                );
                return next_cur;
            }
        }
        FastEvRes::Fallback => {}
    }
    fallback_event_line_compact(
        data,
        cur,
        lim,
        date_cache,
        win_start,
        win_len_u64,
        products,
        overflow,
        tid,
        idx_bits,
        extras,
        eid_staged,
    )
}

/// Two-stream interleaved direct single-pass events CSV scanner with 256-bit AVX2
/// `pack_b37_quad_89` for `view`/`cart` fast rows, inline `purchase`/tombstone handling,
/// and exact line-by-line fallback.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(never)]
unsafe fn scan_events_direct_compact(
    data: &[u8],
    start: usize,
    end: usize,
    date_cache: &FastDateCache,
    win_start: i64,
    win_len_u64: u64,
    products: &ProductIndex,
    overflow: &OverflowInterner,
    tid: usize,
    idx_bits: u32,
    extras: &mut Vec<ExtraEvent>,
    eid_staged: &mut NtSlabBuckets<RawEvent16, 4, NT_RAW_BLOCK>,
) {
    use std::arch::x86_64::*;

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
    let v_nl = _mm256_set1_epi8(b'\n' as i8);
    let v_cm = _mm256_set1_epi8(b',' as i8);
    let v_qt = _mm256_set1_epi8(b'"' as i8);

    let span = stop_excl - start_pos;
    if span >= 4096 {
        let mid_guess = start_pos + (span >> 1);
        if let Some(mrel) = memchr::memchr(b'\n', &data[mid_guess..stop_excl]) {
            let mid = mid_guess + mrel + 1;
            let mut cur_a = start_pos;
            let mut cur_b = mid;
            let safe_a = mid.saturating_sub(144);
            let safe_b = stop_excl.saturating_sub(144);
            while cur_a < safe_a && cur_b < safe_b {
                let oa = parse_one_ev_res(
                    base,
                    cur_a,
                    date_cache,
                    win_len_u64,
                    products,
                    v_nl,
                    v_cm,
                    v_qt,
                );
                let ob = parse_one_ev_res(
                    base,
                    cur_b,
                    date_cache,
                    win_len_u64,
                    products,
                    v_nl,
                    v_cm,
                    v_qt,
                );
                if let (FastEvRes::Fast(a), FastEvRes::Fast(b)) = (oa, ob) {
                    if let Some((ea, ua, eb, ub)) = crate::pack::pack_b37_quad_89(
                        a.eid_p, a.is9_0, a.uid_p, a.is9_1, b.eid_p, b.is9_0, b.uid_p, b.is9_1,
                    ) {
                        let ha = mix42(ea);
                        let hb = mix42(eb);
                        eid_staged.push(
                            (ha as usize) & EID_MASK,
                            RawEvent16 {
                                lo: (((ha >> EID_BITS) as u32 as u64) << 32) | a.ts_rel,
                                hi: ua | (a.ing_delta_b << 45),
                            },
                        );
                        eid_staged.push(
                            (hb as usize) & EID_MASK,
                            RawEvent16 {
                                lo: (((hb >> EID_BITS) as u32 as u64) << 32) | b.ts_rel,
                                hi: ub | (b.ing_delta_b << 45),
                            },
                        );
                        cur_a = a.next_cur;
                        cur_b = b.next_cur;
                        continue;
                    }
                }
                cur_a = handle_single_fast_res(
                    oa,
                    data,
                    cur_a,
                    mid,
                    date_cache,
                    win_start,
                    win_len_u64,
                    products,
                    overflow,
                    tid,
                    idx_bits,
                    extras,
                    eid_staged,
                );
                cur_b = handle_single_fast_res(
                    ob,
                    data,
                    cur_b,
                    stop_excl,
                    date_cache,
                    win_start,
                    win_len_u64,
                    products,
                    overflow,
                    tid,
                    idx_bits,
                    extras,
                    eid_staged,
                );
            }
            for (mut cur, lim) in [(cur_a, mid), (cur_b, stop_excl)] {
                let sf = lim.saturating_sub(144);
                while cur < sf {
                    let res = parse_one_ev_res(
                        base,
                        cur,
                        date_cache,
                        win_len_u64,
                        products,
                        v_nl,
                        v_cm,
                        v_qt,
                    );
                    cur = handle_single_fast_res(
                        res,
                        data,
                        cur,
                        lim,
                        date_cache,
                        win_start,
                        win_len_u64,
                        products,
                        overflow,
                        tid,
                        idx_bits,
                        extras,
                        eid_staged,
                    );
                }
                while cur < lim {
                    cur = fallback_event_line_compact(
                        data,
                        cur,
                        lim,
                        date_cache,
                        win_start,
                        win_len_u64,
                        products,
                        overflow,
                        tid,
                        idx_bits,
                        extras,
                        eid_staged,
                    );
                }
            }
            return;
        }
    }
    let mut cur = start_pos;
    let sf = stop_excl.saturating_sub(144);
    while cur < sf {
        let res = parse_one_ev_res(
            base,
            cur,
            date_cache,
            win_len_u64,
            products,
            v_nl,
            v_cm,
            v_qt,
        );
        cur = handle_single_fast_res(
            res,
            data,
            cur,
            stop_excl,
            date_cache,
            win_start,
            win_len_u64,
            products,
            overflow,
            tid,
            idx_bits,
            extras,
            eid_staged,
        );
    }
    while cur < stop_excl {
        cur = fallback_event_line_compact(
            data,
            cur,
            stop_excl,
            date_cache,
            win_start,
            win_len_u64,
            products,
            overflow,
            tid,
            idx_bits,
            extras,
            eid_staged,
        );
    }
}

#[inline(always)]
fn drain_event_jobs(
    next_ev_job: &AtomicUsize,
    event_jobs: &[ChunkJob],
    event_files: &[OpenCsvFile],
    date_cache: &FastDateCache,
    win_start: i64,
    win_len_u64: u64,
    use_compact_ts: bool,
    products: &ProductIndex,
    overflow: &OverflowInterner,
    tid: usize,
    idx_bits: u32,
    extras: &mut Vec<ExtraEvent>,
    eid_staged: &mut NtSlabBuckets<RawEvent16, 4, NT_RAW_BLOCK>,
) {
    let mut io_buf: Vec<u8> = vec![0u8; IO_CHUNK_BYTES + 32_768];
    loop {
        let jid = next_ev_job.fetch_add(1, MemOrdering::Relaxed);
        let Some(job) = event_jobs.get(jid) else {
            break;
        };
        let open_file = &event_files[job.fidx];
        if use_compact_ts && date_cache.win_start_whole_sec {
            #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
            {
                let (got, ls, le) =
                    pread_csv_chunk(&open_file.file, open_file.len, job.start, job.end, &mut io_buf);
                if got > 0 {
                    unsafe {
                        scan_events_direct_compact(
                            &io_buf[..got],
                            ls,
                            le,
                            date_cache,
                            win_start,
                            win_len_u64,
                            products,
                            overflow,
                            tid,
                            idx_bits,
                            extras,
                            eid_staged,
                        );
                    }
                }
                continue;
            }
        }
        if use_compact_ts {
            struct EvSink<'a> {
                date_cache: &'a FastDateCache,
                win_start: i64,
                win_len_u64: u64,
                products: &'a ProductIndex,
                overflow: &'a OverflowInterner,
                tid: usize,
                idx_bits: u32,
                extras: &'a mut Vec<ExtraEvent>,
                eid_staged: &'a mut NtSlabBuckets<RawEvent16, 4, NT_RAW_BLOCK>,
            }
            impl CsvRowSink<7> for EvSink<'_> {
                #[inline(always)]
                unsafe fn on_pos(&mut self, ptr: *const u8, s0: usize, rpos: *const u16) {
                    let e0 = *rpos as usize;
                    let q0 = usize::from(*ptr.add(s0) == b'"');
                    let eid_len = (e0 - s0) - (q0 << 1);
                    if eid_len == 0 {
                        return;
                    }
                    let eid = std::slice::from_raw_parts(ptr.add(s0 + q0), eid_len);

                    let e1 = *rpos.add(1) as usize;
                    let e2 = *rpos.add(2) as usize;
                    let e3 = *rpos.add(3) as usize;
                    let s3 = e2 + 1;
                    let q3 = usize::from(*ptr.add(s3) == b'"');
                    let ingest_b =
                        std::slice::from_raw_parts(ptr.add(s3 + q3), (e3 - s3) - (q3 << 1));

                    if let Some(ing_sec) = parse_ingest_fast_sec(ingest_b, self.date_cache) {
                        let e4 = *rpos.add(4) as usize;
                        let s4 = e3 + 1;
                        let q4 = usize::from(*ptr.add(s4) == b'"');
                        let et_len = (e4 - s4) - (q4 << 1);
                        let et_ptr = ptr.add(s4 + q4);

                        let s1 = e0 + 1;
                        let q1 = usize::from(*ptr.add(s1) == b'"');
                        let uid_len = (e1 - s1) - (q1 << 1);

                        let e5 = *rpos.add(5) as usize;
                        let s5 = e4 + 1;
                        let pid_raw = e5 - s5;
                        let pid_ok = pid_raw >= 3
                            || (pid_raw > 0 && !(pid_raw == 2 && *ptr.add(s5) == b'"'));

                        if uid_len != 0 && pid_ok {
                            if et_len == 4 {
                                let tag = (et_ptr as *const u32).read_unaligned();
                                if (tag == VIEW_U32) | (tag == CART_U32) {
                                    let s2 = e1 + 1;
                                    let q2 = usize::from(*ptr.add(s2) == b'"');
                                    let ts_b = std::slice::from_raw_parts(
                                        ptr.add(s2 + q2),
                                        (e2 - s2) - (q2 << 1),
                                    );
                                    if let Some((ts_rel, ts_rel_sec)) =
                                        parse_ts_rel_ms_and_sec(ts_b, self.date_cache)
                                    {
                                        let ingest_rel_ms = (ing_sec as i64) * 1_000;
                                        if (ts_rel as u64) < self.win_len_u64
                                            && ingest_rel_ms - ts_rel <= LATE_LIMIT_MS
                                        {
                                            let uid = std::slice::from_raw_parts(
                                                ptr.add(s1 + q1),
                                                uid_len,
                                            );
                                            let ing_delta_b =
                                                (ing_sec as i64 - ts_rel_sec) + ING_DELTA_BIAS;
                                            if ing_delta_b >= 1 && ing_delta_b <= ING_DELTA_MAX {
                                                if let Some((eid_42, uid_42)) =
                                                    pack_b37_pair(eid, b'e', uid, b'u')
                                                {
                                                    let h = mix42(eid_42);
                                                    let eb = (h as usize) & EID_MASK;
                                                    self.eid_staged.push(
                                                        eb,
                                                        RawEvent16 {
                                                            lo: (((h >> EID_BITS) as u32 as u64)
                                                                << 32)
                                                                | (ts_rel as u64),
                                                            hi: uid_42
                                                                | ((ing_delta_b as u64) << 45),
                                                        },
                                                    );
                                                    return;
                                                }
                                            }
                                            let tomb_ing_b =
                                                tomb_ing_biased_from_rel_sec(ing_sec as i64);
                                            let eid_b37 = pack_b37(eid, b'e');
                                            let uid_k = self.overflow.encode_user(uid);
                                            push_raw_fallback(
                                                eid,
                                                eid_b37,
                                                uid_k,
                                                true,
                                                self.win_start + ts_rel,
                                                self.win_start + ingest_rel_ms,
                                                tomb_ing_b,
                                                0,
                                                0,
                                                self.overflow,
                                                self.tid,
                                                self.idx_bits,
                                                self.extras,
                                                self.eid_staged,
                                            );
                                            return;
                                        }
                                    }
                                }
                            } else if et_len == 8
                                && (et_ptr as *const u64).read_unaligned() == PURCH_U64
                            {
                                let uid = std::slice::from_raw_parts(ptr.add(s1 + q1), uid_len);
                                let s2 = e1 + 1;
                                let q2 = usize::from(*ptr.add(s2) == b'"');
                                let ts_b = std::slice::from_raw_parts(
                                    ptr.add(s2 + q2),
                                    (e2 - s2) - (q2 << 1),
                                );
                                let q5 = usize::from(*ptr.add(s5) == b'"');
                                let pid = std::slice::from_raw_parts(
                                    ptr.add(s5 + q5),
                                    pid_raw - (q5 << 1),
                                );
                                let e6 = *rpos.add(6) as usize;
                                let s6 = e5 + 1;
                                let q6 = usize::from(*ptr.add(s6) == b'"');
                                let price_b = std::slice::from_raw_parts(
                                    ptr.add(s6 + q6),
                                    (e6 - s6) - (q6 << 1),
                                );
                                handle_event_row_rest::<true>(
                                    eid,
                                    pack_b37(eid, b'e'),
                                    uid,
                                    ts_b,
                                    ing_sec as i64,
                                    true,
                                    std::slice::from_raw_parts(et_ptr, 8),
                                    pid,
                                    price_b,
                                    self.date_cache,
                                    self.win_start,
                                    self.win_len_u64,
                                    self.products,
                                    self.overflow,
                                    self.tid,
                                    self.idx_bits,
                                    self.extras,
                                    self.eid_staged,
                                );
                                return;
                            }
                        }

                        let tomb_ing_b = tomb_ing_biased_from_rel_sec(ing_sec as i64);
                        push_raw_fallback(
                            eid,
                            pack_b37(eid, b'e'),
                            0,
                            false,
                            0,
                            self.win_start + (ing_sec as i64) * 1_000,
                            tomb_ing_b,
                            0,
                            0,
                            self.overflow,
                            self.tid,
                            self.idx_bits,
                            self.extras,
                            self.eid_staged,
                        );
                        return;
                    }

                    let s1 = e0 + 1;
                    let q1 = usize::from(*ptr.add(s1) == b'"');
                    let uid = std::slice::from_raw_parts(ptr.add(s1 + q1), (e1 - s1) - (q1 << 1));
                    let s2 = e1 + 1;
                    let q2 = usize::from(*ptr.add(s2) == b'"');
                    let ts_b = std::slice::from_raw_parts(ptr.add(s2 + q2), (e2 - s2) - (q2 << 1));
                    let e4 = *rpos.add(4) as usize;
                    let s4 = e3 + 1;
                    let q4 = usize::from(*ptr.add(s4) == b'"');
                    let etype =
                        std::slice::from_raw_parts(ptr.add(s4 + q4), (e4 - s4) - (q4 << 1));
                    let e5 = *rpos.add(5) as usize;
                    let s5 = e4 + 1;
                    let q5 = usize::from(*ptr.add(s5) == b'"');
                    let pid = std::slice::from_raw_parts(ptr.add(s5 + q5), (e5 - s5) - (q5 << 1));
                    let e6 = *rpos.add(6) as usize;
                    let s6 = e5 + 1;
                    let q6 = usize::from(*ptr.add(s6) == b'"');
                    let price_b =
                        std::slice::from_raw_parts(ptr.add(s6 + q6), (e6 - s6) - (q6 << 1));
                    handle_event_row_cold::<true>(
                        eid,
                        uid,
                        ts_b,
                        ingest_b,
                        etype,
                        pid,
                        price_b,
                        self.date_cache,
                        self.win_start,
                        self.win_len_u64,
                        self.products,
                        self.overflow,
                        self.tid,
                        self.idx_bits,
                        self.extras,
                        self.eid_staged,
                    );
                }

                #[inline(always)]
                fn on_row(&mut self, row: &[&[u8]; 7]) {
                    handle_event_row::<true>(
                        row,
                        self.date_cache,
                        self.win_start,
                        self.win_len_u64,
                        self.products,
                        self.overflow,
                        self.tid,
                        self.idx_bits,
                        self.extras,
                        self.eid_staged,
                    );
                }
            }
            let mut sink = EvSink {
                date_cache,
                win_start,
                win_len_u64,
                products,
                overflow,
                tid,
                idx_bits,
                extras,
                eid_staged,
            };
            scan_open_file_sink::<7, _>(open_file, job.start, job.end, &mut io_buf, &mut sink);
        } else {
            struct WideEvSink<'a> {
                date_cache: &'a FastDateCache,
                win_start: i64,
                win_len_u64: u64,
                products: &'a ProductIndex,
                overflow: &'a OverflowInterner,
                tid: usize,
                idx_bits: u32,
                extras: &'a mut Vec<ExtraEvent>,
                eid_staged: &'a mut NtSlabBuckets<RawEvent16, 4, NT_RAW_BLOCK>,
            }
            impl CsvRowSink<7> for WideEvSink<'_> {
                #[inline(always)]
                unsafe fn on_pos(&mut self, ptr: *const u8, mut s: usize, rpos: *const u16) {
                    let mut fields = [&[][..]; 7];
                    for i in 0..7 {
                        let e = *rpos.add(i) as usize;
                        let q = usize::from(*ptr.add(s) == b'"');
                        *fields.get_unchecked_mut(i) =
                            std::slice::from_raw_parts(ptr.add(s + q), (e - s) - (q << 1));
                        s = e + 1;
                    }
                    self.on_row(&fields);
                }
                #[inline(always)]
                fn on_row(&mut self, row: &[&[u8]; 7]) {
                    handle_event_row::<false>(
                        row,
                        self.date_cache,
                        self.win_start,
                        self.win_len_u64,
                        self.products,
                        self.overflow,
                        self.tid,
                        self.idx_bits,
                        self.extras,
                        self.eid_staged,
                    );
                }
            }
            let mut sink = WideEvSink {
                date_cache,
                win_start,
                win_len_u64,
                products,
                overflow,
                tid,
                idx_bits,
                extras,
                eid_staged,
            };
            scan_open_file_sink::<7, _>(open_file, job.start, job.end, &mut io_buf, &mut sink);
        }
    }
}

#[cold]
#[inline(never)]
fn fallback_click_line(
    data: &[u8],
    line_start: usize,
    stop_excl: usize,
    campaigns: &CampaignIndex,
    date_cache: &FastDateCache,
    win_len_i64: i64,
    overflow: &OverflowInterner,
    links_empty: bool,
    resolver: &mut ClickResolver<'_>,
) -> usize {
    let rem_slice = &data[line_start..stop_excl];
    if let Some(rel) = memchr::memchr(b'\n', rem_slice) {
        if let Some(row) = parse_csv_line::<4>(&rem_slice[..rel]) {
            handle_click_row(
                &row, campaigns, date_cache, win_len_i64, overflow, links_empty, resolver,
            );
        }
        line_start + rel + 1
    } else {
        if !rem_slice.is_empty() {
            if let Some(row) = parse_csv_line::<4>(rem_slice) {
                handle_click_row(
                    &row, campaigns, date_cache, win_len_i64, overflow, links_empty, resolver,
                );
            }
        }
        stop_excl
    }
}

#[derive(Clone, Copy)]
struct FastParsedCk {
    cid_p: *const u8,
    is9_0: usize,
    uid_p: *const u8,
    is9_1: usize,
    camp_idx: u32,
    ts_rel: i64,
    next_cur: usize,
}

#[inline(always)]
unsafe fn parse_one_ck_fast(
    base: *const u8,
    cur: usize,
    campaigns: &CampaignIndex,
    date_cache: &FastDateCache,
    win_len_i64: i64,
) -> Option<(FastParsedCk, bool)> {
    let mut p = base.add(cur);
    let q0 = usize::from(*p == b'"');
    p = p.add(q0);
    let cid_p = p;
    let is9_0 = usize::from(*p.add(8) >= b'0');
    let len0 = 8 + is9_0;
    let ok0 = (*p == b'k') & (*p.add(len0 + q0) == b',') & ((q0 == 0) | (*p.add(len0) == b'"'));
    p = p.add(len0 + q0 + 1);

    let q1 = usize::from(*p == b'"');
    p = p.add(q1);
    let uid_p = p;
    let is9_1 = usize::from(*p.add(8) >= b'0');
    let len1 = 8 + is9_1;
    let ok1 = (*p == b'u') & (*p.add(len1 + q1) == b',') & ((q1 == 0) | (*p.add(len1) == b'"'));
    p = p.add(len1 + q1 + 1);
    if !(ok0 & ok1) {
        return None;
    }

    let q2 = usize::from(*p == b'"');
    p = p.add(q2);
    if *p != b'c' || *p.add(1) != b'_' {
        return None;
    }
    let d0 = (*p.add(2)).wrapping_sub(b'0');
    if d0 == 0 || d0 > 9 {
        return None;
    }
    let mut val = d0 as usize;
    let mut clen = 3usize;
    while clen < 8 {
        let d = (*p.add(clen)).wrapping_sub(b'0');
        if d > 9 {
            break;
        }
        val = val * 10 + d as usize;
        clen += 1;
    }
    if !((*p.add(clen + q2) == b',') & ((q2 == 0) | (*p.add(clen) == b'"'))) {
        return None;
    }
    let camp_dir = campaigns.direct.as_slice();
    if val >= camp_dir.len() {
        return None;
    }
    let camp_idx = *camp_dir.get_unchecked(val);
    if camp_idx == u32::MAX {
        return None;
    }
    p = p.add(clen + q2 + 1);

    let q3 = usize::from(*p == b'"');
    p = p.add(q3);
    let ts_rel: i64;
    if *p.add(19) == b'Z' && *p.add(20 + q3) == b'\n' && (q3 == 0 || *p.add(20) == b'"') {
        let ts_w0 = (p as *const u64).read_unaligned().to_le();
        let ts_ddt = (p.add(8) as *const u32).read_unaligned().to_le() & 0x00FF_FFFF;
        let ts_whms = (p.add(11) as *const u64).read_unaligned().to_le();
        let ts_hms = parse_hms_swar(ts_whms)?;
        let ts_day = resolve_iso_day_rel_sec(ts_w0, ts_ddt, date_cache)?;
        ts_rel = (ts_day + ts_hms) * 1_000;
        p = p.add(21 + q3);
    } else if *p.add(13 + q3) == b'\n' && (q3 == 0 || *p.add(13) == b'"') {
        let (tr, _) = parse_f3_13_ptr(p, date_cache)?;
        ts_rel = tr;
        p = p.add(14 + q3);
    } else if (*p.add(19) == b'+' || *p.add(19) == b'-')
        && *p.add(22) == b':'
        && *p.add(25 + q3) == b'\n'
        && (q3 == 0 || *p.add(25) == b'"')
    {
        let oh = parse_d2(*p.add(20), *p.add(21))?;
        let om = parse_d2(*p.add(23), *p.add(24))?;
        if oh > 23 || om > 59 {
            return None;
        }
        let sign = if *p.add(19) == b'+' { -1i64 } else { 1i64 };
        let off_sec = sign * (oh * 3_600 + om * 60);
        let ts_w0 = (p as *const u64).read_unaligned().to_le();
        let ts_ddt = (p.add(8) as *const u32).read_unaligned().to_le() & 0x00FF_FFFF;
        let ts_whms = (p.add(11) as *const u64).read_unaligned().to_le();
        let ts_hms = parse_hms_swar(ts_whms)?;
        let ts_day = resolve_iso_day_rel_sec(ts_w0, ts_ddt, date_cache)?;
        ts_rel = (ts_day + ts_hms + off_sec) * 1_000;
        p = p.add(26 + q3);
    } else {
        return None;
    }
    let next_cur = p.offset_from(base) as usize;
    let in_win = ts_rel >= -CLICK_REACH_MS && ts_rel < win_len_i64;
    Some((
        FastParsedCk {
            cid_p,
            is9_0,
            uid_p,
            is9_1,
            camp_idx,
            ts_rel,
            next_cur,
        },
        in_win,
    ))
}

/// Direct single-pass clicks CSV scanner bypassing intermediate delimiter arrays
/// on 8/9-byte `'k'`/`'u'` IDs and `c_<digits>` campaigns, with two-stream AVX2 `pack_b37_quad_89`
/// and exact line fallback.
#[inline(never)]
unsafe fn scan_clicks_direct(
    data: &[u8],
    start: usize,
    end: usize,
    campaigns: &CampaignIndex,
    date_cache: &FastDateCache,
    win_len_i64: i64,
    overflow: &OverflowInterner,
    links_empty: bool,
    resolver: &mut ClickResolver<'_>,
) {
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
    let emit_ck =
        |resolver: &mut ClickResolver<'_>, uid_k: u64, cid_k: u64, ts_rel: i64, camp_idx: u32| {
            if links_empty {
                emit_surv_click(
                    mix42(uid_k),
                    cid_k,
                    ts_rel,
                    camp_idx,
                    resolver.use_compact_ts,
                    resolver.ck_init_elems,
                    resolver.ck_extra_elems,
                    resolver.win_start,
                    resolver.extra_ck_buckets,
                    resolver.ck_compact_staged,
                    resolver.ck_wide_staged,
                );
            } else {
                resolver.push(PendingClick {
                    uid_k,
                    cid_k,
                    ts_rel,
                    camp_idx,
                });
            }
        };

    let span = stop_excl - start_pos;
    if span >= 4096 {
        let mid_guess = start_pos + (span >> 1);
        if let Some(mrel) = memchr::memchr(b'\n', &data[mid_guess..stop_excl]) {
            let mid = mid_guess + mrel + 1;
            let mut cur_a = start_pos;
            let mut cur_b = mid;
            let safe_a = mid.saturating_sub(80);
            let safe_b = stop_excl.saturating_sub(80);
            while cur_a < safe_a && cur_b < safe_b {
                let oa = parse_one_ck_fast(base, cur_a, campaigns, date_cache, win_len_i64);
                let ob = parse_one_ck_fast(base, cur_b, campaigns, date_cache, win_len_i64);
                if let (Some((a, ina)), Some((b, inb))) = (oa, ob) {
                    if ina & inb {
                        #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
                        {
                            if let Some((ua, ca, ub, cb)) = crate::pack::pack_b37_quad_89(
                                a.uid_p, a.is9_1, a.cid_p, a.is9_0, b.uid_p, b.is9_1, b.cid_p,
                                b.is9_0,
                            ) {
                                emit_ck(resolver, ua, ca, a.ts_rel, a.camp_idx);
                                emit_ck(resolver, ub, cb, b.ts_rel, b.camp_idx);
                                cur_a = a.next_cur;
                                cur_b = b.next_cur;
                                continue;
                            }
                        }
                    } else if !ina & !inb {
                        cur_a = a.next_cur;
                        cur_b = b.next_cur;
                        continue;
                    }
                }
                if let Some((a, ina)) = oa {
                    if !ina {
                        cur_a = a.next_cur;
                    } else if let Some((ua, ca)) =
                        pack_b37_pair_89(a.uid_p, a.is9_1, a.cid_p, a.is9_0)
                    {
                        emit_ck(resolver, ua, ca, a.ts_rel, a.camp_idx);
                        cur_a = a.next_cur;
                    } else {
                        cur_a = fallback_click_line(
                            data,
                            cur_a,
                            mid,
                            campaigns,
                            date_cache,
                            win_len_i64,
                            overflow,
                            links_empty,
                            resolver,
                        );
                    }
                } else {
                    cur_a = fallback_click_line(
                        data,
                        cur_a,
                        mid,
                        campaigns,
                        date_cache,
                        win_len_i64,
                        overflow,
                        links_empty,
                        resolver,
                    );
                }
                if let Some((b, inb)) = ob {
                    if !inb {
                        cur_b = b.next_cur;
                    } else if let Some((ub, cb)) =
                        pack_b37_pair_89(b.uid_p, b.is9_1, b.cid_p, b.is9_0)
                    {
                        emit_ck(resolver, ub, cb, b.ts_rel, b.camp_idx);
                        cur_b = b.next_cur;
                    } else {
                        cur_b = fallback_click_line(
                            data,
                            cur_b,
                            stop_excl,
                            campaigns,
                            date_cache,
                            win_len_i64,
                            overflow,
                            links_empty,
                            resolver,
                        );
                    }
                } else {
                    cur_b = fallback_click_line(
                        data,
                        cur_b,
                        stop_excl,
                        campaigns,
                        date_cache,
                        win_len_i64,
                        overflow,
                        links_empty,
                        resolver,
                    );
                }
            }
            for (mut cur, lim) in [(cur_a, mid), (cur_b, stop_excl)] {
                let sf = lim.saturating_sub(80);
                while cur < sf {
                    if let Some((ck, in_w)) =
                        parse_one_ck_fast(base, cur, campaigns, date_cache, win_len_i64)
                    {
                        if !in_w {
                            cur = ck.next_cur;
                            continue;
                        }
                        if let Some((uk, ck_k)) =
                            pack_b37_pair_89(ck.uid_p, ck.is9_1, ck.cid_p, ck.is9_0)
                        {
                            emit_ck(resolver, uk, ck_k, ck.ts_rel, ck.camp_idx);
                            cur = ck.next_cur;
                            continue;
                        }
                    }
                    cur = fallback_click_line(
                        data,
                        cur,
                        lim,
                        campaigns,
                        date_cache,
                        win_len_i64,
                        overflow,
                        links_empty,
                        resolver,
                    );
                }
                while cur < lim {
                    cur = fallback_click_line(
                        data,
                        cur,
                        lim,
                        campaigns,
                        date_cache,
                        win_len_i64,
                        overflow,
                        links_empty,
                        resolver,
                    );
                }
            }
            return;
        }
    }

    let mut cur = start_pos;
    let sf = stop_excl.saturating_sub(80);
    while cur < sf {
        if let Some((ck, in_w)) = parse_one_ck_fast(base, cur, campaigns, date_cache, win_len_i64)
        {
            if !in_w {
                cur = ck.next_cur;
                continue;
            }
            if let Some((uk, ck_k)) = pack_b37_pair_89(ck.uid_p, ck.is9_1, ck.cid_p, ck.is9_0) {
                emit_ck(resolver, uk, ck_k, ck.ts_rel, ck.camp_idx);
                cur = ck.next_cur;
                continue;
            }
        }
        cur = fallback_click_line(
            data,
            cur,
            stop_excl,
            campaigns,
            date_cache,
            win_len_i64,
            overflow,
            links_empty,
            resolver,
        );
    }
    while cur < stop_excl {
        cur = fallback_click_line(
            data,
            cur,
            stop_excl,
            campaigns,
            date_cache,
            win_len_i64,
            overflow,
            links_empty,
            resolver,
        );
    }
}

#[inline(always)]
fn drain_click_jobs(
    next_ck_job: &AtomicUsize,
    click_jobs: &[ChunkJob],
    click_files: &[OpenCsvFile],
    campaigns: &CampaignIndex,
    date_cache: &FastDateCache,
    win_len_i64: i64,
    win_start: i64,
    overflow: &OverflowInterner,
    use_compact_ts: bool,
    ck_init_elems: usize,
    ck_extra_elems: usize,
    link_redirect: &LinkRedirect,
    extra_ck_buckets: &mut Vec<Vec<ExtraClick>>,
    ck_compact_staged: &mut Option<NtSlabBuckets<SurvClick16, 4, NT_CLICK_BLOCK>>,
    ck_wide_staged: &mut Option<DirectSlabBuckets<SurvClickWide, WIDE_BLOCK>>,
) {
    let links_empty = link_redirect.is_empty();
    let mut resolver = ClickResolver::new(
        link_redirect,
        use_compact_ts,
        ck_init_elems,
        ck_extra_elems,
        win_start,
        extra_ck_buckets,
        ck_compact_staged,
        ck_wide_staged,
    );

    struct CkSink<'a, 'b> {
        campaigns: &'a CampaignIndex,
        date_cache: &'a FastDateCache,
        win_len_i64: i64,
        overflow: &'a OverflowInterner,
        links_empty: bool,
        resolver: &'a mut ClickResolver<'b>,
    }
    impl CsvRowSink<4> for CkSink<'_, '_> {
        #[inline(always)]
        unsafe fn on_pos(&mut self, ptr: *const u8, s0: usize, rpos: *const u16) {
            let e0 = *rpos as usize;
            let q0 = usize::from(*ptr.add(s0) == b'"');
            let cid_len = (e0 - s0) - (q0 << 1);
            if cid_len == 0 {
                return;
            }
            let e1 = *rpos.add(1) as usize;
            let s1 = e0 + 1;
            let q1 = usize::from(*ptr.add(s1) == b'"');
            let uid_len = (e1 - s1) - (q1 << 1);
            if uid_len == 0 {
                return;
            }
            let e2 = *rpos.add(2) as usize;
            let s2 = e1 + 1;
            let q2 = usize::from(*ptr.add(s2) == b'"');
            let camp_b = std::slice::from_raw_parts(ptr.add(s2 + q2), (e2 - s2) - (q2 << 1));
            let Some(camp_idx) = self.campaigns.lookup(camp_b) else {
                return;
            };
            let e3 = *rpos.add(3) as usize;
            let s3 = e2 + 1;
            let q3 = usize::from(*ptr.add(s3) == b'"');
            let ts_b = std::slice::from_raw_parts(ptr.add(s3 + q3), (e3 - s3) - (q3 << 1));
            let Some(ts_rel) = parse_ts_rel_ms(ts_b, self.date_cache) else {
                return;
            };
            if ts_rel < -CLICK_REACH_MS || ts_rel >= self.win_len_i64 {
                return;
            }
            let cid = std::slice::from_raw_parts(ptr.add(s0 + q0), cid_len);
            let uid = std::slice::from_raw_parts(ptr.add(s1 + q1), uid_len);
            let (uid_k, cid_k) = pack_b37_pair(uid, b'u', cid, b'k')
                .unwrap_or_else(|| (self.overflow.encode_user(uid), self.overflow.encode_click(cid)));
            if self.links_empty {
                let ph = if uid_k < B37_LIMIT {
                    mix42(uid_k)
                } else {
                    uid_k
                };
                emit_surv_click(
                    ph,
                    cid_k,
                    ts_rel,
                    camp_idx,
                    self.resolver.use_compact_ts,
                    self.resolver.ck_init_elems,
                    self.resolver.ck_extra_elems,
                    self.resolver.win_start,
                    self.resolver.extra_ck_buckets,
                    self.resolver.ck_compact_staged,
                    self.resolver.ck_wide_staged,
                );
            } else {
                self.resolver.push(PendingClick {
                    uid_k,
                    cid_k,
                    ts_rel,
                    camp_idx,
                });
            }
        }
        #[inline(always)]
        fn on_row(&mut self, row: &[&[u8]; 4]) {
            handle_click_row(
                row,
                self.campaigns,
                self.date_cache,
                self.win_len_i64,
                self.overflow,
                self.links_empty,
                self.resolver,
            );
        }
    }

    let mut sink = CkSink {
        campaigns,
        date_cache,
        win_len_i64,
        overflow,
        links_empty,
        resolver: &mut resolver,
    };

    let mut io_buf: Vec<u8> = vec![0u8; IO_CHUNK_BYTES + 32_768];
    loop {
        let jid = next_ck_job.fetch_add(1, MemOrdering::Relaxed);
        let Some(job) = click_jobs.get(jid) else {
            break;
        };
        let cf = &click_files[job.fidx];
        if date_cache.win_start_whole_sec && !campaigns.direct.is_empty() {
            let (got, ls, le) = pread_csv_chunk(&cf.file, cf.len, job.start, job.end, &mut io_buf);
            if got > 0 {
                unsafe {
                    scan_clicks_direct(
                        &io_buf[..got],
                        ls,
                        le,
                        campaigns,
                        date_cache,
                        win_len_i64,
                        overflow,
                        links_empty,
                        sink.resolver,
                    );
                }
            }
        } else {
            scan_open_file_sink::<4, _>(cf, job.start, job.end, &mut io_buf, &mut sink);
        }
    }
    resolver.finish();
}

#[cold]
fn eids_equal_extra(
    a: &RawEvent16,
    b: &RawEvent16,
    extras: &[Vec<ExtraEvent>],
    idx_bits: u32,
    idx_mask: u64,
) -> bool {
    let ea = (a.hi & MASK42) - 1;
    let eb = (b.hi & MASK42) - 1;
    let ka = unsafe {
        extras
            .get_unchecked((ea >> idx_bits) as usize)
            .get_unchecked((ea & idx_mask) as usize)
    }
    .eid_k;
    let kb = unsafe {
        extras
            .get_unchecked((eb >> idx_bits) as usize)
            .get_unchecked((eb & idx_mask) as usize)
    }
    .eid_k;
    ka == kb
}

#[inline(always)]
fn raw16_same_eid(
    a: &RawEvent16,
    b: &RawEvent16,
    extras: &[Vec<ExtraEvent>],
    idx_bits: u32,
    idx_mask: u64,
) -> bool {
    if (a.lo >> 32) != (b.lo >> 32) || ((a.hi ^ b.hi) & RAW_EXTRA_EID_BIT) != 0 {
        return false;
    }
    if (a.hi & RAW_EXTRA_EID_BIT) == 0 {
        return true;
    }
    eids_equal_extra(a, b, extras, idx_bits, idx_mask)
}

#[inline(always)]
fn dedup_open_addr16<const IDX_BITS: u32>(
    slices: &[&[RawEvent16]],
    total_recs: usize,
    all_extras: &[Vec<ExtraEvent>],
    win_start: i64,
    idx_bits: u32,
    idx_mask: u64,
    slots: &mut Vec<u32>,
    kept: &mut Vec<RawEvent16>,
) {
    let cap = ((total_recs * 3) / 2)
        .max(total_recs + 16)
        .next_power_of_two()
        .max(64);
    if slots.len() < cap {
        slots.resize(cap, 0);
    }
    slots[..cap].fill(0);
    let mask = cap - 1;
    let kept_ptr = kept.as_mut_ptr();
    let mut klen = 0usize;
    let tag_shift = IDX_BITS;
    let kidx_mask = (1u32 << IDX_BITS) - 1;

    for &chunk in slices {
        let clen = chunk.len();
        if clen == 0 {
            continue;
        }
        let pre = clen.min(4);
        for j in 0..pre {
            let h = (unsafe { chunk.get_unchecked(j) }.lo >> 32) as usize;
            prefetch_ptr(unsafe { slots.as_ptr().add(h & mask) });
        }
        for ci in 0..clen {
            if ci + 4 < clen {
                let nh = (unsafe { chunk.get_unchecked(ci + 4) }.lo >> 32) as usize;
                prefetch_ptr(unsafe { slots.as_ptr().add(nh & mask) });
            }
            let rec = unsafe { *chunk.get_unchecked(ci) };
            let h = (rec.lo >> 32) as usize;
            let tag = (h >> tag_shift) as u32;
            let mut pos = h & mask;
            loop {
                let slot = unsafe { *slots.get_unchecked(pos) };
                if slot == 0 {
                    unsafe {
                        *kept_ptr.add(klen) = rec;
                        *slots.get_unchecked_mut(pos) = (tag << IDX_BITS) | ((klen as u32) + 1);
                    }
                    klen += 1;
                    break;
                }
                if (slot >> IDX_BITS) == tag {
                    let kidx = ((slot & kidx_mask) - 1) as usize;
                    let cur = unsafe { &mut *kept_ptr.add(kidx) };
                    if raw16_same_eid(cur, &rec, all_extras, idx_bits, idx_mask) {
                        if cmp_ingest16(&rec, cur, all_extras, win_start, idx_bits, idx_mask)
                            == Ordering::Less
                        {
                            *cur = rec;
                        }
                        break;
                    }
                }
                pos = (pos + 1) & mask;
            }
        }
    }
    unsafe {
        kept.set_len(klen);
    }
}

#[inline(always)]
fn unpack_surv_source(
    rec: RawEvent16,
    all_extras: &[Vec<ExtraEvent>],
    win_start: i64,
    idx_bits: u32,
    idx_mask: u64,
) -> Option<(u64, i64, u32)> {
    if (rec.lo & RAW_SPECIAL_BIT) == 0 {
        return Some((rec.hi & MASK42, (rec.lo & 0x7FFF_FFFF) as i64, 0));
    }
    let eref_p1 = rec.hi & MASK42;
    if eref_p1 == 0 {
        return None;
    }
    let eref = eref_p1 - 1;
    let tid = (eref >> idx_bits) as usize;
    let eidx = (eref & idx_mask) as usize;
    let ex = unsafe { *all_extras.get_unchecked(tid).get_unchecked(eidx) };
    if !ex.valid {
        return None;
    }
    let pc = if ex.price_cents == 0 {
        0
    } else {
        (ex.cat_id << 24) | ex.price_cents
    };
    Some((ex.uid_k, ex.ts - win_start, pc))
}

#[inline(always)]
fn emit_surv_compact_split(
    person: u64,
    ts_rel: u32,
    price_cat: u32,
    ex_init_elems: usize,
    ex_extra_elems: usize,
    out: &mut NtSlabBuckets<u64, 8, NT_SURV_BLOCK>,
    ex_compact: &mut Option<DirectSlabBuckets<ExtraSurv, EX_SURV_BLOCK>>,
) {
    if person < B37_LIMIT && price_cat == 0 {
        let ph = mix42(person);
        out.push(
            (ph as usize) & PERSON_MASK,
            ((ph >> PERSON_BITS) << 31) | (ts_rel as u64),
        );
    } else {
        let pb = person_bucket(person);
        let ex_out = ex_compact.get_or_insert_with(|| {
            DirectSlabBuckets::new(PERSON_BUCKETS, ex_init_elems, ex_extra_elems)
        });
        ex_out.push(
            pb,
            ExtraSurv {
                person,
                ts_rel,
                price_cat,
            },
        );
    }
}

#[inline(always)]
fn emit_surv_wide(
    person: u64,
    ts_abs: i64,
    price_cat: u32,
    out: &mut DirectSlabBuckets<SurvEventWide, WIDE_BLOCK>,
) {
    let pb = person_bucket(person);
    let (price_cents, cat_id) = if price_cat == 0 {
        (0u32, 0u32)
    } else {
        (price_cat & 0x00FF_FFFF, price_cat >> 24)
    };
    out.push(
        pb,
        SurvEventWide {
            person,
            ts: ts_abs,
            price_cents,
            cat_id,
        },
    );
}

#[inline(always)]
fn prefetch_extra_event(f: RawEvent16, all_extras: &[Vec<ExtraEvent>], idx_bits: u32, idx_mask: u64) {
    if (f.lo & RAW_SPECIAL_BIT) != 0 {
        let ep1 = f.hi & MASK42;
        if ep1 != 0 {
            let eref = ep1 - 1;
            prefetch_ptr(unsafe {
                all_extras
                    .get_unchecked((eref >> idx_bits) as usize)
                    .as_ptr()
                    .add((eref & idx_mask) as usize)
            });
        }
    }
}

#[inline(always)]
fn prepare_batch_wide(
    kslice: &[RawEvent16],
    start: usize,
    end: usize,
    all_extras: &[Vec<ExtraEvent>],
    win_start: i64,
    idx_bits: u32,
    idx_mask: u64,
    link_redirect: &LinkRedirect,
    uk_out: &mut [u64; 32],
    h_out: &mut [u64; 32],
    ts_abs_out: &mut [i64; 32],
    pc_out: &mut [u32; 32],
) {
    let len = end - start;
    let pre = len.min(4);
    for j in 0..pre {
        prefetch_extra_event(
            unsafe { *kslice.get_unchecked(start + j) },
            all_extras,
            idx_bits,
            idx_mask,
        );
    }
    for j in 0..len {
        if j + 4 < len {
            prefetch_extra_event(
                unsafe { *kslice.get_unchecked(start + j + 4) },
                all_extras,
                idx_bits,
                idx_mask,
            );
        }
        let rec = unsafe { *kslice.get_unchecked(start + j) };
        if (rec.lo & RAW_SPECIAL_BIT) == 0 {
            let uk = rec.hi & MASK42;
            uk_out[j] = uk;
            h_out[j] = link_redirect.prefetch_bloom_for(uk);
            ts_abs_out[j] = win_start + ((rec.lo & 0x7FFF_FFFF) as i64);
            pc_out[j] = 0;
        } else {
            let ep1 = rec.hi & MASK42;
            if ep1 == 0 {
                uk_out[j] = 0;
            } else {
                let eref = ep1 - 1;
                let ex = unsafe {
                    *all_extras
                        .get_unchecked((eref >> idx_bits) as usize)
                        .get_unchecked((eref & idx_mask) as usize)
                };
                if ex.valid {
                    uk_out[j] = ex.uid_k;
                    h_out[j] = link_redirect.prefetch_bloom_for(ex.uid_k);
                    ts_abs_out[j] = ex.ts;
                    pc_out[j] = if ex.price_cents == 0 {
                        0
                    } else {
                        (ex.cat_id << 24) | ex.price_cents
                    };
                } else {
                    uk_out[j] = 0;
                }
            }
        }
    }
}

#[inline(always)]
fn dedup_and_emit(
    slices: &[&[RawEvent16]],
    total_recs: usize,
    all_extras: &[Vec<ExtraEvent>],
    link_redirect: &LinkRedirect,
    links_empty: bool,
    win_start: i64,
    idx_bits: u32,
    idx_mask: u64,
    ex_init_elems: usize,
    ex_extra_elems: usize,
    slots: &mut Vec<u32>,
    kept: &mut Vec<RawEvent16>,
    ev_compact: &mut Option<NtSlabBuckets<u64, 8, NT_SURV_BLOCK>>,
    ex_compact: &mut Option<DirectSlabBuckets<ExtraSurv, EX_SURV_BLOCK>>,
    ev_wide: &mut Option<DirectSlabBuckets<SurvEventWide, WIDE_BLOCK>>,
) {
    kept.clear();
    kept.reserve(total_recs);

    if total_recs < 65_535 {
        dedup_open_addr16::<16>(
            slices,
            total_recs,
            all_extras,
            win_start,
            idx_bits,
            idx_mask,
            slots,
            kept,
        );
    } else if total_recs < 262_143 {
        dedup_open_addr16::<18>(
            slices,
            total_recs,
            all_extras,
            win_start,
            idx_bits,
            idx_mask,
            slots,
            kept,
        );
    } else if total_recs < 1_048_575 {
        dedup_open_addr16::<20>(
            slices,
            total_recs,
            all_extras,
            win_start,
            idx_bits,
            idx_mask,
            slots,
            kept,
        );
    } else {
        let mut ht: HashTable<usize> = HashTable::with_capacity(total_recs);
        for &chunk in slices {
            for &rec in chunk {
                let h = mix64(rec.lo >> 32);
                if let Some(&kidx) = ht.find(h, |&idx| {
                    raw16_same_eid(&kept[idx], &rec, all_extras, idx_bits, idx_mask)
                }) {
                    if cmp_ingest16(&rec, &kept[kidx], all_extras, win_start, idx_bits, idx_mask)
                        == Ordering::Less
                    {
                        kept[kidx] = rec;
                    }
                } else {
                    let idx = kept.len();
                    kept.push(rec);
                    ht.insert_unique(h, idx, |&i| mix64(kept[i].lo >> 32));
                }
            }
        }
    }

    let kslice = kept.as_slice();
    let klen = kslice.len();
    if klen == 0 {
        return;
    }

    if links_empty {
        if let Some(ref mut out) = ev_compact {
            for j in 0..klen {
                if j + 4 < klen {
                    prefetch_extra_event(
                        unsafe { *kslice.get_unchecked(j + 4) },
                        all_extras,
                        idx_bits,
                        idx_mask,
                    );
                }
                let rec = unsafe { *kslice.get_unchecked(j) };
                if (rec.lo & RAW_SPECIAL_BIT) == 0 {
                    let ph = mix42(rec.hi & MASK42);
                    let ts_rel = rec.lo & 0x7FFF_FFFF;
                    out.push(
                        (ph as usize) & PERSON_MASK,
                        ((ph >> PERSON_BITS) << 31) | ts_rel,
                    );
                } else if let Some((person, ts_rel, pc)) =
                    unpack_surv_source(rec, all_extras, win_start, idx_bits, idx_mask)
                {
                    emit_surv_compact_split(
                        person,
                        ts_rel as u32,
                        pc,
                        ex_init_elems,
                        ex_extra_elems,
                        out,
                        ex_compact,
                    );
                }
            }
        } else if let Some(ref mut out) = ev_wide {
            for j in 0..klen {
                if j + 4 < klen {
                    prefetch_extra_event(
                        unsafe { *kslice.get_unchecked(j + 4) },
                        all_extras,
                        idx_bits,
                        idx_mask,
                    );
                }
                let rec = unsafe { *kslice.get_unchecked(j) };
                if let Some((person, ts_rel, pc)) =
                    unpack_surv_source(rec, all_extras, win_start, idx_bits, idx_mask)
                {
                    emit_surv_wide(person, win_start + ts_rel, pc, out);
                }
            }
        }
        return;
    }

    if let Some(ref mut out) = ev_compact {
        if link_redirect.has_bucket6() {
            const B: usize = 64;
            let mut h_b = [[0u64; B]; 2];
            let mut hits = [0u16; B];
            let mut hit_bidx = [0u32; B];
            let mut misses = [0u16; B];
            let mut specs = [0u16; B];
            let mut cur_buf = 0usize;

            let first_end = klen.min(B);
            for j in 0..first_end {
                let rec = unsafe { *kslice.get_unchecked(j) };
                h_b[0][j] = if (rec.lo & RAW_SPECIAL_BIT) == 0 {
                    link_redirect.prefetch_bloom_b37(rec.hi & MASK42)
                } else {
                    let ep1 = rec.hi & MASK42;
                    if ep1 != 0 {
                        let eref = ep1 - 1;
                        prefetch_ptr(unsafe {
                            all_extras
                                .get_unchecked((eref >> idx_bits) as usize)
                                .as_ptr()
                                .add((eref & idx_mask) as usize)
                        });
                        u64::MAX
                    } else {
                        u64::MAX - 1
                    }
                };
            }

            let mut bstart = 0usize;
            while bstart < klen {
                let bend = (bstart + B).min(klen);
                let blen = bend - bstart;
                let nxt_buf = cur_buf ^ 1;
                let mut n_hits = 0usize;
                let mut n_misses = 0usize;
                let mut n_specs = 0usize;

                for j in 0..blen {
                    let h = unsafe { *h_b.get_unchecked(cur_buf).get_unchecked(j) };
                    if h < (u64::MAX - 1) {
                        let hit = usize::from(link_redirect.bloom_test_b37(h));
                        unsafe {
                            *hits.get_unchecked_mut(n_hits) = j as u16;
                            *misses.get_unchecked_mut(n_misses) = j as u16;
                        }
                        n_hits += hit;
                        n_misses += 1 - hit;
                    } else if h == u64::MAX {
                        unsafe { *specs.get_unchecked_mut(n_specs) = j as u16 };
                        n_specs += 1;
                    }
                }

                for i in 0..n_hits {
                    let j = unsafe { *hits.get_unchecked(i) } as usize;
                    let h = unsafe { *h_b.get_unchecked(cur_buf).get_unchecked(j) };
                    unsafe {
                        *hit_bidx.get_unchecked_mut(i) =
                            link_redirect.bucket6_slot_and_prefetch(h) as u32;
                    }
                }

                // Overlap Bucket6 prefetch latency with next-batch Bloom prefetches & miss emissions
                if bend < klen {
                    let nend = (bend + B).min(klen);
                    for j in 0..(nend - bend) {
                        let rec = unsafe { *kslice.get_unchecked(bend + j) };
                        unsafe {
                            *h_b.get_unchecked_mut(nxt_buf).get_unchecked_mut(j) =
                                if (rec.lo & RAW_SPECIAL_BIT) == 0 {
                                    link_redirect.prefetch_bloom_b37(rec.hi & MASK42)
                                } else {
                                    let ep1 = rec.hi & MASK42;
                                    if ep1 != 0 {
                                        let eref = ep1 - 1;
                                        prefetch_ptr(
                                            all_extras
                                                .get_unchecked((eref >> idx_bits) as usize)
                                                .as_ptr()
                                                .add((eref & idx_mask) as usize),
                                        );
                                        u64::MAX
                                    } else {
                                        u64::MAX - 1
                                    }
                                };
                        }
                    }
                }

                for i in 0..n_misses {
                    let j = unsafe { *misses.get_unchecked(i) } as usize;
                    let h = unsafe { *h_b.get_unchecked(cur_buf).get_unchecked(j) };
                    let ts_rel =
                        (unsafe { kslice.get_unchecked(bstart + j) }.lo & 0x7FFF_FFFF) as u64;
                    out.push(
                        (h as usize) & PERSON_MASK,
                        ((h >> PERSON_BITS) << 31) | ts_rel,
                    );
                }

                for i in 0..n_hits {
                    let j = unsafe { *hits.get_unchecked(i) } as usize;
                    let rec = unsafe { *kslice.get_unchecked(bstart + j) };
                    let uk = rec.hi & MASK42;
                    let h = unsafe { *h_b.get_unchecked(cur_buf).get_unchecked(j) };
                    let sidx = unsafe { *hit_bidx.get_unchecked(i) } as usize;
                    let ph = link_redirect.resolve_from_slot_hashed(uk, h, sidx);
                    let ts_rel = (rec.lo & 0x7FFF_FFFF) as u32;
                    if ph < B37_LIMIT {
                        out.push(
                            (ph as usize) & PERSON_MASK,
                            ((ph >> PERSON_BITS) << 31) | (ts_rel as u64),
                        );
                    } else {
                        let person = decode_canon_val(ph);
                        let pb = (mix64(person) as usize) & PERSON_MASK;
                        let ex_out = ex_compact.get_or_insert_with(|| {
                            DirectSlabBuckets::new(PERSON_BUCKETS, ex_init_elems, ex_extra_elems)
                        });
                        ex_out.push(
                            pb,
                            ExtraSurv {
                                person,
                                ts_rel,
                                price_cat: 0,
                            },
                        );
                    }
                }

                if n_specs > 0 {
                    let mut sp_uk = [0u64; B];
                    let mut sp_h = [0u64; B];
                    let mut sp_ts = [0u32; B];
                    let mut sp_pc = [0u32; B];
                    let mut sp_slot = [usize::MAX; B];
                    let mut nv = 0usize;
                    for i in 0..n_specs {
                        let j = unsafe { *specs.get_unchecked(i) } as usize;
                        let rec = unsafe { *kslice.get_unchecked(bstart + j) };
                        let eref = (rec.hi & MASK42) - 1;
                        let ex = unsafe {
                            *all_extras
                                .get_unchecked((eref >> idx_bits) as usize)
                                .get_unchecked((eref & idx_mask) as usize)
                        };
                        if ex.valid {
                            sp_uk[nv] = ex.uid_k;
                            sp_h[nv] = link_redirect.prefetch_bloom_for(ex.uid_k);
                            sp_ts[nv] = (ex.ts - win_start) as u32;
                            sp_pc[nv] = if ex.price_cents == 0 {
                                0
                            } else {
                                (ex.cat_id << 24) | ex.price_cents
                            };
                            nv += 1;
                        }
                    }
                    for i in 0..nv {
                        if let Some(sidx) = link_redirect.bloom_hit_slot_with_hash(sp_uk[i], sp_h[i])
                        {
                            link_redirect.prefetch_slot(sidx);
                            sp_slot[i] = sidx;
                        }
                    }
                    for i in 0..nv {
                        let uk = sp_uk[i];
                        let h = sp_h[i];
                        let sidx = sp_slot[i];
                        let ph = if sidx == usize::MAX {
                            if uk < B37_LIMIT {
                                h
                            } else {
                                uk
                            }
                        } else {
                            link_redirect.resolve_from_slot_hashed(uk, h, sidx)
                        };
                        let pc = sp_pc[i];
                        let ts_rel = sp_ts[i];
                        if ph < B37_LIMIT && pc == 0 {
                            out.push(
                                (ph as usize) & PERSON_MASK,
                                ((ph >> PERSON_BITS) << 31) | (ts_rel as u64),
                            );
                        } else {
                            let person = decode_canon_val(ph);
                            let pb = if ph < B37_LIMIT {
                                (ph as usize) & PERSON_MASK
                            } else {
                                (mix64(person) as usize) & PERSON_MASK
                            };
                            let ex_out = ex_compact.get_or_insert_with(|| {
                                DirectSlabBuckets::new(
                                    PERSON_BUCKETS,
                                    ex_init_elems,
                                    ex_extra_elems,
                                )
                            });
                            ex_out.push(
                                pb,
                                ExtraSurv {
                                    person,
                                    ts_rel,
                                    price_cat: pc,
                                },
                            );
                        }
                    }
                }

                cur_buf = nxt_buf;
                bstart = bend;
            }
        } else {
            for j in 0..klen {
                let rec = unsafe { *kslice.get_unchecked(j) };
                if let Some((uk, ts_rel_i64, pc)) =
                    unpack_surv_source(rec, all_extras, win_start, idx_bits, idx_mask)
                {
                    let person = link_redirect.resolve(uk);
                    emit_surv_compact_split(
                        person,
                        ts_rel_i64 as u32,
                        pc,
                        ex_init_elems,
                        ex_extra_elems,
                        out,
                        ex_compact,
                    );
                }
            }
        }
    } else if let Some(ref mut out) = ev_wide {
        let mut uk_bufs = [[0u64; 32]; 2];
        let mut hbufs = [[0u64; 32]; 2];
        let mut pc_bufs = [[0u32; 32]; 2];
        let mut cur_buf = 0usize;
        let mut ts_abs_bufs = [[0i64; 32]; 2];
        let first_end = klen.min(32);
        prepare_batch_wide(
            kslice,
            0,
            first_end,
            all_extras,
            win_start,
            idx_bits,
            idx_mask,
            link_redirect,
            &mut uk_bufs[cur_buf],
            &mut hbufs[cur_buf],
            &mut ts_abs_bufs[cur_buf],
            &mut pc_bufs[cur_buf],
        );

        let mut bstart = 0usize;
        while bstart < klen {
            let bend = (bstart + 32).min(klen);
            let blen = bend - bstart;
            let nxt_buf = cur_buf ^ 1;
            let mut slot_idxs = [usize::MAX; 32];

            for j in 0..blen {
                let uk = uk_bufs[cur_buf][j];
                if uk != 0 {
                    if let Some(sidx) = link_redirect.bloom_hit_slot_with_hash(uk, hbufs[cur_buf][j])
                    {
                        link_redirect.prefetch_slot(sidx);
                        slot_idxs[j] = sidx;
                    }
                }
            }

            if bend < klen {
                let nend = (bend + 32).min(klen);
                prepare_batch_wide(
                    kslice,
                    bend,
                    nend,
                    all_extras,
                    win_start,
                    idx_bits,
                    idx_mask,
                    link_redirect,
                    &mut uk_bufs[nxt_buf],
                    &mut hbufs[nxt_buf],
                    &mut ts_abs_bufs[nxt_buf],
                    &mut pc_bufs[nxt_buf],
                );
            }

            for j in 0..blen {
                let uk = uk_bufs[cur_buf][j];
                if uk == 0 {
                    continue;
                }
                let h = hbufs[cur_buf][j];
                let sidx = slot_idxs[j];
                let ph = if sidx == usize::MAX {
                    if uk < B37_LIMIT {
                        h
                    } else {
                        uk
                    }
                } else {
                    link_redirect.resolve_from_slot_hashed(uk, h, sidx)
                };
                emit_surv_wide(
                    decode_canon_val(ph),
                    ts_abs_bufs[cur_buf][j],
                    pc_bufs[cur_buf][j],
                    out,
                );
            }
            cur_buf = nxt_buf;
            bstart = bend;
        }
    }
}

pub fn run_pipeline(data_dir: &Path, out_dir: &Path, num_threads: usize) {
    let t_total = Instant::now();
    let (win_start, win_end) = parse_manifest(&data_dir.join("manifest.json"));
    let click_win_start = win_start - CLICK_REACH_MS;
    let win_len_i64 = win_end - win_start;
    let win_len_u64 = win_len_i64.max(0) as u64;
    let date_cache = FastDateCache::new(win_start, win_end);

    let min_day = win_start.div_euclid(DAY_MS) as i32;
    let max_day = (win_end - 1).max(win_start).div_euclid(DAY_MS) as i32;
    let day_span = (max_day - min_day + 1).max(1) as usize;

    let link_files = list_csvs(&data_dir.join("links"));
    let event_files = list_csvs(&data_dir.join("events"));
    let click_files = list_csvs(&data_dir.join("clicks"));

    let nt_read = num_threads.max(1);
    let overflow = OverflowInterner::new();

    // Stage 1: Build link redirects in parallel with loading metadata and mmaps
    let t0 = Instant::now();
    let (products, campaigns, inputs, link_redirect): (
        ProductIndex,
        CampaignIndex,
        OpenedInputs,
        LinkRedirect,
    ) = std::thread::scope(|s| {
        let hp = s.spawn(|| ProductIndex::load(&data_dir.join("products.csv")));
        let hc = s.spawn(|| CampaignIndex::load(&data_dir.join("campaigns.csv")));
        let hf = s.spawn(|| open_inputs_and_jobs(&event_files, &click_files));
        let lr = build_link_redirect(&link_files, nt_read, &overflow);
        (hp.join().unwrap(), hc.join().unwrap(), hf.join().unwrap(), lr)
    });

    let OpenedInputs {
        event_files: ev_open_files,
        click_files: ck_open_files,
        event_jobs,
        click_jobs,
        total_event_bytes,
        total_click_bytes,
    } = inputs;
    let dt_links = t0.elapsed();

    let use_compact_ts = win_end >= win_start
        && win_len_i64 <= 2_000_000_000
        && (win_end - click_win_start) <= 2_100_000_000
        && products.categories.len() <= 256;

    let (ev_init_elems, ev_extra_elems) = if total_event_bytes >= 10_000_000 {
        let init = (((total_event_bytes as usize / 52 / nt_read).max(1_048_576) + 511) & !511)
            .max(1_048_576);
        (init, 1_048_576usize)
    } else {
        (65_536usize, 589_824usize)
    };
    let (ck_init_elems, ck_extra_elems) = if total_click_bytes >= 2_000_000 {
        let init = (((total_click_bytes as usize / 36 / nt_read).max(65_536) + 255) & !255)
            .max(65_536);
        (init, 262_144usize)
    } else {
        (16_384usize, 65_536usize)
    };

    let tid_bits = if nt_read <= 1 {
        0
    } else {
        (nt_read - 1).ilog2() + 1
    };
    let idx_bits = 30 - tid_bits;
    let idx_mask = (1u64 << idx_bits) - 1;

    // Stage 2: Parallel mmap CSV scan + NtSlabBuckets routing
    let t_read = Instant::now();
    let next_ev_job = AtomicUsize::new(0);
    let next_ck_job = AtomicUsize::new(0);
    let thread_outs: Vec<ReaderThreadOut> = std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(nt_read);
        let next_ev_ref = &next_ev_job;
        let next_ck_ref = &next_ck_job;
        let ev_jobs_ref = &event_jobs;
        let ck_jobs_ref = &click_jobs;
        let ev_files_ref = &ev_open_files;
        let ck_files_ref = &ck_open_files;
        let date_cache_ref = &date_cache;
        let products_ref = &products;
        let campaigns_ref = &campaigns;
        let overflow_ref = &overflow;
        let link_redirect_ref = &link_redirect;

        for tid in 0..nt_read {
            handles.push(s.spawn(move || {
                let mut eid_staged: NtSlabBuckets<RawEvent16, 4, NT_RAW_BLOCK> =
                    NtSlabBuckets::new(EID_BUCKETS, ev_init_elems, ev_extra_elems);
                let mut extras: Vec<ExtraEvent> = Vec::new();
                let mut extra_ck_buckets: Vec<Vec<ExtraClick>> = Vec::new();
                let mut ck_compact_staged: Option<NtSlabBuckets<SurvClick16, 4, NT_CLICK_BLOCK>> =
                    None;
                let mut ck_wide_staged: Option<DirectSlabBuckets<SurvClickWide, WIDE_BLOCK>> = None;

                drain_event_jobs(
                    next_ev_ref,
                    ev_jobs_ref,
                    ev_files_ref,
                    date_cache_ref,
                    win_start,
                    win_len_u64,
                    use_compact_ts,
                    products_ref,
                    overflow_ref,
                    tid,
                    idx_bits,
                    &mut extras,
                    &mut eid_staged,
                );

                drain_click_jobs(
                    next_ck_ref,
                    ck_jobs_ref,
                    ck_files_ref,
                    campaigns_ref,
                    date_cache_ref,
                    win_len_i64,
                    win_start,
                    overflow_ref,
                    use_compact_ts,
                    ck_init_elems,
                    ck_extra_elems,
                    link_redirect_ref,
                    &mut extra_ck_buckets,
                    &mut ck_compact_staged,
                    &mut ck_wide_staged,
                );

                ReaderThreadOut {
                    eid_out: eid_staged.finish(),
                    extras,
                    extra_ck_buckets,
                    ck_compact_out: ck_compact_staged.map(|s| s.finish()),
                    ck_wide_out: ck_wide_staged.map(|s| s.finish()),
                }
            }));
        }

        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let dt_read = t_read.elapsed();

    let mut all_raw_slabs: Vec<Vec<HugeSlab<RawEvent16>>> = Vec::with_capacity(nt_read);
    let mut eid_runs: Vec<Vec<(u8, u16, u32, u16)>> =
        (0..EID_BUCKETS).map(|_| Vec::with_capacity(nt_read)).collect();
    let mut all_ck_compact_slabs: Vec<Vec<HugeSlab<SurvClick16>>> = if use_compact_ts {
        Vec::with_capacity(nt_read)
    } else {
        Vec::new()
    };
    let mut ck_compact_runs: Vec<Vec<(u8, u16, u32, u16)>> = if use_compact_ts {
        (0..PERSON_BUCKETS)
            .map(|_| Vec::with_capacity(nt_read))
            .collect()
    } else {
        Vec::new()
    };
    let mut all_ck_wide_slabs: Vec<Vec<HugeSlab<SurvClickWide>>> = if !use_compact_ts {
        Vec::with_capacity(nt_read)
    } else {
        Vec::new()
    };
    let mut ck_wide_runs: Vec<Vec<(u8, u16, u32, u16)>> = if !use_compact_ts {
        (0..PERSON_BUCKETS)
            .map(|_| Vec::with_capacity(nt_read))
            .collect()
    } else {
        Vec::new()
    };
    let mut all_extras: Vec<Vec<ExtraEvent>> = Vec::with_capacity(nt_read);
    let mut ck_extra_per_bucket: Vec<Vec<ExtraClick>> =
        (0..PERSON_BUCKETS).map(|_| Vec::new()).collect();

    for (tid_idx, tout) in thread_outs.into_iter().enumerate() {
        let rtid = tid_idx as u8;
        all_extras.push(tout.extras);
        if !tout.extra_ck_buckets.is_empty() {
            for (pb, v) in tout.extra_ck_buckets.into_iter().enumerate() {
                if !v.is_empty() {
                    ck_extra_per_bucket[pb].extend_from_slice(&v);
                }
            }
        }
        all_raw_slabs.push(tout.eid_out.slabs);
        for (b, runs) in tout.eid_out.bucket_runs.into_iter().enumerate() {
            for (sidx, start, len) in runs {
                eid_runs[b].push((rtid, sidx, start, len));
            }
        }
        if use_compact_ts {
            if let Some(ck_out) = tout.ck_compact_out {
                let ctid = all_ck_compact_slabs.len() as u8;
                all_ck_compact_slabs.push(ck_out.slabs);
                for (pb, runs) in ck_out.bucket_runs.into_iter().enumerate() {
                    for (sidx, start, len) in runs {
                        ck_compact_runs[pb].push((ctid, sidx, start, len));
                    }
                }
            }
        } else if let Some(ck_out) = tout.ck_wide_out {
            let ctid = all_ck_wide_slabs.len() as u8;
            all_ck_wide_slabs.push(ck_out.slabs);
            for (pb, runs) in ck_out.bucket_runs.into_iter().enumerate() {
                for (sidx, start, len) in runs {
                    ck_wide_runs[pb].push((ctid, sidx, start, len));
                }
            }
        }
    }

    // Stage 3: Dedup & emit into split DirectSlabBuckets<u64> + DirectSlabBuckets<ExtraSurv>
    let t_dedup = Instant::now();
    let next_eb = AtomicUsize::new(0);
    let links_empty = link_redirect.is_empty();
    let (surv_init_elems, surv_extra_elems) = if total_event_bytes >= 10_000_000 {
        let init = (((total_event_bytes as usize / 52 / nt_read).max(1_048_576) + 511) & !511)
            .max(1_048_576);
        (init, 1_048_576usize)
    } else {
        (16_384usize, 589_824usize)
    };
    let (ex_init_elems, ex_extra_elems) = if total_event_bytes >= 10_000_000 {
        (131_072usize, 131_072usize)
    } else {
        (8_192usize, 16_384usize)
    };

    let (dedup_compact_outs, dedup_ex_outs, dedup_wide_outs): (
        Vec<SlabBucketsOut<u64>>,
        Vec<SlabBucketsOut<ExtraSurv>>,
        Vec<SlabBucketsOut<SurvEventWide>>,
    ) = std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(nt_read);
        let all_raw_slabs_ref = &all_raw_slabs;
        let eid_runs_ref = &eid_runs;
        let all_extras_ref = &all_extras;
        let link_redirect_ref = &link_redirect;
        let next_eb_ref = &next_eb;

        for _stid in 0..nt_read {
            handles.push(s.spawn(move || {
                let mut ev_compact = if use_compact_ts {
                    Some(NtSlabBuckets::<u64, 8, NT_SURV_BLOCK>::new(
                        PERSON_BUCKETS,
                        surv_init_elems,
                        surv_extra_elems,
                    ))
                } else {
                    None
                };
                let mut ex_compact: Option<DirectSlabBuckets<ExtraSurv, EX_SURV_BLOCK>> = None;
                let mut ev_wide = if !use_compact_ts {
                    Some(DirectSlabBuckets::new(
                        PERSON_BUCKETS,
                        surv_init_elems,
                        surv_extra_elems,
                    ))
                } else {
                    None
                };
                let mut slots: Vec<u32> = Vec::new();
                let mut kept: Vec<RawEvent16> = Vec::new();
                let mut chunk_refs: Vec<&[RawEvent16]> = Vec::new();

                loop {
                    let b = next_eb_ref.fetch_add(1, MemOrdering::Relaxed);
                    if b >= EID_BUCKETS {
                        break;
                    }
                    let runs = &eid_runs_ref[b];
                    if runs.is_empty() {
                        continue;
                    }
                    let total_recs: usize = runs.iter().map(|r| r.3 as usize).sum();
                    if total_recs == 0 {
                        continue;
                    }

                    chunk_refs.clear();
                    for &(rtid, sidx, start, len) in runs {
                        let chunk = unsafe {
                            SlabBucketsOut::get_slice(all_raw_slabs_ref, rtid, sidx, start, len)
                        };
                        if !chunk.is_empty() {
                            chunk_refs.push(chunk);
                        }
                    }
                    dedup_and_emit(
                        &chunk_refs,
                        total_recs,
                        all_extras_ref,
                        link_redirect_ref,
                        links_empty,
                        win_start,
                        idx_bits,
                        idx_mask,
                        ex_init_elems,
                        ex_extra_elems,
                        &mut slots,
                        &mut kept,
                        &mut ev_compact,
                        &mut ex_compact,
                        &mut ev_wide,
                    );
                }

                (
                    ev_compact.map(|s| s.finish()),
                    ex_compact.map(|s| s.finish()),
                    ev_wide.map(|s| s.finish()),
                )
            }));
        }

        let mut comp = Vec::new();
        let mut ex_outs = Vec::new();
        let mut wide = Vec::new();
        for h in handles {
            let (c, ex, w) = h.join().unwrap();
            if let Some(x) = c {
                comp.push(x);
            }
            if let Some(x) = ex {
                ex_outs.push(x);
            }
            if let Some(x) = w {
                wide.push(x);
            }
        }
        (comp, ex_outs, wide)
    });
    let dt_dedup = t_dedup.elapsed();

    // Stage 4: Sessionize, attribute, write parquet & drop prior inputs/slabs via drop_gate
    let t_sess = Instant::now();
    let user_daily_dir = out_dir.join("user_daily");
    let camp_daily_dir = out_dir.join("campaign_daily");
    fs::create_dir_all(&user_daily_dir).expect("mkdir user_daily");
    fs::create_dir_all(&camp_daily_dir).expect("mkdir campaign_daily");

    let drop_gate = (Mutex::new(false), Condvar::new());
    let (camp_stores, wrote_any_user) = std::thread::scope(|s| {
        let drop_gate_ref = &drop_gate;
        let drop_handle = s.spawn(move || {
            {
                let mut g = drop_gate_ref.0.lock().unwrap();
                while !*g {
                    g = drop_gate_ref.1.wait(g).unwrap();
                }
            }
            drop(ev_open_files);
            drop(ck_open_files);
            drop(event_jobs);
            drop(click_jobs);
            drop(eid_runs);
            drop(all_raw_slabs);
            drop(all_extras);
            drop(link_redirect);
        });

        let res = if use_compact_ts {
            let mut all_ev_slabs: Vec<Vec<HugeSlab<u64>>> =
                Vec::with_capacity(dedup_compact_outs.len());
            let mut ev_runs: Vec<Vec<(u8, u16, u32, u16)>> = (0..PERSON_BUCKETS)
                .map(|_| Vec::with_capacity(dedup_compact_outs.len()))
                .collect();
            for (tid_idx, out) in dedup_compact_outs.into_iter().enumerate() {
                let rtid = tid_idx as u8;
                all_ev_slabs.push(out.slabs);
                for (pb, runs) in out.bucket_runs.into_iter().enumerate() {
                    for (sidx, start, len) in runs {
                        ev_runs[pb].push((rtid, sidx, start, len));
                    }
                }
            }
            let mut all_ex_slabs: Vec<Vec<HugeSlab<ExtraSurv>>> =
                Vec::with_capacity(dedup_ex_outs.len());
            let mut ex_ev_runs: Vec<Vec<(u8, u16, u32, u16)>> = (0..PERSON_BUCKETS)
                .map(|_| Vec::with_capacity(dedup_ex_outs.len()))
                .collect();
            for (tid_idx, out) in dedup_ex_outs.into_iter().enumerate() {
                let xtid = tid_idx as u8;
                all_ex_slabs.push(out.slabs);
                for (pb, runs) in out.bucket_runs.into_iter().enumerate() {
                    for (sidx, start, len) in runs {
                        ex_ev_runs[pb].push((xtid, sidx, start, len));
                    }
                }
            }
            process_person_buckets_compact(
                nt_read,
                &all_ev_slabs,
                &ev_runs,
                &all_ex_slabs,
                &ex_ev_runs,
                &all_ck_compact_slabs,
                &ck_compact_runs,
                &ck_extra_per_bucket,
                &overflow,
                &products,
                &campaigns,
                win_start,
                min_day,
                day_span,
                &user_daily_dir,
                drop_gate_ref,
            )
        } else {
            let mut all_ev_slabs: Vec<Vec<HugeSlab<SurvEventWide>>> =
                Vec::with_capacity(dedup_wide_outs.len());
            let mut ev_runs: Vec<Vec<(u8, u16, u32, u16)>> = (0..PERSON_BUCKETS)
                .map(|_| Vec::with_capacity(dedup_wide_outs.len()))
                .collect();
            for (tid_idx, out) in dedup_wide_outs.into_iter().enumerate() {
                let rtid = tid_idx as u8;
                all_ev_slabs.push(out.slabs);
                for (pb, runs) in out.bucket_runs.into_iter().enumerate() {
                    for (sidx, start, len) in runs {
                        ev_runs[pb].push((rtid, sidx, start, len));
                    }
                }
            }
            process_person_buckets_wide(
                nt_read,
                &all_ev_slabs,
                &ev_runs,
                &all_ck_wide_slabs,
                &ck_wide_runs,
                &overflow,
                &products,
                &campaigns,
                min_day,
                day_span,
                &user_daily_dir,
                drop_gate_ref,
            )
        };
        {
            let mut g = drop_gate_ref.0.lock().unwrap();
            if !*g {
                *g = true;
                drop_gate_ref.1.notify_all();
            }
        }
        drop_handle.join().unwrap();
        res
    });

    if !wrote_any_user {
        write_empty_user_daily(&user_daily_dir.join("part-00000.parquet"));
    }

    // Merge thread-local CampStores and write campaign_daily/part-00000.parquet
    let num_camps = campaigns.ids.len();
    let mut cd_ids: Vec<&str> = Vec::new();
    let mut cd_channels: Vec<&str> = Vec::new();
    let mut cd_days: Vec<i32> = Vec::new();
    let mut cd_sessions: Vec<i32> = Vec::new();
    let mut cd_purchasing: Vec<i32> = Vec::new();
    let mut cd_revenues: Vec<i64> = Vec::new();
    let mut cd_assisted: Vec<i32> = Vec::new();

    if day_span <= 128 && num_camps <= 20_000 {
        let total_slots = num_camps * day_span;
        let mut merged_sessions = vec![0i32; total_slots];
        let mut merged_purch_rev = vec![(0i32, 0i64); total_slots];
        let mut merged_assisted = vec![0i32; total_slots];
        for st in &camp_stores {
            if let CampStore::Flat {
                sess_ass,
                purch_rev,
                organic_sess,
                organic_id,
                day_span: ds,
                ..
            } = st
            {
                for (i, &(sv, av)) in sess_ass.iter().enumerate() {
                    if sv != 0 {
                        merged_sessions[i] += sv;
                    }
                    if av != 0 {
                        merged_assisted[i] += av;
                    }
                }
                let org_base = (*organic_id as usize) * (*ds);
                for d in 0..(*ds) {
                    let ov = organic_sess[d];
                    if ov != 0 {
                        merged_sessions[org_base + d] += ov;
                    }
                }
                for (i, &(ps, rev)) in purch_rev.iter().enumerate() {
                    if ps != 0 {
                        merged_purch_rev[i].0 += ps;
                        merged_purch_rev[i].1 += rev;
                    }
                }
            }
        }
        for c in 0..num_camps {
            for d in 0..day_span {
                let slot = c * day_span + d;
                let sess = merged_sessions[slot];
                let ass = merged_assisted[slot];
                if sess > 0 || ass > 0 {
                    let (ps, rev) = merged_purch_rev[slot];
                    cd_ids.push(&campaigns.ids[c]);
                    cd_channels.push(&campaigns.channels[c]);
                    cd_days.push(min_day + d as i32);
                    cd_sessions.push(sess);
                    cd_purchasing.push(ps);
                    cd_revenues.push(rev);
                    cd_assisted.push(ass);
                }
            }
        }
    } else {
        let mut merged: HashMap<(u32, i32), CampAgg, RandomState> =
            HashMap::with_hasher(RandomState::with_seeds(3, 5, 7, 9));
        for st in camp_stores {
            if let CampStore::Map(m) = st {
                for (k, agg) in m {
                    let e = merged.entry(k).or_default();
                    e.sessions += agg.sessions;
                    e.purchasing_sessions += agg.purchasing_sessions;
                    e.revenue_cents += agg.revenue_cents;
                    e.assisted_sessions += agg.assisted_sessions;
                }
            }
        }
        for ((c, day), agg) in merged {
            if agg.sessions > 0 || agg.assisted_sessions > 0 {
                cd_ids.push(&campaigns.ids[c as usize]);
                cd_channels.push(&campaigns.channels[c as usize]);
                cd_days.push(day);
                cd_sessions.push(agg.sessions);
                cd_purchasing.push(agg.purchasing_sessions);
                cd_revenues.push(agg.revenue_cents);
                cd_assisted.push(agg.assisted_sessions);
            }
        }
    }

    write_campaign_daily_parquet(
        &camp_daily_dir.join("part-00000.parquet"),
        &cd_ids,
        &cd_channels,
        &cd_days,
        &cd_sessions,
        &cd_purchasing,
        &cd_revenues,
        &cd_assisted,
    );

    let dt_sess = t_sess.elapsed();
    eprintln!(
        "PROFILE links={:.3}s read={:.3}s dedup={:.3}s sess_write={:.3}s total={:.3}s",
        dt_links.as_secs_f64(),
        dt_read.as_secs_f64(),
        dt_dedup.as_secs_f64(),
        dt_sess.as_secs_f64(),
        t_total.elapsed().as_secs_f64()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nt_slab_buckets_multi_slab_and_partial_flushes() {
        // Test RawEvent16 (K=4, BLOCK=256) with forced multi-slab rollovers & partial tail flushes
        let mut nt_raw: NtSlabBuckets<RawEvent16, 4, 256> = NtSlabBuckets::new(16, 1024, 1024);
        let mut expected_raw: Vec<(u64, u64)> = Vec::new();
        for b in 0..16usize {
            let n = 250 + b;
            for i in 0..n {
                let item = RawEvent16 {
                    lo: ((b as u64) << 32) | (i as u64),
                    hi: ((b as u64) << 16) ^ (i as u64),
                };
                expected_raw.push((item.lo, item.hi));
                nt_raw.push(b, item);
            }
        }
        let out_raw = nt_raw.finish();
        assert!(out_raw.slabs.len() > 2, "expected multiple slab rollovers");
        let mut got_raw: Vec<(u64, u64)> = Vec::new();
        for b in 0..16usize {
            for &(sidx, start, len) in &out_raw.bucket_runs[b] {
                let slice = unsafe {
                    SlabBucketsOut::get_slice(
                        std::slice::from_ref(&out_raw.slabs),
                        0,
                        sidx,
                        start,
                        len,
                    )
                };
                for r in slice {
                    got_raw.push((r.lo, r.hi));
                }
            }
        }
        expected_raw.sort_unstable();
        got_raw.sort_unstable();
        assert_eq!(got_raw, expected_raw, "NtSlabBuckets RawEvent16 mismatch");

        // Test u64 (K=8, BLOCK=512) with partial tail lengths 0..8 and slab rollover
        let mut nt_ev: NtSlabBuckets<u64, 8, 512> = NtSlabBuckets::new(12, 1024, 1024);
        let mut expected_ev: Vec<u64> = Vec::new();
        for b in 0..12usize {
            let n = 508 + b;
            for i in 0..n {
                let item = ((b as u64) << 40) | ((i as u64) * 10 + 2);
                expected_ev.push(item);
                nt_ev.push(b, item);
            }
        }
        let out_ev = nt_ev.finish();
        assert!(out_ev.slabs.len() > 2, "expected multiple u64 slabs");
        let mut got_ev: Vec<u64> = Vec::new();
        for b in 0..12usize {
            for &(sidx, start, len) in &out_ev.bucket_runs[b] {
                let slice = unsafe {
                    SlabBucketsOut::get_slice(
                        std::slice::from_ref(&out_ev.slabs),
                        0,
                        sidx,
                        start,
                        len,
                    )
                };
                got_ev.extend_from_slice(slice);
            }
        }
        expected_ev.sort_unstable();
        got_ev.sort_unstable();
        assert_eq!(got_ev, expected_ev, "NtSlabBuckets u64 mismatch");
    }

    #[test]
    fn verify_avx2_bitonic_01() {
        for n in 2..=8usize {
            let lim = 1u32 << n;
            for pat in 0..lim {
                let mut got: Vec<u32> = (0..n).map(|k| (pat >> k) & 1).collect();
                let mut exp = got.clone();
                exp.sort_unstable();
                sort_u32_small_2_16(&mut got);
                assert_eq!(got, exp, "0-1 mismatch n={n} pat={pat:#b}");
            }
        }
        for n in 9..=16usize {
            let lim = 1u32 << n;
            for pat in 0..lim {
                let mut got: Vec<u32> = (0..n).map(|k| (pat >> k) & 1).collect();
                let mut exp = got.clone();
                exp.sort_unstable();
                sort_u32_small_2_16(&mut got);
                assert_eq!(got, exp, "0-1 mismatch n={n} pat={pat:#b}");
            }
        }
        let mut x = 0x1234_5678_9ABC_DEF0u64;
        for n in 2..=32usize {
            let mut got = Vec::with_capacity(n);
            for k in 0..n {
                x = mix64(x);
                got.push(if k % 5 == 0 {
                    u32::MAX
                } else {
                    (x >> 32) as u32
                });
            }
            let mut exp = got.clone();
            exp.sort_unstable();
            sort_u32_small(&mut got);
            assert_eq!(got, exp, "u32::MAX mixed mismatch n={n}");
        }
    }

    #[test]
    fn sort_ts_u32_networks_msd_and_radix() {
        let mut tmp = Vec::new();
        for n in 2..=6usize {
            let vals: Vec<u32> = (0..n).map(|i| ((i / 2 + 1) * 1000) as u32).collect();
            let mut p: Vec<usize> = (0..n).collect();
            loop {
                let mut cur: Vec<u32> = p.iter().map(|&idx| vals[idx]).collect();
                let mut expected = cur.clone();
                expected.sort_unstable();
                sort_ts_u32(&mut cur, &mut tmp);
                assert_eq!(cur, expected, "perm mismatch n={n}");

                let mut i = n - 1;
                while i > 0 && p[i - 1] >= p[i] {
                    i -= 1;
                }
                if i == 0 {
                    break;
                }
                let mut j = n - 1;
                while p[j] <= p[i - 1] {
                    j -= 1;
                }
                p.swap(i - 1, j);
                p[i..].reverse();
            }
        }

        let mut x = 0x517c_c1b7_2722_0a95u64;
        for n in (0..=25).chain([32, 64, 100, 150, 160, 161, 200, 513, 1000, 2000, 5000, 10_000]) {
            let mut arr = Vec::with_capacity(n);
            for _ in 0..n {
                x = mix64(x);
                arr.push((x >> 32) as u32);
            }
            let mut expected = arr.clone();
            expected.sort_unstable();
            sort_ts_u32(&mut arr, &mut tmp);
            assert_eq!(arr, expected, "sort_ts_u32 full-range mismatch n={n}");

            // msd5_sort_u32 path (v >> 26 < 32), concentrated single bucket (>160)
            let mut arr_msd5 = Vec::with_capacity(n);
            for k in 0..n {
                x = mix64(x);
                let bucket5 = if k % 3 == 0 { 17u32 } else { (x as u32) & 31 };
                arr_msd5.push((bucket5 << 26) | ((x as u32) >> 6));
            }
            let mut exp_msd5 = arr_msd5.clone();
            exp_msd5.sort_unstable();
            sort_ts_u32(&mut arr_msd5, &mut tmp);
            assert_eq!(arr_msd5, exp_msd5, "sort_ts_u32 msd5 mismatch n={n}");

            // msd8_sort_u32 path (v >> 24 < 128), concentrated single bucket (>160)
            let mut arr_msd8 = Vec::with_capacity(n);
            for k in 0..n {
                x = mix64(x);
                let bucket = if k % 3 == 0 { 17u32 } else { (x as u32) & 63 };
                arr_msd8.push((bucket << 24) | ((x as u32) >> 8));
            }
            let mut exp_msd8 = arr_msd8.clone();
            exp_msd8.sort_unstable();
            sort_ts_u32(&mut arr_msd8, &mut tmp);
            assert_eq!(arr_msd8, exp_msd8, "sort_ts_u32 msd8 mismatch n={n}");
        }
    }

    #[test]
    fn find_session_end_u32_matches_scalar() {
        let gap = SESSION_GAP_MS as u32;
        fn scalar_end(ts: &[u32], i: usize) -> usize {
            let mut prev = ts[i];
            let mut j = i + 1;
            while j < ts.len() && ts[j] - prev <= (SESSION_GAP_MS as u32) {
                prev = ts[j];
                j += 1;
            }
            j
        }
        for n in 1..=64usize {
            for split_pos in 1..=n {
                for &step in &[gap, gap + 1] {
                    let mut ts = Vec::with_capacity(n);
                    let mut cur = 10_000u32;
                    ts.push(cur);
                    for idx in 1..n {
                        cur += if idx == split_pos { step } else { (idx as u32) * 100 };
                        ts.push(cur);
                    }
                    for i in 0..n {
                        assert_eq!(
                            find_session_end_u32(&ts, i),
                            scalar_end(&ts, i),
                            "mismatch n={n} split={split_pos} step={step} i={i}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn sort_ck_u64_tie_break() {
        let interner = OverflowInterner::new();
        let cids_all = [
            b"k1234567".as_slice(),
            b"k1234568".as_slice(),
            b"k12345678".as_slice(),
            b"k1".as_slice(),
            b"click_overflow_very_long_id".as_slice(),
            b"A_prefix_before_k".as_slice(),
        ];

        for all_b37 in [true, false] {
            let pool = if all_b37 { &cids_all[..3] } else { &cids_all[..] };
            for n in [2usize, 3, 5, 8, 16, 17, 64] {
                let mut matched_cids = Vec::with_capacity(n);
                let mut keys = Vec::with_capacity(n);
                let mut orig_meta = Vec::with_capacity(n);
                for i in 0..n {
                    let ts_rel = ((n - 1 - i) / 3 * 5_000) as u64;
                    let raw_cid = pool[(i * 7 + 3) % pool.len()];
                    let cid_k = interner.encode_click(raw_cid);
                    let camp_idx = (i * 11 + 1) as u64;
                    matched_cids.push(cid_k);
                    keys.push((ts_rel << 33) | ((i as u64) << 16) | camp_idx);
                    orig_meta.push((ts_rel, raw_cid, camp_idx));
                }
                let mut exp = orig_meta.clone();
                exp.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| b.1.cmp(a.1)));

                sort_ck_u64(&mut keys, &matched_cids, all_b37, &interner);

                let got: Vec<(u64, &[u8], u64)> = keys
                    .iter()
                    .map(|&k| {
                        let ts = k >> 33;
                        let idx = ((k >> 16) & 0x1_FFFF) as usize;
                        let camp = k & 0xFFFF;
                        let ck_raw = if all_b37 {
                            pool.iter()
                                .copied()
                                .find(|&s| interner.encode_click(s) == matched_cids[idx])
                                .unwrap()
                        } else {
                            let ck = matched_cids[idx];
                            pool.iter()
                                .copied()
                                .find(|&s| interner.encode_click(s) == ck)
                                .unwrap()
                        };
                        (ts, ck_raw, camp)
                    })
                    .collect();
                assert_eq!(got.len(), exp.len());
                for (g, e) in got.iter().zip(exp.iter()) {
                    assert_eq!((g.0, g.1), (e.0, e.1), "tie break mismatch n={n} all_b37={all_b37}");
                }
            }
        }
    }

    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    #[test]
    fn inline_tomb_rejects_malformed_uid_quote() {
        let dir = std::env::temp_dir().join("shopping_inline_tomb_malformed_uid_test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let prod_path = dir.join("products.csv");
        std::fs::write(&prod_path, b"product_id,category\np1,apparel\n").unwrap();
        let products = ProductIndex::load(&prod_path);

        let win_start = parse_ts(b"2026-09-01T00:00:00Z").unwrap();
        let win_end = parse_ts(b"2026-09-15T00:00:00Z").unwrap();
        let win_len_u64 = (win_end - win_start) as u64;
        let date_cache = FastDateCache::new(win_start, win_end);
        let overflow = OverflowInterner::new();
        let mut extras: Vec<ExtraEvent> = Vec::new();
        let mut eid_staged: NtSlabBuckets<RawEvent16, 4, NT_RAW_BLOCK> =
            NtSlabBuckets::new(EID_BUCKETS, 1024, 1024);

        // Place the malformed out-of-window line early so it executes inside the fast path
        // (well before stop_excl - 144), followed by a valid in-window event with later ingest_ts
        // and padding rows. If InlineTomb accepted the malformed uid quote, it would emit a tombstone
        // that overrides e1234567!
        let csv = concat!(
            "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n",
            "e1234567,u12\"4567,2026-08-01T00:00:00Z,2026-09-01T00:00:00Z,view,p1,\n",
            "e1234567,u1234567,2026-09-02T00:00:00Z,2026-09-02T00:05:00Z,view,p1,\n",
            "e9999991,u9999991,2026-09-03T00:00:00Z,2026-09-03T00:05:00Z,view,p1,\n",
            "e9999992,u9999992,2026-09-04T00:00:00Z,2026-09-04T00:05:00Z,view,p1,\n",
            "e9999993,u9999993,2026-09-05T00:00:00Z,2026-09-05T00:05:00Z,view,p1,\n",
        );
        unsafe {
            scan_events_direct_compact(
                csv.as_bytes(),
                0,
                csv.len(),
                &date_cache,
                win_start,
                win_len_u64,
                &products,
                &overflow,
                0,
                27,
                &mut extras,
                &mut eid_staged,
            );
        }

        let out = eid_staged.finish();
        let target_eid = pack_b37(b"e1234567", b'e').unwrap();
        let target_uid = pack_b37(b"u1234567", b'u').unwrap();
        let th = mix42(target_eid);
        let tb = (th as usize) & EID_MASK;

        let mut records_for_e1234567 = Vec::new();
        for &(sidx, start, len) in &out.bucket_runs[tb] {
            let sl = unsafe {
                SlabBucketsOut::get_slice(std::slice::from_ref(&out.slabs), 0, sidx, start, len)
            };
            for &r in sl {
                if (r.lo >> 32) == (th >> EID_BITS) {
                    records_for_e1234567.push(r);
                }
            }
        }
        assert_eq!(
            records_for_e1234567.len(),
            1,
            "malformed uid line must not emit a tombstone; expected only the 1 valid event"
        );
        assert_eq!(records_for_e1234567[0].lo & RAW_SPECIAL_BIT, 0);
        assert_eq!(records_for_e1234567[0].hi & MASK42, target_uid);
        assert!(extras.is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn raw_event16_ing_delta_subsec_and_tombstone_cmp() {
        let win_start = 1_700_000_000_000i64;
        let idx_bits = 27u32;
        let idx_mask = (1u64 << idx_bits) - 1;

        let make_inline = |eid_idx: u64, ts_rel_ms: i64, ing_delta_sec: i64| -> RawEvent16 {
            let eid_k = pack_b37(format!("e{eid_idx:07}").as_bytes(), b'e').unwrap();
            let uid_k = pack_b37(b"u0000001", b'u').unwrap();
            let h = mix42(eid_k);
            let ts_rel_sec = ts_rel_ms.div_euclid(1_000);
            let ing_sec = ts_rel_sec + ing_delta_sec;
            let ing_delta_b = (ing_sec - ts_rel_sec) + ING_DELTA_BIAS;
            let lo = (((h >> EID_BITS) as u32 as u64) << 32) | (ts_rel_ms as u64);
            let hi = uid_k | ((ing_delta_b as u64) << 45);
            RawEvent16 { lo, hi }
        };

        let make_tomb = |eid_idx: u64, ing_rel_sec: i64| -> RawEvent16 {
            let eid_k = pack_b37(format!("e{eid_idx:07}").as_bytes(), b'e').unwrap();
            let h = mix42(eid_k);
            let tb = (ing_rel_sec + TOMB_ING_BIAS) as u64;
            let lo = (((h >> EID_BITS) as u32 as u64) << 32) | RAW_SPECIAL_BIT | tb;
            RawEvent16 { lo, hi: 0 }
        };

        let mut extras = vec![vec![
            ExtraEvent {
                eid_k: pack_b37(b"e0000001", b'e').unwrap(),
                uid_k: pack_b37(b"u0000001", b'u').unwrap(),
                ts: win_start + 5_000,
                ingest_ts: win_start + 10_000,
                price_cents: 500,
                cat_id: 2,
                valid: true,
            },
            ExtraEvent {
                eid_k: pack_b37(b"e0000001", b'e').unwrap(),
                uid_k: 0,
                ts: win_start,
                ingest_ts: win_start + 10_500,
                price_cents: 0,
                cat_id: 0,
                valid: false,
            },
        ]];

        // Sub-second ts_rel_ms (e.g., 5_999 ms -> ts_sec=5): 5 + (-2) = 3 vs tombstone 4 => inline(3) < tomb(4)
        let r_inline_subsec = make_inline(1, 5_999, -2); // ing_rel_sec = 3
        let r_tomb_early = make_tomb(1, 4); // ing_rel_sec = 4
        let r_tomb_equal = make_tomb(1, 3); // ing_rel_sec = 3
        assert!(raw16_same_eid(&r_inline_subsec, &r_tomb_early, &extras, idx_bits, idx_mask));
        assert_eq!(
            cmp_ingest16(&r_inline_subsec, &r_tomb_early, &extras, win_start, idx_bits, idx_mask),
            Ordering::Less
        );
        assert_eq!(
            cmp_ingest16(&r_inline_subsec, &r_tomb_equal, &extras, win_start, idx_bits, idx_mask),
            Ordering::Equal
        );

        // ExtraEvent with eref -> eid_1
        let eid_k1 = pack_b37(b"e0000001", b'e').unwrap();
        let h1 = mix42(eid_k1);
        let r_extra_valid = RawEvent16 {
            lo: (((h1 >> EID_BITS) as u32 as u64) << 32) | RAW_SPECIAL_BIT,
            hi: 1, // eref 0 -> ing = +10.0s
        };
        let r_extra_subsec = RawEvent16 {
            lo: (((h1 >> EID_BITS) as u32 as u64) << 32) | RAW_SPECIAL_BIT,
            hi: 2, // eref 1 -> ing = +10.5s
        };
        assert_eq!(
            cmp_ingest16(&r_tomb_early, &r_extra_valid, &extras, win_start, idx_bits, idx_mask),
            Ordering::Less
        );
        assert_eq!(
            cmp_ingest16(&r_extra_valid, &r_extra_subsec, &extras, win_start, idx_bits, idx_mask),
            Ordering::Less
        );

        // Unpack check on inline sub-second RawEvent16
        assert_eq!(
            unpack_surv_source(r_inline_subsec, &extras, win_start, idx_bits, idx_mask),
            Some((pack_b37(b"u0000001", b'u').unwrap(), 5_999, 0))
        );
        assert_eq!(
            unpack_surv_source(r_tomb_early, &extras, win_start, idx_bits, idx_mask),
            None
        );

        // Different eid differing in lo[32..64] must NOT compare equal
        let mut r_other = r_inline_subsec;
        r_other.lo ^= 1u64 << 32;
        assert!(!raw16_same_eid(&r_inline_subsec, &r_other, &extras, idx_bits, idx_mask));
        extras.clear();
    }
}
