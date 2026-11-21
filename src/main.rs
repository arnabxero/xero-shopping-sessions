//! Shopping Sessions pipeline.
//!
//! Stages: init (manifest, products, campaigns, mmaps) -> links (exact Key8
//! interning, lock-free union-find, LinkTable + Bloom) -> clicks (parse, validate,
//! resolve person, scatter to person shards) -> events (parse, validate, scatter a
//! compact RawEv to event_id shards) -> dedup (per event_id shard: exact dedup,
//! resolve person, scatter PEv to person shards) -> solve (per person shard:
//! sessionize, attribute, assist, aggregate) -> write Parquet.
//!
//! All shard data lives in pooled 2 MB blocks (`arena`), written through
//! write-combining staging lines, and is returned to the pool as soon as each
//! shard is consumed; the pool is released in parallel before exit.

mod arena;
mod csv;
mod event_front;
mod fixed;
#[allow(dead_code)]
mod hash;
mod key;
mod links;
mod parquet;
#[allow(dead_code)]
mod timebins;
mod ts;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Barrier, Mutex};
use std::time::Instant;

use arena::{ShardSet, Scatter, Segs};
use key::Key8;
use links::{prefetch, LinkTable, SEED_A};
use parquet::{Column, Kind};
use ts::{parse_ts, DateCache, DAY_MS};

const GAP_MS: i64 = 1_800_000;
const SEVEN_DAYS_MS: i64 = 7 * DAY_MS;
const LATE_MS: i64 = DAY_MS;

const SEED_ID: u64 = 0x4b33_a62e_d433_d4a3;
const SEED_MAP: u64 = 0x8bb8_4b93_962e_acc9;

const MAX_ROWS_PER_FILE: usize = 2_000_000;

pub(crate) struct SyncPtr(pub *mut u64);
unsafe impl Sync for SyncPtr {}
unsafe impl Send for SyncPtr {}

// ---------------------------------------------------------------------------
// mmap

struct Mmap {
    ptr: *const u8,
    len: usize,
}

impl Mmap {
    fn open(path: &Path) -> std::io::Result<Mmap> {
        use std::os::unix::io::AsRawFd;
        let f = std::fs::File::open(path)?;
        let len = f.metadata()?.len() as usize;
        if len == 0 {
            return Ok(Mmap { ptr: std::ptr::NonNull::<u8>::dangling().as_ptr(), len: 0 });
        }
        let p = unsafe {
            libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ, libc::MAP_PRIVATE, f.as_raw_fd(), 0)
        };
        if p == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Mmap { ptr: p as *const u8, len })
    }
    fn data(&self) -> &'static [u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            let name = e.file_name();
            let name = name.to_string_lossy();
            if name.ends_with(".csv") && p.is_file() {
                v.push(p);
            }
        }
    }
    v.sort();
    v
}

fn map_dir(dir: &Path) -> Vec<&'static [u8]> {
    list_csv(dir)
        .iter()
        .map(|p| {
            // Mappings live for the whole process (no Drop / munmap).
            Mmap::open(p).unwrap_or_else(|e| panic!("mmap {}: {e}", p.display())).data()
        })
        .collect()
}

#[derive(Clone, Copy)]
pub(crate) struct Chunk {
    pub data: &'static [u8],
}

/// Splits every file's body (after its header line) into newline-aligned chunks.
pub(crate) fn make_chunks(files: &[&'static [u8]], target: usize) -> Vec<Chunk> {
    let mut out = Vec::new();
    for &d in files {
        let start = match d.iter().position(|&c| c == b'\n') {
            Some(i) => i + 1,
            None => continue,
        };
        let mut s = start;
        while s < d.len() {
            let mut e = (s + target).min(d.len());
            if e < d.len() {
                e = match d[e..].iter().position(|&c| c == b'\n') {
                    Some(i) => e + i + 1,
                    None => d.len(),
                };
            }
            out.push(Chunk { data: &d[s..e] });
            s = e;
        }
    }
    // Largest first for better balance.
    out.sort_by_key(|c| std::cmp::Reverse(c.data.len()));
    out
}

pub(crate) fn chunk_target(files: &[&'static [u8]], threads: usize) -> usize {
    let total: usize = files.iter().map(|d| d.len()).sum();
    (total / (threads * 12)).clamp(256 << 10, 16 << 20)
}

pub(crate) fn par<R: Send>(threads: usize, f: impl Fn(usize) -> R + Sync) -> Vec<R> {
    std::thread::scope(|s| {
        let f = &f;
        let hs: Vec<_> = (0..threads).map(|t| s.spawn(move || f(t))).collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

#[inline(always)]
pub(crate) fn populate(d: &[u8]) {
    #[cfg(target_os = "linux")]
    unsafe {
        // MADV_POPULATE_READ (Linux 5.14+); harmless EINVAL elsewhere.
        let page = 4096usize;
        let a = (d.as_ptr() as usize) & !(page - 1);
        let e = d.as_ptr() as usize + d.len();
        libc::madvise(a as *mut libc::c_void, e - a, 22);
    }
    #[cfg(not(target_os = "linux"))]
    let _ = d;
}

/// Drops the page-table entries of a fully parsed input chunk (whole pages only),
/// spreading the unmap work over the parse threads instead of process exit. The
/// mapping stays valid: a later access just re-reads the page cache.
#[inline(always)]
pub(crate) fn release_chunk(d: &[u8]) {
    #[cfg(all(target_os = "linux", not(test)))]
    unsafe {
        let page = 4096usize;
        let a = (d.as_ptr() as usize + page - 1) & !(page - 1);
        let e = (d.as_ptr() as usize + d.len()) & !(page - 1);
        if e > a {
            libc::madvise(a as *mut libc::c_void, e - a, libc::MADV_DONTNEED);
        }
    }
    #[cfg(any(not(target_os = "linux"), test))]
    let _ = d;
}

// ---------------------------------------------------------------------------
// inline Key8 string maps (products, campaigns) — zero pointer chase to CSV

struct KeyMap {
    slots: Vec<(Key8, u32)>,
    len: usize,
    mask: usize,
    fast_prefix: &'static [u8],
    fast: Vec<u32>,
}

#[inline(always)]
fn parse_canon_idx(s: &[u8], prefix: &[u8], limit: usize) -> Option<usize> {
    let plen = prefix.len();
    let n = s.len();
    if n <= plen || n > plen + 6 {
        return None;
    }
    let p = s.as_ptr();
    unsafe {
        for i in 0..plen {
            if *p.add(i) != *prefix.get_unchecked(i) {
                return None;
            }
        }
        let d0 = (*p.add(plen)).wrapping_sub(b'0');
        // Reject leading zero so "p01" / "c_01" always falls back to exact KeyMap.
        if d0 == 0 || d0 > 9 {
            return None;
        }
        let mut acc = d0 as usize;
        let mut i = plen + 1;
        while i < n {
            let d = (*p.add(i)).wrapping_sub(b'0');
            if d > 9 {
                return None;
            }
            acc = acc * 10 + d as usize;
            i += 1;
        }
        if acc < limit {
            Some(acc)
        } else {
            None
        }
    }
}

impl KeyMap {
    fn new(n: usize) -> KeyMap {
        let cap = (n * 2).next_power_of_two().max(16);
        KeyMap {
            slots: vec![(Key8::EMPTY, u32::MAX); cap],
            len: 0,
            mask: cap - 1,
            fast_prefix: b"",
            fast: Vec::new(),
        }
    }
    fn new_with_fast(_n: usize, fast_prefix: &'static [u8], fast_cap: usize) -> KeyMap {
        // Canonical IDs live solely in `fast`; `slots` starts tiny and grows lazily if non-canonical IDs appear.
        let cap = 16usize;
        KeyMap {
            slots: vec![(Key8::EMPTY, u32::MAX); cap],
            len: 0,
            mask: cap - 1,
            fast_prefix,
            fast: vec![u32::MAX; fast_cap],
        }
    }
    #[cold]
    fn grow_slots(&mut self) {
        let new_cap = (self.slots.len() * 2).max(32);
        let old = std::mem::replace(&mut self.slots, vec![(Key8::EMPTY, u32::MAX); new_cap]);
        self.mask = new_cap - 1;
        for (k, v) in old {
            if v != u32::MAX {
                let mut i = k.hash(SEED_MAP) as usize & self.mask;
                while self.slots[i].1 != u32::MAX {
                    i = (i + 1) & self.mask;
                }
                self.slots[i] = (k, v);
            }
        }
    }
    fn insert(&mut self, k: &[u8], v: u32) {
        if !self.fast.is_empty() {
            if let Some(idx) = parse_canon_idx(k, self.fast_prefix, self.fast.len()) {
                if self.fast[idx] == u32::MAX {
                    self.fast[idx] = v;
                }
                return;
            }
        }
        if self.len * 2 >= self.slots.len() {
            self.grow_slots();
        }
        let key = Key8::new(k);
        let mut i = key.hash(SEED_MAP) as usize & self.mask;
        loop {
            let s = &mut self.slots[i];
            if s.1 == u32::MAX {
                *s = (key, v);
                self.len += 1;
                return;
            }
            if s.0 == key {
                return; // first wins (keys are unique anyway)
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn get_hash(&self, k: &[u8]) -> Option<u32> {
        let key = Key8::new(k);
        let mut i = key.hash(SEED_MAP) as usize & self.mask;
        loop {
            let s = unsafe { self.slots.get_unchecked(i) };
            if s.1 == u32::MAX {
                return None;
            }
            if s.0 == key {
                return Some(s.1);
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn get(&self, k: &[u8]) -> Option<u32> {
        if !self.fast.is_empty() {
            if let Some(idx) = parse_canon_idx(k, self.fast_prefix, self.fast.len()) {
                let v = unsafe { *self.fast.get_unchecked(idx) };
                return if v == u32::MAX { None } else { Some(v) };
            }
        }
        self.get_hash(k)
    }
}

fn read_static(path: &Path) -> &'static [u8] {
    match std::fs::read(path) {
        Ok(v) => Box::leak(v.into_boxed_slice()),
        Err(_) => &[],
    }
}

fn body(d: &'static [u8]) -> &'static [u8] {
    match d.iter().position(|&c| c == b'\n') {
        Some(i) => &d[i + 1..],
        None => &[],
    }
}

// ---------------------------------------------------------------------------
// records

const TS_INVALID: u32 = u32::MAX;
const INGEST_OOB_BASE: u32 = 0xF000_0000;
const INGEST_NONE: u32 = u32::MAX;

/// One parsed event line of any validity (48 bytes), sharded by event_id.
/// `ingest` = ms offset from `base` (< INGEST_OOB_BASE), or `INGEST_OOB_BASE + idx`
/// into `oob_ingest`, or `INGEST_NONE`. `ehash` = low 32 bits of `eid.hash(SEED_ID)`
/// so `dedup` never re-hashes `eid`.
#[derive(Clone, Copy)]
#[repr(C)]
struct RawEv {
    eid: Key8,
    uid: Key8,
    ts: u32,
    price: u32,
    ingest: u32,
    ehash: u32,
}

#[inline(always)]
fn enc_ingest_fast(ms: Option<i64>, base: i64, oob: &Mutex<Vec<i64>>) -> u32 {
    match ms {
        Some(m) => {
            let d = m.wrapping_sub(base);
            if d >= 0 && (d as u64) < INGEST_OOB_BASE as u64 {
                d as u32
            } else {
                enc_ingest_oob(m, oob)
            }
        }
        None => INGEST_NONE,
    }
}

#[cold]
fn enc_ingest_oob(m: i64, oob: &Mutex<Vec<i64>>) -> u32 {
    let mut g = oob.lock().unwrap();
    let idx = g.len() as u32;
    g.push(m);
    INGEST_OOB_BASE + idx
}

#[inline(always)]
fn ingest_less(a: u32, b: u32, base: i64, oob: &[i64]) -> bool {
    if (a | b) < INGEST_OOB_BASE {
        a < b
    } else {
        ingest_less_slow(a, b, base, oob)
    }
}

#[cold]
fn ingest_less_slow(a: u32, b: u32, base: i64, oob: &[i64]) -> bool {
    let dec = |v: u32| -> (u8, i64) {
        if v == INGEST_NONE {
            (1, i64::MAX)
        } else if v >= INGEST_OOB_BASE {
            (0, oob[(v - INGEST_OOB_BASE) as usize])
        } else {
            (0, base + v as i64)
        }
    };
    dec(a) < dec(b)
}

/// A surviving event, sharded by person (24 bytes). `pc` is 0 for non-purchases;
/// otherwise price << 8 | category, or (with more than 256 categories) 1 + an
/// index into the wide side table.
#[derive(Clone, Copy)]
#[repr(C)]
struct PEv {
    person: Key8,
    ts: u32,
    pc: u32,
}

/// A valid click, sharded by raw uid / canonical person (24 bytes).
#[derive(Clone, Copy)]
#[repr(C)]
struct Click {
    person: Key8,
    cid: Key8,
    ts: u32,
    camp: u32,
}

/// Compact 8-byte event inside solve packed as `(ts << 32) | pc` for single-register sorting.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
struct Ev8(u64);

impl Ev8 {
    #[inline(always)]
    fn new(ts: u32, pc: u32) -> Ev8 {
        Ev8(((ts as u64) << 32) | pc as u64)
    }
    #[inline(always)]
    fn ts(self) -> u32 {
        (self.0 >> 32) as u32
    }
    #[inline(always)]
    fn pc(self) -> u32 {
        self.0 as u32
    }
}

/// Compact 24-byte click inside solve (2x smaller than Click).
#[derive(Clone, Copy)]
#[repr(C)]
struct Cl24 {
    cid: Key8,
    ts: u32,
    camp: u32,
}

const _: () = assert!(std::mem::size_of::<RawEv>() == 32);
const _: () = assert!(std::mem::size_of::<PEv>() == 16);
const _: () = assert!(std::mem::size_of::<Click>() == 24);
const _: () = assert!(std::mem::size_of::<Ev8>() == 8);
const _: () = assert!(std::mem::size_of::<Cl24>() == 16);
const _: () = assert!(arena::SEG / std::mem::size_of::<RawEv>() <= 512);

struct Ctx {
    ws: i64,
    base: i64,
    we: i64,
    day0: i64,
    ndays: usize,
    dates: DateCache,
    products: KeyMap,
    unknown_cat: u32,
    cat_names: Vec<&'static [u8]>,
    wide: bool,
    camps: KeyMap,
    camp_ids: Vec<&'static [u8]>,
    camp_channels: Vec<&'static [u8]>,
}

/// Dedup-table slot: a short tag of the event_id hash plus 1 + record index.
trait Slot: Copy {
    fn make(h: u32, me: u32) -> Self;
    fn tag_eq(self, h: u32) -> bool;
    fn idx(self) -> u32;
}

impl Slot for u32 {
    #[inline(always)]
    fn make(h: u32, me: u32) -> u32 {
        (h & 0xFF00_0000) | (me + 1)
    }
    #[inline(always)]
    fn tag_eq(self, h: u32) -> bool {
        (self & 0xFF00_0000) == (h & 0xFF00_0000)
    }
    #[inline(always)]
    fn idx(self) -> u32 {
        (self & 0xFF_FFFF) - 1
    }
}

impl Slot for u64 {
    #[inline(always)]
    fn make(h: u32, me: u32) -> u64 {
        ((h as u64) << 32) | (me as u64 + 1)
    }
    #[inline(always)]
    fn tag_eq(self, h: u32) -> bool {
        (self >> 32) as u32 == h
    }
    #[inline(always)]
    fn idx(self) -> u32 {
        self as u32 - 1
    }
}

const CTRL_EMPTY: u8 = 0x80;

#[inline(always)]
unsafe fn ctrl_masks(ptr: *const u8, tag: u8) -> (u16, u16) {
    #[cfg(target_arch = "x86_64")]
    {
        use core::arch::x86_64::*;
        let bytes = _mm_loadu_si128(ptr.cast());
        let matching = _mm_movemask_epi8(_mm_cmpeq_epi8(bytes, _mm_set1_epi8(tag as i8)));
        // Occupied tags have bit 7 clear (0..=127); CTRL_EMPTY has bit 7 set.
        (matching as u16, _mm_movemask_epi8(bytes) as u16)
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        let (mut matching, mut empty) = (0u16, 0u16);
        for i in 0..16 {
            let c = *ptr.add(i);
            matching |= ((c == tag) as u16) << i;
            empty |= ((c == CTRL_EMPTY) as u16) << i;
        }
        (matching, empty)
    }
}

/// Exact event_id dedup of one shard using 16-byte SIMD SwissTable control groups
/// (4x smaller memset per shard, 16-way parallel tag probe) and in-place loser
/// marking (`ts = TS_INVALID`, zero loser bitset). Keeps smallest ingest_ts (first on tie).
fn mark_losers<S: Slot + Default + Clone>(
    segs: &mut Segs<RawEv>,
    table: &mut Vec<S>,
    controls: &mut Vec<u8>,
    cap: usize,
    base: i64,
    oob: &[i64],
) {
    debug_assert!(cap >= 16 && cap.is_power_of_two());
    if table.len() < cap {
        table.resize(cap, S::default());
    }
    controls.clear();
    controls.resize(cap, CTRL_EMPTY);
    let group_mask = (cap - 1) & !15;
    let tptr = table.as_mut_ptr();
    let cptr = controls.as_mut_ptr();
    let seg_meta = segs.segs.as_ptr();
    let nseg = segs.segs.len();
    const PF: usize = 16;
    for si in 0..nseg {
        let (rptr, slen) = segs.seg_ptr_mut(si);
        for ri in 0..slen {
            unsafe {
                if ri + PF < slen {
                    let fg = (*rptr.add(ri + PF)).ehash as usize & group_mask;
                    prefetch(cptr.add(fg));
                    prefetch(tptr.add(fg) as *const u8);
                }
                let r = rptr.add(ri);
                let h = (*r).ehash;
                // Bits 17..23 are disjoint from both group index (bits 4..16) and u32 slot tag (bits 24..31),
                // cutting false-positive DRAM probes by 128x vs reusing bits 25..31!
                let tag = ((h >> 17) & 0x7F) as u8;
                let me = ((si << 9) | ri) as u32;
                let mut group = h as usize & group_mask;
                'probe: loop {
                    let (mut matching, empty) = ctrl_masks(cptr.add(group), tag);
                    while matching != 0 {
                        let lane = matching.trailing_zeros() as usize;
                        matching &= matching - 1;
                        let i = group + lane;
                        let slot = *tptr.add(i);
                        if !slot.tag_eq(h) {
                            continue;
                        }
                        let oi = slot.idx();
                        let os = *seg_meta.add((oi >> 9) as usize);
                        let o = (os.ptr as *mut RawEv).add((oi & 511) as usize);
                        if (*o).eid == (*r).eid {
                            if ingest_less((*r).ingest, (*o).ingest, base, oob) {
                                (*o).ts = TS_INVALID;
                                *tptr.add(i) = S::make(h, me);
                            } else {
                                (*r).ts = TS_INVALID;
                            }
                            break 'probe;
                        }
                    }
                    if empty != 0 {
                        let i = group + empty.trailing_zeros() as usize;
                        *tptr.add(i) = S::make(h, me);
                        *cptr.add(i) = tag;
                        break;
                    }
                    group = (group + 16) & group_mask;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// solve

#[derive(Clone, Copy, Default)]
struct CampAcc {
    sessions: u32,
    purchasing: u32,
    assisted: u32,
    revenue: u64,
}

/// L1-tiny 2-byte per-(campaign, day) counter pair, laid out as `[camp * ndays + day]`.
/// All 14 days of a campaign sit in 28 contiguous bytes (half a cache line), so
/// multi-day `close()` and multi-day attributed sessions stay in L1/L2 cache.
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Cell8 {
    sess: u8,
    assisted: u8,
}

#[derive(Clone, Copy, Default)]
struct CellHi {
    sess: u32,
    assisted: u32,
}

#[derive(Clone, Copy, Default)]
struct PurchRev {
    purchasing: u32,
    revenue: u64,
}

#[derive(Clone, Copy)]
struct Sess {
    start: i64,
    rev: u64,
    day: u32,
    purch: bool,
}

fn new_user_cols(cap: usize) -> [Column; 6] {
    [
        Column::new("user_id", Kind::Str, cap),
        Column::new("day", Kind::Date, cap),
        Column::new("sessions", Kind::I32, cap),
        Column::new("events", Kind::I32, cap),
        Column::new("revenue_cents", Kind::I64, cap),
        Column::new("top_category", Kind::OptStr, cap),
    ]
}

#[derive(Clone, Copy)]
#[repr(C)]
struct AggBin {
    min_ts: u32,
    max_ts: u32,
    cnt: u32,
    head: u32, // head index into `bin_purch`, or u32::MAX if no purchases in this bin
}

const AGG_BIN_EMPTY: AggBin = AggBin { min_ts: u32::MAX, max_ts: 0, cnt: 0, head: u32::MAX };
const BIN_AGG_MIN_EVENTS: usize = 96;

struct Worker<'a> {
    ctx: &'a Ctx,
    wide_store: &'a [(u32, u16)],
    k1: usize,
    ndays: usize,
    cnt: Vec<u32>,
    since: Vec<u32>,
    /// Transposed `[camp * ndays + day]` L1-tiny u8 counters (~56 KB for 2001x14).
    cells: Vec<Cell8>,
    /// Overflow carries for `cells` (rarely touched, only on 256-wrap).
    cells_hi: Vec<CellHi>,
    /// Purchasing sessions & revenue `[camp * ndays + day]` (touched on ~3% of sessions).
    purch_rev: Vec<PurchRev>,
    cat_rev: Vec<u64>,
    touched: Vec<u32>,
    sess: Vec<Sess>,
    day_first: Vec<u32>,
    /// L1-resident half-hour bins for fused 1-pass whale/heavy-user sessionization (672*16B = 10.75 KB).
    agg_bins: Vec<AggBin>,
    /// Intrusive per-bin linked list of purchases `(pc, next_idx)` (touched on only ~3% of events).
    bin_purch: Vec<(u32, u32)>,
    cols: [Column; 6],
    done_cols: Vec<[Column; 6]>,
}

impl<'a> Worker<'a> {
    fn new(ctx: &'a Ctx, wide_store: &'a [(u32, u16)], est_rows: usize) -> Worker<'a> {
        let k1 = ctx.camp_ids.len();
        let ndays = ctx.ndays;
        let total = k1 * ndays;
        Worker {
            ctx,
            wide_store,
            k1,
            ndays,
            cnt: vec![0; k1],
            since: vec![0; k1],
            cells: vec![Cell8::default(); total],
            cells_hi: vec![CellHi::default(); total],
            purch_rev: vec![PurchRev::default(); total],
            cat_rev: vec![0; ctx.cat_names.len()],
            touched: Vec::new(),
            sess: Vec::new(),
            day_first: vec![0; ndays + 1],
            agg_bins: vec![AGG_BIN_EMPTY; ndays * 48],
            bin_purch: Vec::new(),
            cols: new_user_cols(est_rows),
            done_cols: Vec::new(),
        }
    }

    fn into_acc(self) -> Vec<CampAcc> {
        let k1 = self.k1;
        let ndays = self.ndays;
        let mut out = vec![CampAcc::default(); k1 * ndays];
        for k in 0..k1 {
            let base = k * ndays;
            for d in 0..ndays {
                let src = base + d;
                let c = self.cells[src];
                let h = self.cells_hi[src];
                let pr = self.purch_rev[src];
                let sessions = h.sess + c.sess as u32;
                let assisted = h.assisted + c.assisted as u32;
                if sessions != 0 || assisted != 0 {
                    out[d * k1 + k] = CampAcc {
                        sessions,
                        purchasing: pr.purchasing,
                        assisted,
                        revenue: pr.revenue,
                    };
                }
            }
        }
        out
    }

    #[inline(always)]
    fn add_session(&mut self, camp: usize, day: usize, purch: bool, rev: u64) {
        let idx = camp * self.ndays + day;
        let c = unsafe { self.cells.get_unchecked_mut(idx) };
        let (nv, ov) = c.sess.overflowing_add(1);
        c.sess = nv;
        if ov {
            unsafe { self.cells_hi.get_unchecked_mut(idx).sess += 256 };
        }
        if purch {
            let pr = unsafe { self.purch_rev.get_unchecked_mut(idx) };
            pr.purchasing += 1;
            pr.revenue += rev;
        }
    }

    #[inline(always)]
    fn add_assisted(&mut self, idx: usize, add: u32) {
        let c = unsafe { self.cells.get_unchecked_mut(idx) };
        let sum = c.assisted as u32 + add;
        c.assisted = sum as u8;
        if sum >= 256 {
            unsafe { self.cells_hi.get_unchecked_mut(idx).assisted += sum & !255 };
        }
    }

    #[inline(always)]
    fn price_cat(&self, pc: u32) -> (u64, usize) {
        if !self.ctx.wide {
            ((pc >> 8) as u64, (pc & 0xFF) as usize)
        } else {
            let (p, c) = unsafe { *self.wide_store.get_unchecked((pc - 1) as usize) };
            (p as u64, c as usize)
        }
    }

    #[inline(always)]
    fn flush_row(&mut self, pslice: &[u8], day: u32, ns: u32, ne: u32, rev: u64) {
        let mut top = -1i32;
        let mut best = 0u64;
        for &c in &self.touched {
            let v = unsafe { *self.cat_rev.get_unchecked(c as usize) };
            if v > best || (v == best && (c as i32) < top) {
                best = v;
                top = c as i32;
            }
            unsafe { *self.cat_rev.get_unchecked_mut(c as usize) = 0 };
        }
        self.touched.clear();
        if self.cols[0].len() == MAX_ROWS_PER_FILE {
            let prev = std::mem::replace(&mut self.cols, new_user_cols(32_768));
            self.done_cols.push(prev);
        }
        self.cols[0].push_str(pslice);
        self.cols[1].push_date((self.ctx.day0 + day as i64) as i32);
        self.cols[2].push_i32(ns as i32);
        self.cols[3].push_i32(ne as i32);
        self.cols[4].push_i64(rev as i64);
        self.cols[5].push_opt_str(if top >= 0 { Some(self.ctx.cat_names[top as usize]) } else { None });
    }

    #[inline(always)]
    fn close(&mut self, camp: usize, a: u32, b: u32) {
        if a >= b {
            return;
        }
        let first = unsafe { self.sess.get_unchecked(a as usize).day as usize };
        let last = unsafe { self.sess.get_unchecked(b as usize - 1).day as usize };
        let base = camp * self.ndays;
        if first == last {
            self.add_assisted(base + first, b - a);
            return;
        }
        for d in first..=last {
            let lo = a.max(unsafe { *self.day_first.get_unchecked(d) });
            let hi = b.min(unsafe { *self.day_first.get_unchecked(d + 1) });
            if hi > lo {
                self.add_assisted(base + d, hi - lo);
            }
        }
    }

    /// One person: events in session-preserving order, clicks unsorted (sorted lazily when nsess >= 2).
    fn person(&mut self, person: Key8, evs: &[Ev8], clicks: &mut [Cl24], ctmp: &mut Vec<Cl24>) {
        let mut pbuf = [0u8; 16];
        let pslice = person.as_slice(&mut pbuf);
        let day0ms = self.ctx.day0 * DAY_MS - self.ctx.base;
        if evs.len() == 1 && clicks.is_empty() {
            let e = unsafe { *evs.get_unchecked(0) };
            let day = ((e.ts() as i64 - day0ms) / DAY_MS) as u32;
            let pc = e.pc();
            let (rev, top_opt) = if pc != 0 {
                let (p, c) = self.price_cat(pc);
                (p, Some(self.ctx.cat_names[c]))
            } else {
                (0u64, None)
            };
            if self.cols[0].len() == MAX_ROWS_PER_FILE {
                let prev = std::mem::replace(&mut self.cols, new_user_cols(32_768));
                self.done_cols.push(prev);
            }
            self.cols[0].push_str(pslice);
            self.cols[1].push_date((self.ctx.day0 + day as i64) as i32);
            self.cols[2].push_i32(1);
            self.cols[3].push_i32(1);
            self.cols[4].push_i64(rev as i64);
            self.cols[5].push_opt_str(top_opt);
            self.add_session(0, day as usize, pc != 0, rev);
            return;
        }
        let e0 = unsafe { *evs.get_unchecked(0) };
        let ts0 = e0.ts() as i64;
        let mut day_idx = ((ts0 - day0ms) / DAY_MS) as u32;
        let mut next_day_ms = day0ms + (day_idx as i64 + 1) * DAY_MS;
        let mut cur_day = day_idx;
        let (mut ns, mut ne, mut rev) = (1u32, 1u32, 0u64);
        let mut prev = ts0;
        let mut s_start = ts0;
        let mut s_rev = 0u64;
        let mut s_purch = false;
        let pc0 = e0.pc();
        if pc0 != 0 {
            let (p, c) = self.price_cat(pc0);
            s_rev = p;
            s_purch = true;
            rev = p;
            self.touched.push(c as u32);
            unsafe { *self.cat_rev.get_unchecked_mut(c) = p };
        }
        if clicks.is_empty() {
            // Fast organic-only path: zero `self.sess` writes or second pass!
            for &e in &evs[1..] {
                let ts = e.ts() as i64;
                if ts - prev > GAP_MS {
                    self.add_session(0, cur_day as usize, s_purch, s_rev);
                    s_rev = 0;
                    s_purch = false;
                    if ts >= next_day_ms {
                        while ts >= next_day_ms {
                            day_idx += 1;
                            next_day_ms += DAY_MS;
                        }
                        self.flush_row(pslice, cur_day, ns, ne, rev);
                        cur_day = day_idx;
                        ns = 0;
                        ne = 0;
                        rev = 0;
                    }
                    ns += 1;
                }
                prev = ts;
                ne += 1;
                let pc = e.pc();
                if pc != 0 {
                    let (p, c) = self.price_cat(pc);
                    s_rev += p;
                    s_purch = true;
                    rev += p;
                    let cr = unsafe { self.cat_rev.get_unchecked_mut(c) };
                    if *cr == 0 {
                        self.touched.push(c as u32);
                    }
                    *cr += p;
                }
            }
            self.add_session(0, cur_day as usize, s_purch, s_rev);
            self.flush_row(pslice, cur_day, ns, ne, rev);
            return;
        }
        self.sess.clear();
        for &e in &evs[1..] {
            let ts = e.ts() as i64;
            if ts - prev > GAP_MS {
                self.sess.push(Sess { start: s_start, rev: s_rev, day: cur_day, purch: s_purch });
                s_start = ts;
                s_rev = 0;
                s_purch = false;
                if ts >= next_day_ms {
                    while ts >= next_day_ms {
                        day_idx += 1;
                        next_day_ms += DAY_MS;
                    }
                    self.flush_row(pslice, cur_day, ns, ne, rev);
                    cur_day = day_idx;
                    ns = 0;
                    ne = 0;
                    rev = 0;
                }
                ns += 1;
            }
            prev = ts;
            ne += 1;
            let pc = e.pc();
            if pc != 0 {
                let (p, c) = self.price_cat(pc);
                s_rev += p;
                s_purch = true;
                rev += p;
                let cr = unsafe { self.cat_rev.get_unchecked_mut(c) };
                if *cr == 0 {
                    self.touched.push(c as u32);
                }
                *cr += p;
            }
        }
        self.sess.push(Sess { start: s_start, rev: s_rev, day: cur_day, purch: s_purch });
        self.flush_row(pslice, cur_day, ns, ne, rev);
        self.attribute_clicks(clicks, ctmp);
    }

    /// Fused 1-pass L1 half-hour bin sessionizer for persons with >= BIN_AGG_MIN_EVENTS
    /// unsorted events: zero sorting, zero 8MB temp scatter, single sequential read over `evs`!
    fn person_binned(&mut self, person: Key8, evs: &[Ev8], clicks: &mut [Cl24], ctmp: &mut Vec<Cl24>) {
        let mut pbuf = [0u8; 16];
        let pslice = person.as_slice(&mut pbuf);
        let day0_off = (self.ctx.day0 * DAY_MS - self.ctx.base) as u32;
        const GAP_U32: u32 = GAP_MS as u32;
        let n_bins = self.agg_bins.len();
        let bptr = self.agg_bins.as_mut_ptr();
        self.bin_purch.clear();
        let (mut first_b, mut last_b) = (n_bins, 0usize);

        // Single sequential pass over unsorted `evs`, updating 10.75 KB L1-resident bins.
        for &e in evs {
            let ts = e.ts();
            let b = ((ts - day0_off) / GAP_U32) as usize;
            let bin = unsafe { &mut *bptr.add(b) };
            if bin.cnt == 0 {
                bin.min_ts = ts;
                bin.max_ts = ts;
                bin.cnt = 1;
                first_b = first_b.min(b);
                last_b = last_b.max(b);
            } else {
                if ts < bin.min_ts {
                    bin.min_ts = ts;
                }
                if ts > bin.max_ts {
                    bin.max_ts = ts;
                }
                bin.cnt += 1;
            }
            let pc = e.pc();
            if pc != 0 {
                let idx = self.bin_purch.len() as u32;
                self.bin_purch.push((pc, bin.head));
                bin.head = idx;
            }
        }

        // Walk at most ~672 bins in chronological order, resetting each visited bin in place.
        let organic_only = clicks.is_empty();
        if !organic_only {
            self.sess.clear();
        }
        let mut have_sess = false;
        let mut prev_max = i64::MIN;
        let mut cur_day = 0u32;
        let (mut ns, mut ne, mut rev) = (0u32, 0u32, 0u64);
        let (mut s_start, mut s_rev, mut s_purch) = (0i64, 0u64, false);

        for b in first_b..=last_b {
            let bin = unsafe { *bptr.add(b) };
            if bin.cnt == 0 {
                continue;
            }
            unsafe { *bptr.add(b) = AGG_BIN_EMPTY };
            let min_ts = bin.min_ts as i64;
            if !have_sess || min_ts - prev_max > GAP_MS {
                let new_day = (b / 48) as u32;
                if have_sess {
                    if organic_only {
                        self.add_session(0, cur_day as usize, s_purch, s_rev);
                    } else {
                        self.sess.push(Sess { start: s_start, rev: s_rev, day: cur_day, purch: s_purch });
                    }
                    if new_day != cur_day {
                        self.flush_row(pslice, cur_day, ns, ne, rev);
                        cur_day = new_day;
                        ns = 0;
                        ne = 0;
                        rev = 0;
                    }
                } else {
                    have_sess = true;
                    cur_day = new_day;
                }
                s_start = min_ts;
                s_rev = 0;
                s_purch = false;
                ns += 1;
            }
            prev_max = bin.max_ts as i64;
            ne += bin.cnt;
            let mut pi = bin.head;
            while pi != u32::MAX {
                let (pc, next_pi) = unsafe { *self.bin_purch.get_unchecked(pi as usize) };
                let (p, c) = self.price_cat(pc);
                s_rev += p;
                s_purch = true;
                rev += p;
                let cr = unsafe { self.cat_rev.get_unchecked_mut(c) };
                if *cr == 0 {
                    self.touched.push(c as u32);
                }
                *cr += p;
                pi = next_pi;
            }
        }
        if organic_only {
            self.add_session(0, cur_day as usize, s_purch, s_rev);
            self.flush_row(pslice, cur_day, ns, ne, rev);
            return;
        }
        self.sess.push(Sess { start: s_start, rev: s_rev, day: cur_day, purch: s_purch });
        self.flush_row(pslice, cur_day, ns, ne, rev);
        self.attribute_clicks(clicks, ctmp);
    }

    #[inline(always)]
    fn attribute_clicks(&mut self, clicks: &mut [Cl24], ctmp: &mut Vec<Cl24>) {
        let nsess = self.sess.len();
        if nsess == 1 {
            // Single-session O(n) fast path: zero click sorting, zero day_first, zero close().
            let s = self.sess[0];
            let lo = s.start - SEVEN_DAYS_MS;
            let hi = s.start;
            let sday = s.day as usize;
            let ndays = self.ndays;
            let mut best_ts = i64::MIN;
            let mut best_cid = Key8::EMPTY;
            let mut credited = 0usize;
            for c in clicks.iter() {
                let cts = c.ts as i64;
                if cts >= lo && cts <= hi {
                    if cts > best_ts || (cts == best_ts && c.cid.cmp_bytes(&best_cid) == std::cmp::Ordering::Less) {
                        best_ts = cts;
                        best_cid = c.cid;
                        credited = c.camp as usize;
                    }
                    let k = c.camp as usize;
                    if unsafe { *self.cnt.get_unchecked(k) } == 0 {
                        unsafe { *self.cnt.get_unchecked_mut(k) = 1 };
                        self.add_assisted(k * ndays + sday, 1);
                        self.touched.push(k as u32);
                    }
                }
            }
            for &k in &self.touched {
                unsafe { *self.cnt.get_unchecked_mut(k as usize) = 0 };
            }
            self.touched.clear();
            self.add_session(credited, sday, s.purch, s.rev);
            return;
        }
        if clicks.len() > 1 {
            sort_person_clicks(clicks, ctmp);
        }
        // Only build day_first over the person's active day span (skip entirely if all sessions share one day).
        let first_day = unsafe { self.sess.get_unchecked(0).day as usize };
        let last_day = unsafe { self.sess.get_unchecked(nsess - 1).day as usize };
        if first_day < last_day {
            let mut j = 0usize;
            for d in first_day..=last_day + 1 {
                while j < nsess && (unsafe { self.sess.get_unchecked(j).day as usize }) < d {
                    j += 1;
                }
                unsafe { *self.day_first.get_unchecked_mut(d) = j as u32 };
            }
        }
        let n = clicks.len();
        let (mut cl, mut cr) = (0usize, 0usize);
        let mut latest_ts = i64::MIN;
        let mut latest_camp = 0usize;
        for j in 0..nsess {
            let s = unsafe { *self.sess.get_unchecked(j) };
            while cr < n && unsafe { clicks.get_unchecked(cr).ts as i64 } <= s.start {
                let c = unsafe { clicks.get_unchecked(cr) };
                if c.ts as i64 != latest_ts {
                    latest_ts = c.ts as i64;
                    latest_camp = c.camp as usize;
                }
                let k = c.camp as usize;
                let ck = unsafe { self.cnt.get_unchecked_mut(k) };
                *ck += 1;
                if *ck == 1 {
                    unsafe { *self.since.get_unchecked_mut(k) = j as u32 };
                }
                cr += 1;
            }
            let lo = s.start - SEVEN_DAYS_MS;
            while cl < cr && (unsafe { clicks.get_unchecked(cl).ts as i64 }) < lo {
                let k = unsafe { clicks.get_unchecked(cl).camp as usize };
                let ck = unsafe { self.cnt.get_unchecked_mut(k) };
                *ck -= 1;
                if *ck == 0 {
                    let a = unsafe { *self.since.get_unchecked(k) };
                    self.close(k, a, j as u32);
                }
                cl += 1;
            }
            let credited = if cl < cr { latest_camp } else { 0 };
            self.add_session(credited, s.day as usize, s.purch, s.rev);
        }
        for i in cl..cr {
            let k = unsafe { clicks.get_unchecked(i).camp as usize };
            let ck = unsafe { self.cnt.get_unchecked_mut(k) };
            if *ck > 0 {
                *ck = 0;
                let a = unsafe { *self.since.get_unchecked(k) };
                self.close(k, a, nsess as u32);
            }
        }
    }

}

// ---------------------------------------------------------------------------
// per-shard direct person interning + compact L2 solve (8B Ev8 + 24B Cl24)

/// Stable L1-resident LSD radix sort of `v` by a 32-bit key (4 passes of 8 bits,
/// only 4 KB of stack histograms; passes in which every key shares the same byte are skipped).
fn radix_by_u32<T: Copy>(v: &mut [T], tmp: &mut Vec<T>, key: impl Fn(&T) -> u32) {
    let n = v.len();
    tmp.clear();
    tmp.reserve(n);
    unsafe { tmp.set_len(n) };
    let mut cnt = [[0u32; 256]; 4];
    for x in v.iter() {
        let k = key(x);
        unsafe {
            *cnt[0].get_unchecked_mut((k & 255) as usize) += 1;
            *cnt[1].get_unchecked_mut(((k >> 8) & 255) as usize) += 1;
            *cnt[2].get_unchecked_mut(((k >> 16) & 255) as usize) += 1;
            *cnt[3].get_unchecked_mut((k >> 24) as usize) += 1;
        }
    }
    let mut in_v = true;
    for (pass, c) in cnt.iter_mut().enumerate() {
        if c.iter().any(|&x| x as usize == n) {
            continue;
        }
        let mut sum = 0u32;
        for x in c.iter_mut() {
            let t = *x;
            *x = sum;
            sum += t;
        }
        let sh = pass as u32 * 8;
        let (src, dst): (&[T], &mut [T]) = if in_v { (&*v, &mut tmp[..]) } else { (&tmp[..], &mut *v) };
        let dst = dst.as_mut_ptr();
        for x in src {
            let d = ((key(x) >> sh) & 255) as usize;
            unsafe {
                let slot = c.get_unchecked_mut(d);
                dst.add(*slot as usize).write(*x);
                *slot += 1;
            }
        }
        in_v = !in_v;
    }
    if !in_v {
        v.copy_from_slice(&tmp[..n]);
    }
}

#[inline(always)]
fn sort_person_events(sl: &mut [Ev8], tmp: &mut Vec<Ev8>) {
    let n = sl.len();
    if n < 2 {
        return;
    }
    if n == 2 {
        unsafe {
            let a = *sl.get_unchecked(0);
            let b = *sl.get_unchecked(1);
            if a.0 > b.0 {
                *sl.get_unchecked_mut(0) = b;
                *sl.get_unchecked_mut(1) = a;
            }
        }
        return;
    }
    if n == 3 {
        unsafe {
            let mut a = *sl.get_unchecked(0);
            let mut b = *sl.get_unchecked(1);
            let mut c = *sl.get_unchecked(2);
            if a.0 > b.0 {
                std::mem::swap(&mut a, &mut b);
            }
            if b.0 > c.0 {
                std::mem::swap(&mut b, &mut c);
            }
            if a.0 > b.0 {
                std::mem::swap(&mut a, &mut b);
            }
            *sl.get_unchecked_mut(0) = a;
            *sl.get_unchecked_mut(1) = b;
            *sl.get_unchecked_mut(2) = c;
        }
        return;
    }
    if n <= 24 {
        for i in 1..n {
            let x = unsafe { *sl.get_unchecked(i) };
            let mut j = i;
            while j > 0 {
                let prev = unsafe { *sl.get_unchecked(j - 1) };
                if prev.0 <= x.0 {
                    break;
                }
                unsafe { *sl.get_unchecked_mut(j) = prev };
                j -= 1;
            }
            unsafe { *sl.get_unchecked_mut(j) = x };
        }
        return;
    }
    if n > 256 {
        radix_by_u32(sl, tmp, |e| e.ts());
    } else {
        sl.sort_unstable();
    }
}

#[inline(always)]
fn click_gt(a: &Cl24, b: &Cl24) -> bool {
    a.ts > b.ts || (a.ts == b.ts && a.cid.cmp_bytes(&b.cid) == std::cmp::Ordering::Greater)
}

#[inline(always)]
fn sort_person_clicks(sl: &mut [Cl24], tmp: &mut Vec<Cl24>) {
    let n = sl.len();
    if n < 2 {
        return;
    }
    if n == 2 {
        unsafe {
            let a = *sl.get_unchecked(0);
            let b = *sl.get_unchecked(1);
            if click_gt(&a, &b) {
                *sl.get_unchecked_mut(0) = b;
                *sl.get_unchecked_mut(1) = a;
            }
        }
        return;
    }
    if n <= 24 {
        for i in 1..n {
            let x = unsafe { *sl.get_unchecked(i) };
            let mut j = i;
            while j > 0 {
                let prev = unsafe { *sl.get_unchecked(j - 1) };
                if !click_gt(&prev, &x) {
                    break;
                }
                unsafe { *sl.get_unchecked_mut(j) = prev };
                j -= 1;
            }
            unsafe { *sl.get_unchecked_mut(j) = x };
        }
        return;
    }
    if n > 256 {
        radix_by_u32(sl, tmp, |c| c.ts);
        let mut i = 0;
        while i < n {
            let mut j = i + 1;
            while j < n && sl[j].ts == sl[i].ts {
                j += 1;
            }
            if j - i > 1 {
                sl[i..j].sort_unstable_by(|a, b| a.cid.cmp_bytes(&b.cid));
            }
            i = j;
        }
    } else {
        sl.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| a.cid.cmp_bytes(&b.cid)));
    }
}

/// 16-byte inline person-intern slot: single cacheline access gives key, pid, and count.
#[derive(Clone, Copy)]
#[repr(C)]
struct HtEnt {
    key: Key8,
    pid: u32,
    cnt: u32,
}

const HT_EMPTY: HtEnt = HtEnt { key: Key8::EMPTY, pid: 0, cnt: 0 };
const _: () = assert!(std::mem::size_of::<HtEnt>() == 16);

#[derive(Clone, Copy)]
#[repr(C)]
struct CanonInfo {
    key: Key8,
    hash: u64,
}

const CANON_EMPTY: CanonInfo = CanonInfo { key: Key8::EMPTY, hash: 0 };

/// Scratch buffers reused across all shards handled by one solve worker.
struct SolveScratch {
    ht: Vec<HtEnt>,
    ht_mask: usize,
    pkeys: Vec<Key8>,
    pkeys_h: Vec<u64>,
    canon: Vec<CanonInfo>,
    link_probe: Vec<(u32, u64)>,
    miss_clicks: Vec<(Click, u64)>,
    miss_probe: Vec<u32>,
    ev_off: Vec<u32>,
    ev_cur: Vec<u32>,
    ev_pids: Vec<u32>,
    evs: Vec<Ev8>,
    etmp: Vec<Ev8>,
    cl_off: Vec<u32>,
    cl_cur: Vec<u32>,
    cl_pids: Vec<u32>,
    cls: Vec<Cl24>,
    ctmp: Vec<Cl24>,
}

impl SolveScratch {
    fn new() -> Self {
        SolveScratch {
            ht: Vec::new(),
            ht_mask: 0,
            pkeys: Vec::new(),
            pkeys_h: Vec::new(),
            canon: Vec::new(),
            link_probe: Vec::new(),
            miss_clicks: Vec::new(),
            miss_probe: Vec::new(),
            ev_off: Vec::new(),
            ev_cur: Vec::new(),
            ev_pids: Vec::new(),
            evs: Vec::new(),
            etmp: Vec::new(),
            cl_off: Vec::new(),
            cl_cur: Vec::new(),
            cl_pids: Vec::new(),
            cls: Vec::new(),
            ctmp: Vec::new(),
        }
    }

    fn reset_ht(&mut self, cap: usize) {
        self.ht.clear();
        self.ht.resize(cap, HT_EMPTY);
        self.ht_mask = cap - 1;
    }

    #[inline(always)]
    fn intern(&mut self, person: Key8, h: u64) -> u32 {
        if self.pkeys.len() * 2 >= self.ht.len() {
            self.grow_ht();
        }
        let mask = self.ht_mask;
        let mut i = h as usize & mask;
        loop {
            let e = unsafe { self.ht.get_unchecked_mut(i) };
            if e.key == person {
                e.cnt += 1;
                return e.pid;
            }
            if e.key.is_empty() {
                let pid = self.pkeys.len() as u32;
                self.pkeys.push(person);
                self.pkeys_h.push(h);
                *e = HtEnt { key: person, pid, cnt: 1 };
                return pid;
            }
            i = (i + 1) & mask;
        }
    }

    #[cold]
    fn grow_ht(&mut self) {
        let new_cap = (self.ht.len() * 2).max(256);
        let old = std::mem::replace(&mut self.ht, vec![HT_EMPTY; new_cap]);
        self.ht_mask = new_cap - 1;
        let mask = self.ht_mask;
        for e in old {
            if !e.key.is_empty() {
                let mut i = e.key.hash(SEED_A) as usize & mask;
                while !self.ht[i].key.is_empty() {
                    i = (i + 1) & mask;
                }
                self.ht[i] = e;
            }
        }
    }

    #[inline(always)]
    fn lookup(&self, person: Key8, phash: u64) -> Option<u32> {
        let mask = self.ht_mask;
        let mut i = phash as usize & mask;
        loop {
            let e = unsafe { self.ht.get_unchecked(i) };
            if e.key == person {
                return Some(e.pid);
            }
            if e.key.is_empty() {
                return None;
            }
            i = (i + 1) & mask;
        }
    }

    fn run_shard(&mut self, es: Segs<PEv>, cs: Segs<Click>, w: &mut Worker<'_>) {
        let nev = es.len();
        if nev == 0 {
            es.free();
            cs.free();
            return;
        }
        // Estimate ~1 person per 12 events (safe initial load, grows automatically if needed)
        let init_cap = ((nev / 6).next_power_of_two()).max(256);
        self.reset_ht(init_cap);
        self.pkeys.clear();
        self.pkeys_h.clear();
        self.ev_pids.clear();
        self.ev_pids.reserve(nev);

        // Pass 1 over events: intern exact Key8 persons in inline 16B HtEnt with 8-ahead single-hash prefetch
        const PF: usize = 8;
        let mut hring = [0u64; PF];
        for seg in es.iter() {
            let slen = seg.len();
            let pre = slen.min(PF);
            for ri in 0..pre {
                let h = unsafe { seg.get_unchecked(ri).person.hash(SEED_A) };
                hring[ri] = h;
                prefetch(unsafe { self.ht.as_ptr().add(h as usize & self.ht_mask) } as *const u8);
            }
            for ri in 0..slen {
                let h = hring[ri & (PF - 1)];
                if ri + PF < slen {
                    let fh = unsafe { seg.get_unchecked(ri + PF).person.hash(SEED_A) };
                    hring[(ri + PF) & (PF - 1)] = fh;
                    prefetch(unsafe { self.ht.as_ptr().add(fh as usize & self.ht_mask) } as *const u8);
                }
                let e = unsafe { seg.get_unchecked(ri) };
                let pid = self.intern(e.person, h);
                self.ev_pids.push(pid);
            }
        }
        let np = self.pkeys.len();

        // Populate per-person counts from inline HtEnt table, then prefix sum
        self.ev_off.clear();
        self.ev_off.resize(np + 1, 0);
        for e in &self.ht {
            if !e.key.is_empty() {
                unsafe { *self.ev_off.get_unchecked_mut(e.pid as usize) = e.cnt };
            }
        }
        let mut sum = 0u32;
        self.ev_cur.clear();
        self.ev_cur.reserve(np);
        for i in 0..np {
            let cnt = unsafe { *self.ev_off.get_unchecked(i) };
            unsafe { *self.ev_off.get_unchecked_mut(i) = sum };
            self.ev_cur.push(sum);
            sum += cnt;
        }
        unsafe { *self.ev_off.get_unchecked_mut(np) = sum };

        // Pass 2 over events: scatter compact 8-byte Ev8 into L2-resident buffer
        self.evs.clear();
        self.evs.reserve(nev);
        let eo = self.evs.as_mut_ptr();
        let mut k = 0usize;
        for seg in es.iter() {
            for e in seg {
                unsafe {
                    let pid = *self.ev_pids.get_unchecked(k) as usize;
                    let dst = self.ev_cur.get_unchecked_mut(pid);
                    eo.add(*dst as usize).write(Ev8::new(e.ts, e.pc));
                    *dst += 1;
                }
                k += 1;
            }
        }
        unsafe { self.evs.set_len(nev) };
        es.free();

        // Clicks: lookup in the same inline HtEnt table with 8-ahead prefetch; drop clicks for 0-event persons!
        let ncl_raw = cs.len();
        self.cl_off.clear();
        self.cl_off.resize(np + 1, 0);
        self.cl_pids.clear();
        self.cl_pids.reserve(ncl_raw);
        let mut ncl_kept = 0usize;
        if ncl_raw > 0 {
            let ht_ptr = self.ht.as_ptr();
            let ht_mask = self.ht_mask;
            for seg in cs.iter() {
                let slen = seg.len();
                let pre = slen.min(PF);
                for ri in 0..pre {
                    let h = unsafe { seg.get_unchecked(ri).person.hash(SEED_A) };
                    hring[ri] = h;
                    prefetch(unsafe { ht_ptr.add(h as usize & ht_mask) } as *const u8);
                }
                for ri in 0..slen {
                    let h = hring[ri & (PF - 1)];
                    if ri + PF < slen {
                        let fh = unsafe { seg.get_unchecked(ri + PF).person.hash(SEED_A) };
                        hring[(ri + PF) & (PF - 1)] = fh;
                        prefetch(unsafe { ht_ptr.add(fh as usize & ht_mask) } as *const u8);
                    }
                    let c = unsafe { seg.get_unchecked(ri) };
                    if let Some(pid) = self.lookup(c.person, h) {
                        self.cl_pids.push(pid);
                        unsafe { *self.cl_off.get_unchecked_mut(pid as usize) += 1 };
                        ncl_kept += 1;
                    } else {
                        self.cl_pids.push(u32::MAX);
                    }
                }
            }
        }

        self.cls.clear();
        if ncl_kept > 0 {
            let mut csum = 0u32;
            self.cl_cur.clear();
            self.cl_cur.reserve(np);
            for i in 0..np {
                let cnt = self.cl_off[i];
                self.cl_off[i] = csum;
                self.cl_cur.push(csum);
                csum += cnt;
            }
            self.cl_off[np] = csum;
            self.cls.reserve(ncl_kept);
            let co = self.cls.as_mut_ptr();
            let mut ck = 0usize;
            for seg in cs.iter() {
                for c in seg {
                    let pid = unsafe { *self.cl_pids.get_unchecked(ck) };
                    ck += 1;
                    if pid != u32::MAX {
                        unsafe {
                            let dst = self.cl_cur.get_unchecked_mut(pid as usize);
                            co.add(*dst as usize).write(Cl24 { cid: c.cid, ts: c.ts, camp: c.camp });
                            *dst += 1;
                        }
                    }
                }
            }
            unsafe { self.cls.set_len(ncl_kept) };
        }
        cs.free();

        // Process each person cleanly in L1/L2 cache (fused 1-pass L1 bins for >= BIN_AGG_MIN_EVENTS!)
        for pid in 0..np {
            let es0 = unsafe { *self.ev_off.get_unchecked(pid) } as usize;
            let es1 = unsafe { *self.ev_off.get_unchecked(pid + 1) } as usize;
            let cls_slice: &mut [Cl24] = if ncl_kept > 0 {
                let cs0 = unsafe { *self.cl_off.get_unchecked(pid) } as usize;
                let cs1 = unsafe { *self.cl_off.get_unchecked(pid + 1) } as usize;
                unsafe { self.cls.get_unchecked_mut(cs0..cs1) }
            } else {
                &mut []
            };
            let pkey = unsafe { *self.pkeys.get_unchecked(pid) };
            let ev_slice = unsafe { self.evs.get_unchecked_mut(es0..es1) };
            if ev_slice.len() >= BIN_AGG_MIN_EVENTS {
                w.person_binned(pkey, ev_slice, cls_slice, &mut self.ctmp);
            } else {
                sort_person_events(ev_slice, &mut self.etmp);
                w.person(pkey, ev_slice, cls_slice, &mut self.ctmp);
            }
        }
    }

    fn flush_miss_clicks<const STAGE: usize>(&mut self, links: &LinkTable, lcl_w: &mut Scatter<'_, Click, STAGE>, l_shift: u32) {
        let nm = self.miss_clicks.len();
        if nm == 0 {
            return;
        }
        const PF: usize = 8;
        self.miss_probe.clear();
        for i in 0..nm.min(PF) {
            links.prefetch_bloom(unsafe { self.miss_clicks.get_unchecked(i).1 });
        }
        for i in 0..nm {
            if i + PF < nm {
                links.prefetch_bloom(unsafe { self.miss_clicks.get_unchecked(i + PF).1 });
            }
            if links.maybe(unsafe { self.miss_clicks.get_unchecked(i).1 }) {
                self.miss_probe.push(i as u32);
            }
        }
        let npr = self.miss_probe.len();
        for j in 0..npr.min(PF) {
            let idx = unsafe { *self.miss_probe.get_unchecked(j) as usize };
            links.prefetch(unsafe { self.miss_clicks.get_unchecked(idx).1 });
        }
        for j in 0..npr {
            if j + PF < npr {
                let fidx = unsafe { *self.miss_probe.get_unchecked(j + PF) as usize };
                links.prefetch(unsafe { self.miss_clicks.get_unchecked(fidx).1 });
            }
            let idx = unsafe { *self.miss_probe.get_unchecked(j) as usize };
            let (c, h) = unsafe { *self.miss_clicks.get_unchecked(idx) };
            if let Some(cp) = links.lookup(h, c.person) {
                let ch = cp.hash(SEED_A);
                lcl_w.push(
                    (ch >> l_shift) as usize,
                    Click { person: cp, cid: c.cid, ts: c.ts, camp: c.camp },
                );
            }
        }
        self.miss_clicks.clear();
    }

    /// Two-tier solve Tier 1: raw-uid shard. Resolves each distinct uid ONCE in `links`,
    /// solves truly unlinked users immediately in place, and forwards linked users'
    /// events and clicks (including 0-event-in-shard linked clicks!) to Tier 2.
    fn run_tier1_shard<const STAGE: usize>(
        &mut self,
        es: Segs<PEv>,
        cs: Segs<Click>,
        links: &LinkTable,
        w: &mut Worker<'_>,
        lev_w: &mut Scatter<'_, PEv, STAGE>,
        lcl_w: &mut Scatter<'_, Click, 192>,
        l_shift: u32,
    ) {
        if links.is_empty() {
            self.run_shard(es, cs, w);
            return;
        }
        let nev = es.len();
        if nev == 0 {
            if cs.len() > 0 {
                self.miss_clicks.clear();
                for seg in cs.iter() {
                    for &c in seg {
                        self.miss_clicks.push((c, c.person.hash(SEED_A)));
                    }
                }
                self.flush_miss_clicks(links, lcl_w, l_shift);
            }
            es.free();
            cs.free();
            return;
        }

        let init_cap = ((nev / 6).next_power_of_two()).max(256);
        self.reset_ht(init_cap);
        self.pkeys.clear();
        self.pkeys_h.clear();
        self.ev_pids.clear();
        self.ev_pids.reserve(nev);

        // Pass 1 over events: intern exact raw Key8 uids in inline 16B HtEnt
        const PF: usize = 8;
        let mut hring = [0u64; PF];
        for seg in es.iter() {
            let slen = seg.len();
            let pre = slen.min(PF);
            for ri in 0..pre {
                let h = unsafe { seg.get_unchecked(ri).person.hash(SEED_A) };
                hring[ri] = h;
                prefetch(unsafe { self.ht.as_ptr().add(h as usize & self.ht_mask) } as *const u8);
            }
            for ri in 0..slen {
                let h = hring[ri & (PF - 1)];
                if ri + PF < slen {
                    let fh = unsafe { seg.get_unchecked(ri + PF).person.hash(SEED_A) };
                    hring[(ri + PF) & (PF - 1)] = fh;
                    prefetch(unsafe { self.ht.as_ptr().add(fh as usize & self.ht_mask) } as *const u8);
                }
                let e = unsafe { seg.get_unchecked(ri) };
                let pid = self.intern(e.person, h);
                self.ev_pids.push(pid);
            }
        }
        let np = self.pkeys.len();

        // Resolve each distinct raw uid ONCE in LinkTable using saved SEED_A==SEED_L hashes.
        // Precompute (canon_key, canon_hash) once per linked pid!
        self.canon.clear();
        self.canon.resize(np, CANON_EMPTY);
        self.link_probe.clear();
        let pre = np.min(PF);
        for pid in 0..pre {
            links.prefetch_bloom(unsafe { *self.pkeys_h.get_unchecked(pid) });
        }
        for pid in 0..np {
            if pid + PF < np {
                links.prefetch_bloom(unsafe { *self.pkeys_h.get_unchecked(pid + PF) });
            }
            let h = unsafe { *self.pkeys_h.get_unchecked(pid) };
            if links.maybe(h) {
                self.link_probe.push((pid as u32, h));
            }
        }
        let npr = self.link_probe.len();
        for j in 0..npr.min(PF) {
            links.prefetch(unsafe { self.link_probe.get_unchecked(j).1 });
        }
        for j in 0..npr {
            if j + PF < npr {
                links.prefetch(unsafe { self.link_probe.get_unchecked(j + PF).1 });
            }
            let (pid, h) = unsafe { *self.link_probe.get_unchecked(j) };
            let uid = unsafe { *self.pkeys.get_unchecked(pid as usize) };
            if let Some(cp) = links.lookup(h, uid) {
                unsafe {
                    *self.canon.get_unchecked_mut(pid as usize) =
                        CanonInfo { key: cp, hash: cp.hash(SEED_A) };
                }
            }
        }

        // Prefix sum counts for ALL users (branchless Pass 2 event grouping!).
        self.ev_off.clear();
        self.ev_off.resize(np + 1, 0);
        for e in &self.ht {
            if !e.key.is_empty() {
                unsafe { *self.ev_off.get_unchecked_mut(e.pid as usize) = e.cnt };
            }
        }
        let mut sum = 0u32;
        self.ev_cur.clear();
        self.ev_cur.reserve(np);
        for i in 0..np {
            let cnt = unsafe { *self.ev_off.get_unchecked(i) };
            unsafe { *self.ev_off.get_unchecked_mut(i) = sum };
            self.ev_cur.push(sum);
            sum += cnt;
        }
        unsafe { *self.ev_off.get_unchecked_mut(np) = sum };

        // Pass 2 over events: branchlessly group ALL events into L2-resident Ev8 runs by pid.
        self.evs.clear();
        self.evs.reserve(nev);
        let eo = self.evs.as_mut_ptr();
        let mut k = 0usize;
        for seg in es.iter() {
            for e in seg {
                unsafe {
                    let pid = *self.ev_pids.get_unchecked(k) as usize;
                    let dst = self.ev_cur.get_unchecked_mut(pid);
                    eo.add(*dst as usize).write(Ev8::new(e.ts, e.pc));
                    *dst += 1;
                }
                k += 1;
            }
        }
        unsafe { self.evs.set_len(nev) };
        es.free();

        // Clicks: group all in-ht clicks branchlessly by pid; pipeline 0-event-in-shard clicks through LinkTable.
        let ncl_raw = cs.len();
        self.cl_off.clear();
        self.cl_off.resize(np + 1, 0);
        self.cl_pids.clear();
        self.cl_pids.reserve(ncl_raw);
        self.miss_clicks.clear();
        let mut ncl_kept = 0usize;
        if ncl_raw > 0 {
            let ht_ptr = self.ht.as_ptr();
            let ht_mask = self.ht_mask;
            for seg in cs.iter() {
                let slen = seg.len();
                let pre = slen.min(PF);
                for ri in 0..pre {
                    let h = unsafe { seg.get_unchecked(ri).person.hash(SEED_A) };
                    hring[ri] = h;
                    prefetch(unsafe { ht_ptr.add(h as usize & ht_mask) } as *const u8);
                }
                for ri in 0..slen {
                    let h = hring[ri & (PF - 1)];
                    if ri + PF < slen {
                        let fh = unsafe { seg.get_unchecked(ri + PF).person.hash(SEED_A) };
                        hring[(ri + PF) & (PF - 1)] = fh;
                        prefetch(unsafe { ht_ptr.add(fh as usize & ht_mask) } as *const u8);
                    }
                    let c = unsafe { seg.get_unchecked(ri) };
                    if let Some(pid) = self.lookup(c.person, h) {
                        self.cl_pids.push(pid);
                        unsafe { *self.cl_off.get_unchecked_mut(pid as usize) += 1 };
                        ncl_kept += 1;
                    } else {
                        self.cl_pids.push(u32::MAX);
                        self.miss_clicks.push((*c, h));
                    }
                }
            }
            self.flush_miss_clicks(links, lcl_w, l_shift);
        }

        self.cls.clear();
        if ncl_kept > 0 {
            let mut csum = 0u32;
            self.cl_cur.clear();
            self.cl_cur.reserve(np);
            for i in 0..np {
                let cnt = unsafe { *self.cl_off.get_unchecked(i) };
                unsafe { *self.cl_off.get_unchecked_mut(i) = csum };
                self.cl_cur.push(csum);
                csum += cnt;
            }
            self.cl_off[np] = csum;
            self.cls.reserve(ncl_kept);
            let co = self.cls.as_mut_ptr();
            let mut ck = 0usize;
            for seg in cs.iter() {
                for c in seg {
                    let pid = unsafe { *self.cl_pids.get_unchecked(ck) };
                    ck += 1;
                    if pid != u32::MAX {
                        unsafe {
                            let dst = self.cl_cur.get_unchecked_mut(pid as usize);
                            co.add(*dst as usize).write(Cl24 { cid: c.cid, ts: c.ts, camp: c.camp });
                            *dst += 1;
                        }
                    }
                }
            }
            unsafe { self.cls.set_len(ncl_kept) };
        }
        cs.free();

        // Per distinct user: solve unlinked in place, or forward contiguous event & click runs to ONE hoisted Tier-2 shard!
        for pid in 0..np {
            let es0 = unsafe { *self.ev_off.get_unchecked(pid) } as usize;
            let es1 = unsafe { *self.ev_off.get_unchecked(pid + 1) } as usize;
            let ci = unsafe { *self.canon.get_unchecked(pid) };
            if ci.key.is_empty() {
                let cls_slice: &mut [Cl24] = if ncl_kept > 0 {
                    let cs0 = unsafe { *self.cl_off.get_unchecked(pid) } as usize;
                    let cs1 = unsafe { *self.cl_off.get_unchecked(pid + 1) } as usize;
                    unsafe { self.cls.get_unchecked_mut(cs0..cs1) }
                } else {
                    &mut []
                };
                let pkey = unsafe { *self.pkeys.get_unchecked(pid) };
                let ev_slice = unsafe { self.evs.get_unchecked_mut(es0..es1) };
                if ev_slice.len() >= BIN_AGG_MIN_EVENTS {
                    w.person_binned(pkey, ev_slice, cls_slice, &mut self.ctmp);
                } else {
                    sort_person_events(ev_slice, &mut self.etmp);
                    w.person(pkey, ev_slice, cls_slice, &mut self.ctmp);
                }
            } else {
                let sh = (ci.hash >> l_shift) as usize;
                let cp = ci.key;
                lev_w.push_mapped(sh, unsafe { self.evs.get_unchecked(es0..es1) }, |e| PEv {
                    person: cp,
                    ts: e.ts(),
                    pc: e.pc(),
                });
                if ncl_kept > 0 {
                    let cs0 = unsafe { *self.cl_off.get_unchecked(pid) } as usize;
                    let cs1 = unsafe { *self.cl_off.get_unchecked(pid + 1) } as usize;
                    if cs0 < cs1 {
                        lcl_w.push_mapped(sh, unsafe { self.cls.get_unchecked(cs0..cs1) }, |c| Click {
                            person: cp,
                            cid: c.cid,
                            ts: c.ts,
                            camp: c.camp,
                        });
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------

fn peak_rss_mb() -> f64 {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let v = ru.ru_maxrss as f64;
    if cfg!(target_os = "macos") {
        v / (1024.0 * 1024.0)
    } else {
        v / 1024.0
    }
}

fn manifest_value(m: &str, key: &str) -> i64 {
    let pat = format!("\"{key}\"");
    let i = m.find(&pat).unwrap_or_else(|| panic!("manifest missing {key}")) + pat.len();
    let rest = &m[i..];
    let rest = &rest[rest.find(':').expect("manifest colon") + 1..];
    let q = rest.find('"').expect("manifest quote") + 1;
    let e = rest[q..].find('"').expect("manifest quote") + q;
    parse_ts(rest[q..e].as_bytes()).unwrap_or_else(|| panic!("bad manifest {key}"))
}

fn run_pipeline<const ID_BITS: u32, const P_BITS: u32, const EV_STAGE: usize, const P_STAGE: usize>(
    ctx: &Ctx,
    link_files: &[&'static [u8]],
    event_files: &[&'static [u8]],
    click_files: &[&'static [u8]],
    threads: usize,
    est_user_rows: usize,
    mark: &mut impl FnMut(&'static str),
) -> (Vec<(Vec<[Column; 6]>, Vec<CampAcc>)>, LinkTable) {
    let id_shards: usize = 1 << ID_BITS;
    let id_shift: u32 = 64 - ID_BITS;
    let p_shards: usize = 1 << P_BITS;
    let p_shift: u32 = 64 - P_BITS;

    // ---- events + dedup + clicks overlapped with background links build
    // (links is not needed until Two-Tier solve!)
    let chunks = make_chunks(event_files, chunk_target(event_files, threads));
    let id_set: ShardSet<RawEv> = ShardSet::new_grouped(id_shards, 8);
    let oob_ingest: Mutex<Vec<i64>> = Mutex::new(Vec::new());
    let wide_store: Mutex<Vec<(u32, u16)>> = Mutex::new(Vec::new());
    let next = AtomicUsize::new(0);
    let links_sync = if threads <= 1 {
        let lt = links::build_links(link_files, 1);
        mark("links");
        Some(lt)
    } else {
        None
    };
    let (links_owned, ev_set, click_set, wide_store) = std::thread::scope(|s| {
        let links_job = if links_sync.is_none() {
            let lt_threads = if ID_BITS >= 10 { (threads / 4).max(1) } else { 1 };
            Some(s.spawn(move || links::build_links(link_files, lt_threads)))
        } else {
            None
        };
        par(threads, |_| {
            let mut w = Scatter::<RawEv, EV_STAGE>::new(&id_set);
            let oob_ref = &oob_ingest;
            let wide_ref = &wide_store;
            loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= chunks.len() {
                    break;
                }
                let d = chunks[i].data;
                populate(d);
                event_front::scan(d, &ctx.dates, |ev| {
                    let eid = ev.eid;
                    if eid.is_empty() {
                        return;
                    }
                    let ingest = ev.ingest;
                    let ek = Key8::new(eid);
                    let eh = ek.hash(SEED_ID);
                    let ing_u32 = enc_ingest_fast(ingest, ctx.base, oob_ref);
                    let mut r = RawEv {
                        eid: ek,
                        uid: Key8::EMPTY,
                        ts: TS_INVALID,
                        price: 0,
                        ingest: ing_u32,
                        ehash: eh as u32,
                    };
                    'v: {
                        let Some(ing) = ingest else { break 'v };
                        if ev.kind == 0 || ev.uid.is_empty() || ev.product.is_empty() {
                            break 'v;
                        }
                        let ts = match ev.timestamp {
                            Some(t) if t >= ctx.ws && t < ctx.we => t,
                            _ => break 'v,
                        };
                        let mut price_enc = 0u32;
                        if ev.kind == 2 {
                            let price = ev.price;
                            if price == 0 {
                                break 'v;
                            }
                            let cat = ctx.products.get(ev.product).unwrap_or(ctx.unknown_cat);
                            price_enc = if !ctx.wide {
                                (price << 8) | cat
                            } else {
                                let mut g = wide_ref.lock().unwrap();
                                g.push((price, cat as u16));
                                g.len() as u32
                            };
                        }
                        if ing - ts > LATE_MS {
                            break 'v;
                        }
                        r.uid = Key8::new(ev.uid);
                        r.ts = (ts - ctx.base) as u32;
                        r.price = price_enc;
                    }
                    w.push((eh >> id_shift) as usize, r);
                });
                release_chunk(d);
            }
            w.finish();
        });
        mark("events");

        let oob_ingest = oob_ingest.into_inner().unwrap();
        let wide_store = wide_store.into_inner().unwrap();

        // ---- dedup (exact event_id) -> scatter directly by raw user_id (ZERO LinkTable lookups!)
        let ev_set: ShardSet<PEv> = ShardSet::new_shared(p_shards);
        let next = AtomicUsize::new(0);
        let oob_slice = &oob_ingest[..];
        par(threads, |_| {
            let mut w = Scatter::<PEv, P_STAGE>::new(&ev_set);
            let (mut t32, mut t64): (Vec<u32>, Vec<u64>) = (Vec::new(), Vec::new());
            let mut ctrls: Vec<u8> = Vec::new();
            loop {
                let s = next.fetch_add(1, Ordering::Relaxed);
                if s >= id_shards {
                    break;
                }
                let mut segs = id_set.take(s);
                let nseg = segs.segs.len();
                assert!(nseg < (1 << 22), "event_id shard too large");
                let n = segs.len();
                let cap = (n + n / 2).next_power_of_two().max(16);
                if nseg < (1 << 15) - 1 {
                    mark_losers(&mut segs, &mut t32, &mut ctrls, cap, ctx.base, oob_slice);
                } else {
                    mark_losers(&mut segs, &mut t64, &mut ctrls, cap, ctx.base, oob_slice);
                }
                for si in (0..nseg).rev() {
                    for r in segs.seg(si) {
                        if r.ts == TS_INVALID {
                            continue;
                        }
                        let uh = r.uid.hash(SEED_A);
                        w.push((uh >> p_shift) as usize, PEv { person: r.uid, ts: r.ts, pc: r.price });
                    }
                }
                segs.free();
                id_set.mark_shard_done(s);
            }
            w.finish();
        });
        id_set.free_all();
        mark("dedup");

        // ---- clicks: parse, validate, scatter directly by raw user_id (ZERO LinkTable lookups!)
        let cws = ctx.ws - SEVEN_DAYS_MS;
        let chunks = make_chunks(click_files, chunk_target(click_files, threads));
        let click_set: ShardSet<Click> = ShardSet::new_shared(p_shards);
        let next = AtomicUsize::new(0);
        par(threads, |_| {
            let mut w = Scatter::<Click, 192>::new(&click_set);
            let mut tok = fixed::Tok::default();
            loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= chunks.len() {
                    break;
                }
                let d = chunks[i].data;
                populate(d);
                fixed::scan::<4, _>(&mut tok, d, |f| {
                    let (cid, uid) = (f[0], f[1]);
                    if cid.is_empty() || uid.is_empty() {
                        return;
                    }
                    let Some(camp) = ctx.camps.get(f[2]) else { return };
                    let ts = match ctx.dates.parse_ts(f[3]) {
                        Some(t) if t >= cws && t < ctx.we => t,
                        _ => return,
                    };
                    let uid_k = Key8::new(uid);
                    let uh = uid_k.hash(SEED_A);
                    w.push(
                        (uh >> p_shift) as usize,
                        Click { person: uid_k, cid: Key8::new(cid), ts: (ts - ctx.base) as u32, camp },
                    );
                });
                release_chunk(d);
            }
            w.finish();
        });
        mark("clicks");

        let lt = match links_job {
            Some(j) => {
                let lt = j.join().expect("links build");
                mark("links_wait");
                lt
            }
            None => links_sync.unwrap(),
        };
        (lt, ev_set, click_set, wide_store)
    });
    let links = &links_owned;

    // ---- Two-tier solve: Tier 1 resolves distinct uids once, solves unlinked in place,
    // forwards linked to Tier 2; Tier 2 solves linked canonical persons in the same Workers!
    let mut order: Vec<(usize, usize)> =
        (0..p_shards).map(|p| (p, ev_set.count(p) * 2 + click_set.count(p))).collect();
    order.sort_by_key(|x| std::cmp::Reverse(x.1));
    let l_bits = P_BITS - 1;
    let l_shards = 1usize << l_bits;
    let l_shift = 64 - l_bits;
    let linked_ev_set: ShardSet<PEv> = ShardSet::new_shared(l_shards);
    let linked_cl_set: ShardSet<Click> = ShardSet::new_shared(l_shards);
    let order2: Mutex<Vec<(usize, usize)>> = Mutex::new(Vec::new());
    let wide_slice = &wide_store[..];
    let next1 = AtomicUsize::new(0);
    let next2 = AtomicUsize::new(0);
    let bar = Barrier::new(threads);
    let results: Vec<(Vec<[Column; 6]>, Vec<CampAcc>)> = par(threads, |_| {
        let mut w = Worker::new(ctx, wide_slice, est_user_rows);
        let mut sc = SolveScratch::new();
        {
            let mut lev_w = Scatter::<PEv, 128>::new(&linked_ev_set);
            let mut lcl_w = Scatter::<Click, 192>::new(&linked_cl_set);
            loop {
                let k = next1.fetch_add(1, Ordering::Relaxed);
                if k >= p_shards {
                    break;
                }
                let p = order[k].0;
                let es = ev_set.take(p);
                let cs = click_set.take(p);
                sc.run_tier1_shard(es, cs, links, &mut w, &mut lev_w, &mut lcl_w, l_shift);
            }
            lev_w.finish();
            lcl_w.finish();
        }
        if bar.wait().is_leader() {
            ev_set.release_blocks();
            click_set.release_blocks();
            if !links.is_empty() {
                let mut o2: Vec<(usize, usize)> = (0..l_shards)
                    .map(|p| (p, linked_ev_set.count(p) * 2 + linked_cl_set.count(p)))
                    .filter(|x| x.1 > 0)
                    .collect();
                o2.sort_by_key(|x| std::cmp::Reverse(x.1));
                *order2.lock().unwrap() = o2;
            }
        }
        bar.wait();
        if !links.is_empty() {
            let o2 = order2.lock().unwrap().clone();
            loop {
                let k = next2.fetch_add(1, Ordering::Relaxed);
                if k >= o2.len() {
                    break;
                }
                let p = o2[k].0;
                let es = linked_ev_set.take(p);
                let cs = linked_cl_set.take(p);
                sc.run_shard(es, cs, &mut w);
            }
        }
        if w.cols[0].len() > 0 {
            let last = std::mem::replace(&mut w.cols, new_user_cols(0));
            w.done_cols.push(last);
        }
        let done_cols = std::mem::take(&mut w.done_cols);
        (done_cols, w.into_acc())
    });
    ev_set.free_all();
    click_set.free_all();
    linked_ev_set.free_all();
    linked_cl_set.free_all();
    mark("solve");
    (results, links_owned)
}

fn main() {
    let t0 = Instant::now();
    let mut marks: Vec<(&str, f64)> = Vec::new();
    let mut last = t0;
    let mut mark = |name: &'static str, marks: &mut Vec<(&str, f64)>| {
        let now = Instant::now();
        marks.push((name, (now - last).as_secs_f64() * 1000.0));
        last = now;
    };

    let args: Vec<String> = std::env::args().collect();
    let env = |k: &str| std::env::var(k).ok().filter(|s| !s.is_empty());
    let data_dir = PathBuf::from(args.get(1).cloned().or_else(|| env("DATA_DIR")).unwrap_or_else(|| "/data".into()));
    let out_dir =
        PathBuf::from(args.get(2).cloned().or_else(|| env("OUTPUT_DIR")).unwrap_or_else(|| "/output".into()));
    let threads = env("NCPU")
        .and_then(|s| s.trim().parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8))
        .clamp(1, 128);

    // ---- init
    let manifest = std::fs::read_to_string(data_dir.join("manifest.json")).expect("read manifest.json");
    let ws = manifest_value(&manifest, "window_start");
    let we = manifest_value(&manifest, "window_end");
    assert!(we - ws + SEVEN_DAYS_MS < u32::MAX as i64, "event window too long");
    let day0 = ws.div_euclid(DAY_MS);
    let ndays = if we > ws { ((we - 1).div_euclid(DAY_MS) - day0 + 1) as usize } else { 1 };

    let pdata = read_static(&data_dir.join("products.csv"));
    let mut prod_rows: Vec<(&'static [u8], &'static [u8])> = Vec::new();
    csv::scan::<2, _>(body(pdata), |f| prod_rows.push((f[0], f[1])));
    let mut cat_names: Vec<&'static [u8]> = prod_rows.iter().map(|r| r.1).collect();
    cat_names.push(b"unknown");
    cat_names.sort_unstable();
    cat_names.dedup();
    assert!(cat_names.len() < u16::MAX as usize);
    let mut catmap = KeyMap::new(cat_names.len());
    for (i, c) in cat_names.iter().enumerate() {
        catmap.insert(c, i as u32);
    }
    let unknown_cat = catmap.get(b"unknown").unwrap();
    let mut products = KeyMap::new_with_fast(prod_rows.len(), b"p", 65_536);
    for (p, c) in &prod_rows {
        products.insert(p, catmap.get(c).unwrap());
    }

    let cdata = read_static(&data_dir.join("campaigns.csv"));
    let mut camp_ids: Vec<&'static [u8]> = vec![b"organic"];
    let mut camp_channels: Vec<&'static [u8]> = vec![b"organic"];
    let mut camp_rows: Vec<(&'static [u8], &'static [u8])> = Vec::new();
    csv::scan::<2, _>(body(cdata), |f| camp_rows.push((f[0], f[1])));
    let mut camps = KeyMap::new_with_fast(camp_rows.len(), b"c_", 8_192);
    for (id, ch) in camp_rows {
        if camps.get(id).is_none() {
            camps.insert(id, camp_ids.len() as u32);
            camp_ids.push(id);
            camp_channels.push(ch);
        }
    }

    let link_files = map_dir(&data_dir.join("links"));
    let click_files = map_dir(&data_dir.join("clicks"));
    let event_files = map_dir(&data_dir.join("events"));
    let wide = cat_names.len() > 256;
    let ctx = Ctx {
        ws,
        base: ws - SEVEN_DAYS_MS,
        we,
        day0,
        ndays,
        dates: DateCache::new(ws, we),
        products,
        unknown_cat,
        cat_names,
        wide,
        camps,
        camp_ids,
        camp_channels,
    };
    let ctx = &ctx;
    mark("init", &mut marks);

    let event_bytes: usize = event_files.iter().map(|d| d.len()).sum();
    let est_user_rows: usize = (event_bytes / (threads * 700)).clamp(1024, 512_000);
    let force_10 = env("FORCE_SHARD_BITS").as_deref() == Some("10");
    let (results, links_owned) = if force_10 || event_bytes > (2500 << 20) {
        let mut m = |name: &'static str| mark(name, &mut marks);
        run_pipeline::<10, 10, 64, 128>(ctx, &link_files, &event_files, &click_files, threads, est_user_rows, &mut m)
    } else {
        let mut m = |name: &'static str| mark(name, &mut marks);
        run_pipeline::<9, 9, 128, 128>(ctx, &link_files, &event_files, &click_files, threads, est_user_rows, &mut m)
    };

    // ---- write (pooled blocks are released in parallel meanwhile)
    let ud = out_dir.join("user_daily");
    let cd = out_dir.join("campaign_daily");
    std::fs::create_dir_all(&ud).expect("create user_daily");
    std::fs::create_dir_all(&cd).expect("create campaign_daily");
    let k1 = ctx.camp_ids.len();
    let mut jobs: Vec<[Column; 6]> = Vec::new();
    let mut accs: Vec<Vec<CampAcc>> = Vec::with_capacity(results.len());
    for (chunks, a) in results {
        jobs.extend(chunks);
        accs.push(a);
    }
    let jobs = &jobs;
    let next = AtomicUsize::new(0);
    std::thread::scope(|s| {
        let freer = s.spawn(|| {
            links_owned.release_pages();
            arena::release_all(threads);
        });
        let h = s.spawn(|| {
            // campaign_daily reduce + write
            let mut tot = vec![CampAcc::default(); k1 * ctx.ndays];
            for a in &accs {
                for (t, x) in tot.iter_mut().zip(a) {
                    t.sessions += x.sessions;
                    t.purchasing += x.purchasing;
                    t.assisted += x.assisted;
                    t.revenue += x.revenue;
                }
            }
            let n = tot.iter().filter(|x| x.sessions > 0 || x.assisted > 0).count();
            let mut c_id = Column::new("campaign_id", Kind::Str, n);
            let mut c_ch = Column::new("channel", Kind::Str, n);
            let mut c_day = Column::new("day", Kind::Date, n);
            let mut c_s = Column::new("sessions", Kind::I64, n);
            let mut c_p = Column::new("purchasing_sessions", Kind::I64, n);
            let mut c_r = Column::new("revenue_cents", Kind::I64, n);
            let mut c_a = Column::new("assisted_sessions", Kind::I64, n);
            for d in 0..ctx.ndays {
                for k in 0..k1 {
                    let x = &tot[d * k1 + k];
                    if x.sessions == 0 && x.assisted == 0 {
                        continue;
                    }
                    c_id.push_str(ctx.camp_ids[k]);
                    c_ch.push_str(ctx.camp_channels[k]);
                    c_day.push_date((ctx.day0 + d as i64) as i32);
                    c_s.push_i64(x.sessions as i64);
                    c_p.push_i64(x.purchasing as i64);
                    c_r.push_i64(x.revenue as i64);
                    c_a.push_i64(x.assisted as i64);
                }
            }
            parquet::write_file(&cd.join("part-00000.parquet"), &[c_id, c_ch, c_day, c_s, c_p, c_r, c_a])
                .expect("write campaign_daily");
        });
        let nw = threads.min(jobs.len()).max(1);
        let hs: Vec<_> = (0..nw)
            .map(|_| {
                s.spawn(|| loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= jobs.len() {
                        break;
                    }
                    parquet::write_file(&ud.join(format!("part-{i:05}.parquet")), &jobs[i])
                        .expect("write user_daily");
                    for c in &jobs[i] {
                        c.release_pages();
                    }
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
        h.join().unwrap();
        freer.join().unwrap();
    });
    if jobs.is_empty() {
        let empty = new_user_cols(0);
        parquet::write_file(&ud.join("part-00000.parquet"), &empty).expect("write user_daily");
    }
    mark("write", &mut marks);

    let total = (Instant::now() - t0).as_secs_f64() * 1000.0;
    let mut line = format!("[timing] total={total:.1}ms");
    for (k, v) in &marks {
        line.push_str(&format!(" {k}={v:.1}ms"));
    }
    line.push_str(&format!(" threads={threads} simd={} peak_rss_mb={:.1}", csv::simd_backend(), peak_rss_mb()));
    eprintln!("{line}");
    std::process::exit(0);
}
