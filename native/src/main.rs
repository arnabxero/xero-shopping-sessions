//! Shopping Sessions, native pipeline. rules.md is the specification; the
//! stages below follow its order.
//!
//!   1. links    parse, union-find, name each person by their smallest ID
//!   2. parse    events and clicks, in parallel over chunks of the mapped files;
//!               each event copy is validated on the spot and sent to a
//!               partition chosen by its event_id
//!   3. dedup    per partition: keep the earliest-ingested copy of each event,
//!               then send the valid survivors to a partition chosen by person
//!   4. sessions per partition: group by person, sort by time, sessionize,
//!               credit and assist, and write the user_daily rows
//!   5. campaign_daily from the per-thread totals

mod bump;
mod key;
mod parquet;
mod scan;
mod time;

#[global_allocator]
static ALLOCATOR: bump::Bump = bump::Bump;

use std::fs::{self, File};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use key::{Key, KeyMap};
use parquet::{Column, Kind, Values, Writer};
use time::DAY;

const PARTITION_BITS: u32 = 8;
const PARTITIONS: usize = 1 << PARTITION_BITS;
const BLOCK_BITS: u32 = 12;
const BLOCK: usize = 1 << BLOCK_BITS;
const CHUNK: usize = 1 << 18;
const INVALID: u32 = u32::MAX;
const SESSION_GAP: u32 = 30 * 60 * 1000;
const WEEK: u32 = 7 * DAY as u32;
const ROW_GROUP: usize = 1 << 17;

// ------------------------------------------------------------------ inputs

/// The two libc calls this needs, declared here so the build has no dependencies at all.
mod libc {
    use std::ffi::{c_int, c_void};
    pub const PROT_READ: c_int = 1;
    pub const MAP_PRIVATE: c_int = 2;
    pub const MADV_WILLNEED: c_int = 3;

    pub const MAP_FAILED: *mut c_void = !0 as *mut c_void;
    extern "C" {
        pub fn mmap(addr: *mut c_void, len: usize, prot: c_int, flags: c_int, fd: c_int, offset: i64) -> *mut c_void;
        pub fn madvise(addr: *mut c_void, len: usize, advice: c_int) -> c_int;
    }
}

/// A read-only mapping of one input file.
struct Mapped {
    pointer: *const u8,
    len: usize,
}
// SAFETY: the mapping is read-only and lives until the process exits.
unsafe impl Sync for Mapped {}
unsafe impl Send for Mapped {}

impl Mapped {
    fn open(path: &Path) -> Option<Mapped> {
        let file = File::open(path).unwrap_or_else(|error| fail(&format!("{}: {error}", path.display())));
        let len = file.metadata().map(|meta| meta.len() as usize).unwrap_or(0);
        if len == 0 {
            return None;
        }
        // SAFETY: a private read-only mapping of an open file.
        let pointer = unsafe {
            libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ, libc::MAP_PRIVATE, file.as_raw_fd(), 0)
        };
        if pointer == libc::MAP_FAILED {
            fail(&format!("{}: mmap failed", path.display()));
        }
        // SAFETY: advice on the mapping just made.
        unsafe { libc::madvise(pointer, len, libc::MADV_WILLNEED) };
        Some(Mapped { pointer: pointer.cast(), len })
    }
    fn bytes(&self) -> &[u8] {
        // SAFETY: the mapping is `len` bytes and never unmapped.
        unsafe { std::slice::from_raw_parts(self.pointer, self.len) }
    }
}

fn fail(message: &str) -> ! {
    eprintln!("shopping-sessions: {message}");
    std::process::exit(1);
}

/// Every `*.csv` in a folder, mapped, with its chunks: `(file, start, end)`.
fn map_folder(folder: &Path, files: &mut Vec<Mapped>) -> Vec<(usize, usize, usize)> {
    let mut paths: Vec<PathBuf> = match fs::read_dir(folder) {
        Ok(entries) => entries
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .filter(|path| path.extension().is_some_and(|extension| extension == "csv") && path.is_file())
            .collect(),
        Err(_) => Vec::new(),
    };
    paths.sort();
    let mut tasks = Vec::new();
    for path in paths {
        if let Some(mapped) = Mapped::open(&path) {
            let at = files.len();
            tasks.extend(scan::chunks(mapped.bytes(), CHUNK).into_iter().map(|(start, end)| (at, start, end)));
            files.push(mapped);
        }
    }
    // Largest chunks first, so the threads finish together.
    tasks.sort_by_key(|&(_, start, end)| std::cmp::Reverse(end - start));
    tasks
}

/// A two-column lookup file: `(key, second column)` per line.
fn read_lookup(path: &Path) -> Vec<(Key, Vec<u8>)> {
    let mut rows = Vec::new();
    if let Some(mapped) = Mapped::open(path) {
        let data = mapped.bytes();
        for (start, end) in scan::chunks(data, usize::MAX) {
            scan::scan::<2>(&mut scan::Index::default(), data, start, end, |fields| {
                rows.push((key::key(data, fields[0].0, fields[0].1), data[fields[1].0..fields[1].1].to_vec()));
            });
        }
    }
    rows
}

fn manifest_time(text: &[u8], name: &str) -> i64 {
    let find = |from: usize, needle: &[u8]| {
        text[from..].windows(needle.len()).position(|window| window == needle).map(|at| from + at)
    };
    let parsed = (|| {
        let at = find(0, format!("\"{name}\"").as_bytes())? + name.len() + 2;
        let open = find(find(at, b":")?, b"\"")? + 1;
        let close = find(open, b"\"")?;
        time::parse(&text[open..close], &mut time::Dates::new())
    })();
    parsed.unwrap_or_else(|| fail(&format!("manifest.json: no valid {name}")))
}

// ------------------------------------------------------------------ records

/// Bytes per block of rows. Every row type uses the same size, so a block
/// that one stage has finished with can hold another stage's rows.
const BLOCK_BYTES: usize = 48 << BLOCK_BITS;
const SLAB_BYTES: usize = 64 * BLOCK_BYTES;

/// A thread's supply of blocks: carved from large slabs, and reused when given back.
struct Arena {
    free: Vec<*mut u8>,
    cursor: *mut u8,
    left: usize,
}

thread_local! {
    static ARENA: std::cell::RefCell<Arena> = const { std::cell::RefCell::new(Arena { free: Vec::new(), cursor: std::ptr::null_mut(), left: 0 }) };
}

impl Arena {
    fn take() -> *mut u8 {
        ARENA.with_borrow_mut(|arena| {
            if let Some(block) = arena.free.pop() {
                return block;
            }
            if arena.left == 0 {
                let layout = std::alloc::Layout::from_size_align(SLAB_BYTES, 4096).unwrap();
                // SAFETY: the layout has a size.
                let slab = unsafe { std::alloc::alloc(layout) };
                if slab.is_null() {
                    fail("out of memory");
                }
                (arena.cursor, arena.left) = (slab, SLAB_BYTES / BLOCK_BYTES);
            }
            let block = arena.cursor;
            // SAFETY: the slab has `left` blocks after `cursor`.
            arena.cursor = unsafe { block.add(BLOCK_BYTES) };
            arena.left -= 1;
            block
        })
    }
    fn give(block: *mut u8) {
        ARENA.with_borrow_mut(|arena| arena.free.push(block));
    }
}

/// A block of rows: `len` of them at `rows`.
struct Block<T> {
    rows: *mut T,
    len: usize,
}
// SAFETY: a block is owned by whoever holds it, like a Vec.
unsafe impl<T: Send> Send for Block<T> {}

impl<T> Block<T> {
    const CAPACITY: usize = BLOCK_BYTES / std::mem::size_of::<T>();
    fn rows(&self) -> &[T] {
        // SAFETY: `len` rows were written at `rows`.
        unsafe { std::slice::from_raw_parts(self.rows, self.len) }
    }
    /// Hands the memory back to this thread's arena. The rows need no dropping.
    fn recycle(self) {
        Arena::give(self.rows.cast());
    }
}

/// Rows headed for one partition, in fixed-size blocks so nothing is ever moved.
struct Part<T> {
    blocks: Vec<Block<T>>,
    current: *mut T,
    len: usize,
}
// SAFETY: as for Block.
unsafe impl<T: Send> Send for Part<T> {}

impl<T> Part<T> {
    fn new() -> Self {
        // "Full" until the first push, so that empty partitions cost nothing.
        Self { blocks: Vec::new(), current: std::ptr::null_mut(), len: Block::<T>::CAPACITY }
    }
    #[cold]
    #[inline(never)]
    fn grow(&mut self) {
        if !self.current.is_null() {
            self.blocks.push(Block { rows: self.current, len: self.len });
        }
        self.current = Arena::take().cast();
        self.len = 0;
    }
    #[inline(always)]
    fn push(&mut self, value: T) {
        if self.len == Block::<T>::CAPACITY {
            self.grow();
        }
        // SAFETY: the current block has room for row `len`.
        unsafe { self.current.add(self.len).write(value) };
        self.len += 1;
    }
    fn into_blocks(mut self) -> Vec<Block<T>> {
        if !self.current.is_null() {
            self.blocks.push(Block { rows: self.current, len: self.len });
        }
        self.blocks
    }
}

fn parts<T>() -> Vec<Part<T>> {
    (0..PARTITIONS).map(|_| Part::new()).collect()
}

/// Regroups each thread's partitions into one list of blocks per partition.
fn gather<T>(threads: Vec<Vec<Part<T>>>) -> Vec<Mutex<Vec<Block<T>>>> {
    let mut out: Vec<Vec<Block<T>>> = (0..PARTITIONS).map(|_| Vec::new()).collect();
    for thread in threads {
        for (at, part) in thread.into_iter().enumerate() {
            out[at].extend(part.into_blocks());
        }
    }
    out.into_iter().map(Mutex::new).collect()
}

#[inline(always)]
fn partition(key: Key) -> usize {
    (key::hash(key) >> (64 - PARTITION_BITS)) as usize
}

/// One copy of an event. `ts` is INVALID when the copy fails validation or is
/// late: it still takes part in the dedup, and if it is the kept copy the event is dropped.
#[repr(C)]
struct Event {
    id: [u64; 2],
    user: [u64; 2],
    ingest: u32, // seconds
    ts: u32,     // milliseconds since `base`
    price: u32,  // 0 unless a purchase
    category: u32,
}

/// A surviving event, by person.
#[repr(C)]
struct Visit {
    person: [u64; 2],
    ts: u32,
    price: u32,
    category: u32,
}

/// A valid click, by person.
#[repr(C)]
struct Click {
    person: [u64; 2],
    id: [u64; 2],
    ts: u32,
    campaign: u32,
}

#[inline(always)]
fn split(key: Key) -> [u64; 2] {
    [(key >> 64) as u64, key as u64]
}
#[inline(always)]
fn join(parts: [u64; 2]) -> Key {
    (u128::from(parts[0]) << 64) | u128::from(parts[1])
}

struct Context {
    window_start: i64,
    window_end: i64,
    /// `window_start - 7 days`: every stored timestamp is milliseconds after this.
    base: i64,
    products: KeyMap<u32>,
    unknown_category: u32,
    categories: Vec<Vec<u8>>,
    campaigns: KeyMap<u32>,
    campaign_rows: Vec<(Vec<u8>, Vec<u8>)>,
    days: usize,
    first_day: i64,
    day_shift: u64,
}

// ------------------------------------------------------------------ 1. links

const SHARD_BITS: u32 = 8;
const SHARDS: usize = 1 << SHARD_BITS;

/// The person (their smallest ID) of every ID that a link joins to another,
/// in shards by the top bits of the ID's hash so the threads can build it together.
struct Links {
    shards: Vec<KeyMap<Key>>,
}

impl Links {
    #[inline(always)]
    fn person(&self, user: Key) -> Key {
        let hash = key::hash(user);
        self.shards[(hash >> (64 - SHARD_BITS)) as usize].get_hashed(user, hash).unwrap_or(user)
    }
    #[inline(always)]
    fn prefetch(&self, user: Key) {
        let hash = key::hash(user);
        self.shards[(hash >> (64 - SHARD_BITS)) as usize].prefetch_hashed(hash);
    }
}

/// One shard's IDs, numbered in the order they were first seen.
#[derive(Default)]
struct Shard {
    ids: Option<KeyMap<Key>>,
    keys: Vec<Key>,
    numbers: Vec<Vec<u32>>,
}

/// One side of a link on its way to a shard: the ID, and `link << 1 | side`
/// with links numbered within the thread that parsed them.
type Side = (Key, u32);

/// Step 1, by many threads: number the IDs of each shard. `sides[thread][shard]` are the
/// parsed links; the shard keeps each side's number in the same order, in `numbers[thread]`.
fn number_ids(sides: &[Vec<Vec<Side>>], shards: &[Mutex<Shard>], next: &AtomicUsize) {
    loop {
        let shard_at = next.fetch_add(1, Ordering::Relaxed);
        if shard_at >= SHARDS {
            return;
        }
        let total: usize = sides.iter().map(|thread| thread[shard_at].len()).sum();
        let mut ids: KeyMap<Key> = KeyMap::with_capacity(total);
        let mut keys = Vec::with_capacity(total);
        let mut numbers = Vec::with_capacity(sides.len());
        for thread in sides {
            numbers.push(Vec::with_capacity(thread[shard_at].len()));
            for &(key, _) in &thread[shard_at] {
                let next = keys.len() as u128;
                let local = ids.insert(key, next);
                if local == next {
                    keys.push(key);
                }
                numbers.last_mut().unwrap().push(local as u32);
            }
        }
        *shards[shard_at].lock().unwrap() = Shard { ids: Some(ids), keys, numbers };
    }
}

/// Step 2, by one thread: union-find over the numbers, then name each person.
fn join_links(sides: &[Vec<Vec<Side>>], counts: &[usize], shards: Vec<Mutex<Shard>>) -> Links {
    let mut shards: Vec<Shard> = shards.into_iter().map(|shard| shard.into_inner().unwrap()).collect();
    let mut bases = Vec::with_capacity(SHARDS);
    let mut keys: Vec<Key> = Vec::new();
    for shard in &shards {
        bases.push(keys.len() as u32);
        keys.extend_from_slice(&shard.keys);
    }
    // Each link's two IDs, numbered across all shards.
    let starts: Vec<usize> = counts.iter().scan(0, |sum, &count| Some(std::mem::replace(sum, *sum + count))).collect();
    let mut links = vec![[0u32; 2]; counts.iter().sum()];
    for (shard_at, shard) in shards.iter().enumerate() {
        for ((thread, numbers), &start) in sides.iter().zip(&shard.numbers).zip(&starts) {
            for (&(_, side), &local) in thread[shard_at].iter().zip(numbers) {
                links[start + (side >> 1) as usize][(side & 1) as usize] = bases[shard_at] + local;
            }
        }
    }
    let mut parent: Vec<u32> = (0..keys.len() as u32).collect();
    fn find(parent: &mut [u32], mut at: u32) -> u32 {
        let mut root = at;
        while parent[root as usize] != root {
            root = parent[root as usize];
        }
        while parent[at as usize] != root {
            at = std::mem::replace(&mut parent[at as usize], root);
        }
        root
    }
    for &[a, b] in &links {
        let (a, b) = (find(&mut parent, a), find(&mut parent, b));
        if a != b {
            parent[a as usize] = b;
        }
    }
    let mut name = keys.clone();
    for at in 0..keys.len() {
        let root = find(&mut parent, at as u32) as usize;
        if key::compare(keys[at], name[root]).is_lt() {
            name[root] = keys[at];
        }
    }
    for (shard, &base) in shards.iter_mut().zip(&bases) {
        for slot in shard.ids.as_mut().unwrap().slots_mut() {
            if slot.0 != 0 {
                slot.1 = name[parent[base as usize + slot.1 as usize] as usize];
            }
        }
    }
    Links { shards: shards.into_iter().map(|shard| shard.ids.unwrap()).collect() }
}

// ------------------------------------------------------------------ 2. parse

fn parse_events(context: &Context, index: &mut scan::Index, data: &[u8], start: usize, end: usize, out: &mut [Part<Event>]) {
    let mut dates = time::Dates::new();
    scan::scan::<7>(index, data, start, end, |fields| {
        // SAFETY: the scan only reports fields inside `data`.
        let field = |at: usize| unsafe { data.get_unchecked(fields[at].0..fields[at].1) };
        let id = key::key(data, fields[0].0, fields[0].1);
        if id == 0 {
            return;
        }
        let user = key::key(data, fields[1].0, fields[1].1);
        let ingest = time::parse(field(3), &mut dates);
        let mut event = Event {
            id: split(id),
            user: split(user),
            ingest: ingest.map_or(INVALID, |ms| ms.div_euclid(1000).clamp(0, i64::from(INVALID - 1)) as u32),
            ts: INVALID,
            price: 0,
            category: 0,
        };
        'valid: {
            if user == 0 || fields[5].0 == fields[5].1 {
                break 'valid;
            }
            let Some(ts) = time::parse(field(2), &mut dates) else { break 'valid };
            if ts < context.window_start || ts >= context.window_end {
                break 'valid;
            }
            let Some(ingest) = ingest else { break 'valid };
            if ingest - ts > DAY {
                break 'valid;
            }
            let kind = field(4);
            if kind.len() == 4 {
                let kind = u32::from_le_bytes(kind.try_into().unwrap());
                if kind != u32::from_le_bytes(*b"view") && kind != u32::from_le_bytes(*b"cart") {
                    break 'valid;
                }
            } else if kind == b"purchase" {
                let Some(price) = time::digits(field(6), 8) else { break 'valid };
                if !(1..=10_000_000).contains(&price) {
                    break 'valid;
                }
                event.price = price as u32;
                let product = key::key(data, fields[5].0, fields[5].1);
                event.category = context.products.get(product).unwrap_or(context.unknown_category);
            } else {
                break 'valid;
            }
            event.ts = (ts - context.base) as u32;
        }
        // SAFETY: a partition number is below PARTITIONS, the length of `out`.
        unsafe { out.get_unchecked_mut(partition(id)) }.push(event);
    });
}

fn parse_clicks(context: &Context, links: &Links, index: &mut scan::Index, data: &[u8], start: usize, end: usize, out: &mut [Part<Click>]) {
    let mut dates = time::Dates::new();
    scan::scan::<4>(index, data, start, end, |fields| {
        // Copies of a click are identical and nothing counts clicks, so no dedup is needed:
        // a second copy can never change which campaign is credited or assists.
        let id = key::key(data, fields[0].0, fields[0].1);
        let user = key::key(data, fields[1].0, fields[1].1);
        if id == 0 || user == 0 {
            return;
        }
        let Some(campaign) = context.campaigns.get(key::key(data, fields[2].0, fields[2].1)) else { return };
        let Some(ts) = time::parse(&data[fields[3].0..fields[3].1], &mut dates) else { return };
        if ts < context.base || ts >= context.window_end {
            return;
        }
        let person = links.person(user);
        out[partition(person)].push(Click { person: split(person), id: split(id), ts: (ts - context.base) as u32, campaign });
    });
}

// ------------------------------------------------------------------ 3. dedup

fn dedup(blocks: &[Block<Event>], table: &mut Vec<u64>, links: &Links, out: &mut [Part<Visit>]) {
    let total: usize = blocks.iter().map(|block| block.len).sum();
    if total == 0 {
        return;
    }
    let size = (total * 2).next_power_of_two();
    table.clear();
    table.resize(size, 0);
    let mask = size - 1;
    let bases: Vec<*mut Event> = blocks.iter().map(|block| block.rows).collect();
    for (block_at, block) in blocks.iter().enumerate() {
        for at in 0..block.len {
            // SAFETY: `at` is inside the block; nothing else holds a reference to this event.
            let event = unsafe { &mut *bases[block_at].add(at) };
            let hash = key::hash(join(event.id));
            let tag = hash & 0xFFFF_FFFF_0000_0000;
            let entry = tag | ((block_at << BLOCK_BITS | at) as u64 + 1);
            let mut slot = hash as usize & mask;
            loop {
                let seen = table[slot];
                if seen == 0 {
                    table[slot] = entry;
                    break;
                }
                if seen & 0xFFFF_FFFF_0000_0000 == tag {
                    let index = (seen as u32 - 1) as usize;
                    // SAFETY: the index was stored from a valid block and offset, and is not this event.
                    let other = unsafe { &mut *bases[index >> BLOCK_BITS].add(index & (BLOCK - 1)) };
                    if other.id == event.id {
                        // Two copies: the earlier ingest is kept, valid or not.
                        if event.ingest < other.ingest {
                            other.ts = INVALID;
                            table[slot] = entry;
                        } else {
                            event.ts = INVALID;
                        }
                        break;
                    }
                }
                slot = (slot + 1) & mask;
            }
        }
    }
    for block in blocks.iter().map(Block::rows) {
        for (at, event) in block.iter().enumerate() {
            if let Some(ahead) = block.get(at + 8) {
                links.prefetch(join(ahead.user));
            }
            if event.ts != INVALID {
                let user = join(event.user);
                let person = links.person(user);
                out[partition(person)].push(Visit { person: split(person), ts: event.ts, price: event.price, category: event.category });
            }
        }
    }
}

// ------------------------------------------------------------------ 4. sessions

#[derive(Clone, Copy, Default)]
struct CampaignDay {
    sessions: u64,
    purchasing: u64,
    revenue: u64,
    assisted: u64,
}

#[derive(Clone, Copy)]
struct PersonClick {
    id: Key,
    ts: u32,
    campaign: u32,
}

const USER_COLUMNS: [Column; 6] = [
    Column { name: "user_id", kind: Kind::Text, nullable: false },
    Column { name: "day", kind: Kind::Date, nullable: false },
    Column { name: "sessions", kind: Kind::Int32, nullable: false },
    Column { name: "events", kind: Kind::Int32, nullable: false },
    Column { name: "revenue_cents", kind: Kind::Int64, nullable: false },
    Column { name: "top_category", kind: Kind::Text, nullable: true },
];

/// One thread's state for stage 4: scratch space, its user_daily file and its campaign totals.
struct Worker<'a> {
    context: &'a Context,
    path: PathBuf,
    writer: Option<Writer>,
    campaign_days: Vec<CampaignDay>,
    // Scratch, reused across partitions.
    table: Vec<u32>,
    people: Vec<Key>,
    visit_person: Vec<u32>,
    visit_start: Vec<u32>,
    visits: Vec<u64>,
    purchases: Vec<(u32, u32)>,
    click_person: Vec<u32>,
    click_start: Vec<u32>,
    clicks: Vec<PersonClick>,
    session_start: Vec<u32>,
    session_day: Vec<u32>,
    category_revenue: Vec<u64>,
    touched: Vec<u32>,
    // The row group being built.
    rows: usize,
    user_id: Vec<u8>,
    day: Vec<u8>,
    sessions: Vec<u8>,
    events: Vec<u8>,
    revenue: Vec<u8>,
    top_valid: Vec<u8>,
    top: Vec<u8>,
}

impl<'a> Worker<'a> {
    fn new(context: &'a Context, path: PathBuf) -> Self {
        Self {
            context,
            path,
            writer: None,
            campaign_days: vec![CampaignDay::default(); (context.campaign_rows.len() + 1) * context.days],
            table: Vec::new(),
            people: Vec::new(),
            visit_person: Vec::new(),
            visit_start: Vec::new(),
            visits: Vec::new(),
            purchases: Vec::new(),
            click_person: Vec::new(),
            click_start: Vec::new(),
            clicks: Vec::new(),
            session_start: Vec::new(),
            session_day: Vec::new(),
            category_revenue: vec![0; context.categories.len()],
            touched: Vec::new(),
            rows: 0,
            user_id: Vec::new(),
            day: Vec::new(),
            sessions: Vec::new(),
            events: Vec::new(),
            revenue: Vec::new(),
            top_valid: Vec::new(),
            top: Vec::new(),
        }
    }

    fn flush(&mut self) {
        if self.rows == 0 {
            return;
        }
        let writer = self.writer.get_or_insert_with(|| {
            Writer::create(&self.path, &USER_COLUMNS).unwrap_or_else(|error| fail(&format!("{}: {error}", self.path.display())))
        });
        let plain = |data| Values { data, validity: None };
        let columns = [
            plain(&self.user_id[..]),
            plain(&self.day),
            plain(&self.sessions),
            plain(&self.events),
            plain(&self.revenue),
            Values { data: &self.top, validity: Some(&self.top_valid) },
        ];
        writer.row_group(self.rows, &columns).unwrap_or_else(|error| fail(&format!("write: {error}")));
        self.rows = 0;
        for buffer in [&mut self.user_id, &mut self.day, &mut self.sessions, &mut self.events, &mut self.revenue, &mut self.top_valid, &mut self.top] {
            buffer.clear();
        }
    }

    /// One user_daily row; the top category comes from `category_revenue`, which is reset.
    fn row(&mut self, person: Key, day: u32, sessions: u32, events: u32, revenue: u64) {
        let length_at = self.user_id.len();
        self.user_id.extend_from_slice(&[0; 4]);
        key::write_bytes(person, &mut self.user_id);
        let length = (self.user_id.len() - length_at - 4) as u32;
        self.user_id[length_at..length_at + 4].copy_from_slice(&length.to_le_bytes());
        self.day.extend_from_slice(&((self.context.first_day + i64::from(day)) as i32).to_le_bytes());
        self.sessions.extend_from_slice(&sessions.to_le_bytes());
        self.events.extend_from_slice(&events.to_le_bytes());
        self.revenue.extend_from_slice(&revenue.to_le_bytes());
        if self.rows % 8 == 0 {
            self.top_valid.push(0);
        }
        if !self.touched.is_empty() {
            let categories = &self.context.categories;
            let mut best = self.touched[0];
            for &category in &self.touched[1..] {
                let (sum, best_sum) = (self.category_revenue[category as usize], self.category_revenue[best as usize]);
                if sum > best_sum || (sum == best_sum && categories[category as usize] < categories[best as usize]) {
                    best = category;
                }
            }
            for &category in &self.touched {
                self.category_revenue[category as usize] = 0;
            }
            self.touched.clear();
            let name = &categories[best as usize];
            self.top.extend_from_slice(&(name.len() as u32).to_le_bytes());
            self.top.extend_from_slice(name);
            *self.top_valid.last_mut().unwrap() |= 1 << (self.rows % 8);
        }
        self.rows += 1;
    }

    fn partition(&mut self, visit_blocks: &[Block<Visit>], click_blocks: &[Block<Click>]) {
        let total: usize = visit_blocks.iter().map(|block| block.len).sum();
        if total == 0 {
            return;
        }
        // Number the people of this partition and count their events.
        let size = (total * 2).next_power_of_two();
        let mask = size - 1;
        self.table.clear();
        self.table.resize(size, 0);
        self.people.clear();
        self.visit_person.clear();
        self.visit_start.clear();
        for visit in visit_blocks.iter().flat_map(Block::rows) {
            let person = join(visit.person);
            let mut slot = key::hash(person) as usize & mask;
            let index = loop {
                let seen = self.table[slot];
                if seen == 0 {
                    self.people.push(person);
                    self.visit_start.push(0);
                    self.table[slot] = self.people.len() as u32;
                    break self.people.len() as u32 - 1;
                }
                if self.people[seen as usize - 1] == person {
                    break seen - 1;
                }
                slot = (slot + 1) & mask;
            };
            self.visit_person.push(index);
            self.visit_start[index as usize] += 1;
        }
        let people = self.people.len();
        // Counts to start offsets, then place each event with its person: time in
        // the high half for sorting, and a reference to its purchase in the low half.
        let mut sum = 0u32;
        for count in self.visit_start.iter_mut() {
            sum += std::mem::replace(count, sum);
        }
        self.visit_start.push(sum);
        self.visits.clear();
        self.visits.resize(total, 0);
        self.purchases.clear();
        for (visit, &person) in visit_blocks.iter().flat_map(Block::rows).zip(&self.visit_person) {
            let mut value = u64::from(visit.ts) << 32;
            if visit.price != 0 {
                self.purchases.push((visit.price, visit.category));
                value |= self.purchases.len() as u64;
            }
            let at = &mut self.visit_start[person as usize];
            self.visits[*at as usize] = value;
            *at += 1;
        }
        // The same for the clicks of people who have events.
        self.click_person.clear();
        self.click_start.clear();
        self.click_start.resize(people + 1, 0);
        for click in click_blocks.iter().flat_map(Block::rows) {
            let person = join(click.person);
            let mut slot = key::hash(person) as usize & mask;
            let index = loop {
                let seen = self.table[slot];
                if seen == 0 {
                    break u32::MAX;
                }
                if self.people[seen as usize - 1] == person {
                    break seen - 1;
                }
                slot = (slot + 1) & mask;
            };
            self.click_person.push(index);
            if index != u32::MAX {
                self.click_start[index as usize] += 1;
            }
        }
        let mut sum = 0u32;
        for count in self.click_start.iter_mut() {
            sum += std::mem::replace(count, sum);
        }
        self.clicks.clear();
        self.clicks.resize(sum as usize, PersonClick { id: 0, ts: 0, campaign: 0 });
        for (click, &person) in click_blocks.iter().flat_map(Block::rows).zip(&self.click_person) {
            if person != u32::MAX {
                let at = &mut self.click_start[person as usize];
                self.clicks[*at as usize] = PersonClick { id: join(click.id), ts: click.ts, campaign: click.campaign };
                *at += 1;
            }
        }
        // After placing, each start offset has moved to its person's end.
        let mut visits = std::mem::take(&mut self.visits);
        let mut clicks = std::mem::take(&mut self.clicks);
        let purchases = std::mem::take(&mut self.purchases);
        let (mut visit_from, mut click_from) = (0usize, 0usize);
        for person in 0..people {
            let (visit_to, click_to) = (self.visit_start[person] as usize, self.click_start[person] as usize);
            self.person(self.people[person], &mut visits[visit_from..visit_to], &mut clicks[click_from..click_to], &purchases);
            (visit_from, click_from) = (visit_to, click_to);
        }
        (self.visits, self.clicks, self.purchases) = (visits, clicks, purchases);
        if self.rows >= ROW_GROUP {
            self.flush();
        }
    }

    /// One person: their events (time << 32 | purchase reference) and their valid clicks.
    fn person(&mut self, person: Key, visits: &mut [u64], clicks: &mut [PersonClick], purchases: &[(u32, u32)]) {
        let context = self.context;
        let days = context.days;
        let organic = context.campaign_rows.len();
        visits.sort_unstable();
        // By time; within one instant the smallest click_id first.
        clicks.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| key::compare(a.id, b.id)));
        self.session_start.clear();
        self.session_day.clear();

        let (mut next_click, mut best) = (0usize, usize::MAX);
        let (mut row_day, mut row_sessions, mut row_events, mut row_revenue) = (u32::MAX, 0u32, 0u32, 0u64);
        let mut at = 0;
        while at < visits.len() {
            // A session: from `at` until a gap of over 30 minutes.
            let start = (visits[at] >> 32) as u32;
            let day = ((u64::from(start) + context.day_shift) / DAY as u64) as u32 - 7;
            if day != row_day {
                if row_day != u32::MAX {
                    self.row(person, row_day, row_sessions, row_events, row_revenue);
                }
                (row_day, row_sessions, row_events, row_revenue) = (day, 0, 0, 0);
            }
            let first = at;
            let (mut previous, mut revenue, mut purchased) = (start, 0u64, false);
            while at < visits.len() {
                let ts = (visits[at] >> 32) as u32;
                if ts - previous > SESSION_GAP {
                    break;
                }
                previous = ts;
                let purchase = visits[at] as u32;
                if purchase != 0 {
                    let (price, category) = purchases[purchase as usize - 1];
                    revenue += u64::from(price);
                    purchased = true;
                    let sum = &mut self.category_revenue[category as usize];
                    if *sum == 0 {
                        self.touched.push(category);
                    }
                    *sum += u64::from(price);
                }
                at += 1;
            }
            // Credit: the latest click at or before the start, if within 7 days.
            while next_click < clicks.len() && clicks[next_click].ts <= start {
                if best == usize::MAX || clicks[next_click].ts != clicks[best].ts {
                    best = next_click;
                }
                next_click += 1;
            }
            let campaign = if best != usize::MAX && start - clicks[best].ts <= WEEK { clicks[best].campaign as usize } else { organic };
            let total = &mut self.campaign_days[campaign * days + day as usize];
            total.sessions += 1;
            total.purchasing += u64::from(purchased);
            total.revenue += revenue;
            row_sessions += 1;
            row_events += (at - first) as u32;
            row_revenue += revenue;
            self.session_start.push(start);
            self.session_day.push(day);
        }
        if row_day != u32::MAX {
            self.row(person, row_day, row_sessions, row_events, row_revenue);
        }
        if clicks.is_empty() {
            return;
        }
        // Assists: per campaign, merge the 7 days after each click into intervals;
        // every session starting inside one is assisted once.
        clicks.sort_unstable_by_key(|click| (click.campaign, click.ts));
        let (starts, session_days) = (&self.session_start[..], &self.session_day[..]);
        let mut at = 0;
        while at < clicks.len() {
            let campaign = clicks[at].campaign;
            let from = clicks[at].ts;
            let mut to = from + WEEK;
            at += 1;
            while at < clicks.len() && clicks[at].campaign == campaign && clicks[at].ts <= to {
                to = clicks[at].ts + WEEK;
                at += 1;
            }
            let mut session = starts.partition_point(|&start| start < from);
            let end = session + starts[session..].partition_point(|&start| start <= to);
            while session < end {
                let day = session_days[session];
                let same = session_days[session..end].partition_point(|&other| other == day);
                self.campaign_days[campaign as usize * days + day as usize].assisted += same as u64;
                session += same;
            }
        }
    }
}

// ------------------------------------------------------------------ main

fn main() {
    let started = Instant::now();
    let env = |name: &str, default: &str| std::env::var(name).unwrap_or_else(|_| default.to_string());
    let data_dir = PathBuf::from(env("DATA_DIR", "/data"));
    let output_dir = PathBuf::from(env("OUTPUT_DIR", "/output"));
    let threads = env("NCPU", "0")
        .parse::<usize>()
        .ok()
        .filter(|&count| count > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(4, usize::from));
    // Address space for the large allocations: generous, since only touched pages count.
    let memory = env("MEM_GB", "0").parse::<usize>().ok().filter(|&gb| gb > 0).unwrap_or(16);
    bump::reserve(memory << 30);
    let log = |stage: &str| eprintln!("{:8.3}s {stage}", started.elapsed().as_secs_f64());

    let manifest = fs::read(data_dir.join("manifest.json")).unwrap_or_else(|error| fail(&format!("manifest.json: {error}")));
    let window_start = manifest_time(&manifest, "window_start");
    let window_end = manifest_time(&manifest, "window_end");
    let base = window_start - 7 * DAY;
    if window_end - base + 7 * DAY >= i64::from(u32::MAX) {
        fail("the window is too long for 32-bit millisecond offsets (limit about 35 days)");
    }

    let mut categories: Vec<Vec<u8>> = Vec::new();
    let mut category_ids: std::collections::HashMap<Vec<u8>, u32> = std::collections::HashMap::new();
    let product_rows = read_lookup(&data_dir.join("products.csv"));
    let mut products = KeyMap::with_capacity(product_rows.len());
    for (product, category) in product_rows {
        let id = *category_ids.entry(category.clone()).or_insert_with(|| {
            categories.push(category);
            categories.len() as u32 - 1
        });
        if product != 0 {
            products.insert(product, id);
        }
    }
    categories.push(b"unknown".to_vec());
    let campaign_rows = read_lookup(&data_dir.join("campaigns.csv"));
    let mut campaigns = KeyMap::with_capacity(campaign_rows.len());
    let mut campaign_names = Vec::new();
    for (campaign, channel) in campaign_rows {
        if campaign != 0 && campaigns.insert(campaign, campaign_names.len() as u32) == campaign_names.len() as u32 {
            let mut name = Vec::new();
            key::write_bytes(campaign, &mut name);
            campaign_names.push((name, channel));
        }
    }
    let first_day = window_start.div_euclid(DAY);
    let context = Context {
        window_start,
        window_end,
        base,
        products,
        unknown_category: categories.len() as u32 - 1,
        categories,
        campaigns,
        campaign_rows: campaign_names,
        days: ((window_end - 1).div_euclid(DAY) - first_day + 1).max(1) as usize,
        first_day,
        day_shift: window_start.rem_euclid(DAY) as u64,
    };
    let context = &context;

    let mut files = Vec::new();
    let link_tasks = map_folder(&data_dir.join("links"), &mut files);
    let event_tasks = map_folder(&data_dir.join("events"), &mut files);
    let click_tasks = map_folder(&data_dir.join("clicks"), &mut files);
    let files = &files;
    log("mapped inputs");

    // 1. Links: parse and number the IDs with every thread, then one thread joins
    // them while the others start on the events.
    let next = AtomicUsize::new(0);
    let sides: Vec<(Vec<Vec<Side>>, usize)> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let (link_tasks, next) = (&link_tasks, &next);
                scope.spawn(move || {
                    let mut sides: Vec<Vec<Side>> = (0..SHARDS).map(|_| Vec::new()).collect();
                    let mut count = 0u32;
                    let mut index = scan::Index::default();
                    while let Some(&(file, start, end)) = link_tasks.get(next.fetch_add(1, Ordering::Relaxed)) {
                        let data = files[file].bytes();
                        scan::scan::<2>(&mut index, data, start, end, |fields| {
                            let a = key::key(data, fields[0].0, fields[0].1);
                            let b = key::key(data, fields[1].0, fields[1].1);
                            // A self-link changes nothing: an ID alone is its own person anyway.
                            if a != 0 && b != 0 && a != b {
                                for (key, side) in [(a, 0), (b, 1)] {
                                    sides[(key::hash(key) >> (64 - SHARD_BITS)) as usize].push((key, count << 1 | side));
                                }
                                count += 1;
                            }
                        });
                    }
                    (sides, count as usize)
                })
            })
            .collect();
        handles.into_iter().map(|handle| handle.join().unwrap()).collect()
    });
    log("links parsed");
    let (sides, counts): (Vec<_>, Vec<usize>) = sides.into_iter().unzip();
    let shards: Vec<Mutex<Shard>> = (0..SHARDS).map(|_| Mutex::new(Shard::default())).collect();
    let next = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| number_ids(&sides, &shards, &next));
        }
    });
    log("links parsed and numbered");

    // 2. Events, then clicks. The clicks wait for the links, since they are sent by person.
    let links: OnceLock<Links> = OnceLock::new();
    let shards = Mutex::new(Some(shards));
    let (next_event, next_click) = (AtomicUsize::new(0), AtomicUsize::new(0));
    let parsed: Vec<(Vec<Part<Event>>, Vec<Part<Click>>)> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|thread| {
                let (links, sides, counts, shards) = (&links, &sides, &counts, &shards);
                let (event_tasks, click_tasks, next_event, next_click) = (&event_tasks, &click_tasks, &next_event, &next_click);
                scope.spawn(move || {
                    if thread == 0 {
                        let shards = shards.lock().unwrap().take().unwrap();
                        let _ = links.set(join_links(sides, counts, shards));
                        log("links joined");
                    }
                    let (mut events, mut clicks) = (parts(), parts());
                    let mut index = scan::Index::default();
                    while let Some(&(file, start, end)) = event_tasks.get(next_event.fetch_add(1, Ordering::Relaxed)) {
                        parse_events(context, &mut index, files[file].bytes(), start, end, &mut events);
                    }
                    let links = loop {
                        match links.get() {
                            Some(links) => break links,
                            None => std::thread::yield_now(),
                        }
                    };
                    while let Some(&(file, start, end)) = click_tasks.get(next_click.fetch_add(1, Ordering::Relaxed)) {
                        parse_clicks(context, links, &mut index, files[file].bytes(), start, end, &mut clicks);
                    }
                    (events, clicks)
                })
            })
            .collect();
        handles.into_iter().map(|handle| handle.join().unwrap()).collect()
    });
    let links = links.get().unwrap();
    let (events, clicks): (Vec<_>, Vec<_>) = parsed.into_iter().unzip();
    let (events, clicks) = (gather(events), gather(clicks));
    log("parsed events and clicks");

    // 3. Dedup each event partition and regroup the survivors by person.
    let next = AtomicUsize::new(0);
    let visits: Vec<Vec<Part<Visit>>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let (events, next) = (&events, &next);
                scope.spawn(move || {
                    let mut out = parts();
                    let mut table = Vec::new();
                    while let Some(blocks) = events.get(next.fetch_add(1, Ordering::Relaxed)) {
                        let blocks = std::mem::take(&mut *blocks.lock().unwrap());
                        dedup(&blocks, &mut table, links, &mut out);
                        // The survivors of later partitions go into these blocks.
                        blocks.into_iter().for_each(Block::recycle);
                    }
                    out
                })
            })
            .collect();
        handles.into_iter().map(|handle| handle.join().unwrap()).collect()
    });
    let visits = gather(visits);
    log("deduplicated events");

    // 4. Sessions, credit and assists per person partition; each thread writes its own user_daily file.
    let user_dir = output_dir.join("user_daily");
    let campaign_dir = output_dir.join("campaign_daily");
    for folder in [&user_dir, &campaign_dir] {
        fs::create_dir_all(folder).unwrap_or_else(|error| fail(&format!("{}: {error}", folder.display())));
    }
    let next = AtomicUsize::new(0);
    let totals: Vec<(Vec<CampaignDay>, bool)> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|thread| {
                let (visits, clicks, next, user_dir) = (&visits, &clicks, &next, &user_dir);
                scope.spawn(move || {
                    let mut worker = Worker::new(context, user_dir.join(format!("part-{thread}.parquet")));
                    loop {
                        let at = next.fetch_add(1, Ordering::Relaxed);
                        if at >= PARTITIONS {
                            break;
                        }
                        let visit_blocks = std::mem::take(&mut *visits[at].lock().unwrap());
                        let click_blocks = std::mem::take(&mut *clicks[at].lock().unwrap());
                        worker.partition(&visit_blocks, &click_blocks);
                    }
                    worker.flush();
                    let wrote = worker.writer.is_some();
                    if let Some(writer) = worker.writer.take() {
                        writer.finish().unwrap_or_else(|error| fail(&format!("write: {error}")));
                    }
                    (std::mem::take(&mut worker.campaign_days), wrote)
                })
            })
            .collect();
        handles.into_iter().map(|handle| handle.join().unwrap()).collect()
    });
    if !totals.iter().any(|&(_, wrote)| wrote) {
        // No rows at all: the table still needs a file with its schema.
        let writer = Writer::create(&user_dir.join("part-0.parquet"), &USER_COLUMNS);
        writer.and_then(Writer::finish).unwrap_or_else(|error| fail(&format!("write: {error}")));
    }
    log("sessions done, user_daily written");

    // 5. campaign_daily: add up the threads' totals.
    let columns = [
        Column { name: "campaign_id", kind: Kind::Text, nullable: false },
        Column { name: "channel", kind: Kind::Text, nullable: false },
        Column { name: "day", kind: Kind::Date, nullable: false },
        Column { name: "sessions", kind: Kind::Int64, nullable: false },
        Column { name: "purchasing_sessions", kind: Kind::Int64, nullable: false },
        Column { name: "revenue_cents", kind: Kind::Int64, nullable: false },
        Column { name: "assisted_sessions", kind: Kind::Int64, nullable: false },
    ];
    let mut data: [Vec<u8>; 7] = Default::default();
    let mut rows = 0;
    let text = |out: &mut Vec<u8>, value: &[u8]| {
        out.extend_from_slice(&(value.len() as u32).to_le_bytes());
        out.extend_from_slice(value);
    };
    for campaign in 0..=context.campaign_rows.len() {
        for day in 0..context.days {
            let mut sum = CampaignDay::default();
            for (total, _) in &totals {
                let total = &total[campaign * context.days + day];
                sum.sessions += total.sessions;
                sum.purchasing += total.purchasing;
                sum.revenue += total.revenue;
                sum.assisted += total.assisted;
            }
            if sum.sessions == 0 && sum.assisted == 0 {
                continue;
            }
            let (name, channel): (&[u8], &[u8]) = match context.campaign_rows.get(campaign) {
                Some((name, channel)) => (name, channel),
                None => (b"organic", b"organic"),
            };
            text(&mut data[0], name);
            text(&mut data[1], channel);
            data[2].extend_from_slice(&((context.first_day + day as i64) as i32).to_le_bytes());
            for (column, value) in [sum.sessions, sum.purchasing, sum.revenue, sum.assisted].into_iter().enumerate() {
                data[3 + column].extend_from_slice(&value.to_le_bytes());
            }
            rows += 1;
        }
    }
    let values: Vec<Values> = data.iter().map(|data| Values { data, validity: None }).collect();
    let written = Writer::create(&campaign_dir.join("part-0.parquet"), &columns).and_then(|mut writer| {
        writer.row_group(rows, &values)?;
        writer.finish()
    });
    written.unwrap_or_else(|error| fail(&format!("campaign_daily: {error}")));
    log("campaign_daily written");
    if let Ok(status) = fs::read_to_string("/proc/self/status") {
        if let Some(peak) = status.lines().find(|line| line.starts_with("VmHWM")) {
            eprintln!("peak memory: {}", peak.trim_start_matches("VmHWM:").trim());
        }
    }
    // Leave without unmapping or freeing anything: the kernel does it faster.
    std::process::exit(0);
}
