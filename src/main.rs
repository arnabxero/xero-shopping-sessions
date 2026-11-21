//! Shopping Sessions: the fast pipeline. See rules.md for the rules and
//! README.md for the design.

mod csv;
mod input;
mod keys;
mod map;
mod out;
mod scatter;
mod ts;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Instant;


use map::{mix, U64Map};
use input::{ChunkBuf, Input};
use scatter::Scatter;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const DAY: i64 = 86_400_000;
const GAP: u32 = 1_800_000;
const REACH: u32 = 604_800_000;
const LATE: i64 = 86_400_000;
/// `pc` of an event row that fails validation.
const INVALID: u32 = u32::MAX;
const PRICE_MASK: u32 = 0x00FF_FFFF;

/// Bits of the partition counts (by event_id for dedup, by person for
/// sessions), chosen from the input size so partitions stay cache-sized.
static PBITS: AtomicUsize = AtomicUsize::new(10);
static QBITS: AtomicUsize = AtomicUsize::new(10);

#[inline(always)]
fn pbits() -> u32 {
    PBITS.load(Ordering::Relaxed) as u32
}

#[inline(always)]
fn qbits() -> u32 {
    QBITS.load(Ordering::Relaxed) as u32
}

const CHUNK: usize = 4 << 20;

/// One parsed event line, routed by the hash of its event_id.
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct ERec {
    eid: u64,
    ingest: i64,
    user: u64,
    /// ts - base (base = window_start - 7 days).
    ts: u32,
    /// INVALID, 0 for a view or cart, or price | category << 24 for a purchase.
    pc: u32,
}

/// One valid click.
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct CRec {
    user: u64,
    cid: u64,
    ts: u32,
    camp: u32,
}

/// A surviving event, routed by the hash of its person.
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct PRec {
    person: u64,
    /// ts << 32 | pc
    tspc: u64,
}

#[derive(Clone, Copy, Default)]
struct URow {
    person: u64,
    day: i32,
    sessions: u32,
    events: u32,
    revenue: u64,
    top: i32,
}

struct Ctx {
    base: i64,
    ws: i64,
    we: i64,
    day0: i64,
    ndays: usize,
    /// Campaign columns in the accumulator: campaigns, then organic.
    ncol: usize,
    organic: u32,
    prod_packed: U64Map<u32>,
    prod_other: HashMap<Vec<u8>, u32>,
    unknown_cat: u32,
    cat_names: Vec<Vec<u8>>,
    camp_packed: U64Map<u32>,
    camp_other: HashMap<Vec<u8>, u32>,
    camp_ids: Vec<Vec<u8>>,
    camp_channels: Vec<Vec<u8>>,
}

impl Ctx {
    #[inline(always)]
    fn category(&self, product: &[u8]) -> u32 {
        let found = match keys::pack(product) {
            Some(k) => self.prod_packed.get(k),
            None => self.prod_other.get(product).copied(),
        };
        found.unwrap_or(self.unknown_cat)
    }

    #[inline(always)]
    fn campaign(&self, id: &[u8]) -> Option<u32> {
        match keys::pack(id) {
            Some(k) => self.camp_packed.get(k),
            None => self.camp_other.get(id).copied(),
        }
    }
}

static TIMERS: [std::sync::atomic::AtomicU64; 16] = [const { std::sync::atomic::AtomicU64::new(0) }; 16];

/// CPU time of the calling thread in ns (wall time where unsupported).
#[inline(always)]
fn cpu_now() -> u64 {
    #[cfg(target_os = "linux")]
    unsafe {
        let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
        libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts);
        return ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64;
    }
    #[allow(unreachable_code)]
    {
        static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
        START.get_or_init(Instant::now).elapsed().as_nanos() as u64
    }
}

#[derive(Clone, Copy)]
struct Tm(u64);

impl Tm {
    #[inline(always)]
    fn now() -> Tm {
        Tm(cpu_now())
    }
}

#[inline(always)]
fn tadd(i: usize, t: Tm) {
    TIMERS[i].fetch_add(cpu_now().wrapping_sub(t.0), Ordering::Relaxed);
}

fn rusage() -> String {
    #[cfg(target_os = "linux")]
    unsafe {
        let mut r: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut r);
        return format!(
            "cpu user {:.0} ms sys {:.0} ms, minflt {} majflt {}",
            r.ru_utime.tv_sec as f64 * 1e3 + r.ru_utime.tv_usec as f64 / 1e3,
            r.ru_stime.tv_sec as f64 * 1e3 + r.ru_stime.tv_usec as f64 / 1e3,
            r.ru_minflt,
            r.ru_majflt
        );
    }
    #[allow(unreachable_code)]
    String::new()
}

fn log(msg: &str, t0: Instant) {
    if std::env::var_os("SHOPPING_QUIET").is_none() {
        eprintln!("[{:8.3} ms] {}", t0.elapsed().as_secs_f64() * 1e3, msg);
    }
}

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map_or(false, |x| x == "csv") && p.is_file())
            .collect(),
        Err(_) => Vec::new(),
    };
    v.sort();
    v
}

/// Nominal byte ranges of a file, without touching its data (so no I/O
/// happens before the readahead starts). `chunk_lines` turns a range into the
/// lines that start in it.
fn chunks(len: usize, out: &mut Vec<(usize, usize)>) {
    let mut s = 0;
    while s < len {
        let e = (s + CHUNK).min(len);
        out.push((s, e));
        s = e;
    }
}

/// Lines of a small file after the header.
fn small_rows<const N: usize>(path: &Path, mut f: impl FnMut([&[u8]; N])) {
    let data = std::fs::read(path).unwrap_or_default();
    let mut lines = csv::Lines::new(&data);
    lines.next();
    for line in lines {
        if let Some(r) = csv::split::<N>(line) {
            f(r);
        }
    }
}

fn parse_manifest(path: &Path) -> (i64, i64) {
    let text = std::fs::read(path).expect("manifest.json");
    let find = |name: &str| -> i64 {
        let pat = format!("\"{name}\"");
        let i = text
            .windows(pat.len())
            .position(|w| w == pat.as_bytes())
            .expect("manifest key");
        let rest = &text[i + pat.len()..];
        let q1 = rest.iter().position(|&b| b == b'"').expect("value") + 1;
        let q2 = rest[q1..].iter().position(|&b| b == b'"').expect("value") + q1;
        let t = ts::parse(&rest[q1..q2]);
        assert!(t != ts::BAD && t != ts::HUGE, "bad manifest timestamp");
        t
    };
    (find("window_start"), find("window_end"))
}

/// Run `f(worker, task)` for every task on `threads` threads.
fn par_tasks<F: Fn(usize, usize) + Sync>(threads: usize, ntasks: usize, f: F) {
    let next = AtomicUsize::new(0);
    std::thread::scope(|s| {
        for w in 0..threads {
            let next = &next;
            let f = &f;
            s.spawn(move || loop {
                let t = next.fetch_add(1, Ordering::Relaxed);
                if t >= ntasks {
                    break;
                }
                f(w, t);
            });
        }
    });
}

#[inline(always)]
fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &b in s {
        let d = b.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        v = v * 10 + d as u64;
        if v > 10_000_000 {
            return None;
        }
    }
    if v == 0 {
        None
    } else {
        Some(v as u32)
    }
}

fn parse_events(ctx: &Ctx, data: &[u8], parts: &mut Scatter<ERec>) {
    let (ws, we, base) = (ctx.ws, ctx.we, ctx.base);
    let shift = 64 - pbits();
    csv::for_each_line::<7, 128, _>(data, |f| {
        if f[0].is_empty() {
            return;
        }
        let ingest = ts::parse(f[3]);
        if ingest == ts::BAD {
            return;
        }
        // Readable bytes end at the line end at the latest (the last field's end).
        let end = f[6].as_ptr().wrapping_add(f[6].len());
        let eid = keys::key_bounded(f[0], end);
        let mut rec = ERec { eid, ingest, user: 0, ts: 0, pc: INVALID };
        'v: {
            if f[1].is_empty() || f[5].is_empty() {
                break 'v;
            }
            let t = ts::parse(f[2]);
            if t < ws || t >= we {
                break 'v;
            }
            if ingest.saturating_sub(t) > LATE {
                break 'v;
            }
            let pc = match f[4] {
                b"view" | b"cart" => 0,
                b"purchase" => {
                    let Some(price) = parse_price(f[6]) else { break 'v };
                    price | (ctx.category(f[5]) << 24)
                }
                _ => break 'v,
            };
            rec.user = keys::key_bounded(f[1], end);
            rec.ts = (t - base) as u32;
            rec.pc = pc;
        }
        parts.push((mix(eid) >> shift) as usize, rec);
    });
}

fn parse_clicks(ctx: &Ctx, data: &[u8], out: &mut Vec<CRec>) {
    let (base, we) = (ctx.base, ctx.we);
    csv::for_each_line::<4, 64, _>(data, |f| {
        if f[0].is_empty() || f[1].is_empty() {
            return;
        }
        let t = ts::parse(f[3]);
        if t < base || t >= we {
            return;
        }
        let Some(camp) = ctx.campaign(f[2]) else { return };
        let end = f[3].as_ptr().wrapping_add(f[3].len());
        out.push(CRec { user: keys::key_bounded(f[1], end), cid: keys::key_bounded(f[0], end), ts: (t - base) as u32, camp });
    });
}

fn parse_links(data: &[u8], out: &mut Vec<(u64, u64)>) {
    csv::for_each_line::<2, 64, _>(data, |f| {
        if f[0].is_empty() || f[1].is_empty() {
            return;
        }
        out.push((keys::key(f[0]), keys::key(f[1])));
    });
}

/// Union-find over linked IDs. Returns a map from every linked ID to its
/// person's name (the byte-wise smallest ID of the component).
fn build_persons(pairs: &[Vec<(u64, u64)>]) -> U64Map<u64> {
    let n: usize = pairs.iter().map(|v| v.len()).sum();
    let mut m: U64Map<u64> = U64Map::with_capacity(2 * n);
    // Dense indices for the IDs, in first-seen order; map values are index + 1.
    let mut keys_d: Vec<u64> = Vec::with_capacity(2 * n);
    let mut pairs_d: Vec<(u32, u32)> = Vec::with_capacity(n);
    const AHEAD: usize = 16;
    for v in pairs {
        for (i, &(a, b)) in v.iter().enumerate() {
            if let Some(&(pa, pb)) = v.get(i + AHEAD) {
                m.prefetch(pa);
                m.prefetch(pb);
            }
            let mut dense = |k: u64| -> u32 {
                let s = m.insert_slot(k);
                let x = m.val_at(s);
                if x != 0 {
                    (x - 1) as u32
                } else {
                    keys_d.push(k);
                    m.set_val_at(s, keys_d.len() as u64);
                    (keys_d.len() - 1) as u32
                }
            };
            let da = dense(a);
            let db = dense(b);
            pairs_d.push((da, db));
        }
    }
    let nd = keys_d.len();
    let mut parent: Vec<u32> = (0..nd as u32).collect();
    fn find(p: &mut [u32], mut x: u32) -> u32 {
        while p[x as usize] != x {
            let gp = p[p[x as usize] as usize];
            p[x as usize] = gp;
            x = gp;
        }
        x
    }
    for &(a, b) in &pairs_d {
        let ra = find(&mut parent, a);
        let rb = find(&mut parent, b);
        if ra != rb {
            // The root always holds the smallest key of its component.
            if keys::cmp(keys_d[ra as usize], keys_d[rb as usize]).is_lt() {
                parent[rb as usize] = ra;
            } else {
                parent[ra as usize] = rb;
            }
        }
    }
    let mut name: Vec<u64> = vec![0; nd];
    for d in 0..nd {
        let r = find(&mut parent, d as u32);
        name[d] = keys_d[r as usize];
    }
    for s in 0..m.capacity() {
        if m.key_at(s) != map::EMPTY {
            let d = (m.val_at(s) - 1) as usize;
            m.set_val_at(s, name[d]);
        }
    }
    m
}

struct Agg {
    urows: Vec<URow>,
    camp: Vec<[u64; 4]>,
    cnt: Vec<u32>,
    inact: Vec<bool>,
    active: Vec<u32>,
    cats: Vec<(u32, u64)>,
}

impl Agg {
    fn new(ctx: &Ctx) -> Agg {
        Agg {
            urows: Vec::new(),
            camp: vec![[0; 4]; ctx.ndays * ctx.ncol],
            cnt: vec![0; ctx.ncol],
            inact: vec![false; ctx.ncol],
            active: Vec::new(),
            cats: Vec::new(),
        }
    }
}

#[derive(Clone, Copy)]
struct PClick {
    ts: u32,
    camp: u32,
    cid: u64,
}

fn flush_user(agg: &mut Agg, mut u: URow) {
    let mut top = -1i32;
    let mut best = 0u64;
    for &(c, s) in &agg.cats {
        if s > best || (s == best && (c as i32) < top) {
            best = s;
            top = c as i32;
        }
    }
    u.top = top;
    agg.urows.push(u);
    agg.cats.clear();
}

/// Sessionize one person's events (sorted by ts) and credit them against the
/// person's clicks (sorted by ts, then click_id).
fn process_person(ctx: &Ctx, agg: &mut Agg, person: u64, evs: &[u64], cks: &[PClick]) {
    let n = evs.len();
    let mut i = 0;
    let (mut lo, mut hi) = (0usize, 0usize);
    let mut cur_day = i64::MIN;
    let mut u = URow::default();
    while i < n {
        let start = (evs[i] >> 32) as u32;
        let day = (ctx.base + start as i64).div_euclid(DAY);
        if day != cur_day {
            if cur_day != i64::MIN {
                flush_user(agg, u);
            }
            cur_day = day;
            u = URow { person, day: day as i32, ..URow::default() };
        }
        let mut prev = start;
        let mut cnt = 0u32;
        let mut rev = 0u64;
        let mut purchased = false;
        while i < n {
            let v = evs[i];
            let t = (v >> 32) as u32;
            if t - prev > GAP {
                break;
            }
            let pc = v as u32;
            if pc != 0 {
                purchased = true;
                let price = (pc & PRICE_MASK) as u64;
                let cat = pc >> 24;
                rev += price;
                match agg.cats.iter_mut().find(|(c, _)| *c == cat) {
                    Some(e) => e.1 += price,
                    None => agg.cats.push((cat, price)),
                }
            }
            cnt += 1;
            prev = t;
            i += 1;
        }
        u.sessions += 1;
        u.events += cnt;
        u.revenue += rev;

        while hi < cks.len() && cks[hi].ts <= start {
            let c = cks[hi].camp as usize;
            agg.cnt[c] += 1;
            if !agg.inact[c] {
                agg.inact[c] = true;
                agg.active.push(c as u32);
            }
            hi += 1;
        }
        let lb = start - REACH;
        while lo < hi && cks[lo].ts < lb {
            agg.cnt[cks[lo].camp as usize] -= 1;
            lo += 1;
        }
        let di = (day - ctx.day0) as usize * ctx.ncol;
        let credited = if hi > lo {
            let best = cks[hi - 1].ts;
            let mut j = hi - 1;
            while j > lo && cks[j - 1].ts == best {
                j -= 1;
            }
            cks[j].camp
        } else {
            ctx.organic
        };
        let a = &mut agg.camp[di + credited as usize];
        a[0] += 1;
        a[1] += purchased as u64;
        a[2] += rev;
        if hi > lo {
            let Agg { active, cnt, inact, camp, .. } = agg;
            active.retain(|&c| {
                if cnt[c as usize] > 0 {
                    true
                } else {
                    inact[c as usize] = false;
                    false
                }
            });
            for &c in active.iter() {
                camp[di + c as usize][3] += 1;
            }
        }
    }
    if cur_day != i64::MIN {
        flush_user(agg, u);
    }
    for &c in &agg.active {
        agg.cnt[c as usize] = 0;
        agg.inact[c as usize] = false;
    }
    agg.active.clear();
}

/// Time the pieces of event parsing separately (SHOPPING_PROFILE=1).
fn profile(ctx: &Ctx, input: &Input, tasks: &[(usize, usize, usize)], threads: usize) {
    let sink = AtomicUsize::new(0);
    let run = |name: &str, f: &(dyn Fn(&[u8]) -> usize + Sync)| {
        for _ in 0..2 {
            let t = Instant::now();
            par_tasks(threads, tasks.len(), |_, i| {
                let (fi, s, e) = tasks[i];
                let mut buf = ChunkBuf::default();
                sink.fetch_add(f(input.lines(fi, s, e, &mut buf)), Ordering::Relaxed);
            });
            eprintln!("{name:>28}: {:8.1} ms", t.elapsed().as_secs_f64() * 1e3);
        }
    };
    run("touch bytes", &|d| d.iter().step_by(64).map(|&b| b as usize).sum());
    run("scan rows", &|d| {
        let mut n = 0;
        csv::for_each_row::<7, _>(d, |_| n += 1);
        n
    });
    run("scan rows (line)", &|d| {
        let mut n = 0;
        csv::for_each_line::<7, 128, _>(d, |_| n += 1);
        n
    });
    run("scan + 2 ts", &|d| {
        let mut n = 0;
        csv::for_each_line::<7, 128, _>(d, |f| n += (ts::parse(f[2]) ^ ts::parse(f[3])) as usize);
        n
    });
    run("scan + 2 keys", &|d| {
        let mut n = 0;
        csv::for_each_row::<7, _>(d, |f| {
            if !f[0].is_empty() && !f[1].is_empty() {
                n += (keys::key(f[0]) ^ keys::key(f[1])) as usize
            }
        });
        n
    });
    run("full parse, fresh bufs", &|d| {
        let np = 1usize << pbits();
        let mut parts: Scatter<ERec> = Scatter::new(np, d.len() / 60 / np + 16);
        parse_events(ctx, d, &mut parts);
        (0..np).map(|p| parts.part_len(p)).sum()
    });
    run("full parse, no scatter", &|d| {
        let mut parts: Vec<Vec<ERec>> = vec![Vec::with_capacity(d.len() / 40)];
        let mut n = 0;
        csv::for_each_row::<7, _>(d, |f| {
            if f[0].is_empty() {
                return;
            }
            let ingest = ts::parse(f[3]);
            let t = ts::parse(f[2]);
            parts[0].push(ERec {
                eid: keys::key(f[0]),
                ingest,
                user: if f[1].is_empty() { 0 } else { keys::key(f[1]) },
                ts: (t - ctx.base) as u32,
                pc: f[4].len() as u32,
            });
            n += 1;
        });
        n + parts[0].len()
    });
    eprintln!("sink {}", sink.load(Ordering::Relaxed));
}

fn main() {
    let t0 = Instant::now();
    // Keep freed memory mapped for reuse: purging (MADV_DONTNEED) makes the
    // kernel zero the pages again when the next phase touches them.
    // 15 = mi_option_purge_delay in mimalloc v2's mimalloc.h; -1 = never purge.
    unsafe { libmimalloc_sys::mi_option_set(15, -1) };
    let args: Vec<String> = std::env::args().collect();
    let (data_dir, out_dir) = if args.len() >= 3 {
        (PathBuf::from(&args[args.len() - 2]), PathBuf::from(&args[args.len() - 1]))
    } else {
        (
            PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into())),
            PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into())),
        )
    };
    let threads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(8, |n| n.get()))
        .min(24);

    // ---------------------------------------------------------------- inputs
    let ev_files = list_csv(&data_dir.join("events"));
    let ck_files = list_csv(&data_dir.join("clicks"));
    let ln_files = list_csv(&data_dir.join("links"));
    // SHOPPING_IO=mmap|pread forces a read mode; by default each file is
    // mapped when its pages are already cached and read with pread otherwise.
    let io_mode = match std::env::var("SHOPPING_IO").as_deref() {
        Ok("mmap") => input::Mode::Mmap,
        Ok("pread") => input::Mode::Pread,
        Ok("direct") => input::Mode::Direct,
        _ => input::Mode::Auto,
    };
    let mut ev_tasks: Vec<(usize, usize, usize)> = Vec::new();
    let mut ck_tasks: Vec<(usize, usize, usize)> = Vec::new();
    let mut ln_tasks: Vec<(usize, usize, usize)> = Vec::new();
    let all_files: Vec<PathBuf> = ev_files.iter().chain(ck_files.iter()).chain(ln_files.iter()).cloned().collect();
    let input = std::sync::Arc::new(Input::open(&all_files, io_mode));
    for (fi, _) in all_files.iter().enumerate() {
        let tasks = if fi < ev_files.len() {
            &mut ev_tasks
        } else if fi < ev_files.len() + ck_files.len() {
            &mut ck_tasks
        } else {
            &mut ln_tasks
        };
        let mut cs = Vec::new();
        chunks(input.len(fi), &mut cs);
        tasks.extend(cs.into_iter().map(|(s, e)| (fi, s, e)));
    }
    // Start reading every input range now, in the order the parsers will
    // want it, so the device queue stays full (cold page cache) while the
    // small files are read and the parsers run.
    {
        let inp = input.clone();
        let order: Vec<(usize, usize, usize)> =
            ln_tasks.iter().chain(ck_tasks.iter()).chain(ev_tasks.iter()).copied().collect();
        std::thread::spawn(move || {
            for (fi, s, e) in order {
                inp.prefetch(fi, s, e);
            }
        });
    }

    // ---------------------------------------------------------------- setup
    let (ws, we) = parse_manifest(&data_dir.join("manifest.json"));
    let base = ws - REACH as i64;
    assert!(we > ws, "empty window");
    assert!(we - base < u32::MAX as i64, "window too long for 32-bit offsets");

    let mut cat_set: Vec<Vec<u8>> = Vec::new();
    let mut prods: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    small_rows::<2>(&data_dir.join("products.csv"), |f| {
        prods.push((f[0].to_vec(), f[1].to_vec()));
        cat_set.push(f[1].to_vec());
    });
    cat_set.push(b"unknown".to_vec());
    cat_set.sort();
    cat_set.dedup();
    assert!(cat_set.len() <= 256, "too many categories");
    let cat_index = |c: &[u8]| cat_set.binary_search_by(|x| x.as_slice().cmp(c)).unwrap() as u32;
    let mut prod_packed = U64Map::with_capacity(prods.len());
    let mut prod_other = HashMap::new();
    for (p, c) in &prods {
        let ci = cat_index(c);
        match keys::pack(p) {
            Some(k) => {
                if prod_packed.get(k).is_none() {
                    prod_packed.insert(k, ci);
                }
            }
            None => {
                prod_other.entry(p.clone()).or_insert(ci);
            }
        }
    }
    let unknown_cat = cat_index(b"unknown");

    let mut camp_ids: Vec<Vec<u8>> = Vec::new();
    let mut camp_channels: Vec<Vec<u8>> = Vec::new();
    small_rows::<2>(&data_dir.join("campaigns.csv"), |f| {
        camp_ids.push(f[0].to_vec());
        camp_channels.push(f[1].to_vec());
    });
    let mut camp_packed = U64Map::with_capacity(camp_ids.len());
    let mut camp_other = HashMap::new();
    for (i, c) in camp_ids.iter().enumerate() {
        match keys::pack(c) {
            Some(k) => {
                if camp_packed.get(k).is_none() {
                    camp_packed.insert(k, i as u32);
                }
            }
            None => {
                camp_other.entry(c.clone()).or_insert(i as u32);
            }
        }
    }
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1) as usize;
    let ctx = Ctx {
        base,
        ws,
        we,
        day0,
        ndays,
        ncol: camp_ids.len() + 1,
        organic: camp_ids.len() as u32,
        prod_packed,
        prod_other,
        unknown_cat,
        cat_names: cat_set.clone(),
        camp_packed,
        camp_other,
        camp_ids,
        camp_channels,
    };

    let (mapped, direct, nfiles) = input.counts();
    // Workers reading uncached files wait on the disk; two per CPU keep the
    // CPUs busy parsing while the other half waits (SHOPPING_P1X overrides).
    let p1_threads = match std::env::var("SHOPPING_P1X").ok().and_then(|v| v.parse::<usize>().ok()) {
        Some(x) if x > 0 => threads * x,
        _ if input.unmapped_share() > 0.5 => threads * 2,
        _ => threads,
    }
    .min(31);
    log(
        &format!(
            "setup: {} event, {} click, {} link chunks; files: {mapped} mapped, {direct} O_DIRECT, {nfiles} total; {p1_threads} parse workers",
            ev_tasks.len(),
            ck_tasks.len(),
            ln_tasks.len()
        ),
        t0,
    );
    // About 70 bytes per event line; aim for ~16k rows per partition.
    let ev_bytes: usize = ev_tasks.iter().map(|&(_, s, e)| e - s).sum();
    let est_rows = ev_bytes / 70 + 1;
    let bits = ((est_rows / 16384).max(1).next_power_of_two().trailing_zeros()).clamp(8, 13) as usize;
    PBITS.store(bits, Ordering::Relaxed);
    QBITS.store(bits, Ordering::Relaxed);
    let np = 1usize << pbits();
    let nq = 1usize << qbits();

    if std::env::var_os("SHOPPING_PROFILE").is_some() {
        profile(&ctx, &input, &ev_tasks, threads);
        return;
    }

    // ----------------------------------------- links + events + clicks (+ persons)
    // One task queue: link chunks first, then clicks, then events. The
    // union-find starts as soon as the last link chunk is parsed, while the
    // workers go on with clicks and events.
    let link_out: Vec<Mutex<Vec<(u64, u64)>>> = (0..ln_tasks.len()).map(|_| Mutex::new(Vec::new())).collect();
    let links_done = AtomicUsize::new(0);
    // ------------------------------------------- events + clicks (+ persons)
    // SHOPPING_TEST_SMALLCAP=1 forces tiny partition regions so the overflow paths get tested.
    let smallcap = std::env::var_os("SHOPPING_TEST_SMALLCAP").is_some();
    let per_part = if smallcap { 1 } else { est_rows * 13 / 10 / np / p1_threads + 16 };
    let ev_parts: Vec<Mutex<Option<Scatter<ERec>>>> = (0..p1_threads).map(|_| Mutex::new(None)).collect();
    let ck_out: Vec<Mutex<Vec<CRec>>> = (0..ck_tasks.len()).map(|_| Mutex::new(Vec::new())).collect();
    let persons: Mutex<Option<U64Map<u64>>> = Mutex::new(None);
    let nl = ln_tasks.len();
    let ntasks = nl + ck_tasks.len() + ev_tasks.len();
    let next = AtomicUsize::new(0);
    std::thread::scope(|s| {
        let dsu = s.spawn(|| {
            while links_done.load(Ordering::Acquire) < nl {
                std::thread::park_timeout(std::time::Duration::from_micros(200));
            }
            log("links parsed", t0);
            let link_pairs: Vec<Vec<(u64, u64)>> =
                link_out.iter().map(|m| std::mem::take(&mut *m.lock().unwrap())).collect();
            let tm = Tm::now();
            let m = build_persons(&link_pairs);
            tadd(7, tm);
            log("persons built", t0);
            *persons.lock().unwrap() = Some(m);
        });
        let dsu_thread = dsu.thread().clone();
        for w in 0..p1_threads {
            let (next, ctx, input, ev_parts, ck_out, ck_tasks, ev_tasks, ln_tasks, link_out, links_done) = (
                &next, &ctx, &input, &ev_parts, &ck_out, &ck_tasks, &ev_tasks, &ln_tasks, &link_out, &links_done,
            );
            let dsu_thread = dsu_thread.clone();
            s.spawn(move || {
                let mut guard = ev_parts[w].lock().unwrap();
                let parts = guard.insert(Scatter::new(np, per_part));
                let mut buf = ChunkBuf::default();
                loop {
                    let t = next.fetch_add(1, Ordering::Relaxed);
                    if t >= ntasks {
                        break;
                    }
                    let tm = Tm::now();
                    if t < nl {
                        let (fi, s, e) = ln_tasks[t];
                        let mut v = Vec::new();
                        parse_links(input.lines(fi, s, e, &mut buf), &mut v);
                        *link_out[t].lock().unwrap() = v;
                        if links_done.fetch_add(1, Ordering::AcqRel) + 1 == nl {
                            dsu_thread.unpark();
                        }
                        continue;
                    }
                    let t = t - nl;
                    if t < ck_tasks.len() {
                        let (fi, s, e) = ck_tasks[t];
                        let mut v = Vec::with_capacity((e - s) / 40 + 16);
                        parse_clicks(ctx, input.lines(fi, s, e, &mut buf), &mut v);
                        *ck_out[t].lock().unwrap() = v;
                        tadd(6, tm);
                    } else {
                        let (fi, s, e) = ev_tasks[t - ck_tasks.len()];
                        parse_events(ctx, input.lines(fi, s, e, &mut buf), parts);
                        tadd(5, tm);
                    }
                }
            });
        }
    });
    let persons = persons.into_inner().unwrap().unwrap();
    keys::freeze();
    // Nothing refers to the input mappings any more: unmap them off the
    // critical path instead of at process exit.
    std::thread::spawn(move || drop(input));
    log("events/clicks parsed, persons built", t0);
    if std::env::var_os("SHOPPING_DEBUG").is_some() {
        eprintln!("phase1 thread-ms: events {:.0} clicks {:.0} persons {:.0}",
            TIMERS[5].load(Ordering::Relaxed) as f64 / 1e6, TIMERS[6].load(Ordering::Relaxed) as f64 / 1e6, TIMERS[7].load(Ordering::Relaxed) as f64 / 1e6);
    }

    // ------------------------------------------------ dedup, route by person
    let ev_parts: Vec<Scatter<ERec>> = ev_parts.into_iter().filter_map(|m| m.into_inner().unwrap()).collect();
    let ck_recs: Vec<Vec<CRec>> = ck_out.into_iter().map(|m| m.into_inner().unwrap()).collect();
    let per_q = if smallcap { 1 } else { est_rows * 13 / 10 / nq / threads + 16 };
    let pe: Vec<Mutex<Option<Scatter<PRec>>>> = (0..threads).map(|_| Mutex::new(None)).collect();
    let n_clicks: usize = ck_recs.iter().map(|v| v.len()).sum();
    let per_qc = if smallcap { 1 } else { n_clicks * 13 / 10 / nq / threads + 16 };
    let pc: Vec<Mutex<Option<Scatter<(u64, PClick)>>>> = (0..threads).map(|_| Mutex::new(None)).collect();
    let person_of = |user: u64| persons.get(user).unwrap_or(user);
    const WSHIFT: u32 = 26;
    const IMASK: u32 = (1 << WSHIFT) - 1;
    assert!(2 * p1_threads <= 63);
    par_tasks(threads, np + ck_recs.len(), |w, t| {
        let qshift = 64 - qbits();
        if t < np {
            let srcs: Vec<&[ERec]> = ev_parts.iter().flat_map(|v| v.part(t)).collect();
            let n: usize = srcs.iter().map(|v| v.len()).sum();
            if n == 0 {
                return;
            }
            assert!(srcs.iter().all(|v| v.len() <= IMASK as usize));
            let row = |x: u32| -> &ERec { unsafe { srcs.get_unchecked((x >> WSHIFT) as usize).get_unchecked((x & IMASK) as usize) } };
            let cap = (2 * n).next_power_of_two();
            let mask = cap - 1;
            let tm = Tm::now();
            let mut table: Vec<u32> = vec![u32::MAX; cap];
            for (wi, src) in srcs.iter().enumerate() {
                for (i, r) in src.iter().enumerate() {
                    let id = ((wi as u32) << WSHIFT) | i as u32;
                    let mut s = mix(r.eid) as usize & mask;
                    loop {
                        let x = table[s];
                        if x == u32::MAX {
                            table[s] = id;
                            break;
                        }
                        let o = row(x);
                        if o.eid == r.eid {
                            if r.ingest < o.ingest {
                                table[s] = id;
                            }
                            break;
                        }
                        s = (s + 1) & mask;
                    }
                }
            }
            tadd(0, tm);
            let tm = Tm::now();
            let mut guard = pe[w].lock().unwrap();
            let out = guard.get_or_insert_with(|| Scatter::new(nq, per_q));
            // One pass over the table: each valid winner's person-map slot is
            // prefetched, and the winner is routed 16 winners later.
            let mut emit = |x: u32| {
                let r = row(x);
                let person = person_of(r.user);
                out.push((mix(person) >> qshift) as usize, PRec { person, tspc: ((r.ts as u64) << 32) | r.pc as u64 });
            };
            let mut ring = [0u32; 16];
            let mut n_in = 0usize;
            for &x in &table {
                if x == u32::MAX {
                    continue;
                }
                let r = row(x);
                if r.pc == INVALID {
                    continue;
                }
                persons.prefetch(r.user);
                let slot = n_in & 15;
                if n_in >= 16 {
                    emit(ring[slot]);
                }
                ring[slot] = x;
                n_in += 1;
            }
            for j in n_in.saturating_sub(16)..n_in {
                emit(ring[j & 15]);
            }
            tadd(1, tm);
        } else {
            let tm = Tm::now();
            let mut guard = pc[w].lock().unwrap();
            let out = guard.get_or_insert_with(|| Scatter::new(nq, per_qc));
            let cr = &ck_recs[t - np];
            for (i, c) in cr.iter().enumerate() {
                if let Some(y) = cr.get(i + 24) {
                    persons.prefetch(y.user);
                }
                let person = person_of(c.user);
                out.push((mix(person) >> qshift) as usize, (person, PClick { ts: c.ts, camp: c.camp, cid: c.cid }));
            }
            tadd(2, tm);
        }
    });
    if std::env::var_os("SHOPPING_DEBUG").is_some() {
        let g = |i: usize| TIMERS[i].load(Ordering::Relaxed) as f64 / 1e6;
        eprintln!("phase2 thread-ms: dedup {:.0} winners {:.0} clicks {:.0}", g(0), g(1), g(2));
    }
    drop(ev_parts);
    drop(ck_recs);
    log("dedup + routing done", t0);

    // ------------------------------------- sessionize, attribute, aggregate
    let pe: Vec<Scatter<PRec>> = pe.into_iter().filter_map(|m| m.into_inner().unwrap()).collect();
    let pc: Vec<Scatter<(u64, PClick)>> = pc.into_iter().filter_map(|m| m.into_inner().unwrap()).collect();
    let mut order: Vec<(usize, usize)> =
        (0..nq).map(|q| (pe.iter().map(|v| v.part_len(q)).sum::<usize>(), q)).collect();
    order.sort_unstable_by(|a, b| b.cmp(a));
    let aggs: Vec<Mutex<Agg>> = (0..threads).map(|_| Mutex::new(Agg::new(&ctx))).collect();
    par_tasks(threads, nq, |w, t| {
        let (n, q) = order[t];
        if n == 0 {
            return;
        }
        let mut agg = aggs[w].lock().unwrap();
        let tm = Tm::now();
        let mut lmap: U64Map<u32> = U64Map::with_capacity(n);
        let mut persons_l: Vec<u64> = Vec::new();
        let mut counts: Vec<u32> = Vec::new();
        let mut loc: Vec<u32> = Vec::with_capacity(n);
        for r in pe.iter().flat_map(|v| v.part(q)).flatten() {
            {
                let s = lmap.insert_slot(r.person);
                let l = if lmap.val_at(s) == 0 {
                    persons_l.push(r.person);
                    counts.push(0);
                    let l = persons_l.len() as u32;
                    lmap.set_val_at(s, l);
                    l - 1
                } else {
                    lmap.val_at(s) - 1
                };
                counts[l as usize] += 1;
                loc.push(l);
            }
        }
        let np = persons_l.len();
        let mut off: Vec<u32> = Vec::with_capacity(np + 1);
        let mut acc = 0u32;
        for &c in &counts {
            off.push(acc);
            acc += c;
        }
        off.push(acc);
        let mut cur: Vec<u32> = off[..np].to_vec();
        let mut evs: Vec<u64> = vec![0; n];
        let mut k = 0;
        for r in pe.iter().flat_map(|v| v.part(q)).flatten() {
            {
                let l = loc[k] as usize;
                evs[cur[l] as usize] = r.tspc;
                cur[l] += 1;
                k += 1;
            }
        }
        // clicks of persons with events
        let mut ccounts: Vec<u32> = vec![0; np + 1];
        let mut cloc: Vec<u32> = Vec::new();
        for (person, _) in pc.iter().flat_map(|v| v.part(q)).flatten() {
            {
                let l = lmap.get(*person).unwrap_or(0);
                cloc.push(l);
                ccounts[l as usize] += 1;
            }
        }
        // ccounts index l is local+1, slot 0 collects clicks of persons without events
        let mut coff: Vec<u32> = Vec::with_capacity(np + 2);
        let mut acc = 0u32;
        for &c in &ccounts {
            coff.push(acc);
            acc += c;
        }
        coff.push(acc);
        let mut ccur: Vec<u32> = coff[..np + 1].to_vec();
        let mut cks: Vec<PClick> = vec![PClick { ts: 0, camp: 0, cid: 0 }; acc as usize];
        let mut k = 0;
        for (_, c) in pc.iter().flat_map(|v| v.part(q)).flatten() {
            {
                let l = cloc[k] as usize;
                cks[ccur[l] as usize] = *c;
                ccur[l] += 1;
                k += 1;
            }
        }
        tadd(3, tm);
        let tm = Tm::now();
        for l in 0..np {
            let e = &mut evs[off[l] as usize..off[l + 1] as usize];
            e.sort_unstable();
            let c = &mut cks[coff[l + 1] as usize..coff[l + 2] as usize];
            c.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| keys::cmp(a.cid, b.cid)));
            process_person(&ctx, &mut agg, persons_l[l], e, c);
        }
        tadd(4, tm);
    });
    if std::env::var_os("SHOPPING_DEBUG").is_some() {
        eprintln!("phase3 thread-ms: group {:.0} sort+process {:.0}",
            TIMERS[3].load(Ordering::Relaxed) as f64 / 1e6, TIMERS[4].load(Ordering::Relaxed) as f64 / 1e6);
    }
    log("sessions done", t0);

    // ---------------------------------------------------------------- output
    let aggs: Vec<Agg> = aggs.into_iter().map(|m| m.into_inner().unwrap()).collect();
    let ud = out_dir.join("user_daily");
    let cd = out_dir.join("campaign_daily");
    std::fs::create_dir_all(&ud).expect("mkdir user_daily");
    std::fs::create_dir_all(&cd).expect("mkdir campaign_daily");
    let ctx_ref = &ctx;
    let aggs_ref = &aggs;
    let ud_ref = &ud;
    let cd_ref = &cd;
    par_tasks(threads, threads + 1, |_, t| {
        if t == threads {
            // campaign_daily
            let mut ids = Vec::new();
            let mut id_ends = Vec::new();
            let mut chans = Vec::new();
            let mut chan_ends = Vec::new();
            let (mut day, mut s, mut p, mut r, mut a) = (vec![], vec![], vec![], vec![], vec![]);
            for di in 0..ctx_ref.ndays {
                for c in 0..ctx_ref.ncol {
                    let mut tot = [0u64; 4];
                    for g in aggs_ref {
                        let x = &g.camp[di * ctx_ref.ncol + c];
                        for j in 0..4 {
                            tot[j] += x[j];
                        }
                    }
                    if tot[0] == 0 && tot[3] == 0 {
                        continue;
                    }
                    if c == ctx_ref.organic as usize {
                        ids.extend_from_slice(b"organic");
                        chans.extend_from_slice(b"organic");
                    } else {
                        ids.extend_from_slice(&ctx_ref.camp_ids[c]);
                        chans.extend_from_slice(&ctx_ref.camp_channels[c]);
                    }
                    id_ends.push(ids.len());
                    chan_ends.push(chans.len());
                    day.push((ctx_ref.day0 + di as i64) as i32);
                    s.push(tot[0] as i64);
                    p.push(tot[1] as i64);
                    r.push(tot[2] as i64);
                    a.push(tot[3] as i64);
                }
            }
            out::write(
                &cd_ref.join("part-0.parquet"),
                out::CAMPAIGN_SCHEMA,
                vec![
                    out::Col::Str(ids, id_ends),
                    out::Col::Str(chans, chan_ends),
                    out::Col::I32(day),
                    out::Col::I64(s),
                    out::Col::I64(p),
                    out::Col::I64(r),
                    out::Col::I64(a),
                ],
            )
            .expect("write campaign_daily");
        } else {
            let rows = &aggs_ref[t].urows;
            if rows.is_empty() && t != 0 {
                return;
            }
            let mut names = Vec::with_capacity(rows.len() * 10);
            let mut name_ends = Vec::with_capacity(rows.len());
            let mut cats = Vec::new();
            let mut cat_ends = Vec::new();
            let mut defs = Vec::with_capacity(rows.len());
            let (mut day, mut s, mut e, mut r) = (
                Vec::with_capacity(rows.len()),
                Vec::with_capacity(rows.len()),
                Vec::with_capacity(rows.len()),
                Vec::with_capacity(rows.len()),
            );
            for u in rows {
                keys::decode_into(u.person, &mut names);
                name_ends.push(names.len());
                day.push(u.day);
                s.push(u.sessions as i64);
                e.push(u.events as i64);
                r.push(u.revenue as i64);
                if u.top >= 0 {
                    cats.extend_from_slice(&ctx_ref.cat_names[u.top as usize]);
                    cat_ends.push(cats.len());
                    defs.push(1i16);
                } else {
                    defs.push(0i16);
                }
            }
            out::write(
                &ud_ref.join(format!("part-{t}.parquet")),
                out::USER_SCHEMA,
                vec![
                    out::Col::Str(names, name_ends),
                    out::Col::I32(day),
                    out::Col::I64(s),
                    out::Col::I64(e),
                    out::Col::I64(r),
                    out::Col::OptStr(cats, cat_ends, defs),
                ],
            )
            .expect("write user_daily");
        }
    });
    log("output written", t0);
    if std::env::var_os("SHOPPING_DEBUG").is_some() {
        eprintln!("{}", rusage());
        for f in ["/proc/self/status", "/proc/self/smaps_rollup"] {
            if let Ok(text) = std::fs::read_to_string(f) {
                for l in text.lines() {
                    if l.starts_with("VmHWM") || l.starts_with("VmRSS") || l.starts_with("AnonHugePages") || l.starts_with("Rss") {
                        eprintln!("{f}: {l}");
                    }
                }
            }
        }
    }
    let _ = keys::any_fallback();
    // Skip destructors of the big tables; the kernel reclaims everything.
    std::process::exit(0);
}
