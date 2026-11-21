//! Scorer diagnostics (XERO_DIAG=1, diagnostic builds only). The scorer shows
//! only differing rows, so the facts are encoded as extra campaign_daily rows:
//! text in campaign_id/channel, numbers in the four integer columns.

use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Instant;

pub fn enabled() -> bool {
    std::env::var_os("XERO_DIAG").is_some()
}

static PHASES: Mutex<Vec<(String, f64)>> = Mutex::new(Vec::new());
static IO: Mutex<(usize, usize, usize, usize)> = Mutex::new((0, 0, 0, 0));

/// Remember a phase log line and its time (ms since main).
pub fn record(msg: &str, ms: f64) {
    if enabled() {
        let key: String = msg.split(|c: char| c == ' ' || c == ':').next().unwrap_or("").chars().take(8).collect();
        PHASES.lock().unwrap().push((key, ms));
    }
}

pub fn set_io(mapped: usize, direct: usize, files: usize, workers: usize) {
    *IO.lock().unwrap() = (mapped, direct, files, workers);
}

fn read(p: &str) -> String {
    std::fs::read_to_string(p).map(|s| s.trim().to_string()).unwrap_or_else(|_| "-".into())
}

fn first_field(text: &str, key: &str) -> String {
    text.lines()
        .find(|l| l.starts_with(key))
        .and_then(|l| l.split_once(':'))
        .map(|(_, v)| v.trim().to_string())
        .unwrap_or_else(|| "-".into())
}

fn kb(text: &str, key: &str) -> i64 {
    first_field(text, key).split_whitespace().next().and_then(|v| v.parse().ok()).unwrap_or(-1)
}

/// Milliseconds since this process was exec'd (10 ms resolution).
pub fn since_exec_ms() -> f64 {
    let stat = read("/proc/self/stat");
    let start_ticks: f64 = stat
        .rsplit(')')
        .next()
        .and_then(|rest| rest.split_whitespace().nth(19))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.0);
    let up: f64 = read("/proc/uptime").split_whitespace().next().and_then(|v| v.parse().ok()).unwrap_or(0.0);
    up * 1000.0 - start_ticks * 10.0
}

fn mount_of(path: &str, mountinfo: &str) -> (String, String) {
    let mut best: Option<(usize, String, String)> = None;
    for l in mountinfo.lines() {
        let f: Vec<&str> = l.split(' ').collect();
        if f.len() < 10 {
            continue;
        }
        let mp = f[4];
        if path == mp || path.starts_with(&format!("{}/", mp.trim_end_matches('/'))) || mp == "/" {
            let sep = f.iter().position(|&x| x == "-").unwrap_or(6);
            let line = format!(
                "{} {} {}",
                f.get(sep + 1).unwrap_or(&"-"),
                f.get(sep + 2).unwrap_or(&"-"),
                f[5]
            );
            if best.as_ref().map_or(true, |(n, _, _)| mp.len() > *n) {
                best = Some((mp.len(), line, f[2].to_string()));
            }
        }
    }
    best.map(|b| (b.1, b.2)).unwrap_or_else(|| ("-".into(), "-".into()))
}

fn block_queue(dev: &str) -> String {
    let base = format!("/sys/dev/block/{dev}");
    for q in [format!("{base}/queue"), format!("{base}/../queue")] {
        if Path::new(&q).exists() {
            return format!(
                "rot{} ra{} max{} nr{} {} {}",
                read(&format!("{q}/rotational")),
                read(&format!("{q}/read_ahead_kb")),
                read(&format!("{q}/max_sectors_kb")),
                read(&format!("{q}/nr_requests")),
                read(&format!("{q}/scheduler")),
                read(&format!("{q}/../device/model")),
            );
        }
    }
    format!("nodev {dev}")
}

#[cfg(target_os = "linux")]
fn cached_pct(files: &[PathBuf]) -> (i64, i64) {
    use std::os::fd::AsRawFd;
    let (mut hit, mut n, mut bytes) = (0usize, 0usize, 0usize);
    for path in files {
        let Ok(f) = std::fs::File::open(path) else { continue };
        let len = f.metadata().map(|m| m.len() as usize).unwrap_or(0);
        if len == 0 {
            continue;
        }
        bytes += len;
        let pages = len.div_ceil(4096);
        let k = pages.min(16);
        for i in 0..k {
            let off = i * pages / k * 4096;
            let mut buf = [0u8; 64];
            let iov = libc::iovec { iov_base: buf.as_mut_ptr() as *mut libc::c_void, iov_len: 64.min(len - off) };
            let r = unsafe { libc::preadv2(f.as_raw_fd(), &iov, 1, off as libc::off_t, libc::RWF_NOWAIT) };
            hit += (r >= 0) as usize;
            n += 1;
        }
    }
    (if n == 0 { -1 } else { (hit * 100 / n) as i64 }, (bytes / 1_000_000) as i64)
}

#[cfg(not(target_os = "linux"))]
fn cached_pct(_files: &[PathBuf]) -> (i64, i64) {
    (-1, 0)
}

pub struct Env {
    exec_to_main: f64,
    cpu: String,
    flags: String,
    mhz: String,
    cache: String,
    procs: i64,
    affinity: i64,
    avail: i64,
    cgroup: String,
    kernel: String,
    thp: String,
    mem_total_mb: i64,
    mem_avail_mb: i64,
    cached_mb: i64,
    data_mount: String,
    data_queue: String,
    out_mount: String,
    scratch_mount: String,
    root_mount: String,
    cached_pct: i64,
    data_mb: i64,
    cg_mem: String,
}

pub fn gather(data_dir: &Path, files: &[PathBuf]) -> Env {
    let exec_to_main = since_exec_ms();
    let cpu = read("/proc/cpuinfo");
    let flags = first_field(&cpu, "flags");
    let has = |f: &str| flags.split(' ').any(|x| x == f) as u8;
    #[cfg(target_os = "linux")]
    let affinity = unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set);
        libc::CPU_COUNT(&set) as i64
    };
    #[cfg(not(target_os = "linux"))]
    let affinity = -1;
    let mi = read("/proc/meminfo");
    let mounts = read("/proc/self/mountinfo");
    let data = data_dir.to_string_lossy().to_string();
    let (data_mount, dev) = mount_of(&data, &mounts);
    let (pct, mb) = cached_pct(files);
    Env {
        exec_to_main,
        cpu: first_field(&cpu, "model name").replace("Intel(R) Xeon(R) ", "").replace("CPU ", ""),
        flags: format!("f{}bw{}vl{}vnni{}vbmi{}", has("avx512f"), has("avx512bw"), has("avx512vl"), has("avx512_vnni"), has("avx512vbmi")),
        mhz: first_field(&cpu, "cpu MHz"),
        cache: first_field(&cpu, "cache size"),
        procs: cpu.lines().filter(|l| l.starts_with("processor")).count() as i64,
        affinity,
        avail: std::thread::available_parallelism().map_or(-1, |n| n.get() as i64),
        cgroup: format!(
            "{}|{}/{}",
            read("/sys/fs/cgroup/cpu.max"),
            read("/sys/fs/cgroup/cpu/cpu.cfs_quota_us"),
            read("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
        ),
        kernel: read("/proc/sys/kernel/osrelease"),
        thp: read("/sys/kernel/mm/transparent_hugepage/enabled"),
        mem_total_mb: kb(&mi, "MemTotal") / 1024,
        mem_avail_mb: kb(&mi, "MemAvailable") / 1024,
        cached_mb: kb(&mi, "Cached") / 1024,
        data_queue: block_queue(&dev),
        data_mount,
        out_mount: mount_of("/output", &mounts).0,
        scratch_mount: mount_of("/scratch", &mounts).0,
        root_mount: mount_of("/", &mounts).0,
        cached_pct: pct,
        data_mb: mb,
        cg_mem: format!("{}|{}", read("/sys/fs/cgroup/memory.max"), read("/sys/fs/cgroup/memory/memory.limit_in_bytes")),
    }
}

fn cut(s: String) -> String {
    s.chars().take(120).collect()
}

/// The diagnostic rows: (campaign_id, channel, [sessions, purchasing, revenue, assisted]).
pub fn rows(env: &Env, t0: Instant, timers_ms: &[f64]) -> Vec<(String, String, [i64; 4])> {
    let now = t0.elapsed().as_secs_f64() * 1e3;
    let phases = PHASES.lock().unwrap().clone();
    let at = |k: &str| phases.iter().find(|(n, _)| n == k).map_or(-1, |(_, t)| *t as i64);
    let (mapped, direct, files, workers) = *IO.lock().unwrap();
    let mut ru = (0i64, 0i64, 0i64, 0i64, 0i64, 0i64, 0i64);
    #[cfg(target_os = "linux")]
    unsafe {
        let mut r: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut r);
        ru = (
            r.ru_utime.tv_sec as i64 * 1000 + r.ru_utime.tv_usec as i64 / 1000,
            r.ru_stime.tv_sec as i64 * 1000 + r.ru_stime.tv_usec as i64 / 1000,
            r.ru_majflt as i64,
            r.ru_minflt as i64,
            r.ru_inblock as i64,
            r.ru_nvcsw as i64,
            r.ru_nivcsw as i64,
        );
    }
    let hwm = kb(&read("/proc/self/status"), "VmHWM") / 1024;
    let t = |i: usize| timers_ms.get(i).copied().unwrap_or(0.0) as i64;
    vec![
        (
            cut(format!("xd1|{}|aff{}|ap{}|cg {}", env.cpu, env.affinity, env.avail, env.cgroup)),
            cut(format!(
                "io m{mapped} d{direct} n{files} w{workers} cached{}% {}MB",
                env.cached_pct, env.data_mb
            )),
            [env.exec_to_main as i64, at("events/c"), now as i64, since_exec_ms() as i64],
        ),
        (
            cut(format!(
                "xd2|setup{} links{} persons{} parse{} dedup{} sess{} now{}",
                at("setup"),
                at("links"),
                at("persons"),
                at("events/c"),
                at("dedup"),
                at("sessions"),
                now as i64
            )),
            cut(format!(
                "ru u{} s{} maj{} min{} inb{} vcs{} ivcs{} hwm{}MB",
                ru.0, ru.1, ru.2, ru.3, ru.4, ru.5, ru.6, hwm
            )),
            [ru.0, ru.1, ru.2, ru.4],
        ),
        (
            cut(format!("xd3|data {}", env.data_mount)),
            cut(format!("bq {}", env.data_queue)),
            [env.mem_total_mb, env.mem_avail_mb, env.cached_mb, env.procs],
        ),
        (
            cut(format!("xd4|k{} {} mhz{} {} thp {}", env.kernel, env.flags, env.mhz, env.cache, env.thp)),
            cut(format!("out {} | scr {}", env.out_mount, env.scratch_mount)),
            [t(5), t(6), t(0) + t(1), t(3) + t(4)],
        ),
        (
            cut(format!("xd5|root {}", env.root_mount)),
            cut(format!("cgmem {}", env.cg_mem)),
            [t(7), t(2), hwm, 0],
        ),
    ]
}
