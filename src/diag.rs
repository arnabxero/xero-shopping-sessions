//! Scorer diagnostics (XERO_DIAG=1): a compact environment and timing report
//! on stderr. Diagnostic builds only.

use std::path::{Path, PathBuf};

pub fn enabled() -> bool {
    std::env::var_os("XERO_DIAG").is_some()
}

fn read(p: &str) -> String {
    std::fs::read_to_string(p).map(|s| s.trim().to_string()).unwrap_or_else(|_| "-".into())
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

fn first_field(text: &str, key: &str) -> String {
    text.lines()
        .find(|l| l.starts_with(key))
        .and_then(|l| l.split_once(':'))
        .map(|(_, v)| v.trim().to_string())
        .unwrap_or_else(|| "-".into())
}

/// Fraction of 16 sampled pages per file that are cached: (RWF_NOWAIT probe, mincore).
#[cfg(target_os = "linux")]
fn residency(files: &[PathBuf]) -> (f64, f64, usize, usize) {
    use std::os::fd::AsRawFd;
    let (mut hit_p, mut hit_m, mut n, mut bytes) = (0usize, 0usize, 0usize, 0usize);
    for path in files {
        let Ok(f) = std::fs::File::open(path) else { continue };
        let len = f.metadata().map(|m| m.len() as usize).unwrap_or(0);
        if len == 0 {
            continue;
        }
        bytes += len;
        let pages = len.div_ceil(4096);
        let k = pages.min(16);
        let map = unsafe { memmap2::Mmap::map(&f).ok() };
        for i in 0..k {
            let off = i * pages / k * 4096;
            let mut buf = [0u8; 64];
            let iov = libc::iovec { iov_base: buf.as_mut_ptr() as *mut libc::c_void, iov_len: 64.min(len - off) };
            let r = unsafe { libc::preadv2(f.as_raw_fd(), &iov, 1, off as libc::off_t, libc::RWF_NOWAIT) };
            hit_p += (r >= 0) as usize;
            if let Some(m) = &map {
                let mut v = [0u8; 1];
                let r = unsafe { libc::mincore(m.as_ptr().add(off) as *mut libc::c_void, 1, v.as_mut_ptr()) };
                hit_m += (r == 0 && v[0] & 1 == 1) as usize;
            }
            n += 1;
        }
    }
    let files_n = files.len();
    if n == 0 {
        return (0.0, 0.0, files_n, bytes);
    }
    (hit_p as f64 / n as f64, hit_m as f64 / n as f64, files_n, bytes)
}

#[cfg(not(target_os = "linux"))]
fn residency(files: &[PathBuf]) -> (f64, f64, usize, usize) {
    (0.0, 0.0, files.len(), 0)
}

/// Mount line for the filesystem holding `path` (longest mount-point prefix).
fn mount_of(path: &str, mountinfo: &str) -> String {
    let mut best: Option<(usize, String)> = None;
    for l in mountinfo.lines() {
        let f: Vec<&str> = l.split(' ').collect();
        if f.len() < 10 {
            continue;
        }
        let mp = f[4];
        if path == mp || path.starts_with(&format!("{}/", mp.trim_end_matches('/'))) || mp == "/" {
            let sep = f.iter().position(|&x| x == "-").unwrap_or(6);
            let fstype = f.get(sep + 1).unwrap_or(&"-");
            let src = f.get(sep + 2).unwrap_or(&"-");
            let line = format!("{} dev={} {} src={} opts={}", mp, f[2], fstype, src, f[5]);
            if best.as_ref().map_or(true, |(n, _)| mp.len() > *n) {
                best = Some((mp.len(), line));
            }
        }
    }
    best.map(|b| b.1).unwrap_or_else(|| "-".into())
}

fn block_queue(dev: &str) -> String {
    // dev is MAJ:MIN; a partition's queue lives in its parent directory.
    let base = format!("/sys/dev/block/{dev}");
    for q in [format!("{base}/queue"), format!("{base}/../queue")] {
        if Path::new(&q).exists() {
            return format!(
                "rot={} ra_kb={} max_kb={} nr_req={} sched={} model={}",
                read(&format!("{q}/rotational")),
                read(&format!("{q}/read_ahead_kb")),
                read(&format!("{q}/max_sectors_kb")),
                read(&format!("{q}/nr_requests")),
                read(&format!("{q}/scheduler")),
                read(&format!("{q}/../device/model")),
            );
        }
    }
    "-".into()
}

pub fn env_report(data_dir: &Path, files: &[PathBuf]) {
    let e = |s: String| eprintln!("DIAG {s}");
    e(format!("main at {:.0} ms after exec", since_exec_ms()));
    e(format!("kernel {}; thp {}", read("/proc/sys/kernel/osrelease"), read("/sys/kernel/mm/transparent_hugepage/enabled")));
    let cpu = read("/proc/cpuinfo");
    let flags = first_field(&cpu, "flags");
    let has = |f: &str| flags.split(' ').any(|x| x == f);
    e(format!(
        "cpu {} | {} MHz | cache {} | cpus {} | avx2 {} avx512f {} avx512bw {}",
        first_field(&cpu, "model name"),
        first_field(&cpu, "cpu MHz"),
        first_field(&cpu, "cache size"),
        cpu.lines().filter(|l| l.starts_with("processor")).count(),
        has("avx2"),
        has("avx512f"),
        has("avx512bw")
    ));
    #[cfg(target_os = "linux")]
    let aff = unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set);
        libc::CPU_COUNT(&set)
    };
    #[cfg(not(target_os = "linux"))]
    let aff = 0;
    e(format!(
        "available_parallelism {:?} affinity {} NCPU {:?} MEM_GB {:?}",
        std::thread::available_parallelism().map(|n| n.get()).ok(),
        aff,
        std::env::var("NCPU").ok(),
        std::env::var("MEM_GB").ok()
    ));
    e(format!(
        "cgroup2 cpu.max={} cpu.weight={} memory.max={} | v1 quota={} period={} mem={}",
        read("/sys/fs/cgroup/cpu.max"),
        read("/sys/fs/cgroup/cpu.weight"),
        read("/sys/fs/cgroup/memory.max"),
        read("/sys/fs/cgroup/cpu/cpu.cfs_quota_us"),
        read("/sys/fs/cgroup/cpu/cpu.cfs_period_us"),
        read("/sys/fs/cgroup/memory/memory.limit_in_bytes")
    ));
    let mi = read("/proc/meminfo");
    e(format!(
        "meminfo total {} avail {} cached {} dirty {}",
        first_field(&mi, "MemTotal"),
        first_field(&mi, "MemAvailable"),
        first_field(&mi, "Cached"),
        first_field(&mi, "Dirty")
    ));
    let mounts = read("/proc/self/mountinfo");
    let data = data_dir.to_string_lossy().to_string();
    for p in [data.as_str(), "/output", "/scratch", "/tmp", "/"] {
        let m = mount_of(p, &mounts);
        e(format!("mount {p}: {m}"));
    }
    let dm = mount_of(&data, &mounts);
    if let Some(dev) = dm.split(' ').find_map(|x| x.strip_prefix("dev=")) {
        e(format!("blockdev {dev}: {}", block_queue(dev)));
    }
    let t = std::time::Instant::now();
    let (p, m, n, bytes) = residency(files);
    e(format!(
        "residency before reading: {n} files {:.0} MB; RWF_NOWAIT cached {:.0}%; mincore {:.0}% (probe {:.1} ms)",
        bytes as f64 / 1e6,
        p * 100.0,
        m * 100.0,
        t.elapsed().as_secs_f64() * 1e3
    ));
}

pub fn final_report(internal_ms: f64) {
    #[cfg(target_os = "linux")]
    unsafe {
        let mut r: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut r);
        eprintln!(
            "DIAG rusage user {:.0} ms sys {:.0} ms minflt {} majflt {} nvcsw {} nivcsw {} inblock {}",
            r.ru_utime.tv_sec as f64 * 1e3 + r.ru_utime.tv_usec as f64 / 1e3,
            r.ru_stime.tv_sec as f64 * 1e3 + r.ru_stime.tv_usec as f64 / 1e3,
            r.ru_minflt,
            r.ru_majflt,
            r.ru_nvcsw,
            r.ru_nivcsw,
            r.ru_inblock
        );
    }
    eprintln!("DIAG done: {:.0} ms since exec, {:.0} ms since main", since_exec_ms(), internal_ms);
}
