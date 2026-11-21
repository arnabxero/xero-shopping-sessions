//! Input files. A file already in the page cache is memory-mapped (no copy).
//! A file that is not is read by the workers with positioned reads into their
//! own buffers: with O_DIRECT where the filesystem allows it (straight from the
//! device, no page cache work), else buffered. Every mode gives a task the
//! lines that start in its nominal byte range.

use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::fs::File;
use std::path::Path;

use memmap2::Mmap;

use crate::csv::memchr_nl;

pub struct InFile {
    file: Option<File>,
    /// The same file opened with O_DIRECT, for uncached files.
    direct: Option<File>,
    len: usize,
    map: Option<Mmap>,
}

pub struct Input {
    pub files: Vec<InFile>,
}

/// How to read the inputs.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Mode {
    Mmap,
    Pread,
    Direct,
    /// Per file: mmap when its pages are already cached, O_DIRECT otherwise.
    Auto,
}

const STEP: usize = 1 << 16;
const ALIGN: usize = 4096;

/// Read exactly buf.len() bytes at `off`, or up to the end of the file.
/// Returns the bytes read, or None on an error.
fn read_upto(f: &File, buf: &mut [u8], off: usize) -> Option<usize> {
    let mut done = 0;
    while done < buf.len() {
        #[cfg(unix)]
        let n = std::os::unix::fs::FileExt::read_at(f, &mut buf[done..], (off + done) as u64);
        #[cfg(windows)]
        let n = std::os::windows::fs::FileExt::seek_read(f, &mut buf[done..], (off + done) as u64);
        match n {
            Ok(0) => break,
            Ok(n) => done += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(_) => return None,
        }
    }
    Some(done)
}

/// A reusable, page-aligned read buffer. Bytes `skip..skip + used` of the
/// allocation hold bytes `start..start + used` of a file.
pub struct ChunkBuf {
    ptr: *mut u8,
    cap: usize,
    skip: usize,
    used: usize,
}

unsafe impl Send for ChunkBuf {}

impl Default for ChunkBuf {
    fn default() -> Self {
        ChunkBuf { ptr: std::ptr::null_mut(), cap: 0, skip: 0, used: 0 }
    }
}

impl Drop for ChunkBuf {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { dealloc(self.ptr, Layout::from_size_align(self.cap, ALIGN).unwrap()) };
        }
    }
}

impl ChunkBuf {
    /// Room for `n` bytes from the start of the allocation, keeping the contents.
    fn reserve(&mut self, n: usize) {
        if n <= self.cap {
            return;
        }
        let cap = n.next_power_of_two().max(1 << 20);
        let p = unsafe { alloc_zeroed(Layout::from_size_align(cap, ALIGN).unwrap()) };
        assert!(!p.is_null(), "out of memory");
        if !self.ptr.is_null() {
            unsafe {
                std::ptr::copy_nonoverlapping(self.ptr, p, self.skip + self.used);
                dealloc(self.ptr, Layout::from_size_align(self.cap, ALIGN).unwrap());
            }
        }
        self.ptr = p;
        self.cap = cap;
    }

    fn data(&self) -> &[u8] {
        if self.used == 0 {
            return &[];
        }
        unsafe { std::slice::from_raw_parts(self.ptr.add(self.skip), self.used) }
    }

    fn spare(&mut self, n: usize) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.add(self.skip + self.used), n) }
    }

    /// Append bytes [from, to) of `f` (buffered read).
    fn append(&mut self, f: &File, from: usize, to: usize) {
        self.reserve(self.skip + self.used + (to - from));
        let n = to - from;
        let got = read_upto(f, self.spare(n), from).expect("read failed");
        assert_eq!(got, n, "unexpected end of input file");
        self.used += n;
    }

    /// Load bytes [from, to) of a file of length `flen` with one O_DIRECT read
    /// of the enclosing aligned range. False if the read fails.
    fn load_direct(&mut self, d: &File, flen: usize, from: usize, to: usize) -> bool {
        let a = from & !(ALIGN - 1);
        let b = (to + ALIGN - 1) & !(ALIGN - 1);
        self.skip = 0;
        self.used = 0;
        self.reserve(b - a);
        let want = to.min(flen) - a;
        let buf = unsafe { std::slice::from_raw_parts_mut(self.ptr, b - a) };
        match read_upto(d, buf, a) {
            Some(got) if got >= want => {
                self.skip = from - a;
                self.used = to - from;
                true
            }
            _ => false,
        }
    }
}

impl Input {
    /// One entry per path; an empty or unreadable file has length 0.
    pub fn open(paths: &[std::path::PathBuf], mode: Mode) -> Input {
        let files = paths
            .iter()
            .map(|p| open_one(p, mode).unwrap_or(InFile { file: None, direct: None, len: 0, map: None }))
            .collect();
        Input { files }
    }

    /// (mapped files, files read with O_DIRECT, non-empty files)
    pub fn counts(&self) -> (usize, usize, usize) {
        let n = self.files.iter().filter(|f| f.len > 0).count();
        let m = self.files.iter().filter(|f| f.map.is_some()).count();
        let d = self.files.iter().filter(|f| f.map.is_none() && f.direct.is_some()).count();
        (m, d, n)
    }

    /// Share of the input bytes that are not memory-mapped (read by the workers).
    pub fn unmapped_share(&self) -> f64 {
        let total: usize = self.files.iter().map(|f| f.len).sum();
        let unmapped: usize = self.files.iter().filter(|f| f.map.is_none()).map(|f| f.len).sum();
        if total == 0 {
            0.0
        } else {
            unmapped as f64 / total as f64
        }
    }

    pub fn len(&self, fi: usize) -> usize {
        self.files[fi].len
    }

    /// Ask the kernel to start reading bytes [s, e) of file `fi` into the page
    /// cache (not for O_DIRECT files, which bypass it).
    pub fn prefetch(&self, fi: usize, s: usize, e: usize) {
        let f = &self.files[fi];
        #[cfg(unix)]
        {
            if let Some(m) = &f.map {
                let _ = m.advise_range(memmap2::Advice::WillNeed, s, e - s);
            } else if f.direct.is_some() {
            } else if let Some(file) = &f.file {
                use std::os::fd::AsRawFd;
                unsafe {
                    libc::posix_fadvise(file.as_raw_fd(), s as libc::off_t, (e - s) as libc::off_t, libc::POSIX_FADV_WILLNEED);
                }
            }
        }
        #[cfg(not(unix))]
        let _ = (f, s, e);
    }

    /// The data lines owned by the nominal range [s, e) of file `fi`: those
    /// whose first byte lies in it, the header (the file's first line)
    /// excluded. The last one may run past e.
    pub fn lines<'a>(&'a self, fi: usize, s: usize, e: usize, buf: &'a mut ChunkBuf) -> &'a [u8] {
        let f = &self.files[fi];
        if let Some(m) = &f.map {
            return chunk_lines(m, s, e);
        }
        let flen = f.len;
        let Some(file) = &f.file else { return &[] };
        // buf holds bytes start..start + used.
        let start = s.saturating_sub(1);
        let mut end = (e + STEP).min(flen);
        let loaded = match &f.direct {
            Some(d) => buf.load_direct(d, flen, start, end),
            None => false,
        };
        if !loaded {
            buf.skip = 0;
            buf.used = 0;
            buf.append(file, start, end);
        }
        // First line start: after the first newline at or after s - 1 (or
        // after the header when s == 0).
        let p0 = loop {
            if let Some(i) = memchr_nl(buf.data()) {
                break i + 1;
            }
            if end >= flen {
                return &[];
            }
            let ne = (end + STEP).min(flen);
            buf.append(file, end, ne);
            end = ne;
        };
        let p1 = if e >= flen {
            if end < flen {
                buf.append(file, end, flen);
            }
            buf.used
        } else {
            // The line holding byte e - 1 ends at the first newline at or after it.
            let mut scanned = e - 1 - start;
            loop {
                if let Some(i) = memchr_nl(&buf.data()[scanned..]) {
                    break scanned + i + 1;
                }
                scanned = buf.used;
                if end >= flen {
                    break buf.used;
                }
                let ne = (end + STEP).min(flen);
                buf.append(file, end, ne);
                end = ne;
            }
        };
        if p0 < p1 {
            &buf.data()[p0..p1]
        } else {
            &[]
        }
    }
}

/// Share of a sample of the file's pages that are in the page cache, probed
/// with non-blocking reads (RWF_NOWAIT fails with EAGAIN instead of waiting
/// for I/O). None if the probe is not supported. (mincore() cannot be used:
/// for files the process may not write it reports every page as resident.)
#[cfg(target_os = "linux")]
fn cached_share(file: &File, len: usize) -> Option<f64> {
    use std::os::fd::AsRawFd;
    const PAGE: usize = 4096;
    let pages = len.div_ceil(PAGE);
    let n = pages.min(16);
    let mut buf = [0u8; 64];
    let mut hit = 0;
    for k in 0..n {
        let off = k * pages / n * PAGE;
        let iov = libc::iovec { iov_base: buf.as_mut_ptr() as *mut libc::c_void, iov_len: buf.len().min(len - off) };
        let r = unsafe { libc::preadv2(file.as_raw_fd(), &iov, 1, off as libc::off_t, libc::RWF_NOWAIT) };
        if r >= 0 {
            hit += 1;
        } else if std::io::Error::last_os_error().raw_os_error() != Some(libc::EAGAIN) {
            return None;
        }
    }
    Some(hit as f64 / n as f64)
}

/// The file opened again with O_DIRECT, if the filesystem accepts it.
#[cfg(target_os = "linux")]
fn open_direct(path: &Path) -> Option<File> {
    use std::os::unix::fs::OpenOptionsExt;
    std::fs::OpenOptions::new().read(true).custom_flags(libc::O_DIRECT).open(path).ok()
}

#[cfg(not(target_os = "linux"))]
fn open_direct(_path: &Path) -> Option<File> {
    None
}

fn open_one(path: &Path, mode: Mode) -> Option<InFile> {
    let file = File::open(path).ok()?;
    let len = file.metadata().ok()?.len() as usize;
    if len == 0 {
        return None;
    }
    // Auto: mostly cached -> mmap; partly cached -> buffered reads (they use
    // the cached pages); mostly uncached or unknown -> O_DIRECT.
    #[cfg(target_os = "linux")]
    let share = if mode == Mode::Auto { cached_share(&file, len) } else { None };
    #[cfg(not(target_os = "linux"))]
    let share: Option<f64> = Some(1.0);
    let use_map = match mode {
        Mode::Mmap => true,
        Mode::Pread | Mode::Direct => false,
        Mode::Auto => share.map_or(false, |c| c >= 0.9),
    };
    let partial = mode == Mode::Auto && share.map_or(false, |c| c >= 0.1);
    let map = if use_map {
        let m = unsafe { Mmap::map(&file).ok()? };
        #[cfg(unix)]
        let _ = m.advise(memmap2::Advice::Sequential);
        Some(m)
    } else {
        None
    };
    let direct = if map.is_none() && mode != Mode::Pread && !partial { open_direct(path) } else { None };
    Some(InFile { file: Some(file), direct, len, map })
}

/// The data lines owned by the nominal range [s, e) of a whole file `data`.
pub fn chunk_lines(data: &[u8], s: usize, e: usize) -> &[u8] {
    let after_nl = |from: usize| -> usize {
        match memchr_nl(&data[from..]) {
            Some(i) => from + i + 1,
            None => data.len(),
        }
    };
    let p0 = if s == 0 { after_nl(0) } else { after_nl(s - 1) };
    let p1 = if e >= data.len() { data.len() } else { after_nl(e - 1) };
    if p0 < p1 {
        &data[p0..p1]
    } else {
        &[]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every read mode chunks random files exactly like the mapping does.
    /// SHOPPING_TEST_DIR picks the directory (a real filesystem exercises O_DIRECT).
    #[test]
    fn modes_match_mmap() {
        let root = std::env::var_os("SHOPPING_TEST_DIR").map_or_else(std::env::temp_dir, Into::into);
        let dir = root.join(format!("shopping-input-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut rnd = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let mut direct_used = 0;
        for round in 0..200 {
            let n = (rnd() % 400_000) as usize + 1;
            let long = round % 5 == 0;
            let data: Vec<u8> = (0..n)
                .map(|_| if rnd() % if long { 200_000 } else { 40 } == 0 { b'\n' } else { b'a' + (rnd() % 3) as u8 })
                .collect();
            let path = dir.join(format!("f{round}.csv"));
            std::fs::write(&path, &data).unwrap();
            let mm = Input::open(&[path.clone()], Mode::Mmap);
            let header_end = data.iter().position(|&b| b == b'\n').map_or(n, |i| i + 1);
            for mode in [Mode::Pread, Mode::Direct, Mode::Auto] {
                let other = Input::open(&[path.clone()], mode);
                direct_used += other.counts().1;
                for chunk in [997usize, 4096, 70_000, 1 << 20] {
                    let mut all_m = Vec::new();
                    let mut all_o = Vec::new();
                    let mut s = 0;
                    let mut buf = ChunkBuf::default();
                    let mut buf2 = ChunkBuf::default();
                    while s < n {
                        let e = (s + chunk).min(n);
                        all_m.extend_from_slice(mm.lines(0, s, e, &mut buf2));
                        all_o.extend_from_slice(other.lines(0, s, e, &mut buf));
                        s = e;
                    }
                    assert_eq!(all_m, all_o, "round {round} chunk {chunk} {mode:?}");
                    assert_eq!(all_m, &data[header_end..], "round {round} chunk {chunk}");
                }
            }
            std::fs::remove_file(&path).unwrap();
        }
        eprintln!("files read with O_DIRECT: {direct_used}");
        let _ = std::fs::remove_dir(&dir);
    }
}
