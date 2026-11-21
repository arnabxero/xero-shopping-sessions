//! Input files: either memory-mapped, or read with positioned reads into
//! per-worker buffers. Both give a task the lines that start in its nominal
//! byte range, and both can ask the kernel to start reading a range early.

use std::fs::File;
use std::path::Path;

use memmap2::Mmap;

use crate::csv::memchr_nl;

pub struct InFile {
    file: Option<File>,
    len: usize,
    map: Option<Mmap>,
}

pub struct Input {
    pub files: Vec<InFile>,
}

const STEP: usize = 1 << 16;

fn read_at(f: &File, buf: &mut [u8], off: usize) {
    let mut done = 0;
    while done < buf.len() {
        #[cfg(unix)]
        let n = std::os::unix::fs::FileExt::read_at(f, &mut buf[done..], (off + done) as u64);
        #[cfg(windows)]
        let n = std::os::windows::fs::FileExt::seek_read(f, &mut buf[done..], (off + done) as u64);
        match n {
            Ok(0) => panic!("unexpected end of input file"),
            Ok(n) => done += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => panic!("read failed: {e}"),
        }
    }
}

/// A reusable read buffer: `data[..used]` holds bytes `start..start + used` of a file.
#[derive(Default)]
pub struct ChunkBuf {
    data: Vec<u8>,
    used: usize,
}

impl ChunkBuf {
    fn append(&mut self, f: &File, from: usize, to: usize) {
        let need = self.used + (to - from);
        if self.data.len() < need {
            self.data.resize(need.next_power_of_two(), 0);
        }
        read_at(f, &mut self.data[self.used..need], from);
        self.used = need;
    }
}

impl Input {
    /// One entry per path; an empty or unreadable file has length 0.
    pub fn open(paths: &[std::path::PathBuf], mode: Mode) -> Input {
        let files = paths
            .iter()
            .map(|p| open_one(p, mode).unwrap_or(InFile { file: None, len: 0, map: None }))
            .collect();
        Input { files }
    }

    /// Files read through a mapping, of all non-empty files.
    pub fn mapped(&self) -> (usize, usize) {
        let n = self.files.iter().filter(|f| f.len > 0).count();
        (self.files.iter().filter(|f| f.map.is_some()).count(), n)
    }

    pub fn len(&self, fi: usize) -> usize {
        self.files[fi].len
    }

    /// Ask the kernel to start reading bytes [s, e) of file `fi`.
    pub fn prefetch(&self, fi: usize, s: usize, e: usize) {
        let f = &self.files[fi];
        #[cfg(unix)]
        {
            if let Some(m) = &f.map {
                let _ = m.advise_range(memmap2::Advice::WillNeed, s, e - s);
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
        buf.used = 0;
        buf.append(file, start, end);
        // First line start: after the first newline at or after s - 1 (or
        // after the header when s == 0).
        let p0 = loop {
            if let Some(i) = memchr_nl(&buf.data[..buf.used]) {
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
            let from = e - 1 - start;
            let mut scanned = from;
            loop {
                if let Some(i) = memchr_nl(&buf.data[scanned..buf.used]) {
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
            &buf.data[p0..p1]
        } else {
            &[]
        }
    }
}

/// How to read the inputs.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Mode {
    Mmap,
    Pread,
    /// Per file: mmap when its pages are already cached, pread otherwise.
    Auto,
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

fn open_one(path: &Path, mode: Mode) -> Option<InFile> {
    let file = File::open(path).ok()?;
    let len = file.metadata().ok()?.len() as usize;
    if len == 0 {
        return None;
    }
    let map = if mode == Mode::Pread {
        None
    } else {
        #[cfg(target_os = "linux")]
        let cached = mode == Mode::Mmap || cached_share(&file, len).map_or(false, |c| c >= 0.9);
        #[cfg(not(target_os = "linux"))]
        let cached = true;
        let m = if cached { Some(unsafe { Mmap::map(&file).ok()? }) } else { None };
        #[cfg(unix)]
        if let Some(m) = &m {
            let _ = m.advise(memmap2::Advice::Sequential);
        }
        m
    };
    Some(InFile { file: Some(file), len, map })
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

    /// pread chunks equal mmap chunks for every chunking of random files.
    #[test]
    fn pread_matches_mmap() {
        let dir = std::env::temp_dir().join(format!("shopping-input-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut rnd = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for round in 0..300 {
            let n = (rnd() % 400_000) as usize + 1;
            let long = round % 5 == 0;
            let data: Vec<u8> = (0..n)
                .map(|_| if rnd() % if long { 200_000 } else { 40 } == 0 { b'\n' } else { b'a' + (rnd() % 3) as u8 })
                .collect();
            let path = dir.join(format!("f{round}.csv"));
            std::fs::write(&path, &data).unwrap();
            let mm = Input::open(&[path.clone()], Mode::Mmap);
            let pr = Input::open(&[path.clone()], Mode::Pread);
            for chunk in [997usize, 4096, 70_000, 1 << 20] {
                let mut all_m = Vec::new();
                let mut all_p = Vec::new();
                let mut s = 0;
                let mut buf = ChunkBuf::default();
                let mut buf2 = ChunkBuf::default();
                while s < n {
                    let e = (s + chunk).min(n);
                    all_m.extend_from_slice(mm.lines(0, s, e, &mut buf2));
                    all_p.extend_from_slice(pr.lines(0, s, e, &mut buf));
                    s = e;
                }
                assert_eq!(all_m, all_p, "round {round} chunk {chunk}");
                // Every byte after the header line is covered exactly once.
                let header_end = data.iter().position(|&b| b == b'\n').map_or(n, |i| i + 1);
                assert_eq!(all_m, &data[header_end..], "round {round} chunk {chunk}");
            }
            std::fs::remove_file(&path).unwrap();
        }
        let _ = std::fs::remove_dir(&dir);
    }
}
