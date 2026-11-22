//! Spec-exact SIMD CSV scanner, L1 window date table,
//! and SWAR timestamp / price parsers.

use std::fs::File;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;

pub const DAY_MS: i64 = 86_400_000;

#[cfg(target_os = "linux")]
extern "C" {
    fn posix_fadvise(fd: i32, offset: i64, len: i64, advice: i32) -> i32;
}

/// Hint sequential kernel readahead (`POSIX_FADV_SEQUENTIAL = 2`) for sequential chunk reads.
#[inline]
pub fn fadvise_sequential(f: &File) {
    #[cfg(target_os = "linux")]
    unsafe {
        let _ = posix_fadvise(f.as_raw_fd(), 0, 0, 2);
    }
    #[cfg(not(target_os = "linux"))]
    let _ = f;
}

#[cfg(unix)]
#[inline]
fn read_at_full(file: &File, dst: &mut [u8], file_off: usize) -> usize {
    let mut got = 0usize;
    while got < dst.len() {
        match file.read_at(&mut dst[got..], (file_off + got) as u64) {
            Ok(0) => break,
            Ok(n) => got += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("read_at failed: {e}"),
        }
    }
    got
}

/// Read chunk `[job_start.saturating_sub(1)..]` plus trailing bytes through the newline
/// closing the line crossing `job_end`. Returns `(got, local_start, local_end)`.
#[cfg(unix)]
#[inline]
pub fn pread_csv_chunk(
    file: &File,
    file_len: usize,
    job_start: usize,
    job_end: usize,
    buf: &mut Vec<u8>,
) -> (usize, usize, usize) {
    let job_end = job_end.min(file_len);
    if job_start >= job_end || job_start >= file_len {
        return (0, 0, 0);
    }
    let read_start = job_start.saturating_sub(1);
    let local_start = job_start - read_start;
    let local_end = job_end - read_start;
    let mut read_end = (job_end + 4096).min(file_len);
    let need = read_end - read_start;
    if buf.len() < need {
        buf.resize(need, 0);
    }
    let mut got = read_at_full(file, &mut buf[..need], read_start);
    if got < local_end {
        return (got, local_start.min(got), got);
    }
    while read_end < file_len && memchr::memchr(b'\n', &buf[local_end - 1..got]).is_none() {
        let extra_end = (read_end + 16_384).min(file_len);
        let new_need = extra_end - read_start;
        if buf.len() < new_need {
            buf.resize(new_need, 0);
        }
        let n = read_at_full(file, &mut buf[got..new_need], read_start + got);
        got += n;
        read_end = extra_end;
        if got < new_need || n == 0 {
            break;
        }
    }
    (got, local_start, local_end)
}
pub const SESSION_GAP_MS: i64 = 1_800_000;
pub const CLICK_REACH_MS: i64 = 7 * DAY_MS;
pub const LATE_LIMIT_MS: i64 = DAY_MS;

/// Unquote a single CSV field according to `rules.md`:
/// - No value ever holds a comma, quote, or newline.
/// - A field starting with `"` must also end with `"` (len >= 2) and contain no inner `"`.
/// - A field not starting with `"` must contain no `"`.
#[inline(always)]
pub fn unquote_field(field: &[u8]) -> Option<&[u8]> {
    if let Some(&first) = field.first() {
        if first == b'"' {
            if field.len() < 2 || field[field.len() - 1] != b'"' {
                return None;
            }
            let inner = &field[1..field.len() - 1];
            if memchr::memchr(b'"', inner).is_some() {
                return None;
            }
            Some(inner)
        } else {
            if memchr::memchr(b'"', field).is_some() {
                return None;
            }
            Some(field)
        }
    } else {
        Some(field)
    }
}

/// Split a CSV line (without trailing `\n`) into exact `N` unquoted fields.
/// Returns `None` if empty line, wrong field count, or malformed quotes.
#[inline(always)]
pub fn parse_csv_line<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    if line.is_empty() {
        return None;
    }
    let mut out = [&[][..]; N];
    let mut start = 0usize;
    if memchr::memchr(b'"', line).is_none() {
        for i in 0..(N - 1) {
            let rel = memchr::memchr(b',', &line[start..])?;
            let end = start + rel;
            out[i] = &line[start..end];
            start = end + 1;
        }
        let last = &line[start..];
        if memchr::memchr(b',', last).is_some() {
            return None;
        }
        out[N - 1] = last;
        return Some(out);
    }
    for i in 0..(N - 1) {
        let rel = memchr::memchr(b',', &line[start..])?;
        let end = start + rel;
        out[i] = unquote_field(&line[start..end])?;
        start = end + 1;
    }
    let last = &line[start..];
    if memchr::memchr(b',', last).is_some() {
        return None;
    }
    out[N - 1] = unquote_field(last)?;
    Some(out)
}

/// Iterate lines in an in-memory CSV byte slice (used for small files like products/campaigns).
#[inline]
pub fn for_each_data_line<'a, F>(data: &'a [u8], skip_header: bool, mut f: F)
where
    F: FnMut(&'a [u8]),
{
    let mut pos = 0usize;
    if skip_header {
        match memchr::memchr(b'\n', data) {
            Some(nl) => pos = nl + 1,
            None => return,
        }
    }
    while pos < data.len() {
        match memchr::memchr(b'\n', &data[pos..]) {
            Some(rel) => {
                f(&data[pos..pos + rel]);
                pos += rel + 1;
            }
            None => {
                f(&data[pos..]);
                break;
            }
        }
    }
}

#[inline(always)]
fn scalar_block_masks(blk: &[u8]) -> (u64, u64, u64) {
    let mut nl_m = 0u64;
    let mut cm_m = 0u64;
    let mut qt_m = 0u64;
    for (i, &b) in blk.iter().enumerate() {
        let bit = 1u64 << i;
        if b == b'\n' {
            nl_m |= bit;
        } else if b == b',' {
            cm_m |= bit;
        } else if b == b'"' {
            qt_m |= bit;
        }
    }
    (nl_m, cm_m, qt_m)
}

/// Zero-copy row sink for batched SIMD CSV scanning.
pub trait CsvRowSink<const N: usize> {
    /// Safety: `rpos` points to `N` valid delimiter offsets (`u16`) within `ptr`,
    /// `s0 <= *rpos`, and Micro-Pass 1 guaranteed all quotes in the row are validly paired.
    unsafe fn on_pos(&mut self, ptr: *const u8, s0: usize, rpos: *const u16);
    fn on_row(&mut self, row: &[&[u8]; N]);
}

#[inline(always)]
fn prefix_xor_scalar(mut x: u64) -> u64 {
    x ^= x << 1;
    x ^= x << 2;
    x ^= x << 4;
    x ^= x << 8;
    x ^= x << 16;
    x ^= x << 32;
    x
}

#[inline(always)]
unsafe fn scan_csv_slice_batched_sink<const N: usize, M, PX, S: CsvRowSink<N>>(
    slice: &[u8],
    sink: &mut S,
    mut load_blk64: M,
    mut prefix_xor: PX,
) where
    M: FnMut(*const u8) -> (u64, u64, u64),
    PX: FnMut(u64) -> u64,
{
    const {
        assert!(N >= 2);
    }
    let len = slice.len();
    let mut off = 0usize;
    let mut pos_buf = [0u16; 4104];

    while off < len {
        let rem_all = len - off;
        let blk = if rem_all <= 4096 {
            let Some(nl_rel) = memchr::memrchr(b'\n', &slice[off..]) else {
                break;
            };
            let b = &slice[off..=off + nl_rel];
            off += nl_rel + 1;
            b
        } else if let Some(nl_rel) = memchr::memrchr(b'\n', &slice[off..off + 4096]) {
            let b = &slice[off..=off + nl_rel];
            off += nl_rel + 1;
            b
        } else if let Some(nl_rel) = memchr::memchr(b'\n', &slice[off + 4096..]) {
            let line = &slice[off..off + 4096 + nl_rel];
            if let Some(row) = parse_csv_line::<N>(line) {
                sink.on_row(&row);
            }
            off += 4096 + nl_rel + 1;
            continue;
        } else {
            break;
        };

        let blen = blk.len();
        let ptr = blk.as_ptr();
        let mut pos = 0usize;
        let mut prev_ended_delim = 1u64;
        let mut prev_in_q = 0u64;
        let mut prev_close63 = 0u64;
        let mut bad = 0u64;
        let mut n_delims = 0usize;
        let mut exp_nl_idx = N - 1;

        while pos < blen {
            let rem = blen - pos;
            let step = rem.min(64);
            let (nl_m, cm_m, qt_m) = if rem >= 64 {
                load_blk64(ptr.add(pos))
            } else {
                scalar_block_masks(&blk[pos..])
            };

            let delim = nl_m | cm_m;
            let open = (delim << 1) | prev_ended_delim;
            let in_q = prefix_xor(qt_m) ^ prev_in_q;
            let opening = qt_m & in_q;
            let closing = qt_m & !in_q;
            bad |= (prev_close63 & !delim)
                | (opening & !open)
                | (closing & !(delim >> 1) & 0x7FFF_FFFF_FFFF_FFFF)
                | (in_q & delim);
            prev_close63 = closing >> 63;
            prev_ended_delim = delim >> 63;
            prev_in_q = 0u64.wrapping_sub(in_q >> 63);

            let old_n = n_delims;
            let mut nl = nl_m;
            while nl != 0 {
                let nl_bit = nl & nl.wrapping_neg();
                let before = (delim & (nl_bit - 1)).count_ones() as usize;
                bad |= ((old_n + before) ^ exp_nl_idx) as u64;
                exp_nl_idx += N;
                nl &= nl - 1;
            }

            let cnt = delim.count_ones() as usize;
            let mut dst = pos_buf.as_mut_ptr().add(old_n);
            let p16 = pos as u16;
            let mut d = delim;
            while d != 0 {
                *dst = p16.wrapping_add(d.trailing_zeros() as u16);
                d &= d - 1;
                *dst.add(1) = p16.wrapping_add(d.trailing_zeros() as u16);
                d &= d - 1;
                *dst.add(2) = p16.wrapping_add(d.trailing_zeros() as u16);
                d &= d - 1;
                *dst.add(3) = p16.wrapping_add(d.trailing_zeros() as u16);
                d &= d - 1;
                dst = dst.add(4);
            }
            n_delims = old_n + cnt;
            pos += step;
        }

        if (bad | prev_close63 | prev_in_q) != 0 || exp_nl_idx != n_delims + (N - 1) {
            for_each_data_line(blk, false, |line| {
                if let Some(row) = parse_csv_line::<N>(line) {
                    sink.on_row(&row);
                }
            });
            continue;
        }

        let mut k = 0usize;
        let mut s = 0usize;
        let pbase = pos_buf.as_ptr();
        while k < n_delims {
            let rp = pbase.add(k);
            sink.on_pos(ptr, s, rp);
            s = (*rp.add(N - 1) as usize) + 1;
            k += N;
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512bw,avx512vl,pclmulqdq")]
unsafe fn scan_csv_slice_avx512_sink<const N: usize, S: CsvRowSink<N>>(
    slice: &[u8],
    sink: &mut S,
) {
    use std::arch::x86_64::*;

    let v_nl = _mm512_set1_epi8(b'\n' as i8);
    let v_cm = _mm512_set1_epi8(b',' as i8);
    let v_qt = _mm512_set1_epi8(b'"' as i8);
    let all_ones = _mm_set1_epi8(-1);

    scan_csv_slice_batched_sink::<N, _, _, S>(
        slice,
        sink,
        |p| {
            let blk = _mm512_loadu_si512(p as *const _);
            (
                _mm512_cmpeq_epi8_mask(blk, v_nl),
                _mm512_cmpeq_epi8_mask(blk, v_cm),
                _mm512_cmpeq_epi8_mask(blk, v_qt),
            )
        },
        |q| _mm_cvtsi128_si64(_mm_clmulepi64_si128(_mm_cvtsi64_si128(q as i64), all_ones, 0)) as u64,
    );
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,pclmulqdq")]
unsafe fn scan_csv_slice_avx2_sink<const N: usize, S: CsvRowSink<N>>(
    slice: &[u8],
    sink: &mut S,
) {
    use std::arch::x86_64::*;

    let v_nl = _mm256_set1_epi8(b'\n' as i8);
    let v_cm = _mm256_set1_epi8(b',' as i8);
    let v_qt = _mm256_set1_epi8(b'"' as i8);
    let all_ones = _mm_set1_epi8(-1);

    scan_csv_slice_batched_sink::<N, _, _, S>(
        slice,
        sink,
        |p| {
            let lo = _mm256_loadu_si256(p as *const __m256i);
            let hi = _mm256_loadu_si256(p.add(32) as *const __m256i);

            let nl_lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, v_nl)) as u32 as u64;
            let nl_hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, v_nl)) as u32 as u64;
            let cm_lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, v_cm)) as u32 as u64;
            let cm_hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, v_cm)) as u32 as u64;
            let qt_lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, v_qt)) as u32 as u64;
            let qt_hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, v_qt)) as u32 as u64;

            (nl_lo | (nl_hi << 32), cm_lo | (cm_hi << 32), qt_lo | (qt_hi << 32))
        },
        |q| _mm_cvtsi128_si64(_mm_clmulepi64_si128(_mm_cvtsi64_si128(q as i64), all_ones, 0)) as u64,
    );
}

#[inline]
pub fn scan_csv_slice_sink<const N: usize, S: CsvRowSink<N>>(slice: &[u8], sink: &mut S) {
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx512f")
            && std::arch::is_x86_feature_detected!("avx512bw")
            && std::arch::is_x86_feature_detected!("avx512vl")
            && std::arch::is_x86_feature_detected!("avx512vbmi2")
            && std::arch::is_x86_feature_detected!("pclmulqdq")
        {
            unsafe { scan_csv_slice_avx512_sink::<N, S>(slice, sink) };
            return;
        }
        if std::arch::is_x86_feature_detected!("avx2")
            && std::arch::is_x86_feature_detected!("pclmulqdq")
        {
            unsafe { scan_csv_slice_avx2_sink::<N, S>(slice, sink) };
            return;
        }
    }
    unsafe {
        scan_csv_slice_batched_sink::<N, _, _, S>(
            slice,
            sink,
            |p| scalar_block_masks(std::slice::from_raw_parts(p, 64)),
            prefix_xor_scalar,
        );
    }
}

/// Scan every valid CSV data line owned by byte range `[start, end)` in a memory-mapped file slice,
/// dispatching valid rows directly through `CsvRowSink<N>` without intermediate slice arrays.
#[inline]
pub fn scan_csv_mmap_chunk_sink<const N: usize, S: CsvRowSink<N>>(
    data: &[u8],
    start: usize,
    end: usize,
    sink: &mut S,
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
    if let Some(rel) = memchr::memchr(b'\n', &data[end - 1..]) {
        let stop = (end - 1) + rel;
        scan_csv_slice_sink::<N, S>(&data[start_pos..=stop], sink);
    } else if let Some(last_nl) = memchr::memrchr(b'\n', &data[start_pos..]) {
        let nl_abs = start_pos + last_nl;
        scan_csv_slice_sink::<N, S>(&data[start_pos..=nl_abs], sink);
        let tail = &data[nl_abs + 1..];
        if !tail.is_empty() {
            if let Some(row) = parse_csv_line::<N>(tail) {
                sink.on_row(&row);
            }
        }
    } else {
        let tail = &data[start_pos..];
        if !tail.is_empty() {
            if let Some(row) = parse_csv_line::<N>(tail) {
                sink.on_row(&row);
            }
        }
    }
}

/// Scan every valid CSV data line owned by byte range `[start, end)` in a memory-mapped file slice
/// (lines starting at offset `L` with `start <= L < end`, skipping line 0 header).
#[inline]
pub fn scan_csv_mmap_chunk<const N: usize, F: FnMut(&[&[u8]; N])>(
    data: &[u8],
    start: usize,
    end: usize,
    mut f: F,
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
    if let Some(rel) = memchr::memchr(b'\n', &data[end - 1..]) {
        let stop = (end - 1) + rel;
        scan_csv_slice::<N, F>(&data[start_pos..=stop], &mut f);
    } else if let Some(last_nl) = memchr::memrchr(b'\n', &data[start_pos..]) {
        let nl_abs = start_pos + last_nl;
        scan_csv_slice::<N, F>(&data[start_pos..=nl_abs], &mut f);
        let tail = &data[nl_abs + 1..];
        if !tail.is_empty() {
            if let Some(row) = parse_csv_line::<N>(tail) {
                f(&row);
            }
        }
    } else {
        let tail = &data[start_pos..];
        if !tail.is_empty() {
            if let Some(row) = parse_csv_line::<N>(tail) {
                f(&row);
            }
        }
    }
}

/// Scan every `\n`-terminated CSV line in `slice` using AVX-512 / AVX2 (64B/iter) or scalar dispatch.
#[inline]
pub fn scan_csv_slice<const N: usize, F: FnMut(&[&[u8]; N])>(slice: &[u8], f: &mut F) {
    struct FnSink<'a, F>(&'a mut F);
    impl<const N: usize, F: FnMut(&[&[u8]; N])> CsvRowSink<N> for FnSink<'_, F> {
        #[inline(always)]
        unsafe fn on_pos(&mut self, ptr: *const u8, mut s: usize, rpos: *const u16) {
            let mut fields = [&[][..]; N];
            for i in 0..N {
                let e = *rpos.add(i) as usize;
                let q = usize::from(*ptr.add(s) == b'"');
                *fields.get_unchecked_mut(i) =
                    std::slice::from_raw_parts(ptr.add(s + q), (e - s) - (q << 1));
                s = e + 1;
            }
            (self.0)(&fields);
        }
        #[inline(always)]
        fn on_row(&mut self, row: &[&[u8]; N]) {
            (self.0)(row);
        }
    }
    let mut sink = FnSink(f);
    scan_csv_slice_sink::<N, _>(slice, &mut sink);
}

#[inline(always)]
fn d2(a: u8, b: u8) -> Option<i64> {
    let da = a.wrapping_sub(b'0');
    let db = b.wrapping_sub(b'0');
    if da <= 9 && db <= 9 {
        Some((da as i64) * 10 + (db as i64))
    } else {
        None
    }
}

#[inline(always)]
fn d4(b: &[u8]) -> Option<i64> {
    let d0 = b[0].wrapping_sub(b'0');
    let d1 = b[1].wrapping_sub(b'0');
    let d2v = b[2].wrapping_sub(b'0');
    let d3 = b[3].wrapping_sub(b'0');
    if d0 <= 9 && d1 <= 9 && d2v <= 9 && d3 <= 9 {
        Some((d0 as i64) * 1000 + (d1 as i64) * 100 + (d2v as i64) * 10 + (d3 as i64))
    } else {
        None
    }
}

#[inline(always)]
pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

#[inline(always)]
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

#[inline(always)]
fn max_day_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct DayCacheEntry {
    ym: u64,
    ddt: u32,
    rel_sec: i32,
}

#[inline(always)]
fn day_cache_slot(ym: u64, ddt: u32) -> usize {
    ((((ddt & 3) << 4) | ((ddt >> 8) & 15)) as usize) | ((((ym >> 48) & 1) << 6) as usize)
}

/// Compact L1-resident 128-entry day cache (`2 KB`) for zero-multiply ISO date resolution.
pub struct FastDateCache {
    pub win_start_ms: i64,
    pub win_start_sec: i64,
    pub win_start_whole_sec: bool,
    entries: [DayCacheEntry; 128],
}

impl FastDateCache {
    pub fn new(win_start_ms: i64, win_end_ms: i64) -> Self {
        let win_start_sec = win_start_ms.div_euclid(1_000);
        let win_start_whole_sec = win_start_ms.rem_euclid(1_000) == 0;
        let start_day = (win_start_ms - 8 * DAY_MS).div_euclid(DAY_MS);
        let end_day = (win_end_ms + 7 * DAY_MS).div_euclid(DAY_MS);

        let mut entries = [DayCacheEntry {
            ym: 0,
            ddt: 0,
            rel_sec: 0,
        }; 128];
        let mut filled = 0usize;
        let mut d = start_day;
        while d <= end_day && filled < 120 {
            let (y, m, dom) = civil_from_days(d);
            if (0..=9999).contains(&y) {
                let ym_str = format!("{y:04}-{m:02}-");
                let ym = u64::from_le_bytes(ym_str.as_bytes().try_into().unwrap());
                let ddt = u32::from_le_bytes([
                    b'0' + (dom / 10) as u8,
                    b'0' + (dom % 10) as u8,
                    b'T',
                    0,
                ]);
                let rel_sec_i64 = d * 86_400 - win_start_sec;
                if rel_sec_i64 >= (i32::MIN as i64 + 100_000)
                    && rel_sec_i64 <= (i32::MAX as i64 - 100_000)
                {
                    let slot = day_cache_slot(ym, ddt);
                    if entries[slot].ym == 0 {
                        entries[slot] = DayCacheEntry {
                            ym,
                            ddt,
                            rel_sec: rel_sec_i64 as i32,
                        };
                        filled += 1;
                    }
                }
            }
            d += 1;
        }

        Self {
            win_start_ms,
            win_start_sec,
            win_start_whole_sec,
            entries,
        }
    }
}

#[inline(always)]
fn parse_iso_datetime(b: &[u8]) -> Option<i64> {
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let year = d4(&b[0..4])?;
    let month = d2(b[5], b[6])?;
    if !(1..=12).contains(&month) {
        return None;
    }
    let day = d2(b[8], b[9])?;
    if day < 1 || day > max_day_in_month(year, month) {
        return None;
    }
    let hour = d2(b[11], b[12])?;
    let minute = d2(b[14], b[15])?;
    let second = d2(b[17], b[18])?;
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let days = days_from_civil(year, month, day);
    Some((days * 86_400 + hour * 3_600 + minute * 60 + second) * 1_000)
}

#[cold]
fn parse_iso_datetime_cold_rel_sec(b: &[u8], win_start_sec: i64) -> Option<i64> {
    parse_iso_datetime(b).map(|ms| ms / 1_000 - win_start_sec)
}

#[inline(always)]
pub fn parse_d2(a: u8, b: u8) -> Option<i64> {
    d2(a, b)
}

#[inline(always)]
pub fn parse_hms_swar(w_hms: u64) -> Option<i64> {
    let d = w_hms.wrapping_sub(0x3030_3A30_303A_3030);
    let bad1 = (d & 0x0000_FF00_00FF_0000)
        | ((d.wrapping_add(0x7676_0076_7600_7676) | d) & 0x8080_0080_8000_8080);
    let pairs = (d.wrapping_mul(0x0A01) >> 8) & 0x00FF_0000_FF00_00FF;
    let bad2 = pairs.wrapping_add(0x0044_0000_4400_0068) & 0x0080_0000_8000_0080;
    if (bad1 | bad2) != 0 {
        return None;
    }
    let lo = pairs & 0x0000_0000_FF00_00FF;
    Some((((lo.wrapping_mul((3_600u64 << 24) | 60u64) >> 24) & 0x1FFFF) + (pairs >> 48)) as i64)
}

/// Unchecked `HH:MM:SS` SWAR parser used strictly for `ingest_ts`, which `rules.md` guarantees is valid F1.
#[inline(always)]
fn parse_hms_unchecked(w_hms: u64) -> i64 {
    let d = w_hms.wrapping_sub(0x3030_3A30_303A_3030);
    let pairs = (d.wrapping_mul(0x0A01) >> 8) & 0x00FF_0000_FF00_00FF;
    let lo = pairs & 0x0000_0000_FF00_00FF;
    (((lo.wrapping_mul((3_600u64 << 24) | 60u64) >> 24) & 0x1FFFF) + (pairs >> 48)) as i64
}

#[inline(always)]
fn parse_iso_body_rel_sec(b: &[u8], cache: &FastDateCache) -> Option<i64> {
    let p = b.as_ptr();
    let w0 = unsafe { (p as *const u64).read_unaligned() }.to_le();
    let ddt = unsafe { (p.add(8) as *const u32).read_unaligned() }.to_le() & 0x00FF_FFFF;
    let w_hms = unsafe { (p.add(11) as *const u64).read_unaligned() }.to_le();

    let time_sec = parse_hms_swar(w_hms)?;
    let slot = day_cache_slot(w0, ddt);
    let entry = unsafe { *cache.entries.get_unchecked(slot) };
    if w0 == entry.ym && ddt == entry.ddt {
        return Some(entry.rel_sec as i64 + time_sec);
    }

    parse_iso_datetime_cold_rel_sec(b, cache.win_start_sec)
}

#[cfg(test)]
#[inline(always)]
pub fn parse_8_digits_swar(chunk: [u8; 8]) -> Option<u64> {
    let v = u64::from_le_bytes(chunk);
    let d = v.wrapping_sub(0x3030_3030_3030_3030);
    if ((d.wrapping_add(0x7676_7676_7676_7676) | d) & 0x8080_8080_8080_8080) != 0 {
        return None;
    }
    let mut x = d;
    x = (x * 10 + (x >> 8)) & 0x00FF_00FF_00FF_00FF;
    x = (x * 100 + (x >> 16)) & 0x0000_FFFF_0000_FFFF;
    x = (x * 10_000 + (x >> 32)) & 0x0000_0000_FFFF_FFFF;
    Some(x)
}

#[inline(always)]
pub fn resolve_iso_day_rel_sec(w0: u64, ddt: u32, cache: &FastDateCache) -> Option<i64> {
    let slot = day_cache_slot(w0, ddt);
    let entry = unsafe { *cache.entries.get_unchecked(slot) };
    if w0 == entry.ym && ddt == entry.ddt {
        return Some(entry.rel_sec as i64);
    }
    None
}

#[inline(always)]
pub fn parse_ingest_fast_sec(b: &[u8], cache: &FastDateCache) -> Option<i32> {
    if b.len() == 20 && b[19] == b'Z' && cache.win_start_whole_sec {
        let p = b.as_ptr();
        let w0 = unsafe { (p as *const u64).read_unaligned() }.to_le();
        let ddt = unsafe { (p.add(8) as *const u32).read_unaligned() }.to_le() & 0x00FF_FFFF;
        let w_hms = unsafe { (p.add(11) as *const u64).read_unaligned() }.to_le();
        let time_sec = parse_hms_unchecked(w_hms);
        if let Some(day_rel_sec) = resolve_iso_day_rel_sec(w0, ddt, cache) {
            return Some((day_rel_sec + time_sec) as i32);
        }
    }
    None
}

#[cold]
fn parse_ingest_cold_rel_sec(b: &[u8], cache: &FastDateCache) -> Option<(i64, bool)> {
    if b.len() == 20 && b[19] == b'Z' && cache.win_start_whole_sec {
        if let Some(s) = parse_iso_datetime_cold_rel_sec(b, cache.win_start_sec) {
            return Some((s, true));
        }
    }
    parse_ts(b).map(|ms| (ms, false))
}

#[inline(always)]
pub fn parse_ingest_rel_sec(b: &[u8], cache: &FastDateCache) -> Option<(i64, bool)> {
    if b.len() == 20 && b[19] == b'Z' && cache.win_start_whole_sec {
        let p = b.as_ptr();
        let w0 = unsafe { (p as *const u64).read_unaligned() }.to_le();
        let ddt = unsafe { (p.add(8) as *const u32).read_unaligned() }.to_le() & 0x00FF_FFFF;
        let w_hms = unsafe { (p.add(11) as *const u64).read_unaligned() }.to_le();
        let time_sec = parse_hms_swar(w_hms)?;
        if let Some(day_rel_sec) = resolve_iso_day_rel_sec(w0, ddt, cache) {
            return Some((day_rel_sec + time_sec, true));
        }
    }
    parse_ingest_cold_rel_sec(b, cache)
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "ssse3")))]
#[inline(always)]
fn swar8_from_digits(mut x: u64) -> u64 {
    x = (x * 10 + (x >> 8)) & 0x00FF_00FF_00FF_00FF;
    x = (x * 100 + (x >> 16)) & 0x0000_FFFF_0000_FFFF;
    (x * 10_000 + (x >> 32)) & 0x0000_0000_FFFF_FFFF
}

/// Zero-division F3 13-digit epoch-ms parser returning `(ts_ms - win_start_ms, (ts_ms - win_start_ms).div_euclid(1000))`.
#[inline(always)]
fn parse_f3_13_ms_and_sec(b: &[u8], cache: &FastDateCache) -> Option<(i64, i64)> {
    let p = b.as_ptr();
    let v0 = unsafe { (p as *const u64).read_unaligned() }.to_le();
    let v1 = unsafe { (p.add(5) as *const u64).read_unaligned() }.to_le();
    let d0 = v0.wrapping_sub(0x3030_3030_3030_3030);
    let d1 = v1.wrapping_sub(0x3030_3030_3030_3030);
    let bad = ((d0.wrapping_add(0x7676_7676_7676_7676) | d0)
        | (d1.wrapping_add(0x7676_7676_7676_7676) | d1))
        & 0x8080_8080_8080_8080;
    if bad != 0 {
        return None;
    }
    let sec_hi2 = (d0 & 0xFF) * 10 + ((d0 >> 8) & 0xFF);
    let sec_lo8_d = (d0 >> 16) | (((d1 >> 24) & 0xFFFF) << 48);
    #[cfg(all(target_arch = "x86_64", target_feature = "ssse3"))]
    let lo8_val = {
        use std::arch::x86_64::*;
        unsafe {
            let x = _mm_cvtsi64_si128(sec_lo8_d as i64);
            let p = _mm_maddubs_epi16(x, _mm_set1_epi16((1i16 << 8) | 10));
            let quads = _mm_madd_epi16(p, _mm_set1_epi32((1i32 << 16) | 100));
            let q = _mm_cvtsi128_si64(quads) as u64;
            (q & 0xFFFF_FFFF) * 10_000 + (q >> 32)
        }
    };
    #[cfg(not(all(target_arch = "x86_64", target_feature = "ssse3")))]
    let lo8_val = swar8_from_digits(sec_lo8_d);

    let ts_abs_sec = (sec_hi2 * 100_000_000 + lo8_val) as i64;
    let sub_ms = (((d1 >> 40) & 0xFF) * 100 + ((d1 >> 48) & 0xFF) * 10 + (d1 >> 56)) as i64;
    if cache.win_start_whole_sec {
        let rel_sec = ts_abs_sec - cache.win_start_sec;
        Some((rel_sec * 1_000 + sub_ms, rel_sec))
    } else {
        let rel_ms = ts_abs_sec * 1_000 + sub_ms - cache.win_start_ms;
        Some((rel_ms, rel_ms.div_euclid(1_000)))
    }
}

#[inline(always)]
pub unsafe fn parse_f3_13_ptr(p: *const u8, cache: &FastDateCache) -> Option<(i64, i64)> {
    parse_f3_13_ms_and_sec(std::slice::from_raw_parts(p, 13), cache)
}

/// Fast timestamp parser returning `(ts_ms - win_start_ms, (ts_ms - win_start_ms).div_euclid(1000))`
/// with zero division on whole-second F1/F2 forms and 13-digit F3 timestamps.
#[inline(always)]
pub fn parse_ts_rel_ms_and_sec(b: &[u8], cache: &FastDateCache) -> Option<(i64, i64)> {
    let len = b.len();
    if len == 20 && b[19] == b'Z' {
        if cache.win_start_whole_sec {
            let s = parse_iso_body_rel_sec(b, cache)?;
            return Some((s * 1_000, s));
        }
        let ms = parse_iso_datetime(b)? - cache.win_start_ms;
        return Some((ms, ms.div_euclid(1_000)));
    }
    if len == 13 {
        return parse_f3_13_ms_and_sec(b, cache);
    }
    if len == 25 && (b[19] == b'+' || b[19] == b'-') && b[22] == b':' {
        let oh = d2(b[20], b[21])?;
        let om = d2(b[23], b[24])?;
        if oh > 23 || om > 59 {
            return None;
        }
        let sign = if b[19] == b'+' { -1i64 } else { 1i64 };
        let off_sec = sign * (oh * 3_600 + om * 60);
        if cache.win_start_whole_sec {
            let s = parse_iso_body_rel_sec(b, cache)? + off_sec;
            return Some((s * 1_000, s));
        }
        let ms = parse_iso_datetime(b)? + off_sec * 1_000 - cache.win_start_ms;
        return Some((ms, ms.div_euclid(1_000)));
    }
    parse_ts_f3_general(b).map(|ms| {
        let rel = ms - cache.win_start_ms;
        (rel, rel.div_euclid(1_000))
    })
}

/// Fast timestamp parser returning `ts_ms - win_start_ms` (e.g. clicks).
#[inline(always)]
pub fn parse_ts_rel_ms(b: &[u8], cache: &FastDateCache) -> Option<i64> {
    let len = b.len();
    if len == 20 && b[19] == b'Z' {
        if cache.win_start_whole_sec {
            return parse_iso_body_rel_sec(b, cache).map(|s| s * 1_000);
        }
        return parse_iso_datetime(b).map(|ms| ms - cache.win_start_ms);
    }
    if len == 13 {
        return parse_f3_13_ms_and_sec(b, cache).map(|(ms, _)| ms);
    }
    if len == 25 && (b[19] == b'+' || b[19] == b'-') && b[22] == b':' {
        let oh = d2(b[20], b[21])?;
        let om = d2(b[23], b[24])?;
        if oh > 23 || om > 59 {
            return None;
        }
        let sign = if b[19] == b'+' { -1i64 } else { 1i64 };
        let off_ms = sign * (oh * 3_600 + om * 60) * 1_000;
        if cache.win_start_whole_sec {
            let utc_sec = parse_iso_body_rel_sec(b, cache)?;
            return Some(utc_sec * 1_000 + off_ms);
        }
        let utc_ms = parse_iso_datetime(b)?;
        return Some(utc_ms + off_ms - cache.win_start_ms);
    }
    parse_ts_f3_general(b).map(|ms| ms - cache.win_start_ms)
}

#[cold]
fn parse_ts_f3_general(b: &[u8]) -> Option<i64> {
    let len = b.len();
    if len == 0 {
        return None;
    }
    for &c in b {
        if !c.is_ascii_digit() {
            return None;
        }
    }
    let mut start = 0usize;
    while start < len && b[start] == b'0' {
        start += 1;
    }
    let sig = &b[start..];
    if sig.is_empty() {
        return Some(0);
    }
    if sig.len() > 18 {
        return None;
    }
    let mut val = 0i64;
    for &c in sig {
        val = val * 10 + (c - b'0') as i64;
    }
    Some(val)
}

/// Standalone timestamp parser (used for `manifest.json` and unit checks).
#[inline]
pub fn parse_ts(b: &[u8]) -> Option<i64> {
    let len = b.len();
    if len == 20 && b[19] == b'Z' {
        return parse_iso_datetime(b);
    }
    if len == 25 && (b[19] == b'+' || b[19] == b'-') && b[22] == b':' {
        let utc_base = parse_iso_datetime(b)?;
        let oh = d2(b[20], b[21])?;
        let om = d2(b[23], b[24])?;
        if oh > 23 || om > 59 {
            return None;
        }
        let offset_ms = (oh * 60 + om) * 60_000;
        return if b[19] == b'+' {
            Some(utc_base - offset_ms)
        } else {
            Some(utc_base + offset_ms)
        };
    }
    parse_ts_f3_general(b)
}

/// Single-pass ASCII purchase price parser (`1..=10_000_000`).
/// Supports arbitrary leading zeros while rejecting any non-ASCII-digit or out-of-range value.
#[inline(always)]
pub fn parse_price_cents(b: &[u8]) -> Option<u32> {
    if b.is_empty() {
        return None;
    }
    let mut val: u32 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        val = val * 10 + d as u32;
        if val > 10_000_000 {
            return None;
        }
    }
    if val >= 1 {
        Some(val)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn swar_digit_verification_exhaustive_single_byte() {
        let mut buf8 = *b"00000000";
        for b in 0u8..=255 {
            for pos in 0..8 {
                buf8[pos] = b;
                let got = parse_8_digits_swar(buf8);
                if b.is_ascii_digit() {
                    let expected = (b - b'0') as u64 * 10u64.pow((7 - pos) as u32);
                    assert_eq!(got, Some(expected));
                } else {
                    assert_eq!(got, None, "byte {b:#x} at pos {pos}");
                }
                buf8[pos] = b'0';
            }
        }
    }

    #[test]
    fn fast_date_cache_and_timestamps() {
        let ws = parse_ts(b"2028-02-20T00:00:00Z").unwrap();
        let we = parse_ts(b"2028-03-05T00:00:00Z").unwrap();
        let cache = FastDateCache::new(ws, we);
        // Valid leap day in 2028, invalid in 2027/2029, Feb 30, offsets, F3 leading zeros, 1900 fallback
        let cases = [
            b"2028-02-29T12:34:56Z".as_slice(),
            b"2027-02-29T12:34:56Z",
            b"2028-02-30T00:00:00Z",
            b"2028-02-28T23:59:59+23:59",
            b"2028-02-28T23:59:59-00:00",
            b"2028-02-28T23:59:59+24:00",
            b"2028-02-28T23:59:59+05:60",
            b"1900-01-01T00:00:00Z",
            b"1835481600000",
            b"0001835481600000",
            b"86400000",
            b"123456789012345678901",
            b"2028-02-28t00:00:00Z",
            b"2028-02-28T00:00:00z",
            b"2028-99-01T00:00:00Z",
            b"2028-00-01T00:00:00Z",
            b"2028-1a-01T00:00:00Z",
        ];
        for y in 2020..=2035 {
            let s = format!("{y:04}-12-31T23:59:59Z");
            assert_eq!(
                parse_ts_rel_ms(s.as_bytes(), &cache),
                parse_ts(s.as_bytes()).map(|ms| ms - ws)
            );
        }
        for c in cases {
            assert_eq!(
                parse_ts_rel_ms(c, &cache),
                parse_ts(c).map(|ms| ms - ws),
                "mismatch on {:?}",
                String::from_utf8_lossy(c)
            );
        }

        // Non-whole-second window_start must stay exact to the millisecond
        let ws_ms = ws + 500;
        let we_ms = we + 500;
        let cache_ms = FastDateCache::new(ws_ms, we_ms);
        assert!(!cache_ms.win_start_whole_sec);
        assert_eq!(
            parse_ingest_rel_sec(b"2028-02-21T00:00:00Z", &cache_ms),
            Some((parse_ts(b"2028-02-21T00:00:00Z").unwrap(), false))
        );
        for c in cases {
            assert_eq!(
                parse_ts_rel_ms(c, &cache_ms),
                parse_ts(c).map(|ms| ms - ws_ms),
                "ms mismatch on {:?}",
                String::from_utf8_lossy(c)
            );
        }
    }

    #[test]
    fn simd_and_chunked_reader_match_reference() {
        let mut content = String::from("a,b,c\n");
        let lines = vec![
            r#"u1,"u2",p1"#,
            "",
            r#""","","""#,
            "u1,u2,p1\r",
            r#"u1,"bad"x,p1"#,
            r#""unclosed,u2,p1"#,
            "only_two,fields",
            "too,many,fields,here",
            "long_user_identifier_0123456789abcdef0123456789abcdef,second_very_long_field_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx,p99",
            r#"u9,"u10","end_no_nl""#,
        ];
        for (i, l) in lines.iter().enumerate() {
            content.push_str(l);
            if i + 1 < lines.len() {
                content.push('\n');
            }
        }
        let bytes = content.as_bytes();
        let mut expected: Vec<[Vec<u8>; 3]> = Vec::new();
        for_each_data_line(bytes, true, |l| {
            if let Some([a, b, c]) = parse_csv_line::<3>(l) {
                expected.push([a.to_vec(), b.to_vec(), c.to_vec()]);
            }
        });

        for chunk_sz in [1usize, 2, 3, 5, 17, 63, 64, 65, 256, 4096] {
            let mut got_mmap: Vec<[Vec<u8>; 3]> = Vec::new();
            let mut s2 = 0usize;
            while s2 < bytes.len() {
                let e2 = (s2 + chunk_sz).min(bytes.len());
                scan_csv_mmap_chunk::<3, _>(bytes, s2, e2, |&[a, b, c]| {
                    got_mmap.push([a.to_vec(), b.to_vec(), c.to_vec()]);
                });
                s2 = e2;
            }
            assert_eq!(got_mmap, expected, "scan_csv_mmap_chunk chunk_sz={chunk_sz}");
        }
    }

    #[test]
    fn swar_hms_exhaustive() {
        for hh in 0u64..=99 {
            for mm in 0u64..=99 {
                for ss in 0u64..=99 {
                    let s = format!("{hh:02}:{mm:02}:{ss:02}");
                    let w_hms = u64::from_le_bytes(s.as_bytes().try_into().unwrap());
                    let got = parse_hms_swar(w_hms);
                    let expected = if hh <= 23 && mm <= 59 && ss <= 59 {
                        Some((hh * 3_600 + mm * 60 + ss) as i64)
                    } else {
                        None
                    };
                    assert_eq!(got, expected, "hms={s}");
                }
            }
        }
        // Check wrong separators and bad byte at every position
        for pos in 0..8usize {
            for b in [b'/', b';', b' ', b'T', b'0' - 1, b':', b'a', 0u8, 0xFF] {
                let mut raw = *b"12:34:56";
                raw[pos] = b;
                let expected = if (pos == 2 || pos == 5) && b == b':' {
                    Some(12 * 3_600 + 34 * 60 + 56)
                } else if pos != 2 && pos != 5 && b.is_ascii_digit() {
                    let d = (b - b'0') as i64;
                    let mut arr = [1, 2, 0, 3, 4, 0, 5, 6];
                    arr[pos] = d;
                    let hh = arr[0] * 10 + arr[1];
                    let mm = arr[3] * 10 + arr[4];
                    let ss = arr[6] * 10 + arr[7];
                    if hh <= 23 && mm <= 59 && ss <= 59 {
                        Some(hh * 3_600 + mm * 60 + ss)
                    } else {
                        None
                    }
                } else {
                    None
                };
                assert_eq!(
                    parse_hms_swar(u64::from_le_bytes(raw)),
                    expected,
                    "pos={pos} b={b:#x}"
                );
            }
        }
    }

    #[test]
    fn simd_scanner_lengths_and_quote_edges() {
        let mut content = String::from("h1,h2,h3\n");
        let variants = [
            "\"\"",
            "\"",
            "\"a",
            "a\"",
            "\"a\"b\"",
            "\"\"\"",
            "\"\"a",
            "x\r",
            "\"ok\"",
            "",
            "abc",
        ];
        // Sweep line lengths from 60..=135 (explicitly exercising 63, 64, 65, 126, 127, 128, 129)
        for target_len in 60..=135usize {
            let core_a = "u".repeat((target_len - 10) / 2);
            let core_b = "v".repeat(target_len - 10 - (target_len - 10) / 2);
            content.push_str(&format!("{core_a},\"{core_b}\",p1234\n"));
            content.push_str(&format!("\"{core_a}\",{core_b},p5678\n"));
        }
        // Sweep quote variants in every column with padding before/after crossing 64B & 128B boundaries
        for pad in [0usize, 1, 15, 29, 30, 31, 61, 62, 63] {
            let pfx = "p".repeat(pad);
            for v in variants {
                content.push_str(&format!("{v},{pfx},end\n"));
                content.push_str(&format!("{pfx},{v},end\n"));
                content.push_str(&format!("{pfx},mid,{v}\n"));
            }
        }
        content.push_str("\"tail_a\",tail_b,\"\"");

        let bytes = content.as_bytes();
        let mut expected: Vec<[Vec<u8>; 3]> = Vec::new();
        for_each_data_line(bytes, true, |l| {
            if let Some([a, b, c]) = parse_csv_line::<3>(l) {
                expected.push([a.to_vec(), b.to_vec(), c.to_vec()]);
            }
        });
        assert!(!expected.is_empty());

        // Direct slice scan (newline-terminated prefix)
        let last_nl = memchr::memrchr(b'\n', bytes).unwrap();
        let hdr_nl = memchr::memchr(b'\n', bytes).unwrap();
        let mut expected_nl: Vec<[Vec<u8>; 3]> = Vec::new();
        for_each_data_line(&bytes[..=last_nl], true, |l| {
            if let Some([a, b, c]) = parse_csv_line::<3>(l) {
                expected_nl.push([a.to_vec(), b.to_vec(), c.to_vec()]);
            }
        });
        let mut got_slice: Vec<[Vec<u8>; 3]> = Vec::new();
        scan_csv_slice::<3, _>(&bytes[hdr_nl + 1..=last_nl], &mut |&[a, b, c]| {
            got_slice.push([a.to_vec(), b.to_vec(), c.to_vec()]);
        });
        assert_eq!(got_slice, expected_nl, "direct scan_csv_slice");

        for chunk_sz in [1usize, 2, 3, 5, 17, 63, 64, 65, 256, 4096] {
            let mut got_mmap: Vec<[Vec<u8>; 3]> = Vec::new();
            let mut s2 = 0usize;
            while s2 < bytes.len() {
                let e2 = (s2 + chunk_sz).min(bytes.len());
                scan_csv_mmap_chunk::<3, _>(bytes, s2, e2, |&[a, b, c]| {
                    got_mmap.push([a.to_vec(), b.to_vec(), c.to_vec()]);
                });
                s2 = e2;
            }
            assert_eq!(got_mmap, expected, "mmap edge chunk_sz={chunk_sz}");
        }
    }

    #[test]
    fn parse_f3_13_exhaustive_single_byte() {
        let ws = parse_ts(b"2028-02-20T00:00:00Z").unwrap();
        let we = parse_ts(b"2028-03-05T00:00:00Z").unwrap();
        let cache = FastDateCache::new(ws, we);
        let base = *b"1835481600123";
        for pos in 0..13usize {
            let mut buf = base;
            for b in 0u8..=255 {
                buf[pos] = b;
                assert_eq!(
                    parse_ts_rel_ms(&buf, &cache),
                    parse_ts(&buf).map(|ms| ms - ws),
                    "pos={pos} b={b:#x}"
                );
            }
        }
        assert_eq!(
            parse_ts_rel_ms(b"9999999999999", &cache),
            parse_ts(b"9999999999999").map(|ms| ms - ws)
        );
        assert_eq!(
            parse_ts_rel_ms(b"0000000000000", &cache),
            parse_ts(b"0000000000000").map(|ms| ms - ws)
        );
    }

    #[test]
    fn batched_scanner_multi_4k_blocks_and_long_lines() {
        let mut content = String::from("a,b,c\n");
        let long_field = "L".repeat(4500);
        for i in 0..160usize {
            match i % 9 {
                0 => content.push_str(&format!("\"u_{i}\",mid,\"p_{i}\"\n")),
                1 => content.push_str(&format!("u_{i},\"bad\"q,p_{i}\n")),
                2 => content.push_str(&format!("\"\",\n")),
                3 => content.push_str(&format!("a,b,c,d\n")),
                4 => content.push_str(&format!("\",u_{i},p_{i}\n")),
                5 => content.push_str("\n"),
                6 => content.push_str(&format!("\"u_{i}\",\"v_{i}\",\"\"\n")),
                7 if i == 70 => content.push_str(&format!("{long_field},\"ok\",end\n")),
                8 if i == 80 => content.push_str(&format!("{long_field},\"bad\"q,end\n")),
                _ => content.push_str(&format!("u_{i},v_{i},p_{i}\n")),
            }
        }
        content.push_str("\"end_a\",end_b,\"end_c\"\n");
        let bytes = content.as_bytes();
        let mut expected: Vec<[Vec<u8>; 3]> = Vec::new();
        for_each_data_line(bytes, true, |l| {
            if let Some([a, b, c]) = parse_csv_line::<3>(l) {
                expected.push([a.to_vec(), b.to_vec(), c.to_vec()]);
            }
        });

        let mut got_mmap = Vec::new();
        scan_csv_mmap_chunk::<3, _>(bytes, 0, bytes.len(), |&[a, b, c]| {
            got_mmap.push([a.to_vec(), b.to_vec(), c.to_vec()]);
        });
        assert_eq!(got_mmap, expected, "multi-4k mmap");
    }

    #[test]
    fn ts_rel_ms_and_sec_exact_across_forms() {
        let ws = parse_ts(b"2028-02-20T00:00:00Z").unwrap();
        let we = parse_ts(b"2028-03-05T00:00:00Z").unwrap();
        let cache = FastDateCache::new(ws, we);
        let cache_ms = FastDateCache::new(ws + 500, we + 500);

        // Build test timestamps across F1, F2, F3 (including sub-second .000/.001/.500/.999 and negative rel)
        let f3_samples: Vec<String> = [-86_400_000i64, -5_999, -5_500, -5_001, -5_000, -1, 0, 1, 500, 999, 1_000, 1_001, 1_500, 1_999, 86_400_500]
            .into_iter()
            .map(|off| format!("{:013}", ws + off))
            .collect();

        let mut all_samples: Vec<&[u8]> = vec![
            b"2028-02-19T23:59:59Z",
            b"2028-02-20T00:00:00Z",
            b"2028-02-29T12:34:56Z",
            b"2028-02-28T23:59:59+23:59",
            b"2028-02-20T05:30:00+05:30",
            b"2028-02-19T19:00:00-05:00",
            b"2028-02-28T23:59:59-00:00",
            b"1900-01-01T00:00:00Z",
            b"86400000",
            b"0001835481600123",
            b"2028-02-29t00:00:00Z",
            b"183548160012a",
            b"2028-02-20T00:00:00+24:00",
        ];
        for s in &f3_samples {
            all_samples.push(s.as_bytes());
        }

        for &(c, c_ws) in &[(&cache, ws), (&cache_ms, ws + 500)] {
            for &s in &all_samples {
                let expected = parse_ts(s).map(|ms| {
                    let rel = ms - c_ws;
                    (rel, rel.div_euclid(1_000))
                });
                assert_eq!(
                    parse_ts_rel_ms_and_sec(s, c),
                    expected,
                    "mismatch for {:?} (win_start={c_ws})",
                    String::from_utf8_lossy(s)
                );
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn pread_csv_chunk_matches_mmap_across_boundaries() {
        use std::io::Write;
        let dir = std::env::temp_dir().join("shopping_pread_chunk_test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.csv");

        let long_field = "X".repeat(5000);
        let mut content = String::from("a,b,c\n");
        for i in 0..120usize {
            match i % 6 {
                0 => content.push_str(&format!("\"u_{i}\",mid,\"p_{i}\"\n")),
                1 => content.push_str(&format!("u_{i},\"bad\"q,p_{i}\n")),
                2 => content.push_str("\n"),
                3 if i == 63 => content.push_str(&format!("{long_field},\"ok\",end_{i}\n")),
                4 => content.push_str("a,b,c,d\n"),
                _ => content.push_str(&format!("u_{i},v_{i},p_{i}\n")),
            }
        }
        content.push_str("last_u,last_v,last_p");
        {
            let mut w = File::create(&path).unwrap();
            w.write_all(content.as_bytes()).unwrap();
        }

        let bytes = content.as_bytes();
        let mut expected: Vec<[Vec<u8>; 3]> = Vec::new();
        for_each_data_line(bytes, true, |l| {
            if let Some([a, b, c]) = parse_csv_line::<3>(l) {
                expected.push([a.to_vec(), b.to_vec(), c.to_vec()]);
            }
        });

        let f = File::open(&path).unwrap();
        fadvise_sequential(&f);
        let flen = f.metadata().unwrap().len() as usize;
        let mut io_buf = Vec::new();

        for &chunk_sz in &[17usize, 64, 256, 4096, flen] {
            let mut got = Vec::new();
            let mut pos = 0usize;
            while pos < flen {
                let end = (pos + chunk_sz).min(flen);
                let (n, ls, le) = pread_csv_chunk(&f, flen, pos, end, &mut io_buf);
                if n > 0 {
                    scan_csv_mmap_chunk::<3, _>(&io_buf[..n], ls, le, |&[a, b, c]| {
                        got.push([a.to_vec(), b.to_vec(), c.to_vec()]);
                    });
                }
                pos = end;
            }
            assert_eq!(got, expected, "pread mismatch for chunk_sz={chunk_sz}");
        }

        let _ = std::fs::remove_dir_all(&dir);
    }
}
