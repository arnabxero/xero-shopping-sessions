//! Fast path for the common shape of event and click lines.
//!
//! One 128-byte window gives the line's newline and the commas before it, so
//! every field's span is known at once; each field is then checked and
//! decoded on its own with 16-byte vector loads: content of a narrow grammar
//! (an ID of 1 to 10 bytes of `[0-9A-Za-z_]`, a timestamp in form F1, F2 or
//! F3, an event type, a run of digits), optionally wrapped in a quote pair.
//! None of these grammars admits `"`, so a line accepted here has quotes only
//! around whole fields and exactly the field values the strict reader
//! (`csv::split`) gives. Everything else (rare forms, bad values, malformed
//! lines) returns None and the caller reads that line the general way, so the
//! fast path never decides anything the general path would decide
//! differently.

use crate::ts;

/// Bytes that must be readable from the start of a line for the fast path
/// (the 128-byte window, and 16-byte loads at field starts inside it).
pub const SPAN: usize = 160;

/// The years whose dates the fast path decodes (at most two: the window plus
/// a few days on each side never spans more), as their four ASCII bytes and the
/// index of their January in the month table.
pub struct Years {
    ystr: [u32; 2],
    yidx: [usize; 2],
}

impl Years {
    /// The years of the instants `lo_ms` and `hi_ms` (milliseconds since the epoch).
    pub fn new(lo_ms: i64, hi_ms: i64) -> Years {
        let mut out = Years { ystr: [0; 2], yidx: [0; 2] };
        let mut n = 0;
        for ms in [lo_ms, hi_ms] {
            if let Some((year, idx)) = ts::year_of(ms) {
                let s = u32::from_le_bytes(format!("{year:04}").as_bytes().try_into().unwrap());
                if n < 2 && !out.ystr[..n].contains(&s) {
                    out.ystr[n] = s;
                    out.yidx[n] = idx;
                    n += 1;
                }
            }
        }
        out
    }
}

/// An event line's values.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Event {
    pub eid: u64,
    pub user: u64,
    pub t: i64,
    pub ingest: i64,
    pub purchase: bool,
    pub product: u64,
    /// The price field's digits (0 when empty; meaningful up to 16 digits).
    pub price: u64,
}

/// A click line's values.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Click {
    pub cid: u64,
    pub user: u64,
    pub campaign: u64,
    pub t: i64,
}

#[cfg(target_feature = "avx2")]
mod simd {
    use super::Years;
    use crate::ts;
    use std::arch::x86_64::*;

    /// Codes of the ID bytes (see keys.rs) and a bitmask of the lanes that are
    /// not ID bytes.
    #[inline(always)]
    unsafe fn classify(v: __m128i) -> (__m128i, u32) {
        let nib = _mm_set1_epi8(0x0F);
        let hi = _mm_and_si128(_mm_srli_epi16(v, 4), nib);
        let lo = _mm_and_si128(v, nib);
        // Class bits: 1 digit, 2 [A-Oa-o] (high nibble 4/6, low 1..F),
        // 4 [P-Zp-z] (high nibble 5/7, low 0..A), 8 '_'.
        let lut_hi = _mm_setr_epi8(0, 0, 0, 1, 2, 4 | 8, 2, 4, 0, 0, 0, 0, 0, 0, 0, 0);
        let lut_lo = _mm_setr_epi8(1 | 4, 7, 7, 7, 7, 7, 7, 7, 7, 7, 2 | 4, 2, 2, 2, 2, 2 | 8);
        let cls = _mm_and_si128(_mm_shuffle_epi8(lut_hi, hi), _mm_shuffle_epi8(lut_lo, lo));
        let bad = _mm_movemask_epi8(_mm_cmpeq_epi8(cls, _mm_setzero_si128())) as u32;
        // '0'..'9' -> 1..10, 'A'..'Z' -> 11..36, '_' -> 37, 'a'..'z' -> 38..63
        let off = _mm_setr_epi8(0, 0, 0, 0x2F, 0x36, 0x36, 0x3B, 0x3B, 0, 0, 0, 0, 0, 0, 0, 0);
        let us = _mm_and_si128(_mm_cmpeq_epi8(cls, _mm_set1_epi8(8)), _mm_set1_epi8(4));
        let codes = _mm_sub_epi8(_mm_sub_epi8(v, _mm_shuffle_epi8(off, hi)), us);
        (codes, bad)
    }

    /// Newline and comma bitmasks of the 128 bytes at `p`.
    #[inline(always)]
    unsafe fn masks(p: *const u8) -> (u128, u128) {
        let v0 = _mm256_loadu_si256(p as *const __m256i);
        let v1 = _mm256_loadu_si256(p.add(32) as *const __m256i);
        let v2 = _mm256_loadu_si256(p.add(64) as *const __m256i);
        let v3 = _mm256_loadu_si256(p.add(96) as *const __m256i);
        let m = |c: u8| -> u128 {
            let s = _mm256_set1_epi8(c as i8);
            let a = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v0, s)) as u32 as u64;
            let b = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v1, s)) as u32 as u64;
            let c2 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v2, s)) as u32 as u64;
            let d = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v3, s)) as u32 as u64;
            ((a | (b << 32)) as u128) | (((c2 | (d << 32)) as u128) << 64)
        };
        (m(b'\n'), m(b','))
    }

    /// The line at `p` split at its first newline and at the commas before
    /// it: the end of each of the `N` fields (the last one ends at the
    /// newline), or None unless there are exactly N - 1 commas.
    #[inline(always)]
    pub unsafe fn split<const N: usize>(p: *const u8) -> Option<[usize; N]> {
        let (nl, cm) = masks(p);
        if nl == 0 {
            return None;
        }
        let ll = nl.trailing_zeros() as usize;
        let mut c = cm & ((1u128 << ll) - 1);
        if c.count_ones() as usize != N - 1 {
            return None;
        }
        let mut ends = [ll; N];
        for e in ends.iter_mut().take(N - 1) {
            *e = c.trailing_zeros() as usize;
            c &= c - 1;
        }
        Some(ends)
    }

    /// The content of field [s, e) of the line at `p`: without its quotes if
    /// it is wrapped in a pair (as csv::split). Any other quote stays in the
    /// content, where no grammar below accepts it.
    #[inline(always)]
    unsafe fn content(p: *const u8, s: usize, e: usize) -> (*const u8, usize) {
        let n = e - s;
        let q = ((n >= 2) & (*p.add(s) == b'"') & (*p.add(e - 1) == b'"')) as usize;
        (p.add(s + q), n - 2 * q)
    }

    /// Lanes 0..n of a 16-lane mask.
    #[inline(always)]
    fn low(n: usize) -> u32 {
        (1u32 << n) - 1
    }

    /// An ID field (1 to 10 ID bytes): its packed key (as keys::pack).
    #[inline(always)]
    pub unsafe fn key_at(p: *const u8, s: usize, e: usize) -> Option<u64> {
        let (c, n) = content(p, s, e);
        let v = _mm_loadu_si128(c as *const __m128i);
        let (codes, bad) = classify(v);
        if n.wrapping_sub(1) >= 10 || bad & low(n) != 0 {
            return None;
        }
        let idx = _mm_setr_epi8(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
        let codes = _mm_and_si128(codes, _mm_cmpgt_epi8(_mm_set1_epi8(n as i8), idx));
        // c[2i] * 64 + c[2i+1], then pairs * 4096 + next pair: 4 codes per 32-bit lane.
        let m16 = _mm_maddubs_epi16(codes, _mm_set1_epi16(0x0140));
        let m32 = _mm_madd_epi16(m16, _mm_set1_epi32(0x0001_1000));
        let l0 = _mm_cvtsi128_si32(m32) as u32 as u64;
        let l1 = _mm_extract_epi32(m32, 1) as u32 as u64;
        let l2 = _mm_extract_epi32(m32, 2) as u32 as u64;
        Some((l0 << 36) | (l1 << 12) | (l2 >> 12))
    }

    const fn shift_table() -> [[i8; 16]; 17] {
        let mut t = [[-1i8; 16]; 17];
        let mut n = 0;
        while n <= 16 {
            let mut i = 16 - n;
            while i < 16 {
                t[n][i] = (i - (16 - n)) as i8;
                i += 1;
            }
            n += 1;
        }
        t
    }
    /// Shuffles moving the first n bytes to the end of the vector (zeros before).
    static SHIFT: [[i8; 16]; 17] = shift_table();

    /// The value of the `n` (at most 16) bytes at `c` if they are all ASCII digits.
    #[inline(always)]
    unsafe fn digits(c: *const u8, n: usize) -> Option<u64> {
        let v = _mm_loadu_si128(c as *const __m128i);
        let d = _mm_sub_epi8(v, _mm_set1_epi8(b'0' as i8));
        let isd = _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_min_epu8(d, _mm_set1_epi8(9)), d)) as u32;
        if n > 16 || !isd & low(n) != 0 {
            return None;
        }
        let x = _mm_shuffle_epi8(d, _mm_loadu_si128(SHIFT.as_ptr().add(n) as *const __m128i));
        let m1 = _mm_maddubs_epi16(x, _mm_set1_epi16(0x010A));
        let m2 = _mm_madd_epi16(m1, _mm_set1_epi32(0x0001_0064));
        let m3 = _mm_packus_epi32(m2, m2);
        let m4 = _mm_madd_epi16(m3, _mm_set1_epi32(0x0001_2710));
        let a = _mm_cvtsi128_si32(m4) as u32 as u64;
        let b = _mm_extract_epi32(m4, 1) as u32 as u64;
        Some(a * 100_000_000 + b)
    }

    /// `YYYY-MM-DDTHH:MM:SS` at `c` (followed by `Z` or an offset, checked by
    /// the caller), in a year of `y`: milliseconds.
    #[inline(always)]
    unsafe fn date_time(c: *const u8, y: &Years) -> Option<i64> {
        let ys = (c as *const u32).read_unaligned();
        let yidx = if ys == y.ystr[0] {
            y.yidx[0]
        } else if ys == y.ystr[1] {
            y.yidx[1]
        } else {
            return None;
        };
        // "-MM-DDTHH:MM:SS" and the byte after it.
        let v = _mm_loadu_si128(c.add(4) as *const __m128i);
        let tmpl = _mm_setr_epi8(b'-' as i8, 0, 0, b'-' as i8, 0, 0, b'T' as i8, 0, 0, b':' as i8, 0, 0, b':' as i8, 0, 0, 0);
        let sep = _mm_movemask_epi8(_mm_cmpeq_epi8(v, tmpl)) as u32;
        let d = _mm_sub_epi8(v, _mm_set1_epi8(b'0' as i8));
        let dig = _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_min_epu8(d, _mm_set1_epi8(9)), d)) as u32;
        let pairs = _mm_shuffle_epi8(d, _mm_setr_epi8(1, 2, 4, 5, 7, 8, 10, 11, 13, 14, -1, -1, -1, -1, -1, -1));
        // month, day, hour, minute, second
        let vals = _mm_maddubs_epi16(pairs, _mm_set1_epi16(0x010A));
        let out = _mm_or_si128(
            _mm_cmpgt_epi16(vals, _mm_setr_epi16(12, 31, 23, 59, 59, 0, 0, 0)),
            _mm_cmpgt_epi16(_mm_setr_epi16(1, 1, 0, 0, 0, 0, 0, 0), vals),
        );
        if (sep & 0x1249) != 0x1249 || (dig & 0x6DB6) != 0x6DB6 || _mm_movemask_epi8(out) != 0 {
            return None;
        }
        let x = _mm_cvtsi128_si64(vals) as u64;
        let month = (x & 0xFFFF) as usize;
        let day = ((x >> 16) & 0xFFFF) as i64;
        let hour = ((x >> 32) & 0xFFFF) as i64;
        let minute = (x >> 48) as i64;
        let second = _mm_extract_epi16(vals, 4) as i64;
        let (first, len) = ts::month(yidx + month - 1);
        if day > len as i64 {
            return None;
        }
        Some(((first + day - 1) * 86_400 + hour * 3600 + minute * 60 + second) * 1000)
    }

    /// A timestamp field: F1 or F2 in a year of `y`, or F3 of 1 to 16 digits.
    #[inline(always)]
    pub unsafe fn ts_at(p: *const u8, s: usize, e: usize, y: &Years) -> Option<i64> {
        let (c, n) = content(p, s, e);
        match n {
            20 if *c.add(19) == b'Z' => date_time(c, y),
            25 => {
                let sign = *c.add(19);
                let o = |i: usize| (*c.add(i)).wrapping_sub(b'0') as i64;
                let (a, b, g, h) = (o(20), o(21), o(23), o(24));
                if (sign != b'+' && sign != b'-') || *c.add(22) != b':' || a > 9 || b > 9 || g > 9 || h > 9 {
                    return None;
                }
                let (oh, om) = (a * 10 + b, g * 10 + h);
                if oh > 23 || om > 59 {
                    return None;
                }
                let ms = date_time(c, y)?;
                let off = (oh * 60 + om) * 60_000;
                Some(if sign == b'+' { ms - off } else { ms + off })
            }
            1..=16 => digits(c, n).map(|v| v as i64),
            _ => None,
        }
    }

    /// The event type field: view or cart (false) or purchase (true).
    #[inline(always)]
    pub unsafe fn type_at(p: *const u8, s: usize, e: usize) -> Option<bool> {
        let (c, n) = content(p, s, e);
        let w = (c as *const u64).read_unaligned();
        if n == 8 && w == u64::from_le_bytes(*b"purchase") {
            return Some(true);
        }
        let w4 = w as u32;
        if n == 4 && (w4 == u32::from_le_bytes(*b"view") || w4 == u32::from_le_bytes(*b"cart")) {
            return Some(false);
        }
        None
    }

    /// A field of 0 to 15 digits: its value.
    #[inline(always)]
    pub unsafe fn digits_at(p: *const u8, s: usize, e: usize) -> Option<u64> {
        let (c, n) = content(p, s, e);
        if n > 15 {
            return None;
        }
        digits(c, n)
    }
}

/// The event line starting at `p`: its values and its length including the
/// `\n`, or None for the general path.
///
/// # Safety
/// `SPAN` bytes from `p` must be readable.
#[inline(always)]
pub unsafe fn event(p: *const u8, y: &Years) -> Option<(Event, usize)> {
    #[cfg(target_feature = "avx2")]
    {
        use simd::*;
        // Field ends first, then each field on its own (independent work).
        let e = split::<7>(p)?;
        let eid = key_at(p, 0, e[0]);
        let user = key_at(p, e[0] + 1, e[1]);
        let t = ts_at(p, e[1] + 1, e[2], y);
        let ingest = ts_at(p, e[2] + 1, e[3], y);
        let purchase = type_at(p, e[3] + 1, e[4]);
        let product = key_at(p, e[4] + 1, e[5]);
        let price = digits_at(p, e[5] + 1, e[6]);
        let ev = Event { eid: eid?, user: user?, t: t?, ingest: ingest?, purchase: purchase?, product: product?, price: price? };
        Some((ev, e[6] + 1))
    }
    #[cfg(not(target_feature = "avx2"))]
    {
        let _ = (p, y);
        None
    }
}

/// The click line starting at `p`, like `event`.
///
/// # Safety
/// `SPAN` bytes from `p` must be readable.
#[inline(always)]
pub unsafe fn click(p: *const u8, y: &Years) -> Option<(Click, usize)> {
    #[cfg(target_feature = "avx2")]
    {
        use simd::*;
        let e = split::<4>(p)?;
        let cid = key_at(p, 0, e[0]);
        let user = key_at(p, e[0] + 1, e[1]);
        let campaign = key_at(p, e[1] + 1, e[2]);
        let t = ts_at(p, e[2] + 1, e[3], y);
        Some((Click { cid: cid?, user: user?, campaign: campaign?, t: t? }, e[3] + 1))
    }
    #[cfg(not(target_feature = "avx2"))]
    {
        let _ = (p, y);
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{csv, keys};

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
        fn chance(&mut self, pct: u64) -> bool {
            self.below(100) < pct
        }
    }

    const ID: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";
    const ODD: &[u8] = b",\"\n\r -+:.TZxe0123456789\x00\xff\t'";

    fn id(r: &mut Rng) -> Vec<u8> {
        let n = match r.below(10) {
            0 => r.below(14) as usize,
            _ => 1 + r.below(10) as usize,
        };
        let mut v: Vec<u8> = (0..n).map(|_| ID[r.below(ID.len() as u64) as usize]).collect();
        if r.chance(3) && !v.is_empty() {
            let i = r.below(v.len() as u64) as usize;
            v[i] = ODD[r.below(ODD.len() as u64) as usize];
        }
        v
    }

    fn num(r: &mut Rng, lo: u64, hi: u64, width: usize) -> Vec<u8> {
        format!("{:0width$}", lo + r.below(hi - lo + 1)).into_bytes()
    }

    fn timestamp(r: &mut Rng) -> Vec<u8> {
        let year: &[u8] = match r.below(8) {
            0 => b"2027",
            1 => b"2025",
            2 => b"1999",
            _ => b"2026",
        };
        let mut s = year.to_vec();
        match r.below(10) {
            0..=5 | 9 => {
                s.push(b'-');
                s.extend(num(r, 0, 13, 2));
                s.push(b'-');
                s.extend(if r.chance(90) { num(r, 1, 31, 2) } else { num(r, 0, 39, 2) });
                s.push(b'T');
                s.extend(num(r, 0, 24, 2));
                s.push(b':');
                s.extend(num(r, 0, 60, 2));
                s.push(b':');
                s.extend(num(r, 0, 60, 2));
                if r.below(10) < 7 {
                    s.push(b'Z');
                } else {
                    s.push(if r.chance(50) { b'+' } else { b'-' });
                    s.extend(num(r, 0, 24, 2));
                    s.push(if r.chance(97) { b':' } else { b'.' });
                    s.extend(num(r, 0, 60, 2));
                }
            }
            _ => {
                let n = if r.chance(80) { 13 } else { r.below(19) as usize };
                s = (0..n).map(|_| b'0' + r.below(10) as u8).collect();
            }
        }
        if r.chance(3) && !s.is_empty() {
            let i = r.below(s.len() as u64) as usize;
            s[i] = ODD[r.below(ODD.len() as u64) as usize];
        }
        if r.chance(1) {
            s.truncate(r.below(s.len() as u64 + 1) as usize);
        }
        s
    }

    fn line(r: &mut Rng, fields: Vec<Vec<u8>>) -> Vec<u8> {
        let mut out = Vec::new();
        for (i, f) in fields.iter().enumerate() {
            if i > 0 {
                out.push(b',');
            }
            let q = r.chance(20);
            if q {
                out.push(b'"');
            }
            out.extend_from_slice(f);
            if q {
                out.push(b'"');
            }
        }
        // Rare damage anywhere in the line.
        if r.chance(5) && !out.is_empty() {
            let i = r.below(out.len() as u64 + 1) as usize;
            match r.below(3) {
                0 => out.insert(i, ODD[r.below(ODD.len() as u64) as usize]),
                1 => {
                    if i < out.len() {
                        out.remove(i);
                    }
                }
                _ => out.truncate(i),
            }
        }
        // A newline inside ends the line there.
        if let Some(i) = out.iter().position(|&b| b == b'\n') {
            out.truncate(i);
        }
        out
    }

    fn years() -> Years {
        // 2026-12-20 .. 2027-01-10: both years in the table
        Years::new(1_797_724_800_000, 1_799_539_200_000)
    }

    /// Lay out `l` + '\n' + filler at the start of a SPAN-sized buffer.
    fn buffer(r: &mut Rng, l: &[u8]) -> Vec<u8> {
        let mut b = l.to_vec();
        b.push(b'\n');
        while b.len() < SPAN + 64 {
            b.push(ODD[r.below(ODD.len() as u64) as usize]);
        }
        b
    }

    fn digits_value(s: &[u8]) -> Option<u64> {
        if s.len() > 15 || !s.iter().all(|b| b.is_ascii_digit()) {
            return None;
        }
        Some(s.iter().fold(0u64, |v, &b| v * 10 + (b - b'0') as u64))
    }

    #[test]
    fn events_match_general_path() {
        let mut r = Rng(0x9E37_79B9_7F4A_7C15);
        let y = years();
        let (mut fast, mut total) = (0, 0);
        for _ in 0..600_000 {
            let typ: Vec<u8> = match r.below(20) {
                0 => b"purchase".to_vec(),
                1 => b"purchases".to_vec(),
                2 => b"View".to_vec(),
                3 => Vec::new(),
                4..=6 => b"cart".to_vec(),
                7..=9 => b"purchase".to_vec(),
                _ => b"view".to_vec(),
            };
            let width = 1 + r.below(17) as usize;
            let price = match r.below(10) {
                0 => Vec::new(),
                1 => b"12.50".to_vec(),
                2 => num(&mut r, 0, 99_999_999_999, width),
                _ => num(&mut r, 0, 20_000_000, 1),
            };
            let fields = vec![id(&mut r), id(&mut r), timestamp(&mut r), timestamp(&mut r), typ, id(&mut r), price];
            let l = line(&mut r, fields);
            let buf = buffer(&mut r, &l);
            total += 1;
            let Some((e, n)) = (unsafe { event(buf.as_ptr(), &y) }) else { continue };
            fast += 1;
            assert_eq!(n, l.len() + 1, "{:?}", String::from_utf8_lossy(&l));
            let f = csv::split::<7>(&l).unwrap_or_else(|| panic!("malformed accepted: {:?}", String::from_utf8_lossy(&l)));
            let ctx = String::from_utf8_lossy(&l).to_string();
            assert_eq!(keys::pack(f[0]), Some(e.eid), "{ctx}");
            assert_eq!(keys::pack(f[1]), Some(e.user), "{ctx}");
            assert_eq!(ts::parse(f[2]), e.t, "{ctx}");
            assert_eq!(ts::parse(f[3]), e.ingest, "{ctx}");
            assert_ne!(e.t, ts::BAD, "{ctx}");
            assert_ne!(e.ingest, ts::BAD, "{ctx}");
            assert_eq!(f[4] == b"purchase", e.purchase, "{ctx}");
            assert!(f[4] == b"purchase" || f[4] == b"view" || f[4] == b"cart", "{ctx}");
            assert_eq!(keys::pack(f[5]), Some(e.product), "{ctx}");
            assert_eq!(digits_value(f[6]), Some(e.price), "{ctx}");
        }
        assert!(fast * 5 > total, "fast path took only {fast} of {total}");
    }

    #[test]
    fn clicks_match_general_path() {
        let mut r = Rng(0x2545_F491_4F6C_DD1D);
        let y = years();
        let (mut fast, mut total) = (0, 0);
        for _ in 0..600_000 {
            let fields = vec![id(&mut r), id(&mut r), id(&mut r), timestamp(&mut r)];
            let l = line(&mut r, fields);
            let buf = buffer(&mut r, &l);
            total += 1;
            let Some((c, n)) = (unsafe { click(buf.as_ptr(), &y) }) else { continue };
            fast += 1;
            assert_eq!(n, l.len() + 1, "{:?}", String::from_utf8_lossy(&l));
            let f = csv::split::<4>(&l).unwrap_or_else(|| panic!("malformed accepted: {:?}", String::from_utf8_lossy(&l)));
            let ctx = String::from_utf8_lossy(&l).to_string();
            assert_eq!(keys::pack(f[0]), Some(c.cid), "{ctx}");
            assert_eq!(keys::pack(f[1]), Some(c.user), "{ctx}");
            assert_eq!(keys::pack(f[2]), Some(c.campaign), "{ctx}");
            assert_eq!(ts::parse(f[3]), c.t, "{ctx}");
            assert_ne!(c.t, ts::BAD, "{ctx}");
        }
        assert!(fast * 5 > total, "fast path took only {fast} of {total}");
    }

    #[test]
    fn years_table() {
        let y = years();
        assert_eq!(y.ystr[0], u32::from_le_bytes(*b"2026"));
        assert_eq!(y.ystr[1], u32::from_le_bytes(*b"2027"));
        let one = Years::new(1_788_652_800_000, 1_789_862_400_000); // 2026-09-06 .. 2026-09-20
        assert_eq!(one.ystr[1], 0);
    }
}
