//! Exact 64-bit keys for ID strings.
//!
//! A string of at most 10 bytes drawn from `[0-9A-Z_a-z]` is packed into 60
//! bits, 6 bits per byte, big-endian, padded with the code 0. The codes keep
//! ASCII order, so for two packed keys `a < b` exactly when the strings compare
//! that way byte by byte (a prefix sorts first because the pad code 0 is below
//! every character code).
//!
//! Any other non-empty string is interned in a global table and gets the key
//! `FALLBACK | index`. Comparisons involving such a key go through the bytes.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Mutex, RwLock};

pub const FALLBACK: u64 = 1 << 63;

const fn build_codes() -> [u8; 256] {
    let mut t = [0xFFu8; 256];
    let mut i = 0;
    while i < 10 {
        t[b'0' as usize + i] = 1 + i as u8;
        i += 1;
    }
    i = 0;
    while i < 26 {
        t[b'A' as usize + i] = 11 + i as u8;
        i += 1;
    }
    t[b'_' as usize] = 37;
    i = 0;
    while i < 26 {
        t[b'a' as usize + i] = 38 + i as u8;
        i += 1;
    }
    t
}

static CODE: [u8; 256] = build_codes();

const fn build_chars() -> [u8; 64] {
    let mut t = [0u8; 64];
    let mut c = 0;
    while c < 256 {
        let code = build_codes()[c];
        if code != 0xFF {
            t[code as usize] = c as u8;
        }
        c += 1;
    }
    t
}

static CHAR: [u8; 64] = build_chars();

/// The packed key of `s`, or None if it cannot be packed.
#[inline(always)]
pub fn pack(s: &[u8]) -> Option<u64> {
    if s.len() > 10 {
        return None;
    }
    let mut k = 0u64;
    let mut bad = 0u8;
    for &b in s {
        let c = CODE[b as usize];
        bad |= c & 0x80;
        k = (k << 6) | (c as u64 & 0x3F);
    }
    if bad != 0 {
        return None;
    }
    Some(k << (6 * (10 - s.len())))
}

struct Interner {
    map: HashMap<Box<[u8]>, u64>,
    strings: Vec<Box<[u8]>>,
}

static INTERNER: Mutex<Option<Interner>> = Mutex::new(None);
/// Read-only snapshot of the interned strings, filled by `freeze`.
static FROZEN: RwLock<Vec<Box<[u8]>>> = RwLock::new(Vec::new());

#[cold]
fn intern(s: &[u8]) -> u64 {
    let mut guard = INTERNER.lock().unwrap();
    let it = guard.get_or_insert_with(|| Interner { map: HashMap::new(), strings: Vec::new() });
    if let Some(&k) = it.map.get(s) {
        return k;
    }
    let k = FALLBACK | it.strings.len() as u64;
    it.strings.push(s.into());
    it.map.insert(s.into(), k);
    k
}

/// The key of a non-empty string (interning it if it cannot be packed).
#[inline(always)]
pub fn key(s: &[u8]) -> u64 {
    match pack(s) {
        Some(k) => k,
        None => intern(s),
    }
}

/// `pack` for a string of at most 16 bytes whose 16 bytes from its start are
/// readable: classify all 16 lanes at once and gather the 6-bit codes with
/// two multiply-adds.
#[cfg(target_feature = "avx2")]
#[inline(always)]
unsafe fn pack16(p: *const u8, len: usize) -> Option<u64> {
    use std::arch::x86_64::*;
    if len > 10 {
        return None;
    }
    let v = _mm_loadu_si128(p as *const __m128i);
    let ge = |c: u8| _mm_cmpgt_epi8(v, _mm_set1_epi8(c as i8 - 1));
    let le = |c: u8| _mm_cmplt_epi8(v, _mm_set1_epi8(c as i8 + 1));
    let dig = _mm_and_si128(ge(b'0'), le(b'9'));
    let up = _mm_and_si128(ge(b'A'), le(b'Z'));
    let low = _mm_and_si128(ge(b'a'), le(b'z'));
    let us = _mm_cmpeq_epi8(v, _mm_set1_epi8(b'_' as i8));
    let codes = _mm_or_si128(
        _mm_or_si128(
            _mm_and_si128(dig, _mm_sub_epi8(v, _mm_set1_epi8(47))),
            _mm_and_si128(up, _mm_sub_epi8(v, _mm_set1_epi8(54))),
        ),
        _mm_or_si128(_mm_and_si128(low, _mm_sub_epi8(v, _mm_set1_epi8(59))), _mm_and_si128(us, _mm_set1_epi8(37))),
    );
    let valid = _mm_or_si128(_mm_or_si128(dig, up), _mm_or_si128(low, us));
    let idx = _mm_setr_epi8(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
    let inlen = _mm_cmplt_epi8(idx, _mm_set1_epi8(len as i8));
    if _mm_movemask_epi8(_mm_andnot_si128(valid, inlen)) != 0 {
        return None;
    }
    let codes = _mm_and_si128(codes, inlen);
    // c[2i] * 64 + c[2i+1]  ->  pairs * 4096 + next pair: 4 chars per 32-bit lane.
    let m16 = _mm_maddubs_epi16(codes, _mm_set1_epi16(0x0140));
    let m32 = _mm_madd_epi16(m16, _mm_set1_epi32(0x0001_1000));
    let l0 = _mm_cvtsi128_si32(m32) as u32 as u64;
    let l1 = _mm_extract_epi32(m32, 1) as u32 as u64;
    let l2 = _mm_extract_epi32(m32, 2) as u32 as u64;
    Some((l0 << 36) | (l1 << 12) | (l2 >> 12))
}

/// `key`, using the 16-byte path when the 16 bytes from the start of `s` lie
/// before `end` (the end of readable memory).
#[inline(always)]
pub fn key_bounded(s: &[u8], end: *const u8) -> u64 {
    #[cfg(target_feature = "avx2")]
    {
        if s.len() <= 10 && (s.as_ptr() as usize).wrapping_add(16) <= end as usize {
            return match unsafe { pack16(s.as_ptr(), s.len()) } {
                Some(k) => k,
                None => intern(s),
            };
        }
    }
    let _ = end;
    key(s)
}

/// Copy the interned strings into a lock-free-to-read snapshot. Call after
/// every key has been created and before `cmp`/`decode` run in hot loops.
pub fn freeze() {
    let guard = INTERNER.lock().unwrap();
    let mut frozen = FROZEN.write().unwrap();
    frozen.clear();
    if let Some(it) = guard.as_ref() {
        frozen.extend(it.strings.iter().cloned());
    }
}

pub fn any_fallback() -> bool {
    let guard = INTERNER.lock().unwrap();
    guard.as_ref().map_or(false, |it| !it.strings.is_empty())
}

/// Append the bytes of `k` to `out`.
pub fn decode_into(k: u64, out: &mut Vec<u8>) {
    if k & FALLBACK != 0 {
        if let Some(s) = FROZEN.read().unwrap().get((k & !FALLBACK) as usize) {
            out.extend_from_slice(s);
            return;
        }
        // Not frozen yet: read the live table.
        let guard = INTERNER.lock().unwrap();
        out.extend_from_slice(&guard.as_ref().unwrap().strings[(k & !FALLBACK) as usize]);
        return;
    }
    for i in (0..10).rev() {
        let c = ((k >> (6 * i)) & 0x3F) as usize;
        if c == 0 {
            break;
        }
        out.push(CHAR[c]);
    }
}

pub fn decode(k: u64) -> Vec<u8> {
    let mut v = Vec::with_capacity(10);
    decode_into(k, &mut v);
    v
}

/// Byte-wise comparison of the strings behind two keys.
#[inline(always)]
pub fn cmp(a: u64, b: u64) -> Ordering {
    if (a | b) & FALLBACK == 0 {
        a.cmp(&b)
    } else {
        cmp_slow(a, b)
    }
}

#[cold]
fn cmp_slow(a: u64, b: u64) -> Ordering {
    if a == b {
        return Ordering::Equal;
    }
    decode(a).cmp(&decode(b))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack16_matches_pack() {
        let mut seed = 0x2545_f491_4f6c_dd1du64;
        let mut rnd = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let alphabet: Vec<u8> = (0u8..=255).collect();
        let common = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";
        for round in 0..400_000 {
            let len = 1 + (rnd() % 12) as usize;
            let mut buf = [0u8; 32];
            for b in buf.iter_mut() {
                *b = if round % 4 == 0 { alphabet[(rnd() % 256) as usize] } else { common[(rnd() % 63) as usize] };
            }
            let s = &buf[..len];
            let end = buf.as_ptr().wrapping_add(32);
            let want = pack(s).unwrap_or_else(|| key(s));
            assert_eq!(key_bounded(s, end), want, "{:?}", s);
        }
    }

    #[test]
    fn order_matches_bytes() {
        let words: Vec<&[u8]> = vec![
            b"u1", b"u10", b"u1a", b"U1", b"u", b"a", b"z", b"0", b"_", b"Z", b"c_1", b"c_10", b"c_2",
            b"zzzzzzzzzz", b"0000000000", b"u14npkdll", b"e1", b"e1 ", b" u1", b"A\tB",
        ];
        for &a in &words {
            for &b in &words {
                let ka = key(a);
                let kb = key(b);
                assert_eq!(cmp(ka, kb), a.cmp(b), "{:?} {:?}", a, b);
                assert_eq!(ka == kb, a == b);
            }
            assert_eq!(decode(key(a)), a.to_vec());
        }
    }
}
