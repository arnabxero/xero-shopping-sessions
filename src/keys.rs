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
