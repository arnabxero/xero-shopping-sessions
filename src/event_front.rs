//! Single-pass guarded AVX2+PCLMULQDQ frontends for events (96B, 7 fields) and
//! clicks (64B, 4 fields).
//!
//! Scans each row directly into bitmasks (commas, newlines, quotes) without
//! intermediate batch offset arrays. Every fast-path assumption is checked before
//! emitting; any other physical row delegates to exact `csv::scan` so semantics
//! are 100% identical on all inputs.

use crate::csv;
use crate::ts::{parse_price, DateCache};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Event<'a> {
    pub eid: &'a [u8],
    pub uid: &'a [u8],
    pub product: &'a [u8],
    pub timestamp: Option<i64>,
    pub ingest: Option<i64>,
    pub kind: u8, // 0 invalid; 1 view/cart; 2 purchase
    pub price: u32,
}

#[cold]
#[inline(never)]
fn fallback<'a, F: FnMut(Event<'a>)>(data: &'a [u8], dates: &DateCache, f: &mut F) {
    csv::scan::<7, _>(data, |fields| {
        let kind = match fields[4] {
            b"view" | b"cart" => 1,
            b"purchase" => 2,
            _ => 0,
        };
        let (timestamp, ingest) = dates.parse_event_ingest(fields[2], fields[3]);
        f(Event {
            eid: fields[0],
            uid: fields[1],
            product: fields[5],
            timestamp,
            ingest,
            kind,
            price: if kind == 2 { parse_price(fields[6]) } else { 0 },
        });
    });
}

#[inline(always)]
fn prefix_xor(mut q: u128) -> u128 {
    q ^= q << 1;
    q ^= q << 2;
    q ^= q << 4;
    q ^= q << 8;
    q ^= q << 16;
    q ^= q << 32;
    q ^= q << 64;
    q
}

#[inline(always)]
fn scalar_masks(data: &[u8; 96]) -> (u128, u128, u128) {
    const LO7: u64 = 0x7f7f_7f7f_7f7f_7f7f;
    #[inline(always)]
    fn eq_mask(word: u64, value: u64) -> u128 {
        let y = word ^ value;
        let high = !(((y & LO7).wrapping_add(LO7)) | y | LO7);
        ((high >> 7).wrapping_mul(0x0102_0408_1020_4080) >> 56) as u128
    }
    let (mut c, mut l, mut q) = (0u128, 0u128, 0u128);
    let p = data.as_ptr();
    for i in 0..12 {
        let w = unsafe { (p.add(i * 8) as *const u64).read_unaligned() };
        c |= eq_mask(w, 0x2c2c_2c2c_2c2c_2c2c) << (i * 8);
        l |= eq_mask(w, 0x0a0a_0a0a_0a0a_0a0a) << (i * 8);
        q |= eq_mask(w, 0x2222_2222_2222_2222) << (i * 8);
    }
    (c, l, q)
}

#[inline(always)]
fn field(data: &[u8; 96], a: usize, b: usize) -> &[u8] {
    let quoted = (unsafe { *data.get_unchecked(a) } == b'"') as usize;
    unsafe { data.get_unchecked(a + quoted..b - quoted) }
}

#[inline(always)]
fn parse_row<'a, P: Fn(u128) -> u128>(
    data: &'a [u8; 96],
    end: usize,
    mut commas: u128,
    quotes: u128,
    dates: &DateCache,
    parity: &P,
) -> Option<Event<'a>> {
    let below = (1u128 << end) - 1;
    commas &= below;
    if commas.count_ones() != 6 {
        return None;
    }
    let low = commas as u64;
    let c0 = low.trailing_zeros() as usize;
    let c1 = (low & low.wrapping_sub(1)).trailing_zeros() as usize;
    if c1 > 32 {
        return None;
    }
    let q = quotes & below;
    let seps = commas | (1u128 << end);
    let edges = (seps << 1) | 1 | (seps >> 1);
    if ((q & !edges) | (parity(q) & seps)) != 0 {
        return None;
    }
    let c5 = 127 - commas.leading_zeros() as usize;

    let p = data.as_ptr();
    unsafe {
        let tq = (*p.add(c1 + 1) == b'"') as usize;
        let ta = c1 + 1 + tq;
        let (c3, timestamp, ingest) = if *p.add(ta + 19) == b'Z' && *p.add(ta + 20 + tq) == b',' {
            let c2 = ta + 20 + tq;
            let iq = (*p.add(c2 + 1) == b'"') as usize;
            let ia = c2 + 1 + iq;
            let c3 = ia + 20 + iq;
            if *p.add(c3) != b',' {
                return None;
            }
            let (ts, ing) =
                dates.parse_event_ingest(data.get_unchecked(ta..ta + 20), data.get_unchecked(ia..ia + 20));
            (c3, ts, ing)
        } else if *p.add(ta + 13 + tq) == b',' {
            let c2 = ta + 13 + tq;
            let iq = (*p.add(c2 + 1) == b'"') as usize;
            let ia = c2 + 1 + iq;
            let c3 = ia + 20 + iq;
            if *p.add(c3) != b',' {
                return None;
            }
            (
                c3,
                DateCache::parse_ts_13(data.get_unchecked(ta..ta + 13)),
                dates.parse_f1_ingest(data.get_unchecked(ia..ia + 20)),
            )
        } else if *p.add(ta + 25 + tq) == b',' {
            let c2 = ta + 25 + tq;
            let iq = (*p.add(c2 + 1) == b'"') as usize;
            let ia = c2 + 1 + iq;
            let c3 = ia + 20 + iq;
            if *p.add(c3) != b',' {
                return None;
            }
            (
                c3,
                dates.parse_ts_25(data.get_unchecked(ta..ta + 25)),
                dates.parse_f1_ingest(data.get_unchecked(ia..ia + 20)),
            )
        } else {
            return None;
        };
        let kq = (*p.add(c3 + 1) == b'"') as usize;
        let ka = c3 + 1 + kq;
        let k4 = (p.add(ka) as *const u32).read_unaligned();
        let is_vc = ((k4 == u32::from_ne_bytes(*b"view")) | (k4 == u32::from_ne_bytes(*b"cart")))
            & (*p.add(ka + 4 + kq) == b',');
        let (kind, c4) = if is_vc {
            (1u8, ka + 4 + kq)
        } else if (p.add(ka) as *const u64).read_unaligned() == u64::from_ne_bytes(*b"purchase")
            && *p.add(ka + 8 + kq) == b','
        {
            (2u8, ka + 8 + kq)
        } else {
            return None;
        };
        if c4 >= c5 {
            return None;
        }
        Some(Event {
            eid: field(data, 0, c0),
            uid: field(data, c0 + 1, c1),
            product: field(data, c4 + 1, c5),
            timestamp,
            ingest,
            kind,
            price: if kind == 2 { parse_price(field(data, c5 + 1, end)) } else { 0 },
        })
    }
}

#[inline(always)]
fn scan_with<'a, F: FnMut(Event<'a>), M: Fn(&[u8; 96]) -> (u128, u128, u128), P: Fn(u128) -> u128>(
    mut data: &'a [u8],
    dates: &DateCache,
    mut f: F,
    masks: M,
    parity: P,
) {
    while data.len() >= 96 {
        let bytes: &'a [u8; 96] = unsafe { &*data.as_ptr().cast::<[u8; 96]>() };
        let (commas, lines, quotes) = masks(bytes);
        let end = lines.trailing_zeros() as usize;
        if end < 96 {
            if let Some(ev) = parse_row(bytes, end, commas, quotes, dates, &parity) {
                f(ev);
            } else {
                fallback(unsafe { data.get_unchecked(..end + 1) }, dates, &mut f);
            }
            data = unsafe { data.get_unchecked(end + 1..) };
        } else {
            let end = data[96..]
                .iter()
                .position(|&b| b == b'\n')
                .map_or(data.len(), |p| 96 + p + 1);
            fallback(&data[..end], dates, &mut f);
            data = &data[end..];
        }
    }
    if !data.is_empty() {
        fallback(data, dates, &mut f);
    }
}

#[inline(always)]
pub fn scan<'a, F: FnMut(Event<'a>)>(data: &'a [u8], dates: &DateCache, f: F) {
    #[cfg(target_arch = "x86_64")]
    if csv::simd_backend() == "avx2"
        && is_x86_feature_detected!("pclmulqdq")
        && is_x86_feature_detected!("popcnt")
    {
        unsafe { scan_avx2(data, dates, f) };
        return;
    }
    scan_with(data, dates, f, scalar_masks, prefix_xor);
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,bmi1,bmi2,popcnt,pclmulqdq")]
unsafe fn scan_avx2<'a, F: FnMut(Event<'a>)>(mut data: &'a [u8], dates: &DateCache, mut f: F) {
    use core::arch::x86_64::*;
    let v_cm = _mm256_set1_epi8(b',' as i8);
    let v_nl = _mm256_set1_epi8(b'\n' as i8);
    let v_qt = _mm256_set1_epi8(b'"' as i8);
    let v_ones = _mm_set1_epi64x(-1);
    let parity = |q: u128| -> u128 {
        let bits = _mm_set_epi64x((q >> 64) as i64, q as i64);
        let lo = _mm_cvtsi128_si64(_mm_clmulepi64_si128::<0>(bits, v_ones)) as u64;
        let hi = _mm_cvtsi128_si64(_mm_clmulepi64_si128::<1>(bits, v_ones)) as u64;
        lo as u128 | (((hi ^ 0u64.wrapping_sub(lo >> 63)) as u128) << 64)
    };
    while data.len() >= 96 {
        let p = data.as_ptr();
        let bytes: &'a [u8; 96] = &*p.cast::<[u8; 96]>();
        let b0 = _mm256_loadu_si256(p.cast());
        let b1 = _mm256_loadu_si256(p.add(32).cast());
        let b2 = _mm256_loadu_si256(p.add(64).cast());
        let commas = (_mm256_movemask_epi8(_mm256_cmpeq_epi8(b0, v_cm)) as u32 as u128)
            | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(b1, v_cm)) as u32 as u128) << 32)
            | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(b2, v_cm)) as u32 as u128) << 64);
        let lines = (_mm256_movemask_epi8(_mm256_cmpeq_epi8(b0, v_nl)) as u32 as u128)
            | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(b1, v_nl)) as u32 as u128) << 32)
            | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(b2, v_nl)) as u32 as u128) << 64);
        let quotes = (_mm256_movemask_epi8(_mm256_cmpeq_epi8(b0, v_qt)) as u32 as u128)
            | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(b1, v_qt)) as u32 as u128) << 32)
            | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(b2, v_qt)) as u32 as u128) << 64);
        let end = lines.trailing_zeros() as usize;
        if end < 96 {
            if let Some(ev) = parse_row(bytes, end, commas, quotes, dates, &parity) {
                f(ev);
            } else {
                fallback(data.get_unchecked(..end + 1), dates, &mut f);
            }
            data = data.get_unchecked(end + 1..);
        } else {
            let end = data[96..]
                .iter()
                .position(|&b| b == b'\n')
                .map_or(data.len(), |pos| 96 + pos + 1);
            fallback(&data[..end], dates, &mut f);
            data = &data[end..];
        }
    }
    if !data.is_empty() {
        fallback(data, dates, &mut f);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dates() -> DateCache {
        DateCache::new(1_791_417_600_000, 1_792_627_200_000)
    }

    fn expected<'a>(input: &'a [u8], dates: &DateCache) -> Vec<Event<'a>> {
        let mut out = Vec::new();
        csv::scan::<7, _>(input, |f| {
            let kind = match f[4] {
                b"view" | b"cart" => 1,
                b"purchase" => 2,
                _ => 0,
            };
            out.push(Event {
                eid: f[0],
                uid: f[1],
                product: f[5],
                timestamp: dates.parse_ts(f[2]),
                ingest: dates.parse_f1_ingest(f[3]),
                kind,
                price: if kind == 2 { parse_price(f[6]) } else { 0 },
            });
        });
        out
    }

    fn check(input: &[u8]) {
        let d = dates();
        let want = expected(input, &d);
        let mut got = Vec::new();
        scan_with(input, &d, |e| got.push(e), scalar_masks, prefix_xor);
        assert_eq!(got, want, "scalar input={input:?}");
        got.clear();
        scan(input, &d, |e| got.push(e));
        assert_eq!(got, want, "selected input={input:?}");
    }

    #[test]
    fn bit_masks_every_lane_and_byte() {
        for lane in 0..96 {
            for byte in 0..=255u8 {
                let mut block = [0u8; 96];
                block[lane] = byte;
                let want = (
                    if byte == b',' { 1u128 << lane } else { 0 },
                    if byte == b'\n' { 1u128 << lane } else { 0 },
                    if byte == b'"' { 1u128 << lane } else { 0 },
                );
                assert_eq!(scalar_masks(&block), want);
            }
        }
        for lane in 0..128 {
            assert_eq!(prefix_xor(1u128 << lane), u128::MAX << lane);
        }
    }

    #[test]
    fn timestamps_quotes_and_truncations() {
        for ts in [
            "2026-10-08T12:34:56Z",
            "2026-10-08T12:34:56-04:00",
            "1791462896000",
            "0001791462896000",
            "86400000",
            "2026-02-30T12:34:56Z",
            "2026-10-08T24:00:00Z",
            "2026-10-08T12:34:56+24:00",
            "2026-10-08T12:34:56Z\r",
            "999999999999999999999999",
            "",
        ] {
            for qmask in 0..128 {
                let vals = ["e1", "u1", ts, "2026-10-08T13:00:00Z", "purchase", "p1", "00250"];
                let row = vals
                    .iter()
                    .enumerate()
                    .map(|(i, s)| if qmask & (1 << i) != 0 { format!("\"{s}\"") } else { (*s).to_owned() })
                    .collect::<Vec<_>>()
                    .join(",")
                    + "\n";
                let input = row.repeat(3).into_bytes();
                check(&input);
                for end in 0..row.len() {
                    check(&input[..row.len() + end]);
                }
            }
        }
    }

    #[test]
    fn mutations_offsets_long_ids_and_seams() {
        let valid = b"e1,u1,2026-10-08T12:34:56Z,2026-10-08T13:00:00Z,purchase,p1,250\n";
        for offset in 0..64 {
            for pos in 0..valid.len() {
                for byte in [0, b'"', b',', b'\n', b'\r', b' ', 255] {
                    let mut input = vec![b'!'; offset];
                    input.extend_from_slice(valid);
                    input[offset + pos] = byte;
                    input.extend_from_slice(valid);
                    check(&input[offset..]);
                }
            }
        }
        for size in [0, 31, 32, 33, 63, 64, 65, 95, 96, 97, 128, 16_385] {
            let mut input = vec![b'x'; size];
            input.extend_from_slice(&valid[2..]);
            input.extend_from_slice(valid);
            check(&input);
        }
    }
}
