//! Timestamps: F1 `YYYY-MM-DDTHH:MM:SSZ`, F2 `YYYY-MM-DDTHH:MM:SS+HH:MM`,
//! F3 ASCII digits (epoch ms). Anything else is bad.

/// A bad timestamp. Smaller than every window start, so window checks fail.
pub const BAD: i64 = i64::MIN;
/// An F3 value too large for any window (more than 18 significant digits).
pub const HUGE: i64 = i64::MAX;

#[inline(always)]
fn d(b: u8) -> u32 {
    b.wrapping_sub(b'0') as u32
}

#[inline(always)]
pub fn days_from_civil(y: i64, m: i64, day: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[inline(always)]
fn days_in_month(y: u32, m: u32) -> u32 {
    match m {
        2 => {
            if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Years with a precomputed month table.
const Y0: u32 = 1900;
const NY: usize = 300;

const fn month_tables() -> ([i32; NY * 12], [u8; NY * 12]) {
    let mut start = [0i32; NY * 12];
    let mut len = [0u8; NY * 12];
    let mut i = 0;
    while i < NY * 12 {
        let y = Y0 as i64 + (i / 12) as i64;
        let m = (i % 12) as i64 + 1;
        // days_from_civil(y, m, 1), written out for const evaluation
        let yy = if m <= 2 { y - 1 } else { y };
        let era = if yy >= 0 { yy } else { yy - 399 } / 400;
        let yoe = yy - era * 400;
        let doy = (153 * ((m + 9) % 12) + 2) / 5;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        start[i] = (era * 146_097 + doe - 719_468) as i32;
        let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
        len[i] = match m {
            2 => {
                if leap {
                    29
                } else {
                    28
                }
            }
            4 | 6 | 9 | 11 => 30,
            _ => 31,
        };
        i += 1;
    }
    (start, len)
}

static MONTHS: ([i32; NY * 12], [u8; NY * 12]) = month_tables();

/// `YYYY-MM-DDTHH:MM:SS` in the first 19 bytes of `s` (caller checks length).
#[inline(always)]
fn civil(s: &[u8]) -> i64 {
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return BAD;
    }
    let (y0, y1, y2, y3) = (d(s[0]), d(s[1]), d(s[2]), d(s[3]));
    let (mo0, mo1, d0, d1) = (d(s[5]), d(s[6]), d(s[8]), d(s[9]));
    let (h0, h1, mi0, mi1, s0, s1) = (d(s[11]), d(s[12]), d(s[14]), d(s[15]), d(s[17]), d(s[18]));
    if (y0 | y1 | y2 | y3 | mo0 | mo1 | d0 | d1 | h0 | h1 | mi0 | mi1 | s0 | s1) >= 10 {
        // Any non-digit wraps to a value >= 10 (u8 wrapping_sub), and OR keeps it >= 10
        // only if some bit >= 16 or the value itself >= 10; check each to be exact.
        if y0 >= 10 || y1 >= 10 || y2 >= 10 || y3 >= 10 || mo0 >= 10 || mo1 >= 10 || d0 >= 10
            || d1 >= 10 || h0 >= 10 || h1 >= 10 || mi0 >= 10 || mi1 >= 10 || s0 >= 10 || s1 >= 10
        {
            return BAD;
        }
    }
    let year = y0 * 1000 + y1 * 100 + y2 * 10 + y3;
    let month = mo0 * 10 + mo1;
    let day = d0 * 10 + d1;
    let hour = h0 * 10 + h1;
    let minute = mi0 * 10 + mi1;
    let second = s0 * 10 + s1;
    if month.wrapping_sub(1) >= 12 || hour > 23 || minute > 59 || second > 59 {
        return BAD;
    }
    let yi = year.wrapping_sub(Y0) as usize;
    let days = if yi < NY {
        let idx = yi * 12 + (month - 1) as usize;
        if day.wrapping_sub(1) >= MONTHS.1[idx] as u32 {
            return BAD;
        }
        MONTHS.0[idx] as i64 + day as i64 - 1
    } else {
        if day < 1 || day > days_in_month(year, month) {
            return BAD;
        }
        days_from_civil(year as i64, month as i64, day as i64)
    };
    (days * 86_400 + (hour * 3600 + minute * 60 + second) as i64) * 1000
}

/// Parse a timestamp to epoch milliseconds; BAD if it is not F1/F2/F3.
/// True if every byte selected by `m` is an ASCII digit.
#[inline(always)]
fn swar_digits(x: u64, m: u64) -> bool {
    let d = x & m;
    let hi = 0xF0F0_F0F0_F0F0_F0F0 & m;
    let three = 0x3030_3030_3030_3030 & m;
    // High nibble 3, and low nibble <= 9 (adding 6 must not reach the high nibble).
    (d & hi) == three && (d.wrapping_add(0x0606_0606_0606_0606 & m) & hi) == three
}

/// F1 `YYYY-MM-DDTHH:MM:SSZ` (exactly 20 bytes, caller checks), branch-light.
#[inline(always)]
fn f1(s: &[u8]) -> i64 {
    let lo = u64::from_le_bytes(s[0..8].try_into().unwrap()); // YYYY-MM-
    let mid = u64::from_le_bytes(s[8..16].try_into().unwrap()); // DDTHH:MM
    let hi = u32::from_le_bytes(s[16..20].try_into().unwrap()) as u64; // :SSZ
    const LO_SEP_M: u64 = 0xFF00_00FF_0000_0000;
    const LO_SEP: u64 = 0x2D00_002D_0000_0000; // '-' at 4 and 7
    const MID_SEP_M: u64 = 0x0000_FF00_00FF_0000;
    const MID_SEP: u64 = 0x0000_3A00_0054_0000; // 'T' at 2, ':' at 5
    const HI_SEP_M: u64 = 0xFF00_00FF;
    const HI_SEP: u64 = 0x5A00_003A; // ':' at 0, 'Z' at 3
    const LO_D: u64 = 0x00FF_FF00_FFFF_FFFF;
    const MID_D: u64 = 0xFFFF_00FF_FF00_FFFF;
    const HI_D: u64 = 0x00FF_FF00;
    let ok = (lo & LO_SEP_M) == LO_SEP
        && (mid & MID_SEP_M) == MID_SEP
        && (hi & HI_SEP_M) == HI_SEP
        && swar_digits(lo, LO_D)
        && swar_digits(mid, MID_D)
        && swar_digits(hi, HI_D);
    if !ok {
        return BAD;
    }
    const Z: u64 = 0x3030_3030_3030_3030;
    let dl = (lo & LO_D) - (Z & LO_D);
    let dm = (mid & MID_D) - (Z & MID_D);
    let dh = (hi & HI_D) - (Z & HI_D);
    // Byte i of t is 10 * digit_i + digit_(i+1) (no carries: each is <= 99).
    let tl = dl * 10 + (dl >> 8);
    let tm = dm * 10 + (dm >> 8);
    let th = dh * 10 + (dh >> 8);
    let byte = |t: u64, i: u32| ((t >> (8 * i)) & 0xFF) as u32;
    let year = byte(tl, 0) * 100 + byte(tl, 2);
    let month = byte(tl, 5);
    let day = byte(tm, 0);
    let hour = byte(tm, 3);
    let minute = byte(tm, 6);
    let second = byte(th, 1);
    if month.wrapping_sub(1) >= 12 || hour > 23 || minute > 59 || second > 59 {
        return BAD;
    }
    let yi = year.wrapping_sub(Y0) as usize;
    let days = if yi < NY {
        let idx = yi * 12 + (month - 1) as usize;
        if day.wrapping_sub(1) >= MONTHS.1[idx] as u32 {
            return BAD;
        }
        MONTHS.0[idx] as i64 + day as i64 - 1
    } else {
        if day < 1 || day > days_in_month(year, month) {
            return BAD;
        }
        days_from_civil(year as i64, month as i64, day as i64)
    };
    (days * 86_400 + (hour * 3600 + minute * 60 + second) as i64) * 1000
}

#[inline(always)]
pub fn parse(s: &[u8]) -> i64 {
    let n = s.len();
    if n == 20 {
        if s[19] == b'Z' {
            return f1(s);
        }
    } else if n == 25 {
        let sign = s[19];
        if (sign == b'+' || sign == b'-') && s[22] == b':' {
            let base = civil(s);
            if base == BAD {
                return BAD;
            }
            let (a, b, c, e) = (d(s[20]), d(s[21]), d(s[23]), d(s[24]));
            if a >= 10 || b >= 10 || c >= 10 || e >= 10 {
                return BAD;
            }
            let oh = a * 10 + b;
            let om = c * 10 + e;
            if oh > 23 || om > 59 {
                return BAD;
            }
            let off = ((oh * 60 + om) as i64) * 60_000;
            return if sign == b'+' { base - off } else { base + off };
        }
    }
    parse_f3(s)
}

/// The value of 8 ASCII digits (first digit in the lowest byte), or None.
#[inline(always)]
fn swar8(chunk: u64) -> Option<u64> {
    if !swar_digits(chunk, u64::MAX) {
        return None;
    }
    let mut v = chunk - 0x3030_3030_3030_3030;
    v = v * 10 + (v >> 8);
    v = ((v & 0x0000_00FF_0000_00FF).wrapping_mul(100 + (1_000_000 << 32))
        + ((v >> 16) & 0x0000_00FF_0000_00FF).wrapping_mul(1 + (10_000 << 32)))
        >> 32;
    Some(v)
}

const POW10: [u64; 9] = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000];

/// F3: one or more ASCII digits.
#[inline]
fn parse_f3(s: &[u8]) -> i64 {
    let n = s.len();
    if (9..=16).contains(&n) {
        // The first 8 digits, then the last n - 8 (the overlap replaced by '0').
        let a = u64::from_le_bytes(s[0..8].try_into().unwrap());
        let b = u64::from_le_bytes(s[n - 8..n].try_into().unwrap());
        let r = n - 8;
        let keep = if r == 8 { u64::MAX } else { !((1u64 << (8 * (8 - r))) - 1) };
        let b = (b & keep) | (0x3030_3030_3030_3030 & !keep);
        return match (swar8(a), swar8(b)) {
            (Some(x), Some(y)) => (x * POW10[r] + y) as i64,
            _ => BAD,
        };
    }
    if s.is_empty() {
        return BAD;
    }
    if s.len() <= 18 {
        let mut v: i64 = 0;
        let mut bad = 0u8;
        for &b in s {
            let x = b.wrapping_sub(b'0');
            bad |= (x >= 10) as u8;
            v = v.wrapping_mul(10).wrapping_add(x as i64);
        }
        return if bad != 0 { BAD } else { v };
    }
    if !s.iter().all(|b| b.is_ascii_digit()) {
        return BAD;
    }
    let sig = &s[s.iter().position(|&b| b != b'0').unwrap_or(s.len())..];
    if sig.len() > 18 {
        HUGE
    } else {
        sig.iter().fold(0i64, |v, &b| v * 10 + (b - b'0') as i64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> i64 {
        parse(s.as_bytes())
    }

    #[test]
    fn forms() {
        assert_eq!(p("2026-10-03T23:58:10Z"), 1_791_071_890_000);
        assert_eq!(p("2026-10-03T19:58:10-04:00"), 1_791_071_890_000);
        assert_eq!(p("1791071890000"), 1_791_071_890_000);
        assert_eq!(p("0001791071890000"), 1_791_071_890_000);
        assert_eq!(p("2026-10-04T01:30:00+02:00"), p("2026-10-03T23:30:00Z"));
        assert_eq!(p("2026-10-03T10:00:00-00:00"), p("2026-10-03T10:00:00Z"));
        assert_eq!(p("2026-10-03T10:00:00+23:59"), p("2026-10-02T10:01:00Z"));
        assert_eq!(p("2028-02-29T00:00:00Z"), 1_835_395_200_000);
        assert_eq!(p("86400000"), 86_400_000);
        assert_eq!(p("0"), 0);
        assert_eq!(p("179102160000000000000"), HUGE);
        assert_eq!(p("0000000000000000000001791071890000"), 1_791_071_890_000);
        assert_eq!(p("999999999999999999"), 999_999_999_999_999_999);
        assert_eq!(p("1000000000000000000"), HUGE);
        assert_eq!(p("1899-12-31T23:59:59Z"), -2_208_988_801_000);
        assert_eq!(p("2199-12-31T23:59:59Z"), 7_258_118_399_000);
        assert_eq!(p("2200-01-01T00:00:00Z"), 7_258_118_400_000);
        assert_eq!(p("0000-03-01T00:00:00Z"), -62_162_035_200_000);
        assert_eq!(p("2100-02-29T00:00:00Z"), BAD);
        assert_eq!(p("2000-02-29T00:00:00Z"), 951_782_400_000);
        assert_eq!(p("1900-02-29T00:00:00Z"), BAD);
        for bad in [
            "", "2026-10-03T23:58:10.000Z", "2026-10-03T23:58:10z", "2026-10-03t23:58:10Z",
            "2026-10-03 23:58:10Z", "2026-10-03T23:58:10", "1791071890000.0", "1.79e12", "-5",
            "2026-13-03T23:58:10Z", "2026-02-30T00:00:00Z", "2027-02-29T00:00:00Z",
            "2026-10-03T24:00:00Z", "2026-10-03T23:60:00Z", "2026-10-03T23:58:60Z",
            "2026-10-03T23:58:10+24:00", "2026-10-03T23:58:10+05:60", "2026-10-03T23:58:10+0200",
            "2026-00-03T23:58:10Z", "2026-10-00T23:58:10Z", "+1791071890000", " 1791071890000",
            "1791071890000\r", "2026-10-03T23:58:10Z\r", "２０26-10-03T23:58:10Z",
        ] {
            assert_eq!(p(bad), BAD, "{bad:?}");
        }
        assert_eq!(parse("١٧٩١٠٧١٨٩٠٠٠٠".as_bytes()), BAD);
    }

    #[test]
    fn f3_matches_simple() {
        let simple = |s: &[u8]| -> i64 {
            if s.is_empty() || !s.iter().all(|b| b.is_ascii_digit()) {
                return BAD;
            }
            let sig = &s[s.iter().position(|&b| b != b'0').unwrap_or(s.len())..];
            if sig.len() > 18 {
                return HUGE;
            }
            sig.iter().fold(0i64, |v, &b| v * 10 + (b - b'0') as i64)
        };
        let mut seed = 0x1234_5678_9abc_def1u64;
        let mut rnd = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for round in 0..400_000 {
            let n = (rnd() % 24) as usize;
            let mut s: Vec<u8> = (0..n).map(|_| b'0' + (rnd() % 10) as u8).collect();
            if round % 3 == 0 && n > 0 {
                let i = (rnd() % n as u64) as usize;
                s[i] = (rnd() % 256) as u8;
            }
            if s.len() == 20 && s[19] == b'Z' || s.len() == 25 {
                continue;
            }
            assert_eq!(parse(&s), simple(&s), "{:?}", String::from_utf8_lossy(&s));
        }
    }

    #[test]
    fn calendar_exhaustive() {
        for y in [0u32, 1, 4, 100, 400, 1600, 1700, 1800, 1890, 9999].into_iter().chain(1895..2205) {
            for m in 0..=13u32 {
                for d in 0..=32u32 {
                    let text = format!("{y:04}-{m:02}-{d:02}T01:02:03Z");
                    let valid = (1..=12).contains(&m) && d >= 1 && d <= days_in_month(y, m);
                    let want = if valid {
                        (days_from_civil(y as i64, m as i64, d as i64) * 86_400 + 3723) * 1000
                    } else {
                        BAD
                    };
                    assert_eq!(p(&text), want, "{text}");
                }
            }
        }
    }

    #[test]
    fn f1_swar_matches_scalar() {
        for base in ["2026-10-03T23:58:10Z", "2028-02-29T00:00:00Z", "1999-12-31T23:59:59Z", "2027-02-28T12:34:56Z"] {
            let b = base.as_bytes();
            for i in 0..19 {
                for v in 0..=255u8 {
                    let mut t = b.to_vec();
                    t[i] = v;
                    assert_eq!(parse(&t), civil(&t), "{:?}", String::from_utf8_lossy(&t));
                }
            }
            // two digits changed at once
            for i in 0..19 {
                for j in 0..19 {
                    for v in [b'0', b'9', b'3', b'/', b':'] {
                        let mut t = b.to_vec();
                        t[i] = v;
                        t[j] = b'0' + ((i + j) % 10) as u8;
                        assert_eq!(parse(&t), civil(&t), "{:?}", String::from_utf8_lossy(&t));
                    }
                }
            }
        }
        assert_eq!(p("00000001791071890000"), 1_791_071_890_000);
        assert_eq!(p("0000000000001791071890000"), 1_791_071_890_000);
    }
}
