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
    if month < 1 || month > 12 || day < 1 || day > days_in_month(year, month) || hour > 23
        || minute > 59 || second > 59
    {
        return BAD;
    }
    let days = days_from_civil(year as i64, month as i64, day as i64);
    (days * 86_400 + (hour * 3600 + minute * 60 + second) as i64) * 1000
}

/// Parse a timestamp to epoch milliseconds; BAD if it is not F1/F2/F3.
#[inline]
pub fn parse(s: &[u8]) -> i64 {
    let n = s.len();
    if n == 20 {
        if s[19] == b'Z' {
            return civil(s);
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

/// F3: one or more ASCII digits.
#[inline]
fn parse_f3(s: &[u8]) -> i64 {
    if s.is_empty() {
        return BAD;
    }
    let mut v: i64 = 0;
    let mut sig = 0usize; // significant digits seen
    for &b in s {
        let x = d(b);
        if x >= 10 {
            return BAD;
        }
        if sig > 0 || x != 0 {
            sig += 1;
            if sig <= 18 {
                v = v * 10 + x as i64;
            }
        }
    }
    if sig > 18 {
        HUGE
    } else {
        v
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
}
