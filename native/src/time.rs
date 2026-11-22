//! Timestamps: the three valid forms of rules.md, as milliseconds since the epoch.

pub const MINUTE: i64 = 60_000;
pub const DAY: i64 = 86_400_000;

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let day_of_year = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year - 719_468
}

/// Two ASCII digits, read as a little-endian u16, to their value; 255 for anything else.
static PAIRS: [u8; 65536] = {
    let mut table = [255u8; 65536];
    let mut value = 0;
    while value < 100 {
        table[(b'0' as usize + value / 10) | (b'0' as usize + value % 10) << 8] = value as u8;
        value += 1;
    }
    table
};

#[inline(always)]
fn pair(bytes: &[u8], at: usize) -> u8 {
    PAIRS[usize::from(u16::from_le_bytes([bytes[at], bytes[at + 1]]))]
}

#[inline(always)]
fn two(bytes: &[u8], at: usize) -> Option<i64> {
    let value = pair(bytes, at);
    (value != 255).then_some(i64::from(value))
}

/// `YYYY-MM-DD` at the start of `bytes`, as milliseconds at that day's midnight.
#[inline(never)]
fn date(bytes: &[u8]) -> Option<i64> {
    if bytes[4] != b'-' || bytes[7] != b'-' {
        return None;
    }
    let year = two(bytes, 0)? * 100 + two(bytes, 2)?;
    let (month, day) = (two(bytes, 5)?, two(bytes, 8)?);
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days_in_month = match month {
        2 => 28 + i64::from(leap),
        4 | 6 | 9 | 11 => 30,
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        _ => return None,
    };
    (1..=days_in_month).contains(&day).then(|| days_from_civil(year, month, day) * DAY)
}

/// The dates a thread has seen lately, by day of the month: a dataset only has
/// a few weeks of them, so the calendar arithmetic is nearly always skipped.
#[derive(Clone, Copy)]
pub struct Dates {
    seen: [(u64, u16, i64); 64],
}

impl Dates {
    pub fn new() -> Self {
        // No date starts with zero bytes, so the empty entries never match.
        Self { seen: [(0, 0, 0); 64] }
    }
}

/// `YYYY-MM-DDTHH:MM:SS` at the start of `bytes` (at least 19 long), as UTC milliseconds.
#[inline(always)]
fn civil(bytes: &[u8], dates: &mut Dates) -> Option<i64> {
    let bytes: &[u8; 19] = bytes[..19].try_into().unwrap();
    let head = u64::from_le_bytes(bytes[..8].try_into().unwrap());
    let day = u16::from_le_bytes([bytes[8], bytes[9]]);
    let entry = &mut dates.seen[usize::from((day & 3) << 4 | (day >> 8 & 15))];
    let midnight = if entry.0 == head && entry.1 == day {
        entry.2
    } else {
        let midnight = date(bytes)?;
        *entry = (head, day, midnight);
        midnight
    };
    let (hour, minute, second) = (pair(bytes, 11), pair(bytes, 14), pair(bytes, 17));
    // 255, for a non-digit, fails these too.
    if bytes[10] != b'T' || bytes[13] != b':' || bytes[16] != b':' || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    Some(midnight + (i64::from(hour) * 3_600 + i64::from(minute) * 60 + i64::from(second)) * 1_000)
}

/// F1, F2 or F3; `None` for a bad timestamp. An F3 value too long to be in any window is `None` too.
#[inline(always)]
pub fn parse(bytes: &[u8], dates: &mut Dates) -> Option<i64> {
    let len = bytes.len();
    if len == 20 && bytes[19] == b'Z' {
        return civil(bytes, dates);
    }
    if len == 25 && (bytes[19] == b'+' || bytes[19] == b'-') && bytes[22] == b':' {
        let (hours, minutes) = (two(bytes, 20)?, two(bytes, 23)?);
        if hours > 23 || minutes > 59 {
            return None;
        }
        let offset = (hours * 60 + minutes) * MINUTE;
        let local = civil(bytes, dates)?;
        return Some(if bytes[19] == b'-' { local + offset } else { local - offset });
    }
    digits(bytes, 18)
}

/// Eight ASCII digits, first digit in the low byte, to their value.
#[inline(always)]
fn eight(chunk: u64) -> Option<u64> {
    let digits = chunk ^ 0x3030_3030_3030_3030;
    if (digits | digits.wrapping_add(0x0606_0606_0606_0606)) & 0xF0F0_F0F0_F0F0_F0F0 != 0 {
        return None;
    }
    let pairs = (digits.wrapping_mul(10) + (digits >> 8)) & 0x00FF_00FF_00FF_00FF;
    let fours = (pairs.wrapping_mul(100) + (pairs >> 16)) & 0x0000_FFFF_0000_FFFF;
    Some((fours.wrapping_mul(10_000) + (fours >> 32)) & 0xFFFF_FFFF)
}

/// A digits-only field of at most `max` significant digits, leading zeros allowed.
#[inline(always)]
pub fn digits(bytes: &[u8], max: usize) -> Option<i64> {
    if bytes.len() == 13 && max >= 13 {
        // The usual epoch-milliseconds length: five digits, then eight.
        let head = u64::from_le_bytes(bytes[..8].try_into().unwrap()) << 24 | 0x30_3030;
        let tail = u64::from_le_bytes(bytes[5..13].try_into().unwrap());
        return Some((eight(head)? * 100_000_000 + eight(tail)?) as i64);
    }
    if bytes.is_empty() {
        return None;
    }
    let mut value = 0i64;
    let mut significant = 0usize;
    for &byte in bytes {
        let digit = byte.wrapping_sub(b'0');
        if digit > 9 {
            return None;
        }
        significant += usize::from(significant > 0 || digit > 0);
        if significant > max {
            // Too long for any window or price, so unusable whatever follows.
            return None;
        }
        value = value * 10 + i64::from(digit);
    }
    Some(value)
}
