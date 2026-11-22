//! The CSV dialect of rules.md: only `\n` ends a line, every comma separates
//! fields, and a field is either free of quotes or wrapped in one pair of them.
//! Lines with the wrong number of fields or a stray quote are skipped.

/// Bytes per step of the scan. One 32-byte compare at a time: joining two
/// movemasks into a 64-bit mask makes LLVM rebuild it bit by bit.
const STEP: usize = 32;

/// Bit masks over the 32 bytes at `block`: separators (commas and newlines), newlines, quotes.
#[cfg(target_feature = "avx2")]
#[inline(always)]
unsafe fn masks(block: *const u8) -> (u32, u32, u32) {
    use std::arch::x86_64::*;
    let bytes = _mm256_loadu_si256(block.cast());
    let newline = _mm256_cmpeq_epi8(bytes, _mm256_set1_epi8(b'\n' as i8));
    let separator = _mm256_or_si256(newline, _mm256_cmpeq_epi8(bytes, _mm256_set1_epi8(b',' as i8)));
    let quote = _mm256_cmpeq_epi8(bytes, _mm256_set1_epi8(b'"' as i8));
    (_mm256_movemask_epi8(separator) as u32, _mm256_movemask_epi8(newline) as u32, _mm256_movemask_epi8(quote) as u32)
}

#[cfg(not(target_feature = "avx2"))]
#[inline(always)]
unsafe fn masks(block: *const u8) -> (u32, u32, u32) {
    let (mut separator, mut newline, mut quote) = (0u32, 0u32, 0u32);
    for at in 0..STEP {
        let byte = *block.add(at);
        newline |= u32::from(byte == b'\n') << at;
        separator |= u32::from(byte == b'\n' || byte == b',') << at;
        quote |= u32::from(byte == b'"') << at;
    }
    (separator, newline, quote)
}

/// The scan's working memory, kept by each thread between chunks.
#[derive(Default)]
pub struct Index {
    /// Where each separator is, as an offset from the start of the chunk.
    separators: Vec<u32>,
    /// Where each newline is: a subset of `separators`.
    newlines: Vec<u32>,
    /// Per 32-byte step: its quote mask, and how many quotes come before it.
    quotes: Vec<(u32, u32)>,
}

/// Appends the offset of every set bit of `mask` (plus `base`) at `out`, and
/// returns the pointer after them. Writes up to 8 slots more than it keeps.
#[inline(always)]
unsafe fn flatten(mut mask: u32, base: u32, mut out: *mut u32) -> *mut u32 {
    let count = mask.count_ones() as usize;
    let end = out.add(count);
    // Four at a time without looking at the count: mispredicting a loop exit
    // costs more than writing a few slots that the next call overwrites.
    loop {
        for slot in 0..4 {
            *out.add(slot) = base + mask.trailing_zeros();
            mask &= mask.wrapping_sub(1);
        }
        out = out.add(4);
        if mask == 0 {
            return end;
        }
    }
}

/// Calls `line` with the unquoted `(start, end)` of each field for every
/// well-formed line of `N` fields (at most 16) in `data[start..end]`. `start` is the start
/// of a line and `end` is just after a newline or the end of the file; the
/// chunk must be shorter than 4 GB.
///
/// Stage one finds every separator, newline and quote with vector compares.
/// Stage two walks the lines: a line has `N` fields exactly when its newline is
/// the `N`th separator after the previous newline, and it is well formed exactly
/// when it has two quotes for each field that both starts and ends with one,
/// and no others.
#[inline(always)]
pub fn scan<const N: usize>(index: &mut Index, data: &[u8], start: usize, end: usize, mut line: impl FnMut(&[(usize, usize); N])) {
    let len = end - start;
    if len == 0 {
        return;
    }
    assert!(len < u32::MAX as usize - 64 && end <= data.len());
    let steps = len.div_ceil(STEP);
    // Worst case every byte is a separator; the slack is for `flatten` and the closing newline.
    index.separators.clear();
    index.separators.reserve(len + 32);
    index.newlines.clear();
    index.newlines.reserve(len + 32);
    index.quotes.clear();
    index.quotes.reserve(steps + 1);
    // SAFETY: the reserved capacities cover everything written below: at most one
    // slot per input byte plus the slack, and one quote entry per step.
    unsafe {
        let chunk = data.as_ptr().add(start);
        let separators = index.separators.as_mut_ptr();
        let newlines = index.newlines.as_mut_ptr();
        let quotes = index.quotes.as_mut_ptr();
        let (mut separator_at, mut newline_at) = (separators, newlines);
        let mut quotes_before = 0u32;
        for step in 0..steps {
            let offset = step * STEP;
            let (separator, newline, quote) = if offset + STEP <= len {
                masks(chunk.add(offset))
            } else {
                let mut tail = [0u8; STEP];
                tail[..len - offset].copy_from_slice(&data[start + offset..end]);
                masks(tail.as_ptr())
            };
            separator_at = flatten(separator, offset as u32, separator_at);
            newline_at = flatten(newline, offset as u32, newline_at);
            *quotes.add(step) = (quote, quotes_before);
            quotes_before += quote.count_ones();
        }
        if *chunk.add(len - 1) != b'\n' {
            // The file's last line has no newline: close it as if it had one.
            *separator_at = len as u32;
            *newline_at = len as u32;
            (separator_at, newline_at) = (separator_at.add(1), newline_at.add(1));
        }
        // Stage two looks up to N separators ahead: past the last one it must find
        // nothing that could pass for a newline.
        for slot in 0..16 {
            *separator_at.add(slot) = u32::MAX;
        }
        *quotes.add(steps) = (0, quotes_before);
        let lines = newline_at.offset_from(newlines) as usize;
        let quotes_until = |offset: usize| {
            let (mask, before) = *quotes.add(offset / STEP);
            before + (mask & ((1u32 << (offset % STEP)) - 1)).count_ones()
        };

        let mut separator_at = separators;
        let mut line_start = 0usize;
        for line_at in 0..lines {
            let newline = *newlines.add(line_at);
            if *separator_at.add(N - 1) != newline {
                // Too few or too many fields: skip to the separator that is this newline.
                while *separator_at != newline {
                    separator_at = separator_at.add(1);
                }
                separator_at = separator_at.add(1);
                line_start = newline as usize + 1;
                continue;
            }
            let mut fields = [(0usize, 0usize); N];
            let mut from = line_start;
            for (at, field) in fields.iter_mut().enumerate() {
                let to = *separator_at.add(at) as usize;
                *field = (start + from, start + to);
                from = to + 1;
            }
            separator_at = separator_at.add(N);
            let line_quotes = quotes_until(newline as usize) - quotes_until(line_start);
            line_start = newline as usize + 1;
            if line_quotes != 0 {
                let mut wrapped = 0;
                for field in fields.iter_mut() {
                    // A field of two or more bytes that starts and ends with a quote loses both.
                    let both = field.1 - field.0 >= 2 && *data.get_unchecked(field.0) == b'"' && *data.get_unchecked(field.1 - 1) == b'"';
                    *field = (field.0 + usize::from(both), field.1 - usize::from(both));
                    wrapped += u32::from(both);
                }
                if line_quotes != 2 * wrapped {
                    continue;
                }
            }
            line(&fields);
        }
    }
}

/// Splits `data` after its header line into chunks of about `size` bytes that end on line ends.
pub fn chunks(data: &[u8], size: usize) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let Some(header) = data.iter().position(|&byte| byte == b'\n') else {
        return out;
    };
    let mut start = header + 1;
    while start < data.len() {
        let mut end = start.saturating_add(size).min(data.len());
        if end < data.len() {
            end = match data[end..].iter().position(|&byte| byte == b'\n') {
                Some(at) => end + at + 1,
                None => data.len(),
            };
        }
        out.push((start, end));
        start = end;
    }
    out
}
