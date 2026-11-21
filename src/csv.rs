//! The strict CSV dialect of rules.md: `\n` ends a line (`\r` is ordinary), a
//! field may be wrapped in quotes as a whole, any other quote makes the line
//! malformed, and a line must have exactly the expected number of fields.

/// Split `line` into exactly `N` fields with quotes removed, or None if the
/// line is malformed. An empty value means null.
#[inline(always)]
pub fn split<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    let mut out: [&[u8]; N] = [&[]; N];
    let mut n = 0usize;
    let mut start = 0usize;
    let mut quotes = 0u32;
    for (i, &b) in line.iter().enumerate() {
        if b == b',' {
            if n + 1 >= N {
                return None;
            }
            out[n] = &line[start..i];
            n += 1;
            start = i + 1;
        } else if b == b'"' {
            quotes += 1;
        }
    }
    if n + 1 != N {
        return None;
    }
    out[n] = &line[start..];
    if quotes != 0 {
        let mut quoted = 0u32;
        for f in out.iter_mut() {
            let l = f.len();
            if l >= 2 && f[0] == b'"' && f[l - 1] == b'"' {
                quoted += 1;
                *f = &f[1..l - 1];
            }
        }
        if quotes != 2 * quoted {
            return None;
        }
    }
    Some(out)
}

/// Iterate the lines of `data` (each without its `\n`). Text after the last
/// `\n` is a line too.
pub struct Lines<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Lines<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Lines { data, pos: 0 }
    }
}

impl<'a> Iterator for Lines<'a> {
    type Item = &'a [u8];
    #[inline(always)]
    fn next(&mut self) -> Option<&'a [u8]> {
        if self.pos >= self.data.len() {
            return None;
        }
        let rest = &self.data[self.pos..];
        match memchr_nl(rest) {
            Some(i) => {
                self.pos += i + 1;
                Some(&rest[..i])
            }
            None => {
                self.pos = self.data.len();
                Some(rest)
            }
        }
    }
}

#[inline(always)]
pub fn memchr_nl(s: &[u8]) -> Option<usize> {
    s.iter().position(|&b| b == b'\n')
}

/// Bitmasks of `\n`, `,` and `"` in a 64-byte block.
#[cfg(target_feature = "avx2")]
#[inline(always)]
fn block_masks(p: &[u8; 64]) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    unsafe {
        let a = _mm256_loadu_si256(p.as_ptr() as *const __m256i);
        let b = _mm256_loadu_si256(p.as_ptr().add(32) as *const __m256i);
        let m = |c: u8| -> u64 {
            let v = _mm256_set1_epi8(c as i8);
            let lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, v)) as u32 as u64;
            let hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, v)) as u32 as u64;
            lo | (hi << 32)
        };
        (m(b'\n'), m(b','), m(b'"'))
    }
}

#[cfg(not(target_feature = "avx2"))]
#[inline(always)]
fn block_masks(p: &[u8; 64]) -> (u64, u64, u64) {
    let (mut n, mut c, mut q) = (0u64, 0u64, 0u64);
    for (i, &b) in p.iter().enumerate() {
        n |= ((b == b'\n') as u64) << i;
        c |= ((b == b',') as u64) << i;
        q |= ((b == b'"') as u64) << i;
    }
    (n, c, q)
}

/// Call `f` with the `N` unquoted fields of every well-formed line of `data`
/// (same dialect as `Lines` + `split`, in one SIMD pass).
#[inline(always)]
pub fn for_each_row<const N: usize, F: FnMut(&[&[u8]; N])>(data: &[u8], mut f: F) {
    let len = data.len();
    // pos[N] is a sink for the fields past the N-th of a too-long line.
    let mut pos = [(0usize, 0usize); 8];
    debug_assert!(N < 8);
    let mut nf = 0usize; // fields completed in the current line
    let mut fstart = 0usize; // start of the current field
    let mut fq = 0u32; // quotes seen so far in the current field
    let mut bad = false;
    // Close the field [fstart, p) holding q quotes.
    #[inline(always)]
    fn close(data: &[u8], pos: &mut [(usize, usize); 8], nf: &mut usize, bad: &mut bool, fstart: usize, p: usize, q: u32, n: usize) {
        let flen = p - fstart;
        let first = unsafe { *data.get_unchecked(fstart.min(data.len() - 1)) };
        let last = unsafe { *data.get_unchecked(p.saturating_sub(1)) };
        let isq = (q == 2) & (flen >= 2) & (first == b'"') & (last == b'"');
        let ok = (q == 0) | isq;
        let k = (*nf).min(n);
        pos[k] = (fstart + isq as usize, p - isq as usize);
        *bad |= !ok;
        *nf += 1;
    }
    let mut block = 0usize;
    while block < len {
        let mut tail = [0u8; 64];
        let bytes: &[u8; 64] = if block + 64 <= len {
            data[block..block + 64].try_into().unwrap()
        } else {
            tail[..len - block].copy_from_slice(&data[block..]);
            &tail
        };
        let (nl, comma, quote) = block_masks(bytes);
        let mut seps = nl | comma;
        let mut qpend = quote;
        while seps != 0 {
            let i = seps.trailing_zeros();
            seps &= seps - 1;
            let bit = 1u64 << i;
            let below = bit - 1;
            let q = fq + (qpend & below).count_ones();
            qpend &= !(below | bit);
            fq = 0;
            let p = block + i as usize;
            close(data, &mut pos, &mut nf, &mut bad, fstart, p, q, N);
            fstart = p + 1;
            if nl & bit != 0 {
                if !bad && nf == N {
                    let mut fields: [&[u8]; N] = [&[]; N];
                    for j in 0..N {
                        fields[j] = unsafe { data.get_unchecked(pos[j].0..pos[j].1) };
                    }
                    f(&fields);
                }
                nf = 0;
                bad = false;
            }
        }
        fq += qpend.count_ones();
        block += 64;
    }
    // A last line without a trailing newline.
    if len > 0 && (nf > 0 || fstart < len) {
        close(data, &mut pos, &mut nf, &mut bad, fstart, len, fq, N);
        if !bad && nf == N {
            let mut fields: [&[u8]; N] = [&[]; N];
            for j in 0..N {
                fields[j] = unsafe { data.get_unchecked(pos[j].0..pos[j].1) };
            }
            f(&fields);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dialect() {
        assert_eq!(split::<2>(b"a,b"), Some([&b"a"[..], &b"b"[..]]));
        assert_eq!(split::<2>(b"\"a\",\"\""), Some([&b"a"[..], &b""[..]]));
        assert_eq!(split::<2>(b"a,b,c"), None);
        assert_eq!(split::<2>(b"a"), None);
        assert_eq!(split::<2>(b""), None);
        assert_eq!(split::<2>(b"u1,u5\""), None);
        assert_eq!(split::<2>(b"\"u1,u5"), None);
        assert_eq!(split::<2>(b"\",u5"), None);
        assert_eq!(split::<2>(b"\"a\"b\",c"), None);
        assert_eq!(split::<2>(b"\"\"\",c"), None);
        assert_eq!(split::<2>(b"a\r,b\r"), Some([&b"a\r"[..], &b"b\r"[..]]));
        assert_eq!(split::<2>(b" a , b"), Some([&b" a "[..], &b" b"[..]]));
        assert_eq!(split::<2>(b",,"), None);
        assert_eq!(split::<2>(b","), Some([&b""[..], &b""[..]]));
        let lines: Vec<&[u8]> = Lines::new(b"a\n\nb\r\nc").collect();
        assert_eq!(lines, vec![&b"a"[..], &b""[..], &b"b\r"[..], &b"c"[..]]);
        let lines: Vec<&[u8]> = Lines::new(b"a\n").collect();
        assert_eq!(lines, vec![&b"a"[..]]);
    }

    fn slow<const N: usize>(data: &[u8]) -> Vec<Vec<Vec<u8>>> {
        Lines::new(data)
            .filter_map(|l| split::<N>(l))
            .map(|f| f.iter().map(|x| x.to_vec()).collect())
            .collect()
    }

    fn fast<const N: usize>(data: &[u8]) -> Vec<Vec<Vec<u8>>> {
        let mut v = Vec::new();
        for_each_row::<N, _>(data, |f| v.push(f.iter().map(|x| x.to_vec()).collect()));
        v
    }

    #[test]
    fn simd_matches_simple() {
        let mut seed = 0x1234_5678_9abc_def0u64;
        let mut rnd = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let alphabet = b"ab,,,\"\"\n\r x";
        for round in 0..20000 {
            let len = (rnd() % 300) as usize;
            let mut data: Vec<u8> = (0..len).map(|_| alphabet[(rnd() % alphabet.len() as u64) as usize]).collect();
            if round % 3 == 0 {
                // realistic rows with random quoting
                data.clear();
                for _ in 0..(rnd() % 12) {
                    let nf = 1 + rnd() % 8;
                    for k in 0..nf {
                        if k > 0 {
                            data.push(b',');
                        }
                        let q = rnd() % 4 == 0;
                        if q {
                            data.push(b'"');
                        }
                        for _ in 0..(rnd() % 12) {
                            data.push(b"abc12 \r"[(rnd() % 7) as usize]);
                        }
                        if q && rnd() % 10 != 0 {
                            data.push(b'"');
                        }
                    }
                    if rnd() % 20 != 0 {
                        data.push(b'\n');
                    }
                }
            }
            assert_eq!(slow::<2>(&data), fast::<2>(&data), "{:?}", String::from_utf8_lossy(&data));
            assert_eq!(slow::<4>(&data), fast::<4>(&data), "{:?}", String::from_utf8_lossy(&data));
            assert_eq!(slow::<7>(&data), fast::<7>(&data), "{:?}", String::from_utf8_lossy(&data));
        }
    }
}
