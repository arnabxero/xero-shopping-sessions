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
}
