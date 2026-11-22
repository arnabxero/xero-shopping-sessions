
use std::arch::x86_64::*;
use std::fs;
use std::path::Path;

pub struct Input {
    file: fs::File,
    len: usize,
}

impl Input {
    pub fn len(&self) -> u64 {
        self.len as u64
    }
}

pub fn open(path: &Path) -> Input {
    let file = fs::File::open(path).unwrap();
    let len = file.metadata().unwrap().len() as usize;
    Input { file, len }
}

#[derive(Clone, Copy)]
pub struct Task {
    pub file: usize,
    from: usize,
    to: usize,
}

pub fn tasks(inputs: &[Input], chunk: usize) -> Vec<Task> {
    let mut out = Vec::new();
    for (file, input) in inputs.iter().enumerate() {
        let mut from = 0;
        while from < input.len {
            let to = (from + chunk).min(input.len);
            out.push(Task { file, from, to });
            from = to;
        }
    }
    out.sort_by_key(|task| std::cmp::Reverse(task.to - task.from));
    out
}

const OVERHANG: usize = 4096;

pub struct Buffer {
    bytes: Vec<u8>,
}

impl Buffer {
    pub fn new() -> Self {
        Buffer { bytes: Vec::new() }
    }

    unsafe fn fill(&mut self, input: &Input, task: Task) -> (usize, usize) {
        use std::os::fd::AsRawFd;
        let fd = input.file.as_raw_fd();
        let origin = task.from.saturating_sub(1);
        let mut want = (task.to + OVERHANG).min(input.len) - origin;
        let mut have = 0;
        loop {
            if self.bytes.len() < want + 65 {
                self.bytes.resize(want + 65, 0);
            }
            while have < want {
                let got = libc::pread(
                    fd,
                    self.bytes.as_mut_ptr().add(have) as *mut libc::c_void,
                    want - have,
                    (origin + have) as libc::off_t,
                );
                assert!(got > 0, "read failed");
                have += got as usize;
            }
            let mut start = 0;
            if task.from > 0 {
                while start < have && self.bytes[start] != b'\n' {
                    start += 1;
                }
                start += 1;
                if start >= task.to - origin {
                    return (0, 0);
                }
            }
            let mut end = task.to - origin;
            if task.to < input.len {
                end -= 1;
                while end < have && self.bytes[end] != b'\n' {
                    end += 1;
                }
                if end == have && origin + have < input.len {
                    want = (want * 2).min(input.len - origin);
                    continue;
                }
                end = (end + 1).min(have);
            }
            if start >= end {
                return (0, 0);
            }
            if self.bytes[end - 1] != b'\n' {
                self.bytes[end] = b'\n';
                end += 1;
            }
            return (start, end);
        }
    }
}

pub type Field = (*const u8, usize);

#[inline(always)]
unsafe fn masks(at: *const u8) -> (u64, u64, u64) {
    let low = _mm256_loadu_si256(at as *const __m256i);
    let high = _mm256_loadu_si256(at.add(32) as *const __m256i);
    let mask = |needle: u8| {
        let needle = _mm256_set1_epi8(needle as i8);
        u64::from(_mm256_movemask_epi8(_mm256_cmpeq_epi8(low, needle)) as u32)
            | u64::from(_mm256_movemask_epi8(_mm256_cmpeq_epi8(high, needle)) as u32) << 32
    };
    (mask(b','), mask(b'\n'), mask(b'"'))
}

#[inline(always)]
unsafe fn take<const K: usize>(at: *const u8, last: bool, out: &mut [Field]) -> usize {
    let (commas, newlines, quotes) = masks(at);
    let separators = commas | newlines;
    let end = _pdep_u64(1 << (K - 1), separators);
    let upto = end | end.wrapping_sub(1);
    let within = quotes & upto;
    let opening = within & (separators << 1 | 1);
    let closing = within & (separators >> 1);
    let opens = _pext_u64(quotes >> 1, separators) << 1 | quotes & 1;
    let closes = _pext_u64(quotes << 1, separators);
    let fields = (1u64 << K) - 1;
    let wrong = (within & !opening & !closing)
        | (opening & closing)
        | ((opens ^ closes) & fields)
        | ((newlines & upto) ^ if last { end } else { 0 });
    if end == 0 || wrong != 0 {
        return 0;
    }
    let mut rest = separators;
    let mut from = 0usize;
    for index in 0..K {
        let to = rest.trailing_zeros() as usize;
        rest &= rest - 1;
        let strip = (opens >> index) as usize & 1;
        *out.get_unchecked_mut(index) = (at.add(from + strip), to - from - 2 * strip);
        from = to + 1;
    }
    from
}

#[cold]
#[inline(never)]
unsafe fn slow_line<const N: usize>(at: *const u8, out: &mut [Field; N]) -> (usize, bool) {
    let mut len = 0;
    while *at.add(len) != b'\n' {
        len += 1;
    }
    let line = std::slice::from_raw_parts(at, len);
    let mut count = 0;
    let mut ok = len > 0;
    for field in line.split(|&byte| byte == b',') {
        let quotes = field.iter().filter(|&&byte| byte == b'"').count();
        let wrapped = quotes == 2 && field.len() >= 2 && field[0] == b'"' && field[field.len() - 1] == b'"';
        if count < N {
            out[count] = if wrapped { (field.as_ptr().add(1), field.len() - 2) } else { (field.as_ptr(), field.len()) };
        }
        ok &= quotes == 0 || wrapped;
        count += 1;
    }
    (len + 1, ok && count == N)
}

#[inline(always)]
pub unsafe fn run<const N: usize>(input: &Input, task: Task, buffer: &mut Buffer, mut line: impl FnMut(&[Field; N])) {
    let (mut from, to) = buffer.fill(input, task);
    let base = buffer.bytes.as_ptr();
    if task.from == 0 {
        while from < to && *base.add(from) != b'\n' {
            from += 1;
        }
        from += 1;
    }
    let mut fields: [Field; N] = [(std::ptr::null(), 0); N];
    while from < to {
        let at = base.add(from);
        let len = if N == 7 {
            let first = take::<3>(at, false, &mut fields[..3]);
            let second = take::<4>(at.add(first), true, &mut fields[3..]);
            if first == 0 || second == 0 { 0 } else { first + second }
        } else if N == 4 {
            take::<4>(at, true, &mut fields)
        } else {
            take::<2>(at, true, &mut fields)
        };
        if len != 0 {
            line(&fields);
            from += len;
        } else {
            let (len, ok) = slow_line(at, &mut fields);
            if ok {
                line(&fields);
            }
            from += len;
        }
    }
}
