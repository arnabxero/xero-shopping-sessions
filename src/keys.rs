
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Mutex;

pub const PACKED: u64 = 1 << 57;
pub const SMALLEST: u64 = 1 << 56;

struct Interner {
    index: HashMap<Vec<u8>, u64>,
    values: Vec<Vec<u8>>,
}

static INTERNER: Mutex<Option<Interner>> = Mutex::new(None);

#[cold]
#[inline(never)]
fn intern(bytes: &[u8]) -> u64 {
    let mut guard = INTERNER.lock().unwrap();
    let interner = guard.get_or_insert_with(|| Interner { index: HashMap::new(), values: Vec::new() });
    if let Some(&at) = interner.index.get(bytes) {
        return SMALLEST | at << 1 | 1;
    }
    let at = interner.values.len() as u64;
    interner.index.insert(bytes.to_vec(), at);
    interner.values.push(bytes.to_vec());
    SMALLEST | at << 1 | 1
}

static KEEP: [u64; 10] = {
    let mut table = [u64::MAX; 10];
    let mut len = 0;
    while len < 8 {
        table[len] = (1u64 << (8 * len)) - 1;
        len += 1;
    }
    table
};

#[inline(always)]
pub unsafe fn pack(at: *const u8, len: usize) -> u64 {
    use std::arch::x86_64::_pext_u64;
    const HIGH: u64 = 0x8080_8080_8080_8080;
    const ONES: u64 = 0x0101_0101_0101_0101;
    if len <= 9 {
        let keep = *KEEP.get_unchecked(len);
        let word = (at as *const u64).read_unaligned() & keep;
        let ninth = if len == 9 { u64::from(*at.add(8)) } else { 0 };
        let filled = word | !keep;
        let zero = filled.wrapping_sub(ONES) & !filled & HIGH;
        if (word | ninth) & HIGH | zero == 0 && (len < 9 || ninth != 0) {
            return _pext_u64(word.swap_bytes(), !HIGH) << 8 | ninth << 1;
        }
    }
    intern(std::slice::from_raw_parts(at, len))
}

#[inline]
pub fn unpack(key: u64, out: &mut Vec<u8>) {
    if key >= PACKED {
        let mut rest = key;
        for _ in 0..9 {
            let byte = (rest >> 57) as u8;
            if byte == 0 {
                break;
            }
            out.push(byte);
            rest <<= 7;
        }
    } else {
        let guard = INTERNER.lock().unwrap();
        out.extend_from_slice(&guard.as_ref().unwrap().values[((key & (SMALLEST - 1)) >> 1) as usize]);
    }
}

#[cold]
#[inline(never)]
fn compare_slow(a: u64, b: u64) -> Ordering {
    let (mut left, mut right) = (Vec::new(), Vec::new());
    unpack(a, &mut left);
    unpack(b, &mut right);
    left.cmp(&right)
}

#[inline(always)]
pub fn compare(a: u64, b: u64) -> Ordering {
    if a >= PACKED && b >= PACKED {
        a.cmp(&b)
    } else {
        compare_slow(a, b)
    }
}

#[inline(always)]
pub fn hash(key: u64) -> u64 {
    key.wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

#[inline]
pub fn unpack_string(key: u64, out: &mut Vec<u8>) {
    let at = out.len();
    out.extend_from_slice(&[0; 4]);
    unpack(key, out);
    let len = (out.len() - at - 4) as u32;
    out[at..at + 4].copy_from_slice(&len.to_le_bytes());
}
