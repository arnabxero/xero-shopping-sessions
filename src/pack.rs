//! Bijective, byte-order-preserving 64-bit key packing (`Pack64` and `Base-37` 42-bit keys)
//! plus invertible 42-bit hashing (`mix42`/`unmix42`) and `OverflowInterner` fallback.

use std::cmp::Ordering;
use std::sync::RwLock;

use ahash::RandomState;
use hashbrown::HashTable;

pub const OVERFLOW_BIT: u64 = 1u64 << 63;
pub const EMPTY_KEY: u64 = 0u64;
pub const B37_LIMIT: u64 = 1u64 << 42;
pub const MASK42: u64 = B37_LIMIT - 1;

const M42_A: u64 = 0x2A5F_9B3D_17C5;

const fn inv_mod_2_42(a: u64) -> u64 {
    let mut inv = 1u64;
    let mut i = 0;
    while i < 6 {
        inv = inv.wrapping_mul(2u64.wrapping_sub(a.wrapping_mul(inv))) & MASK42;
        i += 1;
    }
    inv
}

const M42_A_INV: u64 = inv_mod_2_42(M42_A);

const POW37: [u64; 8] = [
    1,
    37,
    1_369,
    50_653,
    1_874_161,
    69_343_957,
    2_565_726_409,
    94_931_877_133,
];

const B37_ENC: [u8; 256] = {
    let mut t = [0xFFu8; 256];
    let mut i = 0usize;
    while i < 10 {
        t[b'0' as usize + i] = (i + 1) as u8;
        i += 1;
    }
    let mut j = 0usize;
    while j < 26 {
        t[b'a' as usize + j] = (j + 11) as u8;
        j += 1;
    }
    t
};

const B37_DEC: [u8; 37] = [
    0, b'0', b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9', b'a', b'b', b'c', b'd', b'e',
    b'f', b'g', b'h', b'i', b'j', b'k', b'l', b'm', b'n', b'o', b'p', b'q', b'r', b's', b't',
    b'u', b'v', b'w', b'x', b'y', b'z',
];

const _: () = {
    assert!((M42_A.wrapping_mul(M42_A_INV) & MASK42) == 1);
    assert!(37 * POW37[7] == 3_512_479_453_921);
    assert!(37 * POW37[7] < B37_LIMIT);
};

/// Fast bijective splitmix64 finalizer for hashing 64-bit packed keys and IDs.
#[inline(always)]
pub fn mix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// Invertible 42-bit bijection (`mix42(x) < 2^42`) for lossless bucket + remainder splitting.
#[inline(always)]
pub fn mix42(mut x: u64) -> u64 {
    x ^= x >> 21;
    x = x.wrapping_mul(M42_A) & MASK42;
    x ^ (x >> 21)
}

/// Exact inverse of `mix42`.
#[inline(always)]
pub fn unmix42(mut x: u64) -> u64 {
    x ^= x >> 21;
    x = x.wrapping_mul(M42_A_INV) & MASK42;
    x ^ (x >> 21)
}

/// Order-preserving base-37 42-bit encoding for `<prefix>[0-9a-z]{1,8}` (len `2..=9`).
/// Outputs values in `[37^7, 37^8) < 2^42`, strictly disjoint from `EMPTY_KEY`, `Pack64`, and `OVERFLOW_BIT`.
#[inline(always)]
pub fn pack_b37(s: &[u8], prefix: u8) -> Option<u64> {
    let len = s.len();
    if len < 2 || len > 9 {
        return None;
    }
    let p = s.as_ptr();
    if unsafe { *p } != prefix {
        return None;
    }
    if len >= 8 {
        let off = len - 8;
        let w = u64::from_le_bytes(unsafe { *(p.add(off) as *const [u8; 8]) }) >> ((1 - off) << 3);
        #[cfg(all(target_arch = "x86_64", target_feature = "ssse3"))]
        {
            use std::arch::x86_64::*;
            unsafe {
                let v = _mm_cvtsi64_si128(w as i64);
                let m_dig = _mm_cmpgt_epi8(_mm_set1_epi8(-118), _mm_sub_epi8(v, _mm_set1_epi8(-80)));
                let m_low = _mm_cmpgt_epi8(_mm_set1_epi8(-102), _mm_sub_epi8(v, _mm_set1_epi8(-31)));
                let valid = _mm_or_si128(m_dig, m_low);
                let need = 0x7Fu32 | ((off as u32) << 7);
                if ((_mm_movemask_epi8(valid) as u32) & 0xFF) != need {
                    return None;
                }
                let sub = _mm_add_epi8(_mm_set1_epi8(0x2F), _mm_and_si128(m_low, _mm_set1_epi8(39)));
                let digits = _mm_and_si128(_mm_sub_epi8(v, sub), valid);
                let pairs = _mm_maddubs_epi16(digits, _mm_set1_epi16((1i16 << 8) | 37));
                let quads = _mm_madd_epi16(pairs, _mm_set1_epi32((1i32 << 16) | 1369));
                let q = _mm_cvtsi128_si64(quads) as u64;
                return Some((q & 0xFFFF_FFFF) * 1_874_161 + (q >> 32));
            }
        }
        #[cfg(not(all(target_arch = "x86_64", target_feature = "ssse3")))]
        {
            let mask7 = if off == 1 { u64::MAX } else { 0x00FF_FFFF_FFFF_FFFF };
            let w7 = w & 0x7F7F_7F7F_7F7F_7F7F;
            let w_hi = w7 | 0x8080_8080_8080_8080;
            let is_dig = (w_hi - 0x3030_3030_3030_3030) & (0xB9B9_B9B9_B9B9_B9B9 - w7);
            let is_low = (w_hi - 0x6161_6161_6161_6161) & (0xFAFA_FAFA_FAFA_FAFA - w7);
            let ok = (is_dig | is_low) & !w & 0x8080_8080_8080_8080;
            if (ok & mask7) != (0x8080_8080_8080_8080 & mask7) {
                return None;
            }
            let low_bit = (is_low & 0x8080_8080_8080_8080) >> 7;
            let digits = (w.wrapping_sub(0x2F2F_2F2F_2F2F_2F2F).wrapping_sub(low_bit * 39)) & mask7;
            let pair =
                ((digits & 0x00FF_00FF_00FF_00FF) * 37) + ((digits >> 8) & 0x00FF_00FF_00FF_00FF);
            let q =
                ((pair & 0x0000_FFFF_0000_FFFF) * 1_369) + ((pair >> 16) & 0x0000_FFFF_0000_FFFF);
            return Some((q & 0xFFFF_FFFF) * 1_874_161 + (q >> 32));
        }
    }
    let mut acc = 0u64;
    let mut i = 1usize;
    while i < len {
        let d = unsafe { *B37_ENC.get_unchecked(*p.add(i) as usize) };
        if (d & 0x80) != 0 {
            return None;
        }
        acc = acc * 37 + (d as u64);
        i += 1;
    }
    Some(acc * unsafe { *POW37.get_unchecked(9 - len) })
}

/// Pair-parallel order-preserving base-37 42-bit encoding using 128-bit SSE4.1/SSSE3 SIMD.
#[inline(always)]
pub fn pack_b37_pair(sa: &[u8], pa: u8, sb: &[u8], pb: u8) -> Option<(u64, u64)> {
    #[cfg(all(target_arch = "x86_64", target_feature = "sse4.1"))]
    {
        let la = sa.len();
        let lb = sb.len();
        if (la == 8 || la == 9) && (lb == 8 || lb == 9) {
            let p_a = sa.as_ptr();
            let p_b = sb.as_ptr();
            if unsafe { *p_a == pa && *p_b == pb } {
                let oa = la - 8;
                let ob = lb - 8;
                let wa = u64::from_le_bytes(unsafe { *(p_a.add(oa) as *const [u8; 8]) })
                    >> ((1 - oa) << 3);
                let wb = u64::from_le_bytes(unsafe { *(p_b.add(ob) as *const [u8; 8]) })
                    >> ((1 - ob) << 3);
                use std::arch::x86_64::*;
                unsafe {
                    let v = _mm_set_epi64x(wb as i64, wa as i64);
                    let m_dig =
                        _mm_cmpgt_epi8(_mm_set1_epi8(-118), _mm_sub_epi8(v, _mm_set1_epi8(-80)));
                    let m_low =
                        _mm_cmpgt_epi8(_mm_set1_epi8(-102), _mm_sub_epi8(v, _mm_set1_epi8(-31)));
                    let valid = _mm_or_si128(m_dig, m_low);
                    let need =
                        (0x7Fu32 | ((oa as u32) << 7)) | ((0x7Fu32 | ((ob as u32) << 7)) << 8);
                    if (_mm_movemask_epi8(valid) as u32) != need {
                        return None;
                    }
                    let sub = _mm_add_epi8(
                        _mm_set1_epi8(0x2F),
                        _mm_and_si128(m_low, _mm_set1_epi8(39)),
                    );
                    let digits = _mm_and_si128(_mm_sub_epi8(v, sub), valid);
                    let pairs = _mm_maddubs_epi16(digits, _mm_set1_epi16((1i16 << 8) | 37));
                    let quads = _mm_madd_epi16(pairs, _mm_set1_epi32((1i32 << 16) | 1369));
                    let qa = _mm_cvtsi128_si64(quads) as u64;
                    let qb = _mm_extract_epi64::<1>(quads) as u64;
                    return Some((
                        (qa & 0xFFFF_FFFF) * 1_874_161 + (qa >> 32),
                        (qb & 0xFFFF_FFFF) * 1_874_161 + (qb >> 32),
                    ));
                }
            }
            return None;
        }
    }
    Some((pack_b37(sa, pa)?, pack_b37(sb, pb)?))
}

/// Ultra-fast pair-parallel Base-37 encoding when caller already validated prefix and length in {8, 9} (`is9` in {0, 1}).
///
/// # Safety
/// `pa` and `pb` must each point to at least `8 + is9` readable bytes with `is9 in 0..=1` and valid prefix byte.
#[inline(always)]
pub unsafe fn pack_b37_pair_89(
    pa: *const u8,
    is9_a: usize,
    pb: *const u8,
    is9_b: usize,
) -> Option<(u64, u64)> {
    #[cfg(all(target_arch = "x86_64", target_feature = "sse4.1"))]
    {
        let wa = u64::from_le_bytes(*(pa.add(is9_a) as *const [u8; 8])) >> ((1 - is9_a) << 3);
        let wb = u64::from_le_bytes(*(pb.add(is9_b) as *const [u8; 8])) >> ((1 - is9_b) << 3);
        use std::arch::x86_64::*;
        let v = _mm_set_epi64x(wb as i64, wa as i64);
        let m_dig = _mm_cmpgt_epi8(_mm_set1_epi8(-118), _mm_sub_epi8(v, _mm_set1_epi8(-80)));
        let m_low = _mm_cmpgt_epi8(_mm_set1_epi8(-102), _mm_sub_epi8(v, _mm_set1_epi8(-31)));
        let valid = _mm_or_si128(m_dig, m_low);
        let need = (0x7Fu32 | ((is9_a as u32) << 7)) | ((0x7Fu32 | ((is9_b as u32) << 7)) << 8);
        if (_mm_movemask_epi8(valid) as u32) != need {
            return None;
        }
        let sub = _mm_add_epi8(_mm_set1_epi8(0x2F), _mm_and_si128(m_low, _mm_set1_epi8(39)));
        let digits = _mm_and_si128(_mm_sub_epi8(v, sub), valid);
        let pairs = _mm_maddubs_epi16(digits, _mm_set1_epi16((1i16 << 8) | 37));
        let quads = _mm_madd_epi16(pairs, _mm_set1_epi32((1i32 << 16) | 1369));
        let qa = _mm_cvtsi128_si64(quads) as u64;
        let qb = _mm_extract_epi64::<1>(quads) as u64;
        Some((
            (qa & 0xFFFF_FFFF) * 1_874_161 + (qa >> 32),
            (qb & 0xFFFF_FFFF) * 1_874_161 + (qb >> 32),
        ))
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "sse4.1")))]
    {
        let sa = std::slice::from_raw_parts(pa, 8 + is9_a);
        let sb = std::slice::from_raw_parts(pb, 8 + is9_b);
        Some((pack_b37(sa, *pa)?, pack_b37(sb, *pb)?))
    }
}

/// Quad-parallel 256-bit AVX2 Base-37 encoding for two interleaved 8/9-byte `(eid, uid)` pairs.
///
/// # Safety
/// Each pointer must point to at least `8 + is9` readable bytes with `is9 in 0..=1` and valid prefix byte.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
pub unsafe fn pack_b37_quad_89(
    pa: *const u8,
    ia: usize,
    pb: *const u8,
    ib: usize,
    pc: *const u8,
    ic: usize,
    pd: *const u8,
    id: usize,
) -> Option<(u64, u64, u64, u64)> {
    let wa = u64::from_le_bytes(*(pa.add(ia) as *const [u8; 8])) >> ((1 - ia) << 3);
    let wb = u64::from_le_bytes(*(pb.add(ib) as *const [u8; 8])) >> ((1 - ib) << 3);
    let wc = u64::from_le_bytes(*(pc.add(ic) as *const [u8; 8])) >> ((1 - ic) << 3);
    let wd = u64::from_le_bytes(*(pd.add(id) as *const [u8; 8])) >> ((1 - id) << 3);
    use std::arch::x86_64::*;
    let v = _mm256_set_epi64x(wd as i64, wc as i64, wb as i64, wa as i64);
    let m_dig = _mm256_cmpgt_epi8(
        _mm256_set1_epi8(-118),
        _mm256_sub_epi8(v, _mm256_set1_epi8(-80)),
    );
    let m_low = _mm256_cmpgt_epi8(
        _mm256_set1_epi8(-102),
        _mm256_sub_epi8(v, _mm256_set1_epi8(-31)),
    );
    let valid = _mm256_or_si256(m_dig, m_low);
    let need = (0x7Fu32 | ((ia as u32) << 7))
        | ((0x7Fu32 | ((ib as u32) << 7)) << 8)
        | ((0x7Fu32 | ((ic as u32) << 7)) << 16)
        | ((0x7Fu32 | ((id as u32) << 7)) << 24);
    if (_mm256_movemask_epi8(valid) as u32) != need {
        return None;
    }
    let sub = _mm256_add_epi8(
        _mm256_set1_epi8(0x2F),
        _mm256_and_si256(m_low, _mm256_set1_epi8(39)),
    );
    let digits = _mm256_and_si256(_mm256_sub_epi8(v, sub), valid);
    let pairs = _mm256_maddubs_epi16(digits, _mm256_set1_epi16((1i16 << 8) | 37));
    let quads = _mm256_madd_epi16(pairs, _mm256_set1_epi32((1i32 << 16) | 1369));
    let prod = _mm256_mul_epu32(quads, _mm256_set1_epi64x(1_874_161));
    let res = _mm256_add_epi64(prod, _mm256_srli_epi64::<32>(quads));
    let lo = _mm256_castsi256_si128(res);
    let hi = _mm256_extracti128_si256::<1>(res);
    Some((
        _mm_cvtsi128_si64(lo) as u64,
        _mm_extract_epi64::<1>(lo) as u64,
        _mm_cvtsi128_si64(hi) as u64,
        _mm_extract_epi64::<1>(hi) as u64,
    ))
}

/// Unified 64-bit hash for user/person keys (`mix42` for Base-37 keys, `mix64` otherwise).
#[inline(always)]
pub fn link_hash(k: u64) -> u64 {
    if k < B37_LIMIT {
        mix42(k)
    } else {
        mix64(k)
    }
}

/// Encode a canonical person ID into its lookup representation (`mix42(canon)` for Base-37).
#[inline(always)]
pub fn encode_canon_val(canon: u64) -> u64 {
    if canon < B37_LIMIT {
        mix42(canon)
    } else {
        canon
    }
}

/// Decode a lookup representation back to the canonical person ID key.
#[inline(always)]
pub fn decode_canon_val(v: u64) -> u64 {
    if v < B37_LIMIT {
        unmix42(v)
    } else {
        v
    }
}

/// Unpack a base-37 42-bit value with `prefix` into `out`, returning the byte length (`2..=9`).
#[inline(always)]
pub fn unpack_b37(val: u64, prefix: u8, out: &mut [u8; 9]) -> usize {
    out[0] = prefix;
    let q0 = val / 1_874_161;
    let q1 = val - q0 * 1_874_161;
    let p0 = q0 / 1_369;
    let p1 = q0 - p0 * 1_369;
    let p2 = q1 / 1_369;
    let p3 = q1 - p2 * 1_369;
    let d0 = p0 / 37;
    let d1 = p0 - d0 * 37;
    let d2 = p1 / 37;
    let d3 = p1 - d2 * 37;
    let d4 = p2 / 37;
    let d5 = p2 - d4 * 37;
    let d6 = p3 / 37;
    let d7 = p3 - d6 * 37;
    let w = unsafe {
        (*B37_DEC.get_unchecked(d0 as usize) as u64)
            | ((*B37_DEC.get_unchecked(d1 as usize) as u64) << 8)
            | ((*B37_DEC.get_unchecked(d2 as usize) as u64) << 16)
            | ((*B37_DEC.get_unchecked(d3 as usize) as u64) << 24)
            | ((*B37_DEC.get_unchecked(d4 as usize) as u64) << 32)
            | ((*B37_DEC.get_unchecked(d5 as usize) as u64) << 40)
            | ((*B37_DEC.get_unchecked(d6 as usize) as u64) << 48)
            | ((*B37_DEC.get_unchecked(d7 as usize) as u64) << 56)
    };
    unsafe { (out.as_mut_ptr().add(1) as *mut u64).write_unaligned(w.to_le()) };
    if d7 != 0 {
        return 9;
    }
    let zero_mask = w.wrapping_sub(0x0101_0101_0101_0101) & !w & 0x8080_8080_8080_8080;
    1 + ((zero_mask.trailing_zeros() >> 3) as usize)
}

/// Encode up to 9 non-NUL 7-bit ASCII bytes into a 63-bit integer (bit 63 is 0)
/// whose unsigned `u64` ordering is identical to byte-wise `&[u8]` ordering.
#[inline(always)]
pub fn pack64_inline(s: &[u8]) -> Option<u64> {
    let len = s.len();
    if len > 9 {
        return None;
    }
    if len >= 8 {
        let p = s.as_ptr();
        let v = u64::from_le_bytes(unsafe { *(p as *const [u8; 8]) });
        let is9 = len == 9;
        let b8 = unsafe { *p.add(len - 1) } & 0u8.wrapping_sub(u8::from(is9));
        let bad_v = (v | (v.wrapping_sub(0x0101_0101_0101_0101) & !v)) & 0x8080_8080_8080_8080;
        if (bad_v != 0) | (is9 & (b8 == 0)) | (b8 >= 128) {
            return None;
        }
        #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
        let acc = (unsafe { std::arch::x86_64::_pext_u64(v.to_be(), 0x7F7F_7F7F_7F7F_7F7F) } << 7)
            | (b8 as u64);
        #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
        let acc = unsafe {
            ((*p as u64) << 56)
                | ((*p.add(1) as u64) << 49)
                | ((*p.add(2) as u64) << 42)
                | ((*p.add(3) as u64) << 35)
                | ((*p.add(4) as u64) << 28)
                | ((*p.add(5) as u64) << 21)
                | ((*p.add(6) as u64) << 14)
                | ((*p.add(7) as u64) << 7)
                | (b8 as u64)
        };
        return Some(acc);
    }
    let mut acc = 0u64;
    let mut i = 0usize;
    while i < len {
        let b = unsafe { *s.get_unchecked(i) };
        if b == 0 || b >= 128 {
            return None;
        }
        acc |= (b as u64) << (56 - 7 * i);
        i += 1;
    }
    Some(acc)
}

/// Unpack an inline `Pack64` key into `out`, returning the byte length (`0..=9`).
#[inline(always)]
pub fn unpack64_inline(v: u64, out: &mut [u8; 9]) -> usize {
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    {
        let hi8 =
            unsafe { std::arch::x86_64::_pdep_u64(v >> 7, 0x7F7F_7F7F_7F7F_7F7F) }.to_be_bytes();
        unsafe {
            std::ptr::copy_nonoverlapping(hi8.as_ptr(), out.as_mut_ptr(), 8);
        }
        let b8 = (v & 0x7F) as u8;
        out[8] = b8;
        if b8 != 0 {
            return 9;
        }
        let hi_le = u64::from_le_bytes(hi8);
        let zero_mask = hi_le.wrapping_sub(0x0101_0101_0101_0101) & !hi_le & 0x8080_8080_8080_8080;
        if zero_mask == 0 {
            8
        } else {
            (zero_mask.trailing_zeros() >> 3) as usize
        }
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
    {
        let mut len = 0usize;
        while len < 9 {
            let b = ((v >> (56 - 7 * len)) & 0x7F) as u8;
            if b == 0 {
                break;
            }
            out[len] = b;
            len += 1;
        }
        len
    }
}

struct OverflowState {
    hasher: RandomState,
    table: HashTable<u32>,
    arena: Vec<u8>,
    spans: Vec<(u32, u32)>,
}

/// Fallback interner for IDs > 9 bytes or containing NUL / non-ASCII bytes.
/// Inline IDs never touch the lock.
pub struct OverflowInterner {
    state: RwLock<OverflowState>,
}

impl OverflowInterner {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(OverflowState {
                hasher: RandomState::with_seeds(0x1357, 0x2468, 0x9abc, 0xdef0),
                table: HashTable::new(),
                arena: Vec::new(),
                spans: Vec::new(),
            }),
        }
    }

    #[inline(always)]
    pub fn encode(&self, s: &[u8]) -> u64 {
        if let Some(k) = pack64_inline(s) {
            return k;
        }
        self.encode_slow(s)
    }

    #[inline(always)]
    pub fn encode_user(&self, s: &[u8]) -> u64 {
        if let Some(k) = pack_b37(s, b'u') {
            return k;
        }
        if let Some(k) = pack64_inline(s) {
            return k;
        }
        self.encode_slow(s)
    }

    #[inline(always)]
    pub fn encode_click(&self, s: &[u8]) -> u64 {
        if let Some(k) = pack_b37(s, b'k') {
            return k;
        }
        if let Some(k) = pack64_inline(s) {
            return k;
        }
        self.encode_slow(s)
    }

    #[cold]
    fn encode_slow(&self, s: &[u8]) -> u64 {
        {
            let st = self.state.read().unwrap();
            let h = st.hasher.hash_one(s);
            if let Some(&id) = st.table.find(h, |&idx| {
                let (o, l) = st.spans[idx as usize];
                &st.arena[o as usize..(o + l) as usize] == s
            }) {
                return OVERFLOW_BIT | (id as u64);
            }
        }
        let mut st = self.state.write().unwrap();
        let h = st.hasher.hash_one(s);
        if let Some(&id) = st.table.find(h, |&idx| {
            let (o, l) = st.spans[idx as usize];
            &st.arena[o as usize..(o + l) as usize] == s
        }) {
            return OVERFLOW_BIT | (id as u64);
        }
        let id = st.spans.len() as u32;
        let off = st.arena.len() as u32;
        st.arena.extend_from_slice(s);
        st.spans.push((off, s.len() as u32));
        let OverflowState {
            hasher,
            table,
            arena,
            spans,
        } = &mut *st;
        table.insert_unique(h, id, |&idx| {
            let (o, l) = spans[idx as usize];
            hasher.hash_one(&arena[o as usize..(o + l) as usize])
        });
        OVERFLOW_BIT | (id as u64)
    }

    /// Lookup already-encoded overflow key without mutating state.
    #[cfg_attr(not(test), allow(dead_code))]
    #[inline(always)]
    pub fn decode_with<'a>(
        &self,
        k: u64,
        buf: &'a mut [u8; 9],
        out_vec: &'a mut Vec<u8>,
    ) -> &'a [u8] {
        if (k & OVERFLOW_BIT) == 0 {
            let len = unpack64_inline(k, buf);
            &buf[..len]
        } else {
            self.decode_slow(k, out_vec)
        }
    }

    /// Decode user key encoded via `encode_user` (`Base-37` `'u'`, `Pack64`, or overflow).
    #[inline(always)]
    pub fn decode_user_with<'a>(
        &self,
        k: u64,
        buf: &'a mut [u8; 9],
        out_vec: &'a mut Vec<u8>,
    ) -> &'a [u8] {
        if k < B37_LIMIT {
            let len = unpack_b37(k, b'u', buf);
            &buf[..len]
        } else if (k & OVERFLOW_BIT) == 0 {
            let len = unpack64_inline(k, buf);
            &buf[..len]
        } else {
            self.decode_slow(k, out_vec)
        }
    }

    #[cold]
    fn decode_slow<'a>(&self, k: u64, out_vec: &'a mut Vec<u8>) -> &'a [u8] {
        let id = (k & 0xFFFF_FFFF) as usize;
        let st = self.state.read().unwrap();
        let (o, l) = st.spans[id];
        out_vec.clear();
        out_vec.extend_from_slice(&st.arena[o as usize..(o + l) as usize]);
        out_vec.as_slice()
    }

    #[cfg_attr(not(test), allow(dead_code))]
    #[inline(always)]
    pub fn cmp_key(&self, a: u64, b: u64) -> Ordering {
        if a == b {
            return Ordering::Equal;
        }
        if ((a | b) & OVERFLOW_BIT) == 0 {
            return a.cmp(&b);
        }
        self.cmp_key_slow(a, b)
    }

    #[inline(always)]
    pub fn cmp_user_key(&self, a: u64, b: u64) -> Ordering {
        if a == b {
            return Ordering::Equal;
        }
        if (a | b) < B37_LIMIT
            || (((a | b) & OVERFLOW_BIT) == 0 && a >= (1u64 << 56) && b >= (1u64 << 56))
        {
            return a.cmp(&b);
        }
        self.cmp_prefixed_key_slow(a, b, b'u')
    }

    #[inline(always)]
    pub fn cmp_click_key(&self, a: u64, b: u64) -> Ordering {
        if a == b {
            return Ordering::Equal;
        }
        if (a | b) < B37_LIMIT
            || (((a | b) & OVERFLOW_BIT) == 0 && a >= (1u64 << 56) && b >= (1u64 << 56))
        {
            return a.cmp(&b);
        }
        self.cmp_prefixed_key_slow(a, b, b'k')
    }

    #[cfg_attr(not(test), allow(dead_code))]
    #[cold]
    fn cmp_key_slow(&self, a: u64, b: u64) -> Ordering {
        let st = self.state.read().unwrap();
        let mut ba = [0u8; 9];
        let mut bb = [0u8; 9];
        let sa: &[u8] = if (a & OVERFLOW_BIT) == 0 {
            let len = unpack64_inline(a, &mut ba);
            &ba[..len]
        } else {
            let (o, l) = st.spans[(a & 0xFFFF_FFFF) as usize];
            &st.arena[o as usize..(o + l) as usize]
        };
        let sb: &[u8] = if (b & OVERFLOW_BIT) == 0 {
            let len = unpack64_inline(b, &mut bb);
            &bb[..len]
        } else {
            let (o, l) = st.spans[(b & 0xFFFF_FFFF) as usize];
            &st.arena[o as usize..(o + l) as usize]
        };
        sa.cmp(sb)
    }

    #[cold]
    fn cmp_prefixed_key_slow(&self, a: u64, b: u64, prefix: u8) -> Ordering {
        let mut ba = [0u8; 9];
        let mut bb = [0u8; 9];
        if ((a | b) & OVERFLOW_BIT) == 0 {
            let sa: &[u8] = if a < B37_LIMIT {
                let len = unpack_b37(a, prefix, &mut ba);
                &ba[..len]
            } else {
                let len = unpack64_inline(a, &mut ba);
                &ba[..len]
            };
            let sb: &[u8] = if b < B37_LIMIT {
                let len = unpack_b37(b, prefix, &mut bb);
                &bb[..len]
            } else {
                let len = unpack64_inline(b, &mut bb);
                &bb[..len]
            };
            return sa.cmp(sb);
        }
        let st = self.state.read().unwrap();
        let sa: &[u8] = if a < B37_LIMIT {
            let len = unpack_b37(a, prefix, &mut ba);
            &ba[..len]
        } else if (a & OVERFLOW_BIT) == 0 {
            let len = unpack64_inline(a, &mut ba);
            &ba[..len]
        } else {
            let (o, l) = st.spans[(a & 0xFFFF_FFFF) as usize];
            &st.arena[o as usize..(o + l) as usize]
        };
        let sb: &[u8] = if b < B37_LIMIT {
            let len = unpack_b37(b, prefix, &mut bb);
            &bb[..len]
        } else if (b & OVERFLOW_BIT) == 0 {
            let len = unpack64_inline(b, &mut bb);
            &bb[..len]
        } else {
            let (o, l) = st.spans[(b & 0xFFFF_FFFF) as usize];
            &st.arena[o as usize..(o + l) as usize]
        };
        sa.cmp(sb)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack64_roundtrip_and_byte_order() {
        for len in 1..=9 {
            for bad_pos in 0..len {
                for bad_byte in [0u8, 0x80u8, 0xFFu8] {
                    let mut s = vec![b'a'; len];
                    s[bad_pos] = bad_byte;
                    assert_eq!(pack64_inline(&s), None, "len={len} pos={bad_pos} b={bad_byte}");
                }
            }
        }
        let mut samples: Vec<&[u8]> = vec![
            b"",
            b" ",
            b" U1",
            b"@",
            b"a@",
            b"U1",
            b"u",
            b"u1",
            b"u1 ",
            b"u10",
            b"u2",
            b"1234567@",
            b"12345678@",
            b"u9zzzzzzz",
            b"k\r",
            b"\x7f",
            b"~~~~~~~~~",
        ];
        let mut encoded: Vec<(u64, &[u8])> = samples
            .iter()
            .map(|&s| (pack64_inline(s).expect("fits inline"), s))
            .collect();
        for &(k, orig) in &encoded {
            assert_eq!(k & OVERFLOW_BIT, 0);
            if !orig.is_empty() {
                assert_ne!(k, EMPTY_KEY);
            }
            let mut buf = [0u8; 9];
            let len = unpack64_inline(k, &mut buf);
            assert_eq!(&buf[..len], orig);
        }
        samples.sort();
        encoded.sort_by_key(|x| x.0);
        let decoded: Vec<&[u8]> = encoded.iter().map(|x| x.1).collect();
        assert_eq!(decoded, samples);
    }

    #[test]
    fn overflow_interner_mixed_ordering_and_identity() {
        let interner = OverflowInterner::new();
        let strings: Vec<&[u8]> = vec![
            b"u1",
            b"u123456789",
            b"u1234567890",
            b"U1234567890",
            "usuário_ç_1".as_bytes(),
            b"very_long_identifier_beyond_nine_bytes",
            b"a",
            b"a_long_overflow_id_2",
        ];
        let mut keys: Vec<(u64, &[u8])> =
            strings.iter().map(|&s| (interner.encode(s), s)).collect();
        for &(k, s) in &keys {
            assert_ne!(k, EMPTY_KEY);
            assert_eq!(interner.encode(s), k);
            let mut b9 = [0u8; 9];
            let mut v = Vec::new();
            assert_eq!(interner.decode_with(k, &mut b9, &mut v), s);
        }
        let mut expected = strings.clone();
        expected.sort();
        keys.sort_by(|a, b| interner.cmp_key(a.0, b.0));
        let got: Vec<&[u8]> = keys.into_iter().map(|x| x.1).collect();
        assert_eq!(got, expected);
    }

    #[test]
    fn b37_roundtrip_order_and_disjoint_from_pack64() {
        let valid_samples: Vec<&[u8]> = vec![
            b"u0",
            b"u00",
            b"u0a",
            b"u0z",
            b"u1",
            b"u10",
            b"u1nixvyzn",
            b"u2",
            b"u9",
            b"ua",
            b"uzz",
            b"uzzzzzzzz",
        ];
        let mut encoded = Vec::new();
        for &s in &valid_samples {
            let k = pack_b37(s, b'u').expect("valid base37");
            assert!(k > 0 && k < B37_LIMIT);
            let p64 = pack64_inline(s).unwrap();
            assert!(p64 >= (1u64 << 56));
            let mut buf = [0u8; 9];
            let len = unpack_b37(k, b'u', &mut buf);
            assert_eq!(&buf[..len], s);
            encoded.push((k, s));
        }
        let mut by_bytes = valid_samples.clone();
        by_bytes.sort();
        encoded.sort_by_key(|x| x.0);
        let got: Vec<&[u8]> = encoded.into_iter().map(|x| x.1).collect();
        assert_eq!(got, by_bytes);

        for bad in [
            b"".as_slice(),
            b"u",
            b"U1",
            b"e123",
            b"u123456789",
            b"u1_2",
            b"u1A",
            b"u1234567Z",
            b"u\x00",
        ] {
            assert_eq!(pack_b37(bad, b'u'), None, "should reject {bad:?}");
        }
    }

    #[test]
    fn mix42_unmix42_bijection() {
        let mut rng = 0x1234_5678_9ABC_DEF0u64;
        for i in 0..10_000u64 {
            let x = if i < 100 {
                i
            } else {
                rng = mix64(rng);
                rng & MASK42
            };
            let h = mix42(x);
            assert!(h < B37_LIMIT);
            assert_eq!(unmix42(h), x, "failed for x={x:#x}");
        }
        assert_eq!(unmix42(mix42(MASK42)), MASK42);
    }

    #[test]
    fn overflow_interner_mixed_user_keys() {
        let interner = OverflowInterner::new();
        let strings: Vec<&[u8]> = vec![
            b"U1",
            b"0_hub",
            b"u0",
            b"u1",
            b"u1_extra",
            b"u10",
            b"u1nixvyzn",
            b"u123456789",
            b"u2",
            b"user_very_long_identifier_exceeding_9_bytes",
            b"z",
        ];
        let mut keys: Vec<(u64, &[u8])> = strings
            .iter()
            .map(|&s| (interner.encode_user(s), s))
            .collect();
        for &(k, s) in &keys {
            let mut b9 = [0u8; 9];
            let mut v = Vec::new();
            assert_eq!(interner.decode_user_with(k, &mut b9, &mut v), s);
        }
        let mut expected = strings.clone();
        expected.sort();
        keys.sort_by(|a, b| interner.cmp_user_key(a.0, b.0));
        let got: Vec<&[u8]> = keys.into_iter().map(|x| x.1).collect();
        assert_eq!(got, expected);
    }

    #[test]
    fn pack_b37_swar_exhaustive_bytes() {
        fn ref_pack_b37(s: &[u8], prefix: u8) -> Option<u64> {
            if s.len() < 2 || s.len() > 9 || s[0] != prefix {
                return None;
            }
            let mut acc = 0u64;
            for &b in &s[1..] {
                let d = B37_ENC[b as usize];
                if (d & 0x80) != 0 {
                    return None;
                }
                acc = acc * 37 + (d as u64);
            }
            Some(acc * POW37[9 - s.len()])
        }

        for template in [*b"u09az09az", *b"uz0a9z0a9", *b"k0z09a9zz"] {
            let prefix = template[0];
            for len in [8usize, 9usize] {
                for pos in 1..len {
                    for b in 0u8..=255 {
                        let mut s = template[..len].to_vec();
                        s[pos] = b;
                        assert_eq!(
                            pack_b37(&s, prefix),
                            ref_pack_b37(&s, prefix),
                            "mismatch len={len} pos={pos} b={b:#x} s={s:?}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn overflow_interner_mixed_click_keys() {
        let interner = OverflowInterner::new();
        let strings: Vec<&[u8]> = vec![
            b"K1",
            b"0_click",
            b"k0",
            b"k1",
            b"k1_extra",
            b"k10",
            b"k1nixvyzn",
            b"k123456789",
            b"k2",
            b"click_very_long_identifier_exceeding_9_bytes",
            b"z",
        ];
        let mut keys: Vec<(u64, &[u8])> = strings
            .iter()
            .map(|&s| (interner.encode_click(s), s))
            .collect();
        let mut expected = strings.clone();
        expected.sort();
        keys.sort_by(|a, b| interner.cmp_click_key(a.0, b.0));
        let got: Vec<&[u8]> = keys.into_iter().map(|x| x.1).collect();
        assert_eq!(got, expected);
    }

    #[test]
    fn pack_b37_pair_matches_single() {
        let valid_u = [b"u0".as_slice(), b"u123456", b"u1234567", b"u1nixvyzn", b"uz9a0z9a0"];
        let valid_e = [b"e0".as_slice(), b"eabcdef", b"e1234567", b"e5clyayvw", b"ezzzzzzzz"];
        for &eu in &valid_u {
            for &ee in &valid_e {
                assert_eq!(
                    pack_b37_pair(eu, b'u', ee, b'e'),
                    Some((pack_b37(eu, b'u').unwrap(), pack_b37(ee, b'e').unwrap())),
                    "pair mismatch on {eu:?} / {ee:?}"
                );
            }
        }
        // Prefix mismatch & overflow lengths
        assert_eq!(pack_b37_pair(b"x1234567", b'u', b"e1234567", b'e'), None);
        assert_eq!(pack_b37_pair(b"u1234567", b'u', b"x1234567", b'e'), None);
        assert_eq!(pack_b37_pair(b"u123456789", b'u', b"e1234567", b'e'), None);
        assert_eq!(pack_b37_pair(b"u1234567", b'u', b"e123456789", b'e'), None);

        // Exhaustive single-byte corruptions across lengths 8 and 9 on both sides
        for tu in [b"u09az09a".as_slice(), b"u09az09az".as_slice()] {
            for te in [b"e0z9a0z9".as_slice(), b"e0z9a0z9a".as_slice()] {
                for pos in 0..tu.len() {
                    let mut u = tu.to_vec();
                    for b in [0u8, b'/', b':', b'@', b'A', b'Z', b'`', b'{', 0x80, 0xFF] {
                        u[pos] = b;
                        let expected = match (pack_b37(&u, b'u'), pack_b37(te, b'e')) {
                            (Some(a), Some(c)) => Some((a, c)),
                            _ => None,
                        };
                        assert_eq!(pack_b37_pair(&u, b'u', te, b'e'), expected);
                    }
                }
                for pos in 0..te.len() {
                    let mut e = te.to_vec();
                    for b in [0u8, b'/', b':', b'@', b'A', b'Z', b'`', b'{', 0x80, 0xFF] {
                        e[pos] = b;
                        let expected = match (pack_b37(tu, b'u'), pack_b37(&e, b'e')) {
                            (Some(a), Some(c)) => Some((a, c)),
                            _ => None,
                        };
                        assert_eq!(pack_b37_pair(tu, b'u', &e, b'e'), expected);
                    }
                }
            }
        }
    }

    #[test]
    fn pack_b37_pair_and_quad_89_matches_single() {
        for tu in [b"u09az09a".as_slice(), b"u09az09az".as_slice()] {
            for te in [b"e0z9a0z9".as_slice(), b"e0z9a0z9a".as_slice()] {
                let ia = tu.len() - 8;
                let ib = te.len() - 8;
                let exp = (pack_b37(tu, b'u').unwrap(), pack_b37(te, b'e').unwrap());
                assert_eq!(
                    unsafe { pack_b37_pair_89(tu.as_ptr(), ia, te.as_ptr(), ib) },
                    Some(exp)
                );
                #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
                assert_eq!(
                    unsafe {
                        pack_b37_quad_89(
                            tu.as_ptr(),
                            ia,
                            te.as_ptr(),
                            ib,
                            tu.as_ptr(),
                            ia,
                            te.as_ptr(),
                            ib,
                        )
                    },
                    Some((exp.0, exp.1, exp.0, exp.1))
                );
                for pos in 1..tu.len() {
                    for bad in [b'A', b'Z', b'_', b'{', b'/', 0u8, 0xFF] {
                        let mut u = tu.to_vec();
                        u[pos] = bad;
                        assert_eq!(
                            unsafe { pack_b37_pair_89(u.as_ptr(), ia, te.as_ptr(), ib) },
                            None
                        );
                        #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
                        assert_eq!(
                            unsafe {
                                pack_b37_quad_89(
                                    u.as_ptr(),
                                    ia,
                                    te.as_ptr(),
                                    ib,
                                    tu.as_ptr(),
                                    ia,
                                    te.as_ptr(),
                                    ib,
                                )
                            },
                            None
                        );
                    }
                }
            }
        }
    }

    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    #[test]
    fn b37_quad_89_matches_scalar() {
        let strs_u = [b"u09az09a".as_slice(), b"u09az09az".as_slice()];
        let strs_e = [b"e0z9a0z9".as_slice(), b"e0z9a0z9a".as_slice()];
        let strs_k = [b"k12ab34c".as_slice(), b"k12ab34cd".as_slice()];
        let strs_u2 = [b"uzz00aa9".as_slice(), b"u1nixvyzn".as_slice()];

        for &a in &strs_u {
            for &b in &strs_e {
                for &c in &strs_k {
                    for &d in &strs_u2 {
                        let exp = (
                            pack_b37(a, a[0]).unwrap(),
                            pack_b37(b, b[0]).unwrap(),
                            pack_b37(c, c[0]).unwrap(),
                            pack_b37(d, d[0]).unwrap(),
                        );
                        let got = unsafe {
                            pack_b37_quad_89(
                                a.as_ptr(),
                                a.len() - 8,
                                b.as_ptr(),
                                b.len() - 8,
                                c.as_ptr(),
                                c.len() - 8,
                                d.as_ptr(),
                                d.len() - 8,
                            )
                        };
                        assert_eq!(got, Some(exp));

                        // Reject any invalid byte in any of the 4 IDs
                        for lane in 0..4usize {
                            let src = [a, b, c, d][lane];
                            for pos in 1..src.len() {
                                for bad in [b'"', b',', b'\n', b'A', b'Z', b'_', b'{', 0u8, 0xFF] {
                                    let mut v = [a.to_vec(), b.to_vec(), c.to_vec(), d.to_vec()];
                                    v[lane][pos] = bad;
                                    let r = unsafe {
                                        pack_b37_quad_89(
                                            v[0].as_ptr(),
                                            v[0].len() - 8,
                                            v[1].as_ptr(),
                                            v[1].len() - 8,
                                            v[2].as_ptr(),
                                            v[2].len() - 8,
                                            v[3].as_ptr(),
                                            v[3].len() - 8,
                                        )
                                    };
                                    assert_eq!(r, None, "failed reject lane={lane} pos={pos} bad={bad:#x}");
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
