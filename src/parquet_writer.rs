//! Minimal, zero-copy Parquet v1 UNCOMPRESSED + PLAIN writer using Thrift Compact Protocol.

use std::fs::File;
use std::io::{BufWriter, IoSlice, Write};
use std::path::{Path, PathBuf};

const _: () = assert!(cfg!(target_endian = "little"));

const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_BINARY: u8 = 8;
const T_LIST: u8 = 9;
const T_STRUCT: u8 = 12;

const PT_INT32: i32 = 1;
const PT_INT64: i32 = 2;
const PT_BYTE_ARRAY: i32 = 6;

const REP_REQUIRED: i32 = 0;
const REP_OPTIONAL: i32 = 1;

const CT_UTF8: i32 = 0;
const CT_DATE: i32 = 6;

const ENC_PLAIN: i32 = 0;
const ENC_RLE: i32 = 3;
const ENC_RLE_DICTIONARY: i32 = 8;

const CODEC_UNCOMPRESSED: i32 = 0;
const PAGE_DATA_PAGE: i32 = 0;
const PAGE_DICTIONARY_PAGE: i32 = 2;

struct ThriftBuf(Vec<u8>);

impl ThriftBuf {
    fn new() -> Self {
        Self(Vec::with_capacity(1024))
    }

    #[inline(always)]
    fn write_varint_u32(&mut self, mut n: u32) {
        while n >= 0x80 {
            self.0.push(((n as u8) & 0x7F) | 0x80);
            n >>= 7;
        }
        self.0.push(n as u8);
    }

    #[inline(always)]
    fn write_varint_u64(&mut self, mut n: u64) {
        while n >= 0x80 {
            self.0.push(((n as u8) & 0x7F) | 0x80);
            n >>= 7;
        }
        self.0.push(n as u8);
    }

    #[inline(always)]
    fn write_zigzag_i32(&mut self, n: i32) {
        let z = ((n << 1) ^ (n >> 31)) as u32;
        self.write_varint_u32(z);
    }

    #[inline(always)]
    fn write_zigzag_i64(&mut self, n: i64) {
        let z = ((n << 1) ^ (n >> 63)) as u64;
        self.write_varint_u64(z);
    }

    #[inline(always)]
    fn write_field_hdr(&mut self, last_id: &mut i16, id: i16, type_id: u8) {
        let delta = id - *last_id;
        if delta > 0 && delta <= 15 {
            self.0.push(((delta as u8) << 4) | type_id);
        } else {
            self.0.push(type_id);
            self.write_zigzag_i32(id as i32);
        }
        *last_id = id;
    }

    #[inline(always)]
    fn write_field_i32(&mut self, last_id: &mut i16, id: i16, val: i32) {
        self.write_field_hdr(last_id, id, T_I32);
        self.write_zigzag_i32(val);
    }

    #[inline(always)]
    fn write_field_i64(&mut self, last_id: &mut i16, id: i16, val: i64) {
        self.write_field_hdr(last_id, id, T_I64);
        self.write_zigzag_i64(val);
    }

    #[inline(always)]
    fn write_field_bytes(&mut self, last_id: &mut i16, id: i16, bytes: &[u8]) {
        self.write_field_hdr(last_id, id, T_BINARY);
        self.write_varint_u32(bytes.len() as u32);
        self.0.extend_from_slice(bytes);
    }

    #[inline(always)]
    fn write_list_hdr(&mut self, elem_type: u8, len: usize) {
        if len < 15 {
            self.0.push(((len as u8) << 4) | elem_type);
        } else {
            self.0.push(0xF0 | elem_type);
            self.write_varint_u32(len as u32);
        }
    }

    #[inline(always)]
    fn write_struct_end(&mut self) {
        self.0.push(0);
    }
}

#[derive(Clone, Copy)]
enum ColLogical {
    None,
    String,
    Date,
}

#[derive(Clone, Copy)]
struct ColSpec {
    name: &'static str,
    phys_type: i32,
    rep_type: i32,
    logical: ColLogical,
}

const USER_DAILY_COLS: [ColSpec; 6] = [
    ColSpec {
        name: "user_id",
        phys_type: PT_BYTE_ARRAY,
        rep_type: REP_REQUIRED,
        logical: ColLogical::String,
    },
    ColSpec {
        name: "day",
        phys_type: PT_INT32,
        rep_type: REP_REQUIRED,
        logical: ColLogical::Date,
    },
    ColSpec {
        name: "sessions",
        phys_type: PT_INT32,
        rep_type: REP_REQUIRED,
        logical: ColLogical::None,
    },
    ColSpec {
        name: "events",
        phys_type: PT_INT32,
        rep_type: REP_REQUIRED,
        logical: ColLogical::None,
    },
    ColSpec {
        name: "revenue_cents",
        phys_type: PT_INT64,
        rep_type: REP_REQUIRED,
        logical: ColLogical::None,
    },
    ColSpec {
        name: "top_category",
        phys_type: PT_BYTE_ARRAY,
        rep_type: REP_OPTIONAL,
        logical: ColLogical::String,
    },
];

const CAMPAIGN_DAILY_COLS: [ColSpec; 7] = [
    ColSpec {
        name: "campaign_id",
        phys_type: PT_BYTE_ARRAY,
        rep_type: REP_REQUIRED,
        logical: ColLogical::String,
    },
    ColSpec {
        name: "channel",
        phys_type: PT_BYTE_ARRAY,
        rep_type: REP_REQUIRED,
        logical: ColLogical::String,
    },
    ColSpec {
        name: "day",
        phys_type: PT_INT32,
        rep_type: REP_REQUIRED,
        logical: ColLogical::Date,
    },
    ColSpec {
        name: "sessions",
        phys_type: PT_INT32,
        rep_type: REP_REQUIRED,
        logical: ColLogical::None,
    },
    ColSpec {
        name: "purchasing_sessions",
        phys_type: PT_INT32,
        rep_type: REP_REQUIRED,
        logical: ColLogical::None,
    },
    ColSpec {
        name: "revenue_cents",
        phys_type: PT_INT64,
        rep_type: REP_REQUIRED,
        logical: ColLogical::None,
    },
    ColSpec {
        name: "assisted_sessions",
        phys_type: PT_INT32,
        rep_type: REP_REQUIRED,
        logical: ColLogical::None,
    },
];

struct ColumnMetaRecord {
    col_idx: usize,
    dict_page_offset: Option<i64>,
    data_page_offset: i64,
    total_size: i64,
    num_values: i64,
    optional: bool,
}

struct RowGroupRecord {
    cols: Vec<ColumnMetaRecord>,
    total_byte_size: i64,
    num_rows: i64,
}

fn append_data_page_header_enc(
    num_values: i32,
    payload_size: i32,
    encoding: i32,
    out: &mut ThriftBuf,
) -> (usize, usize) {
    let start = out.0.len();
    let mut last = 0i16;
    out.write_field_i32(&mut last, 1, PAGE_DATA_PAGE);
    out.write_field_i32(&mut last, 2, payload_size);
    out.write_field_i32(&mut last, 3, payload_size);
    out.write_field_hdr(&mut last, 5, T_STRUCT);
    let mut dlast = 0i16;
    out.write_field_i32(&mut dlast, 1, num_values);
    out.write_field_i32(&mut dlast, 2, encoding);
    out.write_field_i32(&mut dlast, 3, ENC_RLE);
    out.write_field_i32(&mut dlast, 4, ENC_RLE);
    out.write_struct_end();
    out.write_struct_end();
    (start, out.0.len())
}

fn append_dict_page_header(num_values: i32, payload_size: i32, out: &mut ThriftBuf) -> (usize, usize) {
    let start = out.0.len();
    let mut last = 0i16;
    out.write_field_i32(&mut last, 1, PAGE_DICTIONARY_PAGE);
    out.write_field_i32(&mut last, 2, payload_size);
    out.write_field_i32(&mut last, 3, payload_size);
    out.write_field_hdr(&mut last, 7, T_STRUCT);
    let mut dlast = 0i16;
    out.write_field_i32(&mut dlast, 1, num_values);
    out.write_field_i32(&mut dlast, 2, ENC_PLAIN);
    out.write_struct_end();
    out.write_struct_end();
    (start, out.0.len())
}

fn encode_data_page_header(num_values: i32, payload_size: i32, out: &mut ThriftBuf) {
    out.0.clear();
    let _ = append_data_page_header_enc(num_values, payload_size, ENC_PLAIN, out);
}

fn write_all_iovec(f: &mut File, mut bufs: &mut [IoSlice<'_>]) {
    while !bufs.is_empty() {
        match f.write_vectored(bufs) {
            Ok(0) => panic!("write_vectored returned 0"),
            Ok(n) => IoSlice::advance_slices(&mut bufs, n),
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("write_vectored failed: {e}"),
        }
    }
}

fn encode_file_metadata(schema_cols: &[ColSpec], total_rows: i64, rgs: &[RowGroupRecord]) -> Vec<u8> {
    let mut tb = ThriftBuf::new();
    let mut f_last = 0i16;

    // 1: version = 1
    tb.write_field_i32(&mut f_last, 1, 1);

    // 2: schema
    tb.write_field_hdr(&mut f_last, 2, T_LIST);
    tb.write_list_hdr(T_STRUCT, schema_cols.len() + 1);

    // Root schema element: only name (4) and num_children (5)
    {
        let mut slast = 0i16;
        tb.write_field_bytes(&mut slast, 4, b"schema");
        tb.write_field_i32(&mut slast, 5, schema_cols.len() as i32);
        tb.write_struct_end();
    }

    for col in schema_cols {
        let mut slast = 0i16;
        tb.write_field_i32(&mut slast, 1, col.phys_type);
        tb.write_field_i32(&mut slast, 3, col.rep_type);
        tb.write_field_bytes(&mut slast, 4, col.name.as_bytes());
        match col.logical {
            ColLogical::String => {
                tb.write_field_i32(&mut slast, 6, CT_UTF8);
                tb.write_field_hdr(&mut slast, 10, T_STRUCT);
                let mut ulast = 0i16;
                tb.write_field_hdr(&mut ulast, 1, T_STRUCT);
                tb.write_struct_end();
                tb.write_struct_end();
            }
            ColLogical::Date => {
                tb.write_field_i32(&mut slast, 6, CT_DATE);
                tb.write_field_hdr(&mut slast, 10, T_STRUCT);
                let mut ulast = 0i16;
                tb.write_field_hdr(&mut ulast, 6, T_STRUCT);
                tb.write_struct_end();
                tb.write_struct_end();
            }
            ColLogical::None => {}
        }
        tb.write_struct_end();
    }

    // 3: num_rows
    tb.write_field_i64(&mut f_last, 3, total_rows);

    // 4: row_groups
    tb.write_field_hdr(&mut f_last, 4, T_LIST);
    tb.write_list_hdr(T_STRUCT, rgs.len());
    for rg in rgs {
        let mut rlast = 0i16;
        tb.write_field_hdr(&mut rlast, 1, T_LIST);
        tb.write_list_hdr(T_STRUCT, rg.cols.len());
        for cm in &rg.cols {
            let spec = &schema_cols[cm.col_idx];
            let mut clast = 0i16;
            tb.write_field_i64(&mut clast, 2, cm.dict_page_offset.unwrap_or(cm.data_page_offset));
            tb.write_field_hdr(&mut clast, 3, T_STRUCT);
            {
                let mut mlast = 0i16;
                tb.write_field_i32(&mut mlast, 1, spec.phys_type);
                tb.write_field_hdr(&mut mlast, 2, T_LIST);
                match (cm.dict_page_offset.is_some(), cm.optional) {
                    (true, true) => {
                        tb.write_list_hdr(T_I32, 3);
                        tb.write_zigzag_i32(ENC_RLE);
                        tb.write_zigzag_i32(ENC_PLAIN);
                        tb.write_zigzag_i32(ENC_RLE_DICTIONARY);
                    }
                    (true, false) => {
                        tb.write_list_hdr(T_I32, 2);
                        tb.write_zigzag_i32(ENC_PLAIN);
                        tb.write_zigzag_i32(ENC_RLE_DICTIONARY);
                    }
                    (false, true) => {
                        tb.write_list_hdr(T_I32, 2);
                        tb.write_zigzag_i32(ENC_RLE);
                        tb.write_zigzag_i32(ENC_PLAIN);
                    }
                    (false, false) => {
                        tb.write_list_hdr(T_I32, 1);
                        tb.write_zigzag_i32(ENC_PLAIN);
                    }
                }
                tb.write_field_hdr(&mut mlast, 3, T_LIST);
                tb.write_list_hdr(T_BINARY, 1);
                tb.write_varint_u32(spec.name.len() as u32);
                tb.0.extend_from_slice(spec.name.as_bytes());
                tb.write_field_i32(&mut mlast, 4, CODEC_UNCOMPRESSED);
                tb.write_field_i64(&mut mlast, 5, cm.num_values);
                tb.write_field_i64(&mut mlast, 6, cm.total_size);
                tb.write_field_i64(&mut mlast, 7, cm.total_size);
                tb.write_field_i64(&mut mlast, 9, cm.data_page_offset);
                if let Some(d_off) = cm.dict_page_offset {
                    tb.write_field_i64(&mut mlast, 11, d_off);
                }
                tb.write_struct_end();
            }
            tb.write_struct_end();
        }
        tb.write_field_i64(&mut rlast, 2, rg.total_byte_size);
        tb.write_field_i64(&mut rlast, 3, rg.num_rows);
        tb.write_struct_end();
    }

    // 6: created_by
    tb.write_field_bytes(&mut f_last, 6, b"shopping-sessions");
    tb.write_struct_end();
    tb.0
}

#[inline(always)]
fn as_le_bytes<T>(slice: &[T]) -> &[u8] {
    unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, std::mem::size_of_val(slice)) }
}

/// Encode bit-width=1 definition levels bitmap using Parquet hybrid RLE/BIT_PACKED format
/// prefixed by the 4-byte little-endian byte length of the RLE stream.
fn encode_def_levels_bitwidth1(num_values: usize, def_bits: &[u8], out: &mut Vec<u8>) {
    out.clear();
    let total_bytes = (num_values + 7) >> 3;
    let mut off = 0usize;
    while off < total_bytes {
        let chunk = (total_bytes - off).min(63);
        out.push(((chunk << 1) | 1) as u8);
        out.extend_from_slice(&def_bits[off..off + chunk]);
        off += chunk;
    }
}

fn encode_uid_rle_runs(uid_runs: &[u32], out: &mut Vec<u8>) {
    out.clear();
    let num_dict = uid_runs.len();
    let bw = if num_dict <= 2 {
        1u8
    } else {
        (32 - ((num_dict - 1) as u32).leading_zeros()) as u8
    };
    let wb = ((bw as usize) + 7) >> 3;
    out.push(bw);
    for (did, &cnt) in uid_runs.iter().enumerate() {
        let mut hdr = cnt << 1;
        if hdr < 0x80 {
            out.push(hdr as u8);
        } else {
            while hdr >= 0x80 {
                out.push(((hdr as u8) & 0x7F) | 0x80);
                hdr >>= 7;
            }
            out.push(hdr as u8);
        }
        let b = (did as u32).to_le_bytes();
        out.extend_from_slice(&b[..wb]);
    }
}

fn encode_u8_bitpacked(vals: &[u8], bw: u8, out: &mut Vec<u8>) {
    out.clear();
    let total_vals = vals.len();
    let bw_usize = bw as usize;
    let total_groups = (total_vals + 7) >> 3;
    let num_runs = (total_groups + 62) / 63;
    out.reserve(1 + num_runs + total_groups * bw_usize + 8);
    unsafe {
        let mut dst = out.as_mut_ptr();
        *dst = bw;
        dst = dst.add(1);
        let src = vals.as_ptr();
        let mut off = 0usize;
        while off < total_vals {
            let rem = total_vals - off;
            let run_vals = rem.min(63 * 8);
            let num_groups = (run_vals + 7) >> 3;
            *dst = ((num_groups << 1) | 1) as u8;
            dst = dst.add(1);
            let full_groups = run_vals >> 3;
            for _ in 0..full_groups {
                let p = src.add(off);
                let v0 = *p as u64;
                let v1 = *p.add(1) as u64;
                let v2 = *p.add(2) as u64;
                let v3 = *p.add(3) as u64;
                let v4 = *p.add(4) as u64;
                let v5 = *p.add(5) as u64;
                let v6 = *p.add(6) as u64;
                let v7 = *p.add(7) as u64;
                let packed = match bw {
                    1 => {
                        v0 | (v1 << 1)
                            | (v2 << 2)
                            | (v3 << 3)
                            | (v4 << 4)
                            | (v5 << 5)
                            | (v6 << 6)
                            | (v7 << 7)
                    }
                    2 => {
                        v0 | (v1 << 2)
                            | (v2 << 4)
                            | (v3 << 6)
                            | (v4 << 8)
                            | (v5 << 10)
                            | (v6 << 12)
                            | (v7 << 14)
                    }
                    3 => {
                        v0 | (v1 << 3)
                            | (v2 << 6)
                            | (v3 << 9)
                            | (v4 << 12)
                            | (v5 << 15)
                            | (v6 << 18)
                            | (v7 << 21)
                    }
                    4 => {
                        v0 | (v1 << 4)
                            | (v2 << 8)
                            | (v3 << 12)
                            | (v4 << 16)
                            | (v5 << 20)
                            | (v6 << 24)
                            | (v7 << 28)
                    }
                    5 => {
                        v0 | (v1 << 5)
                            | (v2 << 10)
                            | (v3 << 15)
                            | (v4 << 20)
                            | (v5 << 25)
                            | (v6 << 30)
                            | (v7 << 35)
                    }
                    6 => {
                        v0 | (v1 << 6)
                            | (v2 << 12)
                            | (v3 << 18)
                            | (v4 << 24)
                            | (v5 << 30)
                            | (v6 << 36)
                            | (v7 << 42)
                    }
                    7 => {
                        v0 | (v1 << 7)
                            | (v2 << 14)
                            | (v3 << 21)
                            | (v4 << 28)
                            | (v5 << 35)
                            | (v6 << 42)
                            | (v7 << 49)
                    }
                    _ => (p as *const u64).read_unaligned(),
                };
                (dst as *mut u64).write_unaligned(packed.to_le());
                dst = dst.add(bw_usize);
                off += 8;
            }
            if full_groups < num_groups {
                let mut packed = 0u64;
                let end = (off + 8).min(total_vals);
                for (k, &v) in vals[off..end].iter().enumerate() {
                    packed |= (v as u64) << (k * bw_usize);
                }
                (dst as *mut u64).write_unaligned(packed.to_le());
                dst = dst.add(bw_usize);
                off = end;
            }
        }
        out.set_len(dst.offset_from(out.as_ptr()) as usize);
    }
}

fn encode_u16_rle_hybrid(vals: &[u16], bw: u8, out: &mut Vec<u8>) {
    out.clear();
    let n = vals.len();
    let bw_usize = bw as usize;
    let wb = (bw_usize + 7) >> 3;
    out.reserve(1 + n * wb + (n >> 3) + 64);
    unsafe {
        let mut dst = out.as_mut_ptr();
        *dst = bw;
        dst = dst.add(1);
        let src = vals.as_ptr();
        let mut i = 0usize;
        while i < n {
            let v0 = *src.add(i);
            let mut r = i + 1;
            #[cfg(target_arch = "x86_64")]
            {
                use std::arch::x86_64::*;
                let vv = _mm_set1_epi16(v0 as i16);
                while r + 8 <= n {
                    let blk = _mm_loadu_si128(src.add(r) as *const __m128i);
                    let m = _mm_movemask_epi8(_mm_cmpeq_epi16(blk, vv));
                    if m != 0xFFFF {
                        break;
                    }
                    r += 8;
                }
            }
            while r < n && *src.add(r) == v0 {
                r += 1;
            }
            let rlen = r - i;
            if rlen >= 8 {
                let mut hdr = (rlen as u32) << 1;
                while hdr >= 0x80 {
                    *dst = ((hdr as u8) & 0x7F) | 0x80;
                    dst = dst.add(1);
                    hdr >>= 7;
                }
                *dst = hdr as u8;
                dst = dst.add(1);
                let b = (v0 as u32).to_le_bytes();
                std::ptr::copy_nonoverlapping(b.as_ptr(), dst, wb);
                dst = dst.add(wb);
                i = r;
                continue;
            }
            let bp_start = i;
            let mut groups = 0usize;
            while i < n && groups < 63 {
                if i + 8 <= n {
                    let vv0 = *src.add(i);
                    #[cfg(target_arch = "x86_64")]
                    let all_eq = {
                        use std::arch::x86_64::*;
                        let blk = _mm_loadu_si128(src.add(i) as *const __m128i);
                        _mm_movemask_epi8(_mm_cmpeq_epi16(blk, _mm_set1_epi16(vv0 as i16))) == 0xFFFF
                    };
                    #[cfg(not(target_arch = "x86_64"))]
                    let all_eq = *src.add(i + 1) == vv0
                        && *src.add(i + 2) == vv0
                        && *src.add(i + 3) == vv0
                        && *src.add(i + 4) == vv0
                        && *src.add(i + 5) == vv0
                        && *src.add(i + 6) == vv0
                        && *src.add(i + 7) == vv0;
                    if all_eq {
                        break;
                    }
                }
                i = (i + 8).min(n);
                groups += 1;
            }
            if groups == 0 {
                continue;
            }
            *dst = ((groups << 1) | 1) as u8;
            dst = dst.add(1);
            let mut pos = bp_start;
            while pos + 8 <= i {
                let v0 = *src.add(pos) as u128;
                let v1 = *src.add(pos + 1) as u128;
                let v2 = *src.add(pos + 2) as u128;
                let v3 = *src.add(pos + 3) as u128;
                let v4 = *src.add(pos + 4) as u128;
                let v5 = *src.add(pos + 5) as u128;
                let v6 = *src.add(pos + 6) as u128;
                let v7 = *src.add(pos + 7) as u128;
                let bits = v0
                    | (v1 << bw_usize)
                    | (v2 << (2 * bw_usize))
                    | (v3 << (3 * bw_usize))
                    | (v4 << (4 * bw_usize))
                    | (v5 << (5 * bw_usize))
                    | (v6 << (6 * bw_usize))
                    | (v7 << (7 * bw_usize));
                (dst as *mut u128).write_unaligned(bits.to_le());
                dst = dst.add(bw_usize);
                pos += 8;
            }
            if pos < i {
                let mut bits: u128 = 0;
                for k in 0..(i - pos) {
                    bits |= (*src.add(pos + k) as u128) << (k * bw_usize);
                }
                (dst as *mut u128).write_unaligned(bits.to_le());
                dst = dst.add(bw_usize);
            }
        }
        out.set_len(dst.offset_from(out.as_ptr()) as usize);
    }
}

fn write_required_column(
    writer: &mut BufWriter<File>,
    file_pos: &mut i64,
    hdr_buf: &mut ThriftBuf,
    col_idx: usize,
    num_rows: i32,
    payload: &[u8],
) -> ColumnMetaRecord {
    let data_page_offset = *file_pos;
    encode_data_page_header(num_rows, payload.len() as i32, hdr_buf);
    writer.write_all(&hdr_buf.0).expect("write page hdr");
    writer.write_all(payload).expect("write col payload");
    let total_size = (hdr_buf.0.len() + payload.len()) as i64;
    *file_pos += total_size;
    ColumnMetaRecord {
        col_idx,
        dict_page_offset: None,
        data_page_offset,
        total_size,
        num_values: num_rows as i64,
        optional: false,
    }
}

const MAX_RG_ROWS: usize = 524_288;
const MAX_VAR_BYTES: usize = 128 * 1024 * 1024;

pub struct UserDailyParquetWriter {
    path: PathBuf,
    file: Option<File>,
    file_pos: i64,
    total_rows: i64,
    rgs: Vec<RowGroupRecord>,
    hdr_buf: ThriftBuf,
    encoded_cats: Vec<Vec<u8>>,
    dict_cats_bytes: Vec<u8>,
    num_cats: usize,
    cat_bit_width: u8,
    use_cat_dict: bool,
    dict_days_bytes: Vec<u8>,
    min_day: i32,
    day_span: usize,
    day_bit_width: u8,
    use_day_dict: bool,
    init_cap_rows: usize,

    rg_rows: usize,
    cap_rows: usize,
    dict_uid: Vec<u8>,
    uid_runs: Vec<u32>,
    uid_rle_buf: Vec<u8>,
    col_day: Vec<i32>,
    day_ids: Vec<u8>,
    day_rle_buf: Vec<u8>,
    col_sessions: Vec<i32>,
    sess_dict_bytes: Vec<u8>,
    sess_ids: Vec<u8>,
    sess_rle_buf: Vec<u8>,
    col_events: Vec<i32>,
    dict_rev: Vec<i64>,
    rev_ids: Vec<u16>,
    rev_rle_buf: Vec<u8>,
    def_bits: Vec<u8>,
    rle_buf: Vec<u8>,
    top_cat_ids: Vec<u8>,
    cat_rle_buf: Vec<u8>,
    top_cat_vals: Vec<u8>,
}

impl UserDailyParquetWriter {
    pub fn new(
        path: PathBuf,
        categories: &[String],
        min_day: i32,
        day_span: usize,
        est_rows: usize,
    ) -> Self {
        let mut encoded_cats = Vec::with_capacity(categories.len());
        let mut dict_cats_bytes = Vec::new();
        let num_cats = categories.len();
        let use_cat_dict = !categories.is_empty() && num_cats <= 256;
        if use_cat_dict {
            dict_cats_bytes.reserve(num_cats * 16);
        }
        for c in categories {
            let b = c.as_bytes();
            let len_le = (b.len() as u32).to_le_bytes();
            if use_cat_dict {
                dict_cats_bytes.extend_from_slice(&len_le);
                dict_cats_bytes.extend_from_slice(b);
            } else {
                let mut buf = Vec::with_capacity(4 + b.len());
                buf.extend_from_slice(&len_le);
                buf.extend_from_slice(b);
                encoded_cats.push(buf);
            }
        }
        let cat_bit_width = if num_cats <= 2 {
            1u8
        } else {
            (32 - ((num_cats - 1) as u32).leading_zeros()) as u8
        };

        let use_day_dict = day_span > 0 && day_span <= 256;
        let mut dict_days_bytes = Vec::new();
        if use_day_dict {
            dict_days_bytes.reserve(day_span * 4);
            for d in 0..day_span {
                dict_days_bytes.extend_from_slice(&(min_day + d as i32).to_le_bytes());
            }
        }
        let day_bit_width = if day_span <= 2 {
            1u8
        } else {
            (32 - ((day_span - 1) as u32).leading_zeros()) as u8
        };

        Self {
            path,
            file: None,
            file_pos: 0,
            total_rows: 0,
            rgs: Vec::new(),
            hdr_buf: ThriftBuf::new(),
            encoded_cats,
            dict_cats_bytes,
            num_cats,
            cat_bit_width,
            use_cat_dict,
            dict_days_bytes,
            min_day,
            day_span,
            day_bit_width,
            use_day_dict,
            init_cap_rows: est_rows.clamp(4_096, MAX_RG_ROWS),
            rg_rows: 0,
            cap_rows: 0,
            dict_uid: Vec::new(),
            uid_runs: Vec::new(),
            uid_rle_buf: Vec::new(),
            col_day: Vec::new(),
            day_ids: Vec::new(),
            day_rle_buf: Vec::new(),
            col_sessions: Vec::new(),
            sess_dict_bytes: Vec::new(),
            sess_ids: Vec::new(),
            sess_rle_buf: Vec::new(),
            col_events: Vec::new(),
            dict_rev: vec![0i64],
            rev_ids: Vec::new(),
            rev_rle_buf: Vec::new(),
            def_bits: Vec::new(),
            rle_buf: Vec::new(),
            top_cat_ids: Vec::new(),
            cat_rle_buf: Vec::new(),
            top_cat_vals: Vec::new(),
        }
    }

    pub fn prefault(&mut self) {
        if self.cap_rows == 0 {
            self.ensure_cap_or_flush();
            unsafe {
                let touch = |ptr: *mut u8, bytes: usize| {
                    let mut off = 0usize;
                    while off < bytes {
                        std::ptr::write_volatile(ptr.add(off), 0);
                        off += 4096;
                    }
                };
                let n = self.cap_rows;
                if self.use_day_dict {
                    touch(self.day_ids.as_mut_ptr(), n);
                } else {
                    touch(self.col_day.as_mut_ptr() as *mut u8, n * 4);
                }
                touch(self.col_sessions.as_mut_ptr() as *mut u8, n * 4);
                touch(self.col_events.as_mut_ptr() as *mut u8, n * 4);
                touch(self.rev_ids.as_mut_ptr() as *mut u8, n * 2);
                touch(self.def_bits.as_mut_ptr(), (n + 7) >> 3);
                touch(self.dict_uid.as_mut_ptr(), self.dict_uid.capacity().min(n * 4));
                touch(self.uid_runs.as_mut_ptr() as *mut u8, self.uid_runs.capacity().min(n) * 4);
                if self.use_cat_dict {
                    touch(self.top_cat_ids.as_mut_ptr(), self.top_cat_ids.capacity().min(n));
                }
            }
        }
    }

    #[cold]
    fn ensure_cap_or_flush(&mut self) {
        if self.rg_rows >= MAX_RG_ROWS
            || self.dict_uid.len() >= MAX_VAR_BYTES
            || self.top_cat_vals.len() >= MAX_VAR_BYTES
            || self.dict_rev.len() >= 65_535
        {
            self.flush_rg();
        }
        if self.rg_rows >= self.cap_rows {
            let nr = self.rg_rows;
            if nr > 0 {
                unsafe {
                    if self.use_day_dict {
                        self.day_ids.set_len(nr);
                    } else {
                        self.col_day.set_len(nr);
                    }
                    self.col_sessions.set_len(nr);
                    self.col_events.set_len(nr);
                    self.rev_ids.set_len(nr);
                    self.def_bits.set_len((nr + 7) >> 3);
                }
            }
            let target = if self.cap_rows == 0 {
                self.init_cap_rows
            } else {
                MAX_RG_ROWS
            };
            if self.dict_uid.capacity() == 0 {
                self.dict_uid
                    .reserve((target * 4 + 64).min(MAX_VAR_BYTES + 64).max(65_536));
                self.uid_runs.reserve(target);
                self.dict_rev.reserve(4096);
                if self.use_cat_dict {
                    self.top_cat_ids.reserve(target);
                } else {
                    self.top_cat_vals
                        .reserve((target * 4 + 64).min(MAX_VAR_BYTES + 64).max(65_536));
                }
            }
            if self.use_day_dict {
                self.day_ids.reserve(target - nr);
            } else {
                self.col_day.reserve(target - nr);
            }
            self.col_sessions.reserve(target - nr);
            self.col_events.reserve(target - nr);
            self.rev_ids.reserve(target - nr);
            let cur_def = (nr + 7) >> 3;
            let req_def = ((target + 7) >> 3) + 1;
            if req_def > cur_def {
                self.def_bits.reserve(req_def - cur_def);
                unsafe {
                    std::ptr::write_bytes(
                        self.def_bits.as_mut_ptr().add(cur_def),
                        0,
                        req_def - cur_def,
                    );
                }
            }
            self.cap_rows = target;
        }
    }

    #[inline(always)]
    pub fn push_user_day(
        &mut self,
        uid_bytes: &[u8],
        is_same_user: bool,
        day: i32,
        sessions: i32,
        events: i32,
        revenue: i64,
        top_cat_idx: Option<usize>,
    ) {
        if self.rg_rows >= self.cap_rows
            || self.dict_uid.len() >= MAX_VAR_BYTES
            || self.top_cat_vals.len() >= MAX_VAR_BYTES
            || self.dict_rev.len() >= 65_535
        {
            self.ensure_cap_or_flush();
        }
        if is_same_user && !self.uid_runs.is_empty() {
            *unsafe { self.uid_runs.last_mut().unwrap_unchecked() } += 1;
        } else {
            let ulen = uid_bytes.len();
            let old_len = self.dict_uid.len();
            if ulen <= 12 && old_len + 16 <= self.dict_uid.capacity() {
                unsafe {
                    let dst = self.dict_uid.as_mut_ptr().add(old_len);
                    (dst as *mut u32).write_unaligned((ulen as u32).to_le());
                    std::ptr::copy_nonoverlapping(uid_bytes.as_ptr(), dst.add(4), ulen);
                    self.dict_uid.set_len(old_len + 4 + ulen);
                }
            } else {
                self.dict_uid.extend_from_slice(&(ulen as u32).to_le_bytes());
                self.dict_uid.extend_from_slice(uid_bytes);
            }
            self.uid_runs.push(1);
        }
        self.push_tail_fields(day, sessions, events, revenue, top_cat_idx);
    }

    #[inline(always)]
    pub fn push_user_day_zero_rev(
        &mut self,
        uid_bytes: &[u8],
        is_same_user: bool,
        day: i32,
        sessions: i32,
        events: i32,
    ) {
        if self.rg_rows >= self.cap_rows || self.dict_uid.len() >= MAX_VAR_BYTES {
            self.ensure_cap_or_flush();
        }
        if is_same_user && !self.uid_runs.is_empty() {
            *unsafe { self.uid_runs.last_mut().unwrap_unchecked() } += 1;
        } else {
            let ulen = uid_bytes.len();
            let old_len = self.dict_uid.len();
            if ulen <= 12 && old_len + 16 <= self.dict_uid.capacity() {
                unsafe {
                    let dst = self.dict_uid.as_mut_ptr().add(old_len);
                    (dst as *mut u32).write_unaligned((ulen as u32).to_le());
                    std::ptr::copy_nonoverlapping(uid_bytes.as_ptr(), dst.add(4), ulen);
                    self.dict_uid.set_len(old_len + 4 + ulen);
                }
            } else {
                self.dict_uid.extend_from_slice(&(ulen as u32).to_le_bytes());
                self.dict_uid.extend_from_slice(uid_bytes);
            }
            self.uid_runs.push(1);
        }
        let idx = self.rg_rows;
        unsafe {
            if self.use_day_dict {
                debug_assert!(
                    day >= self.min_day && ((day - self.min_day) as usize) < self.day_span
                );
                *self.day_ids.as_mut_ptr().add(idx) = (day - self.min_day) as u8;
            } else {
                *self.col_day.as_mut_ptr().add(idx) = day;
            }
            *self.col_sessions.as_mut_ptr().add(idx) = sessions;
            *self.col_events.as_mut_ptr().add(idx) = events;
            *self.rev_ids.as_mut_ptr().add(idx) = 0;
        }
        self.rg_rows = idx + 1;
    }

    #[inline(always)]
    fn push_tail_fields(
        &mut self,
        day: i32,
        sessions: i32,
        events: i32,
        revenue: i64,
        top_cat_idx: Option<usize>,
    ) {
        let idx = self.rg_rows;
        unsafe {
            if self.use_day_dict {
                debug_assert!(
                    day >= self.min_day && ((day - self.min_day) as usize) < self.day_span
                );
                *self.day_ids.as_mut_ptr().add(idx) = (day - self.min_day) as u8;
            } else {
                *self.col_day.as_mut_ptr().add(idx) = day;
            }
            *self.col_sessions.as_mut_ptr().add(idx) = sessions;
            *self.col_events.as_mut_ptr().add(idx) = events;
            if revenue == 0 {
                *self.rev_ids.as_mut_ptr().add(idx) = 0;
            } else {
                let rid = self.dict_rev.len() as u16;
                self.dict_rev.push(revenue);
                *self.rev_ids.as_mut_ptr().add(idx) = rid;
            }

            if let Some(cid) = top_cat_idx {
                *self.def_bits.as_mut_ptr().add(idx >> 3) |= 1u8 << (idx & 7);
                if self.use_cat_dict {
                    self.top_cat_ids.push(cid as u8);
                } else {
                    let cat = self.encoded_cats.get_unchecked(cid);
                    self.top_cat_vals.extend_from_slice(cat);
                }
            }
        }
        self.rg_rows = idx + 1;
    }

    #[cold]
    pub fn flush_rg(&mut self) {
        if self.rg_rows == 0 {
            return;
        }
        let nr_usize = self.rg_rows;
        unsafe {
            if self.use_day_dict {
                self.day_ids.set_len(nr_usize);
            } else {
                self.col_day.set_len(nr_usize);
            }
            self.col_sessions.set_len(nr_usize);
            self.col_events.set_len(nr_usize);
            self.rev_ids.set_len(nr_usize);
            self.def_bits.set_len((nr_usize + 7) >> 3);
        }
        let is_first = self.file.is_none();
        let f = self
            .file
            .get_or_insert_with(|| File::create(&self.path).expect("create user_daily parquet"));
        if is_first {
            self.file_pos = 4;
        }

        let nr = self.rg_rows as i32;
        encode_uid_rle_runs(&self.uid_runs, &mut self.uid_rle_buf);
        if self.use_day_dict {
            encode_u8_bitpacked(&self.day_ids, self.day_bit_width, &mut self.day_rle_buf);
        }

        // Col 2: sessions (RLE_DICTIONARY when 1 <= s <= 255)
        let mut min_s = i32::MAX;
        let mut max_s = i32::MIN;
        for &s in &self.col_sessions {
            if s < min_s {
                min_s = s;
            }
            if s > max_s {
                max_s = s;
            }
        }
        let use_sess_dict = min_s >= 1 && max_s <= 255;
        let sess_dict_len = if use_sess_dict { max_s as usize } else { 0 };
        if use_sess_dict {
            self.sess_dict_bytes.clear();
            self.sess_dict_bytes.reserve(sess_dict_len * 4);
            for v in 1..=max_s {
                self.sess_dict_bytes.extend_from_slice(&v.to_le_bytes());
            }
            self.sess_ids.clear();
            self.sess_ids.reserve(nr_usize);
            unsafe {
                let src = self.col_sessions.as_ptr();
                let dst = self.sess_ids.as_mut_ptr();
                for i in 0..nr_usize {
                    *dst.add(i) = (*src.add(i) - 1) as u8;
                }
                self.sess_ids.set_len(nr_usize);
            }
            let bw = if sess_dict_len <= 2 {
                1u8
            } else {
                (32 - ((sess_dict_len - 1) as u32).leading_zeros()) as u8
            };
            encode_u8_bitpacked(&self.sess_ids, bw, &mut self.sess_rle_buf);
        }

        encode_def_levels_bitwidth1(self.rg_rows, &self.def_bits, &mut self.rle_buf);
        let rle_len = self.rle_buf.len() as u32;
        let rle_len_le = rle_len.to_le_bytes();

        let emit_cat_dict = self.use_cat_dict && !self.top_cat_ids.is_empty();
        if emit_cat_dict {
            encode_u8_bitpacked(&self.top_cat_ids, self.cat_bit_width, &mut self.cat_rle_buf);
        }

        let p0_dict = self.dict_uid.as_slice();
        let p0_idx = self.uid_rle_buf.as_slice();
        let p1_plain = as_le_bytes(&self.col_day);
        let p1_data: &[u8] = if self.use_day_dict {
            &self.day_rle_buf
        } else {
            p1_plain
        };
        let p2_plain = as_le_bytes(&self.col_sessions);
        let p2_data: &[u8] = if use_sess_dict {
            &self.sess_rle_buf
        } else {
            p2_plain
        };
        let p3_data = as_le_bytes(&self.col_events);
        let rev_dict_len = self.dict_rev.len();
        let rev_bw = if rev_dict_len <= 2 {
            1u8
        } else {
            (32 - ((rev_dict_len - 1) as u32).leading_zeros()) as u8
        };
        encode_u16_rle_hybrid(&self.rev_ids, rev_bw, &mut self.rev_rle_buf);
        let p4_dict = as_le_bytes(&self.dict_rev);
        let p4_data = self.rev_rle_buf.as_slice();
        let p5_vals: &[u8] = if emit_cat_dict {
            &self.cat_rle_buf
        } else {
            &self.top_cat_vals
        };
        let p5_payload_len = (4 + self.rle_buf.len() + p5_vals.len()) as i32;

        self.hdr_buf.0.clear();
        let dh0 =
            append_dict_page_header(self.uid_runs.len() as i32, p0_dict.len() as i32, &mut self.hdr_buf);
        let h0 = append_data_page_header_enc(
            nr,
            p0_idx.len() as i32,
            ENC_RLE_DICTIONARY,
            &mut self.hdr_buf,
        );
        let dh1 = if self.use_day_dict {
            append_dict_page_header(
                self.day_span as i32,
                self.dict_days_bytes.len() as i32,
                &mut self.hdr_buf,
            )
        } else {
            (0, 0)
        };
        let h1 = append_data_page_header_enc(
            nr,
            p1_data.len() as i32,
            if self.use_day_dict {
                ENC_RLE_DICTIONARY
            } else {
                ENC_PLAIN
            },
            &mut self.hdr_buf,
        );
        let dh2 = if use_sess_dict {
            append_dict_page_header(
                sess_dict_len as i32,
                self.sess_dict_bytes.len() as i32,
                &mut self.hdr_buf,
            )
        } else {
            (0, 0)
        };
        let h2 = append_data_page_header_enc(
            nr,
            p2_data.len() as i32,
            if use_sess_dict {
                ENC_RLE_DICTIONARY
            } else {
                ENC_PLAIN
            },
            &mut self.hdr_buf,
        );
        let h3 = append_data_page_header_enc(nr, p3_data.len() as i32, ENC_PLAIN, &mut self.hdr_buf);
        let dh4 =
            append_dict_page_header(rev_dict_len as i32, p4_dict.len() as i32, &mut self.hdr_buf);
        let h4 = append_data_page_header_enc(
            nr,
            p4_data.len() as i32,
            ENC_RLE_DICTIONARY,
            &mut self.hdr_buf,
        );
        let dh5 = if emit_cat_dict {
            append_dict_page_header(
                self.num_cats as i32,
                self.dict_cats_bytes.len() as i32,
                &mut self.hdr_buf,
            )
        } else {
            (0, 0)
        };
        let h5 = append_data_page_header_enc(
            nr,
            p5_payload_len,
            if emit_cat_dict {
                ENC_RLE_DICTIONARY
            } else {
                ENC_PLAIN
            },
            &mut self.hdr_buf,
        );

        let hb = self.hdr_buf.0.as_slice();
        let mut cols = Vec::with_capacity(6);
        let mut rg_bytes = 0i64;
        let mut iovs = Vec::with_capacity(32);
        if is_first {
            iovs.push(IoSlice::new(b"PAR1"));
        }

        // Col 0: user_id (dict page + RLE_DICTIONARY data page)
        let d0_off = self.file_pos;
        let dh0_s = &hb[dh0.0..dh0.1];
        let h0_s = &hb[h0.0..h0.1];
        let c0_dict_sz = (dh0_s.len() + p0_dict.len()) as i64;
        let p0_off = d0_off + c0_dict_sz;
        let c0_total = c0_dict_sz + (h0_s.len() + p0_idx.len()) as i64;
        self.file_pos += c0_total;
        rg_bytes += c0_total;
        cols.push(ColumnMetaRecord {
            col_idx: 0,
            dict_page_offset: Some(d0_off),
            data_page_offset: p0_off,
            total_size: c0_total,
            num_values: nr as i64,
            optional: false,
        });
        iovs.push(IoSlice::new(dh0_s));
        iovs.push(IoSlice::new(p0_dict));
        iovs.push(IoSlice::new(h0_s));
        iovs.push(IoSlice::new(p0_idx));

        // Cols 1..=4: day, sessions (dict or PLAIN), events, revenue_cents (zero-copy PLAIN)
        for (col_idx, use_dict, dh_rng, dict_bytes, h_rng, data_bytes) in [
            (
                1usize,
                self.use_day_dict,
                dh1,
                self.dict_days_bytes.as_slice(),
                h1,
                p1_data,
            ),
            (
                2usize,
                use_sess_dict,
                dh2,
                self.sess_dict_bytes.as_slice(),
                h2,
                p2_data,
            ),
            (3usize, false, (0, 0), &[][..], h3, p3_data),
            (4usize, true, dh4, p4_dict, h4, p4_data),
        ] {
            let dh_s = &hb[dh_rng.0..dh_rng.1];
            let h_s = &hb[h_rng.0..h_rng.1];
            let (d_off_opt, p_off, tot) = if use_dict {
                let d_off = self.file_pos;
                let d_sz = (dh_s.len() + dict_bytes.len()) as i64;
                let p_off = d_off + d_sz;
                let total = d_sz + (h_s.len() + data_bytes.len()) as i64;
                iovs.push(IoSlice::new(dh_s));
                iovs.push(IoSlice::new(dict_bytes));
                (Some(d_off), p_off, total)
            } else {
                let p_off = self.file_pos;
                let total = (h_s.len() + data_bytes.len()) as i64;
                (None, p_off, total)
            };
            iovs.push(IoSlice::new(h_s));
            iovs.push(IoSlice::new(data_bytes));
            self.file_pos += tot;
            rg_bytes += tot;
            cols.push(ColumnMetaRecord {
                col_idx,
                dict_page_offset: d_off_opt,
                data_page_offset: p_off,
                total_size: tot,
                num_values: nr as i64,
                optional: false,
            });
        }

        // Col 5: top_category (dict + RLE_DICTIONARY if non-empty & <= 256 cats, else PLAIN)
        let dh5_s = &hb[dh5.0..dh5.1];
        let h5_s = &hb[h5.0..h5.1];
        let (d5_off, p5_off, c5_total) = if emit_cat_dict {
            let d_off = self.file_pos;
            let d_sz = (dh5_s.len() + self.dict_cats_bytes.len()) as i64;
            let p_off = d_off + d_sz;
            let tot = d_sz + (h5_s.len() + p5_payload_len as usize) as i64;
            iovs.push(IoSlice::new(dh5_s));
            iovs.push(IoSlice::new(&self.dict_cats_bytes));
            (Some(d_off), p_off, tot)
        } else {
            let p_off = self.file_pos;
            let tot = (h5_s.len() + p5_payload_len as usize) as i64;
            (None, p_off, tot)
        };
        self.file_pos += c5_total;
        rg_bytes += c5_total;
        cols.push(ColumnMetaRecord {
            col_idx: 5,
            dict_page_offset: d5_off,
            data_page_offset: p5_off,
            total_size: c5_total,
            num_values: nr as i64,
            optional: true,
        });
        iovs.push(IoSlice::new(h5_s));
        iovs.push(IoSlice::new(&rle_len_le));
        iovs.push(IoSlice::new(&self.rle_buf));
        iovs.push(IoSlice::new(p5_vals));
        write_all_iovec(f, &mut iovs);

        self.rgs.push(RowGroupRecord {
            cols,
            total_byte_size: rg_bytes,
            num_rows: nr as i64,
        });
        self.total_rows += nr as i64;
        self.rg_rows = 0;
        self.dict_uid.clear();
        self.uid_runs.clear();
        self.col_day.clear();
        self.day_ids.clear();
        self.col_sessions.clear();
        self.col_events.clear();
        self.rev_ids.clear();
        self.dict_rev.clear();
        self.dict_rev.push(0);
        self.def_bits.clear();
        let zero_bytes = ((self.cap_rows + 7) >> 3) + 1;
        if self.def_bits.capacity() >= zero_bytes {
            unsafe {
                std::ptr::write_bytes(self.def_bits.as_mut_ptr(), 0, zero_bytes);
            }
        }
        self.top_cat_ids.clear();
        self.top_cat_vals.clear();
    }

    pub fn close(mut self) -> bool {
        self.flush_rg();
        let Some(mut f) = self.file else {
            return false;
        };
        let mut meta = encode_file_metadata(&USER_DAILY_COLS, self.total_rows, &self.rgs);
        let meta_len = meta.len() as u32;
        meta.extend_from_slice(&meta_len.to_le_bytes());
        meta.extend_from_slice(b"PAR1");
        f.write_all(&meta).expect("write user_daily footer");
        true
    }
}

pub fn write_empty_user_daily(path: &Path) {
    let f = File::create(path).expect("create empty user_daily parquet");
    let mut w = BufWriter::new(f);
    w.write_all(b"PAR1").expect("write PAR1");
    let meta = encode_file_metadata(&USER_DAILY_COLS, 0, &[]);
    let meta_len = meta.len() as u32;
    w.write_all(&meta).expect("write empty metadata");
    w.write_all(&meta_len.to_le_bytes())
        .expect("write empty metadata len");
    w.write_all(b"PAR1").expect("write empty footer PAR1");
    w.flush().expect("flush empty user_daily");
}

pub fn write_campaign_daily_parquet(
    path: &Path,
    ids: &[&str],
    channels: &[&str],
    days: &[i32],
    sessions: &[i32],
    purchasing: &[i32],
    revenues: &[i64],
    assisted: &[i32],
) {
    let f = File::create(path).expect("create campaign_daily parquet");
    let mut w = BufWriter::with_capacity(256 * 1024, f);
    w.write_all(b"PAR1").expect("write cd PAR1");
    let mut file_pos = 4i64;
    let total_rows = days.len();
    let mut rgs = Vec::new();
    let mut hdr_buf = ThriftBuf::new();

    if total_rows > 0 {
        let mut encoded_ids: Vec<u8> = Vec::with_capacity(total_rows * 12);
        for &s in ids {
            let b = s.as_bytes();
            encoded_ids.extend_from_slice(&(b.len() as u32).to_le_bytes());
            encoded_ids.extend_from_slice(b);
        }
        let mut encoded_ch: Vec<u8> = Vec::with_capacity(total_rows * 12);
        for &s in channels {
            let b = s.as_bytes();
            encoded_ch.extend_from_slice(&(b.len() as u32).to_le_bytes());
            encoded_ch.extend_from_slice(b);
        }

        let nr = total_rows as i32;
        let mut cols = Vec::with_capacity(7);
        let mut rg_bytes = 0i64;

        for (col_idx, payload) in [
            encoded_ids.as_slice(),
            encoded_ch.as_slice(),
            as_le_bytes(days),
            as_le_bytes(sessions),
            as_le_bytes(purchasing),
            as_le_bytes(revenues),
            as_le_bytes(assisted),
        ]
        .into_iter()
        .enumerate()
        {
            let cm = write_required_column(&mut w, &mut file_pos, &mut hdr_buf, col_idx, nr, payload);
            rg_bytes += cm.total_size;
            cols.push(cm);
        }

        rgs.push(RowGroupRecord {
            cols,
            total_byte_size: rg_bytes,
            num_rows: nr as i64,
        });
    }

    let meta = encode_file_metadata(&CAMPAIGN_DAILY_COLS, total_rows as i64, &rgs);
    let meta_len = meta.len() as u32;
    w.write_all(&meta).expect("write cd metadata");
    w.write_all(&meta_len.to_le_bytes())
        .expect("write cd metadata len");
    w.write_all(b"PAR1").expect("write cd footer PAR1");
    w.flush().expect("flush cd parquet");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_empty_all_null_and_mixed_user_daily() {
        let dir = std::env::temp_dir().join("shopping_pq_writer_test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let p_empty = dir.join("empty.parquet");
        write_empty_user_daily(&p_empty);
        let b_empty = std::fs::read(&p_empty).unwrap();
        assert!(b_empty.starts_with(b"PAR1"));
        assert!(b_empty.ends_with(b"PAR1"));

        let cats = vec!["apparel".to_string(), "books".to_string(), "unknown".to_string()];
        let p_mix = dir.join("mixed.parquet");
        let mut wr = UserDailyParquetWriter::new(p_mix.clone(), &cats, 21_000, 14, 4096);
        // Write 1025 rows spanning multiple RLE bit-packed runs (> 504 values), multi-day runs, and flush_rg mid-user
        for i in 0..1025usize {
            let uid = format!("u_{}", i / 3);
            let is_same = i % 3 != 0;
            let cat = if i % 3 == 0 { None } else { Some(i % cats.len()) };
            wr.push_user_day(uid.as_bytes(), is_same, 21_000 + (i as i32 % 14), 1, 5, 1000, cat);
            if i == 511 {
                wr.flush_rg(); // force RG split in the middle of a user's consecutive days!
            }
        }
        assert!(wr.close());
        let b_mix = std::fs::read(&p_mix).unwrap();
        assert!(b_mix.starts_with(b"PAR1"));
        assert!(b_mix.ends_with(b"PAR1"));

        // 1-user RG, day_span = 1 (bw=1), + all-null top_category
        let p_null = dir.join("all_null.parquet");
        let mut wr_null = UserDailyParquetWriter::new(p_null.clone(), &cats, 21_000, 1, 4096);
        for d in 0..10i32 {
            wr_null.push_user_day(b"solo", d > 0, 21_000, 1, 1, 0, None);
        }
        assert!(wr_null.close());
        let b_null = std::fs::read(&p_null).unwrap();
        assert!(b_null.starts_with(b"PAR1"));
        assert!(b_null.ends_with(b"PAR1"));

        // day_span = 256 (bw=8) + All-non-null top_category with > 64 categories (bw=7)
        let many_cats: Vec<String> = (0..100).map(|k| format!("cat_{k:03}")).collect();
        let p_nonnull = dir.join("all_nonnull_256.parquet");
        let mut wr_nn = UserDailyParquetWriter::new(p_nonnull.clone(), &many_cats, 21_000, 256, 4096);
        for i in 0..256usize {
            wr_nn.push_user_day(
                b"u0",
                i > 0,
                21_000 + (i as i32),
                2,
                4,
                500,
                Some(i % many_cats.len()),
            );
        }
        assert!(wr_nn.close());
        let b_nn = std::fs::read(&p_nonnull).unwrap();
        assert!(b_nn.starts_with(b"PAR1"));
        assert!(b_nn.ends_with(b"PAR1"));

        // Wide day_span > 256 (falls back cleanly to PLAIN INT32 for day)
        let p_wide = dir.join("wide_days.parquet");
        let mut wr_wide = UserDailyParquetWriter::new(p_wide.clone(), &cats, 21_000, 300, 4096);
        for i in 0..300i32 {
            wr_wide.push_user_day(b"w0", i > 0, 21_000 + i, 1, 2, 0, None);
        }
        assert!(wr_wide.close());
        let b_wide = std::fs::read(&p_wide).unwrap();
        assert!(b_wide.starts_with(b"PAR1"));
        assert!(b_wide.ends_with(b"PAR1"));

        let p_cd = dir.join("cd.parquet");
        write_campaign_daily_parquet(
            &p_cd,
            &["c_1", "organic"],
            &["social", "organic"],
            &[21_000, 21_000],
            &[5, 2],
            &[2, 0],
            &[500, 0],
            &[6, 0],
        );
        let b_cd = std::fs::read(&p_cd).unwrap();
        assert!(b_cd.starts_with(b"PAR1"));
        assert!(b_cd.ends_with(b"PAR1"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn user_daily_sessions_dict_and_plain_fallback() {
        let dir = std::env::temp_dir().join("shopping_pq_fallback_test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let cats = vec!["apparel".to_string()];

        // 1) Low-cardinality sessions (<= 255) uses RLE_DICTIONARY; events & revenue_cents are PLAIN
        let p_dict = dir.join("sessions_dict.parquet");
        let mut wr_d = UserDailyParquetWriter::new(p_dict.clone(), &cats, 21_000, 14, 4096);
        for i in 0..512i32 {
            let uid = format!("d_{:05}", i);
            wr_d.push_user_day(
                uid.as_bytes(),
                false,
                21_000 + (i % 14),
                1 + (i % 12),
                1 + i * 3,
                if i % 3 == 0 { 0 } else { (i as i64) * 100 + 7 },
                if i % 5 == 0 { Some(0) } else { None },
            );
        }
        assert!(wr_d.close());
        let b_d = std::fs::read(&p_dict).unwrap();
        assert!(b_d.starts_with(b"PAR1"));
        assert!(b_d.ends_with(b"PAR1"));

        // 2) sessions > 255 triggers PLAIN fallback cleanly alongside large events & revenue_cents
        let p_fb = dir.join("fallback.parquet");
        let mut wr = UserDailyParquetWriter::new(p_fb.clone(), &cats, 21_000, 14, 4096);
        for i in 0..35_000i32 {
            let uid = format!("u_{:05}", i);
            wr.push_user_day(
                uid.as_bytes(),
                false,
                21_000 + (i % 14),
                1 + (i % 300),
                1 + i,
                if i % 3 == 0 { 0 } else { (i as i64) * 100 + 7 },
                if i % 5 == 0 { Some(0) } else { None },
            );
        }
        assert!(wr.close());
        let b_fb = std::fs::read(&p_fb).unwrap();
        assert!(b_fb.starts_with(b"PAR1"));
        assert!(b_fb.ends_with(b"PAR1"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn user_daily_rev_rle_dict_all_zero_dups_and_split() {
        let dir = std::env::temp_dir().join("shopping_pq_rev_rle_test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let cats = vec!["apparel".to_string(), "books".to_string()];

        // 1) All-zero revenues (only dict[0] = 0 -> bw=1 RLE run)
        let p_z = dir.join("all_zero.parquet");
        let mut wr_z = UserDailyParquetWriter::new(p_z.clone(), &cats, 21_000, 14, 4096);
        for i in 0..1000i32 {
            let uid = format!("z_{i:05}");
            wr_z.push_user_day(uid.as_bytes(), false, 21_000 + (i % 14), 1, 2, 0, None);
        }
        assert!(wr_z.close());
        let b_z = std::fs::read(&p_z).unwrap();
        assert!(b_z.starts_with(b"PAR1") && b_z.ends_with(b"PAR1"));

        // 2) > 65,535 non-zero revenues triggering automatic dict_rev RG split + alternating zeros & non-zeros
        let p_sp = dir.join("rev_split.parquet");
        let mut wr_s = UserDailyParquetWriter::new(p_sp.clone(), &cats, 21_000, 14, 4096);
        for i in 0..70_000i32 {
            let uid = format!("s_{i:06}");
            let rev = if i % 9 == 0 {
                (100 + (i % 50)) as i64
            } else if i < 66_000 {
                (1000 + i) as i64
            } else {
                0
            };
            wr_s.push_user_day(
                uid.as_bytes(),
                false,
                21_000 + (i % 14),
                1,
                3,
                rev,
                if rev > 0 { Some((i as usize) % cats.len()) } else { None },
            );
        }
        assert!(wr_s.close());
        let b_sp = std::fs::read(&p_sp).unwrap();
        assert!(b_sp.starts_with(b"PAR1") && b_sp.ends_with(b"PAR1"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn user_daily_zero_rev_and_non_mult8_growth() {
        let dir = std::env::temp_dir().join("shopping_pq_zero_rev_growth_test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let cats = vec!["apparel".to_string(), "books".to_string()];

        // Phase 1: est_rows = 4099 (non-multiple of 8), mix push_user_day_zero_rev and push_user_day,
        // flush_rg() at non-multiples of 8 (505 and 4101) and grow beyond 4099 -> MAX_RG_ROWS
        let p = dir.join("zrev_growth.parquet");
        let mut wr = UserDailyParquetWriter::new(p.clone(), &cats, 21_000, 14, 4099);
        for i in 0..5_000i32 {
            let uid = format!("zg_{:05}", i / 2);
            let same = (i & 1) == 1 && i != 506 && i != 4102;
            if i % 4 == 0 {
                wr.push_user_day(
                    uid.as_bytes(),
                    same,
                    21_000 + (i % 14),
                    1 + (i % 5),
                    2 + (i % 7),
                    100 + (i as i64),
                    if (i & 8) == 0 {
                        Some((i as usize) % cats.len())
                    } else {
                        None
                    },
                );
            } else {
                wr.push_user_day_zero_rev(
                    uid.as_bytes(),
                    same,
                    21_000 + (i % 14),
                    1 + (i % 5),
                    2 + (i % 7),
                );
            }
            if i == 505 || i == 4101 {
                wr.flush_rg();
            }
        }
        assert!(wr.close());
        let bytes = std::fs::read(&p).unwrap();
        assert!(bytes.starts_with(b"PAR1") && bytes.ends_with(b"PAR1"));
        assert!(bytes.len() > 1000);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
