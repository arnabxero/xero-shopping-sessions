//! A minimal Parquet writer for the two output tables: one row group, one
//! data page (v1) per column, PLAIN values, no compression, no statistics.
//! Metadata is Thrift compact protocol, written by hand.

use std::io::Write;
use std::path::Path;

/// Column values.
pub enum Col<'a> {
    /// Required strings: concatenated bytes and end offsets.
    Str(&'a [u8], &'a [usize]),
    /// Optional strings: bytes and end offsets of the present values, and one
    /// definition level per row (1 present, 0 null).
    OptStr(&'a [u8], &'a [usize], &'a [i16]),
    /// Required DATE (days since the epoch).
    Date(&'a [i32]),
    I64(&'a [i64]),
}

const T_BYTE_ARRAY: i32 = 6;
const T_INT32: i32 = 1;
const T_INT64: i32 = 2;

/// Thrift compact protocol writer.
struct Thrift {
    out: Vec<u8>,
    last: Vec<i16>,
}

impl Thrift {
    fn new() -> Self {
        Thrift { out: Vec::new(), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.out.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.out.push(v as u8);
    }
    fn zigzag(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let delta = id - *self.last.last().unwrap();
        if delta > 0 && delta <= 15 {
            self.out.push(((delta as u8) << 4) | ty);
        } else {
            self.out.push(ty);
            self.zigzag(id as i64);
        }
        *self.last.last_mut().unwrap() = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, 5);
        self.zigzag(v as i64);
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, 6);
        self.zigzag(v);
    }
    fn bin(&mut self, id: i16, v: &[u8]) {
        self.field(id, 8);
        self.varint(v.len() as u64);
        self.out.extend_from_slice(v);
    }
    fn begin_struct(&mut self, id: i16) {
        self.field(id, 12);
        self.last.push(0);
    }
    fn end_struct(&mut self) {
        self.out.push(0);
        self.last.pop();
    }
    fn list_header(&mut self, id: i16, len: usize, elem: u8) {
        self.field(id, 9);
        if len < 15 {
            self.out.push(((len as u8) << 4) | elem);
        } else {
            self.out.push(0xF0 | elem);
            self.varint(len as u64);
        }
    }
    /// A struct element of a list (no field header).
    fn begin_elem(&mut self) {
        self.last.push(0);
    }
}

struct Leaf<'a> {
    name: &'a str,
    ty: i32,
    optional: bool,
    /// converted type and logical type field id (UTF8/STRING or DATE)
    logical: Option<(i32, i16)>,
}

/// Write `cols` (in schema order) as a Parquet file.
pub fn write(path: &Path, names: &[&str], cols: &[Col], rows: usize) -> std::io::Result<()> {
    let mut leaves = Vec::new();
    for (name, c) in names.iter().zip(cols) {
        leaves.push(match c {
            Col::Str(..) => Leaf { name, ty: T_BYTE_ARRAY, optional: false, logical: Some((0, 1)) },
            Col::OptStr(..) => Leaf { name, ty: T_BYTE_ARRAY, optional: true, logical: Some((0, 1)) },
            Col::Date(..) => Leaf { name, ty: T_INT32, optional: false, logical: Some((6, 6)) },
            Col::I64(..) => Leaf { name, ty: T_INT64, optional: false, logical: None },
        });
    }
    let mut file: Vec<u8> = Vec::with_capacity(64 + rows * 64);
    file.extend_from_slice(b"PAR1");
    // (data_page_offset, total size) per column
    let mut chunks: Vec<(i64, i64)> = Vec::new();
    let mut page = Vec::new();
    for c in cols {
        page.clear();
        match c {
            Col::Str(buf, ends) => {
                let mut s = 0;
                for &e in ends.iter() {
                    page.extend_from_slice(&((e - s) as u32).to_le_bytes());
                    page.extend_from_slice(&buf[s..e]);
                    s = e;
                }
            }
            Col::OptStr(buf, ends, present) => {
                // Definition levels: RLE/bit-packed hybrid, one bit-packed run of
                // width 1, preceded by its byte length (data page v1).
                let groups = present.len().div_ceil(8);
                let mut levels = Vec::with_capacity(groups + 10);
                if groups > 0 {
                    let mut h = ((groups as u64) << 1) | 1;
                    while h >= 0x80 {
                        levels.push((h as u8) | 0x80);
                        h >>= 7;
                    }
                    levels.push(h as u8);
                    for g in 0..groups {
                        let mut b = 0u8;
                        for k in 0..8 {
                            if present.get(g * 8 + k).map_or(false, |&d| d != 0) {
                                b |= 1 << k;
                            }
                        }
                        levels.push(b);
                    }
                }
                page.extend_from_slice(&(levels.len() as u32).to_le_bytes());
                page.extend_from_slice(&levels);
                let mut s = 0;
                for &e in ends.iter() {
                    page.extend_from_slice(&((e - s) as u32).to_le_bytes());
                    page.extend_from_slice(&buf[s..e]);
                    s = e;
                }
            }
            Col::Date(v) => {
                for &x in v.iter() {
                    page.extend_from_slice(&x.to_le_bytes());
                }
            }
            Col::I64(v) => {
                for &x in v.iter() {
                    page.extend_from_slice(&x.to_le_bytes());
                }
            }
        }
        let mut t = Thrift::new();
        t.i32(1, 0); // DATA_PAGE
        t.i32(2, page.len() as i32);
        t.i32(3, page.len() as i32);
        t.begin_struct(5);
        t.i32(1, rows as i32);
        t.i32(2, 0); // PLAIN
        t.i32(3, 3); // RLE
        t.i32(4, 3); // RLE
        t.end_struct();
        t.out.push(0);
        let offset = file.len() as i64;
        file.extend_from_slice(&t.out);
        file.extend_from_slice(&page);
        chunks.push((offset, (t.out.len() + page.len()) as i64));
    }
    // FileMetaData
    let mut t = Thrift::new();
    t.i32(1, 1);
    t.list_header(2, leaves.len() + 1, 12);
    t.begin_elem();
    t.bin(4, b"schema");
    t.i32(5, leaves.len() as i32);
    t.end_struct();
    for l in &leaves {
        t.begin_elem();
        t.i32(1, l.ty);
        t.i32(3, if l.optional { 1 } else { 0 });
        t.bin(4, l.name.as_bytes());
        if let Some((converted, logical_field)) = l.logical {
            t.i32(6, converted);
            t.begin_struct(10);
            t.begin_struct(logical_field);
            t.end_struct();
            t.end_struct();
        }
        t.end_struct();
    }
    t.i64(3, rows as i64);
    t.list_header(4, 1, 12);
    t.begin_elem();
    t.list_header(1, leaves.len(), 12);
    let mut total = 0i64;
    for (l, &(offset, size)) in leaves.iter().zip(&chunks) {
        total += size;
        t.begin_elem();
        t.i64(2, offset);
        t.begin_struct(3);
        t.i32(1, l.ty);
        t.list_header(2, 2, 5);
        t.zigzag(0); // PLAIN
        t.zigzag(3); // RLE
        t.list_header(3, 1, 8);
        t.varint(l.name.len() as u64);
        t.out.extend_from_slice(l.name.as_bytes());
        t.i32(4, 0); // UNCOMPRESSED
        t.i64(5, rows as i64);
        t.i64(6, size);
        t.i64(7, size);
        t.i64(9, offset);
        t.end_struct();
        t.end_struct();
    }
    t.i64(2, total);
    t.i64(3, rows as i64);
    t.end_struct();
    t.bin(6, b"shopping-sessions");
    t.out.push(0);
    file.extend_from_slice(&t.out);
    file.extend_from_slice(&(t.out.len() as u32).to_le_bytes());
    file.extend_from_slice(b"PAR1");
    let mut f = std::fs::File::create(path)?;
    f.write_all(&file)
}
