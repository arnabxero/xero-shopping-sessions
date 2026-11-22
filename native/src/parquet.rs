//! A minimal Parquet writer: uncompressed, PLAIN-encoded, one data page per
//! column chunk. Enough for the two output tables, and nothing to link.

use std::fs::File;
use std::io::{self, Write};

pub const INT32: i32 = 1;
pub const INT64: i32 = 2;
pub const BYTE_ARRAY: i32 = 6;
const UTF8: i32 = 0;
const DATE: i32 = 6;

#[derive(Clone, Copy)]
pub enum Kind {
    Int32,
    Int64,
    Date,
    Text,
}

#[derive(Clone, Copy)]
pub struct Column {
    pub name: &'static str,
    pub kind: Kind,
    pub nullable: bool,
}

impl Column {
    fn physical(&self) -> i32 {
        match self.kind {
            Kind::Int32 | Kind::Date => INT32,
            Kind::Int64 => INT64,
            Kind::Text => BYTE_ARRAY,
        }
    }
}

/// Thrift's compact protocol, as far as Parquet metadata needs it.
#[derive(Default)]
struct Thrift {
    bytes: Vec<u8>,
    last: i16,
    stack: Vec<i16>,
}

impl Thrift {
    fn varint(&mut self, mut value: u64) {
        while value >= 0x80 {
            self.bytes.push(value as u8 | 0x80);
            value >>= 7;
        }
        self.bytes.push(value as u8);
    }
    fn field(&mut self, id: i16, kind: u8) {
        let delta = id - self.last;
        if (1..=15).contains(&delta) {
            self.bytes.push((delta as u8) << 4 | kind);
        } else {
            self.bytes.push(kind);
            self.varint(((i64::from(id) << 1) ^ (i64::from(id) >> 63)) as u64);
        }
        self.last = id;
    }
    fn zigzag(&mut self, value: i64) {
        self.varint(((value << 1) ^ (value >> 63)) as u64);
    }
    fn i32(&mut self, id: i16, value: i32) {
        self.field(id, 5);
        self.zigzag(i64::from(value));
    }
    fn i64(&mut self, id: i16, value: i64) {
        self.field(id, 6);
        self.zigzag(value);
    }
    fn text(&mut self, value: &str) {
        self.varint(value.len() as u64);
        self.bytes.extend_from_slice(value.as_bytes());
    }
    fn string(&mut self, id: i16, value: &str) {
        self.field(id, 8);
        self.text(value);
    }
    fn list(&mut self, id: i16, kind: u8, len: usize) {
        self.field(id, 9);
        if len < 15 {
            self.bytes.push((len as u8) << 4 | kind);
        } else {
            self.bytes.push(0xF0 | kind);
            self.varint(len as u64);
        }
    }
    fn open(&mut self) {
        self.stack.push(self.last);
        self.last = 0;
    }
    fn open_field(&mut self, id: i16) {
        self.field(id, 12);
        self.open();
    }
    fn close(&mut self) {
        self.bytes.push(0);
        self.last = self.stack.pop().unwrap();
    }
}

struct Chunk {
    offset: u64,
    size: u64,
    values: u64,
}

/// One column's values for a row group, already PLAIN-encoded. `validity` is
/// for nullable columns: one bit per row, least significant first, set when the
/// row has a value; `data` then holds only the rows that do.
pub struct Values<'a> {
    pub data: &'a [u8],
    pub validity: Option<&'a [u8]>,
}

pub struct Writer {
    file: File,
    columns: Vec<Column>,
    offset: u64,
    groups: Vec<(Vec<Chunk>, u64)>,
    rows: u64,
    buffer: Vec<u8>,
}

impl Writer {
    pub fn create(path: &std::path::Path, columns: &[Column]) -> io::Result<Self> {
        let mut file = File::create(path)?;
        file.write_all(b"PAR1")?;
        Ok(Self { file, columns: columns.to_vec(), offset: 4, groups: Vec::new(), rows: 0, buffer: Vec::new() })
    }

    pub fn row_group(&mut self, rows: usize, values: &[Values]) -> io::Result<()> {
        if rows == 0 {
            return Ok(());
        }
        assert_eq!(values.len(), self.columns.len());
        self.buffer.clear();
        let mut chunks = Vec::new();
        let start = self.offset;
        for column in values {
            let mut levels = Vec::new();
            if let Some(validity) = column.validity {
                // Definition levels: one bit-packed run of the validity bits, with its byte length in front.
                let groups = rows.div_ceil(8);
                let mut run = Thrift::default();
                run.varint((groups as u64) << 1 | 1);
                run.bytes.extend_from_slice(&validity[..groups]);
                levels.extend_from_slice(&(run.bytes.len() as u32).to_le_bytes());
                levels.extend_from_slice(&run.bytes);
            }
            let size = levels.len() + column.data.len();
            let mut header = Thrift::default();
            header.i32(1, 0); // DATA_PAGE
            header.i32(2, size as i32);
            header.i32(3, size as i32);
            header.open_field(5);
            header.i32(1, rows as i32);
            header.i32(2, 0); // PLAIN
            header.i32(3, 3); // RLE
            header.i32(4, 3); // RLE
            header.close();
            header.bytes.push(0);
            let at = start + self.buffer.len() as u64;
            self.buffer.extend_from_slice(&header.bytes);
            self.buffer.extend_from_slice(&levels);
            self.buffer.extend_from_slice(column.data);
            chunks.push(Chunk { offset: at, size: (header.bytes.len() + size) as u64, values: rows as u64 });
        }
        self.file.write_all(&self.buffer)?;
        self.offset += self.buffer.len() as u64;
        self.groups.push((chunks, rows as u64));
        self.rows += rows as u64;
        Ok(())
    }

    pub fn finish(mut self) -> io::Result<()> {
        let mut meta = Thrift::default();
        meta.i32(1, 1);
        meta.list(2, 12, self.columns.len() + 1);
        meta.open();
        meta.string(4, "schema");
        meta.i32(5, self.columns.len() as i32);
        meta.close();
        for column in &self.columns {
            meta.open();
            meta.i32(1, column.physical());
            meta.i32(3, i32::from(column.nullable));
            meta.string(4, column.name);
            match column.kind {
                Kind::Text => meta.i32(6, UTF8),
                Kind::Date => meta.i32(6, DATE),
                _ => {}
            }
            meta.close();
        }
        meta.i64(3, self.rows as i64);
        meta.list(4, 12, self.groups.len());
        for (chunks, rows) in &self.groups {
            meta.open();
            meta.list(1, 12, chunks.len());
            for (chunk, column) in chunks.iter().zip(&self.columns) {
                meta.open();
                meta.i64(2, chunk.offset as i64);
                meta.open_field(3);
                meta.i32(1, column.physical());
                meta.list(2, 5, 2);
                meta.zigzag(0);
                meta.zigzag(3);
                meta.list(3, 8, 1);
                meta.text(column.name);
                meta.i32(4, 0); // UNCOMPRESSED
                meta.i64(5, chunk.values as i64);
                meta.i64(6, chunk.size as i64);
                meta.i64(7, chunk.size as i64);
                meta.i64(9, chunk.offset as i64);
                meta.close();
                meta.close();
            }
            meta.i64(2, chunks.iter().map(|chunk| chunk.size).sum::<u64>() as i64);
            meta.i64(3, *rows as i64);
            meta.close();
        }
        meta.string(6, "shopping-sessions (Team Inventors)");
        meta.bytes.push(0);
        let len = meta.bytes.len() as u32;
        meta.bytes.extend_from_slice(&len.to_le_bytes());
        meta.bytes.extend_from_slice(b"PAR1");
        self.file.write_all(&meta.bytes)
    }
}
