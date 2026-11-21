# Shopping Sessions — Team-Xero

A single Rust binary that turns the raw `events/`, `clicks/` and `links/`
CSVs into the `user_daily` and `campaign_daily` Parquet tables defined in `rules.md`.
Correctness first (a Python reference implementation is the oracle), then speed.

## Pipeline

1. **Input.** Every CSV is split into ~4 MB nominal byte ranges without reading it.
   Each file is probed for page-cache residency (`preadv2(RWF_NOWAIT)`): cached files
   are memory-mapped (no copy); uncached files are read by the workers with
   `O_DIRECT` (one aligned read per range, buffered `pread` as fallback), with twice
   as many workers as CPUs so parsing overlaps the disk.
2. **Parse** (all CPUs, one task queue: links, then clicks, then events). A line is
   classified with one AVX2 window of 64/128 bytes (newline, comma and quote
   bitmasks), so fields come from a few `tzcnt`s; quotes are legal only around a
   whole field. IDs of up to 10 `[0-9A-Za-z_]` bytes are packed into exact,
   order-preserving 60-bit keys (SIMD); anything else is interned. Timestamps (F1,
   F2, F3) are decoded with SWAR code and a per-task date cache; every rule of
   `rules.md` is checked on the row, and the row goes to one of ~2k partitions by the
   hash of its `event_id`.
3. **Persons.** As soon as the link chunks are parsed, a union-find over the linked
   IDs (root = byte-wise smallest ID) runs on its own thread.
4. **Dedup and routing** (per event_id partition): an open-addressing table keeps the
   copy with the smallest `ingest_ts`; if that copy is valid, its user is mapped to
   its person (prefetched lookups) and the event goes to a partition by person.
5. **Sessions** (per person partition): group by person, sort by time, split at gaps
   over 30 minutes, credit each session to the latest click in the 7 days before its
   start (ties: smallest `click_id`), count assists with a sliding window per
   campaign, and aggregate per (person, day) and (campaign, day).
6. **Output**: Parquet written in parallel (`parquet` crate, plain encoding), then
   `_exit` without tearing down the big tables.

Memory stays well under the 24 GB limit (60M events: ~6 GB anonymous).

## Correctness

- `reference/reference.py`: a deliberately simple Python implementation written line
  by line from `rules.md`; it passes the 36 fixtures and matches the published dev
  answer exactly.
- Every change of the Rust code is checked against: unit tests (randomized
  comparisons of the SIMD scanner, ID packing, timestamp decoding and every read
  mode with simple versions), the 36 fixtures, the dev answer, 10 generated datasets
  compared with the Python reference (several seeds and sizes, also with tiny buffers
  to exercise every overflow path), identical output on 20M- and 60M-event sets in
  every read mode, and the Docker image run with the scorer's flags.

## Layout

- `src/` — the pipeline (`main.rs`), CSV dialect (`csv.rs`), keys (`keys.rs`),
  timestamps (`ts.rs`), input modes (`input.rs`), partition buffers (`scatter.rs`),
  hash map (`map.rs`), Parquet output (`out.rs`).
- `reference/` — the Python oracle.
- `fixtures/`, `scripts/` — the organizers' fixtures and test runners, plus our
  build/check/benchmark scripts.

Open-source components: Rust crates `memmap2`, `mimalloc`, `parquet`, `bytes`,
`libc` (pinned in `Cargo.lock`). MIT licensed.
