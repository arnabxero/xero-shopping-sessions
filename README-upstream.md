# Shopping Sessions — High-Performance Rust Engine

A zero-copy, SIMD-accelerated, sharded parallel Rust implementation with a native Parquet v1 writer for the Builders Buildathon **Shopping Sessions** track.

## Open-Source Dependencies

- `ahash` (0.8) — Fast hashing
- `hashbrown` (0.15) — SwissTable hash maps/tables (`raw-entry`)
- `memchr` (2.7) — SIMD-accelerated delimiter scanning
- `memmap2` (0.9) — Memory-mapped read-only CSV input

Using custom lock-free `std::thread` parallelism, carry-bit AVX-512 (`avx512vbmi2`+) / AVX2 + `pclmulqdq` fused `CsvRowSink` CSV scanning, SSSE3/SSE4.1 pair-parallel Base-37 + BMI2 `Pack64` key encoding, `HugeZeroed`/`HugeSlab` transparent-huge-page (`MADV_HUGEPAGE`) memory, non-temporal (`_mm256_stream_si256`) `NtSlabBuckets` and fused-pointer `DirectSlabBuckets`, a 2-pass lock-free DSU + Bloom-filtered linear-probing `Bucket6` `LinkRedirect` with branchless Bloom compaction, a 64-entry L1-resident `FastDateCache` + SWAR/SIMD parsers, cache-resident sorting-network + 5-bit MSD / 11-bit LSD radix Stage 4 sessionization with overlapped background cleanup, and a native zero-copy `write_vectored` Parquet v1 (`UNCOMPRESSED` + `RLE_DICTIONARY`/`PLAIN`) writer.

## Build & Run

```sh
docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .
```
