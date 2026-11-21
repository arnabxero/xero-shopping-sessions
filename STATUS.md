# Status

## Current best verified code: master d6850ef (same tree as clx-target2 690c20b)

All work happens on the benchmark server (Linux): `scripts/build_linux.sh` builds in the
pinned Rust image, `scripts/check_linux.sh` and `scripts/regress_linux.sh` verify.

Verification run for every code commit:
- Unit tests (`scripts/build_linux.sh test`), including randomized comparisons of the
  SIMD CSV scanner, SIMD ID packing, SWAR/cached timestamp parsing and every input
  read mode against simple reference versions.
- 36/36 fixtures, dev answer exact MATCH and 10/10 differential datasets (output identical
  to the Python reference `reference/reference.py`, seeds 2,3,4,5,6,7,9,10,13,42, 1k-2M
  events), in every input mode (SHOPPING_IO=auto|mmap|pread|direct), plus 10/10 with tiny
  partition capacities (SHOPPING_TEST_SMALLCAP=1, all overflow paths).
- Identical output to bd16bde on a 20M-event (seed 11) and a 60M-event (seed 21) set in
  every input mode.
- The image built from the Dockerfile passes 36/36 fixtures and the dev answer under the
  scorer flags (read-only root, uid 1000, no network, 8 CPUs, 24g, tmpfs /tmp); its output
  is also read back with pyarrow.

## What the scorer looks like (diagnostic builds diag-scorer / diag-scorer2, never score)

- Xeon @ 2.80 GHz (Cascade Lake: AVX-512 F/BW/VL/VNNI), 8 CPUs (affinity and cgroup
  800000/100000), host 32 vCPUs / 128 GB, kernel 7.0 (gcp), THP madvise.
- `/data` is **tmpfs** (inputs always in RAM); `/output` and `/scratch` are ext4.
- Dev set (1M events): ~50 ms from container exec to `main`, ~75 ms in the process.
- Consequence: inputs on tmpfs/ramfs are always memory-mapped (zero copy); the build
  targets Cascade Lake.

## Board history (practice set, ~20M events)

| Commit | Change | Score |
|---|---|---|
| 5d67334 | first Rust pipeline | 1.35 s |
| 55a21ed | per-worker scatter buffers | 1.32 s |
| 4868fc8 | SIMD line scanner, SWAR timestamps | 1.31 s |
| 7ec6ba9 | no I/O at setup, readahead, adaptive mmap/pread, early exit | 1.25 s |
| 0a7e68a | O_DIRECT for uncached files, overlapped parse, links in main queue | 1.22 s |
| 690c20b | tmpfs mapping, Cascade Lake build, dedup fingerprints, task order, Parquet writer | pending |

## Notes
- Server timing (EPYC 9554, 8 pinned cores, warm): ~0.5 s in-process for 20M; 60M ~2.1 s,
  peak RSS ~10 GB including 5.4 GB of file pages.
- Tried and dropped (no gain): block-structured scanner, AVX-512 mask compares, closure-free
  row iterator, write-combining scatter stores, smaller routing partition counts, PGO.
