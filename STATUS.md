# Status

## Latest verified commit: see the newest "STATUS" or code commit on master

All work now happens on the benchmark server (Linux): `scripts/build_linux.sh` builds in
the pinned Rust image, `scripts/check_linux.sh` and `scripts/regress_linux.sh` verify.

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
  scorer flags (read-only root, uid 1000, no network, 8 CPUs, 24g, tmpfs /tmp).

## Board history (practice set, ~20M events)

| Commit | Change | Score |
|---|---|---|
| 5d67334 | first Rust pipeline | 1.35 s |
| 55a21ed | per-worker scatter buffers | 1.32 s |
| 4868fc8 | SIMD line scanner, SWAR timestamps | 1.31 s |
| 7ec6ba9 | no I/O at setup, readahead, adaptive mmap/pread, early exit | 1.25 s |
| 0a7e68a | O_DIRECT for uncached files, overlapped parse, links in main queue | 1.22 s |

## Notes
- Server timing (EPYC 9554, 8 pinned cores, warm): ~0.55 s in-process for 20M; 60M ~2.1 s,
  peak RSS ~10 GB including 5.4 GB of file pages.
- Branch diag-scorer (d3a18d9) is a diagnostic build that prints the scorer environment and
  fails the dev check on purpose; never score it.
