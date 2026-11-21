# Status

## Best verified version: da734a3

Correctness evidence for da734a3:
- Docker image built from this repo's Dockerfile (linux/amd64), run with the scorer's flags
  (`--read-only`, `--user 1000:1000`, `--network none`, `--cpus 8`, `--memory 24g`,
  tmpfs /tmp 64m, /data read-only): 36/36 fixtures, dev set (seed 1, 1M events) exact MATCH.
- Native build of the same source: 36/36 fixtures, dev MATCH, identical output to the Python
  reference (`reference/reference.py`, itself 36/36 + dev MATCH) on 10 generated datasets
  (seeds 2,3,4,5,6,7,9,10,13,42; 1k to 2M events), also with forced tiny partition capacities
  (`SHOPPING_TEST_SMALLCAP=1`) so every overflow path runs.
- Identical output to the previous verified version on a 20M-event set (seed 11) and a
  60M-event set (seed 21).

Timing (local dev box, Docker Desktop/WSL2, shared and noisy, so A/B runs alternate):
- 20M events: ~20% faster than the 71e4e14 build (practice board 1.35 s).
  Best local: ~0.92 s inside the process, ~1.5 s docker-run wall.
- 60M events (native Windows): ~3.0 s; memory well under 24 GB.

History: 71e4e14 (= old c563abe) scored 1.35 s on the practice board, rank 3.

How to check: `scripts/check_all.sh native` and `scripts/check_all.sh docker` (Git Bash).
