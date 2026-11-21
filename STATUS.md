# Status

## Best verified version: commit c563abe (see `git log` for later ones)

Correctness evidence for c563abe:
- Docker image (built from this repo's Dockerfile, linux/amd64) run with the scorer's flags
  (`--read-only`, `--user 1000:1000`, `--network none`, `--cpus 8`, `--memory 24g`,
  tmpfs /tmp 64m, /data read-only): 36/36 fixtures pass, dev set (seed 1, 1M events) exact MATCH
  with dev-answer.
- Native build of the same source: 36/36 fixtures, dev MATCH, and identical output to the Python
  reference (`reference/reference.py`, itself 36/36 + dev MATCH) on 10 generated datasets
  (seeds 2,3,4,5,6,7,9,10,13,42; 1k to 2M events; click-heavy and click-light).

Timing (local dev box, Docker Desktop/WSL2, shared and noisy; scorer will differ):
- 20M events (seed 11, 1.8 GB): best run 1.09 s inside the process, 1.64 s docker-run wall
  (container start/stop included). Busy-machine runs reach 2-2.5 s.
- 1M dev set: ~0.4 s inside the process.

How to check: `scripts/check_all.sh native` and `scripts/check_all.sh docker` (Git Bash).
