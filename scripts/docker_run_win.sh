#!/bin/sh
# Windows (Git Bash) variant of docker_run.sh: docker_run_win.sh DATA_DIR OUTPUT_DIR
# Same scorer flags; host paths converted for Docker Desktop.
set -eu
IMAGE="${IMAGE:-shopping-sessions:latest}"
CPUS="${CPUS:-8}"
MEM="${MEM:-24g}"
DATA=$(cygpath -w "$(cd "$1" && pwd)")
mkdir -p "$2"
OUT=$(cygpath -w "$(cd "$2" && pwd)")
SCRATCH_HOST=$(mktemp -d)
SCRATCH=$(cygpath -w "$SCRATCH_HOST")
trap 'rm -rf "$SCRATCH_HOST" 2>/dev/null || true' EXIT
MSYS_NO_PATHCONV=1 docker run --rm --platform linux/amd64 \
  --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 \
  --cap-drop ALL --security-opt no-new-privileges \
  --pids-limit 4096 --cpus "$CPUS" --memory "$MEM" --memory-swap "$MEM" \
  --user 1000:1000 \
  -v "$DATA":/data:ro -v "$OUT":/output -v "$SCRATCH":/scratch \
  -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch -e HOME=/scratch \
  -e NCPU="${CPUS%.*}" -e MEM_GB="${MEM%g}" \
  "$IMAGE"
