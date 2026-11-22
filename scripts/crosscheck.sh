#!/bin/sh
# Cross-check the native pipeline against the DuckDB reference on generated data.
# Usage: scripts/crosscheck.sh SEED [EVENTS] [CLICKS]   (needs `generate` on PATH, duckdb and polars)
set -eu
SEED="$1"; EVENTS="${2:-1000000}"; CLICKS="${3:-$((EVENTS / 4))}"
HERE=$(cd "$(dirname "$0")/.." && pwd)
WORK="${WORK:-$HERE/data}/seed-$SEED-$EVENTS"
mkdir -p "$WORK"
[ -f "$WORK/input/manifest.json" ] || generate --seed "$SEED" --events "$EVENTS" --clicks "$CLICKS" --output "$WORK/input"
rm -rf "$WORK/native" "$WORK/duckdb" "$WORK/scratch"; mkdir -p "$WORK/scratch"
DATA_DIR="$WORK/input" OUTPUT_DIR="$WORK/native" "$HERE/native/target/release/shopping-sessions" 2>/dev/null
DATA_DIR="$WORK/input" OUTPUT_DIR="$WORK/duckdb" SCRATCH_DIR="$WORK/scratch" python3 "$HERE/pipeline/pipeline.py" 2>/dev/null
python3 "$HERE/scripts/compare.py" "$WORK/native" "$WORK/duckdb" | sed "s/^/seed $SEED: /"
