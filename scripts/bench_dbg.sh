#!/bin/bash
# bench_dbg.sh IMAGE VOLUME RUNS [ENV=VAL ...]: debug breakdown runs in docker (CPU-time sub-timers).
IMG="$1"; VOL="$2"; R="$3"; shift 3
E=(); for kv in "$@"; do E+=(-e "$kv"); done
export MSYS_NO_PATHCONV=1
for i in $(seq "$R"); do
  docker run --rm -v shopout:/o debian:bookworm-slim sh -c "rm -rf /o/* ; chmod 777 /o"
  docker run --rm --network none --read-only --cpus 8 --memory 24g --user 1000:1000 \
    -v "$VOL":/data:ro -v shopout:/output -e NCPU=8 -e SHOPPING_DEBUG=1 "${E[@]}" "$IMG" 2>&1 \
    | grep -E "thread-ms|cpu user|output written" | tr '\n' ' '
  echo
done
