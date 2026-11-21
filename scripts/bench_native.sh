#!/bin/bash
# bench_native.sh BINARY DATA [RUNS]: run natively RUNS times, print each run's phase log on one line.
B="$1"; D="$2"; R="${3:-5}"
O=$(mktemp -d)
for i in $(seq "$R"); do
  rm -rf "$O"/*
  NCPU=${NCPU:-8} "$B" "$D" "$O" 2>&1 | sed -E 's/^\[ *([0-9.]+) ms\] (.*)$/\1 \2/' | awk '{printf "%7.0f %-12s", $1, $2} END {print ""}'
done
rm -rf "$O"
