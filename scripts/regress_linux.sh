#!/bin/bash
# regress_linux.sh BINARY BASELINE_BINARY [DATASET...]: outputs on the big sets must equal the
# baseline binary output (cached per dataset), in every I/O mode.
set -e
W=${SHOP_WORK:-/data/eftakhar/shopping}
B=$(realpath "$1"); BASE=$(realpath "$2"); shift 2
PY=${PY:-$W/venv/bin/python}
REPO=$(cd "$(dirname "$0")/.." && pwd)
export NCPU=${NCPU:-8}
run() { taskset -c ${CPUS:-192-223} "$@"; }
for ds in "${@:-big20m big60m}"; do
  for d in $ds; do
    if [ ! -d "$W/out/base-$d/user_daily" ]; then rm -rf "$W/out/base-$d"; SHOPPING_QUIET=1 run "$BASE" "$W/$d" "$W/out/base-$d"; fi
    for io in auto mmap pread direct; do
      rm -rf "$W/out/new-$d"; SHOPPING_IO=$io SHOPPING_QUIET=1 run "$B" "$W/$d" "$W/out/new-$d"
      echo "$d IO=$io: $(run $PY "$REPO/scripts/compare.py" "$W/out/base-$d" "$W/out/new-$d" | tail -1)"
    done
  done
done
