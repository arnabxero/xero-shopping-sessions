#!/bin/bash
# check_linux.sh BINARY: fixtures, dev answer and differential sets in every I/O mode,
# plus tiny partition capacities. Data lives under SHOP_WORK (see build_linux.sh).
set -e
W=${SHOP_WORK:-/data/eftakhar/shopping}
B=$(realpath "$1")
PY=${PY:-$W/venv/bin/python}
REPO=$(cd "$(dirname "$0")/.." && pwd)
cd "$REPO"
export NCPU=${NCPU:-8}
run() { taskset -c ${CPUS:-192-223} "$@"; }
for io in auto mmap pread direct; do
  export SHOPPING_IO=$io SHOPPING_QUIET=1
  f=$(run $PY scripts/run_fixtures.py fixtures -- "$B" | tail -1)
  rm -rf "$W/out/chk-dev"; run "$B" "$W/dev" "$W/out/chk-dev"
  d=$(run $PY scripts/compare.py "$W/out/chk-dev" "$W/dev-answer" | tail -1)
  x=$(cd "$W/diff" && run $PY "$REPO/scripts/difftest.py" "$B" s* | grep -c "^ok")
  echo "IO=$io: $f | dev $d | differential $x/10"
done
unset SHOPPING_IO
x=$(cd "$W/diff" && SHOPPING_TEST_SMALLCAP=1 run $PY "$REPO/scripts/difftest.py" "$B" s* | grep -c "^ok")
echo "SMALLCAP differential $x/10"
