#!/bin/bash
# Full correctness suite (Windows/Git Bash dev box).
#   check_all.sh native   - target/release/shopping.exe: fixtures, dev-answer, differential sets
#   check_all.sh docker   - the image: fixtures and dev-answer under scorer flags
# Datasets live outside the repo in ../shopping-data (dev/, diff/s*).
set -e
HERE=$(cd "$(dirname "$0")/.." && pwd)
DATA="$HERE/../shopping-data"
ANSWER="$HERE/../kits/shopping/dev-answer/dev-answer"
MODE="${1:-native}"
cd "$HERE"
if [ "$MODE" = native ]; then
  B="$(pwd -W)/target/release/shopping.exe"
  SHOPPING_QUIET=1 python scripts/run_fixtures.py fixtures -- "$B" | tail -1
  rm -rf "$DATA/out-check-dev"
  SHOPPING_QUIET=1 "$B" "$DATA/dev" "$DATA/out-check-dev"
  python scripts/compare.py "$DATA/out-check-dev" "$ANSWER" | tail -1
  (cd "$DATA" && python "$HERE/scripts/difftest.py" "$B" diff/s* | grep -c "^ok" | sed 's/$/ differential sets ok/'; python "$HERE/scripts/difftest.py" "$B" diff/s* | grep -v "^ok" || true)
else
  python scripts/run_fixtures.py fixtures -- sh scripts/docker_run_win.sh 2>/dev/null | tail -1
  rm -rf "$DATA/out-check-dev"
  sh scripts/docker_run_win.sh "$DATA/dev" "$DATA/out-check-dev" 2>/dev/null
  python scripts/compare.py "$DATA/out-check-dev" "$ANSWER" | tail -1
fi
