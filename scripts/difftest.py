#!/usr/bin/env python3
"""Differential test: the Rust binary against the Python reference.

Usage: difftest.py BINARY DATASET_DIR...

The reference output for each dataset is cached in DATASET_DIR/../ref-out/<name>
(rebuild it by deleting that folder). Exits non-zero on any difference.
"""
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from run_fixtures import SCHEMAS, load_actual  # noqa: E402

REFERENCE = HERE.parent / "reference" / "reference.py"


def rows(folder, table):
    df = load_actual(folder / table, SCHEMAS[table])
    return sorted(df.rows(), key=lambda r: tuple("" if v is None else str(v) for v in r))


def main():
    binary = sys.argv[1]
    failed = 0
    for ds in map(Path, sys.argv[2:]):
        ref = ds.parent / "ref-out" / ds.name
        if not (ref / "user_daily").is_dir():
            t = time.time()
            subprocess.run([sys.executable, str(REFERENCE), str(ds), str(ref)], check=True)
            print(f"  reference {ds.name}: {time.time() - t:.1f}s")
        out = ds.parent / "rs-out" / ds.name
        shutil.rmtree(out, ignore_errors=True)
        t = time.time()
        env = dict(os.environ, SHOPPING_QUIET="1")
        subprocess.run([binary, str(ds), str(out)], check=True, env=env)
        el = time.time() - t
        ok = True
        for table in SCHEMAS:
            a, b = rows(ref, table), rows(out, table)
            if a != b:
                ok = False
                sa, sb = set(a), set(b)
                print(f"DIFF {ds.name} {table}: ref {len(a)} rows, rust {len(b)} rows; "
                      f"only ref {len(sa - sb)}, only rust {len(sb - sa)}")
                for r in sorted(sa - sb, key=str)[:5]:
                    print("   ref only ", r)
                for r in sorted(sb - sa, key=str)[:5]:
                    print("   rust only", r)
        print(f"{'ok  ' if ok else 'FAIL'} {ds.name} (rust {el:.2f}s, {len(a)} user rows)")
        failed += not ok
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
