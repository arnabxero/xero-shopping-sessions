# Handoff: pushing for the top of the Shopping Sessions board

For a Claude Code session started on a Google Cloud `n2-standard-8` VM (the scorer's
machine type). Read `README.md` and `rules.md` first; this file is what they don't say.

## Where things stand

- Commit `74378ce` scored **1.48 s** on the hidden ~20M-event practice set (submission
  BB-9BA40C7B) and is entered in the final. It is the safe fallback: a slower or rejected
  submission never replaces the best score.
- Leaders are at 0.63–0.70 s. The goal is first place; the owner submits, not you.
- Scoring closes Sunday 11:00 AM ET. 2 submissions an hour. The final runs ~60M events.
- Correctness is absolute: one wrong row scores nothing. Never trade exactness for speed.

## Set up this machine

```sh
sudo apt-get update && sudo apt-get install -y build-essential docker.io python3-pip unzip linux-tools-common linux-tools-$(uname -r)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.97.0
pip install --break-system-packages -r requirements.txt pyarrow
sudo usermod -aG docker $USER   # then log out and in
```

The generator and the dev answer are not in this repo. Download `generator.zip` and
`dev-answer.zip` from the track page
(https://www.joinbuilders.org/buildathon/tracks/shopping-sessions), then:

```sh
unzip generator.zip && (cd generator && cargo build --release) && sudo cp generator/target/release/generate /usr/local/bin/
unzip dev-answer.zip            # gives dev-answer/ in the repo root (git-ignored)
make dev                        # the 1M-event dev set
generate --seed 11 --events 20000000 --clicks 5000000 --output data/s11-20m
generate --seed 23 --events 60000000 --clicks 15000000 --output data/s23-60m
```

## The gate every change must pass before it is committed

```sh
make build && make fixtures && make run-dev && make check-dev   # 36/36 and MATCH
for s in 2 3 4 5 6 7; do scripts/crosscheck.sh $s; done         # native vs DuckDB, all MATCH
scripts/crosscheck.sh 31 5000000                                # MATCH (needs ~12 GB)
```

`crosscheck.sh` has already caught a real divergence once (hour 24 in a timestamp), so
do not skip it. Time with the scorer's flags: `scripts/docker_run.sh data/s11-20m out`.

## What profiling showed (on a 4-vCPU dev box, so re-measure here with perf)

Stage split of a 1.7 s run at 20M events: parse 0.9 s, dedup 0.27 s, sessions 0.35 s,
links 0.08 s, process exit 0.1 s. About 260 ns of CPU per input line in total; the
leaders need roughly 150 ns per line per thread for everything.

- The parse stage is not branch- or instruction-bound in an obvious way: rewriting the
  scanner twice changed instruction counts but not time. Measure cache and TLB misses.
- Fresh memory is expensive: every event is a 48-byte record, about 1 GB of first-touch
  page faults at 20M. Pushing records costs as much as validating them.
- Reading a lazily zeroed page and then writing it costs two faults and a TLB shootdown,
  which stopped threads scaling. Always write-initialise large tables.
- `madvise(MADV_HUGEPAGE)` scaled badly on the dev box; test it here before using it.
- More partitions (512, 1024) did not help at 20M or 60M.

## The redesign to try, in order of expected gain

1. **One shuffle instead of two.** Resolve the person while parsing (links must be
   joined first; parse clicks while that runs) and send a ~16-byte record straight to the
   person's partition. Dedup inside each partition; copies of one event that name
   different users (about 1 in 64 duplicated events) need an exact cross-partition path.
2. **A fast path for the common line shape**, with the current exact scanner as the
   fallback for anything unusual. AVX-512 is available on every scorer.
3. **Less fresh memory:** smaller records, reused buffers, one up-front allocation.
4. **Startup and exit:** static binary on an empty base image; exit without unmapping.

Clicks need no dedup (copies are identical and nothing counts clicks); keep that.

## Rules of engagement

- Commit small, verified steps. Push only commits that pass the whole gate.
- Tell the owner the commit SHA and the measured before/after time; they decide whether
  to spend a submission.
- Keep data, outputs and credentials out of Git. Keep every submitted commit in history.
