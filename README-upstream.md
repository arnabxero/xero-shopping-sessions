# Shopping Sessions — Team Inventors

An entry for the Builders Buildathon track "Databricks - Shopping Sessions": turn dirty
click-stream CSVs into two exact daily Parquet tables, as fast as possible. `rules.md`
(copied here from the track page) is the specification.

Two implementations of the same rules live in this repository:

| | Where | Role |
| --- | --- | --- |
| Native pipeline | `native/` (Rust, no dependencies) | What the image runs and what is scored |
| DuckDB pipeline | `pipeline/pipeline.py` | The readable reference, written first from the DuckDB starter; kept as an independent cross-check |

Both pass the 36 official fixtures and match the public dev answer exactly, and they agree
with each other on every generated dataset tried (see [Results](#results)).

## Layout

- `Dockerfile` builds the native binary (Rust toolchain image pinned by digest, `cargo build --locked`) into a pinned `debian:bookworm-slim`. The binary reads `$DATA_DIR`, writes `$OUTPUT_DIR`, logs stage timings to stderr.
- `native/src/main.rs` is the pipeline; `scan.rs` the CSV dialect, `time.rs` timestamps, `key.rs` IDs and hash maps, `parquet.rs` a small Parquet writer, `bump.rs` the allocator.
- `pipeline/pipeline.py` is the DuckDB reference (needs `requirements.txt`).
- `fixtures/`, `scripts/`, `Makefile` are the starter's checks, plus `scripts/crosscheck.sh`.
- `LICENSE` is MIT.

No data is in Git: `dev/`, `dev-answer/`, `data/` and `out/` are ignored.

## How the native pipeline works

1. **Links.** Parsed by all threads, IDs numbered per hash shard in parallel, then one thread runs union-find and names each person by their byte-wise smallest ID while the others start on the events.
2. **Parse.** Input files are memory-mapped and cut into 256 KB chunks on line ends. A two-stage scan (AVX2 compare for separators, newlines and quotes, then a walk over the lines) applies the CSV dialect exactly: only `\n` ends a line, a field is quote-free or wrapped in one pair of quotes, anything else drops the line. Every event copy is validated on the spot (timestamp forms, window, type, price, lateness) and sent to one of 256 partitions by `event_id`. IDs up to 15 bytes are packed exactly into 128-bit keys; longer ones are interned, so nothing relies on hash equality.
3. **Dedup.** Per partition, the earliest-ingested copy of each `event_id` is kept, valid or not (dedup before validation). Valid survivors are sent to a partition by person.
4. **Sessions.** Per partition: group by person, sort by time, split on gaps over 30 minutes, credit the latest click within 7 days (ties: smallest `click_id`), and count assists by merging each campaign's clicks into 7-day intervals. Each thread writes its own `user_daily` Parquet file.
5. **campaign_daily** is summed from per-thread totals.

Clicks are not deduplicated: copies are identical and nothing counts clicks, so a second
copy can never change a credit or an assist.

## Run the checks

Needs Docker with buildx, Python 3.10+ with `pip install -r requirements.txt`, and
`generate` (the track's generator) on `PATH` for the dev set.

```sh
make build                      # linux/amd64 image
make fixtures                   # image on all 36 fixtures with the scorer's flags
make dev                        # generate --seed 1 --events 1000000 --clicks 250000 --output dev
make run-dev                    # image on dev/ with the scorer's flags, timed
make check-dev                  # exact diff against dev-answer/ (the track's dev-answer.zip, unzipped here)
```

On a machine smaller than the scorer add `CPUS=4 MEM=12g`. Without Docker:

```sh
make fixtures-native            # needs Rust 1.89+
make fixtures-duckdb
scripts/crosscheck.sh 7         # generate seed 7 (1M events), run both pipelines, diff them
scripts/crosscheck.sh 31 5000000
```

## Results

Measured on the development box, **4 vCPUs (Xeon 2.1 GHz) and 15 GB**, not on the
scorer's 8 vCPUs and 24 GB: the scorer's limits could not be reproduced here, so these
are relative numbers, not predictions of the board time.

Correctness:

| Check | Command | Result |
| --- | --- | --- |
| 36 fixtures, image, scorer flags | `make fixtures CPUS=4 MEM=12g` | 36/36 passed |
| 36 fixtures, DuckDB reference | `make fixtures-duckdb` | 36/36 passed |
| Public dev set (seed 1, 1M events) vs dev answer | `make run-dev check-dev CPUS=4 MEM=12g` | MATCH: user_daily 117,038 rows, campaign_daily 27,400 rows, 0 differing |
| Native vs DuckDB, seeds 2 to 7, 1M events each | `scripts/crosscheck.sh N` | MATCH on all six |
| Native vs DuckDB, seed 31, 5M events | `scripts/crosscheck.sh 31 5000000` | MATCH: user_daily 577,254 rows, campaign_daily 28,014 rows (after the fix below) |
| Output readable by polars, pyarrow and DuckDB | ad hoc | yes; `day` is a Parquet DATE |
| Self-check on 20M and 60M outputs | sums of `sessions` and `revenue_cents` equal across the two tables | yes |

The 5M cross-check first differed in one row (`assisted_sessions` 8863 vs 8864). The cause
was a click with `ts` `2025-02-25T24:00:00Z`: the native pipeline rejected hour 24, as the
rules say, but DuckDB's timestamp cast read it as the next midnight. The DuckDB reference
was fixed; the native pipeline was already right. Neither the fixtures nor the dev answer
catch this in a DuckDB-cast-based solution.

Timings of the native binary (wall clock, process start to exit, warm page cache, best of 2 to 3 runs):

| Dataset | CSV size | Files (events/clicks/links) | Threads | Time | Peak memory (incl. mapped input) |
| --- | --- | --- | --- | --- | --- |
| dev, seed 1, 1M events / 250k clicks | 92 MB | 30 / 12 / 9 | 4 | 0.13 s (0.52 s with `docker run` overhead) | 0.26 GB |
| seed 11, 20M events / 5M clicks | 1.8 GB | 85 event files | 4 | 1.7 s (2.2 s with `docker run` overhead; 4.3 s with a cold page cache) | 3.7 GB |
| seed 23, 60M events / 15M clicks | 5.4 GB | 43 event files | 4 | 5.9 s | 11 GB |

The DuckDB reference takes 10.1 s on the 1M dev set with 4 threads, about 75 times slower.

What the profiling changed, at 20M events on 4 threads:

| Step | Time |
| --- | --- |
| First native version | 2.8 s |
| Scanner fix (LLVM rebuilt a joined 64-bit movemask bit by bit) and a date cache | 2.4 s |
| Link IDs numbered in parallel shards instead of on one thread | 2.2 s |
| Blocks from a per-thread arena, event blocks reused for the next stage | 1.9 s |
| Hash tables written at allocation instead of lazily zeroed (read-then-write page faults with TLB shootdowns kept threads from scaling) | 1.7 s |

Stage split of the 1.7 s: links 0.08 s, parse 0.9 s, dedup 0.27 s, sessions and
`user_daily` 0.35 s, exit 0.1 s.

## Limitations

- **Not timed under the scorer's limits.** This box has 4 vCPUs and 15 GB; `docker run --cpus 8 --memory 24g` is refused here. Peak memory at 60M events was 11 GB including 5.4 GB of mapped input, so 24 GB leaves room, but that is an extrapolation.
- **Never submitted.** No scorer run has confirmed the image builds and passes there. The image has no build-time downloads beyond the two pinned base images.
- **Window length.** Timestamps are stored as 32-bit millisecond offsets, so a window longer than about 35 days makes the binary exit with an error. The generator always writes 14 days.
- **`ingest_ts`** is trusted to be valid F1, as the rules promise. An unparsable one makes that copy invalid and sorts it last in the dedup; the DuckDB reference compares the raw strings instead.
- **The cross-check is not fully independent:** both pipelines were written from the same reading of `rules.md`. The fixtures and the dev answer are the only outside checks.
- AVX2 is required (`-C target-cpu=x86-64-v3`); the scorers have it.
- The license is the standard MIT text; no event-specific license file came with the starter.
