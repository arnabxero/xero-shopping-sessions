#!/bin/bash
# build_linux.sh [test]: build target/release/shopping (or run the unit tests) in the pinned
# Rust image, on the CPUs given by CPUS (default 192-223). Work files stay under SHOP_WORK.
set -e
W=${SHOP_WORK:-/data/eftakhar/shopping}
REPO=$(cd "$(dirname "$0")/.." && pwd)
CPUS=${CPUS:-192-223}
IMG=rust:1.98-slim-bookworm@sha256:ff521445a372125ed4f76e1453a1f8098f2d05332d1601d30db1c1f62757e730
CMD="cargo build --release --locked"
[ "${1:-}" = test ] && CMD="cargo test --release --locked -- --nocapture"
sg docker -c "docker run --rm --cpuset-cpus $CPUS --user $(id -u):$(id -g) -e CARGO_HOME=/w/cargo -e TMPDIR=/w/tmp -e CARGO_BUILD_JOBS=16 -e SHOPPING_TEST_DIR=/w/tmp -v $W:/w -v $REPO:/repo -w /repo $IMG $CMD" 2>&1 | grep -vE "^\s+(Compiling|Downloaded|Downloading)" | tail -${TAILN:-15}
