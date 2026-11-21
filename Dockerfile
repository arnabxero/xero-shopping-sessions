# Shopping Sessions: native Rust pipeline.
# The scorer builds this natively on linux/amd64 (Intel Xeon, Cascade Lake or
# newer). No FROM line pins a platform, so it also builds natively on arm64.

FROM rust:1.98-slim-bookworm@sha256:ff521445a372125ed4f76e1453a1f8098f2d05332d1601d30db1c1f62757e730 AS build
ARG TARGETARCH
ARG ENABLE_PGO=1
WORKDIR /src
# Install llvm-tools in its own cached layer before copying src so rebuilds don't
# re-fetch it and any transient download hiccup is retried loudly (never silent).
RUN set -eu; \
    if [ "$ENABLE_PGO" = "1" ]; then \
        rustup component add llvm-tools-preview || rustup component add llvm-tools-preview || echo "WARN: llvm-tools-preview unavailable, will fall back to non-PGO"; \
    fi
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# x86-64-v3 (AVX2/BMI1/BMI2/LZCNT/POPCNT) is available on every Haswell-or-newer
# Xeon, including all scorer CPUs. Pure AVX2 avoids Cascade Lake AVX-512 downclocking.
# Build-time PGO trains on a self-contained synthetic dataset (zero external
# downloads) and falls back cleanly if llvm-profdata is unavailable offline.
RUN set -eu; \
    BASE_FLAGS=""; \
    if [ "$TARGETARCH" = "amd64" ] || [ "$(uname -m)" = "x86_64" ]; then \
        BASE_FLAGS="-C target-cpu=cascadelake -C llvm-args=-x86-branches-within-32B-boundaries"; \
    fi; \
    HOST="$(rustc -vV | sed -n 's/^host: //p')"; \
    PROFDATA="$(rustc --print sysroot)/lib/rustlib/$HOST/bin/llvm-profdata"; \
    if [ "$ENABLE_PGO" = "1" ] && [ -x "$PROFDATA" ]; then \
        echo "=== PGO: ENABLED (host=$HOST rustc=$(rustc --version)) ==="; \
        WORK="$(mktemp -d)"; \
        rustc -O src/pgo_gen.rs -o "$WORK/pgo_gen"; \
        "$WORK/pgo_gen" "$WORK/data1" 1729; \
        "$WORK/pgo_gen" "$WORK/data2" 4242; \
        RUSTFLAGS="$BASE_FLAGS -Cprofile-generate=$WORK/raw" \
            cargo build --release --locked --target "$HOST" --target-dir "$WORK/inst"; \
        NCPU=4 "$WORK/inst/$HOST/release/shopping-sessions" "$WORK/data1" "$WORK/out1"; \
        NCPU=4 FORCE_SHARD_BITS=10 "$WORK/inst/$HOST/release/shopping-sessions" "$WORK/data2" "$WORK/out2"; \
        "$PROFDATA" merge -o "$WORK/merged.profdata" "$WORK"/raw/*.profraw; \
        RUSTFLAGS="$BASE_FLAGS -Cprofile-use=$WORK/merged.profdata" \
            cargo build --release --locked --target "$HOST"; \
        strip -o /shopping-sessions "target/$HOST/release/shopping-sessions"; \
        rm -rf "$WORK"; \
    else \
        echo "=== PGO: FALLBACK (ENABLE_PGO=$ENABLE_PGO profdata=$PROFDATA) ==="; \
        RUSTFLAGS="$BASE_FLAGS" cargo build --release --locked; \
        strip -o /shopping-sessions target/release/shopping-sessions; \
    fi

# Minimal runtime matching bookworm's glibc with libjemalloc2 preloaded.
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libjemalloc2 && \
    ln -sf /usr/lib/*/libjemalloc.so.2 /usr/lib/libjemalloc.so.2 && \
    rm -rf /var/lib/apt/lists/*
COPY --from=build /shopping-sessions /shopping-sessions
# Scorer env: DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch NCPU=8 MEM_GB=24.
# The binary writes only under OUTPUT_DIR and needs no writable root or /tmp.
ENV DATA_DIR=/data OUTPUT_DIR=/output LD_PRELOAD=/usr/lib/libjemalloc.so.2 MALLOC_CONF=thp:always,narenas:1
USER 1000:1000
ENTRYPOINT ["/shopping-sessions"]
