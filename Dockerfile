# Shopping Sessions: one fully static Rust binary in an empty (FROM scratch) image.
# Built natively for linux/amd64 by the scorer; no FROM line pins a platform.

FROM rust:1.98-slim-bookworm@sha256:ff521445a372125ed4f76e1453a1f8098f2d05332d1601d30db1c1f62757e730 AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
ENV RUSTFLAGS="-C target-cpu=cascadelake -C target-feature=-avx512f,+crt-static"
# Fetch and build dependencies first (cached layer).
RUN mkdir src && echo 'fn main() {}' > src/main.rs \
 && cargo fetch --locked \
 && cargo build --release --locked --target x86_64-unknown-linux-gnu \
 && rm -rf src
COPY src ./src
RUN touch src/main.rs \
 && cargo build --release --locked --offline --target x86_64-unknown-linux-gnu \
 && strip target/x86_64-unknown-linux-gnu/release/shopping

FROM scratch
COPY --from=build /src/target/x86_64-unknown-linux-gnu/release/shopping /shopping
# The scorer runs as uid 1000 with a read-only root; the binary only reads
# /data and writes /output, and needs no shell, libc files, home or temp dir.
USER 1000:1000
CMD ["/shopping"]
