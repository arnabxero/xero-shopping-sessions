# Shopping Sessions: one static-ish Rust binary in a slim image.
# Built natively for linux/amd64 by the scorer; no FROM line pins a platform.

FROM rust:1.98-slim-bookworm@sha256:ff521445a372125ed4f76e1453a1f8098f2d05332d1601d30db1c1f62757e730 AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
# Fetch and build dependencies first (cached layer).
RUN mkdir src && echo 'fn main() {}' > src/main.rs \
 && cargo fetch --locked \
 && RUSTFLAGS="-C target-cpu=x86-64-v3" cargo build --release --locked \
 && rm -rf src
COPY src ./src
RUN touch src/main.rs \
 && RUSTFLAGS="-C target-cpu=x86-64-v3" cargo build --release --locked --offline \
 && strip target/release/shopping

FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
COPY --from=build /src/target/release/shopping /usr/local/bin/shopping
# The scorer runs as uid 1000 with a read-only root; the binary only reads
# /data and writes /output, and needs no home or temp directory.
USER 1000:1000
CMD ["/usr/local/bin/shopping"]
