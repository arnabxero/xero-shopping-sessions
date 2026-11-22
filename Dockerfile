# Shopping Sessions, Team Inventors: a native (Rust) pipeline with no dependencies.
# The scorers are linux/amd64 Intel Xeon, Cascade Lake or newer. They build this
# file natively, so no FROM line pins a platform. Base images are pinned by digest.
FROM rust:1.97-slim-bookworm@sha256:2775a09d208ff0d7c1f50490c45b62db929e87ba1dcbc3f2132ac71a704bcdd3 AS build
WORKDIR /src
COPY native/ ./
# Cargo.lock pins every dependency; --locked refuses to build if it would change.
RUN cargo build --release --locked

FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
# The scorer runs the container as uid 1000; give that uid a name and a home.
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /src/target/release/shopping-sessions /app/shopping-sessions
# No arguments: reads $DATA_DIR, writes $OUTPUT_DIR. Exit 0 means "done".
CMD ["/app/shopping-sessions"]
