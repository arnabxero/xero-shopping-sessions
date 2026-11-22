FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo .cargo
COPY src src
RUN RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static -C llvm-args=-x86-branches-within-32B-boundaries" \
    cargo build --release --locked --target x86_64-unknown-linux-gnu --bin nb7x

FROM scratch
COPY --from=build /src/target/x86_64-unknown-linux-gnu/release/nb7x /nb7x
ENV NB7X_QUIET=1
CMD ["/nb7x"]

