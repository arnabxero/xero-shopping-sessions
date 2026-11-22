FROM rust:1.89-bookworm AS builder

WORKDIR /src
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
COPY Cargo.toml /src/Cargo.toml
RUN mkdir -p src && echo "fn main() {}" > src/main.rs && cargo build --release && rm -rf src

COPY src /src/src
RUN touch src/main.rs && cargo build --release && \
    mkdir -p /rootfs/app /rootfs/etc /rootfs/lib64 /rootfs/lib/x86_64-linux-gnu \
             /rootfs/data /rootfs/output /rootfs/scratch /rootfs/tmp && \
    chmod 1777 /rootfs/tmp && \
    chown 1000:1000 /rootfs/scratch && \
    printf 'root:x:0:0:root:/root:/sbin/nologin\napp:x:1000:1000::/scratch:/sbin/nologin\n' > /rootfs/etc/passwd && \
    printf 'root:x:0:\napp:x:1000:\n' > /rootfs/etc/group && \
    cp /src/target/release/shopping-sessions /rootfs/app/shopping-sessions && \
    chmod 0755 /rootfs/app/shopping-sessions && \
    for lib in $(ldd /src/target/release/shopping-sessions | awk '/=>/ {print $3} /^\t\// {print $1}'); do \
        mkdir -p "/rootfs$(dirname "$lib")" && cp -L "$lib" "/rootfs$lib"; \
    done

FROM scratch

COPY --from=builder /rootfs /
WORKDIR /app
CMD ["/app/shopping-sessions"]
