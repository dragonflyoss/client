FROM rust:1.75.0 as builder

RUN apt-get update && apt-get install -y openssl libclang-dev pkg-config protobuf-compiler

WORKDIR /app/client
COPY Cargo.toml ./
COPY Cargo.lock ./

COPY dragonfly-client-init/Cargo.toml ./dragonfly-client-init/Cargo.toml
COPY dragonfly-client-init/src ./dragonfly-client-init/src

RUN cargo build --release --verbose

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends wget \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/client/target/release/dfinit /usr/local/bin/dfinit

ENTRYPOINT ["/usr/local/bin/dfinit"]
