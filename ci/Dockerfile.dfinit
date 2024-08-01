FROM rust:1.75.0 as builder

RUN apt-get update && apt-get install -y openssl libclang-dev pkg-config protobuf-compiler

WORKDIR /app/client
COPY Cargo.toml ./
COPY Cargo.lock ./

COPY dragonfly-client/Cargo.toml ./dragonfly-client/Cargo.toml
COPY dragonfly-client/src ./dragonfly-client/src
COPY dragonfly-client/build.rs ./dragonfly-client/build.rs

COPY dragonfly-client-core/Cargo.toml ./dragonfly-client-core/Cargo.toml
COPY dragonfly-client-core/src ./dragonfly-client-core/src

COPY dragonfly-client-config/Cargo.toml ./dragonfly-client-config/Cargo.toml
COPY dragonfly-client-config/src ./dragonfly-client-config/src

COPY dragonfly-client-storage/Cargo.toml ./dragonfly-client-storage/Cargo.toml
COPY dragonfly-client-storage/src ./dragonfly-client-storage/src

COPY dragonfly-client-backend/Cargo.toml ./dragonfly-client-backend/Cargo.toml
COPY dragonfly-client-backend/src ./dragonfly-client-backend/src

COPY dragonfly-client-backend/examples/plugin/Cargo.toml ./dragonfly-client-backend/examples/plugin/Cargo.toml
COPY dragonfly-client-backend/examples/plugin/src ./dragonfly-client-backend/examples/plugin/src

COPY dragonfly-client-util/Cargo.toml ./dragonfly-client-util/Cargo.toml
COPY dragonfly-client-util/src ./dragonfly-client-util/src

COPY dragonfly-client-init/Cargo.toml ./dragonfly-client-init/Cargo.toml
COPY dragonfly-client-init/src ./dragonfly-client-init/src

RUN cargo build --release --verbose --bin dfinit

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends wget \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/client/target/release/dfinit /usr/local/bin/dfinit

ENTRYPOINT ["/usr/local/bin/dfinit"]
