FROM rust:1.75.0 as builder

RUN apt-get update && apt-get install -y openssl libclang-dev pkg-config protobuf-compiler

WORKDIR /app/client
COPY Cargo.toml ./
COPY Cargo.lock ./
COPY src/ src/

COPY dragonfly-client/Cargo.toml ./dragonfly-client/Cargo.toml
COPY dragonfly-client/src ./dragonfly-client/src

COPY dragonfly-client-core/Cargo.toml ./dragonfly-client-core/Cargo.toml
COPY dragonfly-client-core/src ./dragonfly-client-core/src

COPY dragonfly-client-config/Cargo.toml ./dragonfly-client-config/Cargo.toml
COPY dragonfly-client-config/src ./dragonfly-client-config/src

COPY dragonfly-client-storage/Cargo.toml ./dragonfly-client-storage/Cargo.toml
COPY dragonfly-client-storage/src ./dragonfly-client-storage/src

COPY dragonfly-client-backend/Cargo.toml ./dragonfly-client-backend/Cargo.toml
COPY dragonfly-client-backend/src ./dragonfly-client-backend/src

COPY dragonfly-client-util/Cargo.toml ./dragonfly-client-util/Cargo.toml
COPY dragonfly-client-util/src ./dragonfly-client-util/src

RUN cargo build --release --verbose

FROM alpine:3.17 as health

ENV GRPC_HEALTH_PROBE_VERSION v0.4.24

RUN if [ "$(uname -m)" = "ppc64le" ]; then \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-ppc64le; \
    elif [ "$(uname -m)" = "aarch64" ]; then \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-arm64; \
    else \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64; \
    fi && \
    chmod +x /bin/grpc_health_probe

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends wget \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/client/target/release/dfget /usr/local/bin/dfget
COPY --from=builder /app/client/target/release/dfdaemon /usr/local/bin/dfdaemon
COPY --from=builder /app/client/target/release/dfstore /usr/local/bin/dfstore
COPY --from=health /bin/grpc_health_probe /bin/grpc_health_probe

ENTRYPOINT ["/usr/local/bin/dfdaemon"]
