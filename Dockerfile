FROM rust:1.75.0 as builder

WORKDIR /app/client

RUN apt-get update && apt-get install -y openssl libclang-dev pkg-config protobuf-compiler git

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

RUN cargo build --release --verbose --bin dfget --bin dfdaemon --bin dfstore --bin dfcache

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

FROM golang:1.21.1-alpine3.17 as pprof

RUN go install github.com/google/pprof@latest

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends wget curl \
    bash-completion procps apache2-utils ca-certificates binutils bpfcc-tools \
    dnsutils iputils-ping vim linux-perf llvm graphviz \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/client/target/release/dfget /usr/local/bin/dfget
COPY --from=builder /app/client/target/release/dfdaemon /usr/local/bin/dfdaemon
COPY --from=builder /app/client/target/release/dfstore /usr/local/bin/dfstore
COPY --from=builder /app/client/target/release/dfcache /usr/local/bin/dfcache
COPY --from=health /bin/grpc_health_probe /bin/grpc_health_probe
COPY --from=pprof /go/bin/pprof /bin/pprof

ENTRYPOINT ["/usr/local/bin/dfdaemon"]
