FROM public.ecr.aws/docker/library/rust:1.85.0 AS builder

WORKDIR /app/client

RUN apt-get update && apt-get install -y \
    openssl libclang-dev pkg-config protobuf-compiler git \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo

COPY dragonfly-client/Cargo.toml ./dragonfly-client/Cargo.toml
COPY dragonfly-client/src ./dragonfly-client/src

COPY dragonfly-client-core/Cargo.toml ./dragonfly-client-core/Cargo.toml
COPY dragonfly-client-core/src ./dragonfly-client-core/src

COPY dragonfly-client-config/Cargo.toml ./dragonfly-client-config/Cargo.toml
COPY dragonfly-client-config/src ./dragonfly-client-config/src
COPY dragonfly-client-config/build.rs ./dragonfly-client-config/build.rs

COPY dragonfly-client-storage/Cargo.toml ./dragonfly-client-storage/Cargo.toml
COPY dragonfly-client-storage/src ./dragonfly-client-storage/src
COPY dragonfly-client-storage/benches ./dragonfly-client-storage/benches

COPY dragonfly-client-backend/Cargo.toml ./dragonfly-client-backend/Cargo.toml
COPY dragonfly-client-backend/src ./dragonfly-client-backend/src

COPY dragonfly-client-backend/examples/plugin/Cargo.toml ./dragonfly-client-backend/examples/plugin/Cargo.toml
COPY dragonfly-client-backend/examples/plugin/src ./dragonfly-client-backend/examples/plugin/src

COPY dragonfly-client-util/Cargo.toml ./dragonfly-client-util/Cargo.toml
COPY dragonfly-client-util/src ./dragonfly-client-util/src

COPY dragonfly-client-init/Cargo.toml ./dragonfly-client-init/Cargo.toml
COPY dragonfly-client-init/src ./dragonfly-client-init/src

ARG TARGETPLATFORM
RUN case "${TARGETPLATFORM}" in \
  "linux/arm64") export JEMALLOC_SYS_WITH_LG_PAGE=16;; \
  esac && \
  cargo build --release --verbose --bin dfget --bin dfdaemon --bin dfcache

RUN cargo install tokio-console --version 0.1.13 --locked --root /usr/local

FROM public.ecr.aws/docker/library/alpine:3.20 AS health

ENV GRPC_HEALTH_PROBE_VERSION=v0.4.24

RUN if [ "$(uname -m)" = "ppc64le" ]; then \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-ppc64le; \
    elif [ "$(uname -m)" = "aarch64" ]; then \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-arm64; \
    else \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64; \
    fi && \
    chmod +x /bin/grpc_health_probe

FROM public.ecr.aws/docker/library/golang:1.23.0-alpine3.20 AS pprof

RUN go install github.com/google/pprof@latest
RUN go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest

FROM public.ecr.aws/debian/debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends iperf3 linux-libc-dev libc6-dev \
    iotop sysstat bash-completion procps apache2-utils ca-certificates binutils fio curl \
    dnsutils iputils-ping llvm graphviz lsof strace dstat net-tools bpftrace \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/client/target/release/dfget /usr/local/bin/dfget
COPY --from=builder /app/client/target/release/dfdaemon /usr/local/bin/dfdaemon
COPY --from=builder /app/client/target/release/dfcache /usr/local/bin/dfcache
COPY --from=builder /usr/local/bin/tokio-console /usr/local/bin/
COPY --from=pprof /go/bin/pprof /bin/pprof
COPY --from=pprof /go/bin/grpcurl /bin/grpcurl
COPY --from=health /bin/grpc_health_probe /bin/grpc_health_probe

ENTRYPOINT ["/usr/local/bin/dfdaemon"]
