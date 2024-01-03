FROM rust as builder

RUN apt-get update && apt-get install -y openssl libclang-dev pkg-config protobuf-compiler

WORKDIR /app/client
COPY Cargo.toml ./
COPY src/ src/
RUN cargo build --release

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

COPY --from=builder /app/client/target/release/dfget /usr/local/bin/dfget
COPY --from=builder /app/client/target/release/dfdaemon /usr/local/bin/dfdaemon
COPY --from=builder /app/client/target/release/dfstore /usr/local/bin/dfstore
COPY --from=health /bin/grpc_health_probe /bin/grpc_health_probe

ENTRYPOINT ["/usr/local/bin/dfdaemon"]
