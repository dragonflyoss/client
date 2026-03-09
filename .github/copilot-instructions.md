# GitHub Copilot Instructions for Dragonfly Client

## Project Overview

Dragonfly Client (`dragonflyoss/client`) is a high-performance P2P download client written in **Rust 1.85.0**. It implements both the peer and seed-peer roles in the [Dragonfly](https://d7y.io/) distributed file distribution system. The daemon (`dfdaemon`) intercepts HTTP requests via an MITM proxy, coordinates piece-level downloads across peers, and persists data through a local RocksDB-backed storage layer.

---

## Workspace Structure

The repository is a Cargo workspace (resolver `"2"`) containing nine crates:

| Crate | Purpose |
|---|---|
| `dragonfly-client` | Main binaries: `dfdaemon`, `dfget`, `dfcache`, `dfstore` |
| `dragonfly-client-core` | Shared `DFError` enum, `Result<T>` alias, `OrErr` trait |
| `dragonfly-client-config` | Configuration structs and validation for all binaries |
| `dragonfly-client-storage` | RocksDB metadata, content files, LRU cache, TCP/QUIC servers |
| `dragonfly-client-backend` | Download backends: HTTP, S3/object-storage, HDFS, HuggingFace, plugins |
| `dragonfly-client-util` | Utilities: crypto, TLS, networking, ID generation, rate limiting, shutdown |
| `dragonfly-client-metric` | Prometheus metrics |
| `dragonfly-client-init` | `dfinit` binary — bootstraps the runtime environment |
| `dragonfly-client-backend/examples/plugin` | Example backend plugin |

All crates share `[workspace.package]` metadata (version `1.2.11`, Apache-2.0 license, edition `2021`) and `[workspace.dependencies]` for consistent dependency versions.

---

## Rust Conventions

### Toolchain

Pin the toolchain to `1.85.0` via `rust-toolchain.toml`. Always write code compatible with that version.

### Edition

Use Rust **2021 edition** features throughout.

### Formatting

Format all code with `cargo fmt --all`. There is no custom `rustfmt.toml`; use the default Rustfmt settings.

### Linting

All warnings are errors: `cargo clippy --all --all-targets -- -D warnings`. Fix every Clippy warning before submitting.

### File License Header

Every `.rs` source file must start with the Apache 2.0 license header:

```rust
/*
 *     Copyright 2024 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
```

### Naming

Follow standard Rust naming conventions: `snake_case` for functions, variables, and modules; `PascalCase` for types and traits; `SCREAMING_SNAKE_CASE` for constants.

---

## Error Handling

### Core Error Type

All errors flow through `DFError` defined in `dragonfly-client-core/src/error/mod.rs`:

```rust
use dragonfly_client_core::{Error, Result as ClientResult};
```

`Result<T>` is a type alias for `std::result::Result<T, DFError>`. Use `ClientResult<T>` (or `Result<T>` after the local alias) everywhere — **never use `anyhow` in library code**.

### `OrErr` Trait

Use the `OrErr` trait to attach an `ErrorType` when converting external errors:

```rust
use dragonfly_client_core::error::{ErrorType, OrErr};

let value = some_operation().or_err(ErrorType::SerializeError)?;
```

### Error Propagation

Propagate errors with `?`. Log at the site of handling, not at every propagation step.

```rust
let result = operation()
    .inspect_err(|err| error!("operation failed: {}", err))?;
```

### Avoiding Panics

Do **not** use `unwrap()` or `expect()` in production code paths. Reserve them for tests and truly unreachable branches, where you should add a comment explaining why the invariant holds.

---

## Logging and Tracing

### Framework

Use the `tracing` crate with structured fields. Import macros directly:

```rust
use tracing::{debug, error, info, instrument, warn, Instrument, Span};
```

### Log Levels

| Level | When to use |
|---|---|
| `error!` | Unrecoverable failures that need operator attention |
| `warn!` | Recoverable anomalies or degraded behaviour |
| `info!` | Normal operational lifecycle events |
| `debug!` | Detailed state useful during development |
| `trace!` | Very verbose per-iteration details |

### Instrumentation

Use the `#[instrument(skip_all)]` macro on public async functions to create spans automatically. Skip large or sensitive parameters:

```rust
#[instrument(skip_all)]
pub async fn download_task(&self, request: DownloadRequest) -> ClientResult<()> {
    info!("starting download for task {}", request.task_id);
    // ...
}
```

For spawned tasks, propagate the current span:

```rust
tokio::spawn(
    async move {
        // work
    }
    .in_current_span(),
);
```

### Tracing Setup

The `tracing` module in `dragonfly-client` initializes:
- A rolling file appender (hourly rotation) with RFC 3339 timestamps.
- An optional stdout layer (enabled when `console = true`).
- An optional OpenTelemetry OTLP exporter.

---

## Async Runtime

- Use **Tokio** as the sole async runtime (`tokio = { version = "1.49.0", features = ["full"] }`).
- Annotate async entry points with `#[tokio::main]`.
- Annotate async tests with `#[tokio::test]`.
- Share expensive resources (clients, storage, config) via `Arc<T>`; use `Arc::clone` explicitly.
- Use `tokio::sync::{Mutex, RwLock, mpsc, broadcast, watch}` for shared state — prefer `RwLock` for read-heavy data.
- Avoid blocking the async thread; move CPU-heavy work to `tokio::task::spawn_blocking`.

---

## Testing

### Test Location

Write unit tests in the same file as the code being tested, inside a `#[cfg(test)]` module at the bottom:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_synchronous_behavior() { ... }

    #[tokio::test]
    async fn test_async_behavior() { ... }
}
```

### Test Dependencies

| Library | Purpose |
|---|---|
| `tempfile` | Temporary files and directories |
| `mocktail` | General-purpose mocking |
| `wiremock` | HTTP endpoint mocking (backend crate) |
| `criterion` | Micro-benchmarks (storage crate) |

### Running Tests

```bash
# Run all tests with coverage
cargo llvm-cov --all-features --workspace --lcov --output-path lcov.info

# Run tests for a single crate
cargo test -p dragonfly-client-storage

# Run a specific test
cargo test -p dragonfly-client-storage test_lru_cache
```

### Benchmarks

Benchmarks live in `dragonfly-client-storage/benches/`. Run them with:

```bash
cargo bench -p dragonfly-client-storage
```

---

## Key Dependencies

| Crate | Role |
|---|---|
| `tonic` 0.12 | gRPC client/server |
| `hyper` 1.x + `hyper-util` | HTTP server and MITM proxy |
| `reqwest` 0.12 | HTTP client for backend downloads |
| `rustls` 0.23 | TLS (ring backend, TLS 1.2+) |
| `tokio` 1.x | Async runtime |
| `rocksdb` 0.24 | Embedded metadata storage |
| `serde` + `bincode` + `serde_json` + `serde_yaml` | Serialization |
| `tracing` + `tracing-subscriber` + `opentelemetry-otlp` | Observability |
| `prometheus` 0.13 | Metrics |
| `opendal` 0.55 | Multi-backend object storage |
| `quinn` 0.11 | QUIC transport |
| `thiserror` 2.0 | Error derive macros |
| `clap` 4.x | CLI argument parsing |
| `validator` 0.16 | Config validation |
| `dashmap` 6.x | Concurrent hashmap |
| `tikv-jemallocator` | JeMalloc global allocator (non-MSVC) |
| `dragonfly-api` =2.2.22 | Protobuf/gRPC API definitions |

Centralise all dependency versions in `[workspace.dependencies]` in the root `Cargo.toml`. Do not specify a version in a crate's own `Cargo.toml` unless it differs from the workspace version.

---

## gRPC and Protocol Buffers

- API types come from the `dragonfly-api` crate (generated from protobuf).
- gRPC servers and clients are implemented with `tonic`.
- Interceptors handle authentication and dynamic block-list checks (`grpc/block_list.rs`).
- Timeout constants are defined in `dragonfly-client/src/grpc/mod.rs` as `REQUEST_TIMEOUT`.

---

## Storage Layer

The storage subsystem (`dragonfly-client-storage`) has three layers:

1. **Metadata** — RocksDB key/value store for task and piece state.
2. **Content** — Platform-specific file I/O (`content_linux.rs` / `content_macos.rs`).
3. **Cache** — In-memory LRU cache backed by a `DashMap`.

The `DatabaseObject` trait must be implemented by any type stored in RocksDB:

```rust
pub trait DatabaseObject: Serialize + DeserializeOwned {
    const NAMESPACE: &'static str;
}
```

Serialization uses `bincode` for compactness.

---

## Configuration

- Configuration structs live in `dragonfly-client-config` and are validated with `validator`.
- Dynamic configuration changes (e.g., scheduler cluster config) are managed by `dynconfig/` in `dragonfly-client`.
- Use `Arc<Config>` when sharing config across threads; never clone the full struct.

---

## Build and CI

```bash
# Check compilation
cargo check --all --all-targets

# Format check
cargo fmt --all -- --check

# Lint
cargo clippy --all --all-targets -- -D warnings

# Full test run with coverage
cargo llvm-cov --all-features --workspace --lcov --output-path lcov.info

# Release build
cargo build --release
```

CI runs on GitHub Actions (`.github/workflows/`):

- **ci.yml** — `cargo check`, tests with `cargo llvm-cov`, uploads coverage to Codecov.
- **lint.yml** — `cargo fmt` and `cargo clippy` checks.
- **docker.yml** — Docker image builds.
- **release.yml** — Release packaging (`.deb`, RPM, multi-arch binaries).

Protobuf compilation requires `protoc`. In CI, it is installed via `arduino/setup-protoc`.

---

## Platform Support

Target triples built in CI:

- `x86_64-unknown-linux-gnu`
- `x86_64-unknown-linux-musl`
- `aarch64-unknown-linux-gnu`
- `aarch64-unknown-linux-musl`

Linux-only features (cgroups, `content_linux.rs`, jemalloc profiling) are gated with `#[cfg(target_os = "linux")]`.

---

## Performance Considerations

- **JeMalloc** replaces the system allocator on non-MSVC platforms; profiling is enabled on Linux via `jemalloc_pprof`.
- The release profile uses `lto = "thin"`, `codegen-units = 1`, `opt-level = 3`, and `panic = "abort"`.
- CPU/memory profiles can be generated with `pprof` (flamegraphs) and exposed via the `stats` HTTP server.
- Rate limiting uses `leaky-bucket`; peer selection uses consistent hashing (`hashring`).

---

## Contributing

- Every PR must reference an open issue.
- Follow the [Dragonfly community contributing guide](https://github.com/dragonflyoss/community/blob/master/CONTRIBUTING.md).
- Use the PR template in `.github/PULL_REQUEST_TEMPLATE.md`.
- The project is licensed under **Apache License 2.0**; all new files must carry the license header.
