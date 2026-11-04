## Summary

Implements dragonfly#4025 by allowing `dfcache import` to stream persistent-cache content to dfdaemon over its existing download Unix domain socket. This supports Pods that mount the Node's UDS but do not share their source filesystem with dfdaemon.

The legacy unary path import remains the default and is unchanged. Streaming is explicitly enabled with `--transfer-to-dfdaemon`; failures do not silently fall back to path import.

## Changes

- Add a backward-compatible client-streaming companion RPC carrying initial import metadata and bounded file chunks.
- Stage received bytes in daemon-owned `storage.dir/tmp`, validate protocol order and exact content length, and clean up staging on success or failure.
- Reuse the existing persistent-cache upload handler for task-ID generation, blocklist checks, metrics, scheduler registration, storage import, and rollback.
- Add `--transfer-to-dfdaemon` / `DFCACHE_IMPORT_TRANSFER_TO_DFDAEMON`, 1 MiB file chunking, and source-read error propagation to dfcache.
- Support empty files, multi-chunk files, and non-UTF8 Unix filenames without transmitting the client path.
- Add protocol, chunk-boundary, malformed-stream, peer-interruption, missing-socket, write-failure, cleanup, and real in-process UDS tests.
- Document Kubernetes UDS mounting and Pod usage in `docs/dfcache-import.md`.

## Test plan

Executed with the repository Rust 1.88.0 toolchain and `CARGO_TARGET_DIR=/home/ps/LHL/target-dfc`:

- `cargo build -p dragonfly-client` — passed.
- `cargo test -p dragonfly-client` — **77 passed, 0 failed, 1 ignored**.
- `cargo fmt --all -- --check` — passed.
- `cargo clippy -p dragonfly-client -- -D warnings` — passed.
- Focused streaming lib tests — **8 passed**.
- Focused dfcache import tests — **5 passed, 1 filtered out**.

Logs are retained locally under `artifacts/logs/`. A live Kubernetes cluster, remote scheduler, physical disk-full condition, and multi-gigabyte performance were not exercised.
