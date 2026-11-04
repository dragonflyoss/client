# Issue #4027 implementation report

## Summary

This change adds daemon-wide `storage.type: memory` configuration and a bounded normal-Task content backend with piece storage, task-level LRU eviction, TTL expiration, and periodic GC. Existing disk storage remains the default. This is a partial integration: normal peer Task content is connected, while persistent task families, CacheTask, per-request selection, and memory-backed metadata are not.

## Changed files

- `dragonfly-client-config/src/dfdaemon.rs`: added `StorageType::{Disk, Memory}`, `storage.capacity`, a `64MiB` default, exact byte serialization, and validation for the inclusive range `1 byte..=1TiB`. Added configuration parsing, default, round-trip, missing-capacity, invalid-value, and boundary tests.
- `dragonfly-client-storage/src/content_memory.rs`: added the thread-safe memory content backend. Tasks reserve their declared content length, pieces are stored as `Bytes`, successful reads/writes refresh LRU and TTL state, overlapping or out-of-bounds writes are rejected, expired entries are unreadable, and GC releases expired reservations. Added LRU, TTL, concurrency, bounds, and disk/memory contract tests.
- `dragonfly-client-storage/src/lib.rs`: selected the memory backend from configuration for normal Task capacity checks, creation, source/parent piece writes, uploads, range reads, output copies, and deletion. Capacity and TTL eviction remove local task/piece metadata to prevent stale memory hits. Disk-backed persistent APIs are unchanged.
- `dragonfly-client-storage/src/io.rs`: extended `RangeReader` with a byte-backed source while preserving file reads and added memory reader tests.
- `dragonfly-client-storage/src/server/tcp.rs`: retained `sendfile` for file readers and added `copy_buf` for memory readers, with a real TCP loopback test.
- `dragonfly-client/src/gc/mod.rs`: reused `gc.interval` for memory TTL collection in memory mode and retained scheduler deletion notifications for TTL-expired tasks. Disk GC behavior is unchanged.
- `docs/memory-storage.md`: documented configuration, capacity/TTL/LRU semantics, connected paths, and limitations.
- `dragonfly-client-storage/Cargo.toml`: enabled the existing Tokio `test-util` feature only for dev-dependencies so TTL tests can use deterministic virtual time. No new crate was added and `Cargo.lock` is unchanged.

## Connected entry points

The following existing normal-Task paths inherit memory content through the shared `Storage` facade when `storage.type` is `memory`:

- Normal download gRPC and proxy downloads through the normal Task manager, including source and parent piece writes.
- Normal upload gRPC and local range reads through `Storage::upload_piece`.
- TCP peer uploads through the memory `RangeReader`/`copy_buf` path.
- QUIC peer uploads through the existing `RangeReader::read_chunk` interface.
- Whole-task output copy and normal task deletion. Memory hard links return unsupported so the existing non-forced copy fallback can be used.
- Normal Task capacity eviction with local metadata cleanup.
- Periodic memory TTL GC and its scheduler deletion notification.

Verification is at storage/API level plus a TCP loopback test and daemon compilation. No full multi-process daemon/scheduler end-to-end test was run, and no dedicated QUIC end-to-end test was added.

## Entry points not connected

- Persistent task download/upload/read APIs continue to use disk.
- Persistent-cache task download/upload/read APIs continue to use disk.
- The separate CacheTask cache and its unfinished gRPC paths are unchanged; `storage.cacheCapacity` remains independent.
- Request-scoped storage selection is not implemented; the selection is daemon-wide only.
- Metadata remains in RocksDB and explicit output files remain on disk, so this is not a complete filesystem bypass.
- Memory content cannot survive restart; `storage.keep` does not persist it. Stale metadata is not exposed as a memory hit.
- Capacity-triggered eviction cleans local metadata but does not notify the scheduler. TTL eviction through the daemon GC loop does notify the scheduler.
- Memory hard links are unsupported.

## Memory semantics

- `capacity` limits declared Task content reservations, not allocator or metadata overhead. A Task larger than total capacity is rejected.
- New Task creation removes expired entries and then evicts whole least-recently-used Tasks until the new reservation fits.
- Successful piece reads, writes, and Task output copies refresh recency and last-access time.
- `gc.policy.taskTTL` is measured from last access. Expired Tasks fail reads immediately; periodic GC and Task creation physically reclaim them.
- Piece writes are bounded by the Task reservation, and overlapping distinct pieces are rejected so stored piece bytes cannot exceed the reservation under concurrent operations.

## Verification

Environment used for every final command:

```bash
export PATH=$HOME/.cargo/bin:$PATH
export CARGO_TARGET_DIR=/home/ps/LHL/target-dfc
export LIBCLANG_PATH=/usr/lib/llvm-18/lib
```

| Command | Real result | Log |
| --- | --- | --- |
| `cargo build -p dragonfly-client-storage -p dragonfly-client-config` | Passed; `Finished dev profile` | `artifacts/logs/build.log` |
| `cargo test -p dragonfly-client-config` | **35 passed, 0 failed**; doc-tests 0 | `artifacts/logs/config-test.log` |
| `cargo test -p dragonfly-client-storage` | **119 passed, 0 failed**; doc-tests 0 | `artifacts/logs/storage-test.log` |
| `cargo fmt --all -- --check` | Passed with no output | `artifacts/logs/fmt.log` |
| `cargo clippy -p dragonfly-client-storage -- -D warnings` | Passed; `Finished dev profile` | `artifacts/logs/clippy-storage.log` |
| `cargo build -p dragonfly-client` | Passed; verifies the daemon GC integration compiles | `artifacts/logs/build-client.log` |

The full workspace test suite was not run. Earlier development logs, including the initial Tokio feature and serialization failures that were fixed, remain under `artifacts/logs/`; the table above references final passing logs only. `git diff --check` also passed.
