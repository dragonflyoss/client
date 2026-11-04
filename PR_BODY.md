## Summary

- Add `disk`/`memory` peer storage selection with a validated byte capacity (`64MiB` default, maximum `1TiB`).
- Add a thread-safe normal-Task memory content backend with piece storage, task-level LRU capacity eviction, TTL expiration, and periodic GC.
- Route normal Task downloads, local/proxy reads, and TCP/QUIC uploads through memory when configured, while keeping disk as the default.

## Changes

- Add configuration serde/default/validation coverage and exact capacity serialization.
- Store pieces as `Bytes`, reserve declared Task lengths, refresh recency on reads/writes, reject invalid ranges and overlapping writes, and clean local metadata on eviction.
- Extend `RangeReader` with a memory source; Linux TCP uses `copy_buf` for memory and retains `sendfile` for files.
- Reuse `gc.policy.taskTTL` and `gc.interval` for memory expiration and reclamation.
- Document configuration, semantics, and limitations.

This is intentionally a partial integration. Persistent tasks, persistent-cache tasks, CacheTask, request-scoped selection, and memory-backed metadata are not connected. Metadata and output files still use disk; hard links are unsupported; restart persistence is unavailable. Capacity eviction cleans local metadata without scheduler notification, while periodic TTL GC retains scheduler notification.

## Test plan

- `cargo build -p dragonfly-client-storage -p dragonfly-client-config` — passed.
- `cargo test -p dragonfly-client-config` — 35 passed, 0 failed.
- `cargo test -p dragonfly-client-storage` — 119 passed, 0 failed.
- `cargo fmt --all -- --check` — passed.
- `cargo clippy -p dragonfly-client-storage -- -D warnings` — passed.
- `cargo build -p dragonfly-client` — passed.

The tests cover configuration defaults and invalid values, serialization round trips, LRU order, TTL readability and reclamation, concurrent reads/writes and capacity bounds, disk/memory operation parity, memory readers, local metadata cleanup, and TCP memory streaming. Full multi-process daemon/scheduler and dedicated QUIC end-to-end tests were not run.
