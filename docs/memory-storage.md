# Peer memory storage

Peer tasks can keep their content in process memory instead of the normal task content files. Disk remains the default storage type.

## Configuration

```yaml
storage:
  type: memory
  capacity: 1GiB

gc:
  interval: 15m
  policy:
    taskTTL: 24h
```

`capacity` is the maximum reserved task-content size in bytes. It accepts an integer byte count or a size string such as `512MiB`. The default is `64MiB`; values must be greater than zero and no larger than `1TiB`. The configured limit covers task content reservations, not allocator or metadata overhead, so the daemon needs additional memory headroom.

`storage.cacheCapacity` is independent. It configures the separate CacheTask cache and does not set the peer memory-storage limit.

## LRU and TTL behavior

Each task reserves its declared content length when the download starts. Piece bytes are held in memory and writes must remain within that reservation. A task larger than the total capacity is rejected. When a new task does not fit, complete least-recently-used tasks are evicted until it fits. Successful piece reads and writes refresh the task's LRU position and last-access time. Eviction also removes the corresponding local task and piece metadata so stale metadata cannot produce a memory-cache hit.

`gc.policy.taskTTL` controls expiration. An expired task cannot be read even before its bytes are physically reclaimed. The existing daemon GC loop reclaims expired memory tasks every `gc.interval`; creating a task also removes expired entries before capacity eviction. Reads and writes therefore provide lazy expiration checks, while periodic GC releases reserved capacity.

## Connected paths and limitations

The `memory` setting applies to normal peer Task content. Normal source and parent downloads, local range reads, proxy reads, TCP and QUIC peer uploads, task-output copies, and normal task deletion use the memory content backend. Linux TCP uses buffered socket copying for memory readers and retains `sendfile` for file readers. Hard links from memory content are unsupported; callers must use the existing copy fallback.

Persistent tasks, persistent-cache tasks, and the separate CacheTask API retain their existing storage behavior. Request-scoped storage selection is not added; selection is daemon-wide through `storage.type`. Task metadata and explicit output files still use disk. Capacity eviction currently cleans local metadata but does not send a scheduler deletion notification; TTL evictions from the daemon GC loop do send that notification.

Memory content is volatile and is lost when the process restarts. `storage.keep` cannot preserve memory bytes. Existing metadata is not treated as a memory-cache hit after restart. This backend is intended for short-lived peer caches with sufficient RAM, not durable storage.
