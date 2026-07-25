# RDMA Peer-to-Peer Piece Transport

Peers normally fetch pieces from each other over the TCP piece server. On a host with an RDMA
fabric — AWS EFA, or RoCE/InfiniBand — a single TCP flow leaves most of that fabric idle, and the
piece protocol opens one connection per piece, so the shortfall shows up as slow model and image
distribution exactly where the hardware was bought to avoid it.

This document describes an optional transport that carries piece bytes over the fabric instead.
It is disabled by default, requires a build flag, and always keeps the TCP piece server as a
per-piece fallback.

## Design

Both AWS EFA and conventional RDMA are driven through **libfabric** rather than ibverbs directly.
EFA is not an ibverbs RC transport — it uses Scalable Reliable Datagram and has no reliable
connected queue pairs — so an RC-based ibverbs implementation cannot drive it. libfabric abstracts
both behind one API, which is why it is the single stack here.

The transport splits into two planes:

- **Control plane** — piece request, capability negotiation, metadata, flow control and errors —
  travels over a TCP rendezvous connection. TCP already gives reliable, ordered, framed delivery
  for small messages, and reusing it keeps error reporting legible.
- **Data plane** — the bulk piece bytes — travels over **two-sided tagged messaging** on a shared
  `FI_EP_RDM` endpoint.

Two-sided messaging is deliberate. One-sided RDMA READ/WRITE would require exposing remote-access
memory keys to peers, which in a P2P system means exposing them to whoever can reach the daemon.
With two-sided tagged messages the receiver posts its own buffers and no remote key ever leaves
the process, so a peer can at worst write into a buffer the receiver already offered it.

### Discovery

RDMA capability is discovered on the TCP piece port that peers already know about, rather than
through a new announcement field or a scheduler change. A client opens the ordinary piece
connection and sends a four-byte discriminator; the server peeks at it and, if it matches, answers
with its RDMA capability instead of the piece protocol.

Discovery is **fail-closed**: the client uses RDMA only when it gets a positive, current answer.
Anything else — an older peer that does not understand the discriminator, a peer built without the
feature, a peer whose fabric has failed — leaves the client on TCP. Capability answers are cached
for 60 seconds so discovery costs one extra round trip per parent per minute, not per piece.

### Capability negotiation

Two peers may only speak RDMA when they agree on:

- the concrete libfabric provider (`efa`, `verbs;ofi_rxm`, ...), and
- a non-empty, identical **fabric tag**.

The fabric tag is an operator-supplied label for a reachability domain. libfabric will happily
report a working provider for two nodes that cannot actually reach each other, and there is no
portable way to ask "is this peer on my fabric?" — so the operator asserts it. On EFA this should
identify a VPC and Availability Zone. It should *not* identify a placement group: placement groups
are a latency recommendation, not a reachability boundary, and using one as the tag needlessly
prevents RDMA between nodes that can talk perfectly well.

`provider: auto` is resolved to a concrete provider at startup and only the resolved name is
advertised, so `auto` is never a wire value and two peers cannot "agree" on it while running
different hardware.

### Transferring a piece

A transfer moves in **windows**. A window is `chunkSize × maxInflightChunks` bytes, split into
chunks that each become one tagged message. Each transfer reserves a disjoint block of tags, so
concurrent transfers cannot land in each other's buffers.

```
client                                                     parent
  |  Request(task, piece, capability, endpoint, tag)  ->     |
  |  <-  Ready(offset, length, digest)                       |
  |                                                          |
  |  [post receives for window 0]                            |
  |  RecvPosted(start_chunk, chunk_count)             ->     |
  |  <-  ============ tagged chunks (fabric) ============    |
  |  [post receives for window 1]                            |
  |  RecvPosted(...)                                  ->     |
  |  <-  ============ tagged chunks (fabric) ============    |
  |  <-  Done                                                |
```

The `RecvPosted` frame is the flow control. The parent may not send a window until the client says
its receives are posted, which is what makes unsolicited traffic impossible and keeps the parent
from staging bytes the client has nowhere to put. The parent computes each window itself and
requires the client's frame to match exactly, so a peer cannot replay a window, skip ahead, or
claim more chunks than the piece contains.

The receiver keeps two windows posted where the registration budget allows, so the next window's
receives are already posted when the current one completes. If the budget will not stretch to two,
the transfer continues at one window at a time rather than failing.

### Receive path

Received windows are handed to storage **still resident in registered memory**. The digest and the
`pwrite` both read the window in place, so a piece goes from the NIC to the page cache without an
intermediate bounce buffer.

Two consequences are worth knowing, because they constrain the code:

- The window write loop must never be wrapped in a cancelling timeout. It hands each window to
  blocking threads that cannot be aborted, and a write abandoned between spawn and join could land
  after a TCP fallback had already rewritten the same range. The timeout instead bounds the wait
  for each window.
- A window is published to the consumer only after every receive completion over it has been
  reaped, which is what makes it safe for the digest and the write to read it concurrently.

### Upload path

By default the parent streams piece bytes through the existing upload path, so cache-resident
pieces and every other storage nicety keep working. With `mmapContent: true` the parent instead
memory-maps the finished on-disk piece and fills the registered send ring straight from that
mapping, removing the read-buffer copy. Mapping or registration failures fall back to the
streaming reader, and cache-resident pieces always use the reader.

### Registered memory

Pinning memory for the NIC is expensive, and pinning too much of it is antisocial on a shared
node. Registrations are therefore pooled and bounded by `maxRegisteredBytes`, with buffers reused
across transfers on a best-fit basis. Exhausting the budget degrades a transfer to a single window
or defers it; it never fails a download outright.

### Failure handling

Every RDMA failure falls back to the TCP piece server for that piece. That is the invariant the
rest of the design is arranged around, and it is why the feature can be enabled without changing
the availability story.

Beyond per-piece fallback:

- A parent that reports incompatibility is not retried for 60 seconds.
- A parent whose transfers fail is backed off, doubling from 2 seconds to a 60-second ceiling.
- A local fabric that suffers an unrecoverable completion-queue or cancellation failure is retired
  and rebuilt no more often than every 5 minutes; until then the daemon simply uses TCP.
- On teardown, `fi_close` is not assumed to be a DMA barrier. If the provider does not promise that
  the device has stopped, buffers with operations still outstanding are quarantined for the
  process lifetime rather than returned to the allocator, because handing memory a NIC may still
  write into back to the heap is worse than leaking it.

## Enabling it

RDMA is behind the `rdma` cargo feature, which is Linux-only and needs libfabric headers and
library at build time:

```bash
# Debian/Ubuntu
apt install libfabric-dev

cargo build --release --features rdma
```

Serving and downloading are enabled independently. A daemon can serve RDMA to peers that want it
while still downloading over TCP itself.

```yaml
storage:
  server:
    rdma:
      enable: true                 # serve pieces over RDMA
      fabricTag: vpc-abc123-use1a  # required; must match on both peers

download:
  protocol: rdma                   # download pieces over RDMA when the parent supports it
```

### Configuration reference

All settings live under `storage.server.rdma`. Everything except `enable` also applies when this
daemon is downloading over RDMA.

| Option | Type | Default | Description |
|---|---|---|---|
| `enable` | bool | `false` | Serve pieces over RDMA. Downloading is selected separately with `download.protocol: rdma`. |
| `port` | u16 | `4007` | TCP rendezvous port. Carries control messages only; piece bytes go over the fabric. |
| `provider` | enum | `auto` | `auto`, `efa`, or `verbs`. `auto` probes hardware providers in preference order. |
| `allowSoftwareProvider` | bool | `false` | Permit software providers such as `tcp` under `auto`. Development and CI only. |
| `device` | string | unset | Pin a libfabric domain, for example `efa_0-rdm` or `rdmap16s27`. |
| `fabricTag` | string | unset | Reachability-domain label. RDMA is attempted only when both peers advertise the same non-empty value. Required to serve. |
| `maxRegisteredBytes` | size | `512MiB` | Ceiling on memory pinned by active and pooled transfer buffers. Must be at least `chunkSize × maxInflightChunks`. |
| `chunkSize` | size | `4MiB` | Size of one tagged message. Between 64KiB and 1GiB; clamped to the provider maximum at runtime. |
| `maxInflightChunks` | u32 | `16` | Chunks posted concurrently for one piece; 1–4096. Peers negotiate the lower value. |
| `maxConcurrentTransfers` | u32 | `64` | Concurrent rendezvous transfers served. Excess peers are told the parent is busy and fall back to TCP. |
| `transferTimeout` | duration | `10s` | Maximum life of one fabric operation before cancellation and TCP fallback. Between 1s and 10m. |
| `mmapContent` | bool | `false` | Fill send windows from a memory map of the piece instead of streaming through a reader. |

Settings that parse individually but cannot work together are rejected at load time. The one worth
calling out is a registration budget smaller than a single window: it admits no transfer at all, so
every piece would pay a rendezvous round trip and a rejection before falling back to TCP.

## Measured behaviour

Two `p6-b200.48xlarge` nodes on EFA, one rail, 24 GiB of 512 MiB pieces served from tmpfs, best of
three runs at each concurrency:

| Concurrent pieces | 1 | 2 | 4 | 8 | 16 | 32 |
|---|---|---|---|---|---|---|
| RDMA, transport only | 44.7 | 78.9 | 122.5 | 198.9 | 261.0 | 277.0 |
| TCP, transport only | 4.1 | 8.0 | 14.6 | 27.6 | 50.2 | 74.4 |
| **Speedup** | **10.8×** | **9.9×** | **8.4×** | **7.2×** | **5.2×** | **3.7×** |
| RDMA, CRC32 + write | 22.1 | 39.0 | 72.0 | 118.1 | 139.2 | 130.1 |
| TCP, CRC32 + write | 3.5 | 6.8 | 13.1 | 24.2 | 42.3 | 61.0 |
| **Speedup** | **6.2×** | **5.7×** | **5.5×** | **4.9×** | **3.3×** | **2.1×** |

Figures are Gbps. "CRC32 + write" is the work `dfdaemon` actually does per piece; "transport only"
isolates the wire.

The advantage is largest where it matters most and narrows as concurrency grows. A single TCP flow
on this network tops out near 5 Gbps, so TCP scales almost linearly with the number of piece
connections while RDMA is already close to saturated. Above roughly 16 streams the RDMA side is
bounded by receive-side CPU — the digest and the write — not by the fabric, which accounts for well
under 1% of a transfer.

## Limitations

- Linux only, and only on a host with a libfabric-supported RDMA device.
- Measured on a single EFA rail; the transport does not yet stripe across multiple devices.
- `fabricTag` is an operator assertion. There is no automatic verification that two peers tagged
  alike can actually reach each other; a wrong tag produces a rendezvous failure and a TCP
  fallback rather than a hang.
- The fabric is unauthenticated, as fabrics generally are. A peer on the same fabric could aim
  bytes at another peer's endpoint; the digest check catches it, so the effect is a failed
  download and a TCP retry rather than corruption.
