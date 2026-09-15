# RDMA Peer-to-Peer Piece Transport

This experimental, opt-in Linux transport carries peer piece bytes over libfabric while keeping
TCP available for compatible fallback. It targets large immutable artifacts distributed among
hosts already provisioned with a reachable RDMA fabric. Its value depends on the actual network,
storage, CPU load, and peer fan-out; an RDMA-capable NIC alone does not establish a benefit.

The design is under discussion in [RFC #2041](https://github.com/dragonflyoss/client/pull/2041).
Provider support and production readiness require the validation gates in that RFC.

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

The application uses two-sided tagged messages and does not expose application memory keys.
Providers can still use RDMA internally. This choice does not authenticate peers or isolate
mutually untrusted tenants; deployment requires a trusted fabric and appropriate network policy.
CRC32 detects accidental corruption, not malicious substitution.

### Discovery

RDMA capability is discovered on the TCP piece port that peers already know about, rather than
through a new announcement field or a scheduler change. A client opens the ordinary piece
connection and sends a four-byte discriminator; the server peeks at it and, if it matches, answers
with its RDMA capability instead of the piece protocol.

Discovery is **fail-closed**: the client uses RDMA only when it gets a positive, current answer.
Anything else — an older peer that does not understand the discriminator, a peer built without the
feature, a peer whose fabric has failed — leaves the client on TCP. Successful capability answers are cached for 60 seconds. Concurrent cache misses can
still issue multiple probes; coalescing remains an RFC requirement.

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
its receives are posted. This bounds cooperative senders and avoids staging bytes the client
has nowhere to put; it is not an access-control mechanism. The parent computes each window itself and
requires the client's frame to match exactly, so a peer cannot replay a window, skip ahead, or
claim more chunks than the piece contains.

The receiver keeps two windows posted where the registration budget allows, so the next window's
receives are already posted when the current one completes. If the budget will not stretch to two,
the transfer continues at one window at a time rather than failing.

### Receive path

Completed receive windows feed the existing storage stream writer, preserving its writeback,
length, and digest behavior. The transfer owns the piece until any outstanding positional write
has finished, including on receive failure or caller cancellation. TCP fallback starts only after
that cleanup; it retains the same piece claim.

One absolute deadline covers discovery, setup, buffer admission, and RDMA reception. It is not
reset for each window. Disk writes must drain before ownership is released, so an unresponsive
filesystem can exceed the network deadline. RDMA requires a supported, well-formed parent digest;
pieces registered without one use the existing TCP path.

### Upload path

By default the parent streams piece bytes through the existing upload path, so cache-resident
pieces and every other storage nicety keep working. With `mmapContent: true` the parent instead
memory-maps the finished on-disk piece and fills the registered send ring straight from that
mapping, removing the read-buffer copy. Mapping failures fall back to the
streaming reader, and cache-resident pieces always use the reader. Registered-buffer admission
or registration failure follows the normal RDMA failure/TCP fallback path.

### Registered memory

Pinning memory for the NIC is expensive, and pinning too much of it is antisocial on a shared
node. Registrations are therefore pooled and bounded by `maxRegisteredBytes`, with buffers reused
across transfers on a best-fit basis. One shared budget covers upload and download endpoints,
idle pools, and retained buffers from retired endpoints. Idle buffers can be reclaimed across
pools under pressure. Provider-internal allocations are additional and require headroom.
Exhaustion reduces pipelining or triggers bounded TCP fallback.

### Failure handling

RDMA transport failure falls back to a whole-piece TCP attempt after safe cleanup. Local disk
errors and caller cancellation propagate without penalizing the parent or starting a TCP retry.
Parent success is recorded only after receiving, verifying, and committing the complete piece.

Beyond per-piece fallback:

- A parent that reports incompatibility is not retried for 60 seconds.
- A parent whose transfers fail is backed off, doubling from 2 seconds to a 60-second ceiling.
- A local fabric that suffers an unrecoverable completion-queue or cancellation failure is retired
  and rebuilt no more often than every 5 minutes; until then the daemon simply uses TCP.
- Teardown serializes endpoint close with posting and completion handling. After successful
  `fi_close`, outstanding operations release their buffers according to the libfabric API
  contract. If close fails, buffers, contexts, registrations, and shared-budget charges remain
  retained. Provider/version failure-path validation is still required.

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
| `maxRegisteredBytes` | size | `512MiB` | Daemon-wide ceiling on active, idle, and retained application buffer capacities; excludes provider internals. Must be at least `chunkSize × maxInflightChunks`. |
| `chunkSize` | size | `4MiB` | Size of one tagged message. Between 64KiB and 1GiB; clamped to the provider maximum at runtime. |
| `maxInflightChunks` | u32 | `16` | Chunks posted concurrently for one piece; 1–4096. Peers negotiate the lower value. |
| `maxConcurrentTransfers` | u32 | `64` | Concurrent rendezvous transfers served. Excess peers are told the parent is busy and fall back to TCP. |
| `transferTimeout` | duration | `10s` | Absolute RDMA attempt budget covering discovery, setup, admission and reception; disk cleanup may take longer. Between 1s and 10m. |
| `mmapContent` | bool | `false` | Fill send windows from a memory map of the piece instead of streaming through a reader. |

Settings that parse individually but cannot work together are rejected at load time. The one worth
calling out is a registration budget smaller than a single window: it admits no transfer at all, so
every piece would pay a rendezvous round trip and a rejection before falling back to TCP.

## Historical prototype measurements

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

Figures are Gbps, reported for the original prototype before the receive-path changes above.
They have not been reproduced for this revision and do not establish a fleet-level speedup.
The baseline predates current TCP improvements, excludes QUIC and scheduler fan-out, uses large
pieces on tmpfs, and can be affected by EFA-versus-IP capacity differences. Re-run comparisons
against tuned current TCP and QUIC, realistic piece sizes, cold/warm NVMe caches, concurrent
upload/download, and active training workloads before making performance claims.

## Limitations

- Linux only, and only on a host with a libfabric-supported RDMA device.
- Measured on a single EFA rail; the transport does not yet stripe across multiple devices.
- `fabricTag` is an operator assertion. There is no automatic verification that two peers tagged
  alike can actually reach each other; a wrong tag produces a rendezvous failure and a TCP
  fallback rather than a hang.
- No peer authentication, confidentiality, or adversarial integrity guarantee. Use only within
  an operator-controlled trust domain; a matching fabric tag is not a security boundary.
- RDMA traffic may bypass IP-interface counters used for parent load selection. Fabric-aware
  telemetry and an explicit behavior when those counters are unavailable remain RFC gates.
- Kubernetes device allocation, memory-lock limits, and coexistence with training Pods must be
  validated for each deployment; this change does not install a device plugin or DRA driver.
