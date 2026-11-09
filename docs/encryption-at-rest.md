# Encryption at Rest

dfdaemon can encrypt the task content it caches on disk, so a stolen,
re-mounted or improperly disposed disk does not expose the distributed
artifacts. The encryption is native to the client and does not depend on
LUKS, dm-crypt or encrypted cloud volumes.

## Scope

Encryption at rest protects the **cached content on disk** of the tasks, the
persistent tasks and the persistent cache tasks. It does not change the P2P
protocol: a peer that is authorized to download a piece still receives the
plaintext over the (optionally mTLS protected) transport, exactly as without
encryption, and the piece digests exchanged between peers are still the
digests of the plaintext. Distributing ciphertext end to end between peers is
a separate design.

The in-memory piece cache (`storage.cacheCapacity`) holds plaintext, as it
never reaches the disk. The task metadata stored in RocksDB (task ids, piece
digests, response headers) is not encrypted by this feature.

## Design

- **Per-object data encryption keys.** Every task content file is encrypted
  with its own random 256-bit data encryption key (DEK). Compromising one DEK
  exposes one task only.
- **AES-256-GCM over authenticated chunks.** The plaintext of a piece is split
  into 64 KiB chunks, each encrypted and authenticated independently with a
  random nonce. A chunk is stored as `nonce || ciphertext || tag`, and its
  plaintext offset is bound as associated data, so chunks cannot be swapped
  within a file. Tampered or truncated chunks fail to authenticate and are
  reported as errors instead of being served.
- **Envelope encryption through a KMS.** The DEK is wrapped by a key encryption
  key (KEK) managed by the configured key management service and stored in a
  4 KiB header at the start of the content file, together with the layout
  parameters. The wrapping is bound to the identity of the file (the kind of
  task and the task id) and to the layout parameters, so a content file moved
  to another task, or a header with altered parameters, fails to unwrap its
  DEK. Any dfdaemon holding the KEK can read the file, so the storage can be
  kept across restarts (`storage.keep: true`).
- **Piece-based layout.** The pieces are encrypted independently and stored in
  disjoint regions of the file at a fixed stride, so concurrent and
  out-of-order piece writes, partial downloads and random-access range reads
  within a piece keep working unchanged. A range read decrypts only the chunks
  covering it.

```text
+--------+------------------------------+------------------------------+-----
| header | piece 0                      | piece 1                      | ...
|        | chunk 0  | chunk 1  | ...    | chunk 0  | chunk 1  | ...    |
|        | n|ct|tag | n|ct|tag |        | n|ct|tag | n|ct|tag |        |
+--------+------------------------------+------------------------------+-----
```

The disk overhead is 4 KiB per task plus 28 bytes per 64 KiB chunk (about
0.04%).

## Configuration

```yaml
storage:
  # Keep the encrypted content across restarts, the key encryption key is
  # enough to read it again.
  keep: true
  encryption:
    enable: true
    kms:
      # The provider of the key encryption key, only `file` is supported now.
      provider: file
      file:
        # The active key encryption key: 32 raw bytes, 64 hex characters or
        # standard base64, e.g. a mounted Kubernetes secret.
        keyFile: /etc/dragonfly/kek.key
        # The previous key encryption keys, only used to unwrap the data keys
        # of the tasks created before a rotation.
        previousKeyFiles:
          - /etc/dragonfly/kek-old.key
```

A key encryption key is generated with, for example:

```shell
head -c 32 /dev/urandom | base64 > /etc/dragonfly/kek.key
chmod 0400 /etc/dragonfly/kek.key
```

dfdaemon refuses to start when the encryption is enabled and the active key
file is missing or invalid. The hex and base64 forms are decoded in constant
time, so the decoding does not leak the key through timing.

A key file is also rejected when it cannot be a random key, as an offline
attacker holding the disk can test guesses of a weak key encryption key against
the header of any content file at the speed of AES-GCM:

- a key with fewer than 8 distinct byte values, e.g. a file padded with zeros
  or a short passphrase repeated to 32 bytes;
- a 32 byte file that is entirely printable ASCII, which is a text passphrase
  and not 32 random bytes (a random key is printable with a probability of
  about 1e-14).

Use 32 bytes from a CSPRNG, as in the command above, or a key from your secret
manager. The checks run without branching on or indexing by the key bytes.

### Key rotation

1. Generate a new key and configure it as `keyFile`, moving the old key to
   `previousKeyFiles`.
2. Restart dfdaemon. New tasks are wrapped with the new key, and the existing
   tasks stay readable with the old key.
3. Once the tasks created before the rotation are garbage collected (see the
   `gc.policy` TTLs) or re-downloaded, remove the old key from
   `previousKeyFiles`.

### Enabling, disabling or losing the key

At startup, dfdaemon checks the kept tasks against the encryption
configuration and deletes the tasks whose content on disk is unusable: the
plaintext tasks when the encryption is enabled, the encrypted tasks when it is
disabled, and the encrypted tasks whose key encryption key is no longer
available. The deleted tasks are downloaded again on demand.

## Behavior changes with encryption enabled

- **Hard links are refused.** A hard link between the cache and an output path
  would expose the ciphertext (or import a plaintext file into the encrypted
  cache), so dfdaemon copies the decrypted content to the output path instead.
  Download requests with `force_hard_link: true` fail. Importing a local file
  as a persistent task reads it piece by piece instead of linking it.
- **Zero-copy uploads are disabled.** Pieces served over TCP are decrypted in
  user space instead of being sent with `sendfile`, and the pieces served over
  QUIC or gRPC are decrypted the same way.
- **CPU usage.** AES-256-GCM is hardware accelerated on most platforms through
  aws-lc-rs, the encryption and decryption run on the blocking thread pool and
  overlap the network I/O. See [Performance](#performance) for the measured
  cost and the CPU features it depends on.
- **Creation is atomic and idempotent.** The header of a content file is
  written to a private temporary file and published with a hard link, so
  concurrent creators of the same task agree on one data key and the loser
  keeps the winner's file. A readable existing file is never replaced, an
  unreadable one (a plaintext leftover, or a file of a key encryption key that
  is gone) is. The files are created readable by the owner only (`0600`).
- **Start-up self test.** With the encryption enabled, dfdaemon seals and opens
  a few chunks with a throwaway key at start-up, refuses to start if the
  cipher misbehaves, and logs the sealing throughput. It warns when the rate is
  below 1 GiB/s, which means the hardware AES-GCM is not available.

## Performance

The cost of the encryption was measured against the plaintext path with the
benchmark in `dragonfly-client-storage/benches/encryption.rs`
(`cargo bench -p dragonfly-client-storage --bench encryption`), on a 4 vCPU
host with VAES and AVX-512, on tmpfs, with 4 MiB pieces and the default
2 MiB read and write buffers. Every number below is the mean of runs of the
variants interleaved over several rounds, which cancels the drift of a shared
host, with the plaintext path as the control. A tmpfs has no device latency, so
it is the worst case for the relative overhead of the encryption.

| Path | Plaintext | Encrypted | Encrypted vs plaintext |
|---|---|---|---|
| Piece write | 3.27 GiB/s | 2.71 GiB/s | 83% |
| Piece read, memcpy consumer | 3.18 GiB/s | 2.66 GiB/s | 84% |
| Piece read, consumer at 3 GiB/s (about 25 Gbit/s) | 1.92 GiB/s | 1.91 GiB/s | 99% |
| Piece read, consumer at 1 GiB/s (about 8 Gbit/s) | 0.83 GiB/s | 0.84 GiB/s | 100% |

The paced rows simulate an uploader that spends time per chunk, like a network
sender. They are the relevant ones for serving pieces, because the encrypted
reader decrypts the next batch while the consumer is still sending the current
one.

How the path got there, against the first implementation, with the on-disk
format unchanged:

- The write path seals the chunks in place in a staging buffer that already has
  the layout of the ciphertext on disk, which removes a copy of every byte and
  a buffer per in-flight batch: 2.10 to 2.71 GiB/s (+29%).
- The read path no longer zeroes its buffer before every positional read, and
  reads ahead one batch: 1.93 to 2.66 GiB/s (+38%) with a memcpy consumer, and
  1.17 to 1.91 GiB/s (+63%) with a consumer at 3 GiB/s.

### CPU features

The cipher is the AES-256-GCM of aws-lc-rs, which selects its implementation at
run time from the CPU features, so the same binary uses the fastest available
one. The sealing throughput of 64 KiB chunks on the host above, with features
masked through `OPENSSL_ia32cap`:

| Implementation selected | Seal | Open |
|---|---|---|
| VAES, AVX-512 and VPCLMULQDQ | 8.6 GiB/s | 10.0 GiB/s |
| AES-NI with AVX and PCLMULQDQ | 4.6 GiB/s | 5.0 GiB/s |
| No AES-NI, PCLMULQDQ or AVX (software) | 0.31 GiB/s | 0.32 GiB/s |

A CPU, a virtual machine or an emulator that does not expose AES-NI and
PCLMULQDQ (x86-64) or the crypto extensions (aarch64) falls back to software
AES, which is about 28 times slower and becomes the bottleneck of the whole
cache. Some hypervisors mask CPU features by default, e.g. a conservative CPU
model in the VM definition, so check the start-up log line
`AES-256-GCM self test passed, sealing ... GiB/s` of every node class, and the
warning that accompanies a slow rate. Do not build with `target-cpu=native`
for this: the dispatch is at run time and portable binaries keep it.

## Security properties and limits

- **Confidentiality and integrity of the content on disk.** Every 64 KiB chunk
  is authenticated, and its plaintext offset is bound as associated data. A
  flipped bit, a swapped or moved chunk, an unwritten (zeroed) region and a
  truncated file all fail to authenticate and are reported as errors, never
  served as data. The chunk format is plain AES-256-GCM: the chunks of a file
  written by dfdaemon, with the pieces written out of order, are decrypted and
  matched by an independent implementation (Python `cryptography`), and the
  tests carry known-answer vectors generated by it.
- **Why the nonces are random.** A chunk is rewritten when a piece is
  downloaded again, e.g. after a digest mismatch of a corrupted download, and
  the rewrite can carry different bytes. A deterministic nonce derived from the
  position would then reuse a (key, nonce) pair with different plaintexts,
  which breaks AES-GCM. Every write draws a fresh random 96-bit nonce under the
  per-file data key instead. For `n` chunk writes under one key the probability
  of any repeated nonce is about `n^2 / 2^97`: a 1 TiB file written twice
  (`n = 2^25`) is about `7e-15`. The key encryption key wraps one data key per
  task with a random nonce as well, which stays inside the NIST SP 800-38D
  limit of `2^32` invocations for about four billion tasks per key.
- **Not detected: rollback.** Chunks are authenticated against their position
  in the file, not against a version. An attacker who can write to the cache
  directory can restore an older valid copy of a chunk, or of a whole file,
  of the same task. For a content-addressed cache whose content for a task id
  never legitimately changes this is harmless, and it is not a protection
  against an attacker with write access to the running node.
- **Not encrypted: task metadata.** RocksDB keeps the task ids, URLs, response
  headers and the digests of the plaintext. Anyone with the disk can test
  whether a known file is cached by its digest, and can read URLs and any
  credentials sent in stored headers. Encrypting the metadata store is
  separate work.
- **Key material in memory.** The unwrapped data keys are held in memory for
  the open tasks, in a cache bounded to 16384 tasks (a task evicted from it is
  unwrapped again from its header on the next access). The data key is zeroed
  when dropped, and the expanded AES key schedule is zeroed when the cipher is
  freed, as aws-lc scrubs every allocation it frees. The keys of the open tasks
  are in memory while they are open, so disable core dumps and use encrypted
  swap on nodes that hold sensitive content.
- **The header is untrusted until it unwraps.** Its key identifier and wrapped
  key are length bounded and the identifier must be printable before anything
  else reads them, the layout parameters are authenticated by the wrapping
  before they are used, and a content length that would overflow a file offset
  is refused.

## Threat model

Encryption at rest defends against offline access to the disk: disk theft,
snapshots and backups of volumes, improper disposal, or another tenant
reading a shared or re-provisioned disk. It does not defend against an
attacker with code execution as the dfdaemon user or root on a running node,
who can read the key encryption key, the unwrapped data keys in memory, or
simply request the plaintext through the dfdaemon APIs. Protect the key file
with file permissions and a secret manager, and the dfdaemon APIs with mTLS.
