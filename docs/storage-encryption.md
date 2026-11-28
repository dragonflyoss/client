# Storage Encryption

Dragonfly Client can encrypt task, persistent task, and persistent cache task content before writing it to the peer cache. Encryption is disabled by default and applies to content files, not metadata.

## Configuration

Use exactly one of `keyFile` or `keyEnv` when encryption is enabled. Startup fails if neither or both are configured, if the key cannot be read, or if the key has an invalid length. The key value is never a configuration field.

### Key file

```yaml
storage:
  encryption:
    enable: true
    keyFile: /run/secrets/dragonfly-storage-key
```

A key file can contain either 32 raw bytes or 64 hexadecimal characters. Generate a raw key with restrictive permissions:

```bash
umask 077
openssl rand -out /run/secrets/dragonfly-storage-key 32
```

To generate a hexadecimal key file instead:

```bash
umask 077
openssl rand -hex 32 > /run/secrets/dragonfly-storage-key
```

### Environment variable

```yaml
storage:
  encryption:
    enable: true
    keyEnv: DRAGONFLY_STORAGE_KEY
```

The environment variable must contain a 64-character hexadecimal key. For example, generate a value for a new deployment with:

```bash
export DRAGONFLY_STORAGE_KEY="$(openssl rand -hex 32)"
```

Provision the value through the deployment secret manager so the same key remains available after restarts. Do not put the key value in YAML, command-line arguments, logs, or source control.

## Algorithm and disk format

Content is encrypted with XChaCha20-Poly1305. Its 192-bit nonces support random nonce generation without persistent nonce state, and the algorithm does not depend on AES CPU instructions. Every 4 MiB plaintext chunk has a fresh random 24-byte nonce for every write and a 16-byte authentication tag. A random 16-byte file ID separates AAD for different files.

The file starts with a 48-byte header:

| Offset | Size | Meaning |
| --- | ---: | --- |
| 0 | 8 | `DFENC001` magic and format version |
| 8 | 4 | Little-endian plaintext chunk size |
| 12 | 8 | Little-endian plaintext content length |
| 20 | 16 | Random file ID |
| 36 | 12 | Reserved, zero |

Each fixed-size record contains a 24-byte nonce, a 4-byte little-endian plaintext length, a 4 MiB ciphertext area with zero padding in the last record, and a 16-byte tag. New files also initialize every unwritten chunk with an authenticated 44-byte placeholder, allowing partial writes without treating attacker-supplied zero records as valid plaintext.

The AAD is the complete header, the little-endian 64-bit chunk index, and the little-endian 32-bit plaintext chunk length. Authentication covers the nonce-associated ciphertext, stored length through the AAD, file identity, chunk position, and chunk length. Invalid headers, physical lengths, ciphertext, tags, or keys return an error.

For plaintext length `L` and chunk size `C = 4,194,304`, physical length is:

```text
48 + ceil(L / C) * (C + 44)
```

The steady-state overhead is 44 bytes per 4 MiB chunk plus the 48-byte file header.

## Compatibility and exports

- Disabled encryption preserves the existing plaintext format and remains the default.
- Enabling encryption does not migrate existing plaintext. Existing plaintext content is rejected when opened as encrypted content; start with an empty content directory or migrate data outside the running daemon before enabling it.
- Retained encrypted content is also rejected when encryption is later disabled. Re-enable encryption with the same key, or deliberately clear the content directory. Losing or changing the key makes existing encrypted content unreadable.
- Copy exports decrypt and authenticate content before writing plaintext to the requested destination.
- Hard-link imports and exports are rejected while encryption is enabled because a hard link would expose ciphertext or bypass encryption. Use the corresponding copy operation instead.
- Linux TCP uploads use buffered decrypted reads for encrypted content instead of `sendfile`. Plaintext uploads retain the `sendfile` path. Other upload protocols consume the same decrypting reader.

## Performance and resource use

Chunking avoids loading whole files into memory. Encryption, decryption, and hashing add CPU work, and a partial-chunk write requires an authenticated read-modify-write of that chunk. Per-operation temporary memory is bounded by the chunk size and reader buffer; encrypted readers use a buffer of at least one chunk to avoid repeatedly decrypting the same chunk for small reads.

Creating an encrypted file preallocates its physical length and writes one 44-byte authenticated placeholder per chunk. Encrypted TCP upload does not use zero-copy `sendfile`. These CPU, allocation, and buffered-I/O costs should be benchmarked with the deployment's hardware and workload before enabling encryption broadly. No throughput benchmark result is claimed by this document.
