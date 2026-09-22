# Python S3 range reader

Read byte ranges from S3 through dfdaemon's v2 gRPC API, using the generated
[dragonfly-api](https://pypi.org/project/dragonfly-api/) Python bindings. The
reader implements `read`, `readinto`, `seek`, and `tell`, so a library that
accepts a seekable binary file can use Dragonfly's piece cache and P2P downloads.
The optional video example decodes selected time windows without first
materializing the entire object.

## Requirements

- Python 3.11+ and a configured dfdaemon with access to S3 and its scheduler.
- A trusted local dfdaemon Unix socket, normally
  `/var/run/dragonfly/dfdaemon.sock`.
- AWS credentials available through boto3's normal credential provider chain.
- Immutable S3 object keys. This example does not pin a version on source GETs.

Tested with official dfdaemon v1.5.5 and dragonfly-api 2.3.7. Older daemon
versions may not implement the scheduling policy used for small-range peer
lookup. This example does not change or deploy the daemon.

## Read bytes

From this directory:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
```

```python
from reader import open_s3_reader

with open_s3_reader("s3://my-bucket/immutable/data.bin", region="us-west-2") as source:
    # Exact bytes [5 MiB, 6 MiB), clipped at EOF; does not move tell().
    data = source.read_range(start=5 * 1024 * 1024, length=1024 * 1024)

    source.seek(100)
    next_bytes = source.read(4096)
```

Use `socket="/mounted/dfdaemon.sock"`, `profile="my-profile"`, or
`endpoint="https://my-s3-compatible-host"` as needed. The endpoint must be
reachable and its TLS certificate trusted by both Python and dfdaemon.
The boto3 metadata HEAD goes directly to S3; object data goes through gRPC.
If your environment uses a dfdaemon HTTP proxy, configure `NO_PROXY` or use a
separate environment so the metadata HEAD can reach S3 directly.

Use URL-encoded object keys (for example, `%20` for a space and `%25` for a
literal percent sign). This example accepts ordinary bucket names and file
keys, and rejects query strings, fragments, empty or dot path segments, and
backslashes to avoid path normalization changing which object is downloaded.

Create one reader per worker after forking; a reader is not thread-safe.
Keep it open across related reads to reuse its small application cache.
Intervals larger than the default 64 MiB allocation limit must be consumed in
smaller windows. An unbounded `read()` is rejected if it exceeds that limit.

## Optional video windows

Only this example and its test require PyAV:

```bash
python -m pip install -r requirements-video.txt
python video_chunks.py s3://my-bucket/immutable/video.mp4 \
  --region us-west-2 --start 7200 --chunk-seconds 60 --chunks 2
```

This decodes windows 02:00:00–02:01:00 and 02:01:00–02:02:00, printing frame
counts, timestamps, and gRPC transfer counters. `frames_in_window` yields
`(timestamp, frame)` pairs for use in a training loader. It selects the first
video stream and does not return audio or write standalone video clips.

S3 Range addresses **bytes, not seconds**. PyAV uses the video container index
to map timestamps to byte reads and seeks to an earlier keyframe for decoding.
Use a seekable, indexed container such as MP4. Metadata, keyframes, read-ahead,
and full Dragonfly pieces can require more bytes than the selected frames.
If you already have a byte-offset index, use the reader without PyAV.

## Download behavior

- Each RPC sends the same object URL/tag with a different `Download.range`,
  `need_piece_content=true`, `prefetch=false`, and a fixed 4 MiB piece length.
  Ranges do not create separate object tasks. No output file is requested,
  though dfdaemon still caches pieces on disk.
- The daemon returns complete pieces with absolute offsets, possibly out of
  order. The reader validates metadata and coverage, reorders and trims pieces,
  and waits for a successful final RPC status before returning or caching bytes.
- `scheduling_policy="always"` enables peer lookup for small reads. On v1.5.5,
  `auto` bypasses the scheduler for uncached ranges <= 4 MiB. `always` can add
  scheduling latency; it neither guarantees a peer hit nor disables source
  fallback. The video CLI also exposes `--scheduling-policy auto`.
- The Unix channel sets `grpc.default_authority=localhost` for compatibility
  with the Rust HTTP/2 server and raises the receive limit above a 4 MiB piece
  plus protobuf metadata. This authority is unrelated to S3's Host header.
- Up to four full pieces (16 MiB) are cached in Python. Peak memory also includes
  the requested result, assembly copies, protobuf messages, and decoder buffers.
- Boto3 resolves a fresh frozen credential snapshot for each uncached RPC,
  including the STS session token. Refreshable providers can renew between
  RPCs; static credentials cannot refresh themselves. Credentials already sent
  on an in-flight RPC are not refreshed. Errors propagate without hidden retries.
- dfdaemon signs its actual S3 piece requests using those credentials. This
  does not forward a caller's pre-signed HTTP request or implement a proxy.

The cache tag includes endpoint, region, HEAD ETag, and HEAD version ID. It stays
stable across ranges and credential rotations. Other clients need the same tag
and piece length to share task identity. HEAD and source reads are separate
operations: this tag does not pin a version or detect a same-size overwrite
while a reader is open. Use immutable keys.

The daemon receives AWS credentials. Protect its socket and use your deployment's
isolation model. The tag namespaces cached data; it is not an authorization
boundary or a guarantee of authorization checks on every cached read.

## Tests

```bash
python -m unittest discover -s tests -v
```

The core tests use real protobuf/gRPC streams with a local test server. They
exercise unaligned and out-of-order pieces, EOF and seek behavior, bounded
caching, credential rotation, URL handling, malformed responses, and errors
reported after the last piece. The optional video test compares two windows
from an indexed three-hour synthetic MP4 against a sequential local decode;
it is skipped when PyAV is absent. No AWS credentials or running daemon are
needed for these tests.

Additional manual validation used two real v1.5.5 daemons with separate caches,
a scheduler, and MinIO: signed ranged GETs, STS, source/remote/local traffic,
and partial video reads. This is S3-compatible functional validation, not an
AWS service test or a video throughput benchmark.
