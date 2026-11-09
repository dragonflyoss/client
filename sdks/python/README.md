# Dragonfly Cache (dfcache) Python SDK

Python SDK for the [Dragonfly](https://d7y.io) persistent cache (P2P-backed
fast read/write cache), built for AI workloads that share models, datasets
and checkpoints across nodes.

The SDK talks to the `dfdaemon` download gRPC service over a unix domain
socket and mirrors the [`dfcache`](https://d7y.io/docs/reference/commands/client/dfcache/)
CLI: `import`, `export` and `stat`, plus `delete` which the CLI does not expose.

## Installation

```shell
pip install dragonfly-cache
```

> Prerequisite: a running [dfdaemon](https://d7y.io/), by default listening on
> the unix domain socket `/var/run/dragonfly/dfdaemon.sock`.

## Quick Start

```python
from datetime import timedelta

from dragonfly_cache import DfcacheClient

# Connect to dfdaemon via /var/run/dragonfly/dfdaemon.sock by default.
with DfcacheClient() as client:
    # Import a local file (dfcache import). For large files always provide
    # content_for_calculating_task_id, otherwise dfdaemon hashes the whole
    # file to compute the task id.
    task = client.import_file(
        "/data/model.safetensors",
        content_for_calculating_task_id="deepseek-ai/deepseek-ocr/weights",
        tag="llm",
        application="training",
        replica_count=3,
        ttl=timedelta(hours=6),
    )
    print(task.id)

    # Export it anywhere in the P2P network (dfcache export).
    client.export_file(task.id, output="/tmp/model.safetensors")

    # Query task information (dfcache stat).
    info = client.stat(task.id)
    print(info.state, info.content_length, info.ttl)

    # Free the cache when done.
    client.delete(task.id)
```

## API

### `DfcacheClient(endpoint=..., upload_endpoint=..., timeout=...)`

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `endpoint` | `str \| PathLike` | `/var/run/dragonfly/dfdaemon.sock` | dfdaemon download service unix socket. A `unix:` prefix is accepted. |
| `upload_endpoint` | `str` | `127.0.0.1:4000` | dfdaemon upload service address; `delete` is served by the upload gRPC server which listens on TCP, not on the download socket. |
| `timeout` | `timedelta \| None` | `None` | Default per-RPC timeout; `None` waits indefinitely. |

Usable as a context manager; the client is thread-safe (shares the gRPC channels).

### `client.import_file(path, ...) -> PersistentCacheTask`

`dfcache import`.

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `path` | `str \| PathLike` | required | Local file to import. |
| `content_for_calculating_task_id` | `str \| None` | `None` | Content used by dfdaemon to compute the task id. Recommended for large files. |
| `replica_count` | `int` | `2` | Persistent replica count, `1..5`. |
| `piece_length` | `int` | `0` | Piece length in bytes, `>= 4 MiB`. `0` lets dfdaemon choose by file size. |
| `tag` / `application` | `str \| None` | `None` | Distinguish persistent cache tasks. |
| `ttl` | `timedelta` | `1h` | Between 5 minutes and 7 days. |
| `timeout` | `timedelta \| None` | `30m` | Import timeout. |

### `client.export_file(task_id, *, output, ...) -> Path`

`dfcache export`. dfdaemon downloads the file and hard links (or copies) it to
`output`.

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `task_id` | `str` | required | Persistent cache task id. |
| `output` | `str \| PathLike` | required | Destination file path. |
| `tag` / `application` | `str \| None` | `None` | Task tag/application. |
| `digest` | `str \| None` | `None` | Verify integrity after export, format `<algorithm>:<hash>`, algorithms: `crc32`, `sha256`, `sha512`. |
| `force_hard_link` | `bool` | `False` | Fail instead of copying when hard linking is impossible. |
| `overwrite` | `bool` | `False` | Overwrite an existing file at `output`. |
| `timeout` | `timedelta \| None` | `30m` | Export timeout. |

On mid-transfer failure the partial `output` file is removed, mirroring the CLI.

### `client.stat(task_id) -> PersistentCacheTask`

`dfcache stat`. Returns a dataclass with `id`, `state`, `content_length`,
`piece_count`, `persistent_replica_count`, `current_persistent_replica_count`,
`ttl`, `created_at`, `updated_at`, etc.

### `client.delete(task_id)`

Deletes a persistent cache task. Exposed by the dfdaemon API but not by the
dfcache CLI.

### Exceptions

All errors derive from `DfcacheError`:

| Exception | Meaning |
|-----------|---------|
| `DfcacheValidationError` | Client-side argument validation failed (no RPC made). |
| `DfcacheConnectionError` | dfdaemon unreachable (socket missing, connection refused, deadline). |
| `DfcacheTaskNotFoundError` | The task id does not exist. |
| `DfcachePermissionDeniedError` | Rejected, e.g. by the manager blocklist policy. |
| `DfcacheInvalidArgumentError` | dfdaemon rejected the request as invalid. |
| `DfcacheDownloadError` | Export stream failed mid-transfer. |
| `DfcacheSplitError` | Export stream protocol violation. |

## Examples

See [`examples/`](./examples) for complete flows, including AI model
checkpoint sharing with digest verification.

## Development

```shell
pip install -e ".[dev]"
pytest
```

The test suite runs the client against an in-process fake dfdaemon gRPC
server over a real unix domain socket; no Dragonfly deployment is needed.

## License

Apache 2.0. See the [repository LICENSE](https://github.com/dragonflyoss/client/blob/main/LICENSE).
