# dragonfly#4025 implementation report

## Existing import protocol

Before this change, `dfcache import` resolved the source to an absolute path and connected to `dfdaemon::default_download_unix_socket_path()` (or `--endpoint`) with `DfdaemonDownloadClient::new_unix`. It sent the unary `dfdaemon.v2.DfdaemonDownload/UploadPersistentCacheTask` request containing the path and import options, but no file bytes.

Dfdaemon opened that path in its own mount namespace. It calculated the task ID either from `content_for_calculating_task_id` or from the file content, piece length, tag, and application. It then called `PersistentCacheTask::upload`, which notified the scheduler and imported the content into local persistent-cache storage using a hard link when possible and piece writes as a fallback. Consequently, mounting only the UDS into a Pod was insufficient: the Node's dfdaemon could not open a path that existed only in the Pod.

## Changes

- `dragonfly-client/src/bin/dfcache/import.rs`
  - Added the opt-in `--transfer-to-dfdaemon` flag and `DFCACHE_IMPORT_TRANSFER_TO_DFDAEMON` environment variable. The default remains the original unary path import, with no automatic fallback after a streaming failure.
  - Added sequential file streaming in chunks of at most 1 MiB, declared-length metadata, and preservation of local source-read errors.
  - Added tests for empty input, chunk-boundary and multi-chunk inputs, non-UTF8 Unix filenames, source read errors, and the opt-in/default CLI behavior.
- `dragonfly-client/src/grpc/dfdaemon_download.rs`
  - Added the client-streaming companion RPC `dfdaemon.v2.DfdaemonDownloadContent/UploadPersistentCacheTaskContent`. The existing unary API and message are unchanged. The companion service is implemented locally because the pinned `dragonfly-api = 2.3.8` has no content-upload RPC and the task is limited to the Rust Client.
  - Defined a prost protocol with one initial metadata message followed by byte chunks. The metadata carries the declared content length and the existing import options; it never carries the client pathname.
  - Added server validation for missing or repeated metadata, chunks before metadata, empty/oversized chunks, declared-length overflow, short EOF, and peer errors.
  - Added daemon-owned staging under `storage.dir/tmp` using unpredictable `create_new` files. A guard removes staging on success, validation/write failure, or cancellation. After reception, the staged path is converted to the existing `UploadPersistentCacheTaskRequest` and passed to the original upload handler, preserving task-ID generation, blocklist checks, metrics, scheduler notifications, persistent-cache registration, and storage rollback.
  - Registered the companion service on the existing download UDS and reused the same client channel. Added protocol, failure, cleanup, missing-socket, and real in-process UDS streaming tests.
- `docs/dfcache-import.md`
  - Documented the two modes, a Kubernetes `hostPath` socket-directory mount, the Pod command, staging behavior, permissions, and old-daemon `UNIMPLEMENTED` behavior.
- `Cargo.toml`, `dragonfly-client/Cargo.toml`, `Cargo.lock`
  - Added direct use of the already-present workspace dependency-tree crates `prost`, `tonic-prost`, and `tokio-util` for the local tonic/prost companion service and file reader stream. `Cargo.lock` changed only the `dragonfly-client` dependency list; no unrelated crate versions changed.

## Pod call chain

1. Inside the Pod, dfcache opens the Pod-local file and reads its length. With `--transfer-to-dfdaemon`, it sends import metadata first and then streams file bytes over the mounted download UDS.
2. The Node's dfdaemon creates a unique staging file in its configured `storage.dir/tmp`. It validates message ordering, chunk limits, and exact declared length while writing the bytes.
3. Dfdaemon generates the task ID from the explicit ID content or from the staged file and options, using the same generator as path import.
4. The existing persistent-cache upload handler performs blocklist and metrics handling and calls `PersistentCacheTask::upload`. That existing flow registers the upload with the scheduler, checks capacity, creates task metadata, and hard-links or copies the staged content into persistent-cache storage before notifying completion.
5. Dfdaemon removes the staging name. If the existing import used a hard link, the stored content remains through its storage link. Errors before or during import remove staging and use the existing upload failure/rollback behavior.

The source file and dfdaemon storage directory do not need to be mounted into the Pod. Only the socket directory is shared.

## Verification

All commands ran in this worktree with:

```console
export PATH=$HOME/.cargo/bin:$PATH
export CARGO_TARGET_DIR=/home/ps/LHL/target-dfc
export LIBCLANG_PATH=/usr/lib/llvm-18/lib
```

The repository toolchain is Rust 1.88.0 (`rust-toolchain.toml`). Final commands and real results:

| Command | Result | Log |
| --- | --- | --- |
| `cargo build -p dragonfly-client` | Passed; dev build finished in 31.57s | `artifacts/logs/build.log` |
| `cargo test -p dragonfly-client` | **77 passed, 0 failed, 1 ignored**: 62 lib + 6 dfcache + 0 dfctl + 2 dfdaemon + 5 dfget + 2 dfstore + 0 doc-tests; the ignored doc-test is counted separately | `artifacts/logs/test.log` |
| `cargo fmt --all -- --check` | Passed with no output | `artifacts/logs/fmt.log` |
| `cargo clippy -p dragonfly-client -- -D warnings` | Passed; dev check finished in 1m 17s | `artifacts/logs/clippy.log` |
| `cargo test -p dragonfly-client --lib grpc::dfdaemon_download::tests` | 8 passed, 0 failed | `artifacts/logs/test-streaming-lib.log` |
| `cargo test -p dragonfly-client --bin dfcache import::tests` | 5 passed, 0 failed, 1 filtered out | `artifacts/logs/test-dfcache-import.log` |

There are 12 newly added tests across the lib and dfcache test targets. Earlier development failures and their real output remain in `artifacts/logs/check-initial.log`, `check-second.log`, and `test-streaming-lib-initial.log`; the final commands above all passed.

## Not verified

- No live Kubernetes DaemonSet/Pod or remote scheduler was available, so the real cluster deployment and scheduler registration were not exercised end to end. The UDS RPC was exercised with a real in-process Unix socket and a mock content service.
- Injected writer failure, short streams, cancellation status, and staging cleanup were unit-tested; physical disk-full and daemon-process-crash behavior were not exercised.
- Chunk tests cover empty, exact-boundary, and multi-chunk files through 2 MiB + 17 bytes; multi-gigabyte throughput and memory profiling were not performed.
