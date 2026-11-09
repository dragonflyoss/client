# Copyright The Dragonfly Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Synchronous dfcache client for the Dragonfly persistent cache.

The client talks to the dfdaemon download gRPC service over a unix domain
socket (default /var/run/dragonfly/dfdaemon.sock) and mirrors the dfcache
CLI subcommands:

* :meth:`DfcacheClient.import_file`  - dfcache import
* :meth:`DfcacheClient.export_file`  - dfcache export
* :meth:`DfcacheClient.stat`         - dfcache stat
* :meth:`DfcacheClient.delete`       - proto exposes it, the CLI does not
"""

import logging
import os
import socket
from datetime import timedelta
from pathlib import Path
from typing import Optional, Union

import grpc
from dragonfly_api import common_pb2, dfdaemon_pb2, dfdaemon_pb2_grpc
from google.protobuf import duration_pb2

from dragonfly_cache.exceptions import (
    DfcacheConnectionError,
    DfcacheDownloadError,
    DfcacheError,
    DfcacheSplitError,
    _from_grpc_status,
)
from dragonfly_cache.models import PersistentCacheTask
from dragonfly_cache.validators import validate_export, validate_import, validate_task_id

__all__ = ["DfcacheClient", "DEFAULT_ENDPOINT"]

logger = logging.getLogger(__name__)

# Default dfdaemon download service unix domain socket, mirrors
# dragonfly-client-config::dfdaemon::default_download_unix_socket_path.
DEFAULT_ENDPOINT = "/var/run/dragonfly/dfdaemon.sock"

# Default timeout for individual dfdaemon RPCs when the caller does not
# specify one. Import/export accept an explicit timeout because transfer
# duration depends on file size.
DEFAULT_RPC_TIMEOUT = timedelta(minutes=30)

# RPC timeout upper bound mirrors the CLI default timeout of 30m; export
# and import accept longer timeouts for very large files (models).
_ENDPOINT_TIMEOUT_SECONDS = 5


def _duration(seconds: float) -> duration_pb2.Duration:
    """Convert seconds into a google.protobuf.Duration message.

    Args:
        seconds: Duration value in seconds.

    Returns:
        The protobuf Duration message.
    """
    total_ns = int(seconds * 1e9)
    return duration_pb2.Duration(seconds=total_ns // 1_000_000_000, nanos=total_ns % 1_000_000_000)


class DfcacheClient:
    """Synchronous client for the Dragonfly persistent cache.

    The client is thread-safe: the underlying grpc.Channel is shared and safe
    for concurrent use from multiple threads.

    Example:
        >>> with DfcacheClient() as client:
        ...     task = client.import_file("/data/model.safetensors", tag="llm")
        ...     client.export_file(task.id, output="/tmp/model.safetensors")
    """

    def __init__(
        self,
        endpoint: Union[str, os.PathLike] = DEFAULT_ENDPOINT,
        *,
        timeout: Optional[timedelta] = None,
    ):
        """Create a dfcache client.

        Args:
            endpoint: dfdaemon download service unix domain socket path.
            timeout: Default timeout applied to every RPC, None disables it.

        Raises:
            DfcacheConnectionError: If the socket does not exist or the
                dfdaemon grpc server rejects the connection.
        """
        self._endpoint = str(endpoint)
        self._timeout = timeout

        if self._endpoint.startswith("unix:"):
            target = self._endpoint
            socket_path = self._endpoint[len("unix:"):]
        else:
            target = f"unix:{self._endpoint}"
            socket_path = self._endpoint

        if not os.path.exists(socket_path):
            raise DfcacheConnectionError(
                f"dfdaemon socket {socket_path} does not exist, please check the dfdaemon deployment"
            )

        self._channel = grpc.insecure_channel(target)
        self._download_stub = dfdaemon_pb2_grpc.DfdaemonDownloadStub(self._channel)
        # The dfdaemon download service handles import/export/stat, but
        # DeletePersistentCacheTask is only exposed by the upload service
        # (mirrors dfdaemon v2 proto service definitions).
        self._upload_stub = dfdaemon_pb2_grpc.DfdaemonUploadStub(self._channel)

        try:
            grpc.channel_ready_future(self._channel).result(timeout=_ENDPOINT_TIMEOUT_SECONDS)
        except grpc.FutureTimeoutError as err:
            self._channel.close()
            raise DfcacheConnectionError(
                f"can not connect {self._endpoint}, please check the unix socket"
            ) from err

    # Context manager plumbing.
    def __enter__(self) -> "DfcacheClient":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def close(self) -> None:
        """Close the underlying gRPC channel."""
        self._channel.close()

    def __repr__(self) -> str:
        return f"DfcacheClient(endpoint={self._endpoint!r})"

    def _deadline(self, timeout: Optional[timedelta]) -> Optional[float]:
        """Compose the effective deadline in seconds for a single call.

        Args:
            timeout: Per-call timeout override.

        Returns:
            Effective timeout in seconds, None for no timeout.
        """
        effective = timeout if timeout is not None else self._timeout
        return None if effective is None else effective.total_seconds()

    def import_file(
        self,
        path: Union[str, os.PathLike],
        *,
        content_for_calculating_task_id: Optional[str] = None,
        replica_count: int = 2,
        piece_length: int = 0,
        tag: Optional[str] = None,
        application: Optional[str] = None,
        ttl: timedelta = timedelta(hours=1),
        timeout: Optional[timedelta] = DEFAULT_RPC_TIMEOUT,
    ) -> PersistentCacheTask:
        """Import a local file into the Dragonfly persistent cache.

        Mirrors `dfcache import`. dfdaemon reads the file and replicates it
        to `replica_count` peers. On success returns the created task with
        its id, usable for export/stat/delete.

        Args:
            path: Local file path to import.
            content_for_calculating_task_id: Content used by dfdaemon to
                compute the task id. When omitted, dfdaemon hashes the whole
                file which is slow for large files - for model files always
                provide it (e.g. "org/model/weights").
            replica_count: Persistent replica count, 1-5.
            piece_length: Piece length in bytes, >= 4 MiB, 0 lets dfdaemon
                compute it from the file size.
            tag: Tag distinguishing persistent cache tasks.
            application: Application the task belongs to.
            ttl: Time to live, between 5 minutes (inclusive) and 7 days.
            timeout: Timeout for the import; None waits indefinitely.

        Returns:
            The created PersistentCacheTask.

        Raises:
            DfcacheValidationError: If client-side validation fails.
            DfcacheError: Subclass describing any dfdaemon failure.
        """
        path = os.fspath(path)

        # Note(IMPORTANT): CLI warns that computing task id from file content
        # is slow for large files; promote the hint to a debug-level nudge.
        if content_for_calculating_task_id is None:
            logger.debug(
                "content_for_calculating_task_id is not set, dfdaemon will hash "
                "the whole file to compute the task id which is slow for large files"
            )

        validate_import(path, replica_count, ttl, piece_length)

        try:
            task = self._download_stub.UploadPersistentCacheTask(
                dfdaemon_pb2.UploadPersistentCacheTaskRequest(
                    content_for_calculating_task_id=content_for_calculating_task_id,
                    path=str(Path(path).resolve()),
                    persistent_replica_count=replica_count,
                    tag=tag,
                    application=application,
                    piece_length=piece_length or None,
                    ttl=_duration(ttl.total_seconds()),
                    timeout=_duration(timeout.total_seconds()) if timeout else None,
                    remote_ip=self._preferred_local_ip(),
                ),
                timeout=self._deadline(timeout),
            )
        except grpc.RpcError as err:
            raise _from_grpc_status("", err) from err

        return PersistentCacheTask.from_proto(task)

    def export_file(
        self,
        task_id: str,
        *,
        output: Union[str, os.PathLike],
        tag: Optional[str] = None,
        application: Optional[str] = None,
        digest: Optional[str] = None,
        force_hard_link: bool = False,
        overwrite: bool = False,
        timeout: Optional[timedelta] = DEFAULT_RPC_TIMEOUT,
    ) -> Path:
        """Export a file from the Dragonfly persistent cache.

        Mirrors `dfcache export`. By default dfdaemon downloads the file and
        hard links (or copies) it to the output path. Integrity can be
        verified with a digest, e.g. "sha256:<hexdigest>".

        Args:
            task_id: Persistent cache task id to export.
            output: Destination file path.
            tag: Tag distinguishing persistent cache tasks.
            application: Application the task belongs to.
            digest: Optional digest in "<algorithm>:<hash>" format, supported
                algorithms are crc32, sha256 and sha512.
            force_hard_link: When True the export fails instead of copying.
            overwrite: When True overwrite an existing file at output.
            timeout: Timeout for the export; None waits indefinitely.

        Returns:
            The resolved path of the exported file.

        Raises:
            DfcacheValidationError: If client-side validation fails.
            DfcacheDownloadError: If the transfer fails midway.
            DfcacheError: Subclass describing any dfdaemon failure.
        """
        output_path = os.fspath(output)

        validate_export(task_id, output_path, digest)

        try:
            stream = self._download_stub.DownloadPersistentCacheTask(
                dfdaemon_pb2.DownloadPersistentCacheTaskRequest(
                    task_id=task_id,
                    # Keep persistent=False for user-triggered exports to
                    # mirror the CLI: only scheduler-triggered exports set it.
                    persistent=False,
                    tag=tag,
                    application=application,
                    output_path=str(Path(output_path).resolve()),
                    timeout=_duration(timeout.total_seconds()) if timeout else None,
                    # The SDK relies on dfdaemon-side hard link/copy transfer;
                    # piece streaming assembly stays a CLI concern for now.
                    need_piece_content=False,
                    force_hard_link=force_hard_link,
                    digest=digest,
                    remote_ip=self._preferred_local_ip(),
                    overwrite=overwrite,
                ),
                timeout=self._deadline(timeout),
            )

            # Consume the stream: errors mid transfer arrive as RpcError on
            # iteration. The final piece finished response signals completion.
            saw_start = False
            for response in stream:
                kind = response.WhichOneof("response")
                if kind == "download_persistent_cache_task_started_response":
                    saw_start = True
                elif kind == "download_piece_finished_response":
                    if not saw_start:
                        raise DfcacheSplitError(
                            "protocol violation: piece finished before download started"
                        )
                # Unknown/None oneof kinds from newer daemons are tolerated.
        except grpc.RpcError as err:
            # Clean up a possibly partially written output file, mirroring
            # the CLI's remove_file on error behavior.
            try:
                os.remove(output_path)
            except OSError:
                pass
            raise _from_grpc_status(task_id, err) from err
        except DfcacheSplitError:
            try:
                os.remove(output_path)
            except OSError:
                pass
            raise

        return Path(output_path).resolve()

    def stat(
        self,
        task_id: str,
        *,
        timeout: Optional[timedelta] = None,
    ) -> PersistentCacheTask:
        """Get persistent cache task information.

        Mirrors `dfcache stat`.

        Args:
            task_id: Persistent cache task id to query.
            timeout: Timeout for the stat; None waits indefinitely.

        Returns:
            The PersistentCacheTask information.

        Raises:
            DfcacheTaskNotFoundError: If the task does not exist.
            DfcacheError: Subclass describing any dfdaemon failure.
        """
        validate_task_id(task_id)

        try:
            task = self._download_stub.StatPersistentCacheTask(
                dfdaemon_pb2.StatPersistentCacheTaskRequest(
                    task_id=task_id,
                    remote_ip=self._preferred_local_ip(),
                ),
                timeout=self._deadline(timeout),
            )
        except grpc.RpcError as err:
            raise _from_grpc_status(task_id, err) from err

        return PersistentCacheTask.from_proto(task)

    def delete(
        self,
        task_id: str,
        *,
        timeout: Optional[timedelta] = None,
    ) -> None:
        """Delete a persistent cache task.

        Exposed by the dfdaemon proto but not by the dfcache CLI; cache
        lifecycle management is a common SDK requirement.

        Args:
            task_id: Persistent cache task id to delete.
            timeout: Timeout for the delete; None waits indefinitely.

        Raises:
            DfcacheTaskNotFoundError: If the task does not exist.
            DfcacheError: Subclass describing any dfdaemon failure.
        """
        validate_task_id(task_id)

        try:
            self._upload_stub.DeletePersistentCacheTask(
                dfdaemon_pb2.DeletePersistentCacheTaskRequest(
                    task_id=task_id,
                    remote_ip=self._preferred_local_ip(),
                ),
                timeout=self._deadline(timeout),
            )
        except grpc.RpcError as err:
            raise _from_grpc_status(task_id, err) from err

    @staticmethod
    def _preferred_local_ip() -> Optional[str]:
        """Return the preferred local IP for dfdaemon, None if unavailable.

        Mirrors dragonfly-client-util::net::preferred_local_ip: the source
        address that would be used to reach the internet, falling back to
        the hostname. Used by dfdaemon for observability/metrics tagging.

        Returns:
            Dotted-quad IP string or None when no route exists.
        """
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                # Connect to a public address; no packets are sent for UDP.
                # The tuple form is required on macOS.
                sock.connect(("8.8.8.8", 80))
                return sock.getsockname()[0]
        except OSError:
            return None
