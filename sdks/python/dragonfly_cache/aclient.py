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

"""Asynchronous dfcache client for the Dragonfly persistent cache.

Mirrors :class:`dragonfly_cache.client.DfcacheClient` with ``async def``
methods, for callers already running an event loop (training loops,
notebooks, FastAPI handlers). The two implementations share validation,
models and exception mapping; only the channel plumbing differs:

* grpc.aio channels replace the sync ones,
* the download response stream is consumed with ``async for``.

Example:
    >>> async with AsyncDfcacheClient() as client:
    ...     task = await client.import_file("/data/model.safetensors",
    ...                                     content_for_calculating_task_id="org/model/v1")
    ...     await client.export_file(task.id, output="/tmp/model.safetensors")
"""

import asyncio
import os
from datetime import timedelta
from pathlib import Path
from typing import Optional, Union

import grpc
import grpc.aio
from dragonfly_api import dfdaemon_pb2, dfdaemon_pb2_grpc
from google.protobuf import duration_pb2

from dragonfly_cache.client import (
    DEFAULT_ENDPOINT,
    DEFAULT_RPC_TIMEOUT,
    DEFAULT_UPLOAD_ENDPOINT,
    _ENDPOINT_TIMEOUT_SECONDS,
    _duration,
)
from dragonfly_cache.exceptions import (
    DfcacheConnectionError,
    DfcacheSplitError,
    _from_grpc_status,
)
from dragonfly_cache.models import PersistentCacheTask
from dragonfly_cache.validators import validate_export, validate_import, validate_task_id

__all__ = ["AsyncDfcacheClient"]


class AsyncDfcacheClient:
    """Asynchronous client for the Dragonfly persistent cache.

    The client is safe to use from a single event loop; create one per loop.
    See :class:`dragonfly_cache.client.DfcacheClient` for the matching
    synchronous API and Parameters documentation.
    """

    def __init__(
        self,
        endpoint: Union[str, os.PathLike] = DEFAULT_ENDPOINT,
        *,
        upload_endpoint: str = DEFAULT_UPLOAD_ENDPOINT,
        timeout: Optional[timedelta] = None,
    ):
        """Create an async dfcache client.

        Args:
            endpoint: dfdaemon download service unix domain socket path.
            upload_endpoint: dfdaemon upload service host:port serving
                DeletePersistentCacheTask; the download socket does not
                expose the upload service.
            timeout: Default timeout applied to every RPC, None disables it.

        Raises:
            DfcacheConnectionError: If the socket does not exist; daemon
                reachability is verified lazily on first RPC.
        """
        self._endpoint = str(endpoint)
        self._upload_endpoint = upload_endpoint
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

        # See the sync client for why the authority must be pinned for
        # unix domain socket channels.
        self._channel = grpc.aio.insecure_channel(
            target, options=[("grpc.default_authority", "localhost")]
        )
        self._download_stub = dfdaemon_pb2_grpc.DfdaemonDownloadStub(self._channel)

        # DeletePersistentCacheTask is served by the upload service on TCP.
        self._upload_channel = grpc.aio.insecure_channel(upload_endpoint)
        self._upload_stub = dfdaemon_pb2_grpc.DfdaemonUploadStub(self._upload_channel)

    # Context manager plumbing.
    async def __aenter__(self) -> "AsyncDfcacheClient":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def close(self) -> None:
        """Close the underlying gRPC channels."""
        await self._channel.close()
        await self._upload_channel.close()

    def __repr__(self) -> str:
        return (
            f"AsyncDfcacheClient(endpoint={self._endpoint!r}, "
            f"upload_endpoint={self._upload_endpoint!r})"
        )

    def _deadline(self, timeout: Optional[timedelta]) -> Optional[float]:
        """Effective per-call timeout in seconds, None for no timeout."""
        effective = timeout if timeout is not None else self._timeout
        return None if effective is None else effective.total_seconds()

    async def import_file(
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

        Mirrors the sync :meth:`DfcacheClient.import_file`; see its
        docstring for the full parameter and error documentation.
        """
        path = os.fspath(path)
        validate_import(path, replica_count, ttl, piece_length)
        remote_ip = await _remote_ip()

        try:
            task = await self._download_stub.UploadPersistentCacheTask(
                dfdaemon_pb2.UploadPersistentCacheTaskRequest(
                    content_for_calculating_task_id=content_for_calculating_task_id,
                    path=str(Path(path).resolve()),
                    persistent_replica_count=replica_count,
                    tag=tag,
                    application=application,
                    piece_length=piece_length or None,
                    ttl=_duration(ttl.total_seconds()),
                    timeout=_duration(timeout.total_seconds()) if timeout else None,
                    remote_ip=remote_ip,
                ),
                timeout=self._deadline(timeout),
            )
        except grpc.aio.AioRpcError as err:
            raise _from_grpc_status("", err) from err

        return PersistentCacheTask.from_proto(task)

    async def export_file(
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

        Mirrors the sync :meth:`DfcacheClient.export_file`; see its
        docstring for the full parameter and error documentation.
        """
        output_path = os.fspath(output)
        validate_export(task_id, output_path, digest)
        remote_ip = await _remote_ip()

        try:
            stream = self._download_stub.DownloadPersistentCacheTask(
                dfdaemon_pb2.DownloadPersistentCacheTaskRequest(
                    task_id=task_id,
                    persistent=False,
                    tag=tag,
                    application=application,
                    output_path=str(Path(output_path).resolve()),
                    timeout=_duration(timeout.total_seconds()) if timeout else None,
                    need_piece_content=False,
                    force_hard_link=force_hard_link,
                    digest=digest,
                    remote_ip=remote_ip,
                    overwrite=overwrite,
                ),
                timeout=self._deadline(timeout),
            )

            saw_start = False
            async for response in stream:
                kind = response.WhichOneof("response")
                if kind == "download_persistent_cache_task_started_response":
                    saw_start = True
                elif kind == "download_piece_finished_response":
                    if not saw_start:
                        raise DfcacheSplitError(
                            "protocol violation: piece finished before download started"
                        )
        except (grpc.aio.AioRpcError, DfcacheSplitError) as err:
            # Clean up a possibly partially written output file, mirroring
            # the CLI's remove_file on error behavior.
            try:
                os.remove(output_path)
            except OSError:
                pass
            if isinstance(err, grpc.aio.AioRpcError):
                raise _from_grpc_status(task_id, err) from err
            raise

        return Path(output_path).resolve()

    async def stat(
        self,
        task_id: str,
        *,
        timeout: Optional[timedelta] = None,
    ) -> PersistentCacheTask:
        """Get persistent cache task information.

        Mirrors the sync :meth:`DfcacheClient.stat`; see its docstring for
        the full parameter and error documentation.
        """
        validate_task_id(task_id)
        remote_ip = await _remote_ip()

        try:
            task = await self._download_stub.StatPersistentCacheTask(
                dfdaemon_pb2.StatPersistentCacheTaskRequest(
                    task_id=task_id,
                    remote_ip=remote_ip,
                ),
                timeout=self._deadline(timeout),
            )
        except grpc.aio.AioRpcError as err:
            raise _from_grpc_status(task_id, err) from err

        return PersistentCacheTask.from_proto(task)

    async def delete(
        self,
        task_id: str,
        *,
        timeout: Optional[timedelta] = None,
    ) -> None:
        """Delete a persistent cache task.

        Mirrors the sync :meth:`DfcacheClient.delete`; see its docstring
        for the full parameter and error documentation.
        """
        validate_task_id(task_id)
        remote_ip = await _remote_ip()

        try:
            await self._upload_stub.DeletePersistentCacheTask(
                dfdaemon_pb2.DeletePersistentCacheTaskRequest(
                    task_id=task_id,
                    remote_ip=remote_ip,
                ),
                timeout=self._deadline(timeout),
            )
        except grpc.aio.AioRpcError as err:
            raise _from_grpc_status(task_id, err) from err


async def _remote_ip() -> Optional[str]:
    """Preferred local IP or None, probed off the event loop's thread.

    The sync client runs this check inline; the async client offloads the
    short UDP socket probe to the default executor to keep the loop
    responsive. See DfcacheClient._preferred_local_ip for the rationale.
    """
    return await asyncio.get_event_loop().run_in_executor(
        None, _sync_remote_ip
    )


def _sync_remote_ip() -> Optional[str]:
    from dragonfly_cache.client import DfcacheClient

    return DfcacheClient._preferred_local_ip()
