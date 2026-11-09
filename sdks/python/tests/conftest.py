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

"""Shared pytest fixtures: a fake dfdaemon gRPC server over a unix socket.

The server socket lives in a short session-scoped directory because unix
socket paths are limited to ~104 bytes on macOS while pytest tmp_path can
exceed that.
"""

import os
import tempfile
import threading
from concurrent import futures

import grpc
import pytest
from dragonfly_api import common_pb2, dfdaemon_pb2, dfdaemon_pb2_grpc
from google.protobuf import empty_pb2, timestamp_pb2


class FakeDfdaemon(dfdaemon_pb2_grpc.DfdaemonDownloadServicer):
    """In-memory dfdaemon answering persistent cache RPCs deterministically."""

    # Methods of the upload servicer that the SDK does not exercise delegate to
    # the implementation of their download-service equivalents or fail closed.

    def __init__(self):
        self.tasks = {}
        self.upload_requests = []
        self.download_requests = []
        self.stat_requests = []
        self.delete_requests = []
        # When set, the RPC named by the key raises this gRPC error.
        self.errors = {}
        # When set, the download stream aborts after the started response is
        # delivered, simulating a mid-transfer peer failure.
        self.fail_stream_after_start = False

    def _maybe_error(self, key, context):
        err = self.errors.get(key)
        if err is not None:
            context.abort(err.code, err.details)

    def UploadPersistentCacheTask(self, request, context):
        self._maybe_error("upload", context)
        self.upload_requests.append(request)
        task_id = request.content_for_calculating_task_id or "ca30f9b2-default-task-id"
        task = common_pb2.PersistentCacheTask(
            id=task_id,
            persistent_replica_count=request.persistent_replica_count,
            current_persistent_replica_count=request.persistent_replica_count,
            current_replica_count=request.persistent_replica_count,
            tag=request.tag,
            application=request.application,
            piece_length=request.piece_length or 4194304,
            content_length=1024,
            piece_count=1,
            state="Succeeded",
            ttl=request.ttl,
            created_at=timestamp_pb2.Timestamp(seconds=1700000000),
            updated_at=timestamp_pb2.Timestamp(seconds=1700000001),
        )
        self.tasks[task_id] = task
        return task

    def DownloadPersistentCacheTask(self, request, context):
        self._maybe_error("download", context)
        self.download_requests.append(request)
        if request.task_id not in self.tasks:
            context.abort(grpc.StatusCode.NOT_FOUND, f"task {request.task_id} not found")

        task = self.tasks[request.task_id]
        fail_after_start = self.fail_stream_after_start

        def responses():
            yield dfdaemon_pb2.DownloadPersistentCacheTaskResponse(
                host_id="host-1",
                task_id=task.id,
                peer_id="peer-1",
                download_persistent_cache_task_started_response=(
                    dfdaemon_pb2.DownloadPersistentCacheTaskStartedResponse(content_length=task.content_length)
                ),
            )
            if fail_after_start:
                context.abort(grpc.StatusCode.UNAVAILABLE, "peer lost")
            yield dfdaemon_pb2.DownloadPersistentCacheTaskResponse(
                host_id="host-1",
                task_id=task.id,
                peer_id="peer-1",
                download_piece_finished_response=dfdaemon_pb2.DownloadPieceFinishedResponse(
                    piece=common_pb2.Piece(number=0, offset=0, length=task.content_length)
                ),
            )

        return responses()

    def StatPersistentCacheTask(self, request, context):
        self._maybe_error("stat", context)
        self.stat_requests.append(request)
        if request.task_id not in self.tasks:
            context.abort(grpc.StatusCode.NOT_FOUND, f"task {request.task_id} not found")
        return self.tasks[request.task_id]

    def DeletePersistentCacheTask(self, request, context):
        self._maybe_error("delete", context)
        self.delete_requests.append(request)
        if request.task_id not in self.tasks:
            context.abort(grpc.StatusCode.NOT_FOUND, f"task {request.task_id} not found")
        self.tasks.pop(request.task_id, None)
        return empty_pb2.Empty()


def _unimplemented(name):
    """Build a servicer method placeholder aborting with UNIMPLEMENTED."""

    def method(self, request, context):
        context.abort(grpc.StatusCode.UNIMPLEMENTED, f"{name} is not implemented by the test fake")

    return method


# Fill in the remaining DfdaemonUploadServicer method surface; the SDK never
# calls these, but the grpc server requires the full handler set. Methods that
# FakeDfdaemon already implements for the download service (export/stat) are
# shared across both services, so only add placeholders for missing ones.
for _name in (
    "DeleteCacheTask",
    "DeletePersistentTask",
    "DeleteTask",
    "DownloadCachePiece",
    "DownloadCacheTask",
    "DownloadPersistentCacheTask",
    "DownloadPersistentTask",
    "DownloadTask",
    "ListTaskEntries",
    "StatCacheTask",
    "StatLocalTask",
    "StatPersistentCacheTask",
    "StatPersistentTask",
    "StatTask",
    "SyncCachePieces",
    "SyncHost",
    "SyncPersistentCachePieces",
    "SyncPersistentPieces",
    "SyncPieces",
    "UpdatePersistentCacheTask",
    "UpdatePersistentTask",
):
    if not hasattr(FakeDfdaemon, _name):
        setattr(FakeDfdaemon, _name, _unimplemented(_name))


@pytest.fixture(scope="session")
def daemon_socket_dir():
    """A short-lived directory for the session unix socket."""
    with tempfile.TemporaryDirectory(prefix="dfcache-test-") as directory:
        yield directory


@pytest.fixture
def fake_dfdaemon(daemon_socket_dir):
    """Run a FakeDfdaemon server on a temporary unix domain socket.

    Yields:
        A (FakeDfdaemon, socket_path) tuple.
    """
    socket_path = os.path.join(daemon_socket_dir, f"dfdaemon-{threading.get_ident()}.sock")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    fake = FakeDfdaemon()
    dfdaemon_pb2_grpc.add_DfdaemonDownloadServicer_to_server(fake, server)
    # DeletePersistentCacheTask lives on the upload service.
    dfdaemon_pb2_grpc.add_DfdaemonUploadServicer_to_server(fake, server)
    server.add_insecure_port(f"unix:{socket_path}")
    server.start()

    try:
        yield fake, socket_path
    finally:
        server.stop(grace=None)
        try:
            os.unlink(socket_path)
        except OSError:
            pass


@pytest.fixture
def client(fake_dfdaemon):
    """A DfcacheClient connected to the fake dfdaemon."""
    from dragonfly_cache.client import DfcacheClient

    _, socket_path = fake_dfdaemon
    with DfcacheClient(socket_path) as c:
        yield c


@pytest.fixture
def sample_file(tmp_path):
    """A small local file usable for import tests."""
    path = tmp_path / "model.safetensors"
    path.write_bytes(b"fake-model-weights" * 64)
    return str(path)
