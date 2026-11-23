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

"""Typed exceptions raised by the Dragonfly persistent cache SDK.

gRPC status codes returned by dfdaemon are surfaced as dedicated subclasses so
callers can handle common failure modes (task not found, blocklist rejection,
connectivity) without parsing gRPC details.
"""

import grpc

__all__ = [
    "DfcacheError",
    "DfcacheValidationError",
    "DfcacheConnectionError",
    "DfcacheTaskNotFoundError",
    "DfcachePermissionDeniedError",
    "DfcacheInvalidArgumentError",
    "DfcacheDownloadError",
    "DfcacheSplitError",
]


class DfcacheError(Exception):
    """Base error for all dfcache SDK failures."""


class DfcacheValidationError(DfcacheError):
    """Raised when SDK-side argument validation fails before any RPC is made."""


class DfcacheConnectionError(DfcacheError):
    """Raised when the connection to dfdaemon is unavailable."""


class DfcacheTaskNotFoundError(DfcacheError):
    """Raised when dfdaemon reports the requested persistent cache task does not exist."""

    def __init__(self, task_id: str, message: str):
        self.task_id = task_id
        super().__init__(message)


class DfcachePermissionDeniedError(DfcacheError):
    """Raised when the request is rejected, e.g. by the manager blocklist policy."""


class DfcacheInvalidArgumentError(DfcacheError):
    """Raised when dfdaemon rejects the request as invalid."""


class DfcacheDownloadError(DfcacheError):
    """Raised when a download (export) stream fails mid-transfer."""


class DfcacheSplitError(DfcacheError):
    """Raised when an export response cannot be split into a well-formed stream."""


def _from_grpc_status(task_id: str, status: grpc.RpcError) -> DfcacheError:
    """Translate a gRPC RpcError into a typed dfcache exception.

    Args:
        task_id: The persistent cache task id involved in the failed call, may be empty.
        status: The RpcError raised by grpcio.

    Returns:
        A typed subclass of DfcacheError describing the failure.
    """
    message = status.details() or status.code().name
    code = status.code()

    if code == grpc.StatusCode.NOT_FOUND:
        return DfcacheTaskNotFoundError(task_id, f"persistent cache task {task_id} not found: {message}")
    if code == grpc.StatusCode.PERMISSION_DENIED:
        return DfcachePermissionDeniedError(f"permission denied: {message}")
    if code == grpc.StatusCode.INVALID_ARGUMENT:
        return DfcacheInvalidArgumentError(f"invalid argument: {message}")
    if code in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
        return DfcacheConnectionError(f"failed to reach dfdaemon: {message}")
    return DfcacheError(f"gRPC error ({code.name}): {message}")
