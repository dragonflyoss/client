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

"""Plain-Python dataclasses mirroring dfdaemon's persistent cache messages.

The dataclasses decouple SDK users from the generated protobuf bindings: gRPC
is an implementation detail and the public surface stays stable across
protobuf regeneration.
"""

from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

__all__ = [
    "PersistentCacheTask",
    "DigestAlgorithm",
]

# Supported digest algorithms for export integrity verification. The wire
# format is "<algorithm>:<hash>", e.g. "sha256:9f86d08...".
DIGEST_FORMAT = "{algorithm}:{hash}"

# A_digest maps DigestAlgorithm -> separator-prefixed prefix used on the wire.
SEPARATOR = ":"


@dataclass(frozen=True)
class PersistentCacheTask:
    """A persistent cache task stored in the Dragonfly P2P network.

    Attributes:
        id: Unique task id, also the key used for export/stat/delete.
        persistent_replica_count: Requested replica count across peers.
        current_persistent_replica_count: Replicas currently persisted.
        current_replica_count: Total current replicas including non-persistent.
        tag: Tag distinguishing persistent cache tasks.
        application: Application the task belongs to.
        piece_length: Length of each piece in bytes.
        content_length: Total content length in bytes.
        piece_count: Number of pieces.
        state: Task state reported by dfdaemon, e.g. "Succeeded", "Pending".
        ttl: Remaining time before the task is garbage collected.
        created_at: Seconds since epoch when the task was created.
        updated_at: Seconds since epoch when the task was last updated.
    """

    id: str
    persistent_replica_count: int = 0
    current_persistent_replica_count: int = 0
    current_replica_count: int = 0
    tag: Optional[str] = None
    application: Optional[str] = None
    piece_length: int = 0
    content_length: int = 0
    piece_count: int = 0
    state: str = ""
    ttl: Optional[timedelta] = None
    created_at: Optional[float] = None
    updated_at: Optional[float] = None

    @classmethod
    def from_proto(cls, task) -> "PersistentCacheTask":
        """Build a PersistentCacheTask from a common.v2.PersistentCacheTask message.

        Args:
            task: The protobuf PersistentCacheTask returned by dfdaemon.

        Returns:
            The populated dataclass.
        """
        ttl = timedelta(seconds=task.ttl.seconds, microseconds=task.ttl.nanos // 1000) if task.HasField("ttl") else None
        return cls(
            id=task.id,
            persistent_replica_count=task.persistent_replica_count,
            current_persistent_replica_count=task.current_persistent_replica_count,
            current_replica_count=task.current_replica_count,
            tag=task.tag or None,
            application=task.application or None,
            piece_length=task.piece_length,
            content_length=task.content_length,
            piece_count=task.piece_count,
            state=task.state,
            ttl=ttl,
            created_at=task.created_at.seconds + task.created_at.nanos / 1e9 if task.created_at.seconds else None,
            updated_at=task.updated_at.seconds + task.updated_at.nanos / 1e9 if task.updated_at.seconds else None,
        )

    def __str__(self) -> str:
        return f"PersistentCacheTask(id={self.id}, state={self.state}, content_length={self.content_length}, replicas={self.current_persistent_replica_count}/{self.persistent_replica_count})"


class DigestAlgorithm:
    """Digest algorithms supported for export integrity verification."""

    CRC32 = "crc32"
    SHA256 = "sha256"
    SHA512 = "sha512"

    ALL = (CRC32, SHA256, SHA512)


def format_digest(algorithm: str, hash_value: str) -> str:
    """Compose a digest string in the "<algorithm>:<hash>" wire format.

    Args:
        algorithm: One of DigestAlgorithm.CRC32/SHA256/SHA512.
        hash_value: Hexadecimal digest, e.g. hashlib.sha256(data).hexdigest().

    Returns:
        The formatted digest string accepted by export_file(digest=...).

    Raises:
        DfcacheValidationError: If algorithm is not supported.
    """
    from dragonfly_cache.exceptions import DfcacheValidationError

    if algorithm not in DigestAlgorithm.ALL:
        raise DfcacheValidationError(
            f"unsupported digest algorithm {algorithm!r}, expected one of {DigestAlgorithm.ALL}"
        )
    return DIGEST_FORMAT.format(algorithm=algorithm, hash=hash_value)
