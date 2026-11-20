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

"""Client-side argument validation mirroring dfcache CLI rules.

Validation happens before any RPC is made so users get immediate, actionable
errors instead of dfdaemon rejecting the request deep in the call stack. The
rules match dragonfly-client/src/bin/dfcache/import.rs validate_args and the
proto validate rules in pkg/apis/dfdaemon/v2/dfdaemon.proto.
"""

import os
import re
from datetime import timedelta

from dragonfly_cache.exceptions import DfcacheValidationError
from dragonfly_cache.models import DigestAlgorithm

__all__ = [
    "MIN_TTL",
    "MAX_TTL",
    "MIN_REPLICA_COUNT",
    "MAX_REPLICA_COUNT",
    "MIN_PIECE_LENGTH",
    "validate_import",
    "validate_export",
    "validate_task_id",
    "validate_digest",
]

# Mirror of the dfcache CLI bounds (import.rs validate_args).
MIN_TTL = timedelta(minutes=5)
MAX_TTL = timedelta(days=7)
MIN_REPLICA_COUNT = 1
MAX_REPLICA_COUNT = 5

# Mirror of dragonfly-client-core MIN_PIECE_LENGTH: 4 MiB.
MIN_PIECE_LENGTH = 4 * 1024 * 1024

_DIGEST_RE = re.compile(r"^(crc32|sha256|sha512):[0-9a-fA-F]+$")
_MAX_TASK_ID_LEN = 128


def validate_task_id(task_id: str) -> None:
    """Ensure a task id looks like a persistent cache task id.

    Task ids are opaque strings computed by dfdaemon (typically a blake3
    hex digest of the content parameters), so only sanity constraints are
    enforced here.

    Args:
        task_id: The id supplied by the caller.

    Raises:
        DfcacheValidationError: If the id is empty or implausibly long.
    """
    if not task_id:
        raise DfcacheValidationError("task_id must not be empty")
    if len(task_id) > _MAX_TASK_ID_LEN:
        raise DfcacheValidationError(
            f"invalid persistent cache task id {task_id!r}: longer than {_MAX_TASK_ID_LEN} characters"
        )


def validate_import(
    path: str,
    replica_count: int,
    ttl: timedelta,
    piece_length: int = 0,
) -> None:
    """Validate arguments of DfcacheClient.import_file.

    Args:
        path: Local file path to import.
        replica_count: Requested persistent replica count.
        ttl: Time to live of the persistent cache task.
        piece_length: Piece length in bytes, 0 means auto-computed by dfdaemon.

    Raises:
        DfcacheValidationError: On any rule violation mirroring the CLI rules.
    """
    if not path:
        raise DfcacheValidationError("path must not be empty")

    if path.endswith(os.sep) and len(path) > 1:
        raise DfcacheValidationError(f"path {path} is a directory")

    if os.path.isdir(path):
        raise DfcacheValidationError(f"path {path} is a directory")

    # A dir check under race, existence check mirrors CLI. In containers the
    # file may be on a different node if importing on behalf of a peer, but
    # dfcache semantics require a local readable path.
    if not os.path.isfile(path):
        raise DfcacheValidationError(f"path {path} does not exist or is not a regular file")

    if not (MIN_REPLICA_COUNT <= replica_count <= MAX_REPLICA_COUNT):
        raise DfcacheValidationError(
            f"replica_count must be between {MIN_REPLICA_COUNT} and {MAX_REPLICA_COUNT}, got {replica_count}"
        )

    if ttl < MIN_TTL or ttl > MAX_TTL:
        raise DfcacheValidationError(
            f"ttl must be between {MIN_TTL} and {MAX_TTL}, got {ttl}"
        )

    if piece_length and piece_length < MIN_PIECE_LENGTH:
        raise DfcacheValidationError(
            f"piece length {piece_length} bytes is less than the minimum piece length {MIN_PIECE_LENGTH} bytes"
        )


def validate_export(
    task_id: str,
    output: str,
    digest: object = None,
) -> None:
    """Validate arguments of DfcacheClient.export_file.

    Args:
        task_id: Persistent cache task id to export.
        output: Destination path to write the exported file to.
        digest: Optional digest string in "<algorithm>:<hash>" format or None.

    Raises:
        DfcacheValidationError: On any rule violation.
    """
    validate_task_id(task_id)

    if not output:
        raise DfcacheValidationError("output must not be empty")

    if os.path.isdir(output):
        raise DfcacheValidationError(f"output {output} is a directory")

    if digest is not None:
        validate_digest(digest)


def validate_digest(digest: str) -> None:
    """Validate a digest string in "<algorithm>:<hash>" format.

    Args:
        digest: Digest string, e.g. "sha256:<hex>".

    Raises:
        DfcacheValidationError: If the format or algorithm is unsupported.
    """
    if not isinstance(digest, str) or not _DIGEST_RE.match(digest):
        raise DfcacheValidationError(
            f"invalid digest {digest!r}, expected format '<algorithm>:<hash>' with algorithm in {list(DigestAlgorithm.ALL)}"
        )
