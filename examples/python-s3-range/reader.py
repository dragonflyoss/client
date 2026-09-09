# Copyright 2026 The Dragonfly Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Seekable, bounded-memory native-S3 reader for Dragonfly's v2 gRPC API.

Use one reader per worker and immutable S3 object keys. This is a client example,
not an S3 proxy or a tenant authorization boundary.
"""

import io
import re
from collections import OrderedDict
from collections.abc import Callable
from contextlib import closing, contextmanager
from operator import index
from urllib.parse import quote, unquote, urlsplit

import boto3
import grpc
from dragonfly_api import common_pb2, dfdaemon_pb2, dfdaemon_pb2_grpc

MIB = 1024 * 1024
PIECE_LENGTH = 4 * MIB


class DragonflyReader(io.RawIOBase):
    """Return exact byte ranges, preserving whole-object Dragonfly task identity.

    The daemon returns FULL pieces with absolute offsets. They can arrive out of
    order. Only successful, complete RPCs populate the small client-side cache.
    Dfdaemon still uses its own on-disk piece cache.
    """

    def __init__(
        self,
        stub,
        url: str,
        size: int,
        credentials: Callable[[], common_pb2.ObjectStorage],
        *,
        cache_tag: str,
        cache_pieces: int = 4,
        timeout: float = 60,
        max_range_bytes: int = 64 * MIB,
        scheduling_policy: str = "always",
    ):
        super().__init__()
        self.cache = OrderedDict()
        size, cache_pieces, max_range_bytes = map(
            index, (size, cache_pieces, max_range_bytes)
        )
        if size < 0 or cache_pieces < 0 or max_range_bytes <= 0 or timeout <= 0:
            raise ValueError("Invalid size, cache capacity, range limit, or timeout")
        if scheduling_policy not in ("auto", "always"):
            raise ValueError("Scheduling policy must be auto or always")
        self.stub = stub
        self.url = url
        self.size = size
        self.credentials = credentials
        self.cache_tag = cache_tag
        self.cache_pieces = cache_pieces
        self.timeout = timeout
        self.max_range_bytes = max_range_bytes
        self.scheduling_policy = scheduling_policy
        self.position = 0
        self.task_id = None
        self.rpc_count = 0
        self.piece_bytes_received = 0
        self.range_bytes_requested = 0

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        self._checkClosed()
        return self.position

    def seek(self, offset, whence=io.SEEK_SET):
        self._checkClosed()
        offset, whence = index(offset), index(whence)
        bases = {io.SEEK_SET: 0, io.SEEK_CUR: self.position, io.SEEK_END: self.size}
        if whence not in bases:
            raise ValueError("Invalid whence")
        position = bases[whence] + offset
        if position < 0:
            raise ValueError("Negative seek position")
        self.position = position
        return position

    def read(self, size=-1):
        self._checkClosed()
        size = -1 if size is None else index(size)
        remaining = max(0, self.size - self.position)
        if size is None or size < 0:
            # Protect a training process from accidentally materializing hours
            # of video via read() or readall(). PyAV uses bounded reads.
            if remaining > self.max_range_bytes:
                raise io.UnsupportedOperation(
                    "Use bounded read(size) for large objects"
                )
            size = remaining
        data = self.read_range(self.position, min(size, remaining))
        self.position += len(data)
        return data

    def readall(self):
        return self.read()

    def readinto(self, buffer):
        self._checkClosed()
        with memoryview(buffer).cast("B") as view:
            if view.readonly:
                raise TypeError("readinto requires a writable buffer")
            data = self.read(view.nbytes)
            view[: len(data)] = data
            return len(data)

    def read_range(self, start: int, length: int) -> bytes:
        """Read [start, start+length), clipped at EOF, without moving tell()."""
        self._checkClosed()
        start, length = index(start), index(length)
        if start < 0 or length < 0:
            raise ValueError("Negative range")
        length = min(length, max(0, self.size - start))
        if length > self.max_range_bytes:
            raise ValueError("Range exceeds memory limit; read in smaller windows")
        if length == 0:
            return b""
        end = start + length
        numbers = range(start // PIECE_LENGTH, (end - 1) // PIECE_LENGTH + 1)
        if all(number in self.cache for number in numbers):
            pieces = {number: self.cache[number] for number in numbers}
            for number in numbers:
                self.cache.move_to_end(number)
        else:
            pieces = self._download(start, length, numbers)

        # Keep source coordinates in the daemon; compact only the returned bytes.
        output = bytearray(length)
        for number in numbers:
            piece_start = number * PIECE_LENGTH
            content = pieces[number]
            lo = max(start, piece_start)
            hi = min(end, piece_start + len(content))
            output[lo - start : hi - start] = content[
                lo - piece_start : hi - piece_start
            ]
        return bytes(output)

    def _download(self, start, length, numbers):
        request = dfdaemon_pb2.DownloadTaskRequest(
            download=common_pb2.Download(
                url=self.url,
                type=common_pb2.STANDARD,
                range=common_pb2.Range(start=start, length=length),
                piece_length=PIECE_LENGTH,
                tag=self.cache_tag,
                need_piece_content=True,
                prefetch=False,
                # AUTO bypasses the scheduler for ranges <= 4 MiB. Video
                # demuxers often request only a few KiB, so opt into peer lookup.
                scheduling_policy=(
                    common_pb2.ALWAYS
                    if self.scheduling_policy == "always"
                    else common_pb2.AUTO
                ),
                object_storage=self.credentials(),
                # No output_path, HTTP Authorization/Range, or range-derived ID.
            )
        )
        self.rpc_count += 1
        self.range_bytes_requested += length
        call = self.stub.DownloadTask(request, timeout=self.timeout)
        expected = set(numbers)
        pieces = {}
        started = False
        task_id = None
        try:
            for response in call:
                kind = response.WhichOneof("response")
                if kind == "download_task_started_response":
                    metadata = response.download_task_started_response
                    if started or metadata.content_length != self.size:
                        raise OSError("Unexpected metadata or object size changed")
                    if (
                        not metadata.HasField("range")
                        or metadata.range.start != start
                        or metadata.range.length != length
                    ):
                        raise OSError("Daemon did not honor the requested range")
                    task_id = response.task_id
                    if not task_id or (
                        self.task_id is not None and task_id != self.task_id
                    ):
                        raise OSError(
                            "Object task identity changed across range requests"
                        )
                    started = True
                elif kind == "download_piece_finished_response":
                    piece = response.download_piece_finished_response.piece
                    if not started or response.task_id != task_id:
                        raise OSError("Piece arrived without matching task metadata")
                    expected_offset = piece.number * PIECE_LENGTH
                    expected_length = min(PIECE_LENGTH, self.size - expected_offset)
                    if (
                        piece.number not in expected
                        or piece.number in pieces
                        or piece.offset != expected_offset
                        or piece.length != expected_length
                        or not piece.HasField("content")
                        or len(piece.content) != piece.length
                    ):
                        raise OSError(
                            "Missing, duplicate, unexpected, or malformed piece"
                        )
                    self.piece_bytes_received += len(piece.content)
                    pieces[piece.number] = piece.content
                else:
                    raise OSError("Unexpected download response")
            # Exhaust the stream even after all bytes arrive: its final status
            # may be an error. Never silently return zero-filled missing bytes.
            if not started or set(pieces) != expected:
                raise OSError("Incomplete range response")
        finally:
            call.cancel()

        self.task_id = task_id
        for number, content in pieces.items():
            self.cache[number] = content
            self.cache.move_to_end(number)
            while len(self.cache) > self.cache_pieces:
                self.cache.popitem(last=False)
        return pieces

    def close(self):
        self.cache.clear()
        super().close()


@contextmanager
def open_s3_reader(
    url,
    *,
    socket="/var/run/dragonfly/dfdaemon.sock",
    region=None,
    endpoint=None,
    profile=None,
    timeout=60,
    scheduling_policy="always",
):
    """HEAD metadata with boto3; transfer object bytes through dfdaemon.

    The socket must belong to a trusted daemon: each RPC includes credentials.
    Boto3 resolves credentials normally (including role/web-identity providers).
    Obtain a fresh frozen snapshot before each RPC; this does not refresh a
    credential already sent on an in-flight RPC.
    """
    import hashlib
    import json

    if any(ord(char) < 32 or ord(char) == 127 for char in url):
        raise ValueError("Control characters are not supported in S3 URLs")
    parsed = urlsplit(url)
    if (
        parsed.scheme != "s3"
        or not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", parsed.netloc)
        or not parsed.path.startswith("/")
    ):
        raise ValueError("Expected s3://bucket/key")
    if parsed.query or parsed.fragment:
        raise ValueError("Use an immutable object key, without query or fragment")
    key = unquote(parsed.path[1:], errors="strict")
    # Rust's URL parser and the storage backend normalize path segments. Avoid
    # HEAD and GET referring to different keys; this example supports file keys.
    if any(part in ("", ".", "..") for part in key.split("/")) or "\\" in key:
        raise ValueError("Empty, dot, and backslash key segments are not supported")
    url = f"s3://{parsed.netloc}/{quote(key, safe='/')}"
    session = boto3.Session(profile_name=profile, region_name=region)
    region = region or session.region_name or "us-east-1"
    with closing(session.client("s3", region_name=region, endpoint_url=endpoint)) as s3:
        metadata = s3.head_object(Bucket=parsed.netloc, Key=key)
        # Include endpoints supplied through boto3's environment/config as well
        # as the explicit --endpoint argument in the daemon's S3 configuration.
        endpoint = s3.meta.endpoint_url

    # Stable across ranges/credential rotation, distinct across endpoints and
    # object revisions observed at open. A tag is a cache namespace, not auth.
    cache_tag = hashlib.sha256(
        json.dumps(
            [endpoint or "aws", region, metadata.get("VersionId"), metadata.get("ETag")]
        ).encode()
    ).hexdigest()

    def credentials():
        provider = session.get_credentials()
        if provider is None:
            raise RuntimeError("No AWS credentials found")
        frozen = provider.get_frozen_credentials()
        options = dict(
            region=region,
            access_key_id=frozen.access_key,
            access_key_secret=frozen.secret_key,
        )
        if frozen.token:
            options["session_token"] = frozen.token
        if endpoint:
            options["endpoint"] = endpoint
        return common_pb2.ObjectStorage(**options)

    # The Python gRPC default receive limit is too small for a 4 MiB piece plus
    # protobuf metadata. Request a fixed legal piece length and allow headroom.
    with grpc.insecure_channel(
        "unix:" + socket,
        # Python gRPC otherwise derives :authority from the Unix socket path.
        # The Rust HTTP/2 server rejects that authority before dispatching the
        # RPC. This is routing metadata for the local socket, not an S3 Host.
        options=[
            ("grpc.default_authority", "localhost"),
            ("grpc.max_receive_message_length", PIECE_LENGTH + MIB),
        ],
    ) as channel:
        with DragonflyReader(
            dfdaemon_pb2_grpc.DfdaemonDownloadStub(channel),
            url,
            metadata["ContentLength"],
            credentials,
            cache_tag=cache_tag,
            timeout=timeout,
            scheduling_policy=scheduling_policy,
        ) as reader:
            yield reader
