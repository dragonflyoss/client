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

"""Exercise real protobuf serialization and gRPC streaming over a local socket.

The server models dfdaemon's wire contract; it is NOT a real Dragonfly cluster.
"""

import hashlib
import io
import tempfile
import unittest
from array import array
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import boto3
import grpc
from botocore.stub import Stubber
from dragonfly_api import common_pb2, dfdaemon_pb2, dfdaemon_pb2_grpc

from reader import MIB, PIECE_LENGTH, DragonflyReader, open_s3_reader


class PieceServer(dfdaemon_pb2_grpc.DfdaemonDownloadServicer):
    def __init__(self, content):
        self.content = content
        self.requests = []
        self.mode = "normal"

    def DownloadTask(self, request, context):
        download = request.download
        self.requests.append(download)
        # The range and credentials deliberately do not participate in identity.
        task_id = hashlib.sha256(
            f"{download.url}|{download.tag}|{download.piece_length}".encode()
        ).hexdigest()
        start = download.range.start
        end = min(start + download.range.length, len(self.content))
        numbers = range(start // PIECE_LENGTH, (end - 1) // PIECE_LENGTH + 1)
        pieces = [
            common_pb2.Piece(
                number=number,
                offset=number * PIECE_LENGTH,
                length=min(PIECE_LENGTH, len(self.content) - number * PIECE_LENGTH),
            )
            for number in numbers
        ]
        yield dfdaemon_pb2.DownloadTaskResponse(
            task_id="" if self.mode == "missing_task" else task_id,
            download_task_started_response=dfdaemon_pb2.DownloadTaskStartedResponse(
                content_length=len(self.content),
                range=(
                    common_pb2.Range(start=start + 1, length=end - start)
                    if self.mode == "wrong_range"
                    else download.range
                ),
                pieces=pieces,
            ),
        )
        for index, piece in enumerate(reversed(pieces)):
            if self.mode == "missing" and index == 0:
                continue
            piece.content = self.content[piece.offset : piece.offset + piece.length]
            if self.mode == "truncated":
                piece.content = piece.content[:-1]
            if self.mode == "wrong_offset":
                piece.offset += 1
            response = dfdaemon_pb2.DownloadTaskResponse(
                task_id="different-task" if self.mode == "wrong_task" else task_id,
                download_piece_finished_response=dfdaemon_pb2.DownloadPieceFinishedResponse(
                    piece=piece,
                ),
            )
            yield response
            if self.mode == "duplicate":
                yield response
        if self.mode == "terminal_error":
            context.abort(grpc.StatusCode.INTERNAL, "Simulated final stream failure")


@contextmanager
def serve(content):
    # Short pathname: Unix sockets have a much smaller path limit than files.
    with tempfile.TemporaryDirectory(prefix="df-video-", dir="/tmp") as directory:
        address = "unix:" + str(Path(directory) / "rpc.sock")
        service = PieceServer(content)
        service.address = address
        server = grpc.server(ThreadPoolExecutor(max_workers=2))
        dfdaemon_pb2_grpc.add_DfdaemonDownloadServicer_to_server(service, server)
        if not server.add_insecure_port(address):
            raise RuntimeError("Could not bind test socket")
        server.start()
        try:
            with grpc.insecure_channel(
                address,
                options=[("grpc.max_receive_message_length", PIECE_LENGTH + MIB)],
            ) as channel:
                grpc.channel_ready_future(channel).result(timeout=5)
                yield service, dfdaemon_pb2_grpc.DfdaemonDownloadStub(channel)
        finally:
            server.stop(0).wait()


def example_credentials():
    return common_pb2.ObjectStorage(
        region="us-east-1",
        access_key_id="test-only",
        access_key_secret="test-only",
        session_token="test-only-token",
    )


class ReaderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Different, nonzero patterns in each piece expose offset mistakes.
        cls.content = (
            b"".join(
                bytes((byte + number * 31) % 256 for byte in range(256))
                * (PIECE_LENGTH // 256)
                for number in range(3)
            )
            + b"short final piece"
        )

    def setUp(self):
        self.context = serve(self.content)
        self.service, stub = self.context.__enter__()
        self.addCleanup(self.context.__exit__, None, None, None)
        self.reader = DragonflyReader(
            stub,
            "s3://test/video.mp4",
            len(self.content),
            example_credentials,
            cache_tag="immutable-test-object",
            cache_pieces=2,
        )
        self.addCleanup(self.reader.close)

    def test_unaligned_out_of_order_pieces_and_cache(self):
        start, length = PIECE_LENGTH - 17, 63
        self.assertEqual(
            self.reader.read_range(start, length), self.content[start : start + length]
        )
        self.assertEqual(
            self.reader.read_range(PIECE_LENGTH + 10, 20),
            self.content[PIECE_LENGTH + 10 : PIECE_LENGTH + 30],
        )
        self.assertEqual(len(self.service.requests), 1)
        self.assertEqual(self.reader.piece_bytes_received, 2 * PIECE_LENGTH)
        request = self.service.requests[0]
        self.assertTrue(request.need_piece_content)
        self.assertFalse(request.prefetch)
        self.assertEqual(request.scheduling_policy, common_pb2.ALWAYS)
        self.assertFalse(request.HasField("output_path"))
        self.assertFalse(request.request_header)

    def test_seek_readinto_eof_and_cache_bound(self):
        self.reader.seek(PIECE_LENGTH + 11)
        buffer = bytearray(100)
        self.assertEqual(self.reader.readinto(buffer), 100)
        self.assertEqual(buffer, self.content[PIECE_LENGTH + 11 : PIECE_LENGTH + 111])
        self.reader.seek(-6, io.SEEK_END)
        self.assertEqual(self.reader.read(100), self.content[-6:])
        self.assertEqual(self.reader.read(1), b"")
        self.reader.read_range(0, 1)
        self.assertEqual(len(self.reader.cache), 2)
        self.reader.seek(len(self.content) + 100)
        self.assertEqual(self.reader.read(1), b"")
        with self.assertRaises(ValueError):
            self.reader.seek(-1)

    def test_rotating_credentials_keep_object_task_identity(self):
        tokens = iter(["first-session", "second-session"])

        def credentials():
            value = example_credentials()
            value.session_token = next(tokens)
            return value

        self.reader.credentials = credentials
        self.reader.read_range(0, 1)
        task_id = self.reader.task_id
        self.reader.read_range(2 * PIECE_LENGTH, 1)
        self.assertEqual(self.reader.task_id, task_id)
        self.assertEqual(
            [r.object_storage.session_token for r in self.service.requests],
            ["first-session", "second-session"],
        )

    def test_reject_missing_duplicate_truncated_and_final_error(self):
        for mode, exception in [
            ("missing", OSError),
            ("duplicate", OSError),
            ("truncated", OSError),
            ("wrong_offset", OSError),
            ("wrong_task", OSError),
            ("wrong_range", OSError),
            ("missing_task", OSError),
            ("terminal_error", grpc.RpcError),
        ]:
            with self.subTest(mode=mode):
                self.service.mode = mode
                with self.assertRaises(exception):
                    self.reader.read_range(PIECE_LENGTH - 10, 20)
                self.assertEqual(len(self.reader.cache), 0)
                self.assertIsNone(self.reader.task_id)

    def test_failed_read_does_not_advance_or_cache_and_can_retry(self):
        self.reader.seek(PIECE_LENGTH - 10)
        self.service.mode = "terminal_error"
        with self.assertRaises(grpc.RpcError):
            self.reader.read(20)
        self.assertEqual(self.reader.tell(), PIECE_LENGTH - 10)
        self.assertFalse(self.reader.cache)
        self.service.mode = "normal"
        self.assertEqual(
            self.reader.read(20), self.content[PIECE_LENGTH - 10 : PIECE_LENGTH + 10]
        )

    def test_empty_ranges_and_closed_reader_do_not_make_requests(self):
        self.assertEqual(self.reader.read_range(0, 0), b"")
        self.assertEqual(self.reader.read_range(len(self.content), 1), b"")
        self.reader.close()
        for action in (
            self.reader.tell,
            self.reader.read,
            lambda: self.reader.seek(0),
            lambda: self.reader.read_range(0, 1),
        ):
            with self.assertRaises(ValueError):
                action()
        self.assertFalse(self.service.requests)

    def test_binary_io_argument_validation(self):
        for action in (
            lambda: self.reader.seek(0.5),
            lambda: self.reader.seek(0, 0.5),
            lambda: self.reader.read(1.5),
            lambda: self.reader.read_range(0.5, 1),
            lambda: self.reader.read_range(0, 1.5),
            lambda: self.reader.readinto(b"readonly"),
        ):
            with self.assertRaises(TypeError):
                action()
        self.assertEqual(self.reader.tell(), 0)
        self.assertFalse(self.service.requests)
        buffer = array("I", [0, 0])
        self.assertEqual(self.reader.readinto(buffer), buffer.itemsize * len(buffer))
        self.assertEqual(buffer.tobytes(), self.content[: len(buffer.tobytes())])

    def test_auto_policy_and_disabled_application_cache(self):
        self.reader.scheduling_policy = "auto"
        self.reader.cache_pieces = 0
        for _ in range(2):
            self.assertEqual(self.reader.read_range(0, 1), self.content[:1])
        self.assertEqual(len(self.service.requests), 2)
        self.assertFalse(self.reader.cache)
        self.assertEqual(self.service.requests[0].scheduling_policy, common_pb2.AUTO)

    def test_changed_size_and_allocation_limit(self):
        self.reader.size += 1
        with self.assertRaisesRegex(OSError, "object size changed"):
            self.reader.read_range(0, 10)
        self.reader.max_range_bytes = 8
        self.reader.seek(0)
        with self.assertRaises(io.UnsupportedOperation):
            self.reader.read()
        with self.assertRaises(ValueError):
            self.reader.read_range(0, 9)

    def test_boto3_metadata_and_sts_wiring(self):
        session = boto3.Session(
            aws_access_key_id="test-only",
            aws_secret_access_key="test-only",
            aws_session_token="session-from-boto3",
            region_name="us-east-1",
        )
        s3 = session.client("s3", endpoint_url="http://s3.test")
        with (
            Stubber(s3) as metadata,
            patch("reader.boto3.Session", return_value=session),
            patch.object(session, "client", return_value=s3),
        ):
            metadata.add_response(
                "head_object",
                {"ContentLength": len(self.content), "ETag": '"fixture"'},
                {"Bucket": "test", "Key": "video.mp4"},
            )
            with open_s3_reader(
                "s3://test/video.mp4",
                socket=self.service.address.removeprefix("unix:"),
                endpoint="http://s3.test",
            ) as reader:
                self.assertEqual(reader.read_range(100, 10), self.content[100:110])
            metadata.assert_no_pending_responses()
        options = self.service.requests[0].object_storage
        self.assertEqual(options.session_token, "session-from-boto3")
        self.assertEqual(options.endpoint, "http://s3.test")

    def test_encoded_key_uses_the_same_object_for_head_and_download(self):
        session = boto3.Session(
            aws_access_key_id="test-only", aws_secret_access_key="test-only"
        )
        s3 = session.client("s3", endpoint_url="http://s3.test")
        with (
            Stubber(s3) as metadata,
            patch("reader.boto3.Session", return_value=session),
            patch.object(session, "client", return_value=s3),
        ):
            metadata.add_response(
                "head_object",
                {"ContentLength": len(self.content), "ETag": '"fixture"'},
                {"Bucket": "test", "Key": "folder/a b+100%.bin"},
            )
            with open_s3_reader(
                "s3://test/folder/a%20b%2B100%25.bin",
                socket=self.service.address.removeprefix("unix:"),
            ) as reader:
                self.assertEqual(reader.read(10), self.content[:10])
            metadata.assert_no_pending_responses()
        self.assertEqual(
            self.service.requests[0].url, "s3://test/folder/a%20b%2B100%25.bin"
        )
        self.assertEqual(
            self.service.requests[0].object_storage.endpoint, "http://s3.test"
        )

    def test_reject_ambiguous_urls_before_metadata_requests(self):
        for url in (
            "https://test/key",
            "s3://test/",
            "s3://user@test/key",
            "s3://test:9000/key",
            "s3://test/a/../b",
            "s3://test/a/%2E/b",
            "s3://test//key",
            "s3://test/key?versionId=123",
            "s3://test/key#part",
            "s3://test/a%2F%2Fb",
            "s3://test/a\\b",
            "s3://test/a\nb",
        ):
            with self.subTest(url=url), patch("reader.boto3.Session") as session:
                with self.assertRaises(ValueError), open_s3_reader(url):
                    self.fail("URL should have been rejected")
                session.assert_not_called()


if __name__ == "__main__":
    unittest.main()
