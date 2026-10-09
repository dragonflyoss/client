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

"""A real indexed three-hour MP4, served by the local gRPC test double."""

import io
import random
import unittest
from fractions import Fraction

try:
    import av
except ImportError:
    av = None

from reader import PIECE_LENGTH, DragonflyReader

if av is not None:
    from video_chunks import frames_in_window
from test_reader import example_credentials, serve


def three_hour_video():
    output = io.BytesIO()
    rng = random.Random(42)
    with av.open(output, mode="w", format="mp4") as container:
        # One frame every 10 seconds keeps this test small. Random texture makes
        # the file span several real 4 MiB pieces; it is not padded with zeros.
        stream = container.add_stream("mpeg4", rate=Fraction(1, 10))
        stream.width, stream.height = 160, 128
        stream.pix_fmt = "yuv420p"
        stream.bit_rate = 100_000
        stream.codec_context.gop_size = 6
        for index in range(1080):
            frame = av.VideoFrame(160, 128, "yuv420p")
            for plane in frame.planes:
                plane.update(rng.randbytes(plane.buffer_size))
            frame.pts = index
            frame.time_base = Fraction(10, 1)
            for packet in stream.encode(frame):
                container.mux(packet)
        for packet in stream.encode():
            container.mux(packet)
    return output.getvalue()


@unittest.skipIf(av is None, "Install requirements-video.txt to test video decoding")
class VideoTests(unittest.TestCase):
    def test_two_minutes_at_two_hours_without_full_download(self):
        video = three_hour_video()
        self.assertGreater(len(video), 4 * PIECE_LENGTH)
        expected = []
        # Sequential local decode is the reference; don't use the same seek
        # helper to produce both the expected and actual frames.
        with av.open(io.BytesIO(video)) as local:
            for frame in local.decode(video=0):
                timestamp = float(frame.pts * frame.time_base)
                if 7200 <= timestamp < 7320:
                    expected.append((timestamp, bytes(frame.planes[0])))
        with serve(video) as (service, stub):
            with DragonflyReader(
                stub,
                "s3://test/three-hours.mp4",
                len(video),
                example_credentials,
                cache_tag="three-hour-fixture",
            ) as reader:
                with av.open(reader, mode="r") as remote:
                    actual = []
                    for start in (7200, 7260):
                        actual.extend(
                            (timestamp, bytes(frame.planes[0]))
                            for timestamp, frame in frames_in_window(remote, start, 60)
                        )
                self.assertEqual([x[0] for x in actual], list(range(7200, 7320, 10)))
                self.assertEqual(actual, expected)
                self.assertLess(reader.piece_bytes_received, len(video))
                self.assertTrue(
                    all(not request.prefetch for request in service.requests)
                )
                print(
                    f"\nThree-hour video: {len(video):,} bytes; two one-minute windows: "
                    f"{len(actual)} verified frames; {reader.rpc_count} RPCs; "
                    f"{reader.piece_bytes_received:,} piece bytes transferred"
                )


if __name__ == "__main__":
    unittest.main()
