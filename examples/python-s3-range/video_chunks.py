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

"""Decode minute-long windows without first downloading the whole video."""

import argparse
from fractions import Fraction

import av

from reader import open_s3_reader


def frames_in_window(container, start_seconds, duration_seconds=60):
    """Yield (relative timestamp, VideoFrame) in [start, start+duration).

    Keep the container open across windows. The demuxer obtains metadata/index
    bytes and performs byte seeks through DragonflyReader. It may decode from
    an earlier keyframe and read ahead beyond the requested window.
    """
    start = Fraction(str(start_seconds))
    duration = Fraction(str(duration_seconds))
    if start < 0 or duration <= 0:
        raise ValueError("Start must be nonnegative and duration positive")
    if not container.streams.video:
        raise ValueError("No video stream")
    stream = container.streams.video[0]
    origin = stream.start_time or 0
    target = origin + int(start / stream.time_base)
    container.seek(target, stream=stream, backward=True, any_frame=False)
    origin_seconds = origin * stream.time_base
    for frame in container.decode(stream):
        if frame.pts is None or frame.time_base is None:
            raise ValueError("Video requires timestamps for time-window selection")
        timestamp = frame.pts * frame.time_base - origin_seconds
        if timestamp < start:
            continue
        if timestamp >= start + duration:
            break
        yield float(timestamp), frame


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url", help="Immutable s3://bucket/key video object")
    parser.add_argument(
        "--start", type=float, default=7200, help="Seconds from video start"
    )
    parser.add_argument("--chunk-seconds", type=float, default=60)
    parser.add_argument("--chunks", type=int, default=1)
    parser.add_argument("--socket", default="/var/run/dragonfly/dfdaemon.sock")
    parser.add_argument("--region")
    parser.add_argument("--endpoint", help="Custom S3 endpoint, e.g. MinIO")
    parser.add_argument("--profile")
    parser.add_argument(
        "--timeout", type=float, default=60, help="Deadline per gRPC call"
    )
    parser.add_argument(
        "--scheduling-policy",
        choices=("auto", "always"),
        default="always",
        help="always enables peer lookup for small byte ranges (v1.5.5+)",
    )
    args = parser.parse_args()
    if args.chunks < 1 or args.start < 0 or args.chunk_seconds <= 0:
        parser.error("Invalid start, chunk duration, or chunk count")
    with open_s3_reader(
        args.url,
        socket=args.socket,
        region=args.region,
        endpoint=args.endpoint,
        profile=args.profile,
        timeout=args.timeout,
        scheduling_policy=args.scheduling_policy,
    ) as reader:
        with av.open(reader, mode="r") as container:
            for index in range(args.chunks):
                start = args.start + index * args.chunk_seconds
                count, first, last = 0, None, None
                for timestamp, frame in frames_in_window(
                    container, start, args.chunk_seconds
                ):
                    # Feed frame.to_ndarray(format="rgb24") into your training
                    # pipeline here. Process incrementally; don't retain hours.
                    count += 1
                    if first is None:
                        first = timestamp
                    last = timestamp
                print(
                    f"[{start:g}, {start + args.chunk_seconds:g}) seconds: "
                    f"{count} frames; first={first}, last={last}"
                )
        print(
            f"Object bytes: {reader.size:,}; gRPC calls: {reader.rpc_count}; "
            f"piece bytes received: {reader.piece_bytes_received:,}; "
            f"requested range bytes: {reader.range_bytes_requested:,}"
        )


if __name__ == "__main__":
    main()
