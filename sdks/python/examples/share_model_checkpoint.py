#!/usr/bin/env python3
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

"""Share a model checkpoint between training nodes via the P2P cache.

Scenario: node A imports the newest checkpoint after each training epoch;
every other node exports it directly through the local dfdaemon instead of
pulling from shared storage.

Prerequisites:
    * dfdaemon running on the node (see the Dragonfly helm chart).

Usage:
    python share_model_checkpoint.py import /checkpoints/epoch-10.safetensors
    python share_model_checkpoint.py export <task-id> /checkpoints/epoch-10.safetensors
"""

import argparse
import hashlib
import sys
from datetime import timedelta

from dragonfly_cache import DfcacheClient, DfcacheError, DfcacheTaskNotFoundError

# Scope the task id to "this model + this artifact". Importing the same key
# again is idempotent: dfdaemon computes the task id from this content.
KEY_PREFIX = "my-org/my-model/finetune-v3"


def sha256sum(path: str) -> str:
    """Stream a file through sha256 without loading it into memory.

    Args:
        path: The file to hash.

    Returns:
        Hex digest of the file content.
    """
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)

    import_parser = sub.add_parser("import", help="import a checkpoint into the P2P cache")
    import_parser.add_argument("path", help="local checkpoint file to import")

    export_parser = sub.add_parser("export", help="export a checkpoint from the P2P cache")
    export_parser.add_argument("task_id", help="persistent cache task id")
    export_parser.add_argument("output", help="destination file path")
    export_parser.add_argument(
        "--reference",
        help="local file whose sha256 the export must match (integrity check)",
    )

    args = parser.parse_args()

    try:
        with DfcacheClient() as client:
            if args.command == "import":
                task = client.import_file(
                    args.path,
                    # Large files: derive the id from a stable logical key
                    # instead of hashing tens of GiB of content.
                    content_for_calculating_task_id=(
                        f"{KEY_PREFIX}/{args.path.rsplit('/', 1)[-1]}"
                    ),
                    tag="llm",
                    application="training",
                    replica_count=3,
                    ttl=timedelta(hours=6),
                )
                print(f"imported as task {task.id} (state={task.state})")
                print("share this id with the other nodes:")
                print(f"  python {sys.argv[0]} export {task.id} /checkpoints/...")
            else:
                digest = f"sha256:{sha256sum(args.reference)}" if args.reference else None
                client.export_file(args.task_id, output=args.output, digest=digest, overwrite=True)
                print(f"exported task {args.task_id} to {args.output}")
    except DfcacheTaskNotFoundError as err:
        print(f"task not found: {err}", file=sys.stderr)
        return 1
    except DfcacheError as err:
        print(f"dfcache error: {err}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
