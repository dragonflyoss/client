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

"""Validator tests mirroring validate_args_checks_ttl_path_and_piece_length
in dragonfly-client/src/bin/dfcache/import.rs."""

from datetime import timedelta

import pytest

from dragonfly_cache.exceptions import DfcacheValidationError
from dragonfly_cache.validators import (
    MIN_PIECE_LENGTH,
    validate_digest,
    validate_export,
    validate_import,
    validate_task_id,
)


class TestValidateImport:
    """Case-mirrors the Rust table test in dfcache import.rs."""

    @pytest.mark.parametrize("ttl", [timedelta(minutes=5), timedelta(days=7)])
    def test_ttl_bounds_ok(self, tmp_path, ttl):
        path = tmp_path / "file.txt"
        path.write_text("x")

        validate_import(str(path), 2, ttl)  # Should not raise.

    @pytest.mark.parametrize(
        "ttl",
        [
            timedelta(seconds=299),  # Below 5m, mirrors the Rust 299s case.
            timedelta(seconds=604801),  # Above 7d, mirrors the Rust 604801s case.
        ],
    )
    def test_ttl_out_of_bounds(self, tmp_path, ttl):
        path = tmp_path / "file.txt"
        path.write_text("x")

        with pytest.raises(DfcacheValidationError, match="ttl must be between"):
            validate_import(str(path), 2, ttl)

    def test_piece_length_ok(self, tmp_path):
        path = tmp_path / "file.txt"
        path.write_text("x")

        validate_import(str(path), 2, timedelta(hours=1), piece_length=4 * 1024 * 1024)

    def test_piece_length_below_minimum(self, tmp_path):
        path = tmp_path / "file.txt"
        path.write_text("x")

        with pytest.raises(DfcacheValidationError, match="less than the minimum piece length"):
            validate_import(str(path), 2, timedelta(hours=1), piece_length=1024 * 1024)

    def test_directory_rejected(self, tmp_path):
        with pytest.raises(DfcacheValidationError, match="is a directory"):
            validate_import(str(tmp_path), 2, timedelta(hours=1))

    def test_missing_file_rejected(self, tmp_path):
        missing = str(tmp_path / "missing.txt")

        with pytest.raises(DfcacheValidationError, match="does not exist"):
            validate_import(missing, 2, timedelta(hours=1))

    @pytest.mark.parametrize("replica_count", [0, 6])
    def test_replica_count_out_of_bounds(self, tmp_path, replica_count):
        path = tmp_path / "file.txt"
        path.write_text("x")

        with pytest.raises(DfcacheValidationError, match="replica_count must be between"):
            validate_import(str(path), replica_count, timedelta(hours=1))

    @pytest.mark.parametrize("replica_count", [1, 3, 5])
    def test_replica_count_ok(self, tmp_path, replica_count):
        path = tmp_path / "file.txt"
        path.write_text("x")

        validate_import(str(path), replica_count, timedelta(hours=1))


class TestValidateExport:
    def test_ok(self):
        validate_export("e06ac5f4-task-id-01", "/tmp/out.bin", None)

    def test_empty_output_rejected(self):
        with pytest.raises(DfcacheValidationError, match="output must not be empty"):
            validate_export("e06ac5f4-task-id-01", "", None)

    def test_output_dir_rejected(self, tmp_path):
        with pytest.raises(DfcacheValidationError, match="is a directory"):
            validate_export("e06ac5f4-task-id-01", str(tmp_path), None)

    def test_bad_task_id_rejected(self):
        with pytest.raises(DfcacheValidationError, match="task_id must not be empty"):
            validate_export("", "/tmp/out.bin", None)

    def test_overlong_task_id_rejected(self):
        with pytest.raises(DfcacheValidationError, match="longer than"):
            validate_export("t" * 129, "/tmp/out.bin", None)

    def test_bad_digest_rejected(self):
        with pytest.raises(DfcacheValidationError, match="invalid digest"):
            validate_export("e06ac5f4-task-id-01", "/tmp/out.bin", "md5:abc")


class TestValidateDigest:
    @pytest.mark.parametrize(
        "digest",
        ["crc32:9a9f0822", "sha256:9f86d081884c7d659a2feaa0c55ad015", "sha512:abc12def"],
    )
    def test_supported(self, digest):
        validate_digest(digest)

    @pytest.mark.parametrize("digest", ["", "md5:abc", "sha256", "sha256:nothex!", 42])
    def test_rejected(self, digest):
        with pytest.raises(DfcacheValidationError):
            validate_digest(digest)
