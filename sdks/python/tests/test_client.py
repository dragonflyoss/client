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

"""End-to-end-ish client tests against an in-process fake dfdaemon server."""

from datetime import timedelta

import grpc
import pytest

from dragonfly_cache import (
    DfcacheClient,
    DfcacheConnectionError,
    DfcacheTaskNotFoundError,
    PersistentCacheTask,
)


class TestDfcacheClientInit:
    def test_missing_socket_raises_connection_error(self, tmp_path):
        missing_socket = str(tmp_path / "nonexistent.sock")

        with pytest.raises(DfcacheConnectionError, match="does not exist"):
            DfcacheClient(missing_socket)


class TestImport:
    def test_import_returns_task(self, client, fake_dfdaemon, sample_file):
        task = client.import_file(sample_file, tag="llm", application="training")

        assert isinstance(task, PersistentCacheTask)
        assert task.id == "ca30f9b2-default-task-id"
        assert task.state == "Succeeded"
        assert task.ttl == timedelta(hours=1)

        fake, _, _ = fake_dfdaemon
        req = fake.upload_requests[0]
        assert req.tag == "llm"
        assert req.application == "training"
        assert req.persistent_replica_count == 2

    def test_import_maps_request_fields(self, client, fake_dfdaemon, sample_file):
        fake, _, _ = fake_dfdaemon
        client.import_file(
            sample_file,
            content_for_calculating_task_id="deepseek-ocr/weights",
            replica_count=3,
            ttl=timedelta(hours=2),
            piece_length=4 * 1024 * 1024,
        )

        req = fake.upload_requests[0]
        assert req.content_for_calculating_task_id == "deepseek-ocr/weights"
        assert req.persistent_replica_count == 3
        assert req.piece_length == 4 * 1024 * 1024
        assert req.ttl.seconds == 7200


class TestExport:
    def test_export_returns_resolved_path(self, client, fake_dfdaemon, sample_file, tmp_path):
        task = client.import_file(
            sample_file, content_for_calculating_task_id="deepseek-ocr/weights"
        )
        output = tmp_path / "exported" / "model.bin"

        result = client.export_file(task.id, output=output)

        assert result == output
        fake, _, _ = fake_dfdaemon
        req = fake.download_requests[0]
        assert req.task_id == task.id
        assert req.need_piece_content is False
        assert str(output) in req.output_path

    def test_export_unknown_task_raises_not_found(self, client, tmp_path):
        output = tmp_path / "out.bin"

        with pytest.raises(DfcacheTaskNotFoundError, match="does-not-exist-task"):
            client.export_file("does-not-exist-task-1", output=output)

    def test_export_mid_stream_failure_cleans_partial_file(self, client, fake_dfdaemon, sample_file, tmp_path):
        task = client.import_file(sample_file)
        output = tmp_path / "out.bin"
        output.write_bytes(b"partial")  # Pre-existing partial file.

        # Simulate dfdaemon failing after the transfer started: the started
        # response is delivered, then the stream breaks with UNAVAILABLE.
        fake, _, _ = fake_dfdaemon
        fake.fail_stream_after_start = True

        with pytest.raises(DfcacheConnectionError, match="peer lost"):
            client.export_file(task.id, output=output)

        assert not output.exists()  # Partial file must be removed.


class TestStatAndDelete:
    def test_stat_returns_task_info(self, client, sample_file):
        task = client.import_file(sample_file)

        info = client.stat(task.id)

        assert info.id == task.id
        assert info.content_length == 1024
        assert info.piece_count == 1

    def test_stat_unknown_task_raises_not_found(self, client):
        with pytest.raises(DfcacheTaskNotFoundError):
            client.stat("does-not-exist-task-1")

    def test_delete_removes_task(self, client, sample_file):
        task = client.import_file(sample_file)

        client.delete(task.id)
        with pytest.raises(DfcacheTaskNotFoundError):
            client.stat(task.id)

    def test_export_after_delete_fails(self, client, sample_file, tmp_path):
        task = client.import_file(sample_file)
        client.delete(task.id)

        with pytest.raises(DfcacheTaskNotFoundError):
            client.export_file(task.id, output=tmp_path / "out.bin")


class TestModels:
    def test_from_proto_maps_duration_and_timestamps(self, fake_dfdaemon, client, sample_file):
        task = client.import_file(sample_file)

        assert task.created_at == 1700000000
        assert task.ttl.total_seconds() == 3600

    def test_str_is_readable(self, client, sample_file):
        task = client.import_file(sample_file)
        assert "state=Succeeded" in str(task)
