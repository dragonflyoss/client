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

"""Tests for the asyncio client against the fake dfdaemon."""

from datetime import timedelta

import pytest

from dragonfly_cache import (
    AsyncDfcacheClient,
    DfcacheConnectionError,
    DfcacheTaskNotFoundError,
)

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def aclient(fake_dfdaemon):
    """An AsyncDfcacheClient connected to the fake dfdaemon."""
    _, socket_path, upload_addr = fake_dfdaemon
    async with AsyncDfcacheClient(socket_path, upload_endpoint=upload_addr) as c:
        yield c


class TestAsyncImport:
    async def test_import_returns_task(self, aclient, fake_dfdaemon, sample_file):
        task = await aclient.import_file(
            sample_file,
            content_for_calculating_task_id="org/model/v1",
            tag="llm",
            application="training",
        )

        assert task.id == "org/model/v1"
        assert task.state == "Succeeded"
        assert task.ttl == timedelta(hours=1)

        fake, _, _ = fake_dfdaemon
        req = fake.upload_requests[0]
        assert req.tag == "llm"
        assert req.application == "training"
        assert req.persistent_replica_count == 2

    async def test_validation_error_does_not_reach_daemon(self, aclient, sample_file):
        with pytest.raises(Exception):
            await aclient.import_file(sample_file, ttl=timedelta(seconds=10))


class TestAsyncExport:
    async def test_export_returns_resolved_path(self, aclient, fake_dfdaemon, sample_file, tmp_path):
        task = await aclient.import_file(sample_file, content_for_calculating_task_id="k")
        output = tmp_path / "out.bin"

        result = await aclient.export_file(task.id, output=output)

        assert result == output.resolve()
        fake, _, _ = fake_dfdaemon
        req = fake.download_requests[0]
        assert req.task_id == task.id
        assert req.need_piece_content is False

    async def test_missing_task_raises_not_found(self, aclient, tmp_path):
        with pytest.raises(DfcacheTaskNotFoundError):
            await aclient.export_file("no-such-task", output=tmp_path / "out.bin")


class TestAsyncStatDelete:
    async def test_stat_round_trip(self, aclient, sample_file):
        task = await aclient.import_file(sample_file, content_for_calculating_task_id="k")

        info = await aclient.stat(task.id)

        assert info.id == task.id
        assert info.state == "Succeeded"

    async def test_delete_routes_to_upload_service(self, aclient, fake_dfdaemon, sample_file):
        task = await aclient.import_file(sample_file, content_for_calculating_task_id="k")

        await aclient.delete(task.id)

        fake, _, _ = fake_dfdaemon
        assert fake.delete_requests[-1].task_id == task.id
        with pytest.raises(DfcacheTaskNotFoundError):
            await aclient.stat(task.id)


class TestAsyncClientConstruction:
    async def test_missing_socket_raises_connection_error(self, tmp_path):
        missing = str(tmp_path / "nonexistent.sock")
        with pytest.raises(DfcacheConnectionError):
            AsyncDfcacheClient(missing)
