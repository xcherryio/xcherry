# Copyright (c) 2022-2026 Super Durable, Inc.
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

from __future__ import annotations

import asyncio
from collections.abc import Callable
from pathlib import Path

import pytest
from dex import AsyncClient, BlobCacheConfig, open_blob_cache

from dex_examples.config import start_options
from dex_examples.production.pull_workers import start_pull_greeting
from tests.integ.conftest import WAIT_TIMEOUT, server_address

pytestmark = pytest.mark.integ


async def test_pull_worker_serves_greeting_flow(
    client: AsyncClient,
    new_flow_id: Callable[[str], str],
    tmp_path: Path,
) -> None:
    cache = open_blob_cache(BlobCacheConfig(str(tmp_path / "pull-cache"), 64 << 20))
    flow, worker, pull_client, worker_task = await start_pull_greeting(
        cache, server_address()
    )
    try:
        flow_id = new_flow_id("pull-greeting")
        await pull_client.start_flow(flow, flow_id, "Dex", start_options())
        result = await pull_client.wait_for_flow(flow_id, WAIT_TIMEOUT)
        assert result.single_output(str) == "Hello, Dex!"
        assert await pull_client.invoke_rpc(flow.get_greeting, flow_id) == "Hello, Dex!"
        assert not worker_task.done()
    finally:
        await pull_client.close()
        # Remove this cancellation when SDK 1.10.1 ships the #690 shutdown fix.
        worker_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker_task
        await worker.close()
        cache.close()
