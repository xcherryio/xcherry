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

"""Tests that a failed pull Worker registration closes every started resource."""

from __future__ import annotations

import socket
import threading
from pathlib import Path
from typing import Any, Iterator

import pytest
from dex import BlobCache, BlobCacheConfig, open_blob_cache

import dex_examples.app as async_app_module
import sync_examples.app as sync_app_module
from dex_examples.app import ExampleApp
from dex_examples.config import ExamplesConfig
from dex_examples.shared.worker_mode import WorkerMode
from sync_examples.app import SyncExampleApp
from sync_examples.config import SyncExamplesConfig

REGISTRATION_TIMEOUT_SECONDS = 0.5


class CloseRecordingBlobCache:
    """Delegates to a native BlobCache and records whether it was closed."""

    def __init__(self, cache: BlobCache) -> None:
        self._cache = cache
        self.is_closed = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cache, name)

    def close(self) -> None:
        self.is_closed = True
        self._cache.close()


@pytest.fixture()
def silent_server_address() -> Iterator[str]:
    """Accepts connections but never replies, so pull registration times out."""
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    # Resetting pending connections soon after the timeout lets the Worker stop quickly.
    closer = threading.Timer(REGISTRATION_TIMEOUT_SECONDS * 2, listener.close)
    closer.start()
    try:
        yield f"127.0.0.1:{listener.getsockname()[1]}"
    finally:
        closer.cancel()
        listener.close()


def _record_blob_cache(
    monkeypatch: pytest.MonkeyPatch, module: Any
) -> list[CloseRecordingBlobCache]:
    caches: list[CloseRecordingBlobCache] = []

    def open_recording_blob_cache(config: BlobCacheConfig) -> CloseRecordingBlobCache:
        cache = CloseRecordingBlobCache(open_blob_cache(config))
        caches.append(cache)
        return cache

    monkeypatch.setattr(module, "open_blob_cache", open_recording_blob_cache)
    monkeypatch.setattr(
        module, "PULL_REGISTRATION_TIMEOUT_SECONDS", REGISTRATION_TIMEOUT_SECONDS
    )
    return caches


async def test_async_app_closes_resources_when_pull_registration_times_out(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, silent_server_address: str
) -> None:
    caches = _record_blob_cache(monkeypatch, async_app_module)
    app = ExampleApp(
        ExamplesConfig(
            server_address=silent_server_address,
            worker_bind_address="127.0.0.1:0",
            worker_target=None,
            http_address="127.0.0.1:0",
            blob_cache_dir=tmp_path / "blob-cache",
            worker_mode=WorkerMode.PULL,
        )
    )
    closed_mcp_registries: list[bool] = []
    original_mcp_close = app.mcp_registry.close

    async def record_mcp_close() -> None:
        closed_mcp_registries.append(True)
        await original_mcp_close()

    monkeypatch.setattr(app.mcp_registry, "close", record_mcp_close)
    worker_task_holder: list[Any] = []
    original_wait = app.worker.wait_pull_registered

    async def capture_task_and_wait(timeout: float | None = None) -> None:
        worker_task_holder.append(app._worker_task)
        await original_wait(timeout=timeout)

    monkeypatch.setattr(app.worker, "wait_pull_registered", capture_task_and_wait)

    with pytest.raises(TimeoutError):
        await app.start_worker()

    assert worker_task_holder and worker_task_holder[0].done()
    assert closed_mcp_registries == [True]
    assert caches[0].is_closed
    with pytest.raises(RuntimeError, match="client is not ready"):
        app.require_client()
    await app.close()


# The stopped Worker thread reports its own startup failure after cleanup.
@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_sync_app_closes_resources_when_pull_registration_times_out(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, silent_server_address: str
) -> None:
    caches = _record_blob_cache(monkeypatch, sync_app_module)
    app = SyncExampleApp(
        SyncExamplesConfig(
            server_address=silent_server_address,
            worker_bind_address="127.0.0.1:0",
            worker_target=None,
            http_address="127.0.0.1:0",
            blob_cache_dir=tmp_path / "blob-cache",
            worker_mode=WorkerMode.PULL,
        )
    )
    worker_threads: list[Any] = []
    original_wait = app.worker.wait_pull_registered

    def capture_thread_and_wait(timeout: float | None = None) -> None:
        worker_threads.append(app._worker_thread)
        original_wait(timeout=timeout)

    monkeypatch.setattr(app.worker, "wait_pull_registered", capture_thread_and_wait)

    with pytest.raises(TimeoutError):
        app.start_worker()

    assert worker_threads and not worker_threads[0].is_alive()
    assert caches[0].is_closed
    with pytest.raises(RuntimeError, match="client is not ready"):
        app.require_client()
    app.close()
