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

"""Tests that failed Worker startup and failed close steps release every resource."""

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
def refused_server_address() -> str:
    """Returns an address with no listener, so Worker startup fails."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return f"127.0.0.1:{probe.getsockname()[1]}"


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


def _new_async_app(server_address: str, tmp_path: Path) -> ExampleApp:
    return ExampleApp(
        ExamplesConfig(
            server_address=server_address,
            worker_bind_address="127.0.0.1:0",
            worker_target=None,
            http_address="127.0.0.1:0",
            blob_cache_dir=tmp_path / "blob-cache",
            worker_mode=WorkerMode.PULL,
        )
    )


def _new_sync_app(server_address: str, tmp_path: Path) -> SyncExampleApp:
    return SyncExampleApp(
        SyncExamplesConfig(
            server_address=server_address,
            worker_bind_address="127.0.0.1:0",
            worker_target=None,
            http_address="127.0.0.1:0",
            blob_cache_dir=tmp_path / "blob-cache",
            worker_mode=WorkerMode.PULL,
        )
    )


def _record_mcp_registry_close(
    monkeypatch: pytest.MonkeyPatch, app: ExampleApp, failures: int = 0
) -> list[bool]:
    """Records each MCP registry close call; the first ``failures`` calls raise."""
    close_calls: list[bool] = []
    original_mcp_close = app.mcp_registry.close

    async def record_mcp_close() -> None:
        close_calls.append(True)
        if len(close_calls) <= failures:
            raise RuntimeError("MCP registry close failed")
        await original_mcp_close()

    monkeypatch.setattr(app.mcp_registry, "close", record_mcp_close)
    return close_calls


async def test_async_app_closes_resources_when_pull_registration_times_out(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, silent_server_address: str
) -> None:
    caches = _record_blob_cache(monkeypatch, async_app_module)
    app = _new_async_app(silent_server_address, tmp_path)
    closed_mcp_registries = _record_mcp_registry_close(monkeypatch, app)
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
    app = _new_sync_app(silent_server_address, tmp_path)
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


async def test_async_app_closes_resources_when_worker_start_raises(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, refused_server_address: str
) -> None:
    caches = _record_blob_cache(monkeypatch, async_app_module)
    app = _new_async_app(refused_server_address, tmp_path)
    closed_mcp_registries = _record_mcp_registry_close(monkeypatch, app)
    worker_task_holder: list[Any] = []
    original_wait = app.worker.wait_pull_registered

    async def capture_task_and_wait(timeout: float | None = None) -> None:
        worker_task_holder.append(app._worker_task)
        await original_wait(timeout=timeout)

    monkeypatch.setattr(app.worker, "wait_pull_registered", capture_task_and_wait)

    with pytest.raises(RuntimeError, match="GetServerInfo failed"):
        await app.start_worker()

    worker_task = worker_task_holder[0]
    assert worker_task.done() and worker_task.exception() is not None
    assert closed_mcp_registries == [True]
    assert caches[0].is_closed
    with pytest.raises(RuntimeError, match="client is not ready"):
        app.require_client()
    await app.close()
    assert closed_mcp_registries == [True]


# The stopped Worker thread reports its own startup failure after cleanup.
@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_sync_app_closes_resources_when_worker_start_raises(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, refused_server_address: str
) -> None:
    caches = _record_blob_cache(monkeypatch, sync_app_module)
    app = _new_sync_app(refused_server_address, tmp_path)
    worker_threads: list[Any] = []
    original_wait = app.worker.wait_pull_registered

    def capture_thread_and_wait(timeout: float | None = None) -> None:
        worker_threads.append(app._worker_thread)
        original_wait(timeout=timeout)

    monkeypatch.setattr(app.worker, "wait_pull_registered", capture_thread_and_wait)

    with pytest.raises(RuntimeError, match="GetServerInfo failed"):
        app.start_worker()

    assert worker_threads and not worker_threads[0].is_alive()
    assert caches[0].is_closed
    with pytest.raises(RuntimeError, match="client is not ready"):
        app.require_client()
    app.close()


async def test_async_app_close_retries_only_failed_steps(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, refused_server_address: str
) -> None:
    caches = _record_blob_cache(monkeypatch, async_app_module)
    app = _new_async_app(refused_server_address, tmp_path)
    mcp_close_calls = _record_mcp_registry_close(monkeypatch, app, failures=1)

    with pytest.raises(RuntimeError, match="MCP registry close failed"):
        await app.close()

    assert caches[0].is_closed
    with pytest.raises(RuntimeError, match="client is not ready"):
        app.require_client()
    await app.close()
    assert mcp_close_calls == [True, True]
    await app.close()
    assert mcp_close_calls == [True, True]


def test_sync_app_close_retries_only_failed_steps(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, refused_server_address: str
) -> None:
    caches = _record_blob_cache(monkeypatch, sync_app_module)
    app = _new_sync_app(refused_server_address, tmp_path)
    worker_close_calls: list[bool] = []
    original_worker_close = app.worker.close

    def fail_first_worker_close() -> None:
        worker_close_calls.append(True)
        if len(worker_close_calls) == 1:
            raise RuntimeError("Worker close failed")
        original_worker_close()

    monkeypatch.setattr(app.worker, "close", fail_first_worker_close)

    with pytest.raises(RuntimeError, match="Worker close failed"):
        app.close()

    assert caches[0].is_closed
    app.close()
    assert worker_close_calls == [True, True]
    app.close()
    assert worker_close_calls == [True, True]
