# Copyright (c) 2026 Super Durable, Inc.
#
# Licensed under the Sustainable Use License 1.0.
# You may not use this file except in compliance with the License.
# See the LICENSE file in the repository root.
#
# SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

from __future__ import annotations

import asyncio
import threading
import time
import traceback
from collections.abc import Awaitable, Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import cast

import grpc
import pytest
from google.protobuf import empty_pb2

from dex import (
    AsyncClient,
    AsyncWorker,
    BlobCache,
    Client,
    ClientOptions,
    FileCredential,
    PullWorkerOptions,
    Worker,
    WorkerOptions,
)
from dex._credential_transport import AsyncCredentialRunner
from dex.dexpb import dex_pb2 as pb
from dex.dexpb import dex_pb2_grpc
from dex.flow import Registry


class RecordingService(
    dex_pb2_grpc.FlowServiceServicer, dex_pb2_grpc.WorkerTaskServiceServicer
):
    def __init__(self) -> None:
        self.headers: list[tuple[str, str | bytes | None]] = []
        self.poll_opened = threading.Event()
        self.second_poll_opened = threading.Event()
        self.reconnect_allowed = threading.Event()
        self._poll_lock = threading.Lock()
        self._poll_count = 0
        self.should_reconnect_first_poll = False

    def _record(self, method: str, context: grpc.ServicerContext) -> None:
        metadata = dict(context.invocation_metadata())
        self.headers.append((method, metadata.get("authorization")))

    def HealthCheck(  # noqa: N802
        self, request: empty_pb2.Empty, context: grpc.ServicerContext
    ) -> pb.HealthInfo:
        self._record("HealthCheck", context)
        return pb.HealthInfo()

    def GetServerInfo(  # noqa: N802
        self, request: empty_pb2.Empty, context: grpc.ServicerContext
    ) -> pb.ServerInfo:
        self._record("GetServerInfo", context)
        return pb.ServerInfo(
            server_version="test",
            minimum_supported_protocol_version=1,
            current_protocol_version=1,
        )

    def SyncAttributeIndexes(  # noqa: N802
        self, request: pb.SyncAttributeIndexRequest, context: grpc.ServicerContext
    ) -> pb.SyncAttributeIndexResponse:
        self._record("SyncAttributeIndexes", context)
        return pb.SyncAttributeIndexResponse()

    def PollWorkerTask(  # noqa: N802
        self,
        request_iterator: Iterator[pb.PollWorkerTaskRequest],
        context: grpc.ServicerContext,
    ) -> Iterator[pb.PollWorkerTaskResponse]:
        self._record("PollWorkerTask", context)
        with self._poll_lock:
            self._poll_count += 1
            poll_count = self._poll_count
        self.poll_opened.set()
        if poll_count >= 2:
            self.second_poll_opened.set()
        next(request_iterator)
        if self.should_reconnect_first_poll and poll_count == 1:
            assert self.reconnect_allowed.wait(3)
            yield pb.PollWorkerTaskResponse(reconnect=pb.PollWorkerTaskReconnect())
        else:
            yield pb.PollWorkerTaskResponse(registered=pb.PollWorkerTaskRegistered())
            for _ in request_iterator:
                pass


@pytest.fixture
def service_endpoint() -> Iterator[tuple[str, RecordingService]]:
    service = RecordingService()
    server = grpc.server(ThreadPoolExecutor(max_workers=6))
    dex_pb2_grpc.add_FlowServiceServicer_to_server(  # type: ignore[no-untyped-call]
        service, server
    )
    dex_pb2_grpc.add_WorkerTaskServiceServicer_to_server(  # type: ignore[no-untyped-call]
        service, server
    )
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    try:
        yield f"127.0.0.1:{port}", service
    finally:
        server.stop(grace=0).wait(timeout=3)


class MutableCredential:
    def __init__(self, value: str) -> None:
        self.value = value

    def token(self) -> str:
        return self.value


class FailedCredential:
    def token(self) -> str:
        raise ValueError("token unavailable")


class FailOnSecondCredential:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._calls = 0

    def token(self) -> str:
        with self._lock:
            self._calls += 1
            if self._calls == 2:
                raise ValueError("second token unavailable")
        return "first-token"


class FailAfterStartupCredential:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._calls = 0
        self.failed = threading.Event()

    def token(self) -> str:
        with self._lock:
            self._calls += 1
            if self._calls > 2:
                self.failed.set()
                raise ValueError("pull token unavailable")
        return "startup-token"


class AsyncCredential:
    def __init__(self) -> None:
        self.called_threads: list[int] = []

    async def token(self) -> str:
        self.called_threads.append(threading.get_ident())
        await asyncio.sleep(0)
        return "async-token"


class BlockedAsyncCredential:
    async def token(self) -> str:
        await asyncio.Event().wait()
        return "unreachable"


class BlockedSyncCredential:
    def __init__(self) -> None:
        self.release = threading.Event()

    def token(self) -> str:
        self.release.wait(2)
        return "late-token"


class TimeoutCredential:
    def token(self) -> str:
        raise TimeoutError("token provider timed out")


class RuntimeErrorCredential:
    def __init__(self) -> None:
        self.calls = 0

    def token(self) -> str:
        self.calls += 1
        raise RuntimeError("no running event loop")


class LoopBoundCredential:
    def __init__(self) -> None:
        self.called_threads: list[int] = []
        self.foreign_loop = asyncio.new_event_loop()

    def token(self) -> asyncio.Future[str]:
        self.called_threads.append(threading.get_ident())
        future: asyncio.Future[str] = self.foreign_loop.create_future()
        future.set_result("loop-token")
        return future


class CoroutineReturningCredential:
    def __init__(self) -> None:
        self.called_threads: list[int] = []

    def token(self) -> Awaitable[str]:
        self.called_threads.append(threading.get_ident())
        return asyncio.sleep(0, result="coroutine-token")


class SignaledBlockingCredential:
    def __init__(self, fast_calls: int = 0) -> None:
        self.started = threading.Event()
        self.release = threading.Event()
        self._lock = threading.Lock()
        self._remaining_fast_calls = fast_calls

    def token(self) -> str | Awaitable[str]:
        with self._lock:
            if self._remaining_fast_calls:
                self._remaining_fast_calls -= 1
                return "startup-token"
        self.started.set()
        self.release.wait(10)
        return "late-token"


def test_sync_client_bearer_and_default(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    address, service = service_endpoint
    cache = cast(BlobCache, object())
    with Client(Registry([]), cache, ClientOptions(server_address=address)) as client:
        assert client.health_check()
    credential = MutableCredential("first")
    with Client(
        Registry([]),
        cache,
        ClientOptions(server_address=address, credential=credential),
    ) as client:
        assert client.health_check()
        credential.value = "second"
        assert client.health_check()
    assert service.headers == [
        ("HealthCheck", None),
        ("HealthCheck", "Bearer first"),
        ("HealthCheck", "Bearer second"),
    ]


def test_sync_client_credential_failure_stays_local(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    address, service = service_endpoint
    with Client(
        Registry([]),
        cast(BlobCache, object()),
        ClientOptions(server_address=address, credential=FailedCredential()),
    ) as client:
        with pytest.raises(ValueError, match="token unavailable"):
            client.health_check()
    assert service.headers == []


def test_async_client_bearer_and_local_failure(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    asyncio.run(_check_async_client_bearer_and_local_failure(service_endpoint))


async def _check_async_client_bearer_and_local_failure(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    address, service = service_endpoint
    cache = cast(BlobCache, object())
    async_credential = AsyncCredential()
    async with AsyncClient(
        Registry([]), cache, ClientOptions(server_address=address)
    ) as client:
        assert await client.health_check()
    async with AsyncClient(
        Registry([]),
        cache,
        ClientOptions(server_address=address, credential=MutableCredential("async")),
    ) as client:
        assert await client.health_check()
    async with AsyncClient(
        Registry([]),
        cache,
        ClientOptions(server_address=address, credential=async_credential),
    ) as client:
        assert await client.health_check()
    async with AsyncClient(
        Registry([]),
        cache,
        ClientOptions(server_address=address, credential=FailedCredential()),
    ) as client:
        with pytest.raises(ValueError, match="token unavailable"):
            await client.health_check()
    assert service.headers == [
        ("HealthCheck", None),
        ("HealthCheck", "Bearer async"),
        ("HealthCheck", "Bearer async-token"),
    ]
    assert async_credential.called_threads == [threading.get_ident()]


def test_async_credential_deadline_stays_local(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    asyncio.run(_check_async_credential_deadline_stays_local(service_endpoint))


async def _check_async_credential_deadline_stays_local(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    address, service = service_endpoint
    for credential in (BlockedAsyncCredential(), BlockedSyncCredential()):
        async with AsyncClient(
            Registry([]),
            cast(BlobCache, object()),
            ClientOptions(server_address=address, credential=credential),
        ) as client:
            try:
                with pytest.raises(grpc.aio.AioRpcError) as failure:
                    await client._service.HealthCheck(empty_pb2.Empty(), timeout=0.05)
                assert failure.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED
            finally:
                if isinstance(credential, BlockedSyncCredential):
                    credential.release.set()
    assert service.headers == []


def test_async_client_preserves_credential_timeout(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    asyncio.run(_check_async_client_preserves_credential_timeout(service_endpoint))


async def _check_async_client_preserves_credential_timeout(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    address, service = service_endpoint
    async with AsyncClient(
        Registry([]),
        cast(BlobCache, object()),
        ClientOptions(server_address=address, credential=TimeoutCredential()),
    ) as client:
        with pytest.raises(TimeoutError, match="token provider timed out"):
            await client.health_check()
        with pytest.raises(TimeoutError, match="token provider timed out"):
            await client._service.HealthCheck(empty_pb2.Empty(), timeout=1)
    assert service.headers == []


def test_async_client_preserves_runtime_error_once(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    asyncio.run(_check_async_client_preserves_runtime_error_once(service_endpoint))


async def _check_async_client_preserves_runtime_error_once(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    address, service = service_endpoint
    credential = RuntimeErrorCredential()
    async with AsyncClient(
        Registry([]),
        cast(BlobCache, object()),
        ClientOptions(server_address=address, credential=credential),
    ) as client:
        with pytest.raises(RuntimeError, match="no running event loop"):
            await client.health_check()
    assert credential.calls == 1
    assert service.headers == []


def test_closing_credential_runner_fails_pending_lookup() -> None:
    credential = SignaledBlockingCredential()
    try:
        asyncio.run(_check_closing_credential_runner_fails_pending_lookup(credential))
    finally:
        credential.release.set()


async def _check_closing_credential_runner_fails_pending_lookup(
    credential: SignaledBlockingCredential,
) -> None:
    runner = AsyncCredentialRunner(credential)
    lookup = asyncio.create_task(runner.token())
    assert await asyncio.to_thread(credential.started.wait, 2)
    runner.close()
    with pytest.raises(RuntimeError, match="credential runner is closed"):
        await asyncio.wait_for(lookup, timeout=0.2)


def test_loop_bound_future_requires_async_token(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    asyncio.run(_check_loop_bound_future_requires_async_token(service_endpoint))


async def _check_loop_bound_future_requires_async_token(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    address, service = service_endpoint
    credential = LoopBoundCredential()
    try:
        async with AsyncClient(
            Registry([]),
            cast(BlobCache, object()),
            ClientOptions(server_address=address, credential=credential),
        ) as client:
            with pytest.raises(TypeError, match="async def token"):
                await client.health_check()
        assert credential.called_threads != [threading.get_ident()]
        assert len(credential.called_threads) == 1
        assert service.headers == []
    finally:
        credential.foreign_loop.close()


def test_sync_function_returning_coroutine_runs_once(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    asyncio.run(_check_sync_function_returning_coroutine_runs_once(service_endpoint))


async def _check_sync_function_returning_coroutine_runs_once(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    address, service = service_endpoint
    credential = CoroutineReturningCredential()
    async with AsyncClient(
        Registry([]),
        cast(BlobCache, object()),
        ClientOptions(server_address=address, credential=credential),
    ) as client:
        assert await client.health_check()
    assert len(credential.called_threads) == 1
    assert credential.called_threads[0] != threading.get_ident()
    assert service.headers == [("HealthCheck", "Bearer coroutine-token")]


def test_slow_sync_credential_does_not_block_other_executors(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    credential = SignaledBlockingCredential()
    started_at = time.monotonic()
    try:
        asyncio.run(
            _check_slow_sync_credential_does_not_block_other_executors(
                service_endpoint, credential
            )
        )
        assert time.monotonic() - started_at < 2
    finally:
        credential.release.set()


async def _check_slow_sync_credential_does_not_block_other_executors(
    service_endpoint: tuple[str, RecordingService],
    credential: SignaledBlockingCredential,
) -> None:
    address, service = service_endpoint
    cache = cast(BlobCache, object())
    async with AsyncClient(
        Registry([]),
        cache,
        ClientOptions(server_address=address, credential=credential),
    ) as slow_client:
        assert slow_client._credential_runner is not None
        assert slow_client._credential_runner._threads == []
        slow_calls = [
            asyncio.ensure_future(
                slow_client._service.HealthCheck(empty_pb2.Empty(), timeout=0.3)
            )
            for _ in range(6)
        ]
        assert await asyncio.wait_for(
            asyncio.to_thread(credential.started.wait, 2), timeout=2.5
        )
        assert len(slow_client._credential_runner._threads) == 4
        assert (
            await asyncio.wait_for(asyncio.to_thread(lambda: "ready"), 0.2) == "ready"
        )
        async with AsyncClient(
            Registry([]),
            cache,
            ClientOptions(server_address=address, credential=MutableCredential("fast")),
        ) as fast_client:
            assert await asyncio.wait_for(fast_client.health_check(), 0.2)
        failures = await asyncio.gather(*slow_calls, return_exceptions=True)
        assert all(
            isinstance(failure, grpc.aio.AioRpcError)
            and failure.code() == grpc.StatusCode.DEADLINE_EXCEEDED
            for failure in failures
        )
    assert service.headers == [("HealthCheck", "Bearer fast")]


def test_file_credential_rotation_and_empty_file(
    tmp_path: Path, service_endpoint: tuple[str, RecordingService]
) -> None:
    address, service = service_endpoint
    path = tmp_path / "token"
    with pytest.raises(OSError):
        FileCredential(path)
    path.write_text(" \n ")
    with pytest.raises(ValueError, match="empty"):
        FileCredential(path)
    path.write_text(" first \n")
    credential = FileCredential(path)
    client = Client(
        Registry([]),
        cast(BlobCache, object()),
        ClientOptions(server_address=address, credential=credential),
    )
    assert credential.token() == "first"
    assert client.health_check()
    checked_at = credential._checked_at
    credential._clock = lambda: checked_at + 0.5
    path.write_text("second-longer")
    assert credential.token() == "first"
    credential._clock = lambda: checked_at + 1.1
    assert credential.token() == "second-longer"
    assert client.health_check()
    path.write_text(" \n")
    credential._clock = lambda: checked_at + 2.2
    with pytest.raises(ValueError, match="empty"):
        credential.token()
    with pytest.raises(ValueError, match="empty"):
        client.health_check()
    with pytest.raises(ValueError, match="empty") as first_failure:
        credential.token()
    with pytest.raises(ValueError, match="empty") as second_failure:
        credential.token()
    assert first_failure.value is not second_failure.value
    assert len(traceback.extract_tb(first_failure.value.__traceback__)) == len(
        traceback.extract_tb(second_failure.value.__traceback__)
    )
    path.unlink()
    credential._clock = lambda: checked_at + 3.3
    with pytest.raises(OSError):
        credential.token()
    path.write_text("restored")
    credential._clock = lambda: checked_at + 4.4
    assert credential.token() == "restored"
    assert client.health_check()
    client.close()
    assert service.headers == [
        ("HealthCheck", "Bearer first"),
        ("HealthCheck", "Bearer second-longer"),
        ("HealthCheck", "Bearer restored"),
    ]


def test_sync_worker_flow_calls_use_credential(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    address, service = service_endpoint
    worker = Worker(
        Registry([]),
        cast(BlobCache, object()),
        WorkerOptions(server_address=address, credential=MutableCredential("worker")),
    )
    try:
        worker._flow_service.GetServerInfo(empty_pb2.Empty())
        worker._flow_service.SyncAttributeIndexes(pb.SyncAttributeIndexRequest())
    finally:
        worker.close()
    assert service.headers == [
        ("GetServerInfo", "Bearer worker"),
        ("SyncAttributeIndexes", "Bearer worker"),
    ]


def test_sync_worker_start_credential_failure_cleans_up(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    address, service = service_endpoint
    worker = Worker(
        Registry([]),
        cast(BlobCache, object()),
        WorkerOptions(server_address=address, credential=FailedCredential()),
    )
    with pytest.raises(RuntimeError, match="server info"):
        worker.start()
    assert worker._state == "stopped"
    assert service.headers == []


def test_sync_worker_index_sync_credential_failure_cleans_up(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    address, service = service_endpoint
    worker = Worker(
        Registry([]),
        cast(BlobCache, object()),
        WorkerOptions(server_address=address, credential=FailOnSecondCredential()),
    )
    with pytest.raises(RuntimeError, match="synchronize Attribute indexes"):
        worker.start()
    assert worker._state == "stopped"
    assert service.headers == [("GetServerInfo", "Bearer first-token")]


def test_sync_pull_worker_stream_uses_credential(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    address, service = service_endpoint
    worker = Worker(
        Registry([]),
        cast(BlobCache, object()),
        WorkerOptions(
            server_address=address,
            pull=PullWorkerOptions(app_worker_id="sync-credential-worker"),
            credential=MutableCredential("sync-worker"),
        ),
    )
    with ThreadPoolExecutor(max_workers=1) as executor:
        started = executor.submit(worker.start)
        try:
            assert service.poll_opened.wait(3)
        finally:
            worker.stop()
        started.result(timeout=3)
    assert ("GetServerInfo", "Bearer sync-worker") in service.headers
    assert ("SyncAttributeIndexes", "Bearer sync-worker") in service.headers
    assert ("PollWorkerTask", "Bearer sync-worker") in service.headers


def test_sync_pull_stop_cancels_pending_credential(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    address, service = service_endpoint
    credential = SignaledBlockingCredential(fast_calls=2)
    worker = Worker(
        Registry([]),
        cast(BlobCache, object()),
        WorkerOptions(
            server_address=address,
            pull=PullWorkerOptions(app_worker_id="sync-stop-credential-worker"),
            credential=credential,
        ),
    )
    with ThreadPoolExecutor(max_workers=1) as executor:
        started = executor.submit(worker.start)
        try:
            assert credential.started.wait(3)
            stopped_at = time.monotonic()
            worker.stop()
            assert time.monotonic() - stopped_at < 1
            started.result(timeout=2)
        finally:
            credential.release.set()
            worker.close()
    assert all(method != "PollWorkerTask" for method, _ in service.headers)


def test_async_worker_pull_reconnect_rotates_token(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    asyncio.run(_check_async_worker_pull_reconnect_rotates_token(service_endpoint))


def test_async_pull_stop_cancels_pending_credential(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    credential = SignaledBlockingCredential(fast_calls=2)
    started_at = time.monotonic()
    try:
        asyncio.run(
            _check_async_pull_stop_cancels_pending_credential(
                service_endpoint, credential
            )
        )
        assert time.monotonic() - started_at < 2
    finally:
        credential.release.set()


async def _check_async_pull_stop_cancels_pending_credential(
    service_endpoint: tuple[str, RecordingService],
    credential: SignaledBlockingCredential,
) -> None:
    address, service = service_endpoint
    worker = AsyncWorker(
        Registry([]),
        cast(BlobCache, object()),
        WorkerOptions(
            server_address=address,
            pull=PullWorkerOptions(app_worker_id="async-stop-credential-worker"),
            credential=credential,
        ),
    )
    started = asyncio.create_task(worker.start())
    try:
        assert await asyncio.to_thread(credential.started.wait, 3)
        stopped_at = time.monotonic()
        await worker.stop()
        assert time.monotonic() - stopped_at < 1
        await started
    finally:
        credential.release.set()
        await worker.close()
    assert all(method != "PollWorkerTask" for method, _ in service.headers)


def test_async_worker_pull_default_has_no_authorization(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    asyncio.run(_check_async_worker_pull_default_has_no_authorization(service_endpoint))


def test_async_worker_start_credential_failure_cleans_up(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    asyncio.run(
        _check_async_worker_start_credential_failure_cleans_up(service_endpoint)
    )


async def _check_async_worker_start_credential_failure_cleans_up(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    address, service = service_endpoint
    worker = AsyncWorker(
        Registry([]),
        cast(BlobCache, object()),
        WorkerOptions(server_address=address, credential=FailedCredential()),
    )
    with pytest.raises(RuntimeError, match="server info"):
        await worker.start()
    assert worker._state == "stopped"
    assert worker._stopped.is_set()
    assert service.headers == []


def test_async_worker_index_sync_credential_failure_cleans_up(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    asyncio.run(
        _check_async_worker_index_sync_credential_failure_cleans_up(service_endpoint)
    )


async def _check_async_worker_index_sync_credential_failure_cleans_up(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    address, service = service_endpoint
    worker = AsyncWorker(
        Registry([]),
        cast(BlobCache, object()),
        WorkerOptions(server_address=address, credential=FailOnSecondCredential()),
    )
    with pytest.raises(RuntimeError, match="synchronize Attribute indexes"):
        await worker.start()
    assert worker._state == "stopped"
    assert worker._stopped.is_set()
    assert service.headers == [("GetServerInfo", "Bearer first-token")]


def test_pull_credential_failure_does_not_open_stream(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    asyncio.run(_check_pull_credential_failure_does_not_open_stream(service_endpoint))


async def _check_pull_credential_failure_does_not_open_stream(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    address, service = service_endpoint
    credential = FailAfterStartupCredential()
    worker = AsyncWorker(
        Registry([]),
        cast(BlobCache, object()),
        WorkerOptions(
            server_address=address,
            pull=PullWorkerOptions(app_worker_id="failed-pull-credential-worker"),
            credential=credential,
        ),
    )
    started = asyncio.create_task(worker.start())
    try:
        assert await asyncio.to_thread(credential.failed.wait, 3)
    finally:
        await worker.stop()
        await started
    assert ("GetServerInfo", "Bearer startup-token") in service.headers
    assert ("SyncAttributeIndexes", "Bearer startup-token") in service.headers
    assert all(method != "PollWorkerTask" for method, _ in service.headers)


async def _check_async_worker_pull_default_has_no_authorization(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    address, service = service_endpoint
    worker = AsyncWorker(
        Registry([]),
        cast(BlobCache, object()),
        WorkerOptions(
            server_address=address,
            pull=PullWorkerOptions(app_worker_id="default-credential-worker"),
        ),
    )
    started = asyncio.create_task(worker.start())
    try:
        assert await asyncio.to_thread(service.poll_opened.wait, 3)
    finally:
        await worker.stop()
        await started
    assert ("GetServerInfo", None) in service.headers
    assert ("SyncAttributeIndexes", None) in service.headers
    assert ("PollWorkerTask", None) in service.headers


async def _check_async_worker_pull_reconnect_rotates_token(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    address, service = service_endpoint
    service.should_reconnect_first_poll = True
    credential = MutableCredential("first")
    worker = AsyncWorker(
        Registry([]),
        cast(BlobCache, object()),
        WorkerOptions(
            server_address=address,
            pull=PullWorkerOptions(app_worker_id="credential-test-worker"),
            credential=credential,
        ),
    )
    started = asyncio.create_task(worker.start())
    try:
        assert await asyncio.to_thread(service.poll_opened.wait, 3)
        credential.value = "second"
        service.reconnect_allowed.set()
        assert await asyncio.to_thread(service.second_poll_opened.wait, 3)
    finally:
        service.reconnect_allowed.set()
        await worker.stop()
        await started
    assert ("GetServerInfo", "Bearer first") in service.headers
    assert ("SyncAttributeIndexes", "Bearer first") in service.headers
    assert ("PollWorkerTask", "Bearer first") in service.headers
    assert ("PollWorkerTask", "Bearer second") in service.headers
