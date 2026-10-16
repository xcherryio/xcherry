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
from collections.abc import Iterator
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
from dex.dexpb import dex_pb2 as pb
from dex.dexpb import dex_pb2_grpc
from dex.flow import Registry


class RecordingService(
    dex_pb2_grpc.FlowServiceServicer, dex_pb2_grpc.WorkerTaskServiceServicer
):
    def __init__(self) -> None:
        self.headers: list[tuple[str, str | bytes | None]] = []
        self.poll_opened = threading.Event()
        self.poll_count = 0
        self.reconnect_first_poll = False

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
        self.poll_count += 1
        self.poll_opened.set()
        next(request_iterator)
        if self.reconnect_first_poll and self.poll_count == 1:
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


def test_async_client_bearer_and_local_failure(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    asyncio.run(_check_async_client_bearer_and_local_failure(service_endpoint))


async def _check_async_client_bearer_and_local_failure(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    address, service = service_endpoint
    cache = cast(BlobCache, object())
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
        ClientOptions(server_address=address, credential=FailedCredential()),
    ) as client:
        with pytest.raises(ValueError, match="token unavailable"):
            await client.health_check()
    assert service.headers == [
        ("HealthCheck", None),
        ("HealthCheck", "Bearer async"),
    ]


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


def test_async_worker_pull_reconnect_rotates_token(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    asyncio.run(_check_async_worker_pull_reconnect_rotates_token(service_endpoint))


def test_async_worker_pull_default_has_no_authorization(
    service_endpoint: tuple[str, RecordingService],
) -> None:
    asyncio.run(_check_async_worker_pull_default_has_no_authorization(service_endpoint))


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
    service.reconnect_first_poll = True
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
        for _ in range(100):
            if service.poll_count >= 2:
                break
            await asyncio.sleep(0.01)
        assert service.poll_count >= 2
    finally:
        await worker.stop()
        await started
    assert ("GetServerInfo", "Bearer first") in service.headers
    assert ("SyncAttributeIndexes", "Bearer first") in service.headers
    assert ("PollWorkerTask", "Bearer first") in service.headers
    assert ("PollWorkerTask", "Bearer second") in service.headers
