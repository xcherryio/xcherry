# Copyright (c) 2026 Super Durable, Inc.
#
# Licensed under the Sustainable Use License 1.0.
# You may not use this file except in compliance with the License.
# See the LICENSE file in the repository root.
#
# SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

from __future__ import annotations

import asyncio
import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from time import monotonic

import grpc
import pytest

from dex.async_worker import AsyncWorker
from dex.attribute import Attribute, AttributeIndex, IndexType
from dex.blob_cache import BlobCacheConfig
from dex.dexpb import dex_pb2 as pb
from dex.dexpb import dex_pb2_grpc
from dex.flow import Flow, PersistenceSchema, Registry
from dex.worker import Worker
from dex.worker_options import PullWorkerOptions, WorkerOptions
from google.protobuf import empty_pb2


class IndexedFlow(Flow[None]):
    status = Attribute(
        "status",
        str,
        AttributeIndex(IndexType.KEYWORD, "PythonWorkerStatus"),
    )

    def get_flow_type(self) -> str:
        return "IndexedFlow"

    def get_persistence_schema(self) -> PersistenceSchema:
        return PersistenceSchema.of(self.status)


class HardDeleteFlow(Flow[None]):
    api_key = Attribute("api-key", str, hard_delete_on_null=True)

    def get_flow_type(self) -> str:
        return "HardDeleteFlow"

    def get_persistence_schema(self) -> PersistenceSchema:
        return PersistenceSchema.of(self.api_key)


class MemoryBlobCache:
    config = BlobCacheConfig("memory", 1_024)

    def get(self, _blob_id: str) -> bytes | None:
        return None

    def put(self, _blob_id: str, _payload: bytes) -> bool:
        return True

    def delete(self, _blob_id: str) -> None:
        return None

    def delete_all(self) -> None:
        return None

    def close(self) -> None:
        return None


class SyncService(dex_pb2_grpc.FlowServiceServicer):
    def __init__(
        self,
        worker_port: int,
        failure: bool = False,
        server_info: pb.ServerInfo | None = None,
        server_info_failure: grpc.StatusCode | None = None,
    ) -> None:
        self.worker_port = worker_port
        self.failure = failure
        self.server_info = (
            server_info
            if server_info is not None
            else pb.ServerInfo(
                server_version="test",
                minimum_supported_protocol_version=1,
                current_protocol_version=1,
            )
        )
        self.server_info_failure = server_info_failure
        self.received: pb.SyncAttributeIndexRequest | None = None
        self.listening_during_sync: bool | None = None
        self.called = threading.Event()
        self.calls: list[str] = []

    def GetServerInfo(  # noqa: N802
        self,
        request: empty_pb2.Empty,
        context: grpc.ServicerContext,
    ) -> pb.ServerInfo:
        self.calls.append("GetServerInfo")
        if self.server_info_failure is not None:
            context.abort(self.server_info_failure, "server info failed")
        return self.server_info

    def SyncAttributeIndexes(  # noqa: N802
        self,
        request: pb.SyncAttributeIndexRequest,
        context: grpc.ServicerContext,
    ) -> pb.SyncAttributeIndexResponse:
        self.calls.append("SyncAttributeIndexes")
        self.received = request
        self.listening_during_sync = _can_connect(self.worker_port)
        self.called.set()
        if self.failure:
            context.abort(grpc.StatusCode.PERMISSION_DENIED, "denied")
        return pb.SyncAttributeIndexResponse()


def test_worker_synchronizes_indexes_before_listening() -> None:
    worker_port = _available_port()
    service = SyncService(worker_port)
    flow_server, flow_port = _start_flow_server(service)
    worker = Worker(
        Registry((IndexedFlow(),)),
        MemoryBlobCache(),
        WorkerOptions(
            bind_address=f"127.0.0.1:{worker_port}",
            server_address=f"127.0.0.1:{flow_port}",
        ),
    )
    failures: list[BaseException] = []

    def run_worker() -> None:
        try:
            worker.start()
        except BaseException as failure:
            failures.append(failure)

    thread = threading.Thread(target=run_worker)
    thread.start()
    try:
        assert service.called.wait(timeout=5)
        assert service.received is not None
        assert (
            service.received.attribute_indexes["PythonWorkerStatus"]
            == pb.INDEX_TYPE_KEYWORD
        )
        assert service.listening_during_sync is False
        assert service.calls == ["GetServerInfo", "SyncAttributeIndexes"]
        _await_listening(worker_port)
    finally:
        worker.stop()
        thread.join(timeout=5)
        flow_server.stop(grace=None).wait(timeout=5)
    assert not thread.is_alive()
    assert failures == []


def test_worker_sync_failure_keeps_port_closed() -> None:
    worker_port = _available_port()
    flow_server, flow_port = _start_flow_server(SyncService(worker_port, failure=True))
    worker = Worker(
        Registry((IndexedFlow(),)),
        MemoryBlobCache(),
        WorkerOptions(
            bind_address=f"127.0.0.1:{worker_port}",
            server_address=f"127.0.0.1:{flow_port}",
        ),
    )
    try:
        with pytest.raises(RuntimeError, match="synchronize Attribute indexes"):
            worker.start()
        assert not _can_connect(worker_port)
    finally:
        worker.close()
        flow_server.stop(grace=None).wait(timeout=5)


@pytest.mark.parametrize(
    ("server_info", "server_info_failure", "message"),
    (
        (
            pb.ServerInfo(
                server_version="future",
                minimum_supported_protocol_version=4,
                current_protocol_version=4,
            ),
            None,
            "do not overlap",
        ),
        (pb.ServerInfo(server_version="invalid"), None, "interval is invalid"),
        (
            pb.ServerInfo(
                server_version="invalid",
                minimum_supported_protocol_version=2,
                current_protocol_version=1,
            ),
            None,
            "interval is invalid",
        ),
        (None, grpc.StatusCode.UNIMPLEMENTED, "GetServerInfo failed"),
        (None, grpc.StatusCode.UNAVAILABLE, "GetServerInfo failed"),
    ),
)
def test_worker_protocol_failure_precedes_sync_and_binding(
    server_info: pb.ServerInfo | None,
    server_info_failure: grpc.StatusCode | None,
    message: str,
) -> None:
    worker_port = _available_port()
    service = SyncService(
        worker_port,
        server_info=server_info,
        server_info_failure=server_info_failure,
    )
    flow_server, flow_port = _start_flow_server(service)
    worker = Worker(
        Registry((IndexedFlow(),)),
        MemoryBlobCache(),
        WorkerOptions(
            bind_address=f"127.0.0.1:{worker_port}",
            server_address=f"127.0.0.1:{flow_port}",
        ),
    )
    try:
        with pytest.raises(RuntimeError, match=message):
            worker.start()
        assert service.calls == ["GetServerInfo"]
        assert not _can_connect(worker_port)
    finally:
        worker.close()
        flow_server.stop(grace=None).wait(timeout=5)


PROTOCOL_2_SERVER_INFO = pb.ServerInfo(
    server_version="previous",
    minimum_supported_protocol_version=1,
    current_protocol_version=2,
)


@pytest.mark.parametrize("is_pull_mode", (False, True))
@pytest.mark.parametrize("flow_type", (IndexedFlow, HardDeleteFlow))
def test_worker_starts_on_protocol_2_server_and_records_version(
    is_pull_mode: bool,
    flow_type: type[Flow[None]],
) -> None:
    worker_port = _available_port()
    service = SyncService(worker_port, server_info=PROTOCOL_2_SERVER_INFO)
    flow_server, flow_port = _start_flow_server(service)
    worker = Worker(
        Registry((flow_type(),)),
        MemoryBlobCache(),
        (
            WorkerOptions(
                server_address=f"127.0.0.1:{flow_port}", pull=PullWorkerOptions()
            )
            if is_pull_mode
            else WorkerOptions(
                bind_address=f"127.0.0.1:{worker_port}",
                server_address=f"127.0.0.1:{flow_port}",
            )
        ),
    )
    failures: list[BaseException] = []

    def run_worker() -> None:
        try:
            worker.start()
        except BaseException as failure:
            failures.append(failure)

    thread = threading.Thread(target=run_worker)
    thread.start()
    try:
        assert service.called.wait(timeout=5)
        if not is_pull_mode:
            _await_listening(worker_port)
        assert service.calls[:2] == ["GetServerInfo", "SyncAttributeIndexes"]
        assert worker._server_protocol.version == 2
    finally:
        worker.stop()
        thread.join(timeout=10)
        flow_server.stop(grace=None).wait(timeout=5)
    assert not thread.is_alive()
    assert failures == []


def test_async_worker_with_hard_delete_attributes_starts_on_protocol_2_server() -> None:
    asyncio.run(_test_async_worker_starts_on_protocol_2_server())


async def _test_async_worker_starts_on_protocol_2_server() -> None:
    worker_port = _available_port()
    service = SyncService(worker_port, server_info=PROTOCOL_2_SERVER_INFO)
    flow_server, flow_port = _start_flow_server(service)
    worker = AsyncWorker(
        Registry((HardDeleteFlow(),), allow_async_handlers=True),
        MemoryBlobCache(),
        WorkerOptions(
            bind_address=f"127.0.0.1:{worker_port}",
            server_address=f"127.0.0.1:{flow_port}",
        ),
    )
    worker_task = asyncio.create_task(worker.start())
    try:
        await asyncio.to_thread(_await_listening, worker_port)
        assert service.calls == ["GetServerInfo", "SyncAttributeIndexes"]
        assert worker._server_protocol.version == 2
    finally:
        await worker.stop()
        await asyncio.wait_for(worker_task, timeout=5)
        flow_server.stop(grace=None).wait(timeout=5)


def test_async_worker_synchronizes_indexes_before_listening() -> None:
    asyncio.run(_test_async_worker_synchronizes_indexes_before_listening())


async def _test_async_worker_synchronizes_indexes_before_listening() -> None:
    worker_port = _available_port()
    service = SyncService(worker_port)
    flow_server, flow_port = _start_flow_server(service)
    worker = AsyncWorker(
        Registry((IndexedFlow(),), allow_async_handlers=True),
        MemoryBlobCache(),
        WorkerOptions(
            bind_address=f"127.0.0.1:{worker_port}",
            server_address=f"127.0.0.1:{flow_port}",
        ),
    )
    worker_task = asyncio.create_task(worker.start())
    try:
        called = await asyncio.to_thread(service.called.wait, 5)
        assert called
        assert service.listening_during_sync is False
        await asyncio.to_thread(_await_listening, worker_port)
    finally:
        await worker.stop()
        await asyncio.wait_for(worker_task, timeout=5)
        flow_server.stop(grace=None).wait(timeout=5)


def _start_flow_server(service: SyncService) -> tuple[grpc.Server, int]:
    server = grpc.server(ThreadPoolExecutor(max_workers=2))
    dex_pb2_grpc.add_FlowServiceServicer_to_server(  # type: ignore[no-untyped-call]
        service,
        server,
    )
    port = server.add_insecure_port("127.0.0.1:0")
    if port == 0:
        raise RuntimeError("cannot bind test FlowService")
    server.start()
    return server, port


def _available_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _can_connect(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.05):
            return True
    except OSError:
        return False


def _await_listening(port: int) -> None:
    deadline = monotonic() + 5
    ready = threading.Event()
    while monotonic() < deadline:
        if _can_connect(port):
            return
        ready.wait(0.01)
    raise AssertionError("Worker port did not open")
