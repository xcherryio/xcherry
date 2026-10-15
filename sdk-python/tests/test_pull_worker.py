# Copyright (c) 2026 Super Durable, Inc.
#
# Licensed under the Sustainable Use License 1.0.
# You may not use this file except in compliance with the License.
# See the LICENSE file in the repository root.
#
# SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

from __future__ import annotations

import asyncio
import re
from datetime import timedelta
from typing import cast
from unittest.mock import AsyncMock, patch

import grpc
import pytest

from dex import (
    BlobCache,
    Flow,
    PullWorkerOptions,
    Worker,
    WorkerOptions,
    WorkerTarget,
    retry_after,
)
from dex._pull_worker import PullStream, PullWorkerRuntime, generate_app_worker_id
from dex.dexpb import dex_pb2 as pb
from dex.flow import Registry


def test_pull_options_and_targets_validate_conflicts() -> None:
    assert WorkerTarget(pull=True).address == ""
    for target in (
        {"pull": True, "address": "localhost:8803"},
        {"pull": True, "headless": True},
    ):
        with pytest.raises(ValueError, match="pull mode cannot be combined"):
            WorkerTarget(**target)
    with pytest.raises(ValueError, match="bind_address"):
        WorkerOptions(bind_address="localhost:8803", pull=PullWorkerOptions())
    with pytest.raises(ValueError, match="bind_address"):
        WorkerOptions(bind_address=":8803", pull=PullWorkerOptions())
    with pytest.raises(ValueError, match="worker_target"):
        WorkerOptions(
            worker_target=WorkerTarget("localhost:8803"), pull=PullWorkerOptions()
        )
    with pytest.raises(ValueError, match="requires"):
        WorkerOptions(worker_target=WorkerTarget(pull=True))
    for option in (
        {"max_concurrent_tasks": -1},
        {"idle_streams_per_queue": -1},
        {"stop_drain_timeout": timedelta(seconds=-1)},
        {"app_worker_id": "bad/id"},
        {"app_worker_id": "x" * 64},
        {"app_worker_id": "é"},
    ):
        with pytest.raises(ValueError):
            PullWorkerOptions(**option)


def test_generated_app_worker_id_uses_identity_and_random_suffix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DEX_WORKER_INSTANCE", "Pull.Worker_One")
    first = generate_app_worker_id()
    second = generate_app_worker_id()
    assert re.fullmatch(r"pull-worker-one-[a-z2-7]{8}", first)
    assert first != second
    assert len(first) <= 63


class ConflictingFlow(Flow[None]):
    def get_flow_type(self) -> str:
        return "conflicting-worker"


def test_app_worker_id_cannot_equal_flow_type() -> None:
    with pytest.raises(ValueError, match="conflicts with Flow type"):
        Worker(
            Registry([ConflictingFlow()]),
            cast(BlobCache, object()),
            WorkerOptions(pull=PullWorkerOptions(app_worker_id="conflicting-worker")),
        )


class BlockingDispatcher:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.gate = asyncio.Event()
        self.canceled = asyncio.Event()

    async def invoke_rpc(
        self, request: pb.InvokeWorkerRPCRequest, is_active: object
    ) -> pb.InvokeWorkerRPCResponse:
        self.started.set()
        try:
            await self.gate.wait()
        except asyncio.CancelledError:
            self.canceled.set()
            raise
        return pb.InvokeWorkerRPCResponse()


def test_capacity_reject_and_cancel() -> None:
    asyncio.run(_capacity_reject_and_cancel())


async def _capacity_reject_and_cancel() -> None:
    dispatcher = BlockingDispatcher()
    runtime = PullWorkerRuntime(
        Registry([]),
        dispatcher,
        PullWorkerOptions(app_worker_id="capacity-worker", max_concurrent_tasks=1),
        "localhost:8801",
        None,
    )
    runtime._stopping = asyncio.Event()
    stream = PullStream(runtime, None, "capacity-worker")
    with patch.object(stream, "_send", new_callable=AsyncMock) as send_mock:
        await stream._start_task(
            pb.PollWorkerTask(task_id="first", rpc=pb.InvokeWorkerRPCRequest())
        )
        await asyncio.wait_for(dispatcher.started.wait(), timeout=1)
        await stream._start_task(
            pb.PollWorkerTask(task_id="second", rpc=pb.InvokeWorkerRPCRequest())
        )
        assert send_mock.await_args is not None
        rejected = send_mock.await_args.args[0].reject
        assert rejected.task_id == "second"
        assert rejected.reason == pb.POLL_WORKER_TASK_REJECT_REASON_NO_CAPACITY
        stream.cancel_handler()
        await asyncio.wait_for(dispatcher.canceled.wait(), timeout=1)
        await stream._finish_handler()
    assert runtime._busy_tasks == 0


class FakeCall:
    def __init__(self, frames: list[pb.PollWorkerTaskResponse]) -> None:
        self.frames = iter(frames)
        self.requests: list[pb.PollWorkerTaskRequest] = []
        self.is_canceled = False

    async def write(self, request: pb.PollWorkerTaskRequest) -> None:
        self.requests.append(request)

    async def read(self) -> pb.PollWorkerTaskResponse:
        return next(self.frames)

    def cancel(self) -> None:
        self.is_canceled = True


class FakeStub:
    def __init__(self, call: FakeCall) -> None:
        self.call = call

    def PollWorkerTask(self) -> FakeCall:
        return self.call


def test_register_pong_and_idle_reconnect() -> None:
    asyncio.run(_register_pong_and_idle_reconnect())


async def _register_pong_and_idle_reconnect() -> None:
    call = FakeCall(
        [
            pb.PollWorkerTaskResponse(registered=pb.PollWorkerTaskRegistered()),
            pb.PollWorkerTaskResponse(ping=pb.PollWorkerTaskPing()),
            pb.PollWorkerTaskResponse(
                reconnect=pb.PollWorkerTaskReconnect(reason="rebalance")
            ),
        ]
    )
    runtime = PullWorkerRuntime(
        Registry([]),
        None,
        PullWorkerOptions(app_worker_id="reconnect-worker"),
        "localhost:8801",
        None,
    )
    runtime._stopping = asyncio.Event()
    stream = PullStream(runtime, FakeStub(call), "reconnect-worker")
    assert await stream._poll()
    assert [request.WhichOneof("frame") for request in call.requests] == [
        "register",
        "pong",
    ]
    assert stream._immediate_reconnect
    assert call.is_canceled


def test_worker_error_frame_preserves_push_error_details() -> None:
    asyncio.run(_worker_error_frame_preserves_push_error_details())


async def _worker_error_frame_preserves_push_error_details() -> None:
    runtime = PullWorkerRuntime(
        Registry([]),
        None,
        PullWorkerOptions(app_worker_id="error-worker"),
        "localhost:8801",
        None,
    )
    stream = PullStream(runtime, None, "error-worker")
    with patch.object(stream, "_send", new_callable=AsyncMock) as send_mock:
        await stream._send_error("task", retry_after(7, RuntimeError("broken")))
    assert send_mock.await_args is not None
    frame = send_mock.await_args.args[0].task_frame
    assert frame.task_id == "task"
    assert frame.worker_error.detail == "broken"
    assert frame.worker_error.error_type.endswith("RuntimeError")
    assert frame.worker_error.grpc_status == grpc.StatusCode.UNKNOWN.value[0]
    assert frame.worker_error.retry_after_seconds == 7


def test_stop_forces_async_handler_after_drain_timeout() -> None:
    asyncio.run(_stop_forces_async_handler_after_drain_timeout())


async def _stop_forces_async_handler_after_drain_timeout() -> None:
    dispatcher = BlockingDispatcher()
    runtime = PullWorkerRuntime(
        Registry([]),
        dispatcher,
        PullWorkerOptions(
            app_worker_id="stop-worker", stop_drain_timeout=timedelta(milliseconds=10)
        ),
        "localhost:8801",
        None,
    )
    runtime._stopping = asyncio.Event()
    stream = PullStream(runtime, None, "stop-worker")
    stream.run_task = asyncio.create_task(asyncio.sleep(0))
    runtime._streams.add(stream)
    with patch.object(stream, "_send", new_callable=AsyncMock):
        await stream._start_task(
            pb.PollWorkerTask(task_id="task", rpc=pb.InvokeWorkerRPCRequest())
        )
        await asyncio.wait_for(dispatcher.started.wait(), timeout=1)
        await runtime.stop()
    assert dispatcher.canceled.is_set()
