# Copyright (c) 2026 Super Durable, Inc.
#
# Licensed under the Sustainable Use License 1.0.
# You may not use this file except in compliance with the License.
# See the LICENSE file in the repository root.
#
# SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

from __future__ import annotations

import asyncio
import logging
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Coroutine, cast
from unittest.mock import AsyncMock, patch

import grpc
import pytest
from google.protobuf import timestamp_pb2

from dex import (
    BlobCache,
    Flow,
    FlowConfig,
    PullWorkerOptions,
    Worker,
    WorkerOptions,
    WorkerTarget,
    retry_after,
)
from dex._pull_worker import PullStream, PullWorkerRuntime, generate_app_worker_id
from dex._worker_dispatcher import WorkerDispatcher
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


def test_subflow_pull_override_maps_to_protocol() -> None:
    mapped = WorkerDispatcher._map_sub_flow_config(
        FlowConfig(worker_target=WorkerTarget(pull=True))
    )
    assert mapped.worker_target.is_pull_mode


def test_sync_stop_rejects_event_loop_thread() -> None:
    runtime = PullWorkerRuntime(
        Registry([]),
        None,
        PullWorkerOptions(app_worker_id="loop-thread-worker"),
        "localhost:8801",
        None,
    )
    runtime._loop_thread_id = threading.get_ident()
    with pytest.raises(RuntimeError, match="another thread"):
        runtime.stop_threadsafe()


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


class FakeRpcError(grpc.RpcError):
    def __init__(self, status: grpc.StatusCode) -> None:
        self.status = status

    def code(self) -> grpc.StatusCode:
        return self.status


def test_unimplemented_logs_once_and_retries_after_one_minute(
    caplog: pytest.LogCaptureFixture,
) -> None:
    asyncio.run(_unimplemented_logs_once_and_retries_after_one_minute(caplog))


async def _unimplemented_logs_once_and_retries_after_one_minute(
    caplog: pytest.LogCaptureFixture,
) -> None:
    runtime = PullWorkerRuntime(
        Registry([]),
        None,
        PullWorkerOptions(app_worker_id="old-server-worker"),
        "localhost:8801",
        None,
    )
    timeouts: list[float] = []

    async def capture_wait(
        awaitable: Coroutine[Any, Any, bool], *, timeout: float
    ) -> bool:
        awaitable.close()
        timeouts.append(timeout)
        assert runtime._stopping is not None
        runtime._stopping.set()
        raise asyncio.TimeoutError

    with caplog.at_level(logging.ERROR), patch(
        "dex._pull_worker.asyncio.wait_for", new=capture_wait
    ), patch("dex._pull_worker.random.uniform", return_value=0):
        for _ in range(2):
            runtime._stopping = asyncio.Event()
            stream = PullStream(runtime, None, "old-server-worker")
            with patch.object(stream, "_poll", new_callable=AsyncMock) as poll_mock:
                poll_mock.side_effect = FakeRpcError(grpc.StatusCode.UNIMPLEMENTED)
                await stream.run()
    assert timeouts == [60, 60]
    assert (
        sum(
            "unsupported by this Dex server" in record.message
            for record in caplog.records
        )
        == 1
    )


def test_transient_reconnect_uses_exponential_backoff() -> None:
    asyncio.run(_transient_reconnect_uses_exponential_backoff())


async def _transient_reconnect_uses_exponential_backoff() -> None:
    runtime = PullWorkerRuntime(
        Registry([]),
        None,
        PullWorkerOptions(app_worker_id="backoff-worker"),
        "localhost:8801",
        None,
    )
    runtime._stopping = asyncio.Event()
    stream = PullStream(runtime, None, "backoff-worker")
    timeouts: list[float] = []

    async def capture_wait(
        awaitable: Coroutine[Any, Any, bool], *, timeout: float
    ) -> bool:
        awaitable.close()
        timeouts.append(timeout)
        if len(timeouts) == 2:
            assert runtime._stopping is not None
            runtime._stopping.set()
        raise asyncio.TimeoutError

    with patch("dex._pull_worker.asyncio.wait_for", new=capture_wait), patch(
        "dex._pull_worker.random.uniform", return_value=0
    ), patch.object(stream, "_poll", new_callable=AsyncMock) as poll_mock:
        poll_mock.side_effect = FakeRpcError(grpc.StatusCode.UNAVAILABLE)
        await stream.run()
    assert timeouts == [0.1, 0.2]


class QueueCall:
    def __init__(self, frames: list[pb.PollWorkerTaskResponse]) -> None:
        self.frames: asyncio.Queue[Any] = asyncio.Queue()
        for frame in frames:
            self.frames.put_nowait(frame)
        self.requests: list[pb.PollWorkerTaskRequest] = []
        self.is_closed = False
        self.is_half_closed = False

    async def write(self, request: pb.PollWorkerTaskRequest) -> None:
        self.requests.append(request)

    async def read(self) -> Any:
        return await self.frames.get()

    async def done_writing(self) -> None:
        self.is_half_closed = True
        self.frames.put_nowait(getattr(grpc.aio, "EOF"))

    def cancel(self) -> None:
        self.is_closed = True
        self.frames.put_nowait(getattr(grpc.aio, "EOF"))


def test_busy_reconnect_drains_before_closing_stream() -> None:
    asyncio.run(_busy_reconnect_drains_before_closing_stream())


async def _busy_reconnect_drains_before_closing_stream() -> None:
    dispatcher = BlockingDispatcher()
    task = pb.PollWorkerTask(task_id="busy", rpc=pb.InvokeWorkerRPCRequest())
    call = QueueCall(
        [
            pb.PollWorkerTaskResponse(registered=pb.PollWorkerTaskRegistered()),
            pb.PollWorkerTaskResponse(task=task),
            pb.PollWorkerTaskResponse(
                reconnect=pb.PollWorkerTaskReconnect(reason="rebalance")
            ),
        ]
    )
    runtime = PullWorkerRuntime(
        Registry([]),
        dispatcher,
        PullWorkerOptions(app_worker_id="busy-reconnect-worker"),
        "localhost:8801",
        None,
    )
    runtime._stopping = asyncio.Event()
    stream = PullStream(
        runtime, FakeStub(cast(FakeCall, call)), "busy-reconnect-worker"
    )
    drained = asyncio.Event()
    original_drain = stream.drain

    def mark_draining() -> None:
        original_drain()
        drained.set()

    with patch.object(stream, "drain", new=mark_draining):
        polling = asyncio.create_task(stream._poll())
        await asyncio.wait_for(dispatcher.started.wait(), timeout=1)
        await asyncio.wait_for(drained.wait(), timeout=1)
        assert not polling.done()
        assert not call.is_half_closed
        assert not dispatcher.canceled.is_set()
        dispatcher.gate.set()
        assert await asyncio.wait_for(polling, timeout=1)
    assert call.is_half_closed
    assert call.is_closed
    assert [request.WhichOneof("frame") for request in call.requests] == [
        "register",
        "task_frame",
    ]


class CancelCall(QueueCall):
    def __init__(
        self, dispatcher: BlockingDispatcher, frames: list[pb.PollWorkerTaskResponse]
    ) -> None:
        super().__init__(frames)
        self.dispatcher = dispatcher

    async def read(self) -> Any:
        frame = await super().read()
        if (
            isinstance(frame, pb.PollWorkerTaskResponse)
            and frame.WhichOneof("frame") == "cancel_task"
        ):
            await self.dispatcher.started.wait()
        return frame

    async def write(self, request: pb.PollWorkerTaskRequest) -> None:
        await super().write(request)
        if (
            request.WhichOneof("frame") == "task_frame"
            and request.task_frame.WhichOneof("output") == "worker_error"
        ):
            self.frames.put_nowait(getattr(grpc.aio, "EOF"))


def test_cancel_task_frame_cancels_handler() -> None:
    asyncio.run(_cancel_task_frame_cancels_handler())


async def _cancel_task_frame_cancels_handler() -> None:
    dispatcher = BlockingDispatcher()
    call = CancelCall(
        dispatcher,
        [
            pb.PollWorkerTaskResponse(registered=pb.PollWorkerTaskRegistered()),
            pb.PollWorkerTaskResponse(
                task=pb.PollWorkerTask(
                    task_id="cancel-me", rpc=pb.InvokeWorkerRPCRequest()
                )
            ),
            pb.PollWorkerTaskResponse(
                cancel_task=pb.PollWorkerTaskCancelTask(task_id="cancel-me")
            ),
        ],
    )
    runtime = PullWorkerRuntime(
        Registry([]),
        dispatcher,
        PullWorkerOptions(app_worker_id="cancel-worker"),
        "localhost:8801",
        None,
    )
    runtime._stopping = asyncio.Event()
    stream = PullStream(runtime, FakeStub(cast(FakeCall, call)), "cancel-worker")
    await asyncio.wait_for(stream._poll(), timeout=1)
    assert dispatcher.canceled.is_set()
    errors = [
        request.task_frame.worker_error
        for request in call.requests
        if request.WhichOneof("frame") == "task_frame"
    ]
    assert errors[0].grpc_status == grpc.StatusCode.CANCELLED.value[0]


def test_task_deadline_cancels_handler_and_clears_timer() -> None:
    asyncio.run(_task_deadline_cancels_handler_and_clears_timer())


async def _task_deadline_cancels_handler_and_clears_timer() -> None:
    dispatcher = BlockingDispatcher()
    runtime = PullWorkerRuntime(
        Registry([]),
        dispatcher,
        PullWorkerOptions(app_worker_id="deadline-worker"),
        "localhost:8801",
        None,
    )
    runtime._stopping = asyncio.Event()
    stream = PullStream(runtime, None, "deadline-worker")
    deadline = timestamp_pb2.Timestamp()
    deadline.FromDatetime(datetime.now(timezone.utc) + timedelta(milliseconds=20))
    with patch.object(stream, "_send", new_callable=AsyncMock):
        await stream._start_task(
            pb.PollWorkerTask(
                task_id="deadline", deadline=deadline, rpc=pb.InvokeWorkerRPCRequest()
            )
        )
        await asyncio.wait_for(dispatcher.started.wait(), timeout=1)
        await asyncio.wait_for(dispatcher.canceled.wait(), timeout=1)
        await stream._finish_handler()
    assert stream._deadline_handle is None
    assert runtime._busy_tasks == 0


class SyncCancelDispatcher:
    def __init__(self) -> None:
        self.started = threading.Event()
        self.gate = threading.Event()

    def invoke_rpc(
        self, request: pb.InvokeWorkerRPCRequest, is_active: Callable[[], bool]
    ) -> pb.InvokeWorkerRPCResponse:
        self.started.set()
        while is_active():
            self.gate.wait(timeout=0.01)
        raise RuntimeError("Worker task canceled")


def test_sync_handler_cancellation_reports_cancelled() -> None:
    asyncio.run(_sync_handler_cancellation_reports_cancelled())


async def _sync_handler_cancellation_reports_cancelled() -> None:
    dispatcher = SyncCancelDispatcher()
    with ThreadPoolExecutor(max_workers=1) as executor:
        runtime = PullWorkerRuntime(
            Registry([]),
            dispatcher,
            PullWorkerOptions(app_worker_id="sync-cancel-worker"),
            "localhost:8801",
            executor,
        )
        runtime._stopping = asyncio.Event()
        stream = PullStream(runtime, None, "sync-cancel-worker")
        with patch.object(stream, "_send", new_callable=AsyncMock) as send_mock:
            await stream._start_task(
                pb.PollWorkerTask(task_id="sync", rpc=pb.InvokeWorkerRPCRequest())
            )
            assert await asyncio.to_thread(dispatcher.started.wait, 1)
            stream.cancel_handler()
            assert stream.handler_task is not None
            await asyncio.wait_for(stream.handler_task, timeout=1)
            await stream._finish_handler()
        errors = [
            request.args[0].task_frame.worker_error
            for request in send_mock.await_args_list
            if request.args[0].WhichOneof("frame") == "task_frame"
        ]
        assert errors[0].grpc_status == grpc.StatusCode.CANCELLED.value[0]


def test_stop_drains_handler_before_timeout() -> None:
    asyncio.run(_stop_drains_handler_before_timeout())


async def _stop_drains_handler_before_timeout() -> None:
    dispatcher = BlockingDispatcher()
    runtime = PullWorkerRuntime(
        Registry([]),
        dispatcher,
        PullWorkerOptions(app_worker_id="drain-worker"),
        "localhost:8801",
        None,
    )
    runtime._stopping = asyncio.Event()
    stream = PullStream(runtime, None, "drain-worker")
    stream.run_task = asyncio.create_task(asyncio.sleep(0))
    runtime._streams.add(stream)
    with patch.object(stream, "_send", new_callable=AsyncMock):
        await stream._start_task(
            pb.PollWorkerTask(task_id="drain", rpc=pb.InvokeWorkerRPCRequest())
        )
        await asyncio.wait_for(dispatcher.started.wait(), timeout=1)
        stopping = asyncio.create_task(runtime.stop())
        await asyncio.wait_for(runtime._stopping.wait(), timeout=1)
        assert not stopping.done()
        dispatcher.gate.set()
        await asyncio.wait_for(stopping, timeout=1)
        await stream._finish_handler()
    assert not dispatcher.canceled.is_set()
