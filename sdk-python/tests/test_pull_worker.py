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
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Coroutine, cast
from unittest.mock import AsyncMock, patch

import grpc
import pytest
from google.protobuf import timestamp_pb2

from dex import (
    AsyncWorker,
    BlobCache,
    PullRegistrationError,
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
from dex.dexpb import dex_pb2_grpc
from dex.flow import Registry


_UNKNOWN_TASK_REASON = PullWorkerRuntime.unknown_task_reason


@pytest.fixture(autouse=True)
def accept_mock_worker_tasks(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        PullWorkerRuntime, "unknown_task_reason", lambda self, task: None
    )


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


def test_sync_close_after_start_returns_on_same_thread() -> None:
    runtime = PullWorkerRuntime(
        Registry([]),
        None,
        PullWorkerOptions(app_worker_id="closed-loop-worker"),
        "localhost:8801",
        None,
    )
    runtime._loop_thread_id = threading.get_ident()
    context_worker = object.__new__(Worker)
    context_worker._lock = threading.Lock()
    context_worker._state = "stopped"
    context_worker._pull = runtime
    context_worker._credential_runner = None
    with context_worker:
        pass
    assert context_worker._state == "closed"

    finally_worker = object.__new__(Worker)
    finally_worker._lock = threading.Lock()
    finally_worker._state = "stopped"
    finally_worker._pull = runtime
    finally_worker._credential_runner = None
    try:
        pass
    finally:
        finally_worker.close()
    assert finally_worker._state == "closed"


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


class LargeResultDispatcher(BlockingDispatcher):
    def __init__(self) -> None:
        super().__init__()
        self.second_started = asyncio.Event()
        self.invocation_count = 0

    async def invoke_rpc(
        self, request: pb.InvokeWorkerRPCRequest, is_active: object
    ) -> pb.InvokeWorkerRPCResponse:
        self.invocation_count += 1
        if self.invocation_count == 1:
            self.started.set()
        else:
            self.second_started.set()
        await self.gate.wait()
        return pb.InvokeWorkerRPCResponse(output=pb.Value(string_value="x" * 3_000_000))


class SyncLargeResultDispatcher:
    def __init__(self) -> None:
        self.started = threading.Event()
        self.gate = threading.Event()

    def invoke_rpc(
        self, request: pb.InvokeWorkerRPCRequest, is_active: object
    ) -> pb.InvokeWorkerRPCResponse:
        self.started.set()
        if not self.gate.wait(timeout=10):
            raise TimeoutError("sync Worker task gate did not open")
        return pb.InvokeWorkerRPCResponse(output=pb.Value(string_value="x" * 3_000_000))


class DelayedReadService(dex_pb2_grpc.WorkerTaskServiceServicer):
    def __init__(
        self, is_reconnecting: bool, should_assign_replacement: bool = False
    ) -> None:
        self.is_reconnecting = is_reconnecting
        self.should_assign_replacement = should_assign_replacement
        self.ready = threading.Event()
        self.read_allowed = threading.Event()
        self.finished = threading.Event()
        self.second_poll_opened = threading.Event()
        self.third_poll_opened = threading.Event()
        self.assign_second_task = threading.Event()
        self.final_frame: pb.PollWorkerTaskFrame | None = None
        self.read_error: grpc.RpcError | None = None
        self._poll_count = 0
        self._lock = threading.Lock()

    def PollWorkerTask(  # noqa: N802
        self,
        request_iterator: Iterator[pb.PollWorkerTaskRequest],
        context: grpc.ServicerContext,
    ) -> Iterator[pb.PollWorkerTaskResponse]:
        registration = next(request_iterator)
        assert registration.WhichOneof("frame") == "register"
        yield pb.PollWorkerTaskResponse(registered=pb.PollWorkerTaskRegistered())
        with self._lock:
            self._poll_count += 1
            poll_count = self._poll_count
        if poll_count >= 3:
            self.third_poll_opened.set()
        if poll_count == 2:
            self.second_poll_opened.set()
            if self.should_assign_replacement:
                assert self.assign_second_task.wait(timeout=10)
                yield pb.PollWorkerTaskResponse(
                    task=pb.PollWorkerTask(
                        task_id="replacement-task", rpc=pb.InvokeWorkerRPCRequest()
                    )
                )
        if poll_count != 1:
            for _ in request_iterator:
                pass
            return
        yield pb.PollWorkerTaskResponse(
            task=pb.PollWorkerTask(
                task_id="delayed-result", rpc=pb.InvokeWorkerRPCRequest()
            )
        )
        if self.is_reconnecting:
            yield pb.PollWorkerTaskResponse(
                reconnect=pb.PollWorkerTaskReconnect(reason="move")
            )
        yield pb.PollWorkerTaskResponse(ping=pb.PollWorkerTaskPing())
        pong = next(request_iterator)
        assert pong.WhichOneof("frame") == "pong"
        self.ready.set()
        assert self.read_allowed.wait(timeout=10)
        try:
            for request in request_iterator:
                if request.WhichOneof("frame") == "task_frame":
                    self.final_frame = request.task_frame
        except grpc.RpcError as failure:
            self.read_error = failure
        finally:
            self.finished.set()


@pytest.mark.parametrize("shutdown", ["stop", "reconnect", "reconnect_then_stop"])
@pytest.mark.parametrize("is_sync", [False, True])
def test_large_final_result_survives_delayed_grpc_read(
    shutdown: str, is_sync: bool
) -> None:
    asyncio.run(_large_final_result_survives_delayed_grpc_read(shutdown, is_sync))


async def _large_final_result_survives_delayed_grpc_read(
    shutdown: str, is_sync: bool
) -> None:
    service = DelayedReadService(is_reconnecting=shutdown != "stop")
    executor = ThreadPoolExecutor(max_workers=4)
    server = grpc.server(
        executor,
        options=(
            ("grpc.http2.initial_window_size", 65_536),
            ("grpc.http2.initial_connection_window_size", 65_536),
        ),
    )
    dex_pb2_grpc.add_WorkerTaskServiceServicer_to_server(  # type: ignore[no-untyped-call]
        service, server
    )
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    dispatcher = SyncLargeResultDispatcher() if is_sync else LargeResultDispatcher()
    handler_executor = ThreadPoolExecutor(max_workers=2) if is_sync else None
    runtime = PullWorkerRuntime(
        Registry([]),
        dispatcher,
        PullWorkerOptions(
            app_worker_id="delayed-result-worker",
            stop_drain_timeout=timedelta(
                seconds=4 if shutdown == "reconnect_then_stop" else 5
            ),
        ),
        f"127.0.0.1:{port}",
        handler_executor,
    )
    running = asyncio.create_task(runtime.run())
    stopping: asyncio.Task[None] | None = None
    try:
        assert await asyncio.wait_for(
            asyncio.to_thread(service.ready.wait, 5), timeout=6
        )
        if isinstance(dispatcher, SyncLargeResultDispatcher):
            assert await asyncio.wait_for(
                asyncio.to_thread(dispatcher.started.wait, 5), timeout=6
            )
        else:
            await asyncio.wait_for(dispatcher.started.wait(), timeout=5)
        if shutdown == "stop":
            stopping = asyncio.create_task(runtime.stop())
            assert runtime._stopping is not None
            await asyncio.wait_for(runtime._stopping.wait(), timeout=5)
        dispatcher.gate.set()
        if shutdown == "reconnect_then_stop":
            await asyncio.sleep(2)
            stopping = asyncio.create_task(runtime.stop())
        await asyncio.sleep(2.5 if shutdown == "reconnect_then_stop" else 3.1)
        service.read_allowed.set()
        assert await asyncio.wait_for(
            asyncio.to_thread(service.finished.wait, 6), timeout=7
        )
        assert service.read_error is None
        assert service.final_frame is not None
        assert service.final_frame.task_id == "delayed-result"
        assert len(service.final_frame.rpc.output.string_value) == 3_000_000
        if stopping is None:
            stopping = asyncio.create_task(runtime.stop())
        await asyncio.wait_for(stopping, timeout=6)
        await asyncio.wait_for(running, timeout=6)
    finally:
        service.read_allowed.set()
        dispatcher.gate.set()
        await asyncio.wait_for(runtime.stop(), timeout=6)
        await asyncio.wait_for(running, timeout=6)
        assert server.stop(grace=0).wait(timeout=3)
        if handler_executor is not None:
            handler_executor.shutdown(wait=True)
        executor.shutdown(wait=True)


def test_half_closing_busy_reconnect_keeps_a_pollable_idle_stream() -> None:
    asyncio.run(_half_closing_busy_reconnect_keeps_a_pollable_idle_stream())


async def _half_closing_busy_reconnect_keeps_a_pollable_idle_stream() -> None:
    service = DelayedReadService(is_reconnecting=True, should_assign_replacement=True)
    executor = ThreadPoolExecutor(max_workers=4)
    server = grpc.server(executor)
    dex_pb2_grpc.add_WorkerTaskServiceServicer_to_server(  # type: ignore[no-untyped-call]
        service, server
    )
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    dispatcher = LargeResultDispatcher()
    runtime = PullWorkerRuntime(
        Registry([]),
        dispatcher,
        PullWorkerOptions(
            app_worker_id="half-closing-reconnect-worker",
            stop_drain_timeout=timedelta(seconds=8),
        ),
        f"127.0.0.1:{port}",
        None,
    )
    running = asyncio.create_task(runtime.run())
    try:
        assert await asyncio.wait_for(
            asyncio.to_thread(service.ready.wait, 5), timeout=6
        )
        await asyncio.wait_for(dispatcher.started.wait(), timeout=5)
        assert await asyncio.wait_for(
            asyncio.to_thread(service.second_poll_opened.wait, 5), timeout=6
        )
        dispatcher.gate.set()

        async def wait_for_half_close() -> None:
            while not any(stream.is_half_closing for stream in runtime._streams):
                await asyncio.sleep(0.005)

        await asyncio.wait_for(wait_for_half_close(), timeout=5)
        dispatcher.gate.clear()
        service.assign_second_task.set()
        await asyncio.wait_for(dispatcher.second_started.wait(), timeout=5)
        assert await asyncio.wait_for(
            asyncio.to_thread(service.third_poll_opened.wait, 5), timeout=6
        )
        assert not service.read_allowed.is_set()
        dispatcher.gate.set()
        service.read_allowed.set()
        assert await asyncio.wait_for(
            asyncio.to_thread(service.finished.wait, 5), timeout=6
        )
        assert service.final_frame is not None
        await asyncio.wait_for(runtime.stop(), timeout=8)
        await asyncio.wait_for(running, timeout=8)
    finally:
        service.assign_second_task.set()
        service.read_allowed.set()
        dispatcher.gate.set()
        await asyncio.wait_for(runtime.stop(), timeout=8)
        await asyncio.wait_for(running, timeout=8)
        assert server.stop(grace=0).wait(timeout=3)
        executor.shutdown(wait=True)


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
    assert stream._is_reconnecting
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
    call = DelayedEofCall(
        [
            pb.PollWorkerTaskResponse(registered=pb.PollWorkerTaskRegistered()),
            pb.PollWorkerTaskResponse(
                task=pb.PollWorkerTask(task_id="task", rpc=pb.InvokeWorkerRPCRequest())
            ),
        ]
    )
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
    stream = PullStream(runtime, FakeStub(cast(FakeCall, call)), "stop-worker")
    stream.run_task = asyncio.create_task(_poll_until_closed(stream))
    runtime._streams.add(stream)
    await asyncio.wait_for(dispatcher.started.wait(), timeout=1)
    stopping = asyncio.create_task(runtime.stop())
    await asyncio.wait_for(call.half_closed.wait(), timeout=1)
    await asyncio.wait_for(stopping, timeout=1)
    assert dispatcher.canceled.is_set()
    assert call.is_closed
    assert "read:EOF" not in call.operations
    assert call.operations.index("write:task_frame") < call.operations.index(
        "done_writing"
    )
    errors = [
        request.task_frame.worker_error
        for request in call.requests
        if request.WhichOneof("frame") == "task_frame"
    ]
    assert errors[0].grpc_status == grpc.StatusCode.CANCELLED.value[0]


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


@pytest.mark.parametrize("was_registered", [False, True])
def test_server_reconnect_waits_for_backoff_and_jitter(
    was_registered: bool,
) -> None:
    asyncio.run(_server_reconnect_waits_for_backoff_and_jitter(was_registered))


async def _server_reconnect_waits_for_backoff_and_jitter(
    was_registered: bool,
) -> None:
    runtime = PullWorkerRuntime(
        Registry([]),
        None,
        PullWorkerOptions(app_worker_id="reconnect-backoff-worker"),
        "localhost:8801",
        None,
    )
    runtime._stopping = asyncio.Event()
    stream = PullStream(runtime, None, "reconnect-backoff-worker")
    poll_count = 0
    virtual_time = 0.0
    poll_times: list[float] = []

    async def poll() -> bool:
        nonlocal poll_count
        poll_count += 1
        poll_times.append(virtual_time)
        if poll_count == 1:
            stream._is_reconnecting = True
        else:
            assert runtime._stopping is not None
            runtime._stopping.set()
        return was_registered

    async def capture_wait(
        awaitable: Coroutine[Any, Any, bool], *, timeout: float
    ) -> bool:
        nonlocal virtual_time
        awaitable.close()
        assert poll_count == 1
        virtual_time += timeout
        raise asyncio.TimeoutError

    with patch("dex._pull_worker.asyncio.wait_for", new=capture_wait), patch(
        "dex._pull_worker.random.uniform", return_value=0.025
    ) as jitter_mock, patch.object(stream, "_poll", new=poll):
        await stream.run()

    assert poll_count == 2
    assert poll_times == [0.0, 0.125]
    assert 0.1 <= poll_times[1] - poll_times[0] < 0.15
    jitter_mock.assert_called_once_with(0, 0.05)


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


class DelayedEofCall(QueueCall):
    def __init__(self, frames: list[pb.PollWorkerTaskResponse]) -> None:
        super().__init__(frames)
        self.half_closed = asyncio.Event()
        self.operations: list[str] = []

    async def write(self, request: pb.PollWorkerTaskRequest) -> None:
        await super().write(request)
        self.operations.append(f"write:{request.WhichOneof('frame')}")

    async def read(self) -> Any:
        frame = await super().read()
        if frame is getattr(grpc.aio, "EOF"):
            self.operations.append("read:EOF")
        return frame

    async def done_writing(self) -> None:
        self.is_half_closed = True
        self.operations.append("done_writing")
        self.half_closed.set()

    def release_eof(self) -> None:
        self.frames.put_nowait(getattr(grpc.aio, "EOF"))

    def cancel(self) -> None:
        self.operations.append("cancel")
        super().cancel()


async def _poll_until_closed(stream: PullStream) -> None:
    await stream._poll()


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
    assert stream._is_reconnecting
    assert [request.WhichOneof("frame") for request in call.requests] == [
        "register",
        "task_frame",
    ]


class UnknownTaskCall(QueueCall):
    def __init__(self, supports_unknown_name_rejection: bool) -> None:
        super().__init__(
            [
                pb.PollWorkerTaskResponse(
                    registered=pb.PollWorkerTaskRegistered(
                        supports_unknown_name_rejection=supports_unknown_name_rejection
                    )
                ),
                pb.PollWorkerTaskResponse(
                    task=pb.PollWorkerTask(
                        task_id="unknown-step",
                        wait_for=pb.InvokeWaitForMethodRequest(
                            flow_type="TestFlow", step_type="MissingStep"
                        ),
                    )
                ),
            ]
        )
        self._completed_tasks = 0

    async def write(self, request: pb.PollWorkerTaskRequest) -> None:
        await super().write(request)
        kind = request.WhichOneof("frame")
        if kind in ("task_frame", "reject"):
            self._completed_tasks += 1
            if self._completed_tasks == 1:
                self.frames.put_nowait(
                    pb.PollWorkerTaskResponse(
                        task=pb.PollWorkerTask(
                            task_id="unknown-rpc",
                            rpc=pb.InvokeWorkerRPCRequest(
                                flow_type="TestFlow", rpc_name="MissingRPC"
                            ),
                        )
                    )
                )
            else:
                self.frames.put_nowait(
                    pb.PollWorkerTaskResponse(ping=pb.PollWorkerTaskPing())
                )
        elif kind == "pong":
            self.frames.put_nowait(getattr(grpc.aio, "EOF"))


class UnknownTaskDispatcher:
    async def invoke_wait_for(
        self, request: pb.InvokeWaitForMethodRequest, is_active: Callable[[], bool]
    ) -> Any:
        if is_active():
            raise LookupError(f"Step {request.step_type} is not registered")
        raise RuntimeError("Worker task became inactive")
        yield None

    async def invoke_rpc(
        self, request: pb.InvokeWorkerRPCRequest, is_active: Callable[[], bool]
    ) -> pb.InvokeWorkerRPCResponse:
        if is_active():
            raise LookupError(f"RPC {request.rpc_name} is not registered")
        raise RuntimeError("Worker task became inactive")


def test_unknown_task_rejection_follows_registered_capability_on_reconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(PullWorkerRuntime, "unknown_task_reason", _UNKNOWN_TASK_REASON)
    asyncio.run(_unknown_task_rejection_follows_registered_capability_on_reconnect())


async def _unknown_task_rejection_follows_registered_capability_on_reconnect() -> None:
    runtime = PullWorkerRuntime(
        Registry([]),
        UnknownTaskDispatcher(),
        PullWorkerOptions(app_worker_id="unknown-name-worker"),
        "localhost:8801",
        None,
    )
    runtime._stopping = asyncio.Event()

    legacy_call = UnknownTaskCall(False)
    stream = PullStream(
        runtime, FakeStub(cast(FakeCall, legacy_call)), "unknown-name-worker"
    )
    assert await asyncio.wait_for(stream._poll(), timeout=1)
    legacy_frames = [
        request
        for request in legacy_call.requests
        if request.WhichOneof("frame") == "task_frame"
    ]
    assert [frame.task_frame.task_id for frame in legacy_frames] == [
        "unknown-step",
        "unknown-rpc",
    ]
    assert all(
        "not registered" in frame.task_frame.worker_error.detail
        for frame in legacy_frames
    )
    assert not any(
        request.WhichOneof("frame") == "reject" for request in legacy_call.requests
    )
    assert any(
        request.WhichOneof("frame") == "pong" for request in legacy_call.requests
    )

    capable_call = UnknownTaskCall(True)
    stream.stub = FakeStub(cast(FakeCall, capable_call))
    assert await asyncio.wait_for(stream._poll(), timeout=1)
    rejections = [
        request.reject
        for request in capable_call.requests
        if request.WhichOneof("frame") == "reject"
    ]
    assert [(reject.task_id, reject.reason) for reject in rejections] == [
        ("unknown-step", pb.POLL_WORKER_TASK_REJECT_REASON_UNKNOWN_STEP),
        ("unknown-rpc", pb.POLL_WORKER_TASK_REJECT_REASON_UNKNOWN_RPC),
    ]

    reconnected_call = UnknownTaskCall(False)
    stream.stub = FakeStub(cast(FakeCall, reconnected_call))
    assert await asyncio.wait_for(stream._poll(), timeout=1)
    reconnected_frames = [
        request
        for request in reconnected_call.requests
        if request.WhichOneof("frame") == "task_frame"
    ]
    assert [frame.task_frame.task_id for frame in reconnected_frames] == [
        "unknown-step",
        "unknown-rpc",
    ]
    assert not any(
        request.WhichOneof("frame") == "reject" for request in reconnected_call.requests
    )


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
    call = DelayedEofCall(
        [
            pb.PollWorkerTaskResponse(registered=pb.PollWorkerTaskRegistered()),
            pb.PollWorkerTaskResponse(
                task=pb.PollWorkerTask(task_id="drain", rpc=pb.InvokeWorkerRPCRequest())
            ),
        ]
    )
    runtime = PullWorkerRuntime(
        Registry([]),
        dispatcher,
        PullWorkerOptions(app_worker_id="drain-worker"),
        "localhost:8801",
        None,
    )
    runtime._stopping = asyncio.Event()
    stream = PullStream(runtime, FakeStub(cast(FakeCall, call)), "drain-worker")
    stream.run_task = asyncio.create_task(_poll_until_closed(stream))
    runtime._streams.add(stream)
    await asyncio.wait_for(dispatcher.started.wait(), timeout=1)
    stopping = asyncio.create_task(runtime.stop())
    await asyncio.wait_for(runtime._stopping.wait(), timeout=1)
    assert not stopping.done()
    dispatcher.gate.set()
    await asyncio.wait_for(call.half_closed.wait(), timeout=1)
    assert not call.is_closed
    assert not stopping.done()
    call.release_eof()
    await asyncio.wait_for(stopping, timeout=1)
    assert not dispatcher.canceled.is_set()
    assert call.operations.index("write:task_frame") < call.operations.index(
        "done_writing"
    )
    assert call.operations.index("read:EOF") < call.operations.index("cancel")


class RegistrationCall(QueueCall):
    async def read(self) -> Any:
        frame = await self.frames.get()
        if isinstance(frame, BaseException):
            raise frame
        return frame


@pytest.mark.parametrize("code", list(grpc.StatusCode))
def test_registration_permanent_status_classification(code: grpc.StatusCode) -> None:
    asyncio.run(_registration_permanent_status_classification(code))


async def _registration_permanent_status_classification(code: grpc.StatusCode) -> None:
    runtime = PullWorkerRuntime(
        Registry([]),
        None,
        PullWorkerOptions(app_worker_id="readiness-worker"),
        "unused",
        None,
    )
    runtime._stopping = asyncio.Event()
    call = RegistrationCall([])
    call.frames.put_nowait(FakeRpcError(code))
    stream = PullStream(runtime, FakeStub(cast(FakeCall, call)), "readiness-worker")
    task = asyncio.create_task(stream.run())
    is_permanent = code in {
        grpc.StatusCode.INVALID_ARGUMENT,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
        grpc.StatusCode.UNIMPLEMENTED,
        grpc.StatusCode.PERMISSION_DENIED,
        grpc.StatusCode.UNAUTHENTICATED,
    }
    try:
        if is_permanent:
            with pytest.raises(PullRegistrationError) as rejected:
                await runtime.registration.wait_async(1)
            assert rejected.value.queue == "readiness-worker"
            assert cast(FakeRpcError, rejected.value.error).code() == code
            assert not runtime.registration.snapshot().is_ready
        else:
            with pytest.raises(TimeoutError):
                await runtime.registration.wait_async(0.02)
    finally:
        runtime._stopping.set()
        await asyncio.wait_for(task, 1)


def test_registration_readiness_latches_and_surplus_rejection_is_not_fatal() -> None:
    asyncio.run(_registration_readiness_latches_and_surplus_rejection_is_not_fatal())


async def _registration_readiness_latches_and_surplus_rejection_is_not_fatal() -> None:
    runtime = PullWorkerRuntime(
        Registry([ConflictingFlow()]),
        None,
        PullWorkerOptions(
            app_worker_id="readiness-worker", stop_on_permanent_stream_rejection=True
        ),
        "unused",
        None,
    )
    runtime._stopping = asyncio.Event()
    streams: list[PullStream] = []
    calls: list[RegistrationCall] = []
    for queue in ("readiness-worker", "conflicting-worker", "conflicting-worker"):
        call = RegistrationCall(
            [pb.PollWorkerTaskResponse(registered=pb.PollWorkerTaskRegistered())]
        )
        stream = PullStream(runtime, FakeStub(cast(FakeCall, call)), queue)
        stream.run_task = asyncio.create_task(stream.run())
        streams.append(stream)
        calls.append(call)
    try:
        await runtime.registration.wait_async(1)
        snapshot = runtime.registration.snapshot()
        assert snapshot.is_ready
        assert len(snapshot.queues) == 2
        calls[1].frames.put_nowait(FakeRpcError(grpc.StatusCode.PERMISSION_DENIED))
        while runtime.registration.snapshot().queues[1].last_permanent_error is None:
            await asyncio.sleep(0)
        assert runtime.registration.snapshot().is_ready
        assert runtime._permanent_error is None
        calls[0].frames.put_nowait(FakeRpcError(grpc.StatusCode.UNAVAILABLE))
        while runtime.registration.snapshot().queues[0].has_registered_stream:
            await asyncio.sleep(0)
        assert runtime.registration.snapshot().is_ready
        await runtime.registration.wait_async(0)
        assert snapshot.queues[0].has_registered_stream
        runtime.registration.record_stream_result(
            "conflicting-worker", True, FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT)
        )
        assert not runtime.registration.snapshot().is_ready
        await runtime.registration.wait_async(0)
        runtime.registration.record_registered_stream("conflicting-worker")
        assert runtime.registration.snapshot().is_ready
        assert runtime.registration.snapshot().queues[1].last_permanent_error is None
    finally:
        runtime._stopping.set()
        for call in calls:
            call.cancel()
        await asyncio.gather(*(stream.run_task for stream in streams))
    assert runtime.registration.snapshot().is_ready


@pytest.mark.parametrize("has_registered", [False, True])
def test_fatal_registration_drains_and_stop_stays_safe(has_registered: bool) -> None:
    asyncio.run(_fatal_registration_drains_and_stop_stays_safe(has_registered))


async def _fatal_registration_drains_and_stop_stays_safe(has_registered: bool) -> None:
    call = RegistrationCall(
        [pb.PollWorkerTaskResponse(registered=pb.PollWorkerTaskRegistered())]
        if has_registered
        else []
    )
    runtime = PullWorkerRuntime(
        Registry([]),
        None,
        PullWorkerOptions(
            app_worker_id="fatal-worker", stop_on_permanent_stream_rejection=True
        ),
        "unused",
        None,
    )
    channel = AsyncMock()
    with patch(
        "dex._pull_worker.outbound_worker_task_channel", return_value=channel
    ), patch(
        "dex._pull_worker.dex_pb2_grpc.WorkerTaskServiceStub",
        return_value=FakeStub(cast(FakeCall, call)),
    ):
        running = asyncio.create_task(runtime.run())
        if has_registered:
            await runtime.registration.wait_async(1)
        call.frames.put_nowait(FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT))
        with pytest.raises(PullRegistrationError):
            await asyncio.wait_for(running, 2)
        await runtime.stop()
        assert runtime.registration.snapshot().is_ready is False
        if has_registered:
            await runtime.registration.wait_async(0)
        channel.close.assert_awaited_once()


@pytest.mark.parametrize("is_async", [False, True])
def test_registration_waiters_receive_start_failure_and_stop(is_async: bool) -> None:
    asyncio.run(_registration_waiters_receive_start_failure_and_stop(is_async))


async def _registration_waiters_receive_start_failure_and_stop(is_async: bool) -> None:
    from test_worker_startup import MemoryBlobCache

    worker_type = AsyncWorker if is_async else Worker
    worker = worker_type(
        Registry([]), MemoryBlobCache(), WorkerOptions(pull=PullWorkerOptions())
    )
    failure = RuntimeError("startup failed")
    try:
        if isinstance(worker, AsyncWorker):
            waiting = asyncio.create_task(worker.wait_pull_registered(1))
            with patch.object(
                worker, "_start", new_callable=AsyncMock, side_effect=failure
            ):
                with pytest.raises(RuntimeError) as started:
                    await worker.start()
            with pytest.raises(RuntimeError) as waited:
                await waiting
        else:
            waiting = asyncio.create_task(
                asyncio.to_thread(worker.wait_pull_registered, 1)
            )
            with patch.object(worker, "_start", side_effect=failure):
                with pytest.raises(RuntimeError) as started:
                    worker.start()
            with pytest.raises(RuntimeError) as waited:
                await waiting
        assert started.value is failure
        assert waited.value is failure
    finally:
        if isinstance(worker, AsyncWorker):
            await worker.close()
        else:
            worker.close()
    stopped = worker_type(
        Registry([]), MemoryBlobCache(), WorkerOptions(pull=PullWorkerOptions())
    )
    if isinstance(stopped, AsyncWorker):
        await stopped.stop()
        with pytest.raises(RuntimeError, match="stopped before registration"):
            await stopped.wait_pull_registered(1)
    else:
        stopped.stop()
        with pytest.raises(RuntimeError, match="stopped before registration"):
            stopped.wait_pull_registered(1)


def test_registration_wait_cancellation_does_not_cancel_other_waiters() -> None:
    asyncio.run(_registration_wait_cancellation_does_not_cancel_other_waiters())


async def _registration_wait_cancellation_does_not_cancel_other_waiters() -> None:
    runtime = PullWorkerRuntime(
        Registry([]),
        None,
        PullWorkerOptions(app_worker_id="cancel-wait"),
        "unused",
        None,
    )
    canceled = asyncio.create_task(runtime.registration.wait_async(None))
    waiting = asyncio.create_task(runtime.registration.wait_async(1))
    await asyncio.sleep(0)
    canceled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await canceled
    runtime.registration.record_registered_stream("cancel-wait")
    await waiting


def test_readiness_requires_each_queue_and_waiters_can_time_out_independently() -> None:
    asyncio.run(_readiness_requires_each_queue_and_waiters_can_time_out_independently())


async def _readiness_requires_each_queue_and_waiters_can_time_out_independently() -> (
    None
):
    runtime = PullWorkerRuntime(
        Registry([ConflictingFlow()]),
        None,
        PullWorkerOptions(app_worker_id="all-queues"),
        "unused",
        None,
    )
    runtime._stopping = asyncio.Event()
    worker_call = RegistrationCall(
        [pb.PollWorkerTaskResponse(registered=pb.PollWorkerTaskRegistered())]
    )
    flow_call = RegistrationCall([])
    worker_stream = PullStream(
        runtime, FakeStub(cast(FakeCall, worker_call)), "all-queues"
    )
    flow_stream = PullStream(
        runtime, FakeStub(cast(FakeCall, flow_call)), "conflicting-worker"
    )
    running = [
        asyncio.create_task(stream.run()) for stream in (worker_stream, flow_stream)
    ]
    waiting = asyncio.create_task(runtime.registration.wait_async(1))
    try:
        with pytest.raises(TimeoutError):
            await runtime.registration.wait_async(0.02)
        assert runtime.registration.snapshot().queues[0].has_registered_stream
        assert not runtime.registration.snapshot().is_ready
        flow_call.frames.put_nowait(
            pb.PollWorkerTaskResponse(registered=pb.PollWorkerTaskRegistered())
        )
        await waiting
        assert runtime.registration.snapshot().is_ready
    finally:
        runtime._stopping.set()
        worker_call.cancel()
        flow_call.cancel()
        await asyncio.gather(*running)


def test_transport_creation_failure_preserves_start_error_and_allows_stop() -> None:
    asyncio.run(_transport_creation_failure_preserves_start_error_and_allows_stop())


async def _transport_creation_failure_preserves_start_error_and_allows_stop() -> None:
    from test_worker_startup import MemoryBlobCache

    worker = Worker(
        Registry([]), MemoryBlobCache(), WorkerOptions(pull=PullWorkerOptions())
    )
    assert worker._pull is not None
    failure = RuntimeError("cannot create pull channel")

    def run_pull() -> None:
        worker._state = "running"
        assert worker._pull is not None
        asyncio.run(worker._pull.run())

    try:
        with patch.object(worker, "_start", side_effect=run_pull), patch(
            "dex._pull_worker.outbound_worker_task_channel", side_effect=failure
        ):
            with pytest.raises(RuntimeError) as started:
                await asyncio.to_thread(worker.start)
        assert started.value is failure
        with pytest.raises(RuntimeError) as waited:
            worker.wait_pull_registered(1)
        assert waited.value is failure
        worker.stop()
    finally:
        worker.close()
