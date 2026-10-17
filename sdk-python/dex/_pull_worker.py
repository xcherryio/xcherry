# Copyright (c) 2026 Super Durable, Inc.
#
# Licensed under the Sustainable Use License 1.0.
# You may not use this file except in compliance with the License.
# See the LICENSE file in the repository root.
#
# SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

from __future__ import annotations

import asyncio
import base64
import logging
import os
import random
import re
import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any

import grpc

from dex._credential_transport import AsyncCredentialRunner, async_credential_metadata
from dex._grpc_errors import _worker_error_status
from dex._server_protocol import sdk_version
from dex.dexpb import dex_pb2 as pb
from dex.dexpb import dex_pb2_grpc
from dex.flow import Registry
from dex.worker_options import PullWorkerOptions

_LOGGER = logging.getLogger(__name__)
_GRPC_AIO_EOF = getattr(grpc.aio, "EOF")
_PERMANENT_CODES = {
    grpc.StatusCode.UNAUTHENTICATED,
    grpc.StatusCode.PERMISSION_DENIED,
    grpc.StatusCode.INVALID_ARGUMENT,
    grpc.StatusCode.UNIMPLEMENTED,
    grpc.StatusCode.RESOURCE_EXHAUSTED,
}


def generate_app_worker_id() -> str:
    identity = next(
        (
            value
            for value in (
                os.getenv("DEX_WORKER_INSTANCE"),
                os.getenv("POD_NAME"),
                os.getenv("HOSTNAME"),
                socket.gethostname(),
            )
            if value and value.strip()
        ),
        "worker",
    )
    cleaned = re.sub(r"[^a-z0-9-]+", "-", identity.lower())[:54].strip("-")
    suffix = base64.b32encode(os.urandom(5)).decode("ascii").rstrip("=").lower()
    return f"{cleaned or 'worker'}-{suffix}"


def outbound_worker_task_channel(server_address: str) -> grpc.aio.Channel:
    return grpc.aio.insecure_channel(server_address)


def resolve_app_worker_id(registry: Registry, options: PullWorkerOptions) -> str:
    app_worker_id = options.app_worker_id or generate_app_worker_id()
    if app_worker_id in registry._registered_flows:
        raise ValueError(f"app_worker_id conflicts with Flow type {app_worker_id!r}")
    return app_worker_id


class PullWorkerRuntime:
    def __init__(
        self,
        registry: Registry,
        dispatcher: Any,
        options: PullWorkerOptions,
        server_address: str,
        executor: ThreadPoolExecutor | None,
        app_worker_id: str | None = None,
        credential_runner: AsyncCredentialRunner | None = None,
    ) -> None:
        self._registry = registry
        self.app_worker_id = app_worker_id or resolve_app_worker_id(registry, options)
        self.flow_types = sorted(registry._registered_flows)
        self.queues = (self.app_worker_id, *self.flow_types)
        self.max_concurrent_tasks = options.max_concurrent_tasks or os.cpu_count() or 1
        self.idle_streams_per_queue = options.idle_streams_per_queue or 1
        self.stop_drain_timeout = options.stop_drain_timeout.total_seconds() or 30
        self._dispatcher = dispatcher
        self._server_address = server_address
        self._executor = executor
        self._credential_runner = credential_runner
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread_id: int | None = None
        self._started = threading.Event()
        self._wake: asyncio.Event | None = None
        self._stopping: asyncio.Event | None = None
        self._stop_completed: asyncio.Event | None = None
        self._stop_drain_deadline: float | None = None
        self._streams: set[PullStream] = set()
        self._busy_tasks = 0
        self._unsupported_logged = False

    def unknown_task_reason(
        self, task: pb.PollWorkerTask
    ) -> pb.PollWorkerTaskRejectReason | None:
        kind = task.WhichOneof("request")
        if kind == "rpc":
            flow = self._registry._registered_flows.get(task.rpc.flow_type)
            if flow is None or task.rpc.rpc_name not in flow.rpcs:
                return pb.POLL_WORKER_TASK_REJECT_REASON_UNKNOWN_RPC
        elif kind in ("wait_for", "execute"):
            request = task.wait_for if kind == "wait_for" else task.execute
            flow = self._registry._registered_flows.get(request.flow_type)
            if flow is None:
                return pb.POLL_WORKER_TASK_REJECT_REASON_UNKNOWN_STEP
            if kind == "execute" and request.step_type == "sys:timeout_handler":
                if not flow.has_timeout_handler:
                    return pb.POLL_WORKER_TASK_REJECT_REASON_UNKNOWN_STEP
            elif request.step_type not in flow.steps:
                return pb.POLL_WORKER_TASK_REJECT_REASON_UNKNOWN_STEP
        return None

    async def run(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._loop_thread_id = threading.get_ident()
        self._wake = asyncio.Event()
        self._stopping = asyncio.Event()
        self._stop_completed = asyncio.Event()
        self._started.set()
        channel = outbound_worker_task_channel(self._server_address)
        stub = dex_pb2_grpc.WorkerTaskServiceStub(channel)  # type: ignore[no-untyped-call]
        try:
            self._wake.set()
            while not self._stopping.is_set():
                await self._wake.wait()
                self._wake.clear()
                self._reconcile(stub)
            await self._stop_completed.wait()
            await asyncio.gather(
                *(stream.run_task for stream in tuple(self._streams)),
                return_exceptions=True,
            )
        finally:
            try:
                await channel.close(grace=None)
            finally:
                self._loop_thread_id = None

    def signal(self) -> None:
        if self._wake is not None:
            self._wake.set()

    def _reconcile(self, stub: Any) -> None:
        if self._stopping is None or self._stopping.is_set():
            return
        for queue in self.queues:
            idle = sum(
                stream.queue == queue
                and not stream.is_busy
                and not stream.is_half_closing
                for stream in self._streams
            )
            for _ in range(self.idle_streams_per_queue - idle):
                stream = PullStream(self, stub, queue)
                self._streams.add(stream)
                stream.run_task = asyncio.create_task(stream.run())
                stream.run_task.add_done_callback(stream.on_done)

    def _stream_done(self, stream: PullStream, completed: asyncio.Task[None]) -> None:
        self._streams.discard(stream)
        if not completed.cancelled():
            failure = completed.exception()
            if failure is not None:
                _LOGGER.error("Worker task stream failed", exc_info=failure)
        self.signal()

    def accept_task(self) -> bool:
        if self._stopping is None or self._stopping.is_set():
            return False
        if self._busy_tasks >= self.max_concurrent_tasks:
            return False
        self._busy_tasks += 1
        self.signal()
        return True

    def finish_task(self) -> None:
        self._busy_tasks -= 1
        self.signal()

    async def stop(self) -> None:
        if self._stopping is None:
            return
        if self._stopping.is_set():
            if self._stop_completed is not None:
                await self._stop_completed.wait()
            return
        if self._stop_completed is None:
            self._stop_completed = asyncio.Event()
        self._stop_drain_deadline = (
            asyncio.get_running_loop().time() + self.stop_drain_timeout
        )
        self._stopping.set()
        try:
            for stream in tuple(self._streams):
                stream.drain()
                if not stream.is_busy and not stream.is_half_closing:
                    stream.run_task.cancel()
            self.signal()
            active = [
                stream.handler_task for stream in self._streams if stream.handler_task
            ]
            if active:
                _, pending = await asyncio.wait(
                    active, timeout=self._remaining_stop_drain_time()
                )
                if pending:
                    for stream in tuple(self._streams):
                        stream.cancel_handler()
                    await asyncio.gather(*pending, return_exceptions=True)
            stream_tasks = [stream.run_task for stream in tuple(self._streams)]
            if stream_tasks:
                await asyncio.wait(
                    stream_tasks, timeout=self._remaining_stop_drain_time()
                )
                for stream in tuple(self._streams):
                    if not stream.run_task.done() and not stream.is_half_closing:
                        stream.close()
                await asyncio.gather(*stream_tasks, return_exceptions=True)
        finally:
            self._stop_completed.set()

    def _remaining_stop_drain_time(self) -> float:
        if self._stop_drain_deadline is None:
            raise RuntimeError("pull Worker Stop drain has not started")
        return max(0, self._stop_drain_deadline - asyncio.get_running_loop().time())

    def stop_threadsafe(self) -> None:
        if self.is_loop_thread():
            raise RuntimeError("pull Worker.stop() must be called from another thread")
        if not self._started.wait(timeout=10):
            raise RuntimeError("pull Worker did not start its event loop")
        if self._loop is not None and self._loop.is_running():
            asyncio.run_coroutine_threadsafe(self.stop(), self._loop).result()

    def is_loop_thread(self) -> bool:
        return self._loop_thread_id == threading.get_ident()

    @property
    def is_stopping(self) -> bool:
        return self._stopping is not None and self._stopping.is_set()


class PullStream:
    def __init__(self, runtime: PullWorkerRuntime, stub: Any, queue: str) -> None:
        self.runtime = runtime
        self.stub = stub
        self.queue = queue
        self.run_task: asyncio.Task[None]
        self.handler_task: asyncio.Task[None] | None = None
        self.task_id = ""
        self.is_busy = False
        self.is_draining = False
        self.is_half_closing = False
        self._active = threading.Event()
        self._call: Any = None
        self._send_lock = asyncio.Lock()
        self._is_reconnecting = False
        self._deadline_handle: asyncio.TimerHandle | None = None
        self._supports_unknown_name_rejection = False

    def on_done(self, completed: asyncio.Task[None]) -> None:
        self.runtime._stream_done(self, completed)

    async def run(self) -> None:
        backoff = 0.1
        while not self.runtime.is_stopping:
            self.is_draining = False
            was_registered = False
            error: grpc.RpcError | None = None
            try:
                was_registered = await self._poll()
            except grpc.RpcError as failure:
                error = failure
            except Exception:
                _LOGGER.exception(
                    "Worker task stream failed", extra={"queue": self.queue}
                )
            if self.runtime.is_stopping:
                break
            if self._is_reconnecting:
                self._is_reconnecting = False
                backoff = 0.1
            if was_registered:
                backoff = 0.1
            code = error.code() if error is not None else None
            if code is grpc.StatusCode.UNIMPLEMENTED:
                if not self.runtime._unsupported_logged:
                    self.runtime._unsupported_logged = True
                    _LOGGER.error(
                        "pull Worker polling is unsupported by this Dex server"
                    )
                backoff = 60
            elif code in _PERMANENT_CODES:
                _LOGGER.error("Worker task stream rejected: %s", error)
                backoff = 60
            elif error is not None:
                _LOGGER.warning("Worker task stream disconnected: %s", error)
            if self.runtime._stopping is not None:
                try:
                    await asyncio.wait_for(
                        self.runtime._stopping.wait(),
                        timeout=backoff + random.uniform(0, backoff / 2),
                    )
                except asyncio.TimeoutError:
                    pass
            if code not in _PERMANENT_CODES:
                backoff = min(backoff * 2, 5)

    async def _poll(self) -> bool:
        if self.runtime.is_stopping:
            return False
        self._supports_unknown_name_rejection = False
        if self.runtime._credential_runner is None:
            self._call = self.stub.PollWorkerTask()
        else:
            metadata = await async_credential_metadata(
                self.runtime._credential_runner, timeout=30.0
            )
            if self.runtime.is_stopping:
                return False
            self._call = self.stub.PollWorkerTask(metadata=metadata)
        await self._send(
            pb.PollWorkerTaskRequest(
                register=pb.PollWorkerTaskRegister(
                    queue=self.queue,
                    app_worker_id=self.runtime.app_worker_id,
                    flow_types=self.runtime.flow_types,
                    sdk_name="python",
                    sdk_version=sdk_version(),
                )
            )
        )
        was_registered = False
        reader: asyncio.Task[Any] | None = None
        try:
            while not self.runtime.is_stopping or self.is_busy:
                if reader is None:
                    reader = asyncio.create_task(self._call.read())
                waiting: set[asyncio.Task[Any]] = {reader}
                if self.handler_task is not None:
                    waiting.add(self.handler_task)
                completed, _ = await asyncio.wait(
                    waiting, return_when=asyncio.FIRST_COMPLETED
                )
                if self.handler_task in completed:
                    await self._finish_handler()
                    if self.is_draining:
                        if reader.done():
                            frame = reader.result()
                            reader = None
                            if (
                                frame is not _GRPC_AIO_EOF
                                and frame.WhichOneof("frame") == "task"
                            ):
                                await self._reject_task(frame.task)
                        self.is_half_closing = True
                        self.runtime.signal()
                        await self._call.done_writing()
                        await self._await_stream_eof(reader)
                        reader = None
                        self._is_reconnecting = True
                        return was_registered
                if reader not in completed:
                    continue
                frame = reader.result()
                reader = None
                if frame is _GRPC_AIO_EOF:
                    return was_registered
                kind = frame.WhichOneof("frame")
                if not was_registered:
                    if kind == "reconnect":
                        self._is_reconnecting = True
                        return False
                    if kind != "registered":
                        raise RuntimeError("Worker task stream did not register")
                    self._supports_unknown_name_rejection = frame.registered.supports_unknown_name_rejection
                    was_registered = True
                    continue
                if kind == "task":
                    await self._start_task(frame.task)
                elif (
                    kind == "cancel_task" and frame.cancel_task.task_id == self.task_id
                ):
                    self.cancel_handler()
                elif kind == "ping":
                    await self._send(
                        pb.PollWorkerTaskRequest(pong=pb.PollWorkerTaskPong())
                    )
                elif kind == "reconnect":
                    self.drain()
                    if not self.is_busy:
                        self._is_reconnecting = True
                        return was_registered
        finally:
            self.is_half_closing = False
            self.cancel_handler()
            if self.handler_task is not None:
                await self._finish_handler()
            self._call.cancel()
            if reader is not None:
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)
            self._call = None
        return was_registered

    async def _await_stream_eof(self, reader: asyncio.Task[Any] | None) -> None:
        loop = asyncio.get_running_loop()
        reconnect_deadline = loop.time() + self.runtime.stop_drain_timeout
        stopping = self.runtime._stopping
        if stopping is None:
            raise RuntimeError("pull Worker Stop event is unavailable")
        stop_wait: asyncio.Task[bool] | None = None
        try:
            while True:
                deadline = self.runtime._stop_drain_deadline or reconnect_deadline
                remaining = deadline - loop.time()
                if remaining <= 0:
                    _LOGGER.warning("Worker task stream did not close after draining")
                    return
                if reader is None:
                    reader = asyncio.create_task(self._call.read())
                waiting: set[asyncio.Task[Any]] = {reader}
                if self.runtime._stop_drain_deadline is None:
                    if stop_wait is None:
                        stop_wait = asyncio.create_task(stopping.wait())
                    waiting.add(stop_wait)
                completed, _ = await asyncio.wait(
                    waiting, timeout=remaining, return_when=asyncio.FIRST_COMPLETED
                )
                if not completed:
                    _LOGGER.warning("Worker task stream did not close after draining")
                    return
                if stop_wait in completed:
                    await stop_wait
                    stop_wait = None
                    continue
                frame = await reader
                reader = None
                if frame is _GRPC_AIO_EOF:
                    return
        finally:
            if stop_wait is not None:
                stop_wait.cancel()
                try:
                    await stop_wait
                except asyncio.CancelledError:
                    pass
            if reader is not None:
                reader.cancel()
                try:
                    await reader
                except asyncio.CancelledError:
                    pass

    async def _start_task(self, task: pb.PollWorkerTask) -> None:
        unknown_reason = (
            self.runtime.unknown_task_reason(task)
            if self._supports_unknown_name_rejection
            else None
        )
        if unknown_reason is not None:
            await self._reject_task(task, unknown_reason)
            return
        if self.is_busy or self.is_draining or not self.runtime.accept_task():
            await self._reject_task(task)
            return
        self.is_busy = True
        self.task_id = task.task_id
        self._active.set()
        self.handler_task = asyncio.create_task(self._execute(task))
        if task.HasField("deadline"):
            deadline = task.deadline.ToDatetime(tzinfo=timezone.utc)
            remaining = max(0, (deadline - datetime.now(timezone.utc)).total_seconds())
            self._deadline_handle = asyncio.get_running_loop().call_later(
                remaining, self._cancel_if_current, task.task_id
            )
        self.runtime.signal()

    async def _reject_task(
        self,
        task: pb.PollWorkerTask,
        reason: pb.PollWorkerTaskRejectReason = pb.POLL_WORKER_TASK_REJECT_REASON_NO_CAPACITY,
    ) -> None:
        await self._send(
            pb.PollWorkerTaskRequest(
                reject=pb.PollWorkerTaskReject(
                    task_id=task.task_id,
                    reason=reason,
                )
            )
        )

    def _cancel_if_current(self, task_id: str) -> None:
        if self.task_id == task_id:
            self.cancel_handler()

    async def _execute(self, task: pb.PollWorkerTask) -> None:
        kind = task.WhichOneof("request")
        try:
            if self.runtime._executor is None:
                await self._execute_async(task, kind)
            else:
                await asyncio.get_running_loop().run_in_executor(
                    self.runtime._executor, self._execute_sync, task, kind
                )
        except asyncio.CancelledError:
            await self._send_error(
                task.task_id,
                asyncio.CancelledError("Worker task canceled"),
                grpc.StatusCode.CANCELLED,
            )
        except BaseException as failure:
            _LOGGER.exception("Python Worker invocation failed")
            status = grpc.StatusCode.CANCELLED if not self._active.is_set() else None
            await self._send_error(task.task_id, failure, status)

    async def _execute_async(self, task: pb.PollWorkerTask, kind: str | None) -> None:
        if kind == "wait_for":
            async for output in self.runtime._dispatcher.invoke_wait_for(
                task.wait_for, self._active.is_set
            ):
                await self._send_frame(
                    pb.PollWorkerTaskFrame(task_id=task.task_id, wait_for=output)
                )
        elif kind == "execute":
            async for output in self.runtime._dispatcher.invoke_execute(
                task.execute, self._active.is_set
            ):
                await self._send_frame(
                    pb.PollWorkerTaskFrame(task_id=task.task_id, execute=output)
                )
        elif kind == "rpc":
            output = await self.runtime._dispatcher.invoke_rpc(
                task.rpc, self._active.is_set
            )
            await self._send_frame(
                pb.PollWorkerTaskFrame(task_id=task.task_id, rpc=output)
            )
        else:
            raise ValueError(f"unsupported Worker task request {kind}")

    def _execute_sync(self, task: pb.PollWorkerTask, kind: str | None) -> None:
        if kind == "wait_for":
            outputs = self.runtime._dispatcher.invoke_wait_for(
                task.wait_for, self._active.is_set
            )
            for output in outputs:
                self._send_from_thread(
                    pb.PollWorkerTaskFrame(task_id=task.task_id, wait_for=output)
                )
        elif kind == "execute":
            outputs = self.runtime._dispatcher.invoke_execute(
                task.execute, self._active.is_set
            )
            for output in outputs:
                self._send_from_thread(
                    pb.PollWorkerTaskFrame(task_id=task.task_id, execute=output)
                )
        elif kind == "rpc":
            output = self.runtime._dispatcher.invoke_rpc(task.rpc, self._active.is_set)
            self._send_from_thread(
                pb.PollWorkerTaskFrame(task_id=task.task_id, rpc=output)
            )
        else:
            raise ValueError(f"unsupported Worker task request {kind}")

    def _send_from_thread(self, frame: pb.PollWorkerTaskFrame) -> None:
        loop = self.runtime._loop
        if loop is None:
            raise RuntimeError("pull Worker event loop has stopped")
        asyncio.run_coroutine_threadsafe(self._send_frame(frame), loop).result()

    async def _send_error(
        self,
        task_id: str,
        failure: BaseException,
        status: grpc.StatusCode | None = None,
    ) -> None:
        worker_error = pb.WorkerErrorResponse()
        report = _worker_error_status(failure)
        report.details[0].Unpack(worker_error)
        if status is not None:
            worker_error.grpc_status = status.value[0]
        try:
            await self._send_frame(
                pb.PollWorkerTaskFrame(task_id=task_id, worker_error=worker_error)
            )
        except Exception:
            _LOGGER.exception("send Worker error frame failed")

    async def _send_frame(self, frame: pb.PollWorkerTaskFrame) -> None:
        await self._send(pb.PollWorkerTaskRequest(task_frame=frame))

    async def _send(self, request: pb.PollWorkerTaskRequest) -> None:
        async with self._send_lock:
            await self._call.write(request)

    async def _finish_handler(self) -> None:
        if self.handler_task is None:
            return
        await asyncio.gather(self.handler_task, return_exceptions=True)
        if self._deadline_handle is not None:
            self._deadline_handle.cancel()
            self._deadline_handle = None
        self.handler_task = None
        self.task_id = ""
        self.is_busy = False
        self._active.clear()
        self.runtime.finish_task()

    def cancel_handler(self) -> None:
        self._active.clear()
        if self.runtime._executor is None and self.handler_task is not None:
            self.handler_task.cancel()

    def drain(self) -> None:
        self.is_draining = True
        if not self.is_busy and not self.is_half_closing:
            self.close()

    def close(self) -> None:
        if self._call is not None:
            self._call.cancel()
