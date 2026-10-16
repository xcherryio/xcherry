# Copyright (c) 2026 Super Durable, Inc.
#
# Licensed under the Sustainable Use License 1.0.
# You may not use this file except in compliance with the License.
# See the LICENSE file in the repository root.
#
# SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import suppress
from inspect import isawaitable, isgenerator
from typing import Any, Callable

from dex._async_value_hydrator import AsyncValueHydrator
from dex._invocation_context import InvocationContext, InvocationMethod
from dex._server_protocol import NegotiatedServerProtocol
from dex._value_mapper import ValueMapper
from dex._worker_dispatcher import _TIMEOUT_HANDLER_STEP_TYPE, WorkerDispatcher
from dex.dexpb import dex_pb2 as pb
from dex.flow import Registry, RPCResult
from dex.runtime_errors import InvalidStepResultError, ValueMappingError
from dex.step import StepDecision, StepOutput
from dex.wait import Wait


class _AsyncStepOutputEmitter:
    def __init__(self) -> None:
        self.queue: asyncio.Queue[StepOutput] = asyncio.Queue()
        self._is_closed = False
        self._close_callbacks: list[Callable[[], None]] = []

    def emit_nowait(self, output: StepOutput) -> None:
        if self._is_closed:
            return
        self.queue.put_nowait(output)

    async def emit(self, output: StepOutput) -> None:
        if self._is_closed:
            raise asyncio.CancelledError
        self.queue.put_nowait(output)
        await asyncio.sleep(0)
        if self._is_closed:
            raise asyncio.CancelledError

    def close(self) -> None:
        if self._is_closed:
            return
        self._is_closed = True
        for callback in self._close_callbacks:
            callback()

    def add_close_callback(self, callback: Callable[[], None]) -> None:
        if self._is_closed:
            callback()
            return
        self._close_callbacks.append(callback)


class AsyncWorkerDispatcher(WorkerDispatcher):
    def __init__(
        self,
        registry: Registry,
        values: ValueMapper,
        hydrator: AsyncValueHydrator,
        server_protocol: NegotiatedServerProtocol,
    ) -> None:
        if server_protocol is None:
            raise ValueError(
                "AsyncWorkerDispatcher requires a NegotiatedServerProtocol"
            )
        self._registry = registry
        self._values = values
        self._async_hydrator = hydrator
        self._server_protocol = server_protocol

    async def invoke_wait_for(  # type: ignore[override]
        self,
        original: pb.InvokeWaitForMethodRequest,
        is_active: Callable[[], bool] | None = None,
    ) -> AsyncGenerator[pb.InvokeWaitForMethodOutput, None]:
        emitter = _AsyncStepOutputEmitter()
        handler = asyncio.create_task(
            self._invoke_wait_for_result(original, emitter, is_active)
        )
        try:
            async for output in self._drain_outputs(emitter, handler):
                yield self._map_wait_for_output(output)
            yield pb.InvokeWaitForMethodOutput(result=await handler)
        finally:
            await self._close_invocation(emitter, handler)

    async def _invoke_wait_for_result(
        self,
        original: pb.InvokeWaitForMethodRequest,
        emitter: _AsyncStepOutputEmitter,
        is_active: Callable[[], bool] | None,
    ) -> pb.InvokeWaitForMethodResponse:
        request = await self._async_hydrator.wait_for_request(original)
        flow = self._registry._flow_by_type(request.flow_type)
        step = flow.step(request.step_type)
        context = InvocationContext(
            InvocationMethod.WAIT_FOR,
            flow,
            request.context,
            self._values,
            request.attributes,
            channel_infos=dict(request.channel_infos),
            loaded_channel_messages=dict(request.loaded_channel_messages),
            loaded_attribute_map_instances=request.loaded_attribute_map_instances,
            loaded_channel_names=request.loaded_channel_names,
            loaded_channel_map_instances=request.loaded_channel_map_instances,
            is_active=is_active,
            output_emitter=emitter,
        )
        input = self._values.decode(request.step_input, step.input_codec)
        response: pb.InvokeWaitForMethodResponse | None = None
        failure: BaseException | None = None
        cause: BaseException | None = None
        try:
            wait = step.step.wait_for(context, input)
            if isgenerator(wait):
                wait.close()
                raise InvalidStepResultError(
                    flow.name,
                    step.name,
                    "wait_for",
                    "synchronous generators require Worker",
                )
            if isawaitable(wait):
                wait = await wait
            if not isinstance(wait, Wait):
                raise TypeError("wait_for must return Wait")
            response = pb.InvokeWaitForMethodResponse(
                upsert_attributes=self._result_attribute_writes(context),
                upsert_step_exe_locals=list(context.local_writes.values()),
                record_events=context.events,
                publish_to_channel=context.publications,
                delete_from_channel=context.channel_deletions,
            )
            waiting = self._map_wait(flow, wait)
            if waiting is not None:
                response.waiting_condition.CopyFrom(waiting)
        except InvalidStepResultError as error:
            failure = error
        except (TypeError, ValueError) as error:
            failure = InvalidStepResultError(
                flow.name, step.name, "wait_for", str(error)
            )
            cause = error
        except BaseException as error:
            failure = error
        combined = context._finalize_step_outputs(failure)
        if combined is not None:
            if cause is not None:
                raise combined from cause
            raise combined
        assert response is not None
        return response

    async def invoke_execute(  # type: ignore[override]
        self,
        original: pb.InvokeExecuteMethodRequest,
        is_active: Callable[[], bool] | None = None,
    ) -> AsyncGenerator[pb.InvokeExecuteMethodOutput, None]:
        if original.step_type == _TIMEOUT_HANDLER_STEP_TYPE:
            emitter = _AsyncStepOutputEmitter()
            handler = asyncio.create_task(
                self._invoke_timeout_handler_async(original, emitter, is_active)
            )
            try:
                async for output in self._drain_outputs(emitter, handler):
                    yield self._map_execute_output(output)
                yield pb.InvokeExecuteMethodOutput(result=await handler)
            finally:
                await self._close_invocation(emitter, handler)
            return
        emitter = _AsyncStepOutputEmitter()
        handler = asyncio.create_task(
            self._invoke_execute_result(original, emitter, is_active)
        )
        try:
            async for output in self._drain_outputs(emitter, handler):
                yield self._map_execute_output(output)
            yield pb.InvokeExecuteMethodOutput(result=await handler)
        finally:
            await self._close_invocation(emitter, handler)

    async def _invoke_execute_result(
        self,
        original: pb.InvokeExecuteMethodRequest,
        emitter: _AsyncStepOutputEmitter,
        is_active: Callable[[], bool] | None,
    ) -> pb.InvokeExecuteMethodResponse:
        request = await self._async_hydrator.execute_request(original)
        flow = self._registry._flow_by_type(request.flow_type)
        step = flow.step(request.step_type)
        condition_results = (
            request.condition_results if request.HasField("condition_results") else None
        )
        context = InvocationContext(
            InvocationMethod.EXECUTE,
            flow,
            request.context,
            self._values,
            request.attributes,
            request.step_exe_locals,
            condition_results,
            channel_infos=dict(request.channel_infos),
            loaded_channel_messages=dict(request.loaded_channel_messages),
            loaded_attribute_map_instances=request.loaded_attribute_map_instances,
            loaded_channel_names=request.loaded_channel_names,
            loaded_channel_map_instances=request.loaded_channel_map_instances,
            is_active=is_active,
            output_emitter=emitter,
        )
        input = self._values.decode(request.step_input, step.input_codec)
        response: pb.InvokeExecuteMethodResponse | None = None
        failure: BaseException | None = None
        cause: BaseException | None = None
        try:
            decision: Any = step.step.execute(context, input)
            if isgenerator(decision):
                decision.close()
                raise InvalidStepResultError(
                    flow.name,
                    step.name,
                    "execute",
                    "synchronous generators require Worker",
                )
            if isawaitable(decision):
                decision = await decision
            if not isinstance(decision, StepDecision):
                raise TypeError("execute must return StepDecision")
            response = pb.InvokeExecuteMethodResponse(
                step_decision=self._map_decision(flow, decision),
                upsert_attributes=self._result_attribute_writes(context),
                record_events=context.events,
                upsert_step_exe_locals=list(context.local_writes.values()),
                publish_to_channel=context.publications,
                delete_from_channel=context.channel_deletions,
            )
        except InvalidStepResultError as error:
            failure = error
        except (TypeError, ValueError) as error:
            failure = InvalidStepResultError(
                flow.name, step.name, "execute", str(error)
            )
            cause = error
        except BaseException as error:
            failure = error
        combined = context._finalize_step_outputs(failure)
        if combined is not None:
            if cause is not None:
                raise combined from cause
            raise combined
        assert response is not None
        return response

    async def _invoke_timeout_handler_async(
        self,
        original: pb.InvokeExecuteMethodRequest,
        emitter: _AsyncStepOutputEmitter,
        is_active: Callable[[], bool] | None,
    ) -> pb.InvokeExecuteMethodResponse:
        request = await self._async_hydrator.execute_request(original)
        flow = self._registry._flow_by_type(request.flow_type)
        if request.HasField("step_input"):
            raise InvalidStepResultError(
                flow.name, _TIMEOUT_HANDLER_STEP_TYPE, "execute", "input must be absent"
            )
        if not flow.has_timeout_handler:
            raise InvalidStepResultError(
                flow.name,
                _TIMEOUT_HANDLER_STEP_TYPE,
                "execute",
                "handler is not registered",
            )
        condition_results = (
            request.condition_results if request.HasField("condition_results") else None
        )
        context = InvocationContext(
            InvocationMethod.TIMEOUT,
            flow,
            request.context,
            self._values,
            request.attributes,
            request.step_exe_locals,
            condition_results,
            channel_infos=dict(request.channel_infos),
            loaded_channel_messages=dict(request.loaded_channel_messages),
            loaded_attribute_map_instances=request.loaded_attribute_map_instances,
            loaded_channel_names=request.loaded_channel_names,
            loaded_channel_map_instances=request.loaded_channel_map_instances,
            is_active=is_active,
            output_emitter=emitter,
        )
        response: pb.InvokeExecuteMethodResponse | None = None
        failure: BaseException | None = None
        cause: BaseException | None = None
        try:
            decision: Any = flow.flow.handle_timeout(context)
            if isgenerator(decision):
                decision.close()
                raise InvalidStepResultError(
                    flow.name,
                    _TIMEOUT_HANDLER_STEP_TYPE,
                    "execute",
                    "synchronous generators require Worker",
                )
            if isawaitable(decision):
                decision = await decision
            if not isinstance(decision, StepDecision):
                raise TypeError("handle_timeout must return StepDecision")
            response = pb.InvokeExecuteMethodResponse(
                step_decision=self._map_decision(flow, decision),
                upsert_attributes=self._result_attribute_writes(context),
                record_events=context.events,
                upsert_step_exe_locals=list(context.local_writes.values()),
                publish_to_channel=context.publications,
                delete_from_channel=context.channel_deletions,
            )
        except InvalidStepResultError as error:
            failure = error
        except (TypeError, ValueError) as error:
            failure = InvalidStepResultError(
                flow.name, _TIMEOUT_HANDLER_STEP_TYPE, "execute", str(error)
            )
            cause = error
        except BaseException as error:
            failure = error
        combined = context._finalize_step_outputs(failure)
        if combined is not None:
            if cause is not None:
                raise combined from cause
            raise combined
        assert response is not None
        return response

    async def invoke_rpc(  # type: ignore[override]
        self,
        original: pb.InvokeWorkerRPCRequest,
        is_active: Callable[[], bool] | None = None,
    ) -> pb.InvokeWorkerRPCResponse:
        request = await self._async_hydrator.rpc_request(original)
        flow = self._registry._flow_by_type(request.flow_type)
        rpc = flow.rpc(request.rpc_name)
        context = InvocationContext(
            InvocationMethod.RPC,
            flow,
            request.context,
            self._values,
            request.attributes,
            channel_infos=dict(request.channel_infos),
            loaded_channel_messages=dict(request.loaded_channel_messages),
            loaded_attribute_map_instances=request.loaded_attribute_map_instances,
            loaded_channel_names=request.loaded_channel_names,
            loaded_channel_map_instances=request.loaded_channel_map_instances,
            is_active=is_active,
        )
        arguments: list[object] = [context]
        if rpc.input_codec is not None:
            arguments.append(self._values.decode(request.input, rpc.input_codec))
        returned: Any = rpc.method(*arguments)
        if isawaitable(returned):
            returned = await returned
        try:
            response = pb.InvokeWorkerRPCResponse(
                upsert_attributes=self._result_attribute_writes(context),
                record_events=context.events,
                delete_from_channel=context.channel_deletions,
                publish_to_channel=context.publications,
            )
            if isinstance(returned, RPCResult):
                if rpc.output_codec is None:
                    raise TypeError("RPCResult requires an output type")
                response.output.CopyFrom(
                    self._values.encode(returned.output, rpc.output_codec)
                )
                if returned.next_steps:
                    response.step_decision.next_steps.extend(
                        self._map_movements(flow, returned.next_steps)
                    )
                response.step_decision.cancel_step_types.extend(
                    self._map_cancellation_steps(flow, returned.canceling_steps)
                )
                if not returned.next_steps and not returned.canceling_steps:
                    response.ClearField("step_decision")
            elif returned is None and rpc.output_codec is None:
                response.output.CopyFrom(self._values.encode_dynamic(None))
            else:
                raise TypeError("RPC must return RPCResult or None")
            return response
        except ValueMappingError:
            raise
        except (TypeError, ValueError) as error:
            raise InvalidStepResultError(flow.name, None, "rpc", str(error)) from error

    async def _drain_outputs(
        self,
        emitter: _AsyncStepOutputEmitter,
        handler: asyncio.Task[Any],
    ) -> AsyncIterator[StepOutput]:
        while not handler.done() or not emitter.queue.empty():
            if not emitter.queue.empty():
                yield emitter.queue.get_nowait()
                continue
            output = asyncio.create_task(emitter.queue.get())
            try:
                done, _ = await asyncio.wait(
                    (handler, output),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if output in done:
                    yield output.result()
            finally:
                if not output.done():
                    output.cancel()
                    with suppress(asyncio.CancelledError):
                        await output

    async def _close_invocation(
        self,
        emitter: _AsyncStepOutputEmitter,
        handler: asyncio.Task[Any],
    ) -> None:
        emitter.close()
        if handler.done():
            return
        handler.cancel()
        with suppress(asyncio.CancelledError):
            await handler
