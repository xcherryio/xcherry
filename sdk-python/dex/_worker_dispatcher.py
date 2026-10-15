# Copyright (c) 2026 Super Durable, Inc.
#
# Licensed under the Sustainable Use License 1.0.
# You may not use this file except in compliance with the License.
# See the LICENSE file in the repository root.
#
# SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

from __future__ import annotations

from datetime import timedelta
from inspect import isawaitable, isgenerator
from typing import Any, Callable, Generator, cast

from dex._invocation_context import InvocationContext, InvocationMethod
from dex._value_hydrator import ValueHydrator
from dex._value_mapper import ValueMapper
from dex.attribute import (
    AttributeLock,
    AttributeMap,
    AttributeMapLoad,
    _apply_attribute_store_sync,
)
from dex.channel import Channel, ChannelMap, ChannelMapLoad
from dex.condition import ChannelCondition, Condition, SubFlowCondition, TimerCondition
from dex.dexpb import dex_pb2 as pb
from dex.flow import Registry, RPCResult, _RegisteredFlow, _RegisteredStep
from dex.flow_config import ActiveStepSearchMode, FlowConfig
from dex.flow_options import (
    FlowTimeoutHandlerOptions,
    FlowTimeoutPolicy,
    SubFlowOptions,
    SubFlowReusePolicy,
    _resolve_flow_timeout_policy,
)
from dex.runtime_errors import InvalidStepResultError, ValueMappingError
from dex.step import (
    DecisionKind,
    RetryPolicy,
    Step,
    StepDecision,
    StepDurability,
    StepMovement,
    StepOptions,
    StepOutput,
    WaitForFailurePolicy,
    _HeartbeatStepOutput,
    _StreamStepOutput,
)
from dex.wait import Wait, WaitKind

_TIMEOUT_HANDLER_STEP_TYPE = "sys:timeout_handler"


class WorkerDispatcher:
    def __init__(
        self,
        registry: Registry,
        values: ValueMapper,
        hydrator: ValueHydrator,
    ) -> None:
        self._registry = registry
        self._values = values
        self._hydrator = hydrator

    def invoke_wait_for(
        self,
        original: pb.InvokeWaitForMethodRequest,
        is_active: Callable[[], bool] | None = None,
    ) -> Generator[pb.InvokeWaitForMethodOutput, None, None]:
        request = self._hydrator.wait_for_request(original)
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
        )
        input = self._values.decode(request.step_input, step.input_codec)
        wait = step.step.wait_for(context, input)
        if isawaitable(wait):
            raise InvalidStepResultError(
                flow.name,
                step.name,
                "wait_for",
                "wait_for returned an awaitable; use AsyncWorker for async handlers",
            )
        if isgenerator(wait):
            wait = yield from self._wait_for_generator(flow, step, wait)
        try:
            if not isinstance(wait, Wait):
                raise TypeError("wait_for must return Wait")
            response = pb.InvokeWaitForMethodResponse(
                upsert_attributes=list(context.attribute_writes.values()),
                upsert_step_exe_locals=list(context.local_writes.values()),
                record_events=context.events,
                publish_to_channel=context.publications,
                delete_from_channel=context.channel_deletions,
            )
            waiting = self._map_wait(flow, wait)
            if waiting is not None:
                response.waiting_condition.CopyFrom(waiting)
            yield pb.InvokeWaitForMethodOutput(result=response)
        except (TypeError, ValueError) as error:
            raise InvalidStepResultError(
                flow.name, step.name, "wait_for", str(error)
            ) from error

    def invoke_execute(
        self,
        original: pb.InvokeExecuteMethodRequest,
        is_active: Callable[[], bool] | None = None,
    ) -> Generator[pb.InvokeExecuteMethodOutput, None, None]:
        request = self._hydrator.execute_request(original)
        flow = self._registry._flow_by_type(request.flow_type)
        if request.step_type == _TIMEOUT_HANDLER_STEP_TYPE:
            response = yield from self._invoke_timeout_handler(request, flow)
            yield pb.InvokeExecuteMethodOutput(result=response)
            return
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
        )
        input = self._values.decode(request.step_input, step.input_codec)
        decision = step.step.execute(context, input)
        if isawaitable(decision):
            raise InvalidStepResultError(
                flow.name,
                step.name,
                "execute",
                "execute returned an awaitable; use AsyncWorker for async handlers",
            )
        if isgenerator(decision):
            decision = yield from self._execute_generator(flow, step, decision)
        try:
            if not isinstance(decision, StepDecision):
                raise TypeError("execute must return StepDecision")
            yield pb.InvokeExecuteMethodOutput(
                result=pb.InvokeExecuteMethodResponse(
                    step_decision=self._map_decision(flow, decision),
                    upsert_attributes=list(context.attribute_writes.values()),
                    record_events=context.events,
                    upsert_step_exe_locals=list(context.local_writes.values()),
                    publish_to_channel=context.publications,
                    delete_from_channel=context.channel_deletions,
                )
            )
        except (TypeError, ValueError) as error:
            raise InvalidStepResultError(
                flow.name, step.name, "execute", str(error)
            ) from error

    def _invoke_timeout_handler(
        self,
        request: pb.InvokeExecuteMethodRequest,
        flow: _RegisteredFlow,
    ) -> Generator[pb.InvokeExecuteMethodOutput, None, pb.InvokeExecuteMethodResponse]:
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
        )
        decision = flow.flow.handle_timeout(context)
        if isgenerator(decision):
            while True:
                try:
                    output = next(decision)
                except StopIteration as completion:
                    decision = completion.value
                    break
                try:
                    yield self._map_execute_output(output)
                except (TypeError, ValueError) as error:
                    raise InvalidStepResultError(
                        flow.name,
                        _TIMEOUT_HANDLER_STEP_TYPE,
                        "execute",
                        str(error),
                    ) from error
        try:
            if isawaitable(decision):
                raise TypeError(
                    "handle_timeout returned an awaitable; use AsyncWorker for async handlers"
                )
            if not isinstance(decision, StepDecision):
                raise TypeError("handle_timeout must return StepDecision")
            return pb.InvokeExecuteMethodResponse(
                step_decision=self._map_decision(flow, decision),
                upsert_attributes=list(context.attribute_writes.values()),
                record_events=context.events,
                upsert_step_exe_locals=list(context.local_writes.values()),
                publish_to_channel=context.publications,
                delete_from_channel=context.channel_deletions,
            )
        except (TypeError, ValueError) as error:
            raise InvalidStepResultError(
                flow.name, _TIMEOUT_HANDLER_STEP_TYPE, "execute", str(error)
            ) from error

    def invoke_rpc(
        self,
        original: pb.InvokeWorkerRPCRequest,
        is_active: Callable[[], bool] | None = None,
    ) -> pb.InvokeWorkerRPCResponse:
        request = self._hydrator.rpc_request(original)
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
        returned = rpc.method(*arguments)
        if isawaitable(returned):
            raise TypeError(
                "RPC returned an awaitable; use AsyncWorker for async handlers"
            )
        try:
            response = pb.InvokeWorkerRPCResponse(
                upsert_attributes=list(context.attribute_writes.values()),
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

    def _wait_for_generator(
        self,
        flow: _RegisteredFlow,
        step: _RegisteredStep,
        outputs: Generator[Any, None, Any],
    ) -> Generator[pb.InvokeWaitForMethodOutput, None, Any]:
        while True:
            try:
                output = next(outputs)
            except StopIteration as completion:
                return completion.value
            try:
                yield self._map_wait_for_output(output)
            except (TypeError, ValueError) as error:
                raise InvalidStepResultError(
                    flow.name, step.name, "wait_for", str(error)
                ) from error

    def _execute_generator(
        self,
        flow: _RegisteredFlow,
        step: _RegisteredStep,
        outputs: Generator[Any, None, Any],
    ) -> Generator[pb.InvokeExecuteMethodOutput, None, Any]:
        while True:
            try:
                output = next(outputs)
            except StopIteration as completion:
                return completion.value
            try:
                yield self._map_execute_output(output)
            except (TypeError, ValueError) as error:
                raise InvalidStepResultError(
                    flow.name, step.name, "execute", str(error)
                ) from error

    def _map_wait_for_output(
        self,
        output: StepOutput,
    ) -> pb.InvokeWaitForMethodOutput:
        heartbeat, stream_write = self._map_step_output(output)
        if heartbeat is not None:
            return pb.InvokeWaitForMethodOutput(heartbeat=heartbeat)
        return pb.InvokeWaitForMethodOutput(stream_write=stream_write)

    def _map_execute_output(
        self,
        output: StepOutput,
    ) -> pb.InvokeExecuteMethodOutput:
        heartbeat, stream_write = self._map_step_output(output)
        if heartbeat is not None:
            return pb.InvokeExecuteMethodOutput(heartbeat=heartbeat)
        return pb.InvokeExecuteMethodOutput(stream_write=stream_write)

    def _map_step_output(
        self,
        output: StepOutput,
    ) -> tuple[pb.StepMethodHeartbeat | None, pb.StepStreamWrite | None]:
        if isinstance(output, _HeartbeatStepOutput):
            heartbeat = pb.StepMethodHeartbeat()
            if output.has_value:
                heartbeat.value.CopyFrom(
                    output.encoded_value
                    if output.encoded_value is not None
                    else self._values.encode_dynamic(output.value)
                )
            return heartbeat, None
        if isinstance(output, _StreamStepOutput):
            return None, output.stream_write
        raise TypeError("Step generator must yield StepOutput values")

    def map_step_options(
        self,
        flow: _RegisteredFlow,
        options: StepOptions | None,
    ) -> pb.StepOptions | None:
        if options is None:
            return None
        mapped = pb.StepOptions(
            wait_for_failure_policy=(
                pb.WAIT_FOR_METHOD_FAILURE_POLICY_PROCEED_ON_FAILURE
                if options.wait_for_failure is WaitForFailurePolicy.PROCEED
                else pb.WAIT_FOR_METHOD_FAILURE_POLICY_FAIL_FLOW_ON_FAILURE
            ),
            wait_for_durability_override=cast(
                Any, self._map_durability(options.wait_for_durability)
            ),
            execute_durability_override=cast(
                Any, self._map_durability(options.execute_durability)
            ),
            wait_for_lock_attribute_keys=[
                self._map_lock(lock) for lock in options.wait_for_lock_attributes
            ],
            execute_lock_attribute_keys=[
                self._map_lock(lock) for lock in options.execute_lock_attributes
            ],
        )
        if options.wait_for_method_timeout is not None:
            mapped.wait_for_timeout_seconds = self._seconds32(
                options.wait_for_method_timeout
            )
        if options.execute_method_timeout is not None:
            mapped.execute_timeout_seconds = self._seconds32(
                options.execute_method_timeout
            )
        if options.heartbeat_timeout is not None:
            mapped.heartbeat_timeout_seconds = self._seconds32(
                options.heartbeat_timeout
            )
        if options.wait_for_retry is not None:
            mapped.wait_for_retry_policy.CopyFrom(
                self._map_retry(options.wait_for_retry)
            )
        if options.execute_retry is not None:
            mapped.execute_retry_policy.CopyFrom(self._map_retry(options.execute_retry))
        if options._execute_failure_target is not None:
            target = self._registered_movement_target(
                flow,
                options._execute_failure_target,
            )
            mapped.execute_failure_policy = (
                pb.EXECUTE_METHOD_FAILURE_POLICY_PROCEED_TO_CONFIGURED_STEP
            )
            mapped.execute_failure_proceed_step_type = target.name
            target_options = self.map_step_options(
                flow,
                (
                    options._execute_failure_options
                    if options._execute_failure_options is not None
                    else target.step.get_step_options()
                ),
            )
            if target_options is not None:
                mapped.execute_failure_proceed_step_options.CopyFrom(target_options)
            mapped.execute_failure_proceed_step_options.skip_wait_for = (
                target.skips_wait_for
            )
        wait_for_loads = self._map_handler_state_loads(
            flow,
            "WaitFor",
            options.wait_for_load_attribute_maps,
            options.wait_for_load_attribute_map_instances,
            options.wait_for_load_channels,
            options.wait_for_load_channel_maps,
            options.wait_for_load_channel_map_instances,
        )
        mapped.wait_for_load_attribute_map_instances.extend(wait_for_loads[0])
        mapped.wait_for_load_channel_names.extend(wait_for_loads[1])
        mapped.wait_for_load_channel_map_instances.extend(wait_for_loads[2])
        execute_loads = self._map_handler_state_loads(
            flow,
            "Execute",
            options.execute_load_attribute_maps,
            options.execute_load_attribute_map_instances,
            options.execute_load_channels,
            options.execute_load_channel_maps,
            options.execute_load_channel_map_instances,
        )
        mapped.execute_load_attribute_map_instances.extend(execute_loads[0])
        mapped.execute_load_channel_names.extend(execute_loads[1])
        mapped.execute_load_channel_map_instances.extend(execute_loads[2])
        return mapped

    def map_flow_timeout_handler_options(
        self,
        flow: _RegisteredFlow,
        timeout: timedelta | None,
        policy: FlowTimeoutPolicy,
        options: FlowTimeoutHandlerOptions | None,
    ) -> pb.FlowTimeoutHandlerOptions | None:
        """Map validated timeout-handler options for a Flow start."""
        if options is None:
            return None
        if (
            timeout is None
            or timeout.total_seconds() <= 0
            or policy is not FlowTimeoutPolicy.HANDLER
        ):
            raise ValueError(
                "timeout handler options require a positive timeout with HANDLER policy"
            )
        mapped = pb.FlowTimeoutHandlerOptions(
            durability_override=cast(Any, self._map_durability(options.durability)),
            lock_attribute_keys=[
                self._map_lock(lock) for lock in options.lock_attributes
            ],
        )
        if options.method_timeout is not None:
            mapped.method_timeout_seconds = self._seconds32(options.method_timeout)
        if options.heartbeat_timeout is not None:
            mapped.heartbeat_timeout_seconds = self._seconds32(
                options.heartbeat_timeout
            )
        if options.retry is not None:
            mapped.retry_policy.CopyFrom(self._map_retry(options.retry))
        state_loads = self._map_handler_state_loads(
            flow,
            "timeout handler",
            options.load_attribute_maps,
            options.load_attribute_map_instances,
            options.load_channels,
            options.load_channel_maps,
            options.load_channel_map_instances,
        )
        mapped.load_attribute_map_instances.extend(state_loads[0])
        mapped.load_channel_names.extend(state_loads[1])
        mapped.load_channel_map_instances.extend(state_loads[2])
        if options._failure_target is not None:
            target = self._registered_movement_target(flow, options._failure_target)
            if target.input_type not in (None, type(None)):
                raise ValueError("timeout handler failure Step must use None input")
            mapped.failure_policy = (
                pb.EXECUTE_METHOD_FAILURE_POLICY_PROCEED_TO_CONFIGURED_STEP
            )
            mapped.failure_proceed_step_type = target.name
            target_options = self.map_step_options(
                flow,
                (
                    options._failure_options
                    if options._failure_options is not None
                    else target.step.get_step_options()
                ),
            )
            if target_options is not None:
                mapped.failure_proceed_step_options.CopyFrom(target_options)
            mapped.failure_proceed_step_options.skip_wait_for = target.skips_wait_for
        return mapped

    @staticmethod
    def _map_handler_state_loads(
        flow: _RegisteredFlow,
        source: str,
        attribute_maps: tuple[AttributeMap[Any], ...],
        attribute_map_instances: tuple[AttributeMapLoad, ...],
        channels: tuple[Channel[Any], ...],
        channel_maps: tuple[ChannelMap[Any], ...],
        channel_map_instances: tuple[ChannelMapLoad, ...],
    ) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
        mapped_attribute_maps: list[str] = []
        for attribute_map in attribute_maps:
            WorkerDispatcher._require_state_load(
                flow, source, attribute_map, AttributeMap
            )
            mapped_attribute_maps.append(f"{attribute_map.name}/")
        for attribute_load in attribute_map_instances:
            if not isinstance(attribute_load, AttributeMapLoad):
                raise TypeError(f"{source} has an invalid AttributeMap instance load")
            WorkerDispatcher._require_state_load(
                flow, source, attribute_load.attribute_map, AttributeMap
            )
            mapped_attribute_maps.append(attribute_load.physical_name)
        mapped_channels: list[str] = []
        for channel in channels:
            WorkerDispatcher._require_state_load(flow, source, channel, Channel)
            mapped_channels.append(channel.name)
        mapped_channel_maps: list[str] = []
        for channel_map in channel_maps:
            WorkerDispatcher._require_state_load(flow, source, channel_map, ChannelMap)
            mapped_channel_maps.append(f"{channel_map.name}/")
        for channel_load in channel_map_instances:
            if not isinstance(channel_load, ChannelMapLoad):
                raise TypeError(f"{source} has an invalid ChannelMap instance load")
            WorkerDispatcher._require_state_load(
                flow, source, channel_load.channel_map, ChannelMap
            )
            mapped_channel_maps.append(channel_load.physical_name)
        for kind, values in (
            ("AttributeMap", mapped_attribute_maps),
            ("Channel", mapped_channels),
            ("ChannelMap", mapped_channel_maps),
        ):
            if len(values) != len(set(values)):
                raise ValueError(f"{source} has a duplicate {kind} load")
        return (
            tuple(sorted(mapped_attribute_maps)),
            tuple(sorted(mapped_channels)),
            tuple(sorted(mapped_channel_maps)),
        )

    @staticmethod
    def _require_state_load(
        flow: _RegisteredFlow,
        source: str,
        definition: object,
        expected_type: type[object],
    ) -> None:
        if not isinstance(definition, expected_type):
            raise TypeError(f"{source} has an invalid state load")
        if flow.persistence.get(cast(Any, definition).name) is not definition:
            raise ValueError(f"{source} state load does not belong to Flow {flow.name}")

    def _map_wait(
        self,
        flow: _RegisteredFlow,
        wait: Wait,
    ) -> pb.WaitingCondition | None:
        if wait.kind is WaitKind.SKIP_IMMEDIATELY:
            return None
        mapper = _ConditionMapper(self, flow)
        waiting = pb.WaitingCondition()
        if wait.kind is WaitKind.ALL_OF:
            waiting.waiting_condition_type = pb.WAITING_CONDITION_TYPE_ALL_COMPLETED
            mapper.add_all(wait.conditions)
        elif wait.kind is WaitKind.ANY_OF:
            waiting.waiting_condition_type = pb.WAITING_CONDITION_TYPE_ANY_COMPLETED
            mapper.add_all(wait.conditions)
        elif wait.kind is WaitKind.ANY_COMBINATION_OF:
            waiting.waiting_condition_type = (
                pb.WAITING_CONDITION_TYPE_ANY_COMBINATION_COMPLETED
            )
            for combination in wait.combinations:
                waiting.condition_combinations.add(
                    condition_ids=[
                        mapper.add(condition, id_required=True)
                        for condition in combination.conditions
                    ]
                )
        else:
            raise ValueError("unsupported Wait kind")
        waiting.timer_conditions.extend(mapper.timers)
        waiting.channel_conditions.extend(mapper.channels)
        waiting.sub_flow_conditions.extend(mapper.sub_flows)
        return waiting

    def _map_sub_flow_options(
        self,
        target: _RegisteredFlow,
        options: SubFlowOptions,
    ) -> pb.SubFlowOptions:
        mapped = pb.SubFlowOptions(
            reuse_policy={
                SubFlowReusePolicy.ATTACH: pb.SUB_FLOW_REUSE_POLICY_ATTACH,
                SubFlowReusePolicy.RESTART_IF_PREVIOUS_EXITS_ABNORMALLY: (
                    pb.SUB_FLOW_REUSE_POLICY_RESTART_IF_PREVIOUS_EXITS_ABNORMALLY
                ),
                SubFlowReusePolicy.ALWAYS_RESTART: (
                    pb.SUB_FLOW_REUSE_POLICY_ALWAYS_RESTART
                ),
            }[options.reuse_policy]
        )
        if options.timeout is not None:
            mapped.flow_timeout_seconds = self._seconds32(options.timeout)
        timeout_policy = _resolve_flow_timeout_policy(
            target.name,
            target.has_timeout_handler,
            options.timeout,
            options.timeout_policy,
        )
        mapped.flow_timeout_policy = {
            FlowTimeoutPolicy.DEFAULT: pb.FLOW_TIMEOUT_POLICY_UNSPECIFIED,
            FlowTimeoutPolicy.FAIL: pb.FLOW_TIMEOUT_POLICY_FAIL,
            FlowTimeoutPolicy.CANCEL: pb.FLOW_TIMEOUT_POLICY_CANCEL,
            FlowTimeoutPolicy.HANDLER: pb.FLOW_TIMEOUT_POLICY_HANDLER,
        }[timeout_policy]
        timeout_handler_options = self.map_flow_timeout_handler_options(
            target,
            options.timeout,
            timeout_policy,
            options.timeout_handler_options,
        )
        if timeout_handler_options is not None:
            mapped.timeout_handler_options.CopyFrom(timeout_handler_options)
        if options.start_delay is not None:
            mapped.flow_start_delay_seconds = self._seconds32(options.start_delay)
        if options.retry_policy is not None:
            mapped.retry_policy.CopyFrom(self._map_flow_retry(options.retry_policy))
        for initialization in options._attribute_initializations:
            definition = initialization.definition
            if target.persistence.get(definition.name) is not definition:
                raise ValueError(
                    f"SubFlow Attribute does not belong to {target.name}: {definition.name}"
                )
            key = (
                Registry.physical_name(definition.name, initialization.instance)
                if isinstance(definition, AttributeMap)
                and initialization.instance is not None
                else definition.name
            )
            write = pb.AttributeWrite(
                key=key,
                value=self._values.encode(
                    initialization.value,
                    self._values.codec(definition.value_type),
                ),
            )
            index = self._values.index_config(
                definition.index, isinstance(definition, AttributeMap)
            )
            if index is not None:
                write.index_config.CopyFrom(index)
            _apply_attribute_store_sync(write, definition)
            mapped.attributes.append(write)
        if options.config_override is not None:
            mapped.flow_config_override.CopyFrom(
                self._map_sub_flow_config(options.config_override)
            )
        return mapped

    @staticmethod
    def _map_flow_retry(retry: RetryPolicy) -> pb.FlowRetryPolicy:
        mapped = pb.FlowRetryPolicy(
            backoff_coefficient=retry.backoff_coefficient,
            maximum_attempts=retry.maximum_attempts,
        )
        if retry.initial_interval is not None:
            mapped.initial_interval_seconds = WorkerDispatcher._seconds32(
                retry.initial_interval
            )
        if retry.maximum_interval is not None:
            mapped.maximum_interval_seconds = WorkerDispatcher._seconds32(
                retry.maximum_interval
            )
        return mapped

    @staticmethod
    def _map_sub_flow_config(config: FlowConfig) -> pb.FlowConfig:
        mapped = pb.FlowConfig()
        if config.active_step_search_mode is not None:
            mapped.active_step_search_mode = {
                ActiveStepSearchMode.DEFAULT: pb.ACTIVE_STEP_SEARCH_MODE_UNSPECIFIED,
                ActiveStepSearchMode.ALL: pb.ACTIVE_STEP_SEARCH_MODE_ENABLED_FOR_ALL,
                ActiveStepSearchMode.WITH_WAIT_FOR: (
                    pb.ACTIVE_STEP_SEARCH_MODE_ENABLED_FOR_STEPS_WITH_WAIT_FOR
                ),
                ActiveStepSearchMode.DISABLED: pb.ACTIVE_STEP_SEARCH_MODE_DISABLED,
            }[config.active_step_search_mode]
        if config.continue_as_new_threshold is not None:
            mapped.continue_as_new_threshold = config.continue_as_new_threshold
        if config.continue_as_new_page_size_bytes is not None:
            mapped.continue_as_new_page_size_in_bytes = (
                config.continue_as_new_page_size_bytes
            )
        if config.step_durability is not None:
            mapped.step_durability = {
                StepDurability.DEFAULT: pb.STEP_DURABILITY_UNSPECIFIED,
                StepDurability.SYNC: pb.STEP_DURABILITY_SYNC,
                StepDurability.ASYNC: pb.STEP_DURABILITY_ASYNC,
            }[config.step_durability]
        if config.worker_target is not None:
            mapped.worker_target.CopyFrom(
                pb.WorkerTarget(
                    address=config.worker_target.address,
                    is_headless_address=config.worker_target.headless,
                    is_pull_mode=config.worker_target.pull,
                )
            )
        if config.attribute_store_names is not None:
            mapped.attribute_store_names.CopyFrom(
                pb.AttributeStoreNames(names=config.attribute_store_names)
            )
        return mapped

    def _map_decision(
        self,
        flow: _RegisteredFlow,
        decision: StepDecision,
    ) -> pb.StepDecision:
        mapped = pb.StepDecision()
        if decision.kind is DecisionKind.NEXT:
            if not decision.movements:
                raise ValueError("go_to_many requires a movement")
            mapped.next_steps.extend(self._map_movements(flow, decision.movements))
        elif decision.kind is DecisionKind.GRACEFUL_COMPLETE:
            mapped.close_decision.CopyFrom(
                self._close(
                    pb.CLOSE_DECISION_TYPE_GRACEFUL_COMPLETE,
                    decision.output,
                    decision._has_output(),
                )
            )
        elif decision.kind is DecisionKind.FORCE_COMPLETE:
            mapped.close_decision.CopyFrom(
                self._close(
                    pb.CLOSE_DECISION_TYPE_FORCE_COMPLETE,
                    decision.output,
                    decision._has_output(),
                )
            )
        elif decision.kind is DecisionKind.FORCE_FAIL:
            mapped.close_decision.CopyFrom(
                self._close(pb.CLOSE_DECISION_TYPE_FORCE_FAIL, decision.reason)
            )
        elif decision.kind is DecisionKind.DEAD_END:
            mapped.close_decision.close_decision_type = pb.CLOSE_DECISION_TYPE_DEAD_END
        elif decision.kind is DecisionKind.FORCE_COMPLETE_IF_CHANNELS_EMPTY:
            if decision.fallback is None:
                raise ValueError("conditional close requires a fallback movement")
            names: list[str] = []
            for channel in decision.empty_channels:
                if not isinstance(channel, Channel):
                    raise ValueError("conditional close requires static Channels")
                self._require_persistence_identity(flow, channel)
                names.append(channel.name)
            mapped.close_decision.CopyFrom(
                pb.CloseDecision(
                    close_decision_type=(
                        pb.CLOSE_DECISION_TYPE_FORCE_COMPLETE_ON_CHANNELS_EMPTY
                    ),
                    close_input=self._values.encode_dynamic(decision.output),
                    conditional_channel_names=names,
                )
            )
            mapped.next_steps.append(self._map_movement(flow, decision.fallback))
        else:
            raise ValueError("unsupported StepDecision kind")
        mapped.cancel_step_types.extend(
            self._map_cancellation_steps(flow, decision.canceling_steps)
        )
        global_types = set(mapped.cancel_step_types)
        mapped.cancel_sibling_step_types.extend(
            step_type
            for step_type in self._map_cancellation_steps(
                flow, decision.canceling_sibling_steps
            )
            if step_type not in global_types
        )
        return mapped

    def _map_cancellation_steps(
        self,
        flow: _RegisteredFlow,
        steps: tuple[type[Step[Any]], ...],
    ) -> list[str]:
        step_types: list[str] = []
        seen: set[str] = set()
        for step in steps:
            step_type = self._registered_movement_target(flow, step).name
            if step_type in seen:
                continue
            seen.add(step_type)
            step_types.append(step_type)
        return step_types

    def _close(
        self,
        close_type: int,
        output: object,
        has_output: bool = True,
    ) -> pb.CloseDecision:
        close = pb.CloseDecision(close_decision_type=cast(Any, close_type))
        if has_output:
            close.close_input.CopyFrom(self._values.encode_dynamic(output))
        return close

    def _map_movements(
        self,
        flow: _RegisteredFlow,
        movements: tuple[StepMovement[Any], ...],
    ) -> list[pb.StepMovement]:
        return [self._map_movement(flow, movement) for movement in movements]

    def _map_movement(
        self,
        flow: _RegisteredFlow,
        movement: StepMovement[Any],
    ) -> pb.StepMovement:
        target = self._registered_movement_target(flow, movement.step)
        mapped = pb.StepMovement(
            step_type=target.name,
            step_input=self._values.encode(movement.input, target.input_codec),
        )
        options = self.map_step_options(
            flow,
            (
                movement.options
                if movement.options is not None
                else target.step.get_step_options()
            ),
        )
        if options is not None:
            mapped.step_options.CopyFrom(options)
        mapped.step_options.skip_wait_for = target.skips_wait_for
        return mapped

    @staticmethod
    def _registered_movement_target(
        flow: _RegisteredFlow,
        step: type[Step[Any]],
    ) -> _RegisteredStep:
        return flow.step_class(step)

    @staticmethod
    def _require_persistence_identity(
        flow: _RegisteredFlow,
        definition: Channel[Any] | ChannelMap[Any],
    ) -> None:
        if flow.persistence.get(definition.name) is not definition:
            raise ValueError("Channel does not belong to Flow")

    @staticmethod
    def _map_retry(retry: RetryPolicy) -> pb.RetryPolicy:
        mapped = pb.RetryPolicy(
            backoff_coefficient=retry.backoff_coefficient,
            maximum_attempts=retry.maximum_attempts,
        )
        if retry.initial_interval is not None:
            mapped.initial_interval_seconds = WorkerDispatcher._seconds32(
                retry.initial_interval
            )
        if retry.maximum_interval is not None:
            mapped.maximum_interval_seconds = WorkerDispatcher._seconds32(
                retry.maximum_interval
            )
        if retry.total_duration is not None:
            mapped.total_duration_seconds = WorkerDispatcher._seconds32(
                retry.total_duration
            )
        return mapped

    @staticmethod
    def _map_durability(durability: StepDurability) -> int:
        return {
            StepDurability.DEFAULT: pb.STEP_DURABILITY_UNSPECIFIED,
            StepDurability.SYNC: pb.STEP_DURABILITY_SYNC,
            StepDurability.ASYNC: pb.STEP_DURABILITY_ASYNC,
        }[durability]

    @staticmethod
    def _map_lock(lock: AttributeLock) -> str:
        if lock.instance is None:
            return lock.attribute.name
        return Registry.physical_name(lock.attribute.name, lock.instance)

    @staticmethod
    def _seconds32(duration: timedelta) -> int:
        seconds = duration.total_seconds()
        if seconds < 0 or not seconds.is_integer() or seconds > 2**31 - 1:
            raise ValueError("duration must be whole seconds within int32")
        return int(seconds)


class _ConditionMapper:
    def __init__(self, dispatcher: WorkerDispatcher, flow: _RegisteredFlow) -> None:
        self._dispatcher = dispatcher
        self._flow = flow
        self._ids: dict[int, str] = {}
        self._used: set[str] = set()
        self.timers: list[pb.TimerCondition] = []
        self.channels: list[pb.ChannelCondition] = []
        self.sub_flows: list[pb.SubFlowCondition] = []

    def add_all(self, conditions: tuple[Condition, ...]) -> None:
        if not conditions:
            raise ValueError("Wait requires at least one Condition")
        for condition in conditions:
            self.add(condition)

    def add(self, condition: Condition, *, id_required: bool = False) -> str:
        identity = id(condition)
        existing = self._ids.get(identity)
        if existing is not None:
            return existing
        condition_id = condition.condition_id or ""
        if id_required and not condition_id:
            raise ValueError(
                "any_combination_of requires every Condition to have an ID"
            )
        if condition.condition_id is not None and not condition_id:
            raise ValueError("empty Condition ID")
        if condition_id and condition_id in self._used:
            raise ValueError("duplicate Condition ID")
        if condition_id:
            self._used.add(condition_id)
        if isinstance(condition, TimerCondition):
            seconds = condition.duration.total_seconds()
            if not seconds.is_integer():
                raise ValueError("timer duration must use whole seconds")
            self.timers.append(
                pb.TimerCondition(
                    condition_id=condition_id,
                    duration_seconds=int(seconds),
                )
            )
        elif isinstance(condition, ChannelCondition):
            channel = condition.channel
            if (
                channel is None
                or self._flow.persistence.get(channel.name) is not channel
            ):
                raise ValueError(
                    f"Channel is not registered: {getattr(channel, 'name', '')}"
                )
            channel_name = channel.name
            if isinstance(channel, ChannelMap):
                if condition.instance is None:
                    raise ValueError("ChannelMap condition requires an instance")
                channel_name = Registry.physical_name(channel.name, condition.instance)
            elif condition.instance is not None:
                raise ValueError("static Channel cannot use an instance")
            mapped = pb.ChannelCondition(
                condition_id=condition_id,
                channel_name=channel_name,
            )
            if condition.at_least is not None:
                mapped.at_least = condition.at_least
            if condition.at_most is not None:
                mapped.at_most = condition.at_most
            self.channels.append(mapped)
        elif isinstance(condition, SubFlowCondition):
            if condition.flow is None:
                raise ValueError("SubFlow requires a target Flow")
            target = self._dispatcher._registry._flow_for_instance(condition.flow)
            if target.start_step is None:
                raise ValueError(f"SubFlow {target.name} has no starting Step")
            options = condition.options or SubFlowOptions()
            mapped_sub_flow = pb.SubFlowCondition(
                condition_id=condition_id,
                sub_flow_type=target.name,
                start_step_type=target.start_step.name,
                step_input=self._dispatcher._values.encode(
                    condition.input, target.start_step.input_codec
                ),
                sub_flow_index=len(self.sub_flows),
            )
            step_options = self._dispatcher.map_step_options(
                target, target.start_step.step.get_step_options()
            )
            if step_options is not None:
                mapped_sub_flow.step_options.CopyFrom(step_options)
            mapped_sub_flow.step_options.skip_wait_for = (
                target.start_step.skips_wait_for
            )
            mapped_sub_flow.options.CopyFrom(
                self._dispatcher._map_sub_flow_options(target, options)
            )
            self.sub_flows.append(mapped_sub_flow)
        else:
            raise TypeError("unsupported Condition")
        self._ids[identity] = condition_id
        return condition_id
