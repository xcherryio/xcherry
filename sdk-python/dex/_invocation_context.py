# Copyright (c) 2026 Super Durable, Inc.
#
# Licensed under the Sustainable Use License 1.0.
# You may not use this file except in compliance with the License.
# See the LICENSE file in the repository root.
#
# SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

from __future__ import annotations

from datetime import UTC, datetime
from enum import Enum
from typing import Any, Callable, Protocol, Sequence, TypeVar, cast
from urllib.parse import unquote

from dex._utils import require_map_instance, require_name
from dex._value_mapper import ValueMapper
from dex.attribute import (
    Attribute,
    AttributeMap,
    _apply_attribute_store_sync,
    _apply_hard_delete_on_null,
)
from dex.channel import Channel, ChannelMap, ChannelMessage
from dex.codec import Codec
from dex.context import RecoveryErrorInfo
from dex.dexpb import dex_pb2 as pb
from dex.flow import Registry, _RegisteredFlow
from dex.flow_result import FlowResult, flow_result_from_proto
from dex.runtime_errors import (
    AttributeMapNotLoadedError,
    ChannelMessagesNotLoadedError,
)
from dex.step import (
    _NO_HEARTBEAT_VALUE,
    StepOutput,
    _HeartbeatStepOutput,
    _StreamStepOutput,
)
from dex.stream import Stream

ValueT = TypeVar("ValueT")
_Definition = (
    Attribute[Any] | AttributeMap[Any] | Channel[Any] | ChannelMap[Any] | Stream[Any]
)


class InvocationMethod(Enum):
    WAIT_FOR = "wait_for"
    EXECUTE = "execute"
    RPC = "rpc"
    TIMEOUT = "timeout"


class _StepOutputEmitter(Protocol):
    def emit_nowait(self, output: StepOutput) -> None: ...

    async def emit(self, output: StepOutput) -> None: ...

    def add_close_callback(self, callback: Callable[[], None]) -> None: ...


class _StepOutputFinalizer(Protocol):
    def _finalize_step_output(self) -> None: ...

    def _cancel_step_output(self) -> None: ...


class InvocationContext:
    def __init__(
        self,
        method: InvocationMethod,
        flow: _RegisteredFlow,
        metadata: pb.Context,
        values: ValueMapper,
        attributes: Sequence[pb.KV],
        locals: Sequence[pb.KV] = (),
        condition_results: pb.ConditionResults | None = None,
        channel_infos: dict[str, pb.ChannelInfo] | None = None,
        loaded_channel_messages: dict[str, pb.ChannelValues] | None = None,
        loaded_attribute_map_instances: Sequence[str] = (),
        loaded_channel_names: Sequence[str] = (),
        loaded_channel_map_instances: Sequence[str] = (),
        is_active: Callable[[], bool] | None = None,
        output_emitter: _StepOutputEmitter | None = None,
    ) -> None:
        self._method = method
        self._flow = flow
        self._metadata = metadata
        self._values = values
        self._attributes = self._map_values("Attribute", attributes)
        self._locals = self._map_values("step-execution local", locals)
        self._condition_results = condition_results
        self._channel_infos = dict(channel_infos or {})
        self._loaded_channel_messages = dict(loaded_channel_messages or {})
        self._loaded_attribute_map_instances = frozenset(loaded_attribute_map_instances)
        self._loaded_channel_names = frozenset(loaded_channel_names)
        self._loaded_channel_map_instances = frozenset(loaded_channel_map_instances)
        self._is_active = is_active or _always_active
        self._output_emitter = output_emitter
        self.attribute_writes: dict[str, pb.AttributeWrite] = {}
        self.local_writes: dict[str, pb.KV] = {}
        self.events: list[pb.KV] = []
        self.channel_deletions: list[pb.ChannelMessageDeletion] = []
        self.publications: list[pb.ChannelMessage] = []
        self._event_names: set[str] = set()
        self._step_output_finalizers: list[_StepOutputFinalizer] = []
        self._step_outputs_finalized = False

    @property
    def flow_id(self) -> str:
        return self._metadata.flow_id

    @property
    def run_id(self) -> str:
        return self._metadata.run_id

    @property
    def step_execution_id(self) -> str:
        return self._metadata.step_execution_id

    @property
    def from_step_execution_id(self) -> str:
        return self._metadata.from_step_execution_id

    @property
    def recovery_error(self) -> RecoveryErrorInfo | None:
        if not self._metadata.HasField("recovery_error"):
            return None
        return RecoveryErrorInfo(
            detail=self._metadata.recovery_error.detail,
            error_type=self._metadata.recovery_error.error_type,
        )

    @property
    def first_attempt_at(self) -> datetime:
        return datetime.fromtimestamp(self._metadata.first_attempt_timestamp, UTC)

    @property
    def attempt(self) -> int:
        return self._metadata.attempt

    def has_last_heartbeat_value(self) -> bool:
        return self._metadata.HasField("last_heartbeat_value")

    def get_last_heartbeat_value(
        self,
        value_type: type[ValueT],
    ) -> ValueT | None:
        if not self.has_last_heartbeat_value():
            return None
        return cast(
            ValueT,
            self._values.decode(
                self._metadata.last_heartbeat_value,
                self._flow_codec(value_type),
            ),
        )

    async def heartbeat(self, value: object = _NO_HEARTBEAT_VALUE) -> None:
        if (
            self._method
            not in (
                InvocationMethod.WAIT_FOR,
                InvocationMethod.EXECUTE,
                InvocationMethod.TIMEOUT,
            )
            or self._output_emitter is None
        ):
            raise ValueError("heartbeat requires an asynchronous Step Context")
        has_value = value is not _NO_HEARTBEAT_VALUE
        encoded_value = self._values.encode_dynamic(value) if has_value else None
        await self._output_emitter.emit(
            _HeartbeatStepOutput(has_value, value, encoded_value)
        )

    def has_timer_fired(self, index: int | None = None) -> bool:
        if self._condition_results is None:
            return False
        results = self._condition_results.timer_results
        if index is None:
            return any(
                result.condition_status == pb.CONDITION_STATUS_COMPLETED
                for result in results
            )
        return (
            0 <= index < len(results)
            and results[index].condition_status == pb.CONDITION_STATUS_COMPLETED
        )

    def wait_for_method_failed(self) -> bool:
        return bool(
            self._condition_results is not None
            and self._condition_results.wait_for_failed
        )

    def sub_flow_result(self, index: int = 0) -> FlowResult:
        if self._method is not InvocationMethod.EXECUTE:
            raise ValueError("SubFlow results are available only during execute")
        if (
            self._condition_results is None
            or index < 0
            or index >= len(self._condition_results.sub_flow_results)
        ):
            raise ValueError(f"SubFlow result index is unavailable: {index}")
        return flow_result_from_proto(
            self._condition_results.sub_flow_results[index],
            lambda value, output_type: self._values.decode(
                value, self._values.codec(output_type)
            ),
        )

    def sub_flow_id(self, index: int = 0) -> str:
        self.sub_flow_result(index)
        return f"SubFlow:{self.flow_id}-{self.step_execution_id}-{index}"

    def is_cancellation_requested(self) -> bool:
        return not self._is_active()

    def set_step_execution_local(self, key: str, value: object) -> None:
        require_name(key)
        self.local_writes[key] = pb.KV(
            key=key,
            value=self._values.encode_dynamic(value),
        )

    def get_step_execution_local(
        self,
        key: str,
        value_type: type[ValueT],
    ) -> ValueT | None:
        require_name(key)
        entry = self.local_writes.get(key)
        value = entry.value if entry is not None else self._locals.get(key)
        if value is None:
            return None
        return cast(
            ValueT,
            self._values.decode(value, self._flow_codec(value_type)),
        )

    def record_event(self, name: str, value: object) -> None:
        require_name(name)
        if name in self._event_names:
            raise ValueError(f"event was already recorded: {name}")
        self._event_names.add(name)
        self.events.append(pb.KV(key=name, value=self._values.encode_dynamic(value)))

    def _write_stream(
        self,
        definition: Stream[ValueT],
        value: ValueT,
    ) -> StepOutput | None:
        if self._method not in (
            InvocationMethod.WAIT_FOR,
            InvocationMethod.EXECUTE,
            InvocationMethod.TIMEOUT,
        ):
            raise ValueError("Stream writes require a Step Context")
        self._require_registered(definition)
        output = _StreamStepOutput(
            pb.StepStreamWrite(
                stream_name=definition.name,
                stream_capacity_bytes=definition.stream_capacity_bytes,
                value=self._values.encode(
                    value,
                    self._values.codec(definition.value_type),
                ),
            )
        )
        if self._output_emitter is None:
            return output
        self._output_emitter.emit_nowait(output)
        return None

    def _prepare_buffered_stream(self, definition: Stream[object]) -> bool:
        if self._method not in (
            InvocationMethod.WAIT_FOR,
            InvocationMethod.EXECUTE,
            InvocationMethod.TIMEOUT,
        ):
            raise ValueError("Buffered Streams require a Step Context")
        self._require_registered(definition)
        return self._output_emitter is not None

    def _register_step_output_finalizer(
        self,
        finalizer: _StepOutputFinalizer,
    ) -> None:
        if self._step_outputs_finalized or self._output_emitter is None:
            raise ValueError(
                "Buffered Stream finalizers require an active async Step Context"
            )
        self._step_output_finalizers.append(finalizer)
        self._output_emitter.add_close_callback(finalizer._cancel_step_output)

    def _finalize_step_outputs(
        self,
        failure: BaseException | None = None,
    ) -> BaseException | None:
        if self._step_outputs_finalized:
            return failure
        self._step_outputs_finalized = True
        failures: list[BaseException] = [] if failure is None else [failure]
        for finalizer in self._step_output_finalizers:
            try:
                finalizer._finalize_step_output()
            except BaseException as error:
                failures.append(error)
        if not failures:
            return None
        if len(failures) == 1:
            return failures[0]
        return BaseExceptionGroup(
            "Step handler and buffered Stream finalization failed", failures
        )

    def _get_attribute(
        self,
        definition: Attribute[ValueT] | AttributeMap[ValueT],
        instance: str | None,
    ) -> ValueT:
        self._require_registered(definition)
        self._require_attribute_map_instance_loaded(definition, instance)
        key = self._physical_name(definition, instance)
        write = self.attribute_writes.get(key)
        if write is not None:
            if write.value.WhichOneof("kind") == "null_value":
                return cast(ValueT, self._default_value(definition.value_type))
            value: pb.Value | None = write.value
        else:
            value = self._attributes.get(key)
        if value is None:
            return cast(ValueT, self._default_value(definition.value_type))
        codec = self._flow_codec(definition.value_type)
        return cast(ValueT, self._values.decode(value, codec))

    def _set_attribute(
        self,
        definition: Attribute[ValueT] | AttributeMap[ValueT],
        instance: str | None,
        value: ValueT,
    ) -> None:
        self._require_registered(definition)
        key = self._physical_name(definition, instance)
        write = pb.AttributeWrite(
            key=key,
            value=self._values.encode(value, self._flow_codec(definition.value_type)),
        )
        index = self._values.index_config(
            definition.index,
            isinstance(definition, AttributeMap),
        )
        if index is not None:
            write.index_config.CopyFrom(index)
        _apply_attribute_store_sync(write, definition)
        _apply_hard_delete_on_null(write, definition)
        self.attribute_writes[key] = write

    def _delete_attribute(
        self,
        definition: Attribute[object] | AttributeMap[object],
        instance: str | None,
    ) -> None:
        self._require_registered(definition)
        key = self._physical_name(definition, instance)
        write = pb.AttributeWrite(key=key, value=self._values.deletion())
        index = self._values.index_config(
            definition.index,
            isinstance(definition, AttributeMap),
        )
        if index is not None:
            write.index_config.CopyFrom(index)
        _apply_attribute_store_sync(write, definition)
        _apply_hard_delete_on_null(write, definition)
        self.attribute_writes[key] = write

    def _publish_channel(
        self,
        definition: Channel[ValueT] | ChannelMap[ValueT],
        instance: str | None,
        value: ValueT,
    ) -> None:
        self._require_registered(definition)
        name = self._physical_name(definition, instance)
        self.publications.append(
            pb.ChannelMessage(
                channel_name=name,
                value=self._values.encode(
                    value,
                    self._flow_codec(definition.value_type),
                ),
            )
        )
        current = self._channel_infos.get(name)
        self._channel_infos[name] = pb.ChannelInfo(
            size=(current.size if current is not None else 0) + 1
        )

    def _delete_channel_message(
        self,
        definition: Channel[object] | ChannelMap[object],
        instance: str | None,
        message_id: str,
    ) -> None:
        self._require_registered(definition)
        name = self._physical_name(definition, instance)
        self.channel_deletions.append(
            pb.ChannelMessageDeletion(
                channel_name=name,
                message_id=require_name(message_id),
            )
        )
        current = self._channel_infos.get(name)
        if current is not None and current.size > 0:
            self._channel_infos[name] = pb.ChannelInfo(size=current.size - 1)

    def _attribute_map_keys(
        self,
        definition: AttributeMap[object],
    ) -> tuple[str, ...]:
        self._require_registered(definition)
        self._require_attribute_map_all_loaded(definition)
        prefix = f"{definition.name}/"
        physical_keys = {key for key in self._attributes if key.startswith(prefix)}
        for key, write in self.attribute_writes.items():
            if not key.startswith(prefix):
                continue
            if write.value.WhichOneof("kind") == "null_value":
                physical_keys.discard(key)
            else:
                physical_keys.add(key)
        return tuple(
            sorted(
                require_map_instance(unquote(key[len(prefix) :]))
                for key in physical_keys
            )
        )

    def _channel_map_keys(
        self,
        definition: ChannelMap[object],
    ) -> tuple[str, ...]:
        self._require_registered(definition)
        prefix = f"{definition.name}/"
        return tuple(
            sorted(
                require_map_instance(unquote(key[len(prefix) :]))
                for key, info in self._channel_infos.items()
                if key.startswith(prefix) and info.size > 0
            )
        )

    def _channel_size(
        self,
        definition: Channel[object] | ChannelMap[object],
        instance: str | None,
    ) -> int:
        self._require_registered(definition)
        info = self._channel_infos.get(self._physical_name(definition, instance))
        return info.size if info is not None else 0

    def _pending_channel_messages(
        self,
        definition: Channel[ValueT] | ChannelMap[ValueT],
        instance: str | None,
    ) -> tuple[ChannelMessage[ValueT], ...]:
        self._require_registered(definition)
        if isinstance(definition, ChannelMap):
            channel_name = self._physical_name(definition, instance)
            is_loaded = (
                f"{definition.name}/" in self._loaded_channel_map_instances
                or channel_name in self._loaded_channel_map_instances
            )
        else:
            channel_name = self._physical_name(definition, instance)
            is_loaded = definition.name in self._loaded_channel_names
        if not is_loaded:
            raise ChannelMessagesNotLoadedError(
                f"Channel messages were not loaded for this invocation: {definition.name}"
            )
        values = self._loaded_channel_messages.get(channel_name)
        if values is None:
            return ()
        codec = self._flow_codec(definition.value_type)
        return tuple(
            ChannelMessage(
                message_id=message.message_id,
                value=cast(ValueT, self._values.decode(message.value, codec)),
            )
            for message in values.messages
        )

    def _channel_results(
        self,
        definition: Channel[ValueT] | ChannelMap[ValueT],
        instance: str | None,
    ) -> Sequence[ValueT]:
        self._require_registered(definition)
        if self._condition_results is None:
            return ()
        name = self._physical_name(definition, instance)
        codec = self._flow_codec(definition.value_type)
        return tuple(
            cast(ValueT, self._values.decode(value, codec))
            for result in self._condition_results.channel_results
            if result.channel_name == name
            and result.condition_status == pb.CONDITION_STATUS_COMPLETED
            for value in result.values
        )

    def _flow_codec(self, value_type: object) -> Codec[Any]:
        return self._values.codec(value_type)

    def _require_attribute_map_instance_loaded(
        self,
        definition: Attribute[Any] | AttributeMap[Any],
        instance: str | None,
    ) -> None:
        if not isinstance(definition, AttributeMap):
            return
        physical_name = self._physical_name(definition, instance)
        if (
            f"{definition.name}/" not in self._loaded_attribute_map_instances
            and physical_name not in self._loaded_attribute_map_instances
        ):
            raise AttributeMapNotLoadedError(
                f"AttributeMap instance was not loaded for this invocation: {physical_name}"
            )

    def _require_attribute_map_all_loaded(
        self,
        definition: AttributeMap[object],
    ) -> None:
        if f"{definition.name}/" not in self._loaded_attribute_map_instances:
            raise AttributeMapNotLoadedError(
                f"all AttributeMap instances were not loaded for this invocation: {definition.name}"
            )

    def _require_registered(self, definition: _Definition) -> None:
        registered = self._flow.persistence.get(definition.name)
        if registered is not definition:
            raise ValueError(
                f"persistence definition does not belong to Flow: {definition.name}"
            )

    @staticmethod
    def _physical_name(definition: _Definition, instance: str | None) -> str:
        if isinstance(definition, (AttributeMap, ChannelMap)):
            if instance is None:
                raise ValueError("dynamic definition requires an instance")
            return Registry.physical_name(definition.name, instance)
        if instance is not None:
            raise ValueError("static definition cannot use an instance")
        return definition.name

    @staticmethod
    def _map_values(kind: str, entries: Sequence[pb.KV]) -> dict[str, pb.Value]:
        values: dict[str, pb.Value] = {}
        for entry in entries:
            if not entry.key or not entry.HasField("value") or entry.key in values:
                raise ValueError(f"invalid or duplicate {kind}")
            values[entry.key] = entry.value
        return values

    @staticmethod
    def _default_value(value_type: object) -> object | None:
        if value_type is bool:
            return False
        if value_type is int:
            return 0
        if value_type is float:
            return 0.0
        return None


def _always_active() -> bool:
    return True
