# Legacy Materials in this file remain under their original licenses.
# See LEGACY_NOTICES.md.

# Modifications Copyright (c) 2026 Super Durable, Inc.
#
# Modifications after the Legacy Cutoff are licensed under the
# Sustainable Use License 1.0.
# Legacy Materials remain under their original licenses.
# See LICENSE and LEGACY_NOTICES.md.

from __future__ import annotations

from abc import ABC
from collections.abc import Generator as GeneratorABC
from dataclasses import dataclass, replace
from datetime import timedelta
from inspect import (
    isasyncgenfunction,
    iscoroutinefunction,
    isgeneratorfunction,
    signature,
)
from types import MappingProxyType
from typing import (
    Any,
    Callable,
    Generic,
    Sequence,
    TypeVar,
    get_args,
    get_origin,
    get_type_hints,
    overload,
)
from urllib.parse import quote

from dex._utils import require_map_instance, require_name
from dex.attribute import Attribute, AttributeLock, AttributeMap, AttributeMapLoad
from dex.channel import Channel, ChannelMap, ChannelMapLoad
from dex.codec import Codec, CodecRegistry, WireKind
from dex.context import AsyncContext, Context
from dex.dexpb import dex_pb2 as pb
from dex.runtime_errors import FlowDefinitionError
from dex.rpc_invoke_options import RPCInvokeOptions
from dex.step import Step, StepDecision, StepList, StepMovement, StepOutput, _StepDef
from dex.stream import Stream
from dex.wait import Wait

OutputT = TypeVar("OutputT")
StartT = TypeVar("StartT")
CallableT = TypeVar("CallableT", bound=Callable[..., Any])
_PersistenceDefinition = (
    Attribute[Any] | AttributeMap[Any] | Channel[Any] | ChannelMap[Any] | Stream[Any]
)


@dataclass(frozen=True)
class _RPCOptions:
    name: str | None
    timeout: timedelta | None
    lock_attributes: tuple[AttributeLock, ...]
    is_transactional: bool
    load_attribute_maps: tuple[AttributeMap[Any], ...]
    load_attribute_map_instances: tuple[AttributeMapLoad, ...]
    load_channels: tuple[Channel[Any], ...]
    load_channel_maps: tuple[ChannelMap[Any], ...]
    load_channel_map_instances: tuple[ChannelMapLoad, ...]


@overload
def rpc(handler: CallableT) -> CallableT: ...


@overload
def rpc(
    *,
    name: str | None = None,
    timeout: timedelta | None = None,
    lock_attributes: Sequence[AttributeLock] = (),
    is_transactional: bool = False,
    load_attribute_maps: Sequence[AttributeMap[Any]] = (),
    load_attribute_map_instances: Sequence[AttributeMapLoad] = (),
    load_channels: Sequence[Channel[Any]] = (),
    load_channel_maps: Sequence[ChannelMap[Any]] = (),
    load_channel_map_instances: Sequence[ChannelMapLoad] = (),
) -> Callable[[CallableT], CallableT]: ...


def rpc(
    handler: CallableT | None = None,
    *,
    name: str | None = None,
    timeout: timedelta | None = None,
    lock_attributes: Sequence[AttributeLock] = (),
    is_transactional: bool = False,
    load_attribute_maps: Sequence[AttributeMap[Any]] = (),
    load_attribute_map_instances: Sequence[AttributeMapLoad] = (),
    load_channels: Sequence[Channel[Any]] = (),
    load_channel_maps: Sequence[ChannelMap[Any]] = (),
    load_channel_map_instances: Sequence[ChannelMapLoad] = (),
) -> CallableT | Callable[[CallableT], CallableT]:
    """Mark a Flow method as a typed RPC handler.

    Use ``@rpc`` with defaults or call ``@rpc(...)`` with configuration. A handler
    receives Context plus zero or one typed input and returns ``RPCResult[T]`` or
    ``None``. Registry construction validates annotations and Attribute locks.

    Args:
        handler: The decorated bound-method function when used as ``@rpc``.
        name: Optional protocol RPC name; defaults to the method name.
        timeout: Optional non-negative handler timeout.
        lock_attributes: Attribute locks held for the entire handler invocation.
        is_transactional: Request transactional reads and writes even without locks.
        load_attribute_maps: AttributeMaps whose current instances are loaded.
        load_attribute_map_instances: Exact instances created by ``AttributeMap.load``.
        load_channels: Channels whose pending messages are loaded.
        load_channel_maps: ChannelMaps whose current instance messages are loaded.
        load_channel_map_instances: Exact instances created by
            ``ChannelMap.load_messages``.

    Returns:
        The original handler, or a decorator that returns it after attaching options.

    Raises:
        ValueError: If ``name`` is empty or ``timeout`` is negative.

    Examples:
        >>> class Orders(Flow[None]):
        ...     @rpc(timeout=timedelta(seconds=10), lock_attributes=(status.lock(),))
        ...     def cancel(self, context: Context, reason: str) -> RPCResult[bool]:
        ...         status.set(context, "canceled")
        ...         return RPCResult(True)
    """
    if name is not None:
        require_name(name)
    if timeout is not None and timeout < timedelta(0):
        raise ValueError("RPC timeout must not be negative")

    def decorate(handler: CallableT) -> CallableT:
        setattr(
            handler,
            "__dex_rpc_options__",
            _RPCOptions(
                name,
                timeout,
                tuple(lock_attributes),
                is_transactional,
                tuple(load_attribute_maps),
                tuple(load_attribute_map_instances),
                tuple(load_channels),
                tuple(load_channel_maps),
                tuple(load_channel_map_instances),
            ),
        )
        return handler

    if handler is not None:
        return decorate(handler)
    return decorate


@dataclass(frozen=True)
class RPCResult(Generic[OutputT]):
    """Return typed RPC output, next-Step movements, and Step cancellation.

    Attributes:
        output: The value decoded into the Client caller's expected output type.
        next_steps: Step movements applied atomically with handler persistence writes.
        canceling_steps: Flow-wide Step types canceled before next Steps are queued.
    """

    output: OutputT
    next_steps: tuple[StepMovement[Any], ...] = ()
    canceling_steps: tuple[type[Step[Any]], ...] = ()

    def with_canceling_steps(self, *steps: type[Step[Any]]) -> RPCResult[OutputT]:
        """Return a result canceling all current executions of selected Step types.

        Dex resolves the selection after RPC persistence commits and before ``next_steps``.
        Finished, already-canceled, and absent executions are no-ops. RPCs cannot select
        siblings because an RPC has no Step execution lineage.

        Args:
            *steps: Step classes registered with the current Flow.

        Returns:
            A new RPCResult containing the combined cancellation selectors.
        """
        combined = list(self.canceling_steps)
        for step in steps:
            if all(step is not current for current in combined):
                combined.append(step)
        return replace(self, canceling_steps=tuple(combined))


@dataclass(frozen=True)
class PersistenceSchema:
    """Declare the Attributes, Channels, and Streams owned by a Flow type.

    Definitions must have unique names and are immutable after Registry creation.

    Attributes:
        attributes: Singleton and map Attribute definitions.
        channels: Singleton and map Channel definitions.
        streams: Best-effort Stream definitions.

    Examples:
        >>> schema = PersistenceSchema.of(status, balances, approvals, events)
    """

    attributes: tuple[Attribute[Any] | AttributeMap[Any], ...] = ()
    channels: tuple[Channel[Any] | ChannelMap[Any], ...] = ()
    streams: tuple[Stream[Any], ...] = ()

    @staticmethod
    def of(*definitions: _PersistenceDefinition) -> PersistenceSchema:
        """Partition persistence definitions into Attributes, Channels, and Streams.

        Args:
            *definitions: Attribute, AttributeMap, Channel, ChannelMap, or Stream definitions.

        Returns:
            A schema with each category kept in argument order.

        Raises:
            TypeError: If a value is not a supported persistence definition.
        """
        attributes: list[Attribute[Any] | AttributeMap[Any]] = []
        channels: list[Channel[Any] | ChannelMap[Any]] = []
        streams: list[Stream[Any]] = []
        for definition in definitions:
            if isinstance(definition, (Attribute, AttributeMap)):
                attributes.append(definition)
            elif isinstance(definition, (Channel, ChannelMap)):
                channels.append(definition)
            elif isinstance(definition, Stream):
                streams.append(definition)
            else:
                raise TypeError("unsupported persistence definition")
        return PersistenceSchema(tuple(attributes), tuple(channels), tuple(streams))


class Flow(Generic[StartT], ABC):
    """Define a durable application Flow and its registered API surface.

    Subclasses return their Steps and persistence schema and may expose methods
    decorated with :func:`rpc`. Construct one Flow instance and pass it to Registry;
    Client calls must use that same registered instance.
    """

    def get_flow_type(self) -> str:
        """Return the protocol Flow type used by Client and Dex.

        Override it to decouple the type from the Python class name.

        Returns:
            A non-empty Flow type unique within the Registry.
        """
        return type(self).__name__

    def get_steps(self) -> StepList[StartT]:
        """Return the Step definitions for this Flow.

        Returns:
            A StepList with zero or one starting Step and all reachable Steps.
        """
        return StepList.empty()

    def get_persistence_schema(self) -> PersistenceSchema:
        """Return the Attributes and Channels owned by this Flow.

        Returns:
            The persistence schema; the default is empty.
        """
        return PersistenceSchema.of()

    def handle_timeout(self, context: Context) -> StepDecision:
        """Handle expiration of this Flow's durable soft-timeout timer.

        Override this method to make a positive timeout default to ``HANDLER``.
        Async Flow implementations may use ``async def`` with the same annotations.
        The returned StepDecision uses ordinary Execute validation and may transition,
        dead-end, complete, fail, or request graceful completion.

        Args:
            context: The timeout-handler invocation Context; do not retain it.

        Returns:
            The decision applied after the timeout timer completes or is skipped.

        Raises:
            NotImplementedError: If called without an application override.
        """
        raise NotImplementedError("Flow has no timeout handler")


@dataclass(frozen=True)
class _RegisteredStep:
    name: str
    step: Step[Any]
    input_type: object
    input_codec: Codec[Any]
    starting: bool
    skips_wait_for: bool


@dataclass(frozen=True)
class _RegisteredRPC:
    name: str
    method: Callable[..., Any]
    options: _RPCOptions
    input_codec: Codec[Any] | None
    output_codec: Codec[Any] | None
    locks: tuple[str, ...]
    load_attribute_map_instances: tuple[str, ...]
    load_channel_names: tuple[str, ...]
    load_channel_map_instances: tuple[str, ...]


@dataclass(frozen=True)
class _RegisteredFlow:
    name: str
    flow: Flow[Any]
    has_timeout_handler: bool
    steps: MappingProxyType[str, _RegisteredStep]
    steps_by_class: MappingProxyType[type[Step[Any]], _RegisteredStep]
    start_step: _RegisteredStep | None
    rpcs: MappingProxyType[str, _RegisteredRPC]
    persistence: MappingProxyType[str, _PersistenceDefinition]

    def step(self, name: str) -> _RegisteredStep:
        try:
            return self.steps[name]
        except KeyError as error:
            raise FlowDefinitionError(
                f"Flow {self.name} Step is not registered: {name}"
            ) from error

    def step_class(self, step_class: type[Step[Any]]) -> _RegisteredStep:
        try:
            return self.steps_by_class[step_class]
        except KeyError as error:
            raise FlowDefinitionError(
                f"Flow {self.name} Step class is not registered: "
                f"{step_class.__qualname__}"
            ) from error

    def rpc(self, name: str) -> _RegisteredRPC:
        try:
            return self.rpcs[name]
        except KeyError as error:
            raise FlowDefinitionError(
                f"Flow {self.name} RPC is not registered: {name}"
            ) from error


@dataclass(frozen=True)
class Registry:
    """Validate and store Flow definitions shared by Client and Worker.

    Registry construction inspects Step and RPC signatures, resolves codecs, checks
    names and locks, and assembles Attribute indexes atomically. The result is safe
    for concurrent reads.

    Attributes:
        flows: The registered Flow instances in input order.
        codec_registry: The codec registry used for every handler and persistence type.

    Examples:
        >>> orders = Orders()
        >>> registry = Registry([orders], CodecRegistry({Money: money_codec}))
    """

    flows: tuple[Flow[Any], ...]
    codec_registry: CodecRegistry
    _steps: tuple[_RegisteredStep, ...]
    _rpcs: tuple[_RegisteredRPC, ...]
    _registered_flows: MappingProxyType[str, _RegisteredFlow]
    _stream_flows: MappingProxyType[int, _RegisteredFlow]
    _attribute_indexes: MappingProxyType[str, pb.IndexType]

    def __init__(
        self,
        flows: Sequence[Flow[Any]],
        codec_registry: CodecRegistry | None = None,
        *,
        allow_async_handlers: bool = False,
    ) -> None:
        """Validate and assemble a set of Flow definitions.

        Args:
            flows: Flow instances with unique Flow types.
            codec_registry: Optional custom codecs; defaults to ``CodecRegistry()``.
            allow_async_handlers: Accept ``async def`` Step and RPC handlers. Async
                Steps annotate AsyncContext; async RPCs and timeout handlers keep
                Context. Sync Client/Worker construction leaves this ``False``.

        Raises:
            FlowDefinitionError: If any Flow, Step, RPC, persistence definition,
                annotation, codec, lock, or index is invalid.
        """
        immutable_flows = tuple(flows)
        resolved_codecs = codec_registry or CodecRegistry()
        try:
            attribute_indexes: dict[str, pb.IndexType] = {}
            registered_flows = self._assemble(
                immutable_flows,
                resolved_codecs,
                attribute_indexes,
                allow_async_handlers=allow_async_handlers,
            )
        except FlowDefinitionError:
            raise
        except (TypeError, ValueError) as error:
            raise FlowDefinitionError(str(error)) from error
        registered_steps = tuple(
            step
            for registered_flow in registered_flows.values()
            for step in registered_flow.steps.values()
        )
        registered_rpcs = tuple(
            registered_rpc
            for registered_flow in registered_flows.values()
            for registered_rpc in registered_flow.rpcs.values()
        )
        object.__setattr__(self, "flows", immutable_flows)
        object.__setattr__(self, "codec_registry", resolved_codecs)
        object.__setattr__(self, "_steps", registered_steps)
        object.__setattr__(self, "_rpcs", registered_rpcs)
        object.__setattr__(
            self,
            "_registered_flows",
            MappingProxyType(registered_flows),
        )
        stream_flows: dict[int, _RegisteredFlow] = {}
        for registered_flow in registered_flows.values():
            for definition in registered_flow.persistence.values():
                if not isinstance(definition, Stream):
                    continue
                stream = definition
                if id(stream) in stream_flows:
                    raise FlowDefinitionError(
                        f"Stream {stream.name} is registered by multiple Flows"
                    )
                stream_flows[id(stream)] = registered_flow
        object.__setattr__(self, "_stream_flows", MappingProxyType(stream_flows))
        object.__setattr__(
            self,
            "_attribute_indexes",
            MappingProxyType(attribute_indexes),
        )

    @staticmethod
    def _assemble(
        flows: tuple[Flow[Any], ...],
        codec_registry: CodecRegistry,
        attribute_indexes: dict[str, pb.IndexType],
        *,
        allow_async_handlers: bool,
    ) -> dict[str, _RegisteredFlow]:
        registered_flows: dict[str, _RegisteredFlow] = {}
        for flow in flows:
            flow_name: str | None = None
            try:
                if not isinstance(flow, Flow):
                    raise TypeError("Flow definition is invalid")
                flow_name = flow.get_flow_type()
                require_name(flow_name)
                if flow_name in registered_flows:
                    raise ValueError(f"duplicate Flow {flow_name}")
                registered_flows[flow_name] = Registry._assemble_flow(
                    flow_name,
                    flow,
                    codec_registry,
                    attribute_indexes,
                    allow_async_handlers=allow_async_handlers,
                )
            except FlowDefinitionError:
                raise
            except Exception as error:
                context = (
                    "Flow registration"
                    if flow_name is None
                    else f"Flow {flow_name} registration"
                )
                raise FlowDefinitionError(f"{context} failed: {error}") from error
        return registered_flows

    @staticmethod
    def _assemble_flow(
        flow_name: str,
        flow: Flow[Any],
        codec_registry: CodecRegistry,
        attribute_indexes: dict[str, pb.IndexType],
        *,
        allow_async_handlers: bool,
    ) -> _RegisteredFlow:
        definitions = flow.get_steps()
        if not isinstance(definitions, StepList):
            raise TypeError("Flow steps must be a StepList")
        registered_steps: dict[str, _RegisteredStep] = {}
        registered_steps_by_class: dict[type[Step[Any]], _RegisteredStep] = {}
        start_step: _RegisteredStep | None = None
        for definition in definitions:
            if not isinstance(definition, _StepDef):
                raise TypeError("Flow StepList contains an invalid definition")
            if definition.is_start_step:
                if start_step is not None:
                    raise ValueError("Flow must not have multiple start Steps")
            step = definition.step
            step_name = step.get_step_type()
            require_name(step_name)
            if step_name in registered_steps:
                raise ValueError(f"duplicate Step {step_name}")
            step_class = type(step)
            if step_class in registered_steps_by_class:
                raise ValueError(f"duplicate Step class {step_class.__qualname__}")
            input_type = Registry._step_handler_input_type(
                step,
                "execute",
                step.execute,
                StepDecision,
                allow_async_handlers=allow_async_handlers,
            )
            registered_step = _RegisteredStep(
                step_name,
                step,
                input_type,
                Registry._step_input_codec(
                    step,
                    codec_registry,
                    allow_async_handlers=allow_async_handlers,
                ),
                definition.is_start_step,
                type(step).wait_for is Step.wait_for,
            )
            registered_steps[step_name] = registered_step
            registered_steps_by_class[step_class] = registered_step
            if definition.is_start_step:
                start_step = registered_step

        schema = flow.get_persistence_schema()
        if not isinstance(schema, PersistenceSchema):
            raise TypeError("Flow persistence schema must be a PersistenceSchema")
        persistence = Registry._assemble_persistence(schema, attribute_indexes)
        Registry._validate_hard_delete_attribute_codecs(schema, codec_registry)
        registered_rpcs: dict[str, _RegisteredRPC] = {}
        for attribute_name in dir(flow):
            method = getattr(flow, attribute_name)
            function = getattr(method, "__func__", method)
            options = getattr(function, "__dex_rpc_options__", None)
            if not isinstance(options, _RPCOptions):
                continue
            rpc_name = options.name or attribute_name
            require_name(rpc_name)
            if rpc_name in registered_rpcs:
                raise ValueError(f"duplicate RPC {rpc_name}")
            Registry._validate_rpc_locks(rpc_name, options, schema)
            state_loads = Registry._validate_rpc_state_loads(rpc_name, options, schema)
            input_codec, output_codec = Registry._rpc_codecs(
                method,
                codec_registry,
                allow_async_handlers=allow_async_handlers,
            )
            registered_rpcs[rpc_name] = _RegisteredRPC(
                rpc_name,
                method,
                options,
                input_codec,
                output_codec,
                tuple(
                    Registry._physical_lock(lock) for lock in options.lock_attributes
                ),
                *state_loads,
            )
        return _RegisteredFlow(
            flow_name,
            flow,
            Registry._validate_timeout_handler(
                flow, allow_async_handlers=allow_async_handlers
            ),
            MappingProxyType(registered_steps),
            MappingProxyType(registered_steps_by_class),
            start_step,
            MappingProxyType(registered_rpcs),
            MappingProxyType(persistence),
        )

    @staticmethod
    def _validate_timeout_handler(
        flow: Flow[Any],
        *,
        allow_async_handlers: bool,
    ) -> bool:
        if type(flow).handle_timeout is Flow.handle_timeout:
            return False
        handler = flow.handle_timeout
        if isasyncgenfunction(handler):
            raise TypeError("Flow handle_timeout must not be an async generator")
        if iscoroutinefunction(handler) and not allow_async_handlers:
            raise TypeError("Flow handle_timeout must be synchronous")
        parameters = tuple(signature(handler).parameters.values())
        hints = get_type_hints(handler)
        expected_context = AsyncContext if iscoroutinefunction(handler) else Context
        if (
            len(parameters) != 1
            or hints.get(parameters[0].name) is not expected_context
        ):
            raise TypeError(
                f"Flow handle_timeout must accept {expected_context.__name__}"
            )
        annotated_return = hints.get("return")
        if isgeneratorfunction(handler):
            arguments = get_args(annotated_return)
            if (
                get_origin(annotated_return) is not GeneratorABC
                or len(arguments) != 3
                or arguments[0] is not StepOutput
                or arguments[1] not in (None, type(None))
                or arguments[2] is not StepDecision
            ):
                raise TypeError(
                    "Flow handle_timeout generator must return "
                    "Generator[StepOutput, None, StepDecision]"
                )
        elif annotated_return is not StepDecision:
            raise TypeError("Flow handle_timeout must return StepDecision")
        return True

    @staticmethod
    def _assemble_persistence(
        schema: PersistenceSchema,
        attribute_indexes: dict[str, pb.IndexType],
    ) -> dict[str, _PersistenceDefinition]:
        persistence: dict[str, _PersistenceDefinition] = {}
        for definition in (*schema.attributes, *schema.channels, *schema.streams):
            if definition.name in persistence:
                raise ValueError(f"duplicate persistence definition {definition.name}")
            persistence[definition.name] = definition
            Registry._register_attribute_index(definition, attribute_indexes)
        return persistence

    @staticmethod
    def _register_attribute_index(
        definition: _PersistenceDefinition,
        attribute_indexes: dict[str, pb.IndexType],
    ) -> None:
        if not isinstance(definition, (Attribute, AttributeMap)):
            return
        index = definition.index
        if index is None:
            return
        if isinstance(definition, AttributeMap) and not index.index_key:
            raise ValueError(
                f"indexed AttributeMap {definition.name} requires an index key"
            )
        key = index.index_key or definition.name
        index_type = Registry._map_index_type(index.type)
        existing = attribute_indexes.get(key)
        if existing is not None and existing != index_type:
            raise ValueError(
                f"Attribute index {key} has conflicting types {existing} and {index_type}"
            )
        attribute_indexes[key] = index_type

    @staticmethod
    def _validate_hard_delete_attribute_codecs(
        schema: PersistenceSchema,
        codec_registry: CodecRegistry,
    ) -> None:
        for definition in schema.attributes:
            if not definition.hard_delete_on_null:
                continue
            wire_kind = codec_registry.resolve(definition.value_type).wire_kind
            if wire_kind in (WireKind.BOOL, WireKind.INT64, WireKind.DOUBLE):
                raise TypeError(
                    f"hard-delete Attribute {definition.name} supports only string "
                    f"and object values, not {wire_kind.name} codec values"
                )

    @staticmethod
    def _map_index_type(index_type: Any) -> pb.IndexType:
        from dex.attribute import IndexType

        return {
            IndexType.KEYWORD: pb.INDEX_TYPE_KEYWORD,
            IndexType.FULL_TEXT: pb.INDEX_TYPE_TEXT,
            IndexType.KEYWORD_ARRAY: pb.INDEX_TYPE_KEYWORD_ARRAY,
            IndexType.INT: pb.INDEX_TYPE_INT,
            IndexType.DOUBLE: pb.INDEX_TYPE_DOUBLE,
            IndexType.BOOL: pb.INDEX_TYPE_BOOL,
            IndexType.DATETIME: pb.INDEX_TYPE_DATETIME,
        }[index_type]

    @staticmethod
    def _validate_rpc_locks(
        rpc_name: str,
        options: _RPCOptions,
        schema: PersistenceSchema,
    ) -> None:
        lock_identities: set[tuple[int, str | None]] = set()
        for lock in options.lock_attributes:
            if not isinstance(lock, AttributeLock):
                raise TypeError(f"RPC {rpc_name} has an invalid attribute lock")
            if all(lock.attribute is not attribute for attribute in schema.attributes):
                raise ValueError(f"RPC {rpc_name} locks an unregistered attribute")
            if isinstance(lock.attribute, AttributeMap):
                if lock.instance is None:
                    raise ValueError(
                        f"RPC {rpc_name} attribute-map lock needs an instance"
                    )
                require_map_instance(lock.instance)
            elif lock.instance is not None:
                raise ValueError(
                    f"RPC {rpc_name} attribute lock cannot have an instance"
                )
            identity = (id(lock.attribute), lock.instance)
            if identity in lock_identities:
                raise ValueError(f"RPC {rpc_name} has a duplicate attribute lock")
            lock_identities.add(identity)

    @staticmethod
    def _validate_rpc_state_loads(
        rpc_name: str,
        options: _RPCOptions,
        schema: PersistenceSchema,
    ) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
        attribute_map_instances: list[str] = []
        for attribute_map in options.load_attribute_maps:
            if not isinstance(attribute_map, AttributeMap):
                raise TypeError(f"RPC {rpc_name} has an invalid AttributeMap load")
            Registry._require_registered_rpc_state(
                rpc_name, attribute_map, schema.attributes, "AttributeMap"
            )
            attribute_map_instances.append(f"{attribute_map.name}/")
        for attribute_map_load in options.load_attribute_map_instances:
            if not isinstance(attribute_map_load, AttributeMapLoad):
                raise TypeError(f"RPC {rpc_name} has an invalid AttributeMapLoad load")
            Registry._require_registered_rpc_state(
                rpc_name,
                attribute_map_load.attribute_map,
                schema.attributes,
                "AttributeMap",
            )
            attribute_map_instances.append(attribute_map_load.physical_name)

        channel_names: list[str] = []
        for channel in options.load_channels:
            if not isinstance(channel, Channel):
                raise TypeError(f"RPC {rpc_name} has an invalid Channel load")
            Registry._require_registered_rpc_state(
                rpc_name, channel, schema.channels, "Channel"
            )
            channel_names.append(channel.name)

        channel_map_instances: list[str] = []
        for channel_map in options.load_channel_maps:
            if not isinstance(channel_map, ChannelMap):
                raise TypeError(f"RPC {rpc_name} has an invalid ChannelMap load")
            Registry._require_registered_rpc_state(
                rpc_name, channel_map, schema.channels, "ChannelMap"
            )
            channel_map_instances.append(f"{channel_map.name}/")
        for channel_map_load in options.load_channel_map_instances:
            if not isinstance(channel_map_load, ChannelMapLoad):
                raise TypeError(f"RPC {rpc_name} has an invalid ChannelMapLoad load")
            Registry._require_registered_rpc_state(
                rpc_name,
                channel_map_load.channel_map,
                schema.channels,
                "ChannelMap",
            )
            channel_map_instances.append(channel_map_load.physical_name)

        for kind, instances in (
            ("AttributeMap", attribute_map_instances),
            ("Channel", channel_names),
            ("ChannelMap", channel_map_instances),
        ):
            if len(instances) != len(set(instances)):
                raise ValueError(f"RPC {rpc_name} has a duplicate {kind} load")
        return (
            tuple(sorted(attribute_map_instances)),
            tuple(sorted(channel_names)),
            tuple(sorted(channel_map_instances)),
        )

    @staticmethod
    def _require_registered_rpc_state(
        rpc_name: str,
        selection: object,
        registered: tuple[object, ...],
        kind: str,
    ) -> None:
        if all(selection is not candidate for candidate in registered):
            raise ValueError(f"RPC {rpc_name} loads an unregistered {kind}")

    @staticmethod
    def _step_input_codec(
        step: Step[Any],
        codec_registry: CodecRegistry,
        *,
        allow_async_handlers: bool,
    ) -> Codec[Any]:
        input_type = Registry._step_handler_input_type(
            step,
            "execute",
            step.execute,
            StepDecision,
            allow_async_handlers=allow_async_handlers,
        )
        if type(step).wait_for is not Step.wait_for:
            wait_input_type = Registry._step_handler_input_type(
                step,
                "wait_for",
                step.wait_for,
                Wait,
                allow_async_handlers=allow_async_handlers,
            )
            if wait_input_type != input_type:
                raise TypeError(
                    f"Step {step.get_step_type()} handlers must use the same input type"
                )
        return codec_registry.resolve(input_type)

    @staticmethod
    def _step_handler_input_type(
        step: Step[Any],
        handler_name: str,
        handler: Callable[..., Any],
        return_type: type[Any],
        *,
        allow_async_handlers: bool,
    ) -> Any:
        if isasyncgenfunction(handler):
            raise TypeError(
                f"Step {step.get_step_type()} {handler_name} must not be an async generator"
            )
        if iscoroutinefunction(handler) and not allow_async_handlers:
            raise TypeError(
                f"Step {step.get_step_type()} {handler_name} must be synchronous"
            )
        parameters = tuple(signature(handler).parameters.values())
        hints = get_type_hints(handler)
        if len(parameters) != 2:
            raise TypeError(
                f"Step {step.get_step_type()} {handler_name} must accept context and input"
            )
        context_parameter, input_parameter = parameters
        expected_context = AsyncContext if iscoroutinefunction(handler) else Context
        if hints.get(context_parameter.name) is not expected_context:
            raise TypeError(
                f"Step {step.get_step_type()} {handler_name} context must be "
                f"{expected_context.__name__}"
            )
        input_type = hints.get(input_parameter.name)
        if input_type is None:
            raise TypeError(
                f"Step {step.get_step_type()} {handler_name} input must be annotated"
            )
        annotated_return = hints.get("return")
        if isgeneratorfunction(handler):
            arguments = get_args(annotated_return)
            if (
                get_origin(annotated_return) is not GeneratorABC
                or len(arguments) != 3
                or arguments[0] is not StepOutput
                or arguments[1] not in (None, type(None))
                or arguments[2] is not return_type
            ):
                raise TypeError(
                    f"Step {step.get_step_type()} {handler_name} generator must return "
                    f"Generator[StepOutput, None, {return_type.__name__}]"
                )
        elif annotated_return is not return_type:
            raise TypeError(
                f"Step {step.get_step_type()} {handler_name} must return "
                f"{return_type.__name__}"
            )
        return input_type

    @staticmethod
    def _rpc_codecs(
        method: Callable[..., Any],
        codec_registry: CodecRegistry,
        *,
        allow_async_handlers: bool,
    ) -> tuple[Codec[Any] | None, Codec[Any] | None]:
        if iscoroutinefunction(method) and not allow_async_handlers:
            raise TypeError("RPC must be synchronous")
        parameters = tuple(signature(method).parameters.values())
        hints = get_type_hints(method)
        if len(parameters) not in (1, 2) or "return" not in hints:
            raise TypeError("RPC must annotate Context, optional input, and return")
        if hints.get(parameters[0].name) is not Context:
            raise TypeError("RPC context must be Context")
        if len(parameters) == 2 and parameters[1].name not in hints:
            raise TypeError("RPC input must be annotated")
        input_codec = (
            codec_registry.resolve(hints[parameters[1].name])
            if len(parameters) == 2
            else None
        )
        return_type = hints["return"]
        output_codec = None
        if get_origin(return_type) is RPCResult:
            arguments = get_args(return_type)
            if len(arguments) != 1:
                raise TypeError("RPCResult must declare one output type")
            output_codec = codec_registry.resolve(arguments[0])
        elif return_type not in (None, type(None)):
            raise TypeError("RPC must return RPCResult[O] or None")
        return input_codec, output_codec

    @staticmethod
    def physical_name(name: str, instance: str) -> str:
        """Return the physical name for a map instance.

        Args:
            name: The AttributeMap or ChannelMap name.
            instance: The map instance. Slash is prohibited because it is a reserved character.

        Returns:
            The physical name.

        Raises:
            ValueError: If ``instance`` is empty or contains ``/``.
        """
        require_map_instance(instance)
        return f"{name}/{quote(instance, safe='')}"

    @staticmethod
    def _physical_lock(lock: AttributeLock) -> str:
        if lock.instance is None:
            return lock.attribute.name
        return Registry.physical_name(lock.attribute.name, lock.instance)

    def _flow_by_type(self, flow_type: str) -> _RegisteredFlow:
        try:
            return self._registered_flows[flow_type]
        except KeyError as error:
            raise FlowDefinitionError(f"Flow is not registered: {flow_type}") from error

    def _flow_for_instance(self, flow: Flow[Any]) -> _RegisteredFlow:
        registered = self._flow_by_type(flow.get_flow_type())
        if registered.flow is not flow:
            raise FlowDefinitionError(
                f"Flow instance is not registered: {flow.get_flow_type()}"
            )
        return registered

    def _flow_for_stream(self, stream: Stream[Any]) -> _RegisteredFlow:
        """Return the Flow owning an exact registered Stream definition.

        Args:
            stream: Stream object registered in one Flow schema.

        Returns:
            The owning registered Flow.

        Raises:
            FlowDefinitionError: If the exact Stream object is not registered.
        """
        try:
            return self._stream_flows[id(stream)]
        except KeyError as error:
            raise FlowDefinitionError(
                f"Stream is not registered: {stream.name}"
            ) from error

    def _rpc_for_method(
        self,
        method: Callable[..., Any],
    ) -> tuple[_RegisteredFlow, _RegisteredRPC]:
        receiver = getattr(method, "__self__", None)
        function = getattr(method, "__func__", method)
        for flow in self._registered_flows.values():
            if receiver is not flow.flow:
                continue
            for registered_rpc in flow.rpcs.values():
                registered_function = getattr(
                    registered_rpc.method,
                    "__func__",
                    registered_rpc.method,
                )
                if registered_function is function:
                    return flow, registered_rpc
        raise FlowDefinitionError("RPC method is not registered")

    def _resolve_rpc_invoke_options(
        self,
        flow: _RegisteredFlow,
        options: RPCInvokeOptions,
    ) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
        lock_attribute_map_instances: list[str] = []
        for lock in options.lock_attribute_map_instances:
            if not isinstance(lock, AttributeLock) or not isinstance(
                lock.attribute, AttributeMap
            ):
                raise FlowDefinitionError(
                    "RPCInvokeOptions lock must target an AttributeMap instance"
                )
            if lock.instance is None:
                raise FlowDefinitionError(
                    "RPCInvokeOptions AttributeMap lock needs an instance"
                )
            self._require_rpc_invoke_definition(flow, lock.attribute, "AttributeMap")
            lock_attribute_map_instances.append(
                lock.attribute.load(lock.instance).physical_name
            )

        load_attribute_map_instances: list[str] = []
        for attribute_map_load in options.load_attribute_map_instances:
            if not isinstance(attribute_map_load, AttributeMapLoad):
                raise FlowDefinitionError(
                    "RPCInvokeOptions has an invalid AttributeMap load"
                )
            self._require_rpc_invoke_definition(
                flow, attribute_map_load.attribute_map, "AttributeMap"
            )
            load_attribute_map_instances.append(attribute_map_load.physical_name)

        load_channel_map_instances: list[str] = []
        for channel_map_load in options.load_channel_map_instances:
            if not isinstance(channel_map_load, ChannelMapLoad):
                raise FlowDefinitionError(
                    "RPCInvokeOptions has an invalid ChannelMap load"
                )
            self._require_rpc_invoke_definition(
                flow, channel_map_load.channel_map, "ChannelMap"
            )
            load_channel_map_instances.append(channel_map_load.physical_name)

        return (
            tuple(sorted(set(lock_attribute_map_instances))),
            tuple(sorted(set(load_attribute_map_instances))),
            tuple(sorted(set(load_channel_map_instances))),
        )

    @staticmethod
    def _require_rpc_invoke_definition(
        flow: _RegisteredFlow,
        definition: object,
        kind: str,
    ) -> None:
        registered = flow.persistence.get(getattr(definition, "name", ""))
        if registered is not definition:
            raise FlowDefinitionError(
                f"Flow {flow.name} does not register {kind} definition"
            )
