# Legacy Materials in this file remain under their original licenses.
# See LEGACY_NOTICES.md.

# Modifications Copyright (c) 2026 Super Durable, Inc.
#
# Modifications after the Legacy Cutoff are licensed under the
# Sustainable Use License 1.0.
# Legacy Materials remain under their original licenses.
# See LICENSE and LEGACY_NOTICES.md.

import asyncio
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, Generator, cast

import pytest

from dex import (
    INT64,
    STRING,
    AsyncClient,
    AsyncContext,
    Attribute,
    AttributeMap,
    BlobCache,
    BlobCacheConfig,
    Channel,
    ChannelMap,
    Client,
    CodecRegistry,
    ConditionCombination,
    Context,
    Flow,
    FlowConfig,
    FlowDefinitionError,
    FlowTimeoutHandlerOptions,
    FlowTimeoutPolicy,
    InvalidStepResultError,
    JsonCodec,
    PersistenceSchema,
    Registry,
    RPCResult,
    RPCInvokeOptions,
    RetryPolicy,
    StartFlowOptions,
    Step,
    StepDecision,
    StepDurability,
    StepList,
    StepOptions,
    StepOutput,
    AttributeMapNotLoadedError,
    ChannelMessagesNotLoadedError,
    Stream,
    Timer,
    ValueMappingError,
    Wait,
    WaitForFailurePolicy,
    WireKind,
    graceful_complete,
    heartbeat,
    dead_end,
    open_blob_cache,
    rpc,
)
from dex._async_worker_dispatcher import AsyncWorkerDispatcher
from dex._invocation_context import InvocationContext, InvocationMethod
from dex._value_mapper import ValueMapper
from dex._worker_dispatcher import WorkerDispatcher
from dex.client import Client as ClientModuleClient
from dex.client_options import ClientOptions as ClientModuleOptions
from dex._server_protocol import NegotiatedServerProtocol
from dex.dexpb import dex_pb2 as pb


@dataclass(frozen=True)
class OrderInput:
    order_id: str


@dataclass(frozen=True)
class OrderOutput:
    accepted: bool


ORDER_INPUT = JsonCodec[OrderInput](
    "OrderInput",
    lambda value: OrderInput(order_id=value["order_id"]),
    lambda value: {"order_id": value.order_id},
)
STATUS = Attribute("status", str)
COMMANDS = Channel("commands", OrderInput)
ITEMS = AttributeMap("items", str)
COMMANDS_BY_TENANT = ChannelMap("commands-by-tenant", str)


@pytest.mark.parametrize(
    "definition",
    (
        lambda: Attribute("orders/by-id", str),
        lambda: AttributeMap("orders/by-id", str),
        lambda: Channel("orders/by-id", str),
        lambda: ChannelMap("orders/by-id", str),
    ),
)
def test_persistence_definition_names_reserve_slash(definition: Any) -> None:
    with pytest.raises(ValueError, match="must not contain"):
        definition()


def test_map_instances_reserve_slash() -> None:
    with pytest.raises(ValueError, match="map instances must not contain"):
        ChannelMap("messages", str).for_one("orders/by-id")

    with pytest.raises(ValueError, match="map instances must not contain"):
        AttributeMap("items", str).lock("orders/by-id")


class ApproveOrder(Step[OrderInput]):
    def get_step_type(self) -> str:
        return "ApproveOrder"

    def execute(self, context: Context, input: OrderInput) -> StepDecision:
        del context, input
        return graceful_complete()

    def wait_for(self, context: Context, input: OrderInput) -> Wait:
        del context, input
        return Wait.any_of(
            COMMANDS.for_one(),
            Timer.by_duration(
                timedelta(seconds=10),
                condition_id="approval-timeout",
            ),
        )


class ArchiveOrder(Step[str]):
    def execute(self, context: Context, input: str) -> StepDecision:
        del context, input
        return graceful_complete()


class OrderFlow(Flow[OrderInput]):
    approve = ApproveOrder()
    archive = ArchiveOrder()

    def get_flow_type(self) -> str:
        return "Orders"

    def get_steps(self) -> StepList[OrderInput]:
        return StepList.start_step(self.approve).other_steps(self.archive)

    def get_persistence_schema(self) -> PersistenceSchema:
        return PersistenceSchema.of(STATUS, ITEMS, COMMANDS, COMMANDS_BY_TENANT)

    @rpc(
        name="GetOrder",
        lock_attributes=(STATUS.lock(), ITEMS.lock("tenant-a")),
        load_attribute_map_instances=(ITEMS.load("tenant-a"),),
        load_channel_map_instances=(COMMANDS_BY_TENANT.load_messages("tenant-a"),),
    )
    def get_order(self, context: Context, input: OrderInput) -> RPCResult[OrderOutput]:
        del context, input
        return RPCResult(OrderOutput(accepted=True))


class AsyncApproveOrder(Step[OrderInput]):
    async def execute(  # type: ignore[override]
        self,
        context: AsyncContext,
        input: OrderInput,
    ) -> StepDecision:
        del context, input
        return graceful_complete()

    async def wait_for(  # type: ignore[override]
        self, context: AsyncContext, input: OrderInput
    ) -> Wait:
        del context, input
        return Wait.skip_immediately()


class AsyncOrderFlow(Flow[OrderInput]):
    approve = AsyncApproveOrder()

    def get_steps(self) -> StepList[OrderInput]:
        return StepList.start_step(self.approve)

    @rpc(name="GetOrderAsync")
    async def get_order(
        self,
        context: Context,
        input: OrderInput,
    ) -> RPCResult[OrderOutput]:
        del context, input
        return RPCResult(OrderOutput(accepted=True))


ORDERS = OrderFlow()


def test_typed_interfaces_construct_without_runtime() -> None:
    registry = Registry((ORDERS,))
    definitions = tuple(ORDERS.get_steps())
    assert registry.flows == (ORDERS,)
    assert definitions[0].step is ORDERS.approve
    assert definitions[0].is_start_step
    assert definitions[1].step is ORDERS.archive
    assert not definitions[1].is_start_step


def test_python_defaults_match_java_contracts() -> None:
    class Container:
        class NestedStep(Step[int]):
            def execute(self, context: Context, input: int) -> StepDecision:
                del context, input
                return graceful_complete()

    options = StepOptions()
    assert Container.NestedStep().get_step_type() == "NestedStep"
    assert OrderFlow().get_flow_type() == "Orders"
    assert options.wait_for_failure is WaitForFailurePolicy.FAIL_FLOW
    assert options.wait_for_durability is StepDurability.DEFAULT
    assert options.execute_durability is StepDurability.DEFAULT


def test_persistence_schema_groups_definition_types() -> None:
    items = AttributeMap("items", int)
    events = Stream("events", str, 1024)
    schema = PersistenceSchema.of(STATUS, items, COMMANDS, events)
    assert schema.attributes == (STATUS, items)
    assert schema.channels == (COMMANDS,)
    assert schema.streams == (events,)
    with pytest.raises(TypeError, match="unsupported persistence definition"):
        PersistenceSchema.of(cast(Any, object()))


def test_registry_infers_handler_codecs_from_annotations() -> None:
    registry = Registry((ORDERS,))
    input_codec = registry.codec_registry.resolve(OrderInput)
    output_codec = registry.codec_registry.resolve(OrderOutput)
    input = OrderInput("order-1")
    output = OrderOutput(True)
    assert input_codec.decode(input_codec.encode(input)) == input
    assert output_codec.decode(output_codec.encode(output)) == output


def test_registry_rejects_async_step_and_rpc_handlers() -> None:
    with pytest.raises(FlowDefinitionError, match="must be synchronous"):
        Registry((AsyncOrderFlow(),))


def test_registry_allows_async_handlers_when_enabled() -> None:
    registry = Registry((AsyncOrderFlow(),), allow_async_handlers=True)
    assert registry.flows[0].get_flow_type() == AsyncOrderFlow().get_flow_type()


def test_registry_rejects_duplicate_interfaces() -> None:
    with pytest.raises(FlowDefinitionError, match="duplicate Flow Orders"):
        Registry((ORDERS, ORDERS))


def test_registry_rejects_duplicate_step_classes() -> None:
    class DuplicateStep(Step[int]):
        def __init__(self, step_type: str) -> None:
            self._step_type = step_type

        def get_step_type(self) -> str:
            return self._step_type

        def execute(self, context: Context, input: int) -> StepDecision:
            del context
            return graceful_complete(input)

    class DuplicateStepFlow(Flow[int]):
        def get_steps(self) -> StepList[int]:
            return StepList.start_step(DuplicateStep("first")).other_steps(
                DuplicateStep("second")
            )

    with pytest.raises(FlowDefinitionError, match="duplicate Step class"):
        Registry((DuplicateStepFlow(),))


def test_registry_wraps_user_definition_failures_with_flow_context() -> None:
    class BrokenFlow(Flow[None]):
        def get_steps(self) -> StepList[None]:
            raise RuntimeError("cannot assemble steps")

    with pytest.raises(
        FlowDefinitionError,
        match="Flow BrokenFlow registration failed: cannot assemble steps",
    ):
        Registry((BrokenFlow(),))


def test_registry_rejects_invalid_handler_signatures() -> None:
    class WrongContextStep(Step[int]):
        def execute(self, context: object, input: int) -> StepDecision:
            del context, input
            return graceful_complete()

    class WrongContextFlow(Flow[int]):
        start = WrongContextStep()

        def get_steps(self) -> StepList[int]:
            return StepList.start_step(self.start)

    with pytest.raises(FlowDefinitionError, match="context must be Context"):
        Registry((WrongContextFlow(),))


def test_registry_rejects_mismatched_wait_for_input() -> None:
    class MismatchedStep(Step[int]):
        def wait_for(self, context: Context, input: str) -> Wait:  # type: ignore[override]
            del context, input
            return Wait.skip_immediately()

        def execute(self, context: Context, input: int) -> StepDecision:
            del context, input
            return graceful_complete()

    class MismatchedFlow(Flow[int]):
        start = MismatchedStep()

        def get_steps(self) -> StepList[int]:
            return StepList.start_step(self.start)

    with pytest.raises(
        FlowDefinitionError, match="handlers must use the same input type"
    ):
        Registry((MismatchedFlow(),))


def test_registry_rejects_duplicate_rpc_locks() -> None:
    locked = Attribute("locked", str)

    class DuplicateLockFlow(Flow[None]):
        def get_persistence_schema(self) -> PersistenceSchema:
            return PersistenceSchema.of(locked)

        @rpc(lock_attributes=(locked.lock(), locked.lock()))
        def update(self, context: Context) -> None:
            del context

    with pytest.raises(FlowDefinitionError, match="duplicate attribute lock"):
        Registry((DuplicateLockFlow(),))


def test_registry_validates_and_sorts_rpc_state_loads() -> None:
    attributes = AttributeMap("items", str)
    commands = Channel("commands", str)
    channels = ChannelMap("commands-by-tenant", str)

    class LoadedFlow(Flow[None]):
        def get_persistence_schema(self) -> PersistenceSchema:
            return PersistenceSchema.of(attributes, commands, channels)

        @rpc(
            load_attribute_maps=(attributes,),
            load_attribute_map_instances=(attributes.load("tenant-a"),),
            load_channels=(commands,),
            load_channel_maps=(channels,),
            load_channel_map_instances=(channels.load_messages("tenant-a"),),
        )
        def snapshot(self, context: Context) -> None:
            del context

    flow = LoadedFlow()
    registry = Registry((flow,))
    _, registered = registry._rpc_for_method(flow.snapshot)
    assert registered.load_attribute_map_instances == (
        "items/",
        "items/tenant-a",
    )
    assert registered.load_channel_names == ("commands",)
    assert registered.load_channel_map_instances == (
        "commands-by-tenant/",
        "commands-by-tenant/tenant-a",
    )

    class DuplicateFlow(Flow[None]):
        def get_persistence_schema(self) -> PersistenceSchema:
            return PersistenceSchema.of(attributes)

        @rpc(load_attribute_maps=(attributes, attributes))
        def snapshot(self, context: Context) -> None:
            del context

    with pytest.raises(FlowDefinitionError, match="duplicate AttributeMap"):
        Registry((DuplicateFlow(),))


def test_step_and_timeout_handler_options_map_state_and_execution_policy() -> None:
    status = Attribute("status", str)
    attributes = AttributeMap("items", str)
    commands = Channel("commands", str)
    channels = ChannelMap("commands-by-tenant", str)

    class Recovery(Step[None]):
        def execute(self, context: Context, input: None) -> StepDecision:
            del context, input
            return dead_end()

    class LoadedFlow(Flow[None]):
        recovery = Recovery()

        def get_steps(self) -> StepList[None]:
            return StepList.start_step(self.recovery)

        def get_persistence_schema(self) -> PersistenceSchema:
            return PersistenceSchema.of(status, attributes, commands, channels)

        def handle_timeout(self, context: Context) -> StepDecision:
            del context
            return dead_end()

    registry = Registry((LoadedFlow(),))
    registered = registry._flow_by_type("LoadedFlow")
    dispatcher = WorkerDispatcher(
        registry,
        ValueMapper(registry.codec_registry),
        cast(Any, object()),
        NegotiatedServerProtocol(),
    )
    step_options = StepOptions(
        wait_for_load_attribute_maps=(attributes,),
        wait_for_load_attribute_map_instances=(attributes.load("tenant-a"),),
        wait_for_load_channels=(commands,),
        wait_for_load_channel_maps=(channels,),
        wait_for_load_channel_map_instances=(channels.load_messages("tenant-a"),),
        execute_load_attribute_maps=(attributes,),
        execute_load_attribute_map_instances=(attributes.load("tenant-b"),),
        execute_load_channels=(commands,),
        execute_load_channel_maps=(channels,),
        execute_load_channel_map_instances=(channels.load_messages("tenant-b"),),
    )
    mapped_step = dispatcher.map_step_options(registered, step_options)
    assert mapped_step is not None
    assert tuple(mapped_step.wait_for_load_attribute_map_instances) == (
        "items/",
        "items/tenant-a",
    )
    assert tuple(mapped_step.execute_load_channel_map_instances) == (
        "commands-by-tenant/",
        "commands-by-tenant/tenant-b",
    )
    with pytest.raises(ValueError, match="duplicate"):
        dispatcher.map_step_options(
            registered,
            StepOptions(execute_load_channels=(commands, commands)),
        )

    timeout_options = FlowTimeoutHandlerOptions(
        method_timeout=timedelta(seconds=10),
        heartbeat_timeout=timedelta(seconds=5),
        retry=RetryPolicy(maximum_attempts=3),
        durability=StepDurability.ASYNC,
        lock_attributes=(status.lock(),),
        load_attribute_maps=(attributes,),
        load_attribute_map_instances=(attributes.load("tenant-a"),),
        load_channels=(commands,),
        load_channel_maps=(channels,),
        load_channel_map_instances=(channels.load_messages("tenant-a"),),
    ).on_failure_proceed_to(Recovery)
    mapped_timeout = dispatcher.map_flow_timeout_handler_options(
        registered,
        timedelta(seconds=30),
        FlowTimeoutPolicy.HANDLER,
        timeout_options,
    )
    assert mapped_timeout is not None
    assert mapped_timeout.method_timeout_seconds == 10
    assert mapped_timeout.heartbeat_timeout_seconds == 5
    assert mapped_timeout.retry_policy.maximum_attempts == 3
    assert tuple(mapped_timeout.lock_attribute_keys) == ("status",)
    assert tuple(mapped_timeout.load_attribute_map_instances) == (
        "items/",
        "items/tenant-a",
    )
    assert mapped_timeout.failure_proceed_step_type == "Recovery"
    assert mapped_timeout.failure_proceed_step_options.skip_wait_for

    with pytest.raises(ValueError, match="positive timeout"):
        dispatcher.map_flow_timeout_handler_options(
            registered,
            None,
            FlowTimeoutPolicy.DEFAULT,
            timeout_options,
        )


def test_value_mapping_errors_are_stable() -> None:
    client = ClientModuleClient(Registry((ORDERS,)), cast(BlobCache, object()))
    with pytest.raises(ValueMappingError, match="Cannot encode Dex Value"):
        client._values.encode(cast(OrderInput, object()), ORDER_INPUT)


def test_rpc_invoke_options_union_sort_and_validate_before_transport() -> None:
    class RPCService:
        request: pb.InvokeRPCRequest | None = None

        def InvokeRPC(self, request: pb.InvokeRPCRequest) -> pb.InvokeRPCResponse:
            self.request = request
            return pb.InvokeRPCResponse(
                output=client._values.encode(
                    OrderOutput(True), registry.codec_registry.resolve(OrderOutput)
                )
            )

    registry = Registry((ORDERS,))
    client = Client(registry, cast(BlobCache, object()))
    service = RPCService()
    client._service = cast(Any, service)
    try:
        output = client.invoke_rpc(
            ORDERS.get_order,
            "flow-1",
            OrderInput("order-1"),
            options=RPCInvokeOptions(
                lock_attribute_map_instances=(
                    ITEMS.lock("tenant-b"),
                    ITEMS.lock("tenant-a"),
                ),
                load_attribute_map_instances=(
                    ITEMS.load("tenant-b"),
                    ITEMS.load("tenant-a"),
                ),
                load_channel_map_instances=(
                    COMMANDS_BY_TENANT.load_messages("tenant-b"),
                    COMMANDS_BY_TENANT.load_messages("tenant-a"),
                ),
            ),
        )
        assert output == OrderOutput(True)
        assert service.request is not None
        assert tuple(service.request.lock_attribute_keys) == (
            "items/tenant-a",
            "items/tenant-b",
            "status",
        )
        assert tuple(service.request.load_attribute_map_instances) == (
            "items/tenant-a",
            "items/tenant-b",
        )
        assert tuple(service.request.load_channel_map_instances) == (
            "commands-by-tenant/tenant-a",
            "commands-by-tenant/tenant-b",
        )

        with pytest.raises(FlowDefinitionError, match="must target an AttributeMap"):
            client.invoke_rpc(
                ORDERS.get_order,
                "flow-1",
                OrderInput("order-1"),
                options=RPCInvokeOptions(
                    lock_attribute_map_instances=(STATUS.lock(),),
                ),
            )
        foreign_items = AttributeMap("foreign-items", str)
        with pytest.raises(FlowDefinitionError, match="does not register AttributeMap"):
            client.invoke_rpc(
                ORDERS.get_order,
                "flow-1",
                OrderInput("order-1"),
                options=RPCInvokeOptions(
                    load_attribute_map_instances=(foreign_items.load("tenant-a"),),
                ),
            )
    finally:
        client.close()


def test_async_rpc_invoke_options_shape_the_same_request() -> None:
    asyncio.run(_assert_async_rpc_invoke_options_shape_the_same_request())


async def _assert_async_rpc_invoke_options_shape_the_same_request() -> None:
    class AsyncRPCService:
        request: pb.InvokeRPCRequest | None = None

        async def InvokeRPC(self, request: pb.InvokeRPCRequest) -> pb.InvokeRPCResponse:
            self.request = request
            return pb.InvokeRPCResponse(
                output=client._values.encode(
                    OrderOutput(True), registry.codec_registry.resolve(OrderOutput)
                )
            )

    registry = Registry((ORDERS,), allow_async_handlers=True)
    client = AsyncClient(registry, cast(BlobCache, object()))
    service = AsyncRPCService()
    client._service = cast(Any, service)
    try:
        output = await client.invoke_rpc(
            ORDERS.get_order,
            "flow-1",
            OrderInput("order-1"),
            options=RPCInvokeOptions(
                load_attribute_map_instances=(ITEMS.load("tenant-b"),),
            ),
        )
        assert output == OrderOutput(True)
        assert service.request is not None
        assert tuple(service.request.load_attribute_map_instances) == (
            "items/tenant-a",
            "items/tenant-b",
        )
    finally:
        await client.close()


def test_invalid_step_result_has_flow_and_step_context() -> None:
    class InvalidStep(Step[int]):
        def execute(self, context: Context, input: int) -> StepDecision:
            del context, input
            return cast(StepDecision, None)

    class InvalidFlow(Flow[int]):
        start = InvalidStep()

        def get_steps(self) -> StepList[int]:
            return StepList.start_step(self.start)

    class PassthroughHydrator:
        @staticmethod
        def execute_request(
            request: pb.InvokeExecuteMethodRequest,
        ) -> pb.InvokeExecuteMethodRequest:
            return request

    registry = Registry((InvalidFlow(),))
    values = ValueMapper(registry.codec_registry)
    dispatcher = WorkerDispatcher(
        registry, values, cast(Any, PassthroughHydrator()), NegotiatedServerProtocol()
    )
    request = pb.InvokeExecuteMethodRequest(
        flow_type="InvalidFlow",
        step_type="InvalidStep",
        step_input=values.encode(1, values.codec(int)),
    )
    with pytest.raises(InvalidStepResultError) as captured:
        list(dispatcher.invoke_execute(request))
    assert captured.value.flow_type == "InvalidFlow"
    assert captured.value.step_type == "InvalidStep"
    assert captured.value.method == "execute"


def test_builtin_codecs_enforce_wire_types_and_ranges() -> None:
    encoded = INT64.encode(42)
    assert encoded.kind is WireKind.INT64
    assert INT64.decode(encoded) == 42
    with pytest.raises(OverflowError):
        INT64.encode(2**63)
    with pytest.raises(TypeError):
        INT64.decode(STRING.encode("42"))


def test_object_codecs_use_compact_wire_encodings() -> None:
    values = ValueMapper(CodecRegistry())
    assert values.encode_dynamic({"order_id": "order-1"}).obj_value.encoding == "json"
    assert values.encode_dynamic(b"payload").obj_value.encoding == "raw"
    encoded_null = values.encode_dynamic(None)
    assert encoded_null.WhichOneof("kind") == "null_value"
    assert values.decode(encoded_null, values.codec(type(None))) is None
    assert values.decode(encoded_null, values.codec(str)) is None


def test_fluent_wait_factories_validate_channel_bounds() -> None:
    wait = Wait.until(Timer.by_duration(timedelta(seconds=1)))
    assert len(wait.conditions) == 1
    with pytest.raises(ValueError, match="requires a bound"):
        COMMANDS.for_range()


def test_worker_maps_only_user_provided_condition_ids() -> None:
    channel = Channel("condition-commands", str)

    class ConditionStep(Step[str]):
        def wait_for(self, context: Context, input: str) -> Wait:
            del context
            if input == "unnamed":
                return Wait.any_of(
                    Timer.by_duration(timedelta(seconds=1)), channel.for_one()
                )
            if input == "missing":
                return Wait.any_combination_of(
                    ConditionCombination.of(channel.for_one())
                )
            if input == "duplicate":
                return Wait.any_combination_of(
                    ConditionCombination.of(
                        channel.for_one(condition_id="same"),
                        Timer.by_duration(timedelta(seconds=1), condition_id="same"),
                    )
                )
            reused = channel.for_one(condition_id="__dex_internal_condition_0")
            return Wait.any_combination_of(
                ConditionCombination.of(reused),
                ConditionCombination.of(reused),
            )

        def execute(self, context: Context, input: str) -> StepDecision:
            del context
            return graceful_complete(input)

    class ConditionFlow(Flow[str]):
        start = ConditionStep()

        def get_steps(self) -> StepList[str]:
            return StepList.start_step(self.start)

        def get_persistence_schema(self) -> PersistenceSchema:
            return PersistenceSchema.of(channel)

    class PassthroughHydrator:
        @staticmethod
        def wait_for_request(
            request: pb.InvokeWaitForMethodRequest,
        ) -> pb.InvokeWaitForMethodRequest:
            return request

    registry = Registry((ConditionFlow(),))
    values = ValueMapper(registry.codec_registry)
    dispatcher = WorkerDispatcher(
        registry, values, cast(Any, PassthroughHydrator()), NegotiatedServerProtocol()
    )

    def invoke(input: str) -> pb.InvokeWaitForMethodResponse:
        outputs = list(
            dispatcher.invoke_wait_for(
                pb.InvokeWaitForMethodRequest(
                    flow_type="ConditionFlow",
                    step_type="ConditionStep",
                    step_input=values.encode(input, values.codec(str)),
                )
            )
        )
        return outputs[-1].result

    unnamed = invoke("unnamed").waiting_condition
    assert unnamed.timer_conditions[0].condition_id == ""
    assert unnamed.channel_conditions[0].condition_id == ""
    reused = invoke("reused").waiting_condition
    assert reused.channel_conditions[0].condition_id == "__dex_internal_condition_0"
    assert [list(item.condition_ids) for item in reused.condition_combinations] == [
        ["__dex_internal_condition_0"],
        ["__dex_internal_condition_0"],
    ]
    with pytest.raises(InvalidStepResultError, match="requires every Condition"):
        invoke("missing")
    with pytest.raises(InvalidStepResultError, match="duplicate Condition ID"):
        invoke("duplicate")
    with pytest.raises(ValueError, match="must not be empty"):
        channel.for_one(condition_id="")


def test_map_introspection_tracks_buffered_changes() -> None:
    attributes = AttributeMap("items", str)
    channels = ChannelMap("messages", str)

    class MapFlow(Flow[None]):
        def get_persistence_schema(self) -> PersistenceSchema:
            return PersistenceSchema.of(attributes, channels)

    registry = Registry((MapFlow(),))
    values = ValueMapper(registry.codec_registry)
    special = "special % key"
    context = InvocationContext(
        InvocationMethod.RPC,
        registry._flow_by_type("MapFlow"),
        pb.Context(),
        values,
        (
            pb.KV(
                key=Registry.physical_name("items", special),
                value=values.encode("initial", values.codec(str)),
            ),
            pb.KV(
                key=Registry.physical_name("items", "z"),
                value=values.encode("remove", values.codec(str)),
            ),
        ),
        channel_infos={
            Registry.physical_name("messages", special): pb.ChannelInfo(size=1),
            Registry.physical_name("messages", "empty"): pb.ChannelInfo(size=0),
        },
        loaded_attribute_map_instances=("items/",),
    )
    assert attributes.get_all_instance_keys(context) == (special, "z")
    attributes.set(context, "a", "added")
    attributes.delete(context, "z")
    assert attributes.get_all_instance_keys(context) == ("a", special)
    assert attributes.get_map_size(context) == 2
    assert channels.get_all_instance_keys(context) == (special,)
    channels.publish(context, "a", "published")
    assert channels.get_all_instance_keys(context) == ("a", special)
    assert channels.get_map_size(context) == 2


def test_rpc_selective_state_snapshots_are_typed_and_presence_aware() -> None:
    attributes = AttributeMap("items", str)
    commands = Channel("commands", str)
    channels = ChannelMap("commands-by-tenant", str)

    class SnapshotFlow(Flow[None]):
        def get_persistence_schema(self) -> PersistenceSchema:
            return PersistenceSchema.of(attributes, commands, channels)

    registry = Registry((SnapshotFlow(),))
    values = ValueMapper(registry.codec_registry)
    exact_attribute = Registry.physical_name("items", "tenant-a")
    exact_channel = Registry.physical_name("commands-by-tenant", "tenant-a")
    context = InvocationContext(
        InvocationMethod.RPC,
        registry._flow_by_type("SnapshotFlow"),
        pb.Context(),
        values,
        (
            pb.KV(
                key=exact_attribute,
                value=values.encode("loaded", values.codec(str)),
            ),
        ),
        channel_infos={
            "commands": pb.ChannelInfo(size=1),
            exact_channel: pb.ChannelInfo(size=1),
        },
        loaded_channel_messages={
            "commands": pb.ChannelValues(
                messages=(
                    pb.ChannelMessage(
                        message_id="message-1",
                        value=values.encode("queued", values.codec(str)),
                    ),
                )
            ),
            exact_channel: pb.ChannelValues(
                messages=(
                    pb.ChannelMessage(
                        message_id="message-2",
                        value=values.encode("mapped", values.codec(str)),
                    ),
                )
            ),
        },
        loaded_attribute_map_instances=(exact_attribute,),
        loaded_channel_names=("commands",),
        loaded_channel_map_instances=(exact_channel,),
    )

    assert attributes.get(context, "tenant-a") == "loaded"
    with pytest.raises(AttributeMapNotLoadedError):
        attributes.get(context, "tenant-b")
    with pytest.raises(AttributeMapNotLoadedError):
        attributes.get_all_instance_keys(context)
    assert commands.size(context) == 1
    assert commands.pending_messages(context)[0].value == "queued"
    assert commands.find_pending_message(context, "message-1") is not None
    assert channels.get_all_instance_keys(context) == ("tenant-a",)
    assert channels.pending_messages(context, "tenant-a")[0].value == "mapped"
    with pytest.raises(ChannelMessagesNotLoadedError):
        channels.pending_messages(context, "tenant-b")


def test_explicit_custom_codec_remains_available() -> None:
    codecs = CodecRegistry({OrderInput: ORDER_INPUT})
    assert codecs.resolve(OrderInput) is ORDER_INPUT


def test_blob_cache_contract_matches_core_config(tmp_path: Any) -> None:
    config = BlobCacheConfig(str(tmp_path), 1024, 0)
    assert config.frequency_counters == 10_000
    cache = open_blob_cache(config)
    assert cache.put("blob", b"payload")
    assert cache.get("blob") == b"payload"
    cache.delete("blob")
    assert cache.get("blob") is None
    cache.close()


def test_client_has_single_public_definition() -> None:
    assert Client is ClientModuleClient
    client = ClientModuleClient(Registry((ORDERS,)), cast(BlobCache, object()))
    assert client.options == ClientModuleOptions()


def test_client_transport_is_initialized_lazily() -> None:
    client = Client(Registry((ORDERS,)), cast(BlobCache, object()))
    client.close()


def test_attribute_store_sync_mapping_preserves_presence() -> None:
    plain = Attribute("plain", str)
    synced = Attribute("synced", str, sync_to_attribute_store=True)
    synced_map = AttributeMap("mapped", str, sync_to_attribute_store=True)
    assert not plain.sync_to_attribute_store
    assert synced.sync_to_attribute_store
    assert synced_map.sync_to_attribute_store

    client = ClientModuleClient(Registry((ORDERS,)), cast(BlobCache, object()))
    start = client._map_start_options(
        client.registry._flow_for_instance(ORDERS),
        StartFlowOptions()
        .with_attribute(plain, "plain")
        .with_attribute(synced, "synced")
        .with_attribute(synced_map, "tenant-1", "mapped"),
    )
    assert not start.attributes[0].HasField("sync_config")
    assert start.attributes[1].sync_config.enabled
    assert start.attributes[2].sync_config.enabled

    absent = client._map_flow_config(FlowConfig())
    assert not absent.HasField("attribute_store_names")
    selected = client._map_flow_config(
        FlowConfig(attribute_store_names=["reporting", "audit"])
    )
    assert list(selected.attribute_store_names.names) == ["reporting", "audit"]
    assert selected.HasField("attribute_store_names")
    disabled = client._map_flow_config(FlowConfig(attribute_store_names=[]))
    assert list(disabled.attribute_store_names.names) == []
    assert disabled.HasField("attribute_store_names")
    client.close()


def test_step_stream_write_creates_repeatable_outputs() -> None:
    thinking = Stream("thinking", str, 1_048_576)

    class StreamFlow(Flow[str]):
        def get_persistence_schema(self) -> PersistenceSchema:
            return PersistenceSchema.of(thinking)

    registry = Registry((StreamFlow(),))
    values = ValueMapper(registry.codec_registry)
    context = InvocationContext(
        InvocationMethod.EXECUTE,
        registry._flow_by_type("StreamFlow"),
        pb.Context(
            flow_id="flow-1",
            run_id="run-1",
            step_execution_id="step-1",
        ),
        values,
        (),
    )

    first = thinking.write(context, "checking")
    second = thinking.write(context, "again")
    assert isinstance(first, StepOutput)
    assert isinstance(second, StepOutput)
    assert first is not second
    assert isinstance(heartbeat(), StepOutput)
    assert isinstance(heartbeat(None), StepOutput)
    with pytest.raises(TypeError, match="must be created"):
        StepOutput()
    with pytest.raises(ValueError, match="does not belong"):
        Stream("other", str, 1024).write(context, "unregistered")
    with pytest.raises(ValueMappingError):
        thinking.write(context, cast(Any, 1))

    rpc_context = InvocationContext(
        InvocationMethod.RPC,
        registry._flow_by_type("StreamFlow"),
        pb.Context(),
        values,
        (),
    )
    with pytest.raises(ValueError, match="Step Context"):
        thinking.write(rpc_context, "rejected")


def test_client_stream_transport_and_metadata() -> None:
    thinking = Stream("thinking", str, 1_048_576)

    class StreamFlow(Flow[str]):
        def get_persistence_schema(self) -> PersistenceSchema:
            return PersistenceSchema.of(thinking)

    class StreamService:
        def __init__(self) -> None:
            self.write_request: pb.WriteStreamRequest | None = None
            self.read_request: pb.ReadStreamRequest | None = None
            self.list_request: pb.ListStreamMessagesRequest | None = None

        def WriteStream(self, request: pb.WriteStreamRequest) -> object:
            self.write_request = request
            return object()

        def ReadStream(self, request: pb.ReadStreamRequest) -> pb.ReadStreamResponse:
            self.read_request = request
            created_time = datetime(2026, 8, 27, 12, tzinfo=timezone.utc)
            response = pb.ReadStreamResponse()
            response.message.value.string_value = "working"
            response.message.resume_token = "resume-1"
            response.message.created_time.FromDatetime(created_time)
            response.message.source = "client-1"
            return response

        def ListStreamMessages(
            self,
            request: pb.ListStreamMessagesRequest,
        ) -> pb.ListStreamMessagesResponse:
            self.list_request = request
            response = pb.ListStreamMessagesResponse(next_page_token="next-page")
            for value, token, source, hour in (
                ("newest", "resume-2", "client-2", 13),
                ("older", "resume-1", "client-1", 12),
            ):
                message = response.messages.add(
                    resume_token=token,
                    source=source,
                )
                message.value.string_value = value
                message.created_time.FromDatetime(
                    datetime(2026, 8, 27, hour, tzinfo=timezone.utc)
                )
            return response

    client = Client(Registry((StreamFlow(),)), cast(BlobCache, object()))
    service = StreamService()
    client._service = cast(Any, service)
    try:
        client.write_stream("flow-1", thinking, "client-1", "starting")
        message = client.read_stream(
            "flow-1",
            thinking,
            "previous",
            timedelta(seconds=2),
        )
        assert service.write_request is not None
        assert service.write_request.flow_type == "StreamFlow"
        assert service.write_request.stream_name == "thinking"
        assert service.write_request.stream_capacity_bytes == 1_048_576
        assert service.write_request.source == "client-1"
        assert service.read_request is not None
        assert service.read_request.resume_token == "previous"
        assert service.read_request.wait_time_seconds == 2
        assert message.value == "working"
        assert message.resume_token == "resume-1"
        assert message.created_time == datetime(2026, 8, 27, 12, tzinfo=timezone.utc)
        assert message.source == "client-1"
        page = client.list_stream_messages("flow-1", thinking, 2, "before-page")
        assert service.list_request is not None
        assert service.list_request.page_size == 2
        assert service.list_request.before_page_token == "before-page"
        assert [entry.value for entry in page.messages] == ["newest", "older"]
        assert [entry.resume_token for entry in page.messages] == [
            "resume-2",
            "resume-1",
        ]
        assert page.next_page_token == "next-page"
        with pytest.raises(ValueError, match="page size must be positive"):
            client.list_stream_messages("flow-1", thinking, 0)
        client.write_stream("flow-1", thinking, "allowed#source", "accepted")
        assert service.write_request.source == "allowed#source"
        with pytest.raises(ValueError, match="required"):
            client.write_stream("flow-1", thinking, "", "rejected")
    finally:
        client.close()


def test_sync_step_generator_maps_ordered_progress_and_result() -> None:
    progress = Stream("sync-progress", str, 1_048_576)

    class StreamingStep(Step[str]):
        def wait_for(
            self,
            context: Context,
            input: str,
        ) -> Generator[StepOutput, None, Wait]:
            yield heartbeat(f"wait-{input}")
            yield progress.write(context, "wait-stream")
            return Wait.skip_immediately()

        def execute(
            self,
            context: Context,
            input: str,
        ) -> Generator[StepOutput, None, StepDecision]:
            yield heartbeat()
            yield progress.write(context, "first")
            yield heartbeat(None)
            yield progress.write(context, "second")
            return graceful_complete(input)

    class StreamingFlow(Flow[str]):
        start = StreamingStep()

        def get_steps(self) -> StepList[str]:
            return StepList.start_step(self.start)

        def get_persistence_schema(self) -> PersistenceSchema:
            return PersistenceSchema.of(progress)

    class PassthroughHydrator:
        @staticmethod
        def wait_for_request(
            request: pb.InvokeWaitForMethodRequest,
        ) -> pb.InvokeWaitForMethodRequest:
            return request

        @staticmethod
        def execute_request(
            request: pb.InvokeExecuteMethodRequest,
        ) -> pb.InvokeExecuteMethodRequest:
            return request

    registry = Registry((StreamingFlow(),))
    values = ValueMapper(registry.codec_registry)
    dispatcher = WorkerDispatcher(
        registry, values, cast(Any, PassthroughHydrator()), NegotiatedServerProtocol()
    )
    encoded_input = values.encode("input", values.codec(str))

    wait_outputs = list(
        dispatcher.invoke_wait_for(
            pb.InvokeWaitForMethodRequest(
                flow_type="StreamingFlow",
                step_type="StreamingStep",
                step_input=encoded_input,
            )
        )
    )
    assert [output.WhichOneof("output") for output in wait_outputs] == [
        "heartbeat",
        "stream_write",
        "result",
    ]
    assert (
        values.decode(wait_outputs[0].heartbeat.value, values.codec(str))
        == "wait-input"
    )
    assert wait_outputs[1].stream_write.stream_name == "sync-progress"

    execute_outputs = list(
        dispatcher.invoke_execute(
            pb.InvokeExecuteMethodRequest(
                flow_type="StreamingFlow",
                step_type="StreamingStep",
                step_input=encoded_input,
            )
        )
    )
    assert [output.WhichOneof("output") for output in execute_outputs] == [
        "heartbeat",
        "stream_write",
        "heartbeat",
        "stream_write",
        "result",
    ]
    assert not execute_outputs[0].heartbeat.HasField("value")
    assert execute_outputs[2].heartbeat.HasField("value")
    assert (
        values.decode(execute_outputs[2].heartbeat.value, values.codec(type(None)))
        is None
    )
    assert [
        values.decode(output.stream_write.value, values.codec(str))
        for output in (execute_outputs[1], execute_outputs[3])
    ] == ["first", "second"]


def test_sync_buffered_text_stream_is_cooperative() -> None:
    progress = Stream("sync-buffered-progress", str, 1_048_576)

    class BufferedStep(Step[str]):
        def execute(
            self,
            context: Context,
            input: str,
        ) -> Generator[StepOutput, None, StepDecision]:
            writer = progress.buffered_text(context, max_buffered_bytes=5)
            yield from writer.write(input)
            yield from writer.write(" 世界")
            yield from writer.flush()
            return graceful_complete(input)

    class BufferedFlow(Flow[str]):
        start = BufferedStep()

        def get_steps(self) -> StepList[str]:
            return StepList.start_step(self.start)

        def get_persistence_schema(self) -> PersistenceSchema:
            return PersistenceSchema.of(progress)

    class PassthroughHydrator:
        @staticmethod
        def execute_request(
            request: pb.InvokeExecuteMethodRequest,
        ) -> pb.InvokeExecuteMethodRequest:
            return request

    registry = Registry((BufferedFlow(),))
    values = ValueMapper(registry.codec_registry)
    dispatcher = WorkerDispatcher(
        registry, values, cast(Any, PassthroughHydrator()), NegotiatedServerProtocol()
    )
    outputs = list(
        dispatcher.invoke_execute(
            pb.InvokeExecuteMethodRequest(
                flow_type="BufferedFlow",
                step_type="BufferedStep",
                step_input=values.encode("hello", values.codec(str)),
            )
        )
    )
    assert [output.WhichOneof("output") for output in outputs] == [
        "stream_write",
        "stream_write",
        "result",
    ]
    assert [
        values.decode(output.stream_write.value, values.codec(str))
        for output in outputs[:-1]
    ] == ["hello", " 世界"]


@pytest.mark.parametrize("invalid_output", [graceful_complete(), object()])
def test_sync_step_generator_rejects_invalid_yields(invalid_output: object) -> None:
    class InvalidGeneratorStep(Step[None]):
        def execute(
            self,
            context: Context,
            input: None,
        ) -> Generator[StepOutput, None, StepDecision]:
            del context, input
            yield cast(StepOutput, invalid_output)
            return graceful_complete()

    class InvalidGeneratorFlow(Flow[None]):
        start = InvalidGeneratorStep()

        def get_steps(self) -> StepList[None]:
            return StepList.start_step(self.start)

    class PassthroughHydrator:
        @staticmethod
        def execute_request(
            request: pb.InvokeExecuteMethodRequest,
        ) -> pb.InvokeExecuteMethodRequest:
            return request

    registry = Registry((InvalidGeneratorFlow(),))
    values = ValueMapper(registry.codec_registry)
    dispatcher = WorkerDispatcher(
        registry, values, cast(Any, PassthroughHydrator()), NegotiatedServerProtocol()
    )
    with pytest.raises(InvalidStepResultError, match="must yield StepOutput"):
        list(
            dispatcher.invoke_execute(
                pb.InvokeExecuteMethodRequest(
                    flow_type="InvalidGeneratorFlow",
                    step_type="InvalidGeneratorStep",
                    step_input=values.encode_dynamic(None),
                )
            )
        )


def test_sync_step_generator_requires_final_return() -> None:
    class MissingResultStep(Step[None]):
        def execute(  # type: ignore[return]
            self,
            context: Context,
            input: None,
        ) -> Generator[StepOutput, None, StepDecision]:
            del context, input
            yield heartbeat()

    class MissingResultFlow(Flow[None]):
        start = MissingResultStep()

        def get_steps(self) -> StepList[None]:
            return StepList.start_step(self.start)

    class PassthroughHydrator:
        @staticmethod
        def execute_request(
            request: pb.InvokeExecuteMethodRequest,
        ) -> pb.InvokeExecuteMethodRequest:
            return request

    registry = Registry((MissingResultFlow(),))
    values = ValueMapper(registry.codec_registry)
    dispatcher = WorkerDispatcher(
        registry, values, cast(Any, PassthroughHydrator()), NegotiatedServerProtocol()
    )
    with pytest.raises(InvalidStepResultError, match="must return StepDecision"):
        list(
            dispatcher.invoke_execute(
                pb.InvokeExecuteMethodRequest(
                    flow_type="MissingResultFlow",
                    step_type="MissingResultStep",
                    step_input=values.encode_dynamic(None),
                )
            )
        )


def test_sync_step_generator_preserves_application_errors() -> None:
    class FailingGeneratorStep(Step[None]):
        def execute(
            self,
            context: Context,
            input: None,
        ) -> Generator[StepOutput, None, StepDecision]:
            del context, input
            yield heartbeat()
            raise ValueError("application failure")

    class FailingGeneratorFlow(Flow[None]):
        start = FailingGeneratorStep()

        def get_steps(self) -> StepList[None]:
            return StepList.start_step(self.start)

    class PassthroughHydrator:
        @staticmethod
        def execute_request(
            request: pb.InvokeExecuteMethodRequest,
        ) -> pb.InvokeExecuteMethodRequest:
            return request

    registry = Registry((FailingGeneratorFlow(),))
    values = ValueMapper(registry.codec_registry)
    dispatcher = WorkerDispatcher(
        registry, values, cast(Any, PassthroughHydrator()), NegotiatedServerProtocol()
    )
    with pytest.raises(ValueError, match="application failure"):
        list(
            dispatcher.invoke_execute(
                pb.InvokeExecuteMethodRequest(
                    flow_type="FailingGeneratorFlow",
                    step_type="FailingGeneratorStep",
                    step_input=values.encode_dynamic(None),
                )
            )
        )


def test_registry_validates_generator_shapes() -> None:
    class WrongGeneratorStep(Step[None]):
        def execute(  # type: ignore[override]
            self,
            context: Context,
            input: None,
        ) -> Iterator[StepOutput]:
            del context, input
            yield heartbeat()

    class WrongGeneratorFlow(Flow[None]):
        start = WrongGeneratorStep()

        def get_steps(self) -> StepList[None]:
            return StepList.start_step(self.start)

    class AsyncGeneratorStep(Step[None]):
        async def execute(  # type: ignore[override]
            self,
            context: AsyncContext,
            input: None,
        ) -> AsyncIterator[StepOutput]:
            del context, input
            yield heartbeat()

    class AsyncGeneratorFlow(Flow[None]):
        start = AsyncGeneratorStep()

        def get_steps(self) -> StepList[None]:
            return StepList.start_step(self.start)

    class WrongAsyncContextStep(Step[None]):
        async def execute(  # type: ignore[override]
            self,
            context: Context,
            input: None,
        ) -> StepDecision:
            del context, input
            return graceful_complete()

    class WrongAsyncContextFlow(Flow[None]):
        start = WrongAsyncContextStep()

        def get_steps(self) -> StepList[None]:
            return StepList.start_step(self.start)

    with pytest.raises(FlowDefinitionError, match=r"Generator\[StepOutput"):
        Registry((WrongGeneratorFlow(),))
    with pytest.raises(FlowDefinitionError, match="must not be an async generator"):
        Registry((AsyncGeneratorFlow(),), allow_async_handlers=True)
    with pytest.raises(FlowDefinitionError, match="AsyncContext"):
        Registry((WrongAsyncContextFlow(),), allow_async_handlers=True)


def test_context_decodes_last_heartbeat_presence() -> None:
    class HeartbeatFlow(Flow[None]):
        pass

    registry = Registry((HeartbeatFlow(),))
    values = ValueMapper(registry.codec_registry)
    absent = InvocationContext(
        InvocationMethod.EXECUTE,
        registry._flow_by_type("HeartbeatFlow"),
        pb.Context(),
        values,
        (),
    )
    assert not absent.has_last_heartbeat_value()
    assert absent.get_last_heartbeat_value(str) is None

    present = InvocationContext(
        InvocationMethod.EXECUTE,
        registry._flow_by_type("HeartbeatFlow"),
        pb.Context(last_heartbeat_value=values.encode_dynamic(None)),
        values,
        (),
    )
    assert present.has_last_heartbeat_value()
    assert present.get_last_heartbeat_value(type(None)) is None
    incompatible = InvocationContext(
        InvocationMethod.EXECUTE,
        registry._flow_by_type("HeartbeatFlow"),
        pb.Context(last_heartbeat_value=values.encode_dynamic("checkpoint")),
        values,
        (),
    )
    with pytest.raises(ValueMappingError):
        incompatible.get_last_heartbeat_value(int)


def test_context_exposes_recovery_error() -> None:
    class RecoveryFlow(Flow[None]):
        pass

    registry = Registry((RecoveryFlow(),))
    context = InvocationContext(
        InvocationMethod.EXECUTE,
        registry._flow_by_type("RecoveryFlow"),
        pb.Context(
            recovery_error=pb.RecoveryErrorInfo(
                detail="handler exhausted retries",
                error_type="TimeoutError",
            )
        ),
        ValueMapper(registry.codec_registry),
        (),
    )
    assert context.recovery_error is not None
    assert context.recovery_error.detail == "handler exhausted retries"
    assert context.recovery_error.error_type == "TimeoutError"


def test_context_exposes_first_attempt_at() -> None:
    class AttemptFlow(Flow[None]):
        pass

    registry = Registry((AttemptFlow(),))
    context = InvocationContext(
        InvocationMethod.EXECUTE,
        registry._flow_by_type("AttemptFlow"),
        pb.Context(attempt=3, first_attempt_timestamp=1_791_000_000),
        ValueMapper(registry.codec_registry),
        (),
    )
    assert context.first_attempt_at == datetime.fromtimestamp(1_791_000_000, UTC)
    assert context.first_attempt_at.tzinfo is UTC


def test_async_step_outputs_preserve_call_order() -> None:
    asyncio.run(_assert_async_step_outputs_preserve_call_order())


async def _assert_async_step_outputs_preserve_call_order() -> None:
    progress = Stream("async-progress", str, 1_048_576)

    class AsyncStreamingStep(Step[str]):
        async def execute(  # type: ignore[override]
            self,
            context: AsyncContext,
            input: str,
        ) -> StepDecision:
            progress.write(context, "first")
            await context.heartbeat()
            progress.write(context, "second")
            await context.heartbeat(None)
            return graceful_complete(input)

    class AsyncStreamingFlow(Flow[str]):
        start = AsyncStreamingStep()

        def get_steps(self) -> StepList[str]:
            return StepList.start_step(self.start)

        def get_persistence_schema(self) -> PersistenceSchema:
            return PersistenceSchema.of(progress)

    class AsyncPassthroughHydrator:
        @staticmethod
        async def execute_request(
            request: pb.InvokeExecuteMethodRequest,
        ) -> pb.InvokeExecuteMethodRequest:
            return request

    registry = Registry((AsyncStreamingFlow(),), allow_async_handlers=True)
    values = ValueMapper(registry.codec_registry)
    dispatcher = AsyncWorkerDispatcher(
        registry,
        values,
        cast(Any, AsyncPassthroughHydrator()),
        NegotiatedServerProtocol(),
    )
    outputs = [
        output
        async for output in dispatcher.invoke_execute(
            pb.InvokeExecuteMethodRequest(
                flow_type="AsyncStreamingFlow",
                step_type="AsyncStreamingStep",
                step_input=values.encode("input", values.codec(str)),
            )
        )
    ]
    assert [output.WhichOneof("output") for output in outputs] == [
        "stream_write",
        "heartbeat",
        "stream_write",
        "heartbeat",
        "result",
    ]
    assert not outputs[1].heartbeat.HasField("value")
    assert outputs[3].heartbeat.HasField("value")


def test_async_buffered_text_stream_flushes_on_timer_and_completion() -> None:
    asyncio.run(_assert_async_buffered_text_stream_flushes())


async def _assert_async_buffered_text_stream_flushes() -> None:
    progress = Stream("async-buffered-progress", str, 1_048_576)
    release_handler = asyncio.Event()

    class BufferedStep(Step[str]):
        async def execute(  # type: ignore[override]
            self,
            context: AsyncContext,
            input: str,
        ) -> StepDecision:
            writer = progress.buffered_text(
                context,
                flush_interval=timedelta(milliseconds=5),
            )
            writer.write(input)
            await release_handler.wait()
            writer.write(" ")
            writer.write("世界")
            return graceful_complete(input)

    class BufferedFlow(Flow[str]):
        start = BufferedStep()

        def get_steps(self) -> StepList[str]:
            return StepList.start_step(self.start)

        def get_persistence_schema(self) -> PersistenceSchema:
            return PersistenceSchema.of(progress)

    class AsyncPassthroughHydrator:
        @staticmethod
        async def execute_request(
            request: pb.InvokeExecuteMethodRequest,
        ) -> pb.InvokeExecuteMethodRequest:
            return request

    registry = Registry((BufferedFlow(),), allow_async_handlers=True)
    values = ValueMapper(registry.codec_registry)
    dispatcher = AsyncWorkerDispatcher(
        registry,
        values,
        cast(Any, AsyncPassthroughHydrator()),
        NegotiatedServerProtocol(),
    )
    invocation = dispatcher.invoke_execute(
        pb.InvokeExecuteMethodRequest(
            flow_type="BufferedFlow",
            step_type="BufferedStep",
            step_input=values.encode("hello", values.codec(str)),
        )
    )
    first_output = await anext(invocation)
    release_handler.set()
    outputs = [first_output, *[output async for output in invocation]]
    assert [output.WhichOneof("output") for output in outputs] == [
        "stream_write",
        "stream_write",
        "result",
    ]
    assert [
        values.decode(output.stream_write.value, values.codec(str))
        for output in outputs[:-1]
    ] == ["hello", " 世界"]


def test_async_step_stream_cancellation_cancels_handler() -> None:
    asyncio.run(_assert_async_step_stream_cancellation_cancels_handler())


async def _assert_async_step_stream_cancellation_cancels_handler() -> None:
    canceled = asyncio.Event()
    heartbeat_canceled = asyncio.Event()
    progress = Stream("canceled-progress", str, 1024)

    class BlockingStep(Step[None]):
        async def execute(  # type: ignore[override]
            self,
            context: AsyncContext,
            input: None,
        ) -> StepDecision:
            del input
            await context.heartbeat("running")
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                progress.write(context, "dropped")
                try:
                    await context.heartbeat("dropped")
                except asyncio.CancelledError:
                    heartbeat_canceled.set()
                raise
            finally:
                canceled.set()
            return graceful_complete()

    class BlockingFlow(Flow[None]):
        start = BlockingStep()

        def get_steps(self) -> StepList[None]:
            return StepList.start_step(self.start)

        def get_persistence_schema(self) -> PersistenceSchema:
            return PersistenceSchema.of(progress)

    class AsyncPassthroughHydrator:
        @staticmethod
        async def execute_request(
            request: pb.InvokeExecuteMethodRequest,
        ) -> pb.InvokeExecuteMethodRequest:
            return request

    registry = Registry((BlockingFlow(),), allow_async_handlers=True)
    values = ValueMapper(registry.codec_registry)
    dispatcher = AsyncWorkerDispatcher(
        registry,
        values,
        cast(Any, AsyncPassthroughHydrator()),
        NegotiatedServerProtocol(),
    )
    outputs = dispatcher.invoke_execute(
        pb.InvokeExecuteMethodRequest(
            flow_type="BlockingFlow",
            step_type="BlockingStep",
            step_input=values.encode_dynamic(None),
        )
    )
    assert (await anext(outputs)).WhichOneof("output") == "heartbeat"
    await outputs.aclose()
    await asyncio.wait_for(canceled.wait(), timeout=1)
    await asyncio.wait_for(heartbeat_canceled.wait(), timeout=1)


def test_rpc_rejects_progress_and_timeout_allows_sync_stream_output() -> None:
    stream = Stream("restricted-progress", str, 1024)

    class RestrictedFlow(Flow[None]):
        def get_persistence_schema(self) -> PersistenceSchema:
            return PersistenceSchema.of(stream)

    registry = Registry((RestrictedFlow(),))
    values = ValueMapper(registry.codec_registry)
    rpc_context = InvocationContext(
        InvocationMethod.RPC,
        registry._flow_by_type("RestrictedFlow"),
        pb.Context(),
        values,
        (),
    )
    with pytest.raises(ValueError, match="Step Context"):
        stream.write(rpc_context, "rejected")
    with pytest.raises(ValueError, match="asynchronous Step Context"):
        asyncio.run(rpc_context.heartbeat())

    timeout_context = InvocationContext(
        InvocationMethod.TIMEOUT,
        registry._flow_by_type("RestrictedFlow"),
        pb.Context(),
        values,
        (),
    )
    assert stream.write(timeout_context, "allowed") is not None
    with pytest.raises(ValueError, match="asynchronous Step Context"):
        asyncio.run(timeout_context.heartbeat())


def test_search_run_inclusion_options_reach_transport() -> None:
    class SearchService:
        requests: list[pb.SearchFlowsRequest] = []

        def SearchFlows(self, request: pb.SearchFlowsRequest) -> pb.SearchFlowsResponse:
            self.requests.append(request)
            return pb.SearchFlowsResponse()

    client = Client(Registry((ORDERS,)), cast(BlobCache, object()))
    service = SearchService()
    client._service = cast(Any, service)
    try:
        client.search_flows("WorkflowId = 'flow-1'", 1)
        client.search_flows(
            "WorkflowId = 'flow-1'", 1, "next", include_continued_as_new=True
        )
        assert [request.include_continued_as_new for request in service.requests] == [
            False,
            True,
        ]
        assert service.requests[1].query == "WorkflowId = 'flow-1'"
        assert service.requests[1].next_page_token == "next"
    finally:
        client.close()


def test_async_search_run_inclusion_options_reach_transport() -> None:
    class SearchService:
        requests: list[pb.SearchFlowsRequest] = []

        async def SearchFlows(
            self, request: pb.SearchFlowsRequest
        ) -> pb.SearchFlowsResponse:
            self.requests.append(request)
            return pb.SearchFlowsResponse()

    async def run() -> None:
        client = AsyncClient(Registry((ORDERS,)), cast(BlobCache, object()))
        service = SearchService()
        client._service = cast(Any, service)
        try:
            await client.search_flows("WorkflowId = 'flow-1'", 1)
            await client.search_flows(
                "WorkflowId = 'flow-1'", 1, "next", include_continued_as_new=True
            )
            assert [
                request.include_continued_as_new for request in service.requests
            ] == [False, True]
            assert service.requests[1].query == "WorkflowId = 'flow-1'"
            assert service.requests[1].next_page_token == "next"
        finally:
            await client.close()

    asyncio.run(run())
