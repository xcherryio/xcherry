# Copyright (c) 2026 Super Durable, Inc.
#
# Licensed under the Sustainable Use License 1.0.
# You may not use this file except in compliance with the License.
# See the LICENSE file in the repository root.
#
# SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

from __future__ import annotations

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, cast

import grpc
import pytest
from google.protobuf import empty_pb2

from dex import (
    INT64,
    AsyncClient,
    Attribute,
    AttributeIndex,
    AttributeMap,
    AttributeMatch,
    BlobCache,
    Client,
    ClientOptions,
    CodecRegistry,
    Context,
    Flow,
    FlowDefinitionError,
    IndexType,
    InvalidStepResultError,
    PersistenceSchema,
    Registry,
    StartFlowOptions,
    Step,
    StepDecision,
    StepList,
    SubFlow,
    SubFlowOptions,
    ValueMappingError,
    Wait,
    WaitForAttributeOptions,
    graceful_complete,
    rpc,
)
from dex._async_worker_dispatcher import AsyncWorkerDispatcher
from dex._invocation_context import InvocationContext, InvocationMethod
from dex._value_mapper import ValueMapper
from dex._worker_dispatcher import WorkerDispatcher
from dex.attribute import _apply_hard_delete_on_null
from dex._server_protocol import NegotiatedServerProtocol
from dex.dexpb import dex_pb2 as pb
from dex.dexpb import dex_pb2_grpc


@dataclass(frozen=True)
class Credential:
    token: str


@dataclass(frozen=True)
class AccountNumber:
    value: int


class FinishStep(Step[None]):
    def execute(self, context: Context, input: None) -> StepDecision:
        return graceful_complete()


class SecretFlow(Flow[None]):
    def __init__(self) -> None:
        self.api_key = Attribute("api-key", str, hard_delete_on_null=True)
        self.tokens = AttributeMap("token", str, hard_delete_on_null=True)
        self.credential = Attribute("credential", Credential, hard_delete_on_null=True)
        self.raw = Attribute("raw", bytes, hard_delete_on_null=True)
        self.plain = Attribute("plain", str)
        self.finish = FinishStep()

    def get_steps(self) -> StepList[None]:
        return StepList.start_step(self.finish)

    def get_persistence_schema(self) -> PersistenceSchema:
        return PersistenceSchema.of(
            self.api_key, self.tokens, self.credential, self.raw, self.plain
        )


@pytest.mark.parametrize("definition_type", (Attribute, AttributeMap))
def test_hard_delete_definitions_reject_index_and_store_sync(
    definition_type: Any,
) -> None:
    with pytest.raises(
        ValueError, match="hard-delete Attribute secret cannot use an index"
    ):
        definition_type(
            "secret",
            str,
            AttributeIndex(IndexType.KEYWORD, "SecretKeyword"),
            hard_delete_on_null=True,
        )
    with pytest.raises(ValueError, match="cannot use sync_to_attribute_store"):
        definition_type(
            "secret", str, sync_to_attribute_store=True, hard_delete_on_null=True
        )


@pytest.mark.parametrize("definition_type", (Attribute, AttributeMap))
@pytest.mark.parametrize("value_type", (int, bool, float))
def test_hard_delete_definitions_reject_numeric_and_boolean_types(
    definition_type: Any,
    value_type: type[Any],
) -> None:
    with pytest.raises(TypeError, match="supports only string and object values"):
        definition_type("secret", value_type, hard_delete_on_null=True)


def test_hard_delete_definitions_accept_string_and_object_types() -> None:
    flow = SecretFlow()
    assert flow.api_key.hard_delete_on_null
    assert flow.tokens.hard_delete_on_null
    assert not flow.plain.hard_delete_on_null
    Registry((flow,))


def test_registry_rejects_hard_delete_numeric_custom_codec() -> None:
    class AccountFlow(Flow[None]):
        account = Attribute("account", AccountNumber, hard_delete_on_null=True)

        def get_steps(self) -> StepList[None]:
            return StepList.start_step(FinishStep())

        def get_persistence_schema(self) -> PersistenceSchema:
            return PersistenceSchema.of(self.account)

    with pytest.raises(FlowDefinitionError, match="hard-delete Attribute account"):
        Registry((AccountFlow(),), CodecRegistry({AccountNumber: INT64}))


def test_handler_writes_and_deletes_carry_hard_delete_flag() -> None:
    flow = SecretFlow()
    registry = Registry((flow,))
    values = ValueMapper(registry.codec_registry)
    context = InvocationContext(
        InvocationMethod.RPC,
        registry._flow_by_type("SecretFlow"),
        pb.Context(),
        values,
        (),
    )
    flow.api_key.set(cast(Context, context), "sk-1")
    flow.tokens.set(cast(Context, context), "payments", "tok-1")
    flow.credential.set(cast(Context, context), Credential("cred"))
    flow.raw.set(cast(Context, context), b"raw-secret")
    flow.tokens.delete(cast(Context, context), "billing")
    flow.plain.set(cast(Context, context), "visible")
    writes = context.attribute_writes
    assert writes["api-key"].is_hard_delete_on_null
    assert writes["api-key"].value.string_value == "sk-1"
    assert writes["token/payments"].is_hard_delete_on_null
    assert writes["credential"].is_hard_delete_on_null
    assert writes["raw"].is_hard_delete_on_null
    assert writes["token/billing"].is_hard_delete_on_null
    assert writes["token/billing"].value.WhichOneof("kind") == "null_value"
    assert not writes["plain"].is_hard_delete_on_null

    flow.api_key.set(cast(Context, context), cast(str, None))
    assert writes["api-key"].is_hard_delete_on_null
    assert writes["api-key"].value.WhichOneof("kind") == "null_value"


@pytest.mark.parametrize(
    "value",
    (pb.Value(int_value=1), pb.Value(double_value=1.5), pb.Value(bool_value=True)),
)
def test_hard_delete_write_rejects_numeric_and_boolean_values(value: pb.Value) -> None:
    write = pb.AttributeWrite(key="api-key", value=value)
    with pytest.raises(ValueMappingError, match="supports only string and object"):
        _apply_hard_delete_on_null(write, SecretFlow().api_key)
    assert not write.is_hard_delete_on_null


def test_start_and_subflow_initial_attributes_carry_hard_delete_flag() -> None:
    flow = SecretFlow()
    registry = Registry((flow,))
    options = (
        StartFlowOptions()
        .with_attribute(flow.api_key, "sk-1")
        .with_attribute(flow.tokens, "payments", cast(str, None))
        .with_attribute(flow.plain, "visible")
    )
    client = Client(registry, cast(BlobCache, object()))
    start = client._map_start_options(registry._flow_for_instance(flow), options)
    client.close()
    assert [write.is_hard_delete_on_null for write in start.attributes] == [
        True,
        True,
        False,
    ]
    assert start.attributes[1].value.WhichOneof("kind") == "null_value"

    values = ValueMapper(registry.codec_registry)
    dispatcher = WorkerDispatcher(
        registry, values, cast(Any, object()), _server_protocol(3)
    )
    sub_flow = dispatcher._map_sub_flow_options(
        registry._flow_by_type("SecretFlow"),
        SubFlowOptions()
        .with_attribute(flow.credential, Credential("cred"))
        .with_attribute(flow.plain, "visible"),
    )
    assert [write.is_hard_delete_on_null for write in sub_flow.attributes] == [
        True,
        False,
    ]


SUB_FLOW_TARGET = SecretFlow()
WRITER_API_KEY = Attribute("writer-api-key", str, hard_delete_on_null=True)
WRITER_PLAIN = Attribute("writer-plain", str)


class WriteSecretStep(Step[str]):
    def wait_for(self, context: Context, input: str) -> Wait:
        if input == "sub-flow":
            return Wait.until(
                SubFlow.run(
                    SUB_FLOW_TARGET,
                    None,
                    SubFlowOptions().with_attribute(
                        SUB_FLOW_TARGET.api_key, "sub-flow-secret"
                    ),
                )
            )
        if input == "sub-flow-mismatch":
            return Wait.until(
                SubFlow.run(
                    SUB_FLOW_TARGET,
                    None,
                    SubFlowOptions().with_attribute(
                        Attribute("api-key", str), "inline-secret"
                    ),
                )
            )
        if input == "wait-for":
            WRITER_API_KEY.set(context, "wait-for-secret")
        if input == "plain":
            WRITER_PLAIN.set(context, "visible")
        return Wait.skip_immediately()

    def execute(self, context: Context, input: str) -> StepDecision:
        if input == "plain":
            WRITER_PLAIN.set(context, "visible")
        else:
            WRITER_API_KEY.delete(context)
        return graceful_complete()


class WriterFlow(Flow[str]):
    def __init__(self) -> None:
        self.write = WriteSecretStep()

    def get_steps(self) -> StepList[str]:
        return StepList.start_step(self.write)

    def get_persistence_schema(self) -> PersistenceSchema:
        return PersistenceSchema.of(WRITER_API_KEY, WRITER_PLAIN)

    @rpc
    def set_writer_api_key(self, context: Context, api_key: str) -> None:
        WRITER_API_KEY.set(context, api_key)


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

    @staticmethod
    def rpc_request(request: pb.InvokeWorkerRPCRequest) -> pb.InvokeWorkerRPCRequest:
        return request


class AsyncPassthroughHydrator:
    @staticmethod
    async def rpc_request(
        request: pb.InvokeWorkerRPCRequest,
    ) -> pb.InvokeWorkerRPCRequest:
        return request


def _writer_dispatcher(
    protocol_version: int,
) -> tuple[WorkerDispatcher, ValueMapper]:
    registry = Registry((WriterFlow(), SUB_FLOW_TARGET))
    values = ValueMapper(registry.codec_registry)
    dispatcher = WorkerDispatcher(
        registry,
        values,
        cast(Any, PassthroughHydrator()),
        _server_protocol(protocol_version),
    )
    return dispatcher, values


def _wait_for_result(
    dispatcher: WorkerDispatcher, values: ValueMapper, input: str
) -> pb.InvokeWaitForMethodResponse:
    outputs = list(
        dispatcher.invoke_wait_for(
            pb.InvokeWaitForMethodRequest(
                flow_type="WriterFlow",
                step_type="WriteSecretStep",
                step_input=values.encode(input, values.codec(str)),
            )
        )
    )
    return outputs[-1].result


def _execute_result(
    dispatcher: WorkerDispatcher, values: ValueMapper, input: str
) -> pb.InvokeExecuteMethodResponse:
    outputs = list(
        dispatcher.invoke_execute(
            pb.InvokeExecuteMethodRequest(
                flow_type="WriterFlow",
                step_type="WriteSecretStep",
                step_input=values.encode(input, values.codec(str)),
            )
        )
    )
    return outputs[-1].result


def _rpc_request(values: ValueMapper) -> pb.InvokeWorkerRPCRequest:
    return pb.InvokeWorkerRPCRequest(
        flow_type="WriterFlow",
        rpc_name="set_writer_api_key",
        input=values.encode("rpc-secret", values.codec(str)),
    )


PROTOCOL_2_WRITE_ERROR = (
    "hard-delete Attribute writes require Dex Server protocol version 3 or later, "
    "but the negotiated protocol version is 2"
)


def test_dispatcher_rejects_hard_delete_results_below_protocol_3() -> None:
    dispatcher, values = _writer_dispatcher(2)
    with pytest.raises(RuntimeError, match=f"{PROTOCOL_2_WRITE_ERROR}: writer-api-key"):
        _wait_for_result(dispatcher, values, "wait-for")
    with pytest.raises(RuntimeError, match=f"{PROTOCOL_2_WRITE_ERROR}: writer-api-key"):
        _execute_result(dispatcher, values, "delete")
    with pytest.raises(RuntimeError, match=f"{PROTOCOL_2_WRITE_ERROR}: writer-api-key"):
        dispatcher.invoke_rpc(_rpc_request(values))
    with pytest.raises(RuntimeError, match=f"{PROTOCOL_2_WRITE_ERROR}: api-key"):
        _wait_for_result(dispatcher, values, "sub-flow")

    plain_wait = _wait_for_result(dispatcher, values, "plain")
    assert [write.key for write in plain_wait.upsert_attributes] == ["writer-plain"]
    plain_execute = _execute_result(dispatcher, values, "plain")
    assert not plain_execute.upsert_attributes[0].is_hard_delete_on_null


def test_dispatcher_sends_hard_delete_results_on_protocol_3() -> None:
    dispatcher, values = _writer_dispatcher(3)
    wait_for = _wait_for_result(dispatcher, values, "wait-for")
    assert wait_for.upsert_attributes[0].is_hard_delete_on_null
    deleted = _execute_result(dispatcher, values, "delete")
    assert deleted.upsert_attributes[0].is_hard_delete_on_null
    assert deleted.upsert_attributes[0].value.WhichOneof("kind") == "null_value"
    rpc_response = dispatcher.invoke_rpc(_rpc_request(values))
    assert rpc_response.upsert_attributes[0].is_hard_delete_on_null
    sub_flow = _wait_for_result(dispatcher, values, "sub-flow")
    sub_flow_attributes = sub_flow.waiting_condition.sub_flow_conditions[
        0
    ].options.attributes
    assert [write.is_hard_delete_on_null for write in sub_flow_attributes] == [True]


def test_sub_flow_rejects_initial_attribute_with_mismatched_hard_delete() -> None:
    dispatcher, values = _writer_dispatcher(3)
    with pytest.raises(
        InvalidStepResultError,
        match=(
            "initial Attribute api-key uses hard_delete_on_null=False, but Flow "
            "SecretFlow registers it with hard_delete_on_null=True"
        ),
    ):
        _wait_for_result(dispatcher, values, "sub-flow-mismatch")


def test_start_flow_rejects_initial_attributes_with_mismatched_hard_delete() -> None:
    flow = SecretFlow()
    service = ProtocolService(current_protocol_version=3)
    server, port = _start_flow_server(service)
    client = Client(Registry((flow,)), cast(BlobCache, object()), _options(port))
    try:
        with pytest.raises(
            ValueError,
            match=(
                "initial Attribute api-key uses hard_delete_on_null=False, but Flow "
                "SecretFlow registers it with hard_delete_on_null=True"
            ),
        ):
            client.start_flow(
                flow,
                "flow-1",
                None,
                StartFlowOptions().with_attribute(Attribute("api-key", str), "leak"),
            )
        with pytest.raises(ValueError, match="initial Attribute token uses"):
            client.start_flow(
                flow,
                "flow-1",
                None,
                StartFlowOptions().with_attribute(
                    AttributeMap("token", str), "payments", "leak"
                ),
            )
        with pytest.raises(
            ValueError,
            match=(
                "initial Attribute plain uses hard_delete_on_null=True, but Flow "
                "SecretFlow registers it with hard_delete_on_null=False"
            ),
        ):
            client.start_flow(
                flow,
                "flow-1",
                None,
                StartFlowOptions().with_attribute(
                    Attribute("plain", str, hard_delete_on_null=True), "value"
                ),
            )
        client.start_flow(
            flow,
            "flow-2",
            None,
            StartFlowOptions()
            .with_attribute(Attribute("plain", str), "equal-definition")
            .with_attribute(Attribute("unregistered", str), "unregistered"),
        )
    finally:
        client.close()
        server.stop(grace=None).wait(timeout=5)
    assert service.calls == ["StartFlow:flow-2"]


def test_async_start_flow_rejects_initial_attribute_with_mismatched_hard_delete() -> (
    None
):
    asyncio.run(_test_async_start_flow_rejects_mismatched_hard_delete())


async def _test_async_start_flow_rejects_mismatched_hard_delete() -> None:
    flow = SecretFlow()
    client = AsyncClient(
        Registry((flow,), allow_async_handlers=True), cast(BlobCache, object())
    )
    try:
        with pytest.raises(ValueError, match="initial Attribute api-key uses"):
            await client.start_flow(
                flow,
                "flow-1",
                None,
                StartFlowOptions().with_attribute(Attribute("api-key", str), "leak"),
            )
    finally:
        await client.close()


def test_async_dispatcher_rejects_hard_delete_results_below_protocol_3() -> None:
    asyncio.run(_test_async_dispatcher_protocol_gate())


async def _test_async_dispatcher_protocol_gate() -> None:
    registry = Registry((WriterFlow(), SUB_FLOW_TARGET), allow_async_handlers=True)
    values = ValueMapper(registry.codec_registry)
    old_server = AsyncWorkerDispatcher(
        registry, values, cast(Any, AsyncPassthroughHydrator()), _server_protocol(2)
    )
    with pytest.raises(RuntimeError, match=f"{PROTOCOL_2_WRITE_ERROR}: writer-api-key"):
        await old_server.invoke_rpc(_rpc_request(values))
    new_server = AsyncWorkerDispatcher(
        registry, values, cast(Any, AsyncPassthroughHydrator()), _server_protocol(3)
    )
    response = await new_server.invoke_rpc(_rpc_request(values))
    assert response.upsert_attributes[0].is_hard_delete_on_null


def _server_protocol(version: int) -> NegotiatedServerProtocol:
    server_protocol = NegotiatedServerProtocol()
    server_protocol.record_version(version)
    return server_protocol


def test_client_wait_for_attribute_match_rejects_hard_delete_before_transport() -> None:
    flow = SecretFlow()
    service = ProtocolService(current_protocol_version=3)
    server, port = _start_flow_server(service)
    client = Client(Registry((flow,)), cast(BlobCache, object()), _options(port))
    try:
        with pytest.raises(ValueError, match="hard-delete Attribute api-key"):
            client.wait_for_attribute_match(
                "flow-1",
                flow.api_key,
                AttributeMatch.equal_to("sk-1"),
                WaitForAttributeOptions("wait-1", timedelta(seconds=1)),
            )
        with pytest.raises(ValueError, match="hard-delete Attribute token"):
            client.wait_for_attribute_match(
                "flow-1",
                flow.tokens,
                "payments",
                AttributeMatch.equal_to("tok-1"),
                WaitForAttributeOptions("wait-2", timedelta(seconds=1)),
            )
    finally:
        client.close()
        server.stop(grace=None).wait(timeout=5)
    assert service.calls == []


def test_async_client_wait_for_attribute_match_rejects_hard_delete() -> None:
    asyncio.run(_test_async_client_wait_for_attribute_match_rejects_hard_delete())


async def _test_async_client_wait_for_attribute_match_rejects_hard_delete() -> None:
    flow = SecretFlow()
    client = AsyncClient(
        Registry((flow,), allow_async_handlers=True), cast(BlobCache, object())
    )
    try:
        with pytest.raises(ValueError, match="hard-delete Attribute api-key"):
            await client.wait_for_attribute_match(
                "flow-1",
                flow.api_key,
                AttributeMatch.equal_to("sk-1"),
                WaitForAttributeOptions("wait-1", timedelta(seconds=1)),
            )
    finally:
        await client.close()


def test_client_rejects_hard_delete_start_on_protocol_2_server() -> None:
    flow = SecretFlow()
    service = ProtocolService(current_protocol_version=2)
    server, port = _start_flow_server(service)
    client = Client(Registry((flow,)), cast(BlobCache, object()), _options(port))
    try:
        with pytest.raises(
            RuntimeError,
            match="require Dex Server protocol version 3 or later.*version is 2: api-key",
        ):
            client.start_flow(
                flow,
                "flow-1",
                None,
                StartFlowOptions().with_attribute(flow.api_key, "sk-1"),
            )
        assert client.start_flow(flow, "flow-2", None) == "run-flow-2"
    finally:
        client.close()
        server.stop(grace=None).wait(timeout=5)
    assert service.calls == ["GetServerInfo", "StartFlow:flow-2"]


def test_client_negotiates_once_and_only_for_hard_delete_starts() -> None:
    flow = SecretFlow()
    service = ProtocolService(current_protocol_version=3)
    server, port = _start_flow_server(service)
    client = Client(Registry((flow,)), cast(BlobCache, object()), _options(port))
    try:
        assert client.start_flow(flow, "plain-1", None) == "run-plain-1"
        for flow_id in ("secret-1", "secret-2"):
            client.start_flow(
                flow,
                flow_id,
                None,
                StartFlowOptions().with_attribute(
                    flow.tokens, "payments", cast(str, None)
                ),
            )
    finally:
        client.close()
        server.stop(grace=None).wait(timeout=5)
    assert service.calls == [
        "StartFlow:plain-1",
        "GetServerInfo",
        "StartFlow:secret-1",
        "StartFlow:secret-2",
    ]
    assert all(
        request.flow_start_options.attributes[0].is_hard_delete_on_null
        for request in service.start_requests[1:]
    )


def test_client_does_not_cache_failed_server_info_request() -> None:
    flow = SecretFlow()
    service = ProtocolService(
        current_protocol_version=3, server_info_failure=grpc.StatusCode.UNAVAILABLE
    )
    server, port = _start_flow_server(service)
    client = Client(Registry((flow,)), cast(BlobCache, object()), _options(port))
    options = StartFlowOptions().with_attribute(flow.api_key, "sk-1")
    try:
        with pytest.raises(RuntimeError, match="GetServerInfo failed"):
            client.start_flow(flow, "secret-1", None, options)
        service.server_info_failure = None
        assert client.start_flow(flow, "secret-1", None, options) == "run-secret-1"
    finally:
        client.close()
        server.stop(grace=None).wait(timeout=5)
    assert service.calls == ["GetServerInfo", "GetServerInfo", "StartFlow:secret-1"]


def test_async_client_rejects_hard_delete_start_on_protocol_2_server() -> None:
    asyncio.run(_test_async_client_protocol_gate())


async def _test_async_client_protocol_gate() -> None:
    flow = SecretFlow()
    old_service = ProtocolService(current_protocol_version=2)
    old_server, old_port = _start_flow_server(old_service)
    new_service = ProtocolService(current_protocol_version=3)
    new_server, new_port = _start_flow_server(new_service)
    registry = Registry((flow,), allow_async_handlers=True)
    old_client = AsyncClient(registry, cast(BlobCache, object()), _options(old_port))
    new_client = AsyncClient(registry, cast(BlobCache, object()), _options(new_port))
    options = StartFlowOptions().with_attribute(flow.api_key, "sk-1")
    try:
        with pytest.raises(RuntimeError, match="protocol version 3 or later"):
            await old_client.start_flow(flow, "secret-1", None, options)
        await new_client.start_flow(flow, "secret-1", None, options)
        await new_client.start_flow(flow, "secret-2", None, options)
    finally:
        await old_client.close()
        await new_client.close()
        old_server.stop(grace=None).wait(timeout=5)
        new_server.stop(grace=None).wait(timeout=5)
    assert old_service.calls == ["GetServerInfo"]
    assert new_service.calls == [
        "GetServerInfo",
        "StartFlow:secret-1",
        "StartFlow:secret-2",
    ]


class ProtocolService(dex_pb2_grpc.FlowServiceServicer):
    def __init__(
        self,
        current_protocol_version: int,
        server_info_failure: grpc.StatusCode | None = None,
    ) -> None:
        self.current_protocol_version = current_protocol_version
        self.server_info_failure = server_info_failure
        self.calls: list[str] = []
        self.start_requests: list[pb.StartFlowRequest] = []
        self._lock = threading.Lock()

    def GetServerInfo(  # noqa: N802
        self,
        request: empty_pb2.Empty,
        context: grpc.ServicerContext,
    ) -> pb.ServerInfo:
        with self._lock:
            self.calls.append("GetServerInfo")
        if self.server_info_failure is not None:
            context.abort(self.server_info_failure, "server info failed")
        return pb.ServerInfo(
            server_version="test",
            minimum_supported_protocol_version=1,
            current_protocol_version=self.current_protocol_version,
        )

    def StartFlow(  # noqa: N802
        self,
        request: pb.StartFlowRequest,
        context: grpc.ServicerContext,
    ) -> pb.StartFlowResponse:
        with self._lock:
            self.calls.append(f"StartFlow:{request.flow_id}")
            self.start_requests.append(request)
        return pb.StartFlowResponse(run_id=f"run-{request.flow_id}")


def _start_flow_server(service: ProtocolService) -> tuple[grpc.Server, int]:
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


def _options(port: int) -> ClientOptions:
    return ClientOptions(server_address=f"127.0.0.1:{port}")
