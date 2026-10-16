# Portions of this file are derived from indeedeng/iwf-java-sdk.
# Those portions are licensed under the Apache License, Version 2.0.
# See LICENSES/Apache-2.0.txt and LEGACY_NOTICES.md.
#
# Modifications Copyright (c) 2026 Super Durable, Inc.
#
# Modifications are licensed under the Sustainable Use License 1.0.
# Third-Party Materials remain under the Apache License, Version 2.0.
# See LICENSE and LEGACY_NOTICES.md.

from __future__ import annotations

import asyncio
import os
import time
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from typing import Callable, Iterator, TypeVar

import grpc

from dex import Registry, StartFlowOptions
from dex.dexpb import dex_pb2 as pb
from dex.dexpb import dex_pb2_grpc as pb_grpc

from .async_environment import AsyncDexDevTestEnvironment
from .environment import DexDevTestEnvironment
from .hard_delete_attribute_flow import (
    ALL_SECRETS,
    API_KEY,
    BILLING_TOKEN,
    EXTERNAL_BILLING_TOKEN,
    INITIAL_API_KEY,
    INITIAL_PAYMENTS_TOKEN,
    PAYMENTS_TOKEN,
    RPC_API_KEY,
    STEP_API_KEY,
    TOKENS,
    HardDeleteAttributeFlow,
    SecretSnapshot,
)
from .shared import unique_id

ValueT = TypeVar("ValueT")
WAIT_TIMEOUT = timedelta(seconds=30)
PAYMENTS_KEY = Registry.physical_name(TOKENS.name, PAYMENTS_TOKEN)
BILLING_KEY = Registry.physical_name(TOKENS.name, BILLING_TOKEN)


def test_hard_delete_attributes_store_read_overwrite_and_delete() -> None:
    flow = HardDeleteAttributeFlow()
    with DexDevTestEnvironment(flow) as environment:
        client = environment.client
        flow_id = unique_id("hard-delete-attributes")
        client.start_flow(
            flow,
            flow_id,
            None,
            StartFlowOptions()
            .with_attribute(API_KEY, INITIAL_API_KEY)
            .with_attribute(TOKENS, PAYMENTS_TOKEN, INITIAL_PAYMENTS_TOKEN),
        )

        observations = _eventually(
            lambda: client.invoke_rpc(flow.get_step_observations, flow_id),
            lambda value: value is not None,
        )
        assert observations == "ok,ok"
        assert client.invoke_rpc(flow.get_secret_snapshot, flow_id) == SecretSnapshot(
            STEP_API_KEY, INITIAL_PAYMENTS_TOKEN, None
        )
        assert _present_attributes(flow_id) == {
            API_KEY.name: pb.Value(string_value=STEP_API_KEY),
            PAYMENTS_KEY: pb.Value(string_value=INITIAL_PAYMENTS_TOKEN),
        }

        with _flow_service() as flow_service:
            flow_service.SetAttributes(
                pb.SetAttributesRequest(
                    flow_id=flow_id,
                    request_id=unique_id("set-billing-token"),
                    attributes=[
                        pb.AttributeWrite(
                            key=BILLING_KEY,
                            value=pb.Value(string_value=EXTERNAL_BILLING_TOKEN),
                            is_hard_delete_on_null=True,
                        )
                    ],
                )
            )
        assert client.invoke_rpc(flow.get_secret_snapshot, flow_id) == SecretSnapshot(
            STEP_API_KEY, INITIAL_PAYMENTS_TOKEN, EXTERNAL_BILLING_TOKEN
        )

        client.invoke_rpc(flow.set_api_key, flow_id, RPC_API_KEY)
        assert client.invoke_rpc(flow.get_secret_snapshot, flow_id).api_key == (
            RPC_API_KEY
        )
        assert _present_attributes(flow_id)[API_KEY.name] == pb.Value(
            string_value=RPC_API_KEY
        )
        client.invoke_rpc(flow.clear_api_key, flow_id)
        assert client.invoke_rpc(flow.get_secret_snapshot, flow_id).api_key is None
        assert API_KEY.name not in _present_attributes(flow_id)
        client.invoke_rpc(flow.set_api_key, flow_id, RPC_API_KEY)
        client.invoke_rpc(flow.delete_api_key, flow_id)
        assert client.invoke_rpc(flow.get_secret_snapshot, flow_id).api_key is None
        assert API_KEY.name not in _present_attributes(flow_id)

        client.invoke_rpc(flow.set_api_key, flow_id, RPC_API_KEY)
        client.invoke_rpc(flow.request_secret_deletion, flow_id)
        _eventually(
            lambda: client.invoke_rpc(flow.get_secret_snapshot, flow_id),
            lambda snapshot: snapshot == SecretSnapshot(None, None, None),
        )
        assert _present_attributes(flow_id) == {}

        client.invoke_rpc(flow.finish, flow_id)
        assert (
            client.wait_for_flow(flow_id, WAIT_TIMEOUT).single_output(str)
            == "ok,ok,ok,ok"
        )
        _assert_cache_excludes_secrets(Path(environment._cache_directory.name))


def test_async_client_starts_and_deletes_hard_delete_attributes() -> None:
    asyncio.run(_async_client_starts_and_deletes_hard_delete_attributes())


async def _async_client_starts_and_deletes_hard_delete_attributes() -> None:
    flow = HardDeleteAttributeFlow()
    async with AsyncDexDevTestEnvironment(flow) as environment:
        client = environment.client
        flow_id = unique_id("async-hard-delete-attributes")
        await client.start_flow(
            flow,
            flow_id,
            None,
            StartFlowOptions()
            .with_attribute(API_KEY, INITIAL_API_KEY)
            .with_attribute(TOKENS, PAYMENTS_TOKEN, INITIAL_PAYMENTS_TOKEN),
        )
        deadline = time.monotonic() + WAIT_TIMEOUT.total_seconds()
        while await client.invoke_rpc(flow.get_step_observations, flow_id) is None:
            assert time.monotonic() < deadline
            await asyncio.sleep(0.1)
        assert await client.invoke_rpc(flow.get_step_observations, flow_id) == "ok,ok"
        await client.invoke_rpc(flow.delete_api_key, flow_id)
        assert await client.invoke_rpc(
            flow.get_secret_snapshot, flow_id
        ) == SecretSnapshot(None, INITIAL_PAYMENTS_TOKEN, None)
        await client.invoke_rpc(flow.request_secret_deletion, flow_id)
        await client.invoke_rpc(flow.finish, flow_id)
        result = await client.wait_for_flow(flow_id, WAIT_TIMEOUT)
        assert result.single_output(str) == "ok,ok,ok,ok"
        assert _present_attributes(flow_id) == {}
        _assert_cache_excludes_secrets(Path(environment._cache_directory.name))


@contextmanager
def _flow_service() -> Iterator[pb_grpc.FlowServiceStub]:
    server_address = os.environ.get("DEX_SERVER_ADDRESS", "127.0.0.1:8801")
    with grpc.insecure_channel(server_address) as channel:
        yield pb_grpc.FlowServiceStub(channel)  # type: ignore[no-untyped-call]


def _present_attributes(flow_id: str) -> dict[str, pb.Value]:
    with _flow_service() as flow_service:
        response = flow_service.GetAttributes(
            pb.GetAttributesRequest(
                flow_id=flow_id,
                keys=[API_KEY.name, PAYMENTS_KEY, BILLING_KEY],
            )
        )
    present = {
        entry.key: entry.value
        for entry in response.attributes
        if entry.HasField("value") and entry.value.WhichOneof("kind") != "null_value"
    }
    for value in present.values():
        assert value.WhichOneof("kind") == "string_value"
    return present


def _assert_cache_excludes_secrets(cache_directory: Path) -> None:
    for path in cache_directory.rglob("*"):
        if not path.is_file():
            continue
        payload = path.read_bytes()
        for secret in ALL_SECRETS:
            assert secret.encode("utf-8") not in payload, path


def _eventually(
    read: Callable[[], ValueT],
    is_ready: Callable[[ValueT], bool],
) -> ValueT:
    deadline = time.monotonic() + WAIT_TIMEOUT.total_seconds()
    while True:
        value = read()
        if is_ready(value):
            return value
        if time.monotonic() >= deadline:
            raise AssertionError(f"condition was not reached; last value: {value!r}")
        time.sleep(0.1)
