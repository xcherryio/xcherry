# Copyright (c) 2026 Super Durable, Inc.
#
# Licensed under the Sustainable Use License 1.0.
# You may not use this file except in compliance with the License.
# See the LICENSE file in the repository root.
#
# SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

from __future__ import annotations

import asyncio
import time
from datetime import timedelta
from typing import Any, cast

import grpc
import pytest
from google.protobuf import any_pb2
from google.rpc import status_pb2

from dex import (
    AsyncClient,
    Attribute,
    AttributeMatch,
    BlobCacheConfig,
    Client,
    ClientOptions,
    Registry,
    StepExecutionId,
    WaitForAttributeOptions,
    WaitForStepCompletionOptions,
)
from dex.dexpb import dex_pb2 as pb
from dex.runtime_errors import DexServiceError, RequestTimeoutError


class MemoryBlobCache:
    config = BlobCacheConfig("memory", 1_024)

    def get(self, blob_id: str) -> bytes | None:
        return None

    def put(self, blob_id: str, payload: bytes) -> bool:
        return True

    def delete(self, blob_id: str) -> None:
        return None

    def delete_all(self) -> None:
        return None

    def close(self) -> None:
        return None


class SyncWaitService:
    def __init__(self) -> None:
        self.requests: list[pb.WaitForAttributeRequest] = []

    def WaitForAttribute(  # noqa: N802
        self, request: pb.WaitForAttributeRequest, *, timeout: float | None = None
    ) -> pb.WaitForAttributeResponse:
        self.requests.append(request)
        if len(self.requests) == 1:
            raise _long_poll_timeout()
        return pb.WaitForAttributeResponse(matched_value=pb.Value(int_value=7))


class AsyncWaitService:
    def __init__(self) -> None:
        self.requests: list[pb.WaitForAttributeRequest] = []

    async def WaitForAttribute(  # noqa: N802
        self, request: pb.WaitForAttributeRequest, *, timeout: float | None = None
    ) -> pb.WaitForAttributeResponse:
        self.requests.append(request)
        if len(self.requests) == 1:
            raise _long_poll_timeout()
        return pb.WaitForAttributeResponse(matched_value=pb.Value(int_value=7))


class SyncStepWaitService:
    def __init__(self) -> None:
        self.requests: list[pb.WaitForStepCompletionRequest] = []

    def WaitForStepCompletion(  # noqa: N802
        self, request: pb.WaitForStepCompletionRequest, *, timeout: float | None = None
    ) -> pb.WaitForStepCompletionResponse:
        self.requests.append(request)
        if len(self.requests) == 1:
            raise _long_poll_timeout()
        return pb.WaitForStepCompletionResponse()


class AsyncStepWaitService:
    def __init__(self) -> None:
        self.requests: list[pb.WaitForStepCompletionRequest] = []

    async def WaitForStepCompletion(  # noqa: N802
        self, request: pb.WaitForStepCompletionRequest, *, timeout: float | None = None
    ) -> pb.WaitForStepCompletionResponse:
        self.requests.append(request)
        if len(self.requests) == 1:
            raise _long_poll_timeout()
        return pb.WaitForStepCompletionResponse()


def test_sync_attribute_wait_reattaches_and_returns_matched_value() -> None:
    service = SyncWaitService()
    client = Client(Registry(()), MemoryBlobCache(), ClientOptions("unused:1"))
    client._service = cast(Any, service)
    try:
        options = WaitForAttributeOptions()
        matched = client.wait_for_attribute_match(
            "flow-1",
            Attribute("revision", int),
            AttributeMatch.greater_than(5),
            options,
        )
        assert matched == 7
        assert [request.request_id for request in service.requests] == [
            "",
            "",
        ]
    finally:
        client.close()


def test_async_attribute_wait_reattaches_and_returns_matched_value() -> None:
    asyncio.run(_test_async_attribute_wait_reattaches_and_returns_matched_value())


async def _test_async_attribute_wait_reattaches_and_returns_matched_value() -> None:
    service = AsyncWaitService()
    client = AsyncClient(Registry(()), MemoryBlobCache(), ClientOptions("unused:1"))
    client._service = cast(Any, service)
    try:
        options = WaitForAttributeOptions(request_id="wait-revision")
        matched = await client.wait_for_attribute_match(
            "flow-1",
            Attribute("revision", int),
            AttributeMatch.greater_than(5),
            options,
        )
        assert matched == 7
        assert [request.request_id for request in service.requests] == [
            options.request_id,
            options.request_id,
        ]
    finally:
        await client.close()


def test_step_wait_omits_request_id_for_server_default() -> None:
    service = SyncStepWaitService()
    client = Client(Registry(()), MemoryBlobCache(), ClientOptions("unused:1"))
    client._service = cast(Any, service)
    try:
        client.wait_for_step_completion(
            "flow-1",
            StepExecutionId("Step", 2),
            WaitForStepCompletionOptions(),
        )
        assert [request.request_id for request in service.requests] == [
            "",
            "",
        ]
    finally:
        client.close()


def test_async_step_wait_omits_request_id_for_server_default() -> None:
    asyncio.run(_test_async_step_wait_omits_request_id_for_server_default())


async def _test_async_step_wait_omits_request_id_for_server_default() -> None:
    service = AsyncStepWaitService()
    client = AsyncClient(Registry(()), MemoryBlobCache(), ClientOptions("unused:1"))
    client._service = cast(Any, service)
    try:
        await client.wait_for_step_completion(
            "flow-1",
            StepExecutionId("Step", 2),
            WaitForStepCompletionOptions(),
        )
        assert [request.request_id for request in service.requests] == [
            "",
            "",
        ]
    finally:
        await client.close()


def test_finite_step_wait_uses_server_default_request_id() -> None:
    service = SyncStepWaitService()
    client = Client(Registry(()), MemoryBlobCache(), ClientOptions("unused:1"))
    client._service = cast(Any, service)
    try:
        client.wait_for_step_completion(
            "flow-1",
            StepExecutionId("Step", 1),
            WaitForStepCompletionOptions(
                request_timeout=timedelta(seconds=1),
                internal_handler_timeout=timedelta(seconds=3),
            ),
        )
        assert all(not request.request_id for request in service.requests)
        assert all(request.request_timeout_seconds == 1 for request in service.requests)
        assert all(
            request.internal_handler_timeout_seconds == 3
            for request in service.requests
        )
    finally:
        client.close()


class FakeRpcError(grpc.RpcError):
    def code(self) -> grpc.StatusCode:
        return grpc.StatusCode.DEADLINE_EXCEEDED

    def details(self) -> str:
        return "long poll timed out"

    def trailing_metadata(self) -> tuple[Any, ...]:
        response = pb.ServiceErrorResponse(
            detail="long poll timed out",
            sub_status=pb.ERROR_SUB_STATUS_LONG_POLL_TIME_OUT,
        )
        packed = any_pb2.Any()
        packed.Pack(response)
        status = status_pb2.Status(
            code=grpc.StatusCode.DEADLINE_EXCEEDED.value[0],
            message="long poll timed out",
            details=[packed],
        )
        return (("grpc-status-details-bin", status.SerializeToString()),)


def _long_poll_timeout() -> FakeRpcError:
    return FakeRpcError()


class CancelledRpcError(grpc.RpcError):
    def code(self) -> grpc.StatusCode:
        return grpc.StatusCode.CANCELLED

    def details(self) -> str:
        return "Stream removed (RST_STREAM (Received RST_STREAM with error code 8))"

    def trailing_metadata(self) -> tuple[Any, ...]:
        return ()


class SyncCancelledWaitService:
    def __init__(self, delay_seconds: float) -> None:
        self.delay_seconds = delay_seconds

    def WaitForAttribute(  # noqa: N802
        self, request: pb.WaitForAttributeRequest, *, timeout: float | None = None
    ) -> pb.WaitForAttributeResponse:
        time.sleep(self.delay_seconds)
        raise CancelledRpcError()


class AsyncCancelledWaitService:
    def __init__(self, delay_seconds: float) -> None:
        self.delay_seconds = delay_seconds

    async def WaitForAttribute(  # noqa: N802
        self, request: pb.WaitForAttributeRequest, *, timeout: float | None = None
    ) -> pb.WaitForAttributeResponse:
        await asyncio.sleep(self.delay_seconds)
        raise CancelledRpcError()


def test_cancelled_wait_is_a_timeout_only_after_the_request_budget_expires() -> None:
    attribute = Attribute("count", int)
    options = WaitForAttributeOptions("wait-1", timedelta(seconds=1))
    client = Client(Registry(()), MemoryBlobCache(), ClientOptions("unused:1"))
    try:
        client._service = cast(Any, SyncCancelledWaitService(delay_seconds=1.1))
        with pytest.raises(RequestTimeoutError):
            client.wait_for_attribute_match(
                "flow-1", attribute, AttributeMatch.equal_to(1), options
            )
        client._service = cast(Any, SyncCancelledWaitService(delay_seconds=0))
        with pytest.raises(DexServiceError) as remaining_budget_error:
            client.wait_for_attribute_match(
                "flow-1", attribute, AttributeMatch.equal_to(1), options
            )
        assert not isinstance(remaining_budget_error.value, RequestTimeoutError)
        assert remaining_budget_error.value.code is grpc.StatusCode.CANCELLED
    finally:
        client.close()


def test_async_cancelled_wait_is_a_timeout_only_after_the_request_budget_expires() -> (
    None
):
    asyncio.run(_async_cancelled_wait_is_a_timeout_only_after_budget())


async def _async_cancelled_wait_is_a_timeout_only_after_budget() -> None:
    attribute = Attribute("count", int)
    options = WaitForAttributeOptions("wait-1", timedelta(seconds=1))
    client = AsyncClient(Registry(()), MemoryBlobCache(), ClientOptions("unused:1"))
    try:
        client._service = cast(Any, AsyncCancelledWaitService(delay_seconds=1.1))
        with pytest.raises(RequestTimeoutError):
            await client.wait_for_attribute_match(
                "flow-1", attribute, AttributeMatch.equal_to(1), options
            )
        client._service = cast(Any, AsyncCancelledWaitService(delay_seconds=0))
        with pytest.raises(DexServiceError) as remaining_budget_error:
            await client.wait_for_attribute_match(
                "flow-1", attribute, AttributeMatch.equal_to(1), options
            )
        assert not isinstance(remaining_budget_error.value, RequestTimeoutError)
        assert remaining_budget_error.value.code is grpc.StatusCode.CANCELLED
    finally:
        await client.close()
