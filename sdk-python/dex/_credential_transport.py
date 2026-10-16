# Copyright (c) 2026 Super Durable, Inc.
#
# Licensed under the Sustainable Use License 1.0.
# You may not use this file except in compliance with the License.
# See the LICENSE file in the repository root.
#
# SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

"""Attach bearer metadata to outbound gRPC calls."""

# mypy: disable-error-code=type-arg

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from typing import Any, NamedTuple

import grpc

from dex.credential import Credential


class _SyncCallDetails(NamedTuple):
    method: str
    timeout: float | None
    metadata: Any
    credentials: grpc.CallCredentials | None
    wait_for_ready: bool | None
    compression: grpc.Compression | None


def credential_metadata(credential: Credential | None, metadata: Any = None) -> Any:
    if credential is None:
        return metadata
    token = credential.token()
    if inspect.isawaitable(token):
        if inspect.iscoroutine(token):
            token.close()
        raise TypeError("synchronous calls require a synchronous credential")
    return tuple(metadata or ()) + (("authorization", "Bearer " + token),)


async def async_credential_metadata(
    credential: Credential, metadata: Any = None, timeout: float | None = None
) -> Any:
    try:
        token = await asyncio.wait_for(
            _resolve_async_token(credential), timeout=timeout
        )
    except TimeoutError as error:
        raise credential_deadline_error() from error
    return tuple(metadata or ()) + (("authorization", "Bearer " + token),)


async def _resolve_async_token(credential: Credential) -> str:
    token = await asyncio.to_thread(credential.token)
    if inspect.isawaitable(token):
        return await token
    return token


def credential_deadline_error() -> grpc.aio.AioRpcError:
    return grpc.aio.AioRpcError(
        grpc.StatusCode.DEADLINE_EXCEEDED,
        None,
        None,
        "credential lookup exceeded RPC deadline",
        None,
    )


class _SyncCredentialInterceptor(grpc.UnaryUnaryClientInterceptor):
    def __init__(self, credential: Credential) -> None:
        self._credential = credential

    def intercept_unary_unary(
        self,
        continuation: Callable[..., Any],
        client_call_details: grpc.ClientCallDetails,
        request: Any,
    ) -> Any:
        details = _SyncCallDetails(
            client_call_details.method,
            client_call_details.timeout,
            credential_metadata(self._credential, client_call_details.metadata),
            client_call_details.credentials,
            client_call_details.wait_for_ready,
            client_call_details.compression,
        )
        return continuation(details, request)


class _AsyncCredentialInterceptor(grpc.aio.UnaryUnaryClientInterceptor):
    def __init__(self, credential: Credential) -> None:
        self._credential = credential

    async def intercept_unary_unary(
        self,
        continuation: Callable[..., Any],
        client_call_details: grpc.aio.ClientCallDetails,
        request: Any,
    ) -> Any:
        loop = asyncio.get_running_loop()
        started_at = loop.time()
        metadata = await async_credential_metadata(
            self._credential, client_call_details.metadata, client_call_details.timeout
        )
        remaining_timeout = client_call_details.timeout
        if remaining_timeout is not None:
            remaining_timeout -= loop.time() - started_at
            if remaining_timeout <= 0:
                raise credential_deadline_error()
        details = grpc.aio.ClientCallDetails(
            client_call_details.method,
            remaining_timeout,
            metadata,
            client_call_details.credentials,
            client_call_details.wait_for_ready,
        )
        return await continuation(details, request)


def sync_channel(server_address: str, credential: Credential | None) -> grpc.Channel:
    channel = grpc.insecure_channel(server_address)
    if credential is None:
        return channel
    return grpc.intercept_channel(channel, _SyncCredentialInterceptor(credential))


def async_channel(
    server_address: str, credential: Credential | None
) -> grpc.aio.Channel:
    if credential is None:
        return grpc.aio.insecure_channel(server_address)
    return grpc.aio.insecure_channel(
        server_address, interceptors=[_AsyncCredentialInterceptor(credential)]
    )
