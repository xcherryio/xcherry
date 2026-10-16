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
import queue
import threading
from collections.abc import Callable
from concurrent.futures import Future
from typing import Any, NamedTuple, cast

import grpc

from dex.credential import Credential

_TOKEN_WORKERS = 4


class AsyncCredentialRunner:
    def __init__(self, credential: Credential) -> None:
        self._credential = credential
        self._lock = threading.Lock()
        self._jobs: queue.SimpleQueue[Future[Any] | None] = queue.SimpleQueue()
        self._threads: list[threading.Thread] = []
        self._pending: set[Future[Any]] = set()
        self._closed = False

    async def token(self) -> str:
        token_method = self._credential.token
        if inspect.iscoroutinefunction(token_method) or inspect.iscoroutinefunction(
            getattr(token_method, "__call__", None)
        ):
            token = token_method()
            if inspect.isawaitable(token):
                return await token
            return token
        token = await asyncio.wrap_future(self._submit())
        if inspect.isawaitable(token):
            if isinstance(token, asyncio.Future):
                try:
                    token_loop = token.get_loop()
                except RuntimeError as error:
                    raise TypeError(
                        "loop-bound credentials must use async def token()"
                    ) from error
                if token_loop is not asyncio.get_running_loop():
                    raise TypeError("loop-bound credentials must use async def token()")
            return await token
        return token

    def _submit(self) -> Future[Any]:
        with self._lock:
            if self._closed:
                raise RuntimeError("credential runner is closed")
            future: Future[Any] = Future()
            self._pending.add(future)
            if len(self._threads) < _TOKEN_WORKERS:
                thread = threading.Thread(
                    target=self._run,
                    name="dex-python-credential",
                    daemon=True,
                )
                self._threads.append(thread)
                thread.start()
            self._jobs.put(future)
            return future

    def _run(self) -> None:
        while True:
            future = self._jobs.get()
            if future is None:
                return
            with self._lock:
                if future.done() or not future.set_running_or_notify_cancel():
                    self._pending.discard(future)
                    continue
            try:
                token = self._credential.token()
                failure = None
            except BaseException as error:
                token = None
                failure = error
            with self._lock:
                if not future.done():
                    if failure is None:
                        future.set_result(token)
                    else:
                        future.set_exception(failure)
                self._pending.discard(future)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            for future in self._pending:
                if not future.done():
                    future.set_exception(RuntimeError("credential runner is closed"))
            self._pending.clear()
            while True:
                try:
                    self._jobs.get_nowait()
                except queue.Empty:
                    break
            for _ in self._threads:
                self._jobs.put(None)


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
    runner: AsyncCredentialRunner, metadata: Any = None, timeout: float | None = None
) -> Any:
    if timeout is None:
        token = await runner.token()
    else:
        lookup = asyncio.create_task(runner.token())
        try:
            completed, _ = await asyncio.wait({lookup}, timeout=timeout)
            if not completed:
                lookup.cancel()
                await asyncio.gather(lookup, return_exceptions=True)
                raise credential_deadline_error()
            token = lookup.result()
        except BaseException:
            lookup.cancel()
            await asyncio.gather(lookup, return_exceptions=True)
            raise
    return tuple(metadata or ()) + (("authorization", "Bearer " + token),)


def credential_deadline_error() -> grpc.aio.AioRpcError:
    return grpc.aio.AioRpcError(
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.aio.Metadata(),
        grpc.aio.Metadata(),
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
    def __init__(self, runner: AsyncCredentialRunner) -> None:
        self._runner = runner

    async def intercept_unary_unary(
        self,
        continuation: Callable[..., Any],
        client_call_details: grpc.aio.ClientCallDetails,
        request: Any,
    ) -> Any:
        loop = asyncio.get_running_loop()
        started_at = loop.time()
        metadata = await async_credential_metadata(
            self._runner, client_call_details.metadata, client_call_details.timeout
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
    server_address: str, credential_runner: AsyncCredentialRunner | None
) -> grpc.aio.Channel:
    if credential_runner is None:
        return grpc.aio.insecure_channel(server_address)
    return grpc.aio.insecure_channel(
        server_address,
        interceptors=[
            cast(
                grpc.aio.ClientInterceptor,
                _AsyncCredentialInterceptor(credential_runner),
            )
        ],
    )
