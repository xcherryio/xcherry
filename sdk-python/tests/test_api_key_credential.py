# Copyright (c) 2026 Super Durable, Inc.
#
# Licensed under the Sustainable Use License 1.0.
# You may not use this file except in compliance with the License.
# See the LICENSE file in the repository root.
#
# SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

from __future__ import annotations

import asyncio
import json
import queue
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import cast

import grpc
import pytest
from google.protobuf import empty_pb2

from dex import (
    ApiKeyCredential,
    ApiKeyExchangeError,
    AsyncClient,
    BlobCache,
    Client,
    ClientOptions,
    DexServiceError,
)
from dex.dexpb import dex_pb2 as pb
from dex.dexpb import dex_pb2_grpc
from dex.flow import Registry

API_KEY = "never-print-api-key"
TOKEN_A = "never-print-token-a"
TOKEN_B = "never-print-token-b"


def token_response(token: str, lifetime: int = 60) -> dict[str, object]:
    return {"access_token": token, "token_type": "Bearer", "expires_in": lifetime}


class TokenEndpoint:
    def __init__(self) -> None:
        self.responses: queue.Queue[tuple[int, object]] = queue.Queue()
        self.requests: list[tuple[str, str, dict[str, str], bytes]] = []
        self._lock = threading.Lock()
        self.request_received = threading.Event()
        self.response_allowed = threading.Event()
        self.response_allowed.set()
        endpoint = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802
                body = self.rfile.read(int(self.headers["Content-Length"]))
                with endpoint._lock:
                    endpoint.requests.append(
                        (self.command, self.path, dict(self.headers.items()), body)
                    )
                    endpoint.request_received.set()
                endpoint.response_allowed.wait(3)
                status, payload = endpoint.responses.get(timeout=3)
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                try:
                    self.wfile.write(
                        payload
                        if isinstance(payload, bytes)
                        else json.dumps(payload).encode()
                    )
                except BrokenPipeError:
                    pass

            def log_message(self, format: str, *args: object) -> None:
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}/token"
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()

    def close(self) -> None:
        self.response_allowed.set()
        self.server.shutdown()
        self.thread.join()
        self.server.server_close()


@pytest.fixture
def token_endpoint() -> Iterator[TokenEndpoint]:
    endpoint = TokenEndpoint()
    try:
        yield endpoint
    finally:
        endpoint.close()


def test_api_key_request_and_validation(token_endpoint: TokenEndpoint) -> None:
    endpoint = token_endpoint
    endpoint.responses.put(
        (200, {**token_response(TOKEN_A), "token_type": "bEaReR", "ignored": True})
    )
    credential = ApiKeyCredential(endpoint.url, "server-id", API_KEY)
    try:
        assert credential.token() == TOKEN_A
        assert credential.token() == TOKEN_A
        assert len(endpoint.requests) == 1
        method, path, headers, body = endpoint.requests[0]
        assert method == "POST"
        assert path == "/token"
        assert headers["Content-Type"] == "application/json"
        assert headers["Accept"] == "application/json"
        assert body == b'{"server_id":"server-id","api_key":"never-print-api-key"}'
    finally:
        credential.close()

    for url in (
        "",
        "not-a-url",
        "ftp://example.com/token",
        "http://example.com/token",
        "http://192.168.1.2/token",
    ):
        with pytest.raises(ValueError):
            ApiKeyCredential(url, "server-id", API_KEY)
    for url in (
        "http://localhost/token",
        "http://127.1.2.3/token",
        "http://[::1]/token",
    ):
        ApiKeyCredential(url, "server-id", API_KEY).close()
    with pytest.raises(ValueError):
        ApiKeyCredential(endpoint.url, "", API_KEY)
    with pytest.raises(ValueError):
        ApiKeyCredential(endpoint.url, "server-id", "")
    with pytest.raises(ValueError):
        ApiKeyCredential(endpoint.url, "server-id", API_KEY, exchange_timeout=0)
    with pytest.raises(ValueError):
        ApiKeyCredential(
            endpoint.url, "server-id", API_KEY, exchange_timeout=float("nan")
        )
    with pytest.raises(ValueError):
        ApiKeyCredential(endpoint.url, " ", API_KEY)


@pytest.mark.parametrize(
    ("status", "payload"),
    [
        (201, token_response(TOKEN_A)),
        (200, b"not-json"),
        (200, []),
        (200, {"token_type": "Bearer", "expires_in": 10}),
        (200, {"access_token": "", "token_type": "Bearer", "expires_in": 10}),
        (200, {"access_token": 12, "token_type": "Bearer", "expires_in": 10}),
        (200, {"access_token": TOKEN_A, "expires_in": 10}),
        (200, {"access_token": TOKEN_A, "token_type": "Basic", "expires_in": 10}),
        (200, {"access_token": TOKEN_A, "token_type": "Bearer"}),
        (200, {"access_token": TOKEN_A, "token_type": "Bearer", "expires_in": 0}),
        (200, {"access_token": TOKEN_A, "token_type": "Bearer", "expires_in": True}),
        (200, {"access_token": TOKEN_A, "token_type": "Bearer", "expires_in": 1.5}),
    ],
)
def test_api_key_invalid_responses(
    token_endpoint: TokenEndpoint, status: int, payload: object
) -> None:
    token_endpoint.responses.put((status, payload))
    credential = ApiKeyCredential(token_endpoint.url, "server-id", API_KEY)
    try:
        with pytest.raises(ApiKeyExchangeError) as failure:
            credential.token()
        assert token_endpoint.url in str(failure.value)
        assert f"HTTP status {status}" in str(failure.value)
        assert API_KEY not in repr(failure.value)
        assert TOKEN_A not in repr(failure.value)
        assert API_KEY not in repr(credential)
        assert TOKEN_A not in repr(credential)
    finally:
        credential.close()


def test_api_key_endpoint_error_redacts_secrets(token_endpoint: TokenEndpoint) -> None:
    description = API_KEY + " " + TOKEN_A + " " + TOKEN_B + " " + "x" * 300
    token_endpoint.responses.put(
        (
            401,
            {
                "error": API_KEY,
                "error_description": description,
                "access_token": TOKEN_B,
            },
        )
    )
    credential = ApiKeyCredential(token_endpoint.url, "server-id", API_KEY)
    credential._token = TOKEN_A
    credential._expires_at = 0
    try:
        with pytest.raises(ApiKeyExchangeError) as failure:
            credential.token()
        message = str(failure.value)
        assert "HTTP status 401" in message
        assert "error=" in message and "error_description=" in message
        assert len(message.split("error_description=", 1)[1]) <= 200
        for value in (API_KEY, TOKEN_A, TOKEN_B, description):
            assert value not in message
            assert value not in repr(failure.value)
            assert value not in repr(credential)
    finally:
        credential.close()


def test_api_key_formatting_never_exposes_secrets(
    token_endpoint: TokenEndpoint,
) -> None:
    with pytest.raises(ValueError) as construction_failure:
        ApiKeyCredential("http://example.com/token", "server-id", API_KEY)
    credential = ApiKeyCredential(token_endpoint.url, "server-id", API_KEY)
    credential._token = TOKEN_A
    credential.close()
    with pytest.raises(RuntimeError) as closed_failure:
        credential.token()
    exchange_failure = ApiKeyExchangeError("token exchange failed")
    for value in (
        credential,
        construction_failure.value,
        closed_failure.value,
        exchange_failure,
    ):
        for formatted in (str(value), repr(value)):
            assert API_KEY not in formatted
            assert TOKEN_A not in formatted


def test_api_key_refresh_failure_and_expiry(token_endpoint: TokenEndpoint) -> None:
    token_endpoint.responses.put((200, token_response(TOKEN_A, 2)))
    token_endpoint.responses.put((503, {"error": "temporarily_unavailable"}))
    token_endpoint.responses.put((200, token_response(TOKEN_B, 2)))
    credential = ApiKeyCredential(token_endpoint.url, "server-id", API_KEY)
    try:
        assert credential.token() == TOKEN_A
        issued_at = credential._expires_at - 2
        credential._clock = lambda: issued_at + 1.61
        assert credential.token() == TOKEN_A
        refresh = credential._in_flight
        assert refresh is not None and refresh.event.wait(2)
        assert credential.token() == TOKEN_A
        assert len(token_endpoint.requests) == 2
        credential._clock = lambda: issued_at + 2.1
        assert credential.token() == TOKEN_B
        assert len(token_endpoint.requests) == 3
    finally:
        credential.close()


def test_api_key_failure_after_expiry(token_endpoint: TokenEndpoint) -> None:
    token_endpoint.responses.put((200, token_response(TOKEN_A, 2)))
    token_endpoint.responses.put((503, {"error": "unavailable"}))
    credential = ApiKeyCredential(token_endpoint.url, "server-id", API_KEY)
    try:
        assert credential.token() == TOKEN_A
        credential._clock = lambda: credential._expires_at + 0.1
        with pytest.raises(ApiKeyExchangeError, match="HTTP status 503"):
            credential.token()
        assert len(token_endpoint.requests) == 2
    finally:
        credential.close()


def test_api_key_transport_failure_and_timeout(token_endpoint: TokenEndpoint) -> None:
    unavailable = ApiKeyCredential("http://127.0.0.1:1/token", "server-id", API_KEY)
    with pytest.raises(ApiKeyExchangeError, match="HTTP status unavailable") as failure:
        unavailable.token()
    assert API_KEY not in str(failure.value)
    unavailable.close()

    token_endpoint.responses.put((200, token_response(TOKEN_A)))
    token_endpoint.response_allowed.clear()
    credential = ApiKeyCredential(
        token_endpoint.url, "server-id", API_KEY, exchange_timeout=0.1
    )
    try:
        with pytest.raises(ApiKeyExchangeError, match="HTTP status unavailable"):
            credential.token()
    finally:
        token_endpoint.response_allowed.set()
        credential.close()


def test_api_key_single_flight_and_rejected_token(
    token_endpoint: TokenEndpoint,
) -> None:
    token_endpoint.responses.put((200, token_response(TOKEN_A)))
    token_endpoint.responses.put((200, token_response(TOKEN_B)))
    token_endpoint.response_allowed.clear()
    credential = ApiKeyCredential(token_endpoint.url, "server-id", API_KEY)
    try:
        with ThreadPoolExecutor(max_workers=8) as executor:
            calls = [executor.submit(credential.token) for _ in range(8)]
            assert token_endpoint.request_received.wait(2)
            token_endpoint.response_allowed.set()
            assert [call.result(timeout=2) for call in calls] == [TOKEN_A] * 8
        assert len(token_endpoint.requests) == 1
        credential._reject_token(TOKEN_A)
        assert credential.token() == TOKEN_B
        credential._reject_token(TOKEN_A)
        assert credential.token() == TOKEN_B
        assert len(token_endpoint.requests) == 2
    finally:
        credential.close()


class RejectingFlowService(dex_pb2_grpc.FlowServiceServicer):
    def __init__(self, reject_all: bool = False) -> None:
        self.attempts: list[str | None] = []
        self.reject_all = reject_all

    def HealthCheck(  # noqa: N802
        self, request: empty_pb2.Empty, context: grpc.ServicerContext
    ) -> pb.HealthInfo:
        authorization = dict(context.invocation_metadata()).get("authorization")
        self.attempts.append(authorization)
        if authorization != f"Bearer {TOKEN_B}" or self.reject_all:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "rejected")
        return pb.HealthInfo()


@pytest.fixture
def rejecting_service() -> Iterator[tuple[str, RejectingFlowService]]:
    service = RejectingFlowService()
    server = grpc.server(ThreadPoolExecutor(max_workers=4))
    dex_pb2_grpc.add_FlowServiceServicer_to_server(service, server)  # type: ignore[no-untyped-call]
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    try:
        yield f"127.0.0.1:{port}", service
    finally:
        server.stop(grace=0).wait(timeout=3)


@pytest.mark.parametrize("reject_all", [False, True])
def test_api_key_client_retries_once(
    token_endpoint: TokenEndpoint,
    rejecting_service: tuple[str, RejectingFlowService],
    reject_all: bool,
) -> None:
    token_endpoint.responses.put((200, token_response(TOKEN_A)))
    token_endpoint.responses.put((200, token_response(TOKEN_B)))
    address, service = rejecting_service
    service.reject_all = reject_all
    credential = ApiKeyCredential(token_endpoint.url, "server-id", API_KEY)
    client = Client(
        Registry([]),
        cast(BlobCache, object()),
        ClientOptions(server_address=address, credential=credential),
    )
    try:
        if reject_all:
            with pytest.raises(DexServiceError) as failure:
                client.health_check()
            assert "rejected" in str(failure.value)
        else:
            assert client.health_check()
        assert service.attempts == [f"Bearer {TOKEN_A}", f"Bearer {TOKEN_B}"]
        assert len(token_endpoint.requests) == 2
    finally:
        client.close()
        credential.close()


def test_api_key_async_client_retries_once(
    token_endpoint: TokenEndpoint,
    rejecting_service: tuple[str, RejectingFlowService],
) -> None:
    token_endpoint.responses.put((200, token_response(TOKEN_A)))
    token_endpoint.responses.put((200, token_response(TOKEN_B)))
    address, service = rejecting_service
    credential = ApiKeyCredential(token_endpoint.url, "server-id", API_KEY)

    async def run() -> None:
        async with AsyncClient(
            Registry([]),
            cast(BlobCache, object()),
            ClientOptions(server_address=address, credential=credential),
        ) as client:
            assert await client.health_check()

    try:
        asyncio.run(run())
        assert service.attempts == [f"Bearer {TOKEN_A}", f"Bearer {TOKEN_B}"]
        assert len(token_endpoint.requests) == 2
    finally:
        credential.close()
