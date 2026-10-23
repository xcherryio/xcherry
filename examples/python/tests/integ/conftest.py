# Copyright (c) 2022-2026 Super Durable, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Shared harness for the example integration tests.

Every test in this package needs a running Dex server. Point
DEX_FLOW_SERVICE_ADDRESS at it (default 127.0.0.1:8801); when the server is
unreachable the whole package is skipped instead of failing.
"""

from __future__ import annotations

import asyncio
import os
import socket
import sys
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
import pytest_asyncio
from dex import AsyncClient, DexServiceError, FlowNotFoundError, FlowStatus

from dex_examples.app import ExampleApp
from dex_examples.config import ExamplesConfig
from dex_examples.shared.worker_mode import read_worker_mode_from_environment

DEFAULT_SERVER_ADDRESS = "127.0.0.1:8801"

WAIT_TIMEOUT = timedelta(seconds=45)
# Flows that start child Flows inherit the child's random timer, up to a minute.
LONG_WAIT_TIMEOUT = timedelta(seconds=150)
SERVER_READY_TIMEOUT = timedelta(seconds=20)

POLL_INTERVAL_SECONDS = 0.5


def server_address() -> str:
    return os.environ.get("DEX_FLOW_SERVICE_ADDRESS", DEFAULT_SERVER_ADDRESS)


def skip_when_server_zd(feature: str) -> None:
    """Skips a test whose sample uses a feature that server-zd rejects."""
    if os.environ.get("DEX_INTEG_ENGINE") == "zd":
        pytest.skip(f"server-zd does not support {feature}")


@pytest_asyncio.fixture(scope="session")
async def example_app(
    tmp_path_factory: pytest.TempPathFactory,
) -> AsyncIterator[ExampleApp]:
    """Boots one ExampleApp with every Flow on an ephemeral Worker port."""
    config = ExamplesConfig(
        server_address=server_address(),
        # Reserve first so the readiness probe targets the Worker's actual port.
        worker_bind_address=_available_worker_address(),
        worker_target=None,
        http_address="127.0.0.1:0",
        blob_cache_dir=tmp_path_factory.mktemp("dex-examples-blob-cache"),
        worker_mode=read_worker_mode_from_environment(),
        agent_mcp_config=_agent_mcp_config(tmp_path_factory),
    )
    app = ExampleApp(config)
    if config.worker_mode.is_pull:
        # Pull registration fails without a server, so probe first to keep the skip.
        is_server_ready = await server_is_ready(app.client)
        if is_server_ready:
            await app.start_worker()
    else:
        await app.start_worker()
        is_server_ready = await server_is_ready(app.client)
    if not is_server_ready:
        await app.close()
        pytest.skip(
            "no Dex server at "
            f"{config.server_address}; set DEX_FLOW_SERVICE_ADDRESS to a running "
            "Dex server to run the example integration tests"
        )
    try:
        yield app
    finally:
        await app.close()


def _agent_mcp_config(tmp_path_factory: pytest.TempPathFactory) -> Path:
    config_path = tmp_path_factory.mktemp("dex-agent-mcp") / "servers.yaml"
    server_path = Path(__file__).with_name("ai_agent_mcp_server.py")
    config_path.write_text(
        f"""
servers:
  - name: test
    transport: stdio
    command: {sys.executable}
    args: [{server_path}]
    trust_read_only_annotations: true
""".strip()
    )
    return config_path


@pytest_asyncio.fixture(scope="session")
async def app(example_app: ExampleApp) -> ExampleApp:
    return example_app


@pytest_asyncio.fixture(scope="session")
async def client(example_app: ExampleApp) -> AsyncClient:
    return example_app.client


@pytest.fixture()
def new_flow_id() -> Callable[[str], str]:
    """Returns a factory for Flow IDs that never collide across runs."""

    def make(prefix: str) -> str:
        return f"{prefix}-{uuid4().hex}"

    return make


async def server_is_ready(client: AsyncClient) -> bool:
    deadline = asyncio.get_running_loop().time() + SERVER_READY_TIMEOUT.total_seconds()
    while asyncio.get_running_loop().time() < deadline:
        try:
            return await client.health_check()
        except DexServiceError:
            await asyncio.sleep(POLL_INTERVAL_SECONDS)
    return False


def _available_worker_address() -> str:
    with socket.socket() as worker_socket:
        worker_socket.bind(("127.0.0.1", 0))
        host, port = worker_socket.getsockname()
    return f"{host}:{port}"


async def wait_until(
    description: str,
    predicate: Callable[[], Awaitable[Any]],
    timeout: timedelta = WAIT_TIMEOUT,
) -> Any:
    """Returns the first truthy value from predicate, or fails the test."""
    deadline = asyncio.get_running_loop().time() + timeout.total_seconds()
    while True:
        value = await predicate()
        if value:
            return value
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(f"timed out waiting for {description}")
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


async def flow_status_or_none(client: AsyncClient, flow_id: str) -> FlowStatus | None:
    try:
        return (await client.describe_flow(flow_id)).status
    except FlowNotFoundError:
        return None


async def wait_for_flow_status(
    client: AsyncClient,
    flow_id: str,
    status: FlowStatus,
    timeout: timedelta = WAIT_TIMEOUT,
) -> None:
    await wait_until(
        f"Flow {flow_id} to reach {status.value}",
        lambda: _status_matches(client, flow_id, status),
        timeout,
    )


async def _status_matches(
    client: AsyncClient, flow_id: str, status: FlowStatus
) -> bool:
    return await flow_status_or_none(client, flow_id) is status
