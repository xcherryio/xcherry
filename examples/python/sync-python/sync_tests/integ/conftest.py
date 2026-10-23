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

"""Sync Client/Worker harness for the sync-python showcase integ tests."""

from __future__ import annotations

import os
import socket
import time
from collections.abc import Callable, Iterator
from datetime import timedelta
from uuid import uuid4

import pytest
from dex import Client, DexServiceError
from dex_examples.shared.worker_mode import read_worker_mode_from_environment

from sync_examples.app import SyncExampleApp
from sync_examples.config import SyncExamplesConfig

DEFAULT_SERVER_ADDRESS = "127.0.0.1:8801"
SERVER_READY_TIMEOUT = timedelta(seconds=20)
POLL_INTERVAL_SECONDS = 0.5


@pytest.fixture(scope="session")
def sync_app(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SyncExampleApp]:
    config = SyncExamplesConfig(
        server_address=os.environ.get(
            "DEX_FLOW_SERVICE_ADDRESS", DEFAULT_SERVER_ADDRESS
        ),
        worker_bind_address=_available_worker_address(),
        worker_target=None,
        http_address="127.0.0.1:0",
        blob_cache_dir=tmp_path_factory.mktemp("dex-sync-examples-blob-cache"),
        worker_mode=read_worker_mode_from_environment(),
    )
    app = SyncExampleApp(config)
    if config.worker_mode.is_pull:
        # Pull registration fails without a server, so probe first to keep the skip.
        is_server_ready = _server_is_ready(app.client)
        if is_server_ready:
            app.start_worker()
    else:
        app.start_worker()
        is_server_ready = _server_is_ready(app.client)
    if not is_server_ready:
        app.close()
        pytest.skip(
            "no Dex server at "
            f"{config.server_address}; set DEX_FLOW_SERVICE_ADDRESS to run "
            "sync-python integ tests"
        )
    try:
        yield app
    finally:
        app.close()


def skip_when_server_zd(feature: str) -> None:
    """Skips a test whose sample uses a feature that server-zd rejects."""
    if os.environ.get("DEX_INTEG_ENGINE") == "zd":
        pytest.skip(f"server-zd does not support {feature}")


@pytest.fixture(scope="session")
def app(sync_app: SyncExampleApp) -> SyncExampleApp:
    return sync_app


@pytest.fixture(scope="session")
def client(sync_app: SyncExampleApp) -> Client:
    return sync_app.client


@pytest.fixture()
def new_flow_id() -> Callable[[str], str]:
    def make(prefix: str) -> str:
        return f"{prefix}-{uuid4().hex}"

    return make


def _server_is_ready(client: Client) -> bool:
    deadline = time.monotonic() + SERVER_READY_TIMEOUT.total_seconds()
    while time.monotonic() < deadline:
        try:
            return client.health_check()
        except DexServiceError:
            time.sleep(POLL_INTERVAL_SECONDS)
    return False


def _available_worker_address() -> str:
    with socket.socket() as worker_socket:
        worker_socket.bind(("127.0.0.1", 0))
        host, port = worker_socket.getsockname()
    return f"{host}:{port}"
