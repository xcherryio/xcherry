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

from __future__ import annotations

import logging
import socket
import threading
import time
from typing import Any, Callable

from dex import (
    BlobCacheConfig,
    Client,
    ClientOptions,
    Flow,
    Registry,
    Worker,
    WorkerOptions,
    WorkerTarget,
    open_blob_cache,
)

from dex_examples.patterns.interruptible.interruptible_execution_flow import (
    InterruptibleFlow,
)
from dex_examples.primitives.channel.channel_flow import ChannelFlow
from dex_examples.primitives.step.step_flow import StepFlow
from dex_examples.products.engagement.engagement_flow import EngagementFlow
from dex_examples.products.money_transfer.money_transfer_flow import MoneyTransferFlow
from dex_examples.products.subscription.subscription_flow import SubscriptionFlow
from dex_examples.shared.my_dependency_service import MyDependencyService
from dex_examples.shared.worker_mode import (
    PULL_REGISTRATION_TIMEOUT_SECONDS,
    new_example_pull_worker_options,
)
from sync_examples.config import SyncExamplesConfig
from sync_examples.patterns.parent_child.child_flow import ChildFlow
from sync_examples.patterns.parent_child.parent_flow import ParentFlowV2


class SyncExampleApp:
    def __init__(self, config: SyncExamplesConfig) -> None:
        self.config = config
        self._client: Client | None = None
        client_provider: Callable[[], Client] = self.require_client

        service = MyDependencyService()
        self.step = StepFlow()
        self.channel = ChannelFlow()
        self.money_transfer = MoneyTransferFlow(service)
        self.engagement = EngagementFlow(service)
        self.subscription = SubscriptionFlow(service)
        self.interruptible = InterruptibleFlow()
        self.child_flow = ChildFlow()
        self.parent_flow_v2 = ParentFlowV2(client_provider, self.child_flow)

        flows: list[Flow[Any]] = [
            self.step,
            self.channel,
            self.money_transfer,
            self.engagement,
            self.subscription,
            self.interruptible,
            self.child_flow,
            self.parent_flow_v2,
        ]
        self.registry = Registry(tuple(flows))
        config.blob_cache_dir.mkdir(parents=True, exist_ok=True)
        self.blob_cache = open_blob_cache(
            BlobCacheConfig(str(config.blob_cache_dir), 1 << 30)
        )
        self.worker = Worker(self.registry, self.blob_cache, _new_worker_options(config))
        self._client = Client(
            self.registry,
            self.blob_cache,
            ClientOptions(
                server_address=config.server_address,
                worker_target=self.worker.worker_target,
            ),
        )
        self._worker_thread: threading.Thread | None = None
        self._is_closed = False

    @property
    def client(self) -> Client:
        return self.require_client()

    def require_client(self) -> Client:
        if self._client is None:
            raise RuntimeError("client is not ready")
        return self._client

    def start_worker(self) -> None:
        """Starts the Worker thread, closing this app if startup fails.

        Raises:
            BaseException: The original startup or registration error.
        """
        if self._worker_thread is not None:
            return
        try:
            self._worker_thread = threading.Thread(
                target=self.worker.start,
                name="dex-sync-example-worker",
                daemon=True,
            )
            self._worker_thread.start()
            if self.config.worker_mode.is_pull:
                self.worker.wait_pull_registered(
                    timeout=PULL_REGISTRATION_TIMEOUT_SECONDS
                )
                return
            _await_worker(self.worker.worker_target.address)
        except BaseException:
            try:
                self.close()
            except Exception:
                _logger.exception("failed to close SyncExampleApp after startup failed")
            raise

    def close(self) -> None:
        """Closes the Client, Worker, Worker thread, and BlobCache once."""
        if self._is_closed:
            return
        self._is_closed = True
        if self._client is not None:
            self._client.close()
            self._client = None
        self.worker.close()
        if self._worker_thread is not None:
            self._worker_thread.join(timeout=10)
            self._worker_thread = None
        self.blob_cache.close()


_logger = logging.getLogger(__name__)


def _new_worker_options(config: SyncExamplesConfig) -> WorkerOptions:
    if config.worker_mode.is_pull:
        return WorkerOptions(
            server_address=config.server_address,
            pull=new_example_pull_worker_options(),
        )
    return WorkerOptions(
        bind_address=config.worker_bind_address,
        server_address=config.server_address,
        worker_target=(
            WorkerTarget(config.worker_target)
            if config.worker_target
            else None
        ),
    )


def _await_worker(address: str) -> None:
    host, _, port_text = address.rpartition(":")
    port = int(port_text)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host or "127.0.0.1", port), timeout=0.1):
                return
        except OSError:
            time.sleep(0.01)
    raise RuntimeError("Worker did not become ready")
