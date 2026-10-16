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

import os
import socket
import threading
import time
from tempfile import TemporaryDirectory
from types import TracebackType
from typing import Any

from dex import (
    BlobCacheConfig,
    Client,
    ClientOptions,
    Flow,
    PullWorkerOptions,
    Registry,
    Worker,
    WorkerOptions,
    open_blob_cache,
)


class DexDevTestEnvironment:
    def __init__(self, *flows: Flow[Any], worker_mode: str | None = None) -> None:
        server_address = os.environ.get("DEX_SERVER_ADDRESS", "127.0.0.1:8801")
        is_pull_worker_mode = (worker_mode or integ_worker_mode()) == "pull"
        worker_port = None if is_pull_worker_mode else _available_port()
        registry = Registry(flows)
        self._cache_directory = TemporaryDirectory(
            prefix="dex-python-integration-cache-"
        )
        self._cache = open_blob_cache(
            BlobCacheConfig(
                self._cache_directory.name,
                64 * 1024 * 1024,
            )
        )
        self._worker = Worker(
            registry,
            self._cache,
            (
                WorkerOptions(server_address=server_address, pull=PullWorkerOptions())
                if is_pull_worker_mode
                else WorkerOptions(
                    bind_address=f"127.0.0.1:{worker_port}",
                    server_address=server_address,
                )
            ),
        )
        self._failure: BaseException | None = None
        self._thread = threading.Thread(
            target=self._run_worker,
            name="dex-python-integration-worker",
        )
        self._thread.start()
        if worker_port is not None:
            _await_worker(worker_port, self)
        self.client = Client(
            registry,
            self._cache,
            ClientOptions(server_address, self._worker.worker_target),
        )

    def __enter__(self) -> DexDevTestEnvironment:
        return self

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.client.close()
        self._worker.close()
        self._thread.join(timeout=10)
        self._cache.close()
        self._cache_directory.cleanup()
        if self._thread.is_alive():
            raise RuntimeError("Python integration Worker did not stop")
        if self._failure is not None:
            raise RuntimeError("Python integration Worker failed") from self._failure

    def _run_worker(self) -> None:
        try:
            self._worker.start()
        except BaseException as error:
            self._failure = error


def integ_worker_mode() -> str:
    worker_mode = os.environ.get("DEX_INTEG_WORKER_MODE", "push")
    if worker_mode not in ("push", "pull"):
        raise ValueError("DEX_INTEG_WORKER_MODE must be push or pull")
    return worker_mode


def _available_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _await_worker(port: int, environment: DexDevTestEnvironment) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if environment._failure is not None:
            raise RuntimeError(
                "Python integration Worker failed"
            ) from environment._failure
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                return
        except OSError:
            time.sleep(0.01)
    raise RuntimeError("Python integration Worker did not become ready")
