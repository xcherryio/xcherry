# Copyright (c) 2026 Super Durable, Inc.
#
# Licensed under the Sustainable Use License 1.0.
# You may not use this file except in compliance with the License.
# See the LICENSE file in the repository root.
#
# SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

from dataclasses import dataclass
from datetime import timedelta


class _ImplicitBindAddress(str):
    pass


_DEFAULT_BIND_ADDRESS = _ImplicitBindAddress(":8803")


@dataclass(frozen=True)
class WorkerTarget:
    """Select a listening endpoint or outbound pull queues for a Flow.

    Use ``WorkerTarget(pull=True)`` with a pull Worker. A listening Worker
    advertises its address instead. Client defaults and Flow overrides accept
    the same target.

    Attributes:
        address: A plaintext gRPC target. Headless targets must use ``host:port``.
        headless: Whether Dex should connect directly without service discovery.
            Defaults to ``False``.
        pull: Whether Dex dispatches through Worker task queues. Defaults to
            ``False``. Cannot be combined with an address or headless mode.

    Raises:
        ValueError: If pull mode is combined with an address or headless mode.
    """

    address: str = ""
    headless: bool = False
    pull: bool = False

    def __post_init__(self) -> None:
        if self.pull and (self.address or self.headless):
            raise ValueError(
                "pull mode cannot be combined with address or headless mode"
            )


@dataclass(frozen=True)
class PullWorkerOptions:
    """Configure outbound task polling for a Worker or AsyncWorker.

    Pass this to ``WorkerOptions(pull=PullWorkerOptions())`` and advertise
    ``worker.worker_target`` to a Client. Each stream runs one task at a time.
    Handlers may run concurrently and must synchronize shared state.

    Attributes:
        max_concurrent_tasks: Maximum simultaneous handlers. Zero uses the CPU
            count. Negative values are invalid.
        idle_streams_per_queue: Idle streams for the sticky queue and each Flow
            type queue. Zero uses one. Negative values are invalid.
        app_worker_id: Unique process identity for sticky routing. Empty generates
            an identity with eight random base32 characters. A supplied value
            must contain 1-63 ASCII letters, digits, dots, underscores, or hyphens,
            differ from every registered Flow type, and change after restart.
        stop_drain_timeout: Grace period for active tasks on stop. Zero uses 30
            seconds. After it expires, handlers are canceled and stop waits for
            them to return. Negative values are invalid.

    Raises:
        ValueError: If a numeric option is negative or app_worker_id is invalid.
    """

    max_concurrent_tasks: int = 0
    idle_streams_per_queue: int = 0
    app_worker_id: str = ""
    stop_drain_timeout: timedelta = timedelta(0)

    def __post_init__(self) -> None:
        if self.max_concurrent_tasks < 0:
            raise ValueError("max_concurrent_tasks must not be negative")
        if self.idle_streams_per_queue < 0:
            raise ValueError("idle_streams_per_queue must not be negative")
        if self.stop_drain_timeout < timedelta(0):
            raise ValueError("stop_drain_timeout must not be negative")
        if self.app_worker_id and (
            len(self.app_worker_id) > 63
            or not self.app_worker_id.isascii()
            or any(
                not (character.isalnum() or character in "._-")
                for character in self.app_worker_id
            )
        ):
            raise ValueError(
                "app_worker_id must contain 1-63 ASCII letters, digits, dots, underscores, or hyphens"
            )


@dataclass(frozen=True)
class WorkerOptions:
    """Configure Worker networking, pull polling, and startup synchronization.

    ``attribute_index_sync_timeout`` is the RPC deadline used before the Worker
    opens its listener. It defaults to two minutes and must be positive.

    Attributes:
        bind_address: The local plaintext WorkerService listener in ``host:port``
            form. Defaults to ``":8803"``.
        worker_target: The endpoint advertised to Dex. ``None`` derives an address
            from ``bind_address``.
        server_address: The Dex FlowService target used for startup synchronization
            and blob hydration. Defaults to ``"localhost:8801"``.
        attribute_index_sync_timeout: Maximum duration for startup Attribute index
            synchronization. Defaults to two minutes and must be positive.
        pull: Outbound polling options. ``None`` keeps the listening WorkerService
            mode. Pull mode uses ``server_address`` and cannot advertise an
            address or configure a custom bind address.
    """

    bind_address: str = _DEFAULT_BIND_ADDRESS
    worker_target: WorkerTarget | None = None
    server_address: str = "localhost:8801"
    attribute_index_sync_timeout: timedelta = timedelta(minutes=2)
    pull: PullWorkerOptions | None = None

    def __post_init__(self) -> None:
        if self.attribute_index_sync_timeout <= timedelta(0):
            raise ValueError("attribute_index_sync_timeout must be positive")
        if self.pull is not None and (
            self.bind_address is not _DEFAULT_BIND_ADDRESS
            or self.worker_target is not None
        ):
            raise ValueError(
                "pull Worker cannot configure bind_address or worker_target"
            )
        if (
            self.pull is None
            and self.worker_target is not None
            and self.worker_target.pull
        ):
            raise ValueError("WorkerTarget.pull requires WorkerOptions.pull")
