# Copyright (c) 2026 Super Durable, Inc.
#
# Licensed under the Sustainable Use License 1.0.
# You may not use this file except in compliance with the License.
# See the LICENSE file in the repository root.
#
# SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import os
from dataclasses import dataclass
from datetime import timedelta

from dex.credential import Credential


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

    Examples:
        >>> target = WorkerTarget(pull=True)
        >>> client_options = ClientOptions(worker_target=target)
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
        reserved_rpc_tasks: Capacity reserved to protect synchronous RPC latency
            from long-running Steps. None reserves one slot when resolved capacity is
            at least two, otherwise zero. With the CPU-count default, Steps get one
            fewer slot than before. Set zero to opt out. With one slot and None,
            start logs one warning that RPCs may wait behind Steps. Flows must stay
            responsive; Dex cannot prioritize synchronous RPCs like workflow
            engines prioritize reads.
            WaitFor and Execute tasks, including local attempts, are declined when total busy handlers reach
            max_concurrent_tasks minus this reserve. API-direct and transactional RPCs
            can use the full limit.
            Must be nonnegative and less than the task limit after its CPU-count
            default is resolved. Declines reassign without consuming an attempt.
            If all Workers remain at the Step limit through the dispatch wait,
            NO_WORKER_AVAILABLE consumes an attempt and follows the Step retry
            policy. Local attempts wait the local dispatch wait, then fall back
            according to their policy. Declines emit a Flow-type debug log,
            increment pull_reserved_rpc_declines, and preserve readiness.
        idle_streams_per_queue: Idle streams for the sticky queue and each Flow
            type queue. Zero uses one. Negative values are invalid.
        app_worker_id: Unique process identity for sticky routing. Empty generates
            an identity with eight random base32 characters. A supplied value
            must contain 1-63 ASCII letters, digits, dots, underscores, or hyphens,
            differ from every registered Flow type, and change after restart.
        stop_drain_timeout: Grace period for active tasks on stop. Zero uses 30
            seconds. After it expires, handlers are canceled and stop waits for
            them to return. Negative values are invalid.
            Busy Reconnect waits this long for server EOF after
            the final frame. During Stop, half-closed streams use the remaining
            drain deadline.

        stop_on_permanent_stream_rejection: Whether the first permanent rejection
            leaving a queue without a registered stream drains the Worker and
            raises PullRegistrationError from start. Defaults to False, which
            retries indefinitely. Applies before and after initial readiness.

    Raises:
        ValueError: If a numeric option is negative, the reserve reaches the task
            limit, or app_worker_id is invalid.

    Examples:
        >>> pull = PullWorkerOptions(max_concurrent_tasks=8, reserved_rpc_tasks=2)
        >>> worker = Worker(registry, cache, WorkerOptions(pull=pull))
        >>> worker.worker_target == WorkerTarget(pull=True)
        True
    """

    max_concurrent_tasks: int = 0
    idle_streams_per_queue: int = 0
    app_worker_id: str = ""
    stop_drain_timeout: timedelta = timedelta(0)
    stop_on_permanent_stream_rejection: bool = False
    reserved_rpc_tasks: int | None = None

    def __post_init__(self) -> None:
        if self.max_concurrent_tasks < 0:
            raise ValueError("max_concurrent_tasks must not be negative")
        task_limit = self.max_concurrent_tasks or os.cpu_count() or 1
        if self.reserved_rpc_tasks is not None and (
            self.reserved_rpc_tasks < 0 or self.reserved_rpc_tasks >= task_limit
        ):
            raise ValueError(
                "reserved_rpc_tasks must be nonnegative and less than max_concurrent_tasks"
            )
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
        credential: Supplies the current bearer token for FlowService calls and
            each pull stream open. ``None`` sends no authorization metadata.
            Token errors fail calls before Dex is contacted. Protect the plaintext
            network path. A rotated token is used on the next call or reconnect.
            AsyncWorker accepts awaitable tokens and bounds pull stream token
            lookup to 30 seconds. Synchronous token lookup must return promptly.
    """

    bind_address: str = _DEFAULT_BIND_ADDRESS
    worker_target: WorkerTarget | None = None
    server_address: str = "localhost:8801"
    attribute_index_sync_timeout: timedelta = timedelta(minutes=2)
    pull: PullWorkerOptions | None = None
    credential: Credential | None = None

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
