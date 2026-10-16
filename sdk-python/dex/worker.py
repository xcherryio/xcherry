# Legacy Materials in this file remain under their original licenses.
# See LEGACY_NOTICES.md.

# Modifications Copyright (c) 2026 Super Durable, Inc.
#
# Modifications after the Legacy Cutoff are licensed under the
# Sustainable Use License 1.0.
# Legacy Materials remain under their original licenses.
# See LICENSE and LEGACY_NOTICES.md.

from __future__ import annotations

import asyncio
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from types import TracebackType

import grpc
from google.protobuf import empty_pb2

from dex._credential_transport import AsyncCredentialRunner, sync_channel
from dex._pull_worker import PullWorkerRuntime, resolve_app_worker_id
from dex._server_protocol import (
    negotiate_server_protocol,
    sdk_version,
    server_info_request_error,
)
from dex._value_hydrator import ValueHydrator
from dex._value_mapper import ValueMapper
from dex._worker_dispatcher import WorkerDispatcher
from dex._worker_service import WorkerService
from dex.blob_cache import BlobCache
from dex.dexpb import dex_pb2 as pb
from dex.dexpb import dex_pb2_grpc
from dex.flow import Registry
from dex.worker_options import WorkerOptions, WorkerTarget


class Worker:
    """Host synchronous Step and RPC handlers through a listener or pull streams.

    A Worker is one-shot. Call ``start`` on a dedicated thread because it blocks,
    then call ``stop`` or ``close`` during shutdown. Set ``WorkerOptions.pull``
    to poll Dex without a listener. Handlers run concurrently in a bounded
    thread pool and must synchronize shared application state.

    Attributes:
        registry: The Flow Registry served by this Worker.
        blob_cache: The shared cache used to hydrate large values.
        options: The effective WorkerOptions.

    Examples:
        >>> worker = Worker(registry, cache)
        >>> thread = threading.Thread(target=worker.start, daemon=True)
        >>> thread.start()
        >>> worker.worker_target.address
        'localhost:8803'
        >>> worker.stop()
    """

    def __init__(
        self,
        registry: Registry,
        blob_cache: BlobCache,
        options: WorkerOptions | None = None,
    ) -> None:
        """Construct a Worker without starting its listener or pull streams.

        Args:
            registry: A Registry created for synchronous handlers.
            blob_cache: An open cache shared for value hydration.
            options: Networking, startup, and bearer credential options;
                ``None`` uses defaults and sends no authorization metadata.

        Raises:
            ValueError: If a target, bind address, or pull Worker ID is invalid.
        """
        self.registry = registry
        self.blob_cache = blob_cache
        self.options = options or WorkerOptions()
        self._credential_runner = (
            AsyncCredentialRunner(self.options.credential)
            if self.options.pull is not None and self.options.credential is not None
            else None
        )
        app_worker_id = (
            resolve_app_worker_id(registry, self.options.pull)
            if self.options.pull is not None
            else None
        )
        self._lock = threading.Lock()
        self._state = "created"
        self._flow_channel = sync_channel(
            self.options.server_address, self.options.credential
        )
        self._flow_service = dex_pb2_grpc.FlowServiceStub(  # type: ignore[no-untyped-call]
            self._flow_channel
        )
        self._sdk_version = sdk_version()
        self._negotiated_protocol_version = 0
        values = ValueMapper(registry.codec_registry)
        dispatcher = WorkerDispatcher(
            registry,
            values,
            ValueHydrator(self._flow_service, blob_cache),
        )
        concurrency = (
            self.options.pull.max_concurrent_tasks or os.cpu_count() or 1
            if self.options.pull is not None
            else max(2, min(32, os.cpu_count() or 2))
        )
        self._executor = ThreadPoolExecutor(
            max_workers=concurrency,
            thread_name_prefix="dex-python-handler",
        )
        self._server = None
        self._pull = None
        if self.options.pull is not None:
            self._pull = PullWorkerRuntime(
                registry,
                dispatcher,
                self.options.pull,
                self.options.server_address,
                self._executor,
                app_worker_id,
                self._credential_runner,
            )
        else:
            self._server = grpc.server(
                self._executor,
                maximum_concurrent_rpcs=concurrency,
            )
            dex_pb2_grpc.add_WorkerServiceServicer_to_server(  # type: ignore[no-untyped-call]
                WorkerService(dispatcher),
                self._server,
            )
        self._bound_port = 0
        self._worker_target = (
            WorkerTarget(pull=True)
            if self._pull is not None
            else self.options.worker_target
            or WorkerTarget(self._target_address(self.options.bind_address, 0))
        )

    def __enter__(self) -> Worker:
        """Return this Worker for synchronous context-manager use.

        Entering does not call ``start``.

        Returns:
            This Worker instance.
        """
        return self

    @property
    def worker_target(self) -> WorkerTarget:
        """Return the effective Worker target advertised to Dex.

        Before ``start``, an ephemeral bind port may not yet be resolved. After
        binding, the property contains the actual selected port. Pull mode returns
        ``WorkerTarget(pull=True)`` before and after start.

        Returns:
            The advertised WorkerTarget.
        """
        return self._worker_target

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the Worker when leaving a context-manager block.

        Args:
            exception_type: The active exception type, if any.
            exception: The active exception value, if any.
            traceback: The active traceback, if any.
        """
        self.close()

    def start(self) -> None:
        """Negotiate protocol, synchronize Attribute indexes, and serve.

        ``start`` may be called exactly once. It first contacts FlowService using the
        configured synchronization timeout, then starts a listener or outbound
        pull streams and waits until ``stop`` finishes. Later stream failures are
        logged and retried; they do not raise from ``start``.

        Raises:
            RuntimeError: If lifecycle state, index synchronization, or binding fails.
        """
        with self._lock:
            if self._state != "created":
                raise RuntimeError(f"Worker cannot start from state {self._state}")
            try:
                server_info = self._flow_service.GetServerInfo(
                    empty_pb2.Empty(),
                    timeout=self.options.attribute_index_sync_timeout.total_seconds(),
                )
            except grpc.RpcError as failure:
                self._state = "stopped"
                self._flow_channel.close()
                self._executor.shutdown(wait=True, cancel_futures=True)
                raise server_info_request_error(self._sdk_version, failure) from failure
            except Exception as failure:
                self._state = "stopped"
                self._flow_channel.close()
                self._executor.shutdown(wait=True, cancel_futures=True)
                raise RuntimeError("cannot get Dex server info") from failure
            try:
                self._negotiated_protocol_version = negotiate_server_protocol(
                    server_info,
                    self._sdk_version,
                )
            except RuntimeError:
                self._state = "stopped"
                self._flow_channel.close()
                self._executor.shutdown(wait=True, cancel_futures=True)
                raise
            try:
                self._flow_service.SyncAttributeIndexes(
                    pb.SyncAttributeIndexRequest(
                        attribute_indexes=dict(self.registry._attribute_indexes)
                    ),
                    timeout=self.options.attribute_index_sync_timeout.total_seconds(),
                )
            except grpc.RpcError as failure:
                self._state = "stopped"
                self._flow_channel.close()
                self._executor.shutdown(wait=True, cancel_futures=True)
                raise RuntimeError("cannot synchronize Attribute indexes") from failure
            except Exception as failure:
                self._state = "stopped"
                self._flow_channel.close()
                self._executor.shutdown(wait=True, cancel_futures=True)
                raise RuntimeError("cannot synchronize Attribute indexes") from failure
            if self._pull is None:
                assert self._server is not None
                self._bound_port = self._server.add_insecure_port(
                    self.options.bind_address
                )
                if self._bound_port == 0:
                    self._state = "stopped"
                    self._flow_channel.close()
                    self._executor.shutdown(wait=True, cancel_futures=True)
                    raise RuntimeError(
                        f"cannot bind Python Worker to {self.options.bind_address}"
                    )
                if self.options.worker_target is None:
                    self._worker_target = WorkerTarget(
                        self._target_address(
                            self.options.bind_address, self._bound_port
                        )
                    )
            self._state = "running"
            if self._server is not None:
                self._server.start()
        if self._pull is not None:
            asyncio.run(self._pull.run())
        else:
            assert self._server is not None
            self._server.wait_for_termination()

    def stop(self) -> None:
        """Drain handlers and release Worker resources.

        Calls before ``start`` and repeated calls after shutdown are safe. Remaining
        listening-mode handler futures are canceled after five seconds. Pull mode
        drains for ``PullWorkerOptions.stop_drain_timeout`` (30 seconds by default),
        then requests cancellation and waits for every handler to return. A
        synchronous handler must check ``Context.is_cancellation_requested`` to
        respond promptly to forced shutdown. Call ``stop`` from a thread other
        than the pull Worker's ``start`` thread.
        Half-closed pull streams wait for server EOF until the remaining drain
        deadline before the channel closes.

        Raises:
            RuntimeError: If called on the pull Worker's ``start`` thread.
        """
        with self._lock:
            if self._state in ("stopped", "closed"):
                return
            if self._pull is not None and self._pull.is_loop_thread():
                raise RuntimeError(
                    "pull Worker.stop() must be called from another thread"
                )
            was_created = self._state == "created"
            self._state = "stopping"
        if self._pull is not None:
            if not was_created:
                self._pull.stop_threadsafe()
        else:
            assert self._server is not None
            self._server.stop(grace=5).wait(timeout=10)
        self._flow_channel.close()
        self._executor.shutdown(wait=True, cancel_futures=True)
        if self._credential_runner is not None:
            self._credential_runner.close()
        with self._lock:
            if self._state != "closed":
                self._state = "stopped"

    def close(self) -> None:
        """Stop the Worker and permanently mark it closed.

        Pull streams finish their bounded EOF wait before the channel closes.
        ``close`` is idempotent; a closed Worker cannot be started again.
        """
        try:
            self.stop()
        finally:
            if self._credential_runner is not None:
                self._credential_runner.close()
            with self._lock:
                self._state = "closed"

    @staticmethod
    def _target_address(bind_address: str, bound_port: int) -> str:
        host, separator, port = bind_address.rpartition(":")
        if not separator or not port:
            raise ValueError("Worker bind address requires a port")
        if bound_port == 0:
            bound_port = int(port)
        if host in ("", "0.0.0.0", "::", "[::]"):
            host = "localhost"
        return f"{host}:{bound_port}"
