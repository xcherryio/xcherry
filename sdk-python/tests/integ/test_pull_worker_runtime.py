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

import asyncio
import os
import queue
import threading
from datetime import timedelta
from tempfile import TemporaryDirectory

from dex import (
    AsyncClient,
    AsyncWorker,
    BlobCacheConfig,
    Client,
    ClientOptions,
    Context,
    Flow,
    PullWorkerOptions,
    Registry,
    RPCResult,
    Step,
    StepDecision,
    StepList,
    Wait,
    Worker,
    WorkerOptions,
    go_to,
    graceful_complete,
    open_blob_cache,
    rpc,
)

from .basic_flow import BasicFlow
from .environment import DexDevTestEnvironment
from .shared import unique_id

class PullSecondStep(Step[str]):
    def __init__(
        self,
        worker_id: str,
        observations: queue.Queue[tuple[str, str]],
        gate: threading.Event,
    ) -> None:
        self.worker_id = worker_id
        self.observations = observations
        self.gate = gate

    def wait_for(self, context: Context, input: str) -> Wait:
        self.observations.put((self.worker_id, "second_wait_for"))
        return Wait.skip_immediately()

    def execute(self, context: Context, input: str) -> StepDecision:
        self.observations.put((self.worker_id, "second_execute"))
        if not self.gate.wait(timeout=20):
            raise TimeoutError("pull test gate did not open")
        return graceful_complete(input)

class PullFirstStep(Step[str]):
    def __init__(
        self,
        worker_id: str,
        observations: queue.Queue[tuple[str, str]],
        gate: threading.Event,
    ) -> None:
        self.worker_id = worker_id
        self.observations = observations
        self.gate = gate

    def wait_for(self, context: Context, input: str) -> Wait:
        self.observations.put((self.worker_id, "first_wait_for"))
        return Wait.skip_immediately()

    def execute(self, context: Context, input: str) -> StepDecision:
        self.observations.put((self.worker_id, "first_execute"))
        if not self.gate.wait(timeout=20):
            raise TimeoutError("pull test first gate did not open")
        return go_to(PullSecondStep, input)

class PullFlow(Flow[str]):
    def __init__(
        self,
        worker_id: str,
        observations: queue.Queue[tuple[str, str]],
        first_gate: threading.Event,
        second_gate: threading.Event,
    ) -> None:
        self.worker_id = worker_id
        self.first = PullFirstStep(worker_id, observations, first_gate)
        self.second = PullSecondStep(worker_id, observations, second_gate)

    def get_steps(self) -> StepList[str]:
        return StepList.start_step(self.first).other_steps(self.second)

    @rpc
    def get_worker(self, context: Context) -> RPCResult[str]:
        return RPCResult(self.worker_id)

class PullWorkerHarness:
    def __init__(self, flow: PullFlow, server_address: str, worker_id: str) -> None:
        self.registry = Registry([flow])
        self.cache_directory = TemporaryDirectory(prefix="dex-python-pull-test-")
        self.cache = open_blob_cache(
            BlobCacheConfig(self.cache_directory.name, 64 * 1024 * 1024)
        )
        self.worker = Worker(
            self.registry,
            self.cache,
            WorkerOptions(
                server_address=server_address,
                pull=PullWorkerOptions(app_worker_id=worker_id, max_concurrent_tasks=4),
            ),
        )
        self.failure: BaseException | None = None
        self.thread = threading.Thread(
            target=self._run, name=f"pull-worker-{worker_id}"
        )
        self.thread.start()

    def _run(self) -> None:
        try:
            self.worker.start()
        except BaseException as failure:
            self.failure = failure

    def close(self) -> None:
        self.worker.close()
        self.thread.join(timeout=10)
        self.cache.close()
        self.cache_directory.cleanup()
        if self.thread.is_alive():
            raise RuntimeError("pull Worker did not stop")
        if self.failure is not None:
            raise RuntimeError("pull Worker failed") from self.failure

def test_pull_worker_sticky_rpc_stop_drain_and_push() -> None:
    server_address = os.environ.get("DEX_SERVER_ADDRESS", "127.0.0.1:8801")
    observations: queue.Queue[tuple[str, str]] = queue.Queue()
    first_gate = threading.Event()
    second_gate = threading.Event()
    worker_a_id = unique_id("python-pull-a")
    worker_b_id = unique_id("python-pull-b")
    flow_a = PullFlow(worker_a_id, observations, first_gate, second_gate)
    flow_b = PullFlow(worker_b_id, observations, first_gate, second_gate)
    worker_a = PullWorkerHarness(flow_a, server_address, worker_a_id)
    worker_b: PullWorkerHarness | None = None
    client = Client(
        worker_a.registry,
        worker_a.cache,
        ClientOptions(server_address, worker_a.worker.worker_target),
    )
    try:
        flow_id = unique_id("python-pull-flow")
        client.start_flow(flow_a, flow_id, "completed")
        assert observations.get(timeout=20) == (worker_a_id, "first_wait_for")
        assert observations.get(timeout=20) == (worker_a_id, "first_execute")
        worker_b = PullWorkerHarness(flow_b, server_address, worker_b_id)
        first_gate.set()
        assert client.invoke_rpc(flow_a.get_worker, flow_id) == worker_a_id
        assert observations.get(timeout=20) == (worker_a_id, "second_wait_for")
        assert observations.get(timeout=20) == (worker_a_id, "second_execute")
        stopped = threading.Event()

        def stop_worker() -> None:
            worker_a.worker.stop()
            stopped.set()

        stopper = threading.Thread(target=stop_worker)
        stopper.start()
        assert not stopped.wait(timeout=0.2)
        second_gate.set()
        assert (
            client.wait_for_flow(flow_id, timedelta(seconds=30)).single_output(str)
            == "completed"
        )
        stopper.join(timeout=10)
        assert stopped.is_set()
        second_flow_id = unique_id("python-pull-second")
        client.start_flow(flow_a, second_flow_id, "second-completed")
        assert observations.get(timeout=20) == (worker_b_id, "first_wait_for")
        assert (
            client.wait_for_flow(second_flow_id, timedelta(seconds=30)).single_output(
                str
            )
            == "second-completed"
        )
        push_flow = BasicFlow()
        with DexDevTestEnvironment(push_flow) as push_environment:
            push_flow_id = unique_id("python-push-with-pull")
            push_environment.client.start_flow(push_flow, push_flow_id, 0)
            assert (
                push_environment.client.wait_for_flow(
                    push_flow_id, timedelta(seconds=30)
                ).single_output(int)
                == 2
            )
    finally:
        first_gate.set()
        second_gate.set()
        client.close()
        worker_a.close()
        if worker_b is not None:
            worker_b.close()

def test_async_pull_worker_handles_flow() -> None:
    asyncio.run(_async_pull_worker_handles_flow())

async def _async_pull_worker_handles_flow() -> None:
    server_address = os.environ.get("DEX_SERVER_ADDRESS", "127.0.0.1:8801")
    flow = BasicFlow()
    registry = Registry([flow], allow_async_handlers=True)
    with TemporaryDirectory(prefix="dex-python-async-pull-test-") as cache_directory:
        cache = open_blob_cache(BlobCacheConfig(cache_directory, 64 * 1024 * 1024))
        worker = AsyncWorker(
            registry,
            cache,
            WorkerOptions(
                server_address=server_address,
                pull=PullWorkerOptions(app_worker_id=unique_id("python-async-pull")),
            ),
        )
        worker_task = asyncio.create_task(worker.start())
        client = AsyncClient(
            registry,
            cache,
            ClientOptions(server_address, worker.worker_target),
        )
        try:
            flow_id = unique_id("python-async-pull-flow")
            await client.start_flow(flow, flow_id, 0)
            result = await client.wait_for_flow(flow_id, timedelta(seconds=30))
            assert result.single_output(int) == 2
        finally:
            await client.close()
            await worker.close()
            await asyncio.wait_for(worker_task, timeout=10)
            cache.close()
