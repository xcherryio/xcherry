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

"""A greeting Flow served by an outbound pull Worker."""

from __future__ import annotations

import asyncio

from dex import (
    AsyncClient,
    AsyncWorker,
    Attribute,
    BlobCache,
    ClientOptions,
    Context,
    Flow,
    PersistenceSchema,
    PullWorkerOptions,
    Registry,
    RPCResult,
    Step,
    StepDecision,
    StepList,
    WorkerOptions,
    graceful_complete,
    rpc,
)


class GreetStep(Step[str]):
    def execute(self, context: Context, name: str) -> StepDecision:
        message = f"Hello, {name}!"
        GreetingFlow.message.set(context, message)
        return graceful_complete(message)


class GreetingFlow(Flow[str]):
    message = Attribute("pull-greeting-message", str)

    def __init__(self) -> None:
        self.greet_step = GreetStep()

    def get_steps(self) -> StepList[str]:
        return StepList.start_step(self.greet_step)

    def get_persistence_schema(self) -> PersistenceSchema:
        return PersistenceSchema.of(self.message)

    @rpc
    def get_greeting(self, context: Context) -> RPCResult[str]:
        return RPCResult(self.message.get(context))


async def start_pull_greeting(
    cache: BlobCache, server_address: str
) -> tuple[GreetingFlow, AsyncWorker, AsyncClient, asyncio.Task[None]]:
    flow = GreetingFlow()
    registry = Registry((flow,), allow_async_handlers=True)
    worker = AsyncWorker(
        registry,
        cache,
        WorkerOptions(
            server_address=server_address,
            pull=PullWorkerOptions(max_concurrent_tasks=8),
        ),
    )
    worker_task = asyncio.create_task(worker.start())
    client = AsyncClient(
        registry,
        cache,
        ClientOptions(
            server_address=server_address,
            worker_target=worker.worker_target,
        ),
    )
    return flow, worker, client, worker_task
