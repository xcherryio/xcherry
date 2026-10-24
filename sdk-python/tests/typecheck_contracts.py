# Copyright (c) 2026 Super Durable, Inc.
#
# Licensed under the Sustainable Use License 1.0.
# You may not use this file except in compliance with the License.
# See the LICENSE file in the repository root.
#
# SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

from dataclasses import dataclass
from typing import Generator, cast

from dex import (
    ApiKeyCredential,
    AsyncContext,
    Attribute,
    AttributeMap,
    BlobCache,
    ChannelMap,
    Client,
    ClientOptions,
    Context,
    Credential,
    FileCredential,
    Flow,
    FlowConfig,
    PersistenceSchema,
    PullWorkerOptions,
    Registry,
    RPCInvokeOptions,
    RPCResult,
    StartFlowOptions,
    Step,
    StepDecision,
    StepList,
    StepOutput,
    Stream,
    StreamMessage,
    StreamMessagesPage,
    SubFlowOptions,
    Worker,
    WorkerOptions,
    WorkerTarget,
    graceful_complete,
    heartbeat,
    rpc,
)


def credential_options_contract(token_path: str) -> tuple[ClientOptions, WorkerOptions]:
    credential: Credential = FileCredential(token_path)
    return ClientOptions(credential=credential), WorkerOptions(credential=credential)


def api_key_credential_options_contract(
    token_url: str, server_id: str, api_key: str
) -> tuple[ClientOptions, WorkerOptions]:
    credential: Credential = ApiKeyCredential(token_url, server_id, api_key)
    return ClientOptions(credential=credential), WorkerOptions(credential=credential)


@dataclass(frozen=True)
class Input:
    value: str


@dataclass(frozen=True)
class Output:
    value: int


class TypedStep(Step[Input]):
    def execute(self, context: Context, input: Input) -> StepDecision:
        del context, input
        return graceful_complete()


class StreamingStep(Step[Input]):
    def execute(
        self,
        context: Context,
        input: Input,
    ) -> Generator[StepOutput, None, StepDecision]:
        buffered = progress.buffered_text(context)
        yield from buffered.write("thinking ")
        yield from buffered.flush()
        yield heartbeat({"input": input.value})
        yield progress.write(context, "working")
        return graceful_complete()


class AsyncTypedStep(Step[Input]):
    async def execute(  # type: ignore[override]
        self,
        context: AsyncContext,
        input: Input,
    ) -> StepDecision:
        buffered = progress.buffered_text(context)
        buffered.write("thinking ")
        progress.write(context, input.value)
        await context.heartbeat(input.value)
        return graceful_complete()


progress = Stream("progress", str, 10 * 1024 * 1024)
profiles = AttributeMap("profiles", dict[str, str])
queued_commands = ChannelMap("queued-commands", str)


class TypedFlow(Flow[Input]):
    start = TypedStep()

    def get_steps(self) -> StepList[Input]:
        return StepList.start_step(self.start)

    def get_persistence_schema(self) -> PersistenceSchema:
        return PersistenceSchema.of(progress, profiles, queued_commands)

    @rpc()
    def typed_rpc(self, context: Context, input: Input) -> RPCResult[Output]:
        del context, input
        return RPCResult(Output(1))


flow: Flow[Input] = TypedFlow()
client = Client(Registry((flow,)), cast(BlobCache, object()))
run_id: str = client.start_flow(flow, "flow-id", Input("input"))
typed_flow = cast(TypedFlow, flow)
output: Output = client.invoke_rpc(
    typed_flow.typed_rpc,
    "flow-id",
    Input("input"),
)
selected_output: Output = client.invoke_rpc(
    typed_flow.typed_rpc,
    "flow-id",
    Input("input"),
    options=RPCInvokeOptions(
        lock_attribute_map_instances=(profiles.lock("partition-007"),),
        load_attribute_map_instances=(profiles.load("partition-007"),),
        load_channel_map_instances=(queued_commands.load_messages("partition-007"),),
    ),
)
client.write_stream("flow-id", progress, "frontend/1", "starting")
stream_message: StreamMessage[str] = client.read_stream("flow-id", progress)
stream_source: str = stream_message.source
stream_page: StreamMessagesPage[str] = client.list_stream_messages(
    "flow-id", progress, 100
)
stream_page_value: str = stream_page.messages[0].value

status = Attribute("status", str, sync_to_attribute_store=True)
items = AttributeMap("items", int, sync_to_attribute_store=True)
api_key: Attribute[str] = Attribute("api-key", str, hard_delete_on_null=True)
provider_tokens: AttributeMap[bytes] = AttributeMap(
    "provider-token", bytes, hard_delete_on_null=True
)
is_hard_delete_on_null: bool = api_key.hard_delete_on_null
persistence: PersistenceSchema = PersistenceSchema.of(status, items)
start_options: StartFlowOptions = (
    StartFlowOptions(
        config_override=FlowConfig(attribute_store_names=["reporting", "audit"])
    )
    .with_attribute(status, "ready")
    .with_attribute(items, "order-1", 1)
)


def compile_lifecycle(client: Client, worker: Worker) -> None:
    client.trigger_continue_as_new("flow-id")
    with client:
        pass
    with worker:
        pass


pull_target: WorkerTarget = WorkerTarget(pull=True)
pull_worker_options: WorkerOptions = WorkerOptions(
    pull=PullWorkerOptions(max_concurrent_tasks=8, idle_streams_per_queue=1)
)
pull_start_options: StartFlowOptions = StartFlowOptions(
    config_override=FlowConfig(worker_target=pull_target)
)
pull_subflow_options: SubFlowOptions = SubFlowOptions(
    config_override=FlowConfig(worker_target=pull_target)
)
