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

import asyncio
import logging
import socket
import time
from typing import Any, Callable

from dex import (
    AsyncClient,
    AsyncWorker,
    BlobCacheConfig,
    ClientOptions,
    Flow,
    Registry,
    WorkerOptions,
    WorkerTarget,
    open_blob_cache,
)

from dex_examples.config import ExamplesConfig
from dex_examples.patterns.cron.cron_schedule_flow import CronScheduleFlow
from dex_examples.patterns.drain_channels.internal.drain_internal_channels_flow import (
    DrainInternalChannelFlow,
)
from dex_examples.patterns.drain_channels.external_publishing.draining_channel_flow import (
    DrainingExternalChannelFlow,
)
from dex_examples.patterns.entity_store.user_profile_flow import UserProfileFlow
from dex_examples.patterns.hash_partitioned_attribute_map.customer_directory_flow import (
    CustomerDirectoryFlow,
)
from dex_examples.patterns.interruptible.interruptible_execution_flow import (
    InterruptibleFlow,
)
from dex_examples.patterns.intervention.manual_recovery_flow import (
    ManualRecoveryFlow,
)
from dex_examples.patterns.parallel.await_parallel_steps_flow import AwaitParallelStepsFlow
from dex_examples.patterns.parallel.dynamic_parallel_steps_flow import DynamicParallelStepsFlow
from dex_examples.patterns.parallel.first_win_parallel_steps_flow import FirstWinParallelStepsFlow
from dex_examples.patterns.parallel.static_parallel_steps_flow import StaticParallelStepsFlow
from dex_examples.patterns.parallel_subflows.advanced_long_live_parent_flow import (
    AdvancedLongLiveParentFlow,
)
from dex_examples.patterns.parallel_subflows.advanced_short_live_parent_flow import (
    AdvancedShortLiveParentFlow,
)
from dex_examples.patterns.parallel_subflows.basic_parent_flow import BasicParentFlow
from dex_examples.patterns.parallel_subflows.example_subflow import (
    ExampleSubFlow as ParallelExampleSubFlow,
)
from dex_examples.patterns.parallel_subflows.submit_request_flow import (
    SubmitRequestFlow,
)
from dex_examples.patterns.parallel_subflows.wait_for_half_parent_flow import (
    WaitForHalfParentFlow,
)
from dex_examples.patterns.polling.job_service import FakeJobService
from dex_examples.patterns.polling.polling_flow import PollingFlow
from dex_examples.patterns.recovery.failure_recovery_flow import FailureRecoveryFlow
from dex_examples.patterns.reminders.reminder_flow import ReminderFlow
from dex_examples.patterns.sequentially_chunked_attribute_map.chunked_subscriber_flow import (
    ChunkedSubscriberFlow,
)
from dex_examples.patterns.inactiveness_tracker_timer.inactiveness_tracker_flow import (
    InactivenessTrackerFlow,
)
from dex_examples.patterns.resource_control.controller_flow import ControllerFlow
from dex_examples.patterns.resource_control.processing_flow import ProcessingFlow
from dex_examples.patterns.shared.service_dependency import ServiceDependency
from dex_examples.patterns.timeout.flow_graceful_timeout import FlowGracefulTimeout
from dex_examples.patterns.wait_for_step_completion.wait_for_step_completion_flow import (
    WaitForStepCompletionFlow,
)
from dex_examples.primitives.attribute.attribute_flow import AttributeFlow
from dex_examples.primitives.channel.channel_flow import ChannelFlow
from dex_examples.primitives.client_apis.client_apis_flow import ClientApisFlow
from dex_examples.primitives.custom_retry.custom_retry_flow import CustomRetryFlow
from dex_examples.primitives.flow.example_flow import ExampleFlow
from dex_examples.primitives.durability.durability_flow import DurabilityFlow
from dex_examples.primitives.heartbeat.heartbeat_flow import HeartbeatFlow
from dex_examples.primitives.options_override.options_override_flow import (
    OptionsOverrideFlow,
)
from dex_examples.primitives.proceed_on_wait_failure.proceed_on_wait_failure_flow import (
    ProceedOnWaitFailureFlow,
)
from dex_examples.primitives.rpc.rpc_flow import RpcFlow
from dex_examples.primitives.step.retry_flow import RetryFlow
from dex_examples.primitives.step.step_flow import StepFlow
from dex_examples.primitives.step_execution_local.step_execution_local_flow import (
    StepExecutionLocalFlow,
)
from dex_examples.primitives.step_decision.step_decision_flow import StepDecisionFlow
from dex_examples.primitives.stream.stream_flow import StreamFlow
from dex_examples.primitives.subflow.child_flow import SubFlowChildFlow
from dex_examples.primitives.subflow.parent_flow import SubFlowParentFlow
from dex_examples.primitives.timer.timer_flow import TimerFlow
from dex_examples.primitives.wait_types.wait_types_flow import WaitTypesFlow
from dex_examples.products.ai_agent.ai_agent_flow import AIAgentFlow
from dex_examples.products.ai_agent.mcp_registry import MCPRegistry
from dex_examples.products.ai_agent.model_client import (
    AgentCredentialStore,
    LiteLLMModelClient,
)
from dex_examples.products.deal_dsl.deal_dsl_flow import DealDSLFlow
from dex_examples.products.engagement.engagement_flow import EngagementFlow
from dex_examples.products.job_post.job_post_flow import JobPostingFlow
from dex_examples.products.microservices.orchestration_flow import OrchestrationFlow
from dex_examples.products.money_transfer.money_transfer_flow import MoneyTransferFlow
from dex_examples.products.order_processing.order_processing_flow import OrderProcessingFlow
from dex_examples.products.signup.user_signup_flow import UserOnboardingFlow
from dex_examples.products.subscription.subscription_flow import SubscriptionFlow
from dex_examples.shared.my_dependency_service import MyDependencyService
from dex_examples.shared.worker_mode import (
    PULL_REGISTRATION_TIMEOUT_SECONDS,
    new_example_pull_worker_options,
)


class ExampleApp:
    def __init__(self, config: ExamplesConfig) -> None:
        self.config = config
        self._client: AsyncClient | None = None
        client_provider: Callable[[], AsyncClient] = self.require_client

        service = MyDependencyService()
        pattern_service = ServiceDependency()

        self.money_transfer = MoneyTransferFlow(service)
        self.order_processing = OrderProcessingFlow(service)
        self.orchestration = OrchestrationFlow(service)
        self.engagement = EngagementFlow(service)
        self.subscription = SubscriptionFlow(service)
        self.user_onboarding = UserOnboardingFlow(service)
        self.job_post = JobPostingFlow(service)
        self.deal_dsl = DealDSLFlow()

        self.cron_schedule = CronScheduleFlow()
        self.drain_internal = DrainInternalChannelFlow(pattern_service)
        self.drain_external = DrainingExternalChannelFlow()
        self.interruptible = InterruptibleFlow()
        self.manual_recovery = ManualRecoveryFlow()
        self.static_parallel = StaticParallelStepsFlow()
        self.dynamic_parallel = DynamicParallelStepsFlow()
        self.await_parallel = AwaitParallelStepsFlow()
        self.first_win_parallel = FirstWinParallelStepsFlow()
        self.parallel_subflow_child = ParallelExampleSubFlow()
        self.basic_subflows = BasicParentFlow(self.parallel_subflow_child)
        self.wait_for_half_subflows = WaitForHalfParentFlow(
            client_provider, self.parallel_subflow_child
        )
        self.long_live_subflows = AdvancedLongLiveParentFlow(
            self.parallel_subflow_child
        )
        self.short_live_subflows = AdvancedShortLiveParentFlow(
            self.parallel_subflow_child
        )
        self.submit_subflow_request = SubmitRequestFlow(
            client_provider, self.short_live_subflows
        )
        self.polling_jobs = FakeJobService()
        self.polling = PollingFlow(self.polling_jobs)
        self.failure_recovery = FailureRecoveryFlow()
        self.reminder = ReminderFlow(pattern_service)
        self.inactiveness_tracker = InactivenessTrackerFlow()
        self.user_profile = UserProfileFlow()
        self.chunked_subscriber = ChunkedSubscriberFlow()
        self.customer_directory = CustomerDirectoryFlow()
        self.timeout = FlowGracefulTimeout()
        self.wait_for_step_completion = WaitForStepCompletionFlow(pattern_service)

        self.example_flow = ExampleFlow()
        self.step = StepFlow()
        self.step_retry = RetryFlow()
        self.custom_retry = CustomRetryFlow()
        self.durability = DurabilityFlow()
        self.heartbeat = HeartbeatFlow()
        self.options_override = OptionsOverrideFlow()
        self.proceed_on_wait_failure = ProceedOnWaitFailureFlow()
        self.step_execution_local = StepExecutionLocalFlow()
        self.step_decision = StepDecisionFlow()
        self.wait_types = WaitTypesFlow()
        self.attribute = AttributeFlow()
        self.channel = ChannelFlow()
        self.stream = StreamFlow()
        self.timer = TimerFlow()
        self.rpc = RpcFlow()
        self.subflow_child = SubFlowChildFlow()
        self.subflow_parent = SubFlowParentFlow(self.subflow_child)
        self.client_apis = ClientApisFlow()

        self.controller = ControllerFlow(client_provider, lambda: self.processing)
        self.processing = ProcessingFlow(client_provider, lambda: self.controller)
        self.mcp_registry = MCPRegistry.from_file(config.agent_mcp_config)
        self.ai_agent_credentials = AgentCredentialStore()
        self.ai_agent = AIAgentFlow(
            LiteLLMModelClient(self.ai_agent_credentials),
            self.mcp_registry,
        )

        flows: list[Flow[Any]] = [
            self.money_transfer,
            self.order_processing,
            self.orchestration,
            self.engagement,
            self.subscription,
            self.user_onboarding,
            self.job_post,
            self.deal_dsl,
            self.cron_schedule,
            self.drain_internal,
            self.drain_external,
            self.interruptible,
            self.manual_recovery,
            self.static_parallel,
            self.dynamic_parallel,
            self.await_parallel,
            self.first_win_parallel,
            self.parallel_subflow_child,
            self.basic_subflows,
            self.wait_for_half_subflows,
            self.long_live_subflows,
            self.short_live_subflows,
            self.submit_subflow_request,
            self.polling,
            self.failure_recovery,
            self.reminder,
            self.inactiveness_tracker,
            self.user_profile,
            self.chunked_subscriber,
            self.customer_directory,
            self.timeout,
            self.wait_for_step_completion,
            self.example_flow,
            self.step,
            self.step_retry,
            self.custom_retry,
            self.durability,
            self.heartbeat,
            self.options_override,
            self.proceed_on_wait_failure,
            self.step_execution_local,
            self.step_decision,
            self.wait_types,
            self.attribute,
            self.channel,
            self.stream,
            self.timer,
            self.rpc,
            self.subflow_child,
            self.subflow_parent,
            self.client_apis,
            self.controller,
            self.processing,
            self.ai_agent,
        ]
        self.registry = Registry(tuple(flows), allow_async_handlers=True)
        config.blob_cache_dir.mkdir(parents=True, exist_ok=True)
        self.blob_cache = open_blob_cache(
            BlobCacheConfig(str(config.blob_cache_dir), 1 << 30)
        )
        self.worker = AsyncWorker(
            self.registry, self.blob_cache, _new_worker_options(config)
        )
        self._client = AsyncClient(
            self.registry,
            self.blob_cache,
            ClientOptions(
                server_address=config.server_address,
                worker_target=self.worker.worker_target,
            ),
        )
        self._worker_task: asyncio.Task[None] | None = None
        self._is_closed = False

    @property
    def client(self) -> AsyncClient:
        return self.require_client()

    def require_client(self) -> AsyncClient:
        if self._client is None:
            raise RuntimeError("client is not ready")
        return self._client

    async def start_worker(self) -> None:
        """Starts the MCP registry and Worker, closing this app if startup fails.

        Raises:
            BaseException: The original startup or registration error.
        """
        if self._worker_task is not None:
            return
        try:
            await self.mcp_registry.start()
            self._worker_task = asyncio.create_task(self.worker.start())
            if self.config.worker_mode.is_pull:
                await self.worker.wait_pull_registered(
                    timeout=PULL_REGISTRATION_TIMEOUT_SECONDS
                )
                return
            await _await_worker(self.worker.worker_target.address, self._worker_task)
        except BaseException:
            try:
                await self.close()
            except Exception:
                _logger.exception("failed to close ExampleApp after startup failed")
            raise

    async def close(self) -> None:
        """Closes the Client, Worker, MCP registry, and BlobCache once."""
        if self._is_closed:
            return
        self._is_closed = True
        if self._client is not None:
            await self._client.close()
            self._client = None
        await self.worker.close()
        if self._worker_task is not None:
            try:
                await asyncio.wait_for(self._worker_task, timeout=10)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._worker_task.cancel()
            self._worker_task = None
        await self.mcp_registry.close()
        self.blob_cache.close()


_logger = logging.getLogger(__name__)


def _new_worker_options(config: ExamplesConfig) -> WorkerOptions:
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


async def _await_worker(address: str, worker_task: asyncio.Task[None]) -> None:
    host, _, port_text = address.rpartition(":")
    port = int(port_text)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if worker_task.done():
            error = worker_task.exception()
            if error is not None:
                raise RuntimeError("AsyncWorker failed") from error
            raise RuntimeError("AsyncWorker stopped before becoming ready")
        try:
            with socket.create_connection((host or "127.0.0.1", port), timeout=0.1):
                return
        except OSError:
            await asyncio.sleep(0.01)
    raise RuntimeError("AsyncWorker did not become ready")
