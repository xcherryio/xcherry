# Portions of this file are derived from indeedeng/iwf-java-sdk.
# Those portions are licensed under the Apache License, Version 2.0.
# See LICENSES/Apache-2.0.txt and LEGACY_NOTICES.md.
#
# Modifications Copyright (c) 2026 Super Durable, Inc.
#
# Modifications are licensed under the Sustainable Use License 1.0.
# Third-Party Materials remain under the Apache License, Version 2.0.
# See LICENSE and LEGACY_NOTICES.md.

import asyncio
from datetime import timedelta
from typing import cast

import pytest

from dex import (
    FlowAlreadyStartedError,
    FlowConfig,
    FlowDefinitionError,
    FlowErrorType,
    FlowNotFoundError,
    FlowStatus,
    IdReusePolicy,
    StartFlowOptions,
    StepExecutionId,
    ValueMappingError,
    WaitForStepCompletionOptions,
)

from .abnormal_exit_flow import AbnormalExitFlow
from .async_environment import AsyncDexDevTestEnvironment
from .flow_service_client import skip_if_server_zd
from .basic_flow import BasicFlow
from .empty_input_flow import EmptyInputFlow
from .immutable_step_options_flow import ImmutableStepOptionsFlow
from .mixed_wait_flow import MixedWaitFlow
from .model_input_flow import ModelInputFlow
from .proceed_on_wait_failure_flow import ProceedOnWaitFailureFlow
from .shared import ModelInput, unique_id
from .signal_flow import SignalFlow

WAIT_TIMEOUT = timedelta(seconds=30)


def test_basic_workflow() -> None:
    asyncio.run(_basic_workflow())


async def _basic_workflow() -> None:
    flow = BasicFlow()
    async with AsyncDexDevTestEnvironment(flow) as environment:
        flow_id = unique_id("basic")
        options = StartFlowOptions(id_reuse_policy=IdReusePolicy.DISALLOW)
        await environment.client.start_flow(flow, flow_id, 0, options)
        assert (
            await environment.client.wait_for_flow(flow_id, WAIT_TIMEOUT)
        ).single_output(int) == 2
        with pytest.raises(FlowAlreadyStartedError):
            await environment.client.start_flow(flow, flow_id, 0, options)


def test_basic_workflow_abnormal_exit_reuse() -> None:
    asyncio.run(_basic_workflow_abnormal_exit_reuse())


async def _basic_workflow_abnormal_exit_reuse() -> None:
    abnormal = AbnormalExitFlow()
    basic = BasicFlow()
    async with AsyncDexDevTestEnvironment(abnormal, basic) as environment:
        flow_id = unique_id("abnormal-exit-reuse")
        options = StartFlowOptions(
            id_reuse_policy=IdReusePolicy.ALLOW_IF_PREVIOUS_FAILED
        )
        await environment.client.start_flow(abnormal, flow_id, 0, options)
        failed = await environment.client.wait_for_flow(flow_id, WAIT_TIMEOUT)
        assert failed.status is FlowStatus.FAILED
        await environment.client.start_flow(basic, flow_id, 0, options)
        assert (
            await environment.client.wait_for_flow(flow_id, WAIT_TIMEOUT)
        ).single_output(int) == 2


def test_empty_input_workflow() -> None:
    asyncio.run(_empty_input_workflow())


async def _empty_input_workflow() -> None:
    flow = EmptyInputFlow()
    async with AsyncDexDevTestEnvironment(flow) as environment:
        flow_id = unique_id("empty-input")
        await environment.client.start_flow(flow, flow_id, None)
        assert not (
            await environment.client.wait_for_flow(flow_id, WAIT_TIMEOUT)
        ).completions
        with pytest.raises(FlowNotFoundError):
            (
                await environment.client.wait_for_flow(
                    unique_id("missing"), timedelta(seconds=1)
                )
            ).single_output(int)


def test_custom_flow_type() -> None:
    asyncio.run(_custom_flow_type())


async def _custom_flow_type() -> None:
    flow = EmptyInputFlow()
    async with AsyncDexDevTestEnvironment(flow) as environment:
        flow_id = unique_id("type-specified")
        assert flow.get_flow_type() == "test-customized-flow-type"
        await environment.client.start_flow(flow, flow_id, None)
        completed = await environment.client.wait_for_flow(
            flow_id,
            WAIT_TIMEOUT,
        )
        assert not completed.completions
        with pytest.raises(
            FlowDefinitionError, match="Flow instance is not registered"
        ):
            await environment.client.start_flow(
                EmptyInputFlow(), unique_id("unregistered"), None
            )


def test_model_input_workflow() -> None:
    asyncio.run(_model_input_workflow())


async def _model_input_workflow() -> None:
    flow = ModelInputFlow()
    async with AsyncDexDevTestEnvironment(flow) as environment:
        flow_id = unique_id("model-input")
        await environment.client.start_flow(flow, flow_id, ModelInput(value=10))
        assert (
            await environment.client.wait_for_flow(flow_id, WAIT_TIMEOUT)
        ).single_output(int) == 10
        with pytest.raises(ValueMappingError):
            await environment.client.start_flow(
                flow,
                unique_id("wrong-input"),
                cast(ModelInput, "wrong"),
            )


def test_flow_config_override() -> None:
    asyncio.run(_flow_config_override())


async def _flow_config_override() -> None:
    flow = BasicFlow()
    async with AsyncDexDevTestEnvironment(flow) as environment:
        flow_id = unique_id("config-override")
        options = StartFlowOptions(
            config_override=FlowConfig(continue_as_new_threshold=1)
        )
        await environment.client.start_flow(flow, flow_id, 0, options)
        assert (
            await environment.client.wait_for_flow(flow_id, WAIT_TIMEOUT)
        ).single_output(int) == 2


def test_describe_missing_flow() -> None:
    asyncio.run(_describe_missing_flow())


async def _describe_missing_flow() -> None:
    flow = BasicFlow()
    async with AsyncDexDevTestEnvironment(flow) as environment:
        with pytest.raises(FlowNotFoundError):
            await environment.client.describe_flow(unique_id("missing"))


def test_describe_running_flow() -> None:
    asyncio.run(_describe_running_flow())


async def _describe_running_flow() -> None:
    flow = SignalFlow()
    async with AsyncDexDevTestEnvironment(flow) as environment:
        flow_id = unique_id("running")
        await environment.client.start_flow(flow, flow_id, 0)
        assert (
            await environment.client.describe_flow(flow_id)
        ).status is FlowStatus.RUNNING
        await environment.client.stop_flow(flow_id)


def test_wait_for_step_completion() -> None:
    asyncio.run(_wait_for_step_completion())


async def _wait_for_step_completion() -> None:
    flow = BasicFlow()
    async with AsyncDexDevTestEnvironment(flow) as environment:
        flow_id = unique_id("wait-step")
        await environment.client.start_flow(flow, flow_id, 5)
        await environment.client.wait_for_step_completion(
            flow_id,
            StepExecutionId("BasicSecondStep"),
            WaitForStepCompletionOptions(f"{flow_id}-wait-second-step", WAIT_TIMEOUT),
        )
        assert (
            await environment.client.wait_for_flow(flow_id, WAIT_TIMEOUT)
        ).single_output(int) == 7


def test_proceed_on_wait_failure() -> None:
    # C21: server-zd rejects Step failure policies.
    skip_if_server_zd("Step failure policies")
    asyncio.run(_proceed_on_wait_failure())


async def _proceed_on_wait_failure() -> None:
    flow = ProceedOnWaitFailureFlow()
    async with AsyncDexDevTestEnvironment(flow) as environment:
        flow_id = unique_id("proceed-on-wait-failure")
        await environment.client.start_flow(flow, flow_id, "input")
        assert (
            await environment.client.wait_for_flow(flow_id, WAIT_TIMEOUT)
        ).single_output(str) == "input-recovered"


def test_mixed_wait_styles() -> None:
    asyncio.run(_mixed_wait_styles())


async def _mixed_wait_styles() -> None:
    flow = MixedWaitFlow()
    async with AsyncDexDevTestEnvironment(flow) as environment:
        flow_id = unique_id("mixed-wait")
        await environment.client.start_flow(flow, flow_id, 0)
        assert (
            await environment.client.wait_for_flow(flow_id, WAIT_TIMEOUT)
        ).single_output(int) == 2


def test_movement_options_do_not_mutate_step_defaults() -> None:
    # C21: server-zd rejects Step failure policies.
    skip_if_server_zd("Step failure policies")
    asyncio.run(_movement_options_do_not_mutate_step_defaults())


async def _movement_options_do_not_mutate_step_defaults() -> None:
    flow = ImmutableStepOptionsFlow()
    async with AsyncDexDevTestEnvironment(flow) as environment:
        flow_id = unique_id("immutable-options")
        await environment.client.start_flow(flow, flow_id, 0)
        failure = await environment.client.wait_for_flow(flow_id, WAIT_TIMEOUT)
        assert failure.status is FlowStatus.FAILED
        assert failure.error_type is FlowErrorType.WORKER_API_FAILED
        assert failure.error_message == "expected wait failure 2"
