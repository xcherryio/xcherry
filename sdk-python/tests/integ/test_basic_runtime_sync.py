# Portions of this file are derived from indeedeng/iwf-java-sdk.
# Those portions are licensed under the Apache License, Version 2.0.
# See LICENSES/Apache-2.0.txt and LEGACY_NOTICES.md.
#
# Modifications Copyright (c) 2026 Super Durable, Inc.
#
# Modifications are licensed under the Sustainable Use License 1.0.
# Third-Party Materials remain under the Apache License, Version 2.0.
# See LICENSE and LEGACY_NOTICES.md.

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
from .basic_flow import BasicFlow
from .empty_input_flow import EmptyInputFlow
from .environment import DexDevTestEnvironment
from .flow_service_client import skip_if_server_zd
from .immutable_step_options_flow import ImmutableStepOptionsFlow
from .mixed_wait_flow import MixedWaitFlow
from .model_input_flow import ModelInputFlow
from .proceed_on_wait_failure_flow import ProceedOnWaitFailureFlow
from .shared import ModelInput, unique_id
from .signal_flow import SignalFlow

WAIT_TIMEOUT = timedelta(seconds=30)


def test_basic_workflow() -> None:
    flow = BasicFlow()
    with DexDevTestEnvironment(flow) as environment:
        flow_id = unique_id("basic")
        options = StartFlowOptions(id_reuse_policy=IdReusePolicy.DISALLOW)
        environment.client.start_flow(flow, flow_id, 0, options)
        assert (
            environment.client.wait_for_flow(flow_id, WAIT_TIMEOUT).single_output(int)
            == 2
        )
        with pytest.raises(FlowAlreadyStartedError):
            environment.client.start_flow(flow, flow_id, 0, options)


def test_basic_workflow_abnormal_exit_reuse() -> None:
    abnormal = AbnormalExitFlow()
    basic = BasicFlow()
    with DexDevTestEnvironment(abnormal, basic) as environment:
        flow_id = unique_id("abnormal-exit-reuse")
        options = StartFlowOptions(
            id_reuse_policy=IdReusePolicy.ALLOW_IF_PREVIOUS_FAILED
        )
        environment.client.start_flow(abnormal, flow_id, 0, options)
        failed = environment.client.wait_for_flow(flow_id, WAIT_TIMEOUT)
        assert failed.status is FlowStatus.FAILED
        environment.client.start_flow(basic, flow_id, 0, options)
        assert (
            environment.client.wait_for_flow(flow_id, WAIT_TIMEOUT).single_output(int)
            == 2
        )


def test_empty_input_workflow() -> None:
    flow = EmptyInputFlow()
    with DexDevTestEnvironment(flow) as environment:
        flow_id = unique_id("empty-input")
        environment.client.start_flow(flow, flow_id, None)
        assert not environment.client.wait_for_flow(flow_id, WAIT_TIMEOUT).completions
        with pytest.raises(FlowNotFoundError):
            environment.client.wait_for_flow(
                unique_id("missing"), timedelta(seconds=1)
            ).single_output(int)


def test_custom_flow_type() -> None:
    flow = EmptyInputFlow()
    with DexDevTestEnvironment(flow) as environment:
        flow_id = unique_id("type-specified")
        assert flow.get_flow_type() == "test-customized-flow-type"
        environment.client.start_flow(flow, flow_id, None)
        assert environment.client.wait_for_flow(flow_id, WAIT_TIMEOUT).completions == ()
        with pytest.raises(
            FlowDefinitionError, match="Flow instance is not registered"
        ):
            environment.client.start_flow(
                EmptyInputFlow(), unique_id("unregistered"), None
            )


def test_model_input_workflow() -> None:
    flow = ModelInputFlow()
    with DexDevTestEnvironment(flow) as environment:
        flow_id = unique_id("model-input")
        environment.client.start_flow(flow, flow_id, ModelInput(value=10))
        assert (
            environment.client.wait_for_flow(flow_id, WAIT_TIMEOUT).single_output(int)
            == 10
        )
        with pytest.raises(ValueMappingError):
            environment.client.start_flow(
                flow,
                unique_id("wrong-input"),
                cast(ModelInput, "wrong"),
            )


def test_flow_config_override() -> None:
    skip_if_server_zd("Continue-As-New")
    flow = BasicFlow()
    with DexDevTestEnvironment(flow) as environment:
        flow_id = unique_id("config-override")
        options = StartFlowOptions(
            config_override=FlowConfig(continue_as_new_threshold=1)
        )
        environment.client.start_flow(flow, flow_id, 0, options)
        assert (
            environment.client.wait_for_flow(flow_id, WAIT_TIMEOUT).single_output(int)
            == 2
        )


def test_describe_missing_flow() -> None:
    flow = BasicFlow()
    with DexDevTestEnvironment(flow) as environment:
        with pytest.raises(FlowNotFoundError):
            environment.client.describe_flow(unique_id("missing"))


def test_describe_running_flow() -> None:
    flow = SignalFlow()
    with DexDevTestEnvironment(flow) as environment:
        flow_id = unique_id("running")
        environment.client.start_flow(flow, flow_id, 0)
        assert environment.client.describe_flow(flow_id).status is FlowStatus.RUNNING
        environment.client.stop_flow(flow_id)


def test_wait_for_step_completion() -> None:
    flow = BasicFlow()
    with DexDevTestEnvironment(flow) as environment:
        flow_id = unique_id("wait-step")
        environment.client.start_flow(flow, flow_id, 5)
        environment.client.wait_for_step_completion(
            flow_id,
            StepExecutionId("BasicSecondStep"),
            WaitForStepCompletionOptions(f"{flow_id}-wait-second-step", WAIT_TIMEOUT),
        )
        assert (
            environment.client.wait_for_flow(flow_id, WAIT_TIMEOUT).single_output(int)
            == 7
        )


def test_proceed_on_wait_failure() -> None:
    skip_if_server_zd("Step failure policies")
    flow = ProceedOnWaitFailureFlow()
    with DexDevTestEnvironment(flow) as environment:
        flow_id = unique_id("proceed-on-wait-failure")
        environment.client.start_flow(flow, flow_id, "input")
        assert (
            environment.client.wait_for_flow(flow_id, WAIT_TIMEOUT).single_output(str)
            == "input-recovered"
        )


def test_mixed_wait_styles() -> None:
    flow = MixedWaitFlow()
    with DexDevTestEnvironment(flow) as environment:
        flow_id = unique_id("mixed-wait")
        environment.client.start_flow(flow, flow_id, 0)
        assert (
            environment.client.wait_for_flow(flow_id, WAIT_TIMEOUT).single_output(int)
            == 2
        )


def test_movement_options_do_not_mutate_step_defaults() -> None:
    skip_if_server_zd("Step failure policies")
    flow = ImmutableStepOptionsFlow()
    with DexDevTestEnvironment(flow) as environment:
        flow_id = unique_id("immutable-options")
        environment.client.start_flow(flow, flow_id, 0)
        failure = environment.client.wait_for_flow(flow_id, WAIT_TIMEOUT)
        assert failure.status is FlowStatus.FAILED
        assert failure.error_type is FlowErrorType.WORKER_API_FAILED
        assert failure.error_message == "expected wait failure 2"
