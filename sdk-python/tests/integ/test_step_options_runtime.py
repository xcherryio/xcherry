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
from time import monotonic

from dex import (
    FlowConfig,
    StartFlowOptions,
    StepExecutionId,
    WaitForStepCompletionOptions,
)

from .environment import DexDevTestEnvironment
from .flow_service_client import skip_if_server_zd
from .execute_only_flow import ExecuteOnlyFlow
from .shared import unique_id
from .state_options_flow import StateOptionsFlow
from .state_options_override_flow import StateOptionsOverrideFlow
from .state_recovery_flow import StateRecoveryFlow
from .state_recovery_no_wait_flow import StateRecoveryNoWaitFlow
from .timer_flow import TimerFlow

WAIT_TIMEOUT = timedelta(seconds=30)


def test_skip_wait_for_execute_only_steps() -> None:
    flow = ExecuteOnlyFlow()
    with DexDevTestEnvironment(flow) as environment:
        flow_id = unique_id("skip-wait-for")
        environment.client.start_flow(
            flow,
            flow_id,
            0,
            StartFlowOptions(config_override=FlowConfig(continue_as_new_threshold=1)),
        )
        assert (
            environment.client.wait_for_flow(flow_id, WAIT_TIMEOUT).single_output(int)
            == 2
        )


def test_state_options_locks() -> None:
    flow = StateOptionsFlow()
    with DexDevTestEnvironment(flow) as environment:
        flow_id = unique_id("state-options")
        environment.client.start_flow(flow, flow_id, None)
        assert (
            environment.client.wait_for_flow(flow_id, WAIT_TIMEOUT).single_output(str)
            == "success"
        )


def test_movement_options_override_step_defaults() -> None:
    # C21: server-zd rejects Step failure policies.
    skip_if_server_zd("Step failure policies")
    flow = StateOptionsOverrideFlow()
    with DexDevTestEnvironment(flow) as environment:
        flow_id = unique_id("state-options-override")
        environment.client.start_flow(flow, flow_id, "input")
        assert environment.client.wait_for_flow(flow_id, WAIT_TIMEOUT).single_output(
            str
        ) == ("input_state1_start_state1_decide_state2_start_state2_decide")


def test_execute_failure_recovery_with_wait_for() -> None:
    # C21: server-zd rejects Step failure policies.
    skip_if_server_zd("Step failure policies")
    flow = StateRecoveryFlow()
    with DexDevTestEnvironment(flow) as environment:
        flow_id = unique_id("state-recovery")
        environment.client.start_flow(flow, flow_id, 5)
        assert (
            environment.client.wait_for_flow(flow_id, WAIT_TIMEOUT).single_output(int)
            == 10
        )


def test_execute_failure_recovery_without_wait_for() -> None:
    # C21: server-zd rejects Step failure policies.
    skip_if_server_zd("Step failure policies")
    flow = StateRecoveryNoWaitFlow()
    with DexDevTestEnvironment(flow) as environment:
        flow_id = unique_id("state-recovery-no-wait")
        environment.client.start_flow(flow, flow_id, 5)
        assert (
            environment.client.wait_for_flow(flow_id, WAIT_TIMEOUT).single_output(int)
            == 10
        )


def test_timer_duration_and_step_completion() -> None:
    flow = TimerFlow()
    with DexDevTestEnvironment(flow) as environment:
        flow_id = unique_id("basic-timer")
        started_at = monotonic()
        environment.client.start_flow(flow, flow_id, 5)
        environment.client.wait_for_step_completion(
            flow_id,
            StepExecutionId("TimerStep"),
            WaitForStepCompletionOptions(
                f"{flow_id}-wait-timer", timedelta(seconds=10)
            ),
        )
        environment.client.wait_for_flow(flow_id)
        elapsed = monotonic() - started_at
        assert 4 <= elapsed <= 7, f"actual duration: {elapsed}"
