# Portions of this file are derived from indeedeng/iwf-java-sdk.
# Those portions are licensed under the Apache License, Version 2.0.
# See LICENSES/Apache-2.0.txt and LEGACY_NOTICES.md.
#
# Modifications Copyright (c) 2026 Super Durable, Inc.
#
# Modifications are licensed under the Sustainable Use License 1.0.
# Third-Party Materials remain under the Apache License, Version 2.0.
# See LICENSE and LEGACY_NOTICES.md.

import time
from datetime import timedelta

from dex import (
    DexServiceError,
    FlowConfig,
    FlowNotFoundError,
    FlowStatus,
    TimeTravelOptions,
    TimeTravelType,
    StartFlowOptions,
    StepExecutionId,
    SubFlowReusePolicy,
    TimerId,
)

from .abnormal_exit_flow import AbnormalExitFlow
from .basic_flow import BasicFlow
from .environment import DexDevTestEnvironment
from .flow_service_client import skip_if_server_zd
from .shared import unique_id
from .subflow_flow import (
    AllSubFlowParent,
    AnySubFlowParent,
    ContinueAsNewSubFlowParent,
    SingleSubFlowParent,
)
from .timer_flow import TimerFlow

WAIT_TIMEOUT = timedelta(seconds=30)


def test_subflow_returns_identity_and_output() -> None:
    child = BasicFlow()
    parent = SingleSubFlowParent(child)
    with DexDevTestEnvironment(parent, child) as environment:
        flow_id = unique_id("sub-flow-parent")
        environment.client.start_flow(parent, flow_id, 4)
        output = environment.client.wait_for_flow(flow_id, WAIT_TIMEOUT).single_output(
            str
        )
        assert (
            output
            == f"SubFlow:{flow_id}-{parent.start.get_step_type()}-1-0|COMPLETED|6"
        )


def test_subflow_all_of_returns_stable_terminal_results() -> None:
    child = BasicFlow()
    parent = AllSubFlowParent(child)
    with DexDevTestEnvironment(parent, child) as environment:
        flow_id = unique_id("sub-flow-all")
        environment.client.start_flow(parent, flow_id, 4)
        output = environment.client.wait_for_flow(flow_id, WAIT_TIMEOUT).single_output(
            str
        )
        assert output.split(";") == [
            f"SubFlow:{flow_id}-{parent.start.get_step_type()}-1-0|COMPLETED|6",
            f"SubFlow:{flow_id}-{parent.start.get_step_type()}-1-1|COMPLETED|16",
        ]


def test_subflow_any_of_running_snapshot_can_be_stopped() -> None:
    child = TimerFlow()
    parent = AnySubFlowParent(child)
    with DexDevTestEnvironment(parent, child) as environment:
        flow_id = unique_id("sub-flow-any")
        environment.client.start_flow(parent, flow_id, 300)
        output = environment.client.wait_for_flow(flow_id, WAIT_TIMEOUT).single_output(
            str
        )
        child_id, status, terminal, rejected = output.split("|")
        assert child_id == f"SubFlow:{flow_id}-{parent.start.get_step_type()}-1-0"
        assert (status, terminal, rejected) == ("RUNNING", "false", "true")
        environment.client.stop_flow(child_id)
        assert (
            environment.client.wait_for_flow(child_id, WAIT_TIMEOUT).status
            is FlowStatus.CANCELED
        )


def test_subflow_attach_keeps_running_execution_across_parent_reset() -> None:
    _assert_running_reuse(SubFlowReusePolicy.ATTACH, False)


def test_subflow_always_restart_replaces_running_execution_across_parent_reset() -> (
    None
):
    _assert_running_reuse(SubFlowReusePolicy.ALWAYS_RESTART, True)


def test_subflow_default_reuse_restarts_failed_execution_across_parent_reset() -> None:
    child = AbnormalExitFlow()
    parent = SingleSubFlowParent(child)
    with DexDevTestEnvironment(parent, child) as environment:
        flow_id = unique_id("sub-flow-abnormal")
        child_id = f"SubFlow:{flow_id}-{parent.start.get_step_type()}-1-0"
        environment.client.start_flow(parent, flow_id, 1)
        assert (
            environment.client.wait_for_flow(flow_id, WAIT_TIMEOUT)
            .single_output(str)
            .split("|")[1]
            == "FAILED"
        )
        first_run_id = environment.client.describe_flow(child_id).run_id
        environment.client.time_travel(
            flow_id,
            TimeTravelOptions(
                TimeTravelType.BEGINNING, reason="verify SubFlow abnormal reuse"
            ),
        )
        assert (
            environment.client.wait_for_flow(flow_id, WAIT_TIMEOUT)
            .single_output(str)
            .split("|")[1]
            == "FAILED"
        )
        assert environment.client.describe_flow(child_id).run_id != first_run_id


def test_subflow_partial_results_survive_continue_as_new_without_restart() -> None:
    # C3: server-zd never continues a run as new.
    skip_if_server_zd("Continue-As-New")
    completed = BasicFlow()
    delayed = TimerFlow()
    parent = ContinueAsNewSubFlowParent(completed, delayed)
    with DexDevTestEnvironment(parent, completed, delayed) as environment:
        flow_id = unique_id("sub-flow-can")
        completed_id = f"SubFlow:{flow_id}-{parent.start.get_step_type()}-1-0"
        delayed_id = f"SubFlow:{flow_id}-{parent.start.get_step_type()}-1-1"
        first_parent_run_id = environment.client.start_flow(
            parent,
            flow_id,
            4,
            StartFlowOptions(config_override=FlowConfig(continue_as_new_threshold=1)),
        )
        _await_new_run(environment, flow_id, first_parent_run_id)
        completed_run_id = environment.client.describe_flow(completed_id).run_id
        _skip_subflow_timer_when_pending(environment, delayed_id)
        output = environment.client.wait_for_flow(flow_id, WAIT_TIMEOUT).single_output(
            str
        )
        assert output.split("|") == [completed_id, "6", delayed_id, "COMPLETED"]
        assert environment.client.describe_flow(completed_id).run_id == completed_run_id


def _assert_running_reuse(policy: SubFlowReusePolicy, expects_restart: bool) -> None:
    child = TimerFlow()
    parent = SingleSubFlowParent(child, policy)
    with DexDevTestEnvironment(parent, child) as environment:
        flow_id = unique_id("sub-flow-reuse")
        child_id = f"SubFlow:{flow_id}-{parent.start.get_step_type()}-1-0"
        environment.client.start_flow(parent, flow_id, 300)
        first_run_id = _await_running(environment, child_id)
        environment.client.time_travel(
            flow_id,
            TimeTravelOptions(
                TimeTravelType.BEGINNING, reason="verify SubFlow running reuse"
            ),
        )
        active_run_id = _await_running(
            environment,
            child_id,
            first_run_id if expects_restart else None,
        )
        assert (active_run_id != first_run_id) is expects_restart
        _skip_subflow_timer_when_pending(environment, child_id)
        output = environment.client.wait_for_flow(flow_id, WAIT_TIMEOUT).single_output(
            str
        )
        assert output.split("|")[:2] == [child_id, "COMPLETED"]


def _await_running(
    environment: DexDevTestEnvironment,
    flow_id: str,
    excluded_run_id: str | None = None,
) -> str:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            info = environment.client.describe_flow(flow_id)
        except FlowNotFoundError:
            time.sleep(0.01)
            continue
        if info.status is FlowStatus.RUNNING and info.run_id != excluded_run_id:
            return info.run_id
        time.sleep(0.01)
    raise AssertionError(f"SubFlow did not reach expected running execution: {flow_id}")


def _skip_subflow_timer_when_pending(
    environment: DexDevTestEnvironment,
    flow_id: str,
) -> None:
    deadline = time.monotonic() + 10
    while True:
        try:
            environment.client.skip_timer(
                flow_id,
                StepExecutionId("TimerStep"),
                TimerId.by_condition_id("test-timer-id"),
            )
            return
        except DexServiceError as error:
            if "does not exist or is not pending" not in error.detail:
                raise
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.01)


def _await_new_run(
    environment: DexDevTestEnvironment,
    flow_id: str,
    first_run_id: str,
) -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if environment.client.describe_flow(flow_id).run_id != first_run_id:
            return
        time.sleep(0.01)
    raise AssertionError(f"Flow did not continue as new: {flow_id}")
