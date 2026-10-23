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
from datetime import timedelta

import pytest

from dex import StepExecutionId, WaitForStepCompletionOptions

from .async_environment import AsyncDexDevTestEnvironment
from .flow_service_client import skip_if_server_zd
from .shared import unique_id
from .step_cancellation_flow import CancellationScenario, StepCancellationFlow


@pytest.mark.parametrize("scenario", list(CancellationScenario))
def test_step_cancellation(scenario: CancellationScenario) -> None:
    if scenario not in {
        CancellationScenario.GLOBAL_SELECTOR,
        CancellationScenario.SIBLING_SELECTOR,
    }:
        skip_if_server_zd("Step failure policies")
    asyncio.run(_run_step_cancellation(scenario))


async def _run_step_cancellation(scenario: CancellationScenario) -> None:
    flow = StepCancellationFlow(scenario)
    async with AsyncDexDevTestEnvironment(
        flow, allow_async_handlers=True
    ) as environment:
        flow_id = unique_id(f"python-cancellation-{scenario.value}")
        await environment.client.start_flow(flow, flow_id, scenario.value)

        if scenario not in {
            CancellationScenario.GLOBAL_SELECTOR,
            CancellationScenario.SIBLING_SELECTOR,
        }:
            await asyncio.wait_for(flow.blocking_started.wait(), timeout=10)
            selected = (
                flow.blocking_wait_for
                if scenario is CancellationScenario.HEARTBEAT_WAIT_FOR
                else flow.blocking_execute
            )
            await environment.client.wait_for_step_completion(
                flow_id,
                StepExecutionId(selected.get_step_type()),
                WaitForStepCompletionOptions(
                    f"{flow_id}-wait-selected-step", timedelta(seconds=30)
                ),
            )

        result = await environment.client.wait_for_flow(flow_id, timedelta(seconds=30))
        assert result.single_output(str) == scenario.value

        if scenario is CancellationScenario.GLOBAL_SELECTOR:
            assert not flow.first_selector_executed
            assert not flow.second_selector_executed
            return
        if scenario is CancellationScenario.SIBLING_SELECTOR:
            assert not flow.first_selector_executed
            assert flow.second_selector_executed
            return
        if scenario is CancellationScenario.NO_HEARTBEAT:
            assert not flow.handler_canceled
            assert not flow.late_handler_returned.is_set()
            await asyncio.wait_for(flow.late_handler_returned.wait(), timeout=8)
        else:
            await asyncio.wait_for(flow.cancellation_observed.wait(), timeout=8)
            assert flow.handler_canceled
            assert flow.context_reported_cancellation
        expected_invocations = (
            2
            if scenario
            in {
                CancellationScenario.LOCAL_EXECUTE,
                CancellationScenario.LOCAL_TIMEOUT_FALLBACK,
            }
            else 1
        )
        assert flow.blocking_invocations == expected_invocations
        assert not flow.recovery_ran
