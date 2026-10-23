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
import time
from collections.abc import Callable
from datetime import timedelta

import pytest
from dex_examples.app import ExampleApp
from dex_examples.config import start_options
from dex_examples.products.order_processing.order_request import OrderRequest
from tests.integ.conftest import WAIT_TIMEOUT, skip_when_server_zd

from dex import AsyncClient, StepExecutionId, TimerId, WaitForStepCompletionOptions

pytestmark = pytest.mark.integ

CHARGE_STEP = StepExecutionId("Charge")
SHIP_STEP = StepExecutionId("Ship")
SKIP_TIMEOUT = timedelta(seconds=15)


async def _skip_ship_timer(client: AsyncClient, flow_id: str) -> None:
    deadline = time.monotonic() + SKIP_TIMEOUT.total_seconds()
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            await client.skip_timer(
                flow_id,
                SHIP_STEP,
                TimerId.by_condition_index(0),
            )
            return
        except Exception as error:
            last_error = error
            await asyncio.sleep(0.05)
    raise AssertionError(f"skip timer did not succeed: {last_error}")


async def test_order_processing_happy_path(
    app: ExampleApp,
    client: AsyncClient,
    new_flow_id: Callable[[str], str],
) -> None:
    skip_when_server_zd("failure policies")
    flow_id = new_flow_id("order-processing")
    request = OrderRequest(flow_id, "buyer@example.com", "customer-1", 42)
    await client.start_flow(app.order_processing, flow_id, request, start_options())
    await client.wait_for_step_completion(
        flow_id,
        CHARGE_STEP,
        WaitForStepCompletionOptions(request_timeout=WAIT_TIMEOUT),
    )
    assert await client.invoke_rpc(app.order_processing.approve, flow_id, "") == "ok"
    output = (await client.wait_for_flow(flow_id, WAIT_TIMEOUT)).single_output(str)
    assert output == f"shipped:{flow_id}"


async def test_order_processing_reminder_then_ship(
    app: ExampleApp,
    client: AsyncClient,
    new_flow_id: Callable[[str], str],
) -> None:
    skip_when_server_zd("failure policies")
    flow_id = new_flow_id("order-processing-reminder")
    request = OrderRequest(flow_id, "buyer@example.com", "customer-1", 42)
    await client.start_flow(app.order_processing, flow_id, request, start_options())
    await client.wait_for_step_completion(
        flow_id,
        CHARGE_STEP,
        WaitForStepCompletionOptions(request_timeout=WAIT_TIMEOUT),
    )
    await _skip_ship_timer(client, flow_id)
    await client.wait_for_step_completion(
        flow_id,
        SHIP_STEP,
        WaitForStepCompletionOptions(request_timeout=WAIT_TIMEOUT),
    )
    assert await client.invoke_rpc(app.order_processing.approve, flow_id, "") == "ok"
    output = (await client.wait_for_flow(flow_id, WAIT_TIMEOUT)).single_output(str)
    assert output == f"shipped:{flow_id}"


async def test_order_processing_ship_failure_refunds(
    app: ExampleApp,
    client: AsyncClient,
    new_flow_id: Callable[[str], str],
) -> None:
    skip_when_server_zd("failure policies")
    flow_id = new_flow_id("order-processing-refund")
    request = OrderRequest(flow_id, "buyer@example.com", "customer-1", 42, True)
    await client.start_flow(app.order_processing, flow_id, request, start_options())
    await client.wait_for_step_completion(
        flow_id,
        CHARGE_STEP,
        WaitForStepCompletionOptions(request_timeout=WAIT_TIMEOUT),
    )
    assert await client.invoke_rpc(app.order_processing.approve, flow_id, "") == "ok"
    output = (await client.wait_for_flow(flow_id, WAIT_TIMEOUT)).single_output(str)
    assert output == f"refunded:{flow_id}"
