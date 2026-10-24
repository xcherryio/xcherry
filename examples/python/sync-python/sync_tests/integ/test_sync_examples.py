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

from typing import Callable

import pytest
from dex_examples.products.engagement.engagement_flow import EngagementFlow
from dex_examples.products.engagement.engagement_input import EngagementInput
from dex_examples.products.engagement.status import Status
from dex_examples.products.money_transfer.transfer_request import TransferRequest
from dex_examples.products.subscription.customer import Customer
from dex_examples.products.subscription.subscription import Subscription
from dex_examples.products.subscription.subscription_flow import SubscriptionFlow
from sync_examples.app import SyncExampleApp
from sync_examples.config import DEFAULT_TIMEOUT, start_options
from sync_examples.wait import (
    WAIT_TIMEOUT,
    flow_status_or_none,
    wait_until,
)
from sync_tests.integ.conftest import skip_when_server_zd

from dex import (
    AttributeMatch,
    Client,
    FlowStatus,
    IdReusePolicy,
    StartFlowOptions,
    WaitForAttributeOptions,
)

pytestmark = pytest.mark.integ


def test_channel_approve_completes(
    app: SyncExampleApp,
    client: Client,
    new_flow_id: Callable[[str], str],
) -> None:
    flow_id = new_flow_id("sync-channel")
    client.start_flow(app.channel, flow_id, 5, start_options())
    client.invoke_rpc(app.channel.publish_approval_message, flow_id)
    assert client.wait_for_flow(flow_id, WAIT_TIMEOUT).single_output(str) == "approved"


def test_money_transfer_completes_the_saga(
    app: SyncExampleApp,
    client: Client,
    new_flow_id: Callable[[str], str],
) -> None:
    skip_when_server_zd("failure policies")
    flow_id = new_flow_id("sync-money")
    request = TransferRequest("from-account", "to-account", 100, "test-notes")
    client.start_flow(app.money_transfer, flow_id, request, start_options())
    output = client.wait_for_flow(flow_id, WAIT_TIMEOUT).single_output(str)
    assert "transfer is done" in output
    assert "from from-account to to-account for amount 100" in output


def test_engagement_accept_completes(
    app: SyncExampleApp,
    client: Client,
    new_flow_id: Callable[[str], str],
) -> None:
    flow_id = new_flow_id("sync-engagement")
    client.start_flow(
        app.engagement,
        flow_id,
        EngagementInput("test-employer-id", "test-job-seeker-id", "test-notes"),
        start_options(),
    )
    client.wait_for_attribute_match(
        flow_id,
        EngagementFlow.employer_id,
        AttributeMatch.equal_to("test-employer-id"),
        WaitForAttributeOptions(request_timeout=WAIT_TIMEOUT),
    )
    description = client.invoke_rpc(app.engagement.describe, flow_id)
    assert description.employer_id == "test-employer-id"
    assert description.current_status == Status.INITIATED
    assert client.invoke_rpc(app.engagement.accept, flow_id, "sounds good") is (
        Status.ACCEPTED
    )
    assert client.wait_for_flow(flow_id, WAIT_TIMEOUT).single_output(str) == "done"


def test_subscription_describe_and_cancel(
    app: SyncExampleApp,
    client: Client,
    new_flow_id: Callable[[str], str],
) -> None:
    flow_id = new_flow_id("sync-subscription")
    customer = Customer(
        "Quanzheng",
        "Long",
        "qlong",
        "qlong@example.com",
        Subscription(
            trial_period_seconds=600,
            billing_period_seconds=1,
            max_billing_periods=10,
            billing_period_charge=100,
        ),
    )
    client.start_flow(app.subscription, flow_id, customer, start_options())
    client.wait_for_attribute_match(
        flow_id,
        SubscriptionFlow.billing_period_number,
        AttributeMatch.greater_than_or_equal(0),
        WaitForAttributeOptions(request_timeout=WAIT_TIMEOUT),
    )
    subscription = client.invoke_rpc(app.subscription.describe, flow_id)
    assert subscription.billing_period_charge == 100
    client.invoke_rpc(app.subscription.cancel_subscription_rpc, flow_id)
    assert (
        client.wait_for_flow(flow_id, WAIT_TIMEOUT).single_output(str)
        == "subscription canceled"
    )


def test_parent_child_starts_a_child(
    app: SyncExampleApp,
    client: Client,
    new_flow_id: Callable[[str], str],
) -> None:
    flow_id = new_flow_id("sync-parent-child")
    run_id = client.start_flow(
        app.parent_flow_v2,
        flow_id,
        2,
        StartFlowOptions(
            timeout=DEFAULT_TIMEOUT,
            id_reuse_policy=IdReusePolicy.ALLOW_IF_PREVIOUS_FAILED,
        ),
    )
    assert run_id
    child_id = f"{flow_id}-child-0"
    wait_until(
        "parent-child started a child flow",
        lambda: flow_status_or_none(client, child_id),
        WAIT_TIMEOUT,
    )


def test_interruptible_start_and_cancel(
    app: SyncExampleApp,
    client: Client,
    new_flow_id: Callable[[str], str],
) -> None:
    flow_id = new_flow_id("sync-interruptible")
    client.start_flow(app.interruptible, flow_id, None, start_options())
    wait_until(
        "interruptible flow running",
        lambda: flow_status_or_none(client, flow_id) is FlowStatus.RUNNING,
    )
    client.invoke_rpc(app.interruptible.interrupt, flow_id)
    wait_until(
        "interruptible flow completed",
        lambda: flow_status_or_none(client, flow_id) is FlowStatus.COMPLETED,
        WAIT_TIMEOUT,
    )
