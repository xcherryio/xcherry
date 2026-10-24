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
from dex_examples.app import ExampleApp
from dex_examples.config import start_options
from dex_examples.products.money_transfer.transfer_request import TransferRequest
from tests.integ.conftest import WAIT_TIMEOUT, skip_when_server_zd

from dex import AsyncClient

pytestmark = pytest.mark.integ


async def test_money_transfer_completes_the_saga(
    app: ExampleApp,
    client: AsyncClient,
    new_flow_id: Callable[[str], str],
) -> None:
    skip_when_server_zd("failure policies")
    flow_id = new_flow_id("money-transfer")
    request = TransferRequest("from-account", "to-account", 100, "test-notes")

    await client.start_flow(app.money_transfer, flow_id, request, start_options())
    output = (await client.wait_for_flow(flow_id, WAIT_TIMEOUT)).single_output(str)

    assert "transfer is done" in output
    assert "from from-account to to-account for amount 100" in output
