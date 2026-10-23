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

"""Runs every Dex Python example behind one Quart app and one AsyncWorker."""

from __future__ import annotations

import asyncio
from pathlib import Path

from dex import FlowAlreadyStartedError, StartFlowOptions
from dotenv import load_dotenv

from dex_examples.app import ExampleApp
from dex_examples.config import ExamplesConfig
from dex_examples.http_app import create_app
from dex_examples.patterns.cron.cron_schedule_flow import (
    CRON_SCHEDULE_FLOW_ID,
    CronScheduleInput,
    Interval,
    IntervalUnit,
)


async def main() -> None:
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    config = ExamplesConfig.from_env()
    app_state = ExampleApp(config)
    try:
        await app_state.start_worker()
        await start_cron_schedule(app_state)

        host, port = parse_http_address(config.http_address)
        worker_description = (
            "pull worker"
            if config.worker_mode.is_pull
            else f"worker {app_state.worker.worker_target.address}"
        )
        print(
            f"Dex Python examples listening on http://{config.http_address} "
            f"({worker_description})"
        )
        await create_app(app_state).run_task(host=host, port=port)
    finally:
        await app_state.close()


async def start_cron_schedule(app_state: ExampleApp) -> None:
    try:
        await app_state.client.start_flow(
            app_state.cron_schedule,
            CRON_SCHEDULE_FLOW_ID,
            CronScheduleInput(Interval(1, IntervalUnit.HOUR), 10),
            StartFlowOptions(),
        )
    except FlowAlreadyStartedError:
        pass


def parse_http_address(address: str) -> tuple[str, int]:
    host, _, port = address.rpartition(":")
    if not host:
        return "127.0.0.1", int(address)
    return host, int(port)


if __name__ == "__main__":
    asyncio.run(main())
