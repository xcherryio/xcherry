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
import json
import os
import re
import subprocess
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

RUN_ID_PATTERN = re.compile(r"runId\s+(\S+)")


@dataclass(frozen=True)
class FlowSmokeFlags:
    step_start_may_fail: bool = False
    no_start_step: bool = False
    allowed_failure_event_types: frozenset[str] = frozenset()


@dataclass(frozen=True)
class FlowSmokeEntry:
    name: str
    trigger: Callable[["FlowSmokeHttpClient"], Awaitable[tuple[str, str]]]
    flags: FlowSmokeFlags = field(default_factory=FlowSmokeFlags)
    # Names the feature that makes server-zd reject this sample.
    server_zd_unsupported_feature: str = ""


class FlowSmokeHttpClient:
    def __init__(self, base_url: str, new_flow_id: Callable[[str], str]) -> None:
        self.base_url = base_url.rstrip("/")
        self.new_flow_id = new_flow_id

    async def get(
        self,
        path: str,
        query: dict[str, str] | None = None,
    ) -> tuple[str, str, str]:
        return await self._request("GET", path, query, None)

    async def post(
        self,
        path: str,
        body: Any = None,
        query: dict[str, str] | None = None,
    ) -> tuple[str, str, str]:
        return await self._request("POST", path, query, body)

    async def _request(
        self,
        method: str,
        path: str,
        query: dict[str, str] | None,
        body: Any,
    ) -> tuple[str, str, str]:
        url = self.base_url + path
        if query:
            url += "?" + urlencode(query)
        headers = {"Content-Type": "application/json"} if body is not None else {}
        payload = None if body is None else json.dumps(body).encode("utf-8")

        def do_request() -> tuple[int, str]:
            request = Request(url, data=payload, headers=headers, method=method)
            try:
                with urlopen(request, timeout=30) as response:
                    return response.status, response.read().decode("utf-8")
            except HTTPError as error:
                detail = error.read().decode("utf-8")
                raise AssertionError(
                    f"{method} {path} returned {error.code}: {detail}"
                ) from error

        status, text = await asyncio.to_thread(do_request)
        if status < 200 or status >= 300:
            raise AssertionError(f"{method} {path} returned {status}: {text}")
        workflow_id = (query or {}).get("workflowId", "") or (query or {}).get(
            "username", ""
        )
        flow_id, run_id = parse_flow_trigger_response(text, workflow_id)
        return flow_id, run_id, text


def parse_flow_trigger_response(body: str, workflow_id_from_query: str) -> tuple[str, str]:
    trimmed = body.strip()
    try:
        payload = json.loads(trimmed)
    except json.JSONDecodeError:
        payload = None
    if isinstance(payload, dict):
        flow_id = payload.get("flowID") or payload.get("flowId") or ""
        run_id = payload.get("runID") or payload.get("runId") or ""
        if flow_id:
            return flow_id, run_id
    match = RUN_ID_PATTERN.search(trimmed)
    if match:
        return workflow_id_from_query, match.group(1)
    if trimmed.startswith("Started workflowId: "):
        return trimmed.removeprefix("Started workflowId: "), ""
    if trimmed.startswith("started workflowId: "):
        return trimmed.removeprefix("started workflowId: "), ""
    if workflow_id_from_query:
        return workflow_id_from_query, ""
    return "", trimmed


def flow_service_address() -> str:
    return os.environ.get("DEX_FLOW_SERVICE_ADDRESS", "127.0.0.1:8801")


_DEXCLI_PATH: str | None = None


def dexcli_path() -> str:
    global _DEXCLI_PATH
    if _DEXCLI_PATH:
        return _DEXCLI_PATH
    configured = os.environ.get("DEXCLI_PATH", "").strip()
    if configured:
        _DEXCLI_PATH = configured
        return configured
    repo_root = _find_repo_root()
    output_path = os.path.join(
        os.environ.get("TMPDIR", "/tmp"),
        f"dexcli-python-flow-smoke-{os.getpid()}",
    )
    subprocess.run(
        ["go", "build", "-trimpath", "-o", output_path, "./cmd/dexcli"],
        cwd=os.path.join(repo_root, "cli"),
        env={**os.environ, "GOWORK": "off"},
        check=True,
        capture_output=True,
        text=True,
    )
    _DEXCLI_PATH = output_path
    return output_path


def _find_repo_root() -> str:
    directory = os.path.abspath(os.getcwd())
    while True:
        if os.path.isfile(os.path.join(directory, "cli", "cmd", "dexcli", "main.go")):
            return directory
        parent = os.path.dirname(directory)
        if parent == directory:
            raise RuntimeError(f"find repository root from {os.getcwd()}")
        directory = parent


def run_dexcli_json(args: list[str]) -> dict[str, Any]:
    completed = subprocess.run(
        args,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


async def assert_flow_smoke_start_step(
    entry: FlowSmokeEntry,
    flow_id: str,
    run_id: str,
) -> None:
    if entry.flags.no_start_step:
        return
    deadline = time.monotonic() + 30
    dexcli = dexcli_path()
    server = flow_service_address()
    last_history: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        last_history = await asyncio.to_thread(
            run_dexcli_json,
            [
                dexcli,
                "flow",
                "history",
                flow_id,
                "--server",
                server,
                "--output",
                "json",
                "--page-size",
                "50",
                *([] if not run_id else ["--run-id", run_id]),
            ],
        )
        events = last_history.get("events", [])
        start_step_type = _flow_started_start_step_type(events)
        if start_step_type:
            if entry.flags.step_start_may_fail:
                return
            if _has_start_step_progress(events, start_step_type):
                return
            state = await asyncio.to_thread(
                run_dexcli_json,
                [
                    dexcli,
                    "flow",
                    "state",
                    flow_id,
                    "--server",
                    server,
                    "--output",
                    "json",
                    *([] if not run_id else ["--run-id", run_id]),
                ],
            )
            if state.get("flowStatus") == "FLOW_STATUS_RUNNING" and len(events) > 1:
                return
        await asyncio.sleep(0.2)
    raise AssertionError(
        f"start step did not succeed for {entry.name} "
        f"flow_id={flow_id} run_id={run_id} history={last_history}"
    )


async def assert_flow_smoke_no_unexpected_failures(
    entry: FlowSmokeEntry,
    flow_id: str,
    run_id: str,
) -> None:
    dexcli = dexcli_path()
    server = flow_service_address()
    history = await asyncio.to_thread(
        run_dexcli_json,
        [
            dexcli,
            "flow",
            "history",
            flow_id,
            "--server",
            server,
            "--output",
            "json",
            "--page-size",
            "50",
            *([] if not run_id else ["--run-id", run_id]),
        ],
    )
    events = history.get("events", [])
    for event in events:
        event_type = event.get("type", "")
        if event_type in {"StepExecuteFailed", "StepWaitForFailed"}:
            if (
                not entry.flags.step_start_may_fail
                and event_type not in entry.flags.allowed_failure_event_types
            ):
                raise AssertionError(
                    f"unexpected failure event for {entry.name}: {event_type}"
                )
        elif event_type == "FlowClosed":
            payload = event.get("payload", {})
            if _is_terminal_flow_closed_failure(payload):
                if entry.flags.step_start_may_fail:
                    continue
                raise AssertionError(
                    f"unexpected terminal flow closure for {entry.name}: {payload}"
                )


def _flow_started_start_step_type(events: list[dict[str, Any]]) -> str:
    for event in events:
        payload = event.get("payload")
        if not isinstance(payload, dict):
            payload = event.get("flowStartedOrContinued")
        if not isinstance(payload, dict):
            continue
        event_type = str(event.get("type") or "")
        if event_type and event_type not in {
            "FlowStartedOrContinued",
            "flowStartedOrContinued",
        }:
            continue
        initial_start = payload.get("initialStart") or payload.get("initial_start") or {}
        if not isinstance(initial_start, dict):
            continue
        start_step_type = initial_start.get("startStepType") or initial_start.get(
            "start_step_type"
        )
        if start_step_type:
            return str(start_step_type)
    return ""


def _has_start_step_progress(events: list[dict[str, Any]], start_step_type: str) -> bool:
    for event in events:
        if event.get("type") not in {"StepWaitForCompleted", "StepExecuteCompleted"}:
            continue
        if _history_event_step_type(event.get("payload") or {}) == start_step_type:
            return True
    return False


def _history_event_step_type(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    step_type = payload.get("stepType")
    if step_type:
        return str(step_type)
    nested_context = payload.get("context")
    if isinstance(nested_context, dict):
        nested = nested_context.get("stepType")
        if nested:
            return str(nested)
    input_payload = payload.get("input")
    if isinstance(input_payload, dict):
        nested = input_payload.get("stepType")
        if nested:
            return str(nested)
    return ""


def _is_terminal_flow_closed_failure(payload: dict[str, Any]) -> bool:
    status = payload.get("flowStatus")
    if isinstance(status, str):
        if status in {
            "FLOW_STATUS_COMPLETED",
            "FLOW_STATUS_CONTINUED_AS_NEW",
            "FLOW_STATUS_RUNNING",
            "FLOW_STATUS_UNSPECIFIED",
            "",
        }:
            return False
        return True
    if isinstance(status, (int, float)):
        numeric = int(status)
        return numeric not in {0, 2, 7}
    error_type = payload.get("errorType", "")
    return bool(error_type) and error_type != "FLOW_ERROR_TYPE_UNSPECIFIED"
