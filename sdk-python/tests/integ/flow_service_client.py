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

import os
import time
from functools import cache
from typing import cast

import grpc
import pytest
from dex.dexpb import dex_pb2 as pb
from dex.dexpb import dex_pb2_grpc as pb_grpc
from google.protobuf.empty_pb2 import Empty


def flow_service_client() -> pb_grpc.FlowServiceStub:
    server_address = os.environ.get("DEX_SERVER_ADDRESS", "127.0.0.1:8801")
    channel = grpc.insecure_channel(server_address)
    return pb_grpc.FlowServiceStub(channel)  # type: ignore[no-untyped-call]


def skip_if_server_zd(feature: str) -> None:
    if _server_engine() == pb.SERVER_ENGINE_ZD:
        pytest.skip(f"server-zd does not support {feature}")


@cache
def _server_engine() -> int:
    server_address = os.environ.get("DEX_SERVER_ADDRESS", "127.0.0.1:8801")
    with grpc.insecure_channel(server_address) as channel:
        client = pb_grpc.FlowServiceStub(channel)  # type: ignore[no-untyped-call]
        server_info = client.GetServerInfo(Empty())
    return int(server_info.server_engine)


def await_live_worker_failure(
    flow_id: str,
    run_id: str,
    *,
    timeout_seconds: float = 6.0,
) -> pb.StepMethodFailure:
    client = flow_service_client()
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        response = client.GetFlowState(
            pb.GetFlowStateRequest(flow_id=flow_id, run_id=run_id)
        )
        for step in response.active_step_executions:
            if step.HasField("last_failure_info") and step.last_failure_info.HasField(
                "details"
            ):
                return cast(pb.StepMethodFailure, step.last_failure_info)
        time.sleep(0.05)
    raise AssertionError("active Step did not expose retry failure")


def assert_worker_failure_stack_trace(
    failure: pb.StepMethodFailure,
    expected_detail: str,
) -> None:
    assert failure.attempt == 1
    assert failure.details.original_worker_error_detail == expected_detail
    stack_trace = failure.details.original_worker_error_stack_trace
    assert stack_trace
    assert expected_detail in stack_trace
