# Copyright (c) 2026 Super Durable, Inc.
#
# Licensed under the Sustainable Use License 1.0.
# You may not use this file except in compliance with the License.
# See the LICENSE file in the repository root.
#
# SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

from __future__ import annotations

import traceback
from dataclasses import dataclass
from typing import Any, Literal, cast

import grpc
from google.protobuf import any_pb2
from google.rpc import status_pb2
from grpc_status import rpc_status

from dex.dexpb import dex_pb2 as pb
from dex.runtime_errors import (
    ChannelMessageNotFoundError,
    DexServiceError,
    ErrorSubStatus,
    FlowAlreadyStartedError,
    FlowNotActiveOrNotFoundError,
    FlowNotFoundError,
    LongPollTimeoutError,
    RequestTimeoutError,
    RpcLockConflictError,
    WorkerInvocationError,
)

FlowTargetRequirement = Literal["none", "existing", "active"]

MAX_WORKER_ERROR_DETAIL_BYTES = 1024
MAX_WORKER_ERROR_TYPE_BYTES = 256
MAX_WORKER_STACK_TRACE_BYTES = 4 * 1024
_ERROR_DETAIL_TRUNCATION_MARKER = b"\n... error detail truncated by Dex Python SDK ..."
_ERROR_TYPE_TRUNCATION_MARKER = b"\n... error type truncated by Dex Python SDK ..."
_STACK_TRACE_TRUNCATION_MARKER = b"\n... stack trace truncated by Dex Python SDK ..."


@dataclass(frozen=True)
class RetryAfterError(Exception):
    """Requests a delay before Dex retries a failed worker method.

    Raise an instance with :func:`retry_after` from a Step method when the
    failure is retryable but the next attempt should happen at a known time.
    Dex reports the original cause as the worker failure and uses the requested
    delay instead of the retry policy's calculated interval.

    Attributes:
        after_seconds (int): Positive delay in seconds before the next retry.
        cause (BaseException): Original failure reported to Dex and re-raised
            as the retry cause.
    """

    after_seconds: int
    cause: BaseException

    def __str__(self) -> str:
        return str(self.cause)


def retry_after(after_seconds: int, cause: BaseException) -> RetryAfterError:
    """Creates an error that sets the delay before Dex retries a worker method.

    Raise the returned error from a Step method after an external dependency
    reports that it is not ready. For example::

        raise retry_after(5, RuntimeError("service is not ready"))

    Args:
        after_seconds (int): Positive delay in seconds before the next retry.
        cause (BaseException): Failure that Dex records as the worker error.

    Returns:
        RetryAfterError: Error to raise from the worker method.

    Raises:
        ValueError: If **after_seconds** is not positive or **cause** is None.
    """
    if cause is None:
        raise ValueError("cause is required")
    if after_seconds <= 0:
        raise ValueError("after_seconds must be positive")
    return RetryAfterError(after_seconds=after_seconds, cause=cause)


def translate_rpc_error(
    error: grpc.RpcError,
    operation: str,
    flow_id: str | None,
    requirement: FlowTargetRequirement,
) -> DexServiceError:
    details: pb.ServiceErrorResponse | None = None
    try:
        status = rpc_status.from_call(cast(grpc.Call, error))
        if status is not None:
            for packed in status.details:
                candidate = pb.ServiceErrorResponse()
                if packed.Is(candidate.DESCRIPTOR):
                    packed.Unpack(candidate)
                    details = candidate
                    break
    except Exception as malformed:
        translated = DexServiceError(
            error.code(),
            ErrorSubStatus.UNCATEGORIZED,
            f"Dex returned malformed error details: {malformed}",
            operation,
            flow_id,
        )
        return translated
    detail = (
        details.detail if details is not None and details.detail else error.details()
    ) or str(error)
    code = error.code()
    sub_status = (
        _map_sub_status(details.sub_status)
        if details is not None
        else ErrorSubStatus.UNCATEGORIZED
    )
    parameters = (code, sub_status, detail, operation, flow_id)
    if sub_status is ErrorSubStatus.FLOW_ALREADY_STARTED:
        return FlowAlreadyStartedError(*parameters)
    if sub_status is ErrorSubStatus.FLOW_NOT_EXISTS:
        if requirement == "existing":
            return FlowNotFoundError(*parameters)
        if requirement == "active":
            return FlowNotActiveOrNotFoundError(*parameters)
        return DexServiceError(*parameters)
    if sub_status is ErrorSubStatus.WORKER_API_ERROR:
        if code is grpc.StatusCode.ABORTED:
            return RpcLockConflictError(*parameters)
        return WorkerInvocationError(
            *parameters,
            _worker_code(details),
            details.original_worker_error_type if details is not None else "",
            details.original_worker_error_detail if details is not None else "",
        )
    if sub_status is ErrorSubStatus.LONG_POLL_TIMEOUT:
        return LongPollTimeoutError(*parameters)
    if sub_status is ErrorSubStatus.CHANNEL_MESSAGE_NOT_FOUND:
        return ChannelMessageNotFoundError(*parameters)
    if sub_status is ErrorSubStatus.REQUEST_TIMEOUT:
        return RequestTimeoutError(*parameters)
    return DexServiceError(*parameters)


def _worker_code(details: pb.ServiceErrorResponse | None) -> grpc.StatusCode | None:
    if details is None or details.original_worker_error_status == 0:
        return None
    return next(
        (
            code
            for code in grpc.StatusCode
            if code.value[0] == details.original_worker_error_status
        ),
        grpc.StatusCode.UNKNOWN,
    )


def abort_worker_error(
    context: grpc.ServicerContext,
    error: BaseException,
) -> None:
    context.abort_with_status(rpc_status.to_status(_worker_error_status(error)))


async def async_abort_worker_error(
    context: grpc.aio.ServicerContext[Any, Any],
    error: BaseException,
) -> None:
    # types-grpcio on 3.11 omits aio abort_with_status; runtime provides it.
    await cast(Any, context).abort_with_status(
        rpc_status.to_status(_worker_error_status(error))
    )


def _worker_error_status(error: BaseException) -> status_pb2.Status:
    retry_after_error: RetryAfterError | None = None
    reported = error
    if isinstance(error, RetryAfterError):
        retry_after_error = error
        reported = error.cause

    message = _truncate_worker_failure_field(
        str(reported) or type(reported).__name__,
        MAX_WORKER_ERROR_DETAIL_BYTES,
        _ERROR_DETAIL_TRUNCATION_MARKER,
    )
    stack_trace_source = error if retry_after_error is not None else reported
    worker_error = pb.WorkerErrorResponse(
        detail=message,
        error_type=_truncate_worker_failure_field(
            f"{type(reported).__module__}.{type(reported).__qualname__}",
            MAX_WORKER_ERROR_TYPE_BYTES,
            _ERROR_TYPE_TRUNCATION_MARKER,
        ),
        stack_trace=_worker_stack_trace(stack_trace_source),
        grpc_status=grpc.StatusCode.UNKNOWN.value[0],
    )
    if retry_after_error is not None:
        worker_error.retry_after_seconds = retry_after_error.after_seconds
    packed = any_pb2.Any()
    packed.Pack(worker_error)
    return status_pb2.Status(
        code=grpc.StatusCode.UNKNOWN.value[0],
        message=message,
        details=[packed],
    )


def _worker_stack_trace(error: BaseException) -> str:
    if error.__traceback__ is None:
        return ""
    lines = traceback.format_exception(type(error), error, error.__traceback__)
    return _truncate_worker_failure_field(
        "".join(lines),
        MAX_WORKER_STACK_TRACE_BYTES,
        _STACK_TRACE_TRUNCATION_MARKER,
    )


def _truncate_worker_failure_field(
    value: str,
    maximum_bytes: int,
    truncation_marker: bytes,
) -> str:
    encoded = value.encode()
    if len(encoded) <= maximum_bytes:
        return value
    prefix_length = maximum_bytes - len(truncation_marker)
    while prefix_length > 0 and (encoded[prefix_length] & 0xC0) == 0x80:
        prefix_length -= 1
    return encoded[:prefix_length].decode() + truncation_marker.decode()


def _map_sub_status(value: int) -> ErrorSubStatus:
    statuses: dict[int, ErrorSubStatus] = {
        int(pb.ERROR_SUB_STATUS_UNCATEGORIZED): ErrorSubStatus.UNCATEGORIZED,
        int(
            pb.ERROR_SUB_STATUS_FLOW_ALREADY_STARTED
        ): ErrorSubStatus.FLOW_ALREADY_STARTED,
        int(pb.ERROR_SUB_STATUS_FLOW_NOT_EXISTS): ErrorSubStatus.FLOW_NOT_EXISTS,
        int(pb.ERROR_SUB_STATUS_WORKER_API_ERROR): ErrorSubStatus.WORKER_API_ERROR,
        int(pb.ERROR_SUB_STATUS_LONG_POLL_TIME_OUT): ErrorSubStatus.LONG_POLL_TIMEOUT,
        int(
            pb.ERROR_SUB_STATUS_CHANNEL_MESSAGE_NOT_FOUND
        ): ErrorSubStatus.CHANNEL_MESSAGE_NOT_FOUND,
        int(pb.ERROR_SUB_STATUS_REQUEST_TIMEOUT): ErrorSubStatus.REQUEST_TIMEOUT,
    }
    return statuses.get(value, ErrorSubStatus.UNCATEGORIZED)
