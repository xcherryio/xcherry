# Copyright (c) 2026 Super Durable, Inc.
#
# Licensed under the Sustainable Use License 1.0.
# You may not use this file except in compliance with the License.
# See the LICENSE file in the repository root.
#
# SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

from __future__ import annotations

from enum import Enum
from typing import Literal

import grpc


class ErrorSubStatus(Enum):
    """Classify a FlowService error more precisely than its gRPC status.

    Attributes:
        UNCATEGORIZED: Dex returned no specific classification.
        FLOW_ALREADY_STARTED: The requested Flow ID conflicts with an existing run.
        FLOW_NOT_EXISTS: No Flow with the requested ID exists.
        WORKER_API_ERROR: An application Worker rejected or failed an invocation.
        LONG_POLL_TIMEOUT: A wait ended without observing its condition.
        CHANNEL_MESSAGE_NOT_FOUND: A pending Channel message ID no longer exists.
        REQUEST_TIMEOUT: A caller-defined request budget expired.
    """

    UNCATEGORIZED = "uncategorized"
    FLOW_ALREADY_STARTED = "flow_already_started"
    FLOW_NOT_EXISTS = "flow_not_exists"
    WORKER_API_ERROR = "worker_api_error"
    LONG_POLL_TIMEOUT = "long_poll_timeout"
    CHANNEL_MESSAGE_NOT_FOUND = "channel_message_not_found"
    REQUEST_TIMEOUT = "request_timeout"


class FlowErrorType(Enum):
    """Identify the terminal failure category reported for a Flow.

    Attributes:
        STEP_DECISION_FAILED: A Step decision could not be applied.
        CLIENT_API_FAILED: A Client-originated operation failed the Flow.
        WORKER_API_FAILED: A Worker handler or dispatch failed.
        INVALID_USER_FLOW_CODE: Application Flow definitions or results were invalid.
        INTERNAL: Dex encountered an internal invariant or infrastructure failure.
        FLOW_TIMEOUT: A Dex soft Flow timeout expired under the fail policy.
        SUB_FLOW_START_FAILED: The server could not start a SubFlow within its
            configured retry budget. Only SubFlow results report it. The SubFlow
            never ran, so the result has no outputs and its error message holds
            the last start error.
    """

    STEP_DECISION_FAILED = "step_decision_failed"
    CLIENT_API_FAILED = "client_api_failed"
    WORKER_API_FAILED = "worker_api_failed"
    INVALID_USER_FLOW_CODE = "invalid_user_flow_code"
    INTERNAL = "internal"
    FLOW_TIMEOUT = "flow_timeout"
    SUB_FLOW_START_FAILED = "sub_flow_start_failed"


class DexServiceError(RuntimeError):
    """Represent a structured error returned by a Dex FlowService operation.

    Attributes:
        code: The outer gRPC status code.
        sub_status: The stable Dex-specific error classification.
        detail: The human-readable service detail.
        operation: The Client operation that failed.
        flow_id: The targeted Flow ID, or ``None`` for service-wide calls.
    """

    def __init__(
        self,
        code: grpc.StatusCode,
        sub_status: ErrorSubStatus,
        detail: str,
        operation: str,
        flow_id: str | None,
    ) -> None:
        """Create a structured FlowService error.

        Args:
            code: The outer gRPC status code.
            sub_status: The Dex-specific classification.
            detail: Human-readable error detail.
            operation: The Client operation name.
            flow_id: The targeted Flow ID, if any.
        """
        super().__init__(detail)
        self.code = code
        self.sub_status = sub_status
        self.detail = detail
        self.operation = operation
        self.flow_id = flow_id


class FlowAlreadyStartedError(DexServiceError):
    """Indicate that ``start_flow`` conflicts with an existing Flow ID."""

    pass


class FlowNotFoundError(DexServiceError):
    """Indicate that an operation targeted a Flow ID that does not exist."""

    pass


class FlowNotActiveOrNotFoundError(DexServiceError):
    """Report a missing Flow or an operation that cannot use a closed Flow.

    RPC calls use this error for missing targets, including query-only reads.
    A query-only RPC can read a retained terminal execution when it has no
    locks, transactionality, durable effects, or Server-forced Update routing.
    At that confirmed read boundary, handle this error as not found without
    adding a lifecycle probe. Preserve other service and Worker failures.

    Mutations and active-only RPCs also use this error for closed targets.
    The error does not distinguish a missing Flow from a closed Flow or prove
    that the requested action succeeded. The inherited service metadata and
    chained transport cause remain available. For example::

        try:
            client.invoke_rpc(orders.update_order, order_id, update)
        except FlowNotActiveOrNotFoundError:
            raise OrderUnavailableError(order_id)
    """

    pass


class WorkerInvocationError(DexServiceError):
    """Expose both FlowService and nested WorkerService failure details.

    Attributes:
        code: The outer FlowService gRPC status.
        sub_status: Usually ``WORKER_API_ERROR``.
        detail: The outer service detail.
        operation: The Client operation that invoked the Worker.
        flow_id: The targeted Flow ID, if available.
        worker_code: The nested Worker gRPC status, or ``None`` when unavailable.
        worker_error_type: The Worker-reported application error type.
        worker_error_detail: The Worker-reported human-readable detail.
    """

    def __init__(
        self,
        code: grpc.StatusCode,
        sub_status: ErrorSubStatus,
        detail: str,
        operation: str,
        flow_id: str | None,
        worker_code: grpc.StatusCode | None,
        worker_error_type: str,
        worker_error_detail: str,
    ) -> None:
        """Create an error from outer and nested Worker failure metadata.

        Args:
            code: The outer FlowService gRPC status.
            sub_status: The Dex-specific classification.
            detail: The outer service detail.
            operation: The Client operation name.
            flow_id: The targeted Flow ID, if any.
            worker_code: The nested Worker gRPC status, if available.
            worker_error_type: The nested application error type.
            worker_error_detail: The nested human-readable detail.
        """
        super().__init__(code, sub_status, detail, operation, flow_id)
        self.worker_code = worker_code
        self.worker_error_type = worker_error_type
        self.worker_error_detail = worker_error_detail


class RpcLockConflictError(DexServiceError):
    """Indicate that an RPC could not acquire all requested Attribute locks."""

    pass


class LongPollTimeoutError(DexServiceError):
    """Indicate that a retryable long poll ended before its condition was observed."""

    pass


class RequestTimeoutError(DexServiceError):
    """Indicate that a caller-defined request budget expired."""

    pass


class ChannelMessageNotFoundError(DexServiceError):
    """Indicate that a pending Channel message ID no longer exists."""

    pass


class FlowDefinitionError(RuntimeError):
    """Indicate that Registry construction found an invalid Flow definition."""

    pass


class AttributeMapNotLoadedError(RuntimeError):
    """Indicate that an RPC did not load the requested AttributeMap entries."""

    pass


class ChannelMessagesNotLoadedError(RuntimeError):
    """Indicate that an RPC did not load the requested pending Channel messages."""

    pass


class InvalidStepResultError(FlowDefinitionError):
    """Identify a Step or RPC handler result that violates the SDK contract.

    Attributes:
        flow_type: The containing Flow type.
        step_type: The Step type, or ``None`` for an RPC.
        method: The handler method that returned the invalid value.
        detail: A precise description of the contract violation.
    """

    def __init__(
        self,
        flow_type: str,
        step_type: str | None,
        method: Literal["wait_for", "execute", "rpc"],
        detail: str,
    ) -> None:
        """Create an invalid-result definition error.

        Args:
            flow_type: The containing Flow type.
            step_type: The Step type, or ``None`` for an RPC.
            method: ``"wait_for"``, ``"execute"``, or ``"rpc"``.
            detail: A precise description of the invalid result.
        """
        target = (
            f"Flow {flow_type} Step {step_type}"
            if step_type is not None
            else f"RPC in Flow {flow_type}"
        )
        super().__init__(f"{target} {method} returned an invalid result: {detail}")
        self.flow_type = flow_type
        self.step_type = step_type
        self.method = method
        self.detail = detail


class ValueMappingError(RuntimeError):
    """Report an application-value encoding or decoding failure.

    Attributes:
        operation: The mapping operation, such as ``"encode"`` or ``"decode"``.
        detail: The incompatible type, wire kind, or malformed payload detail.
    """

    def __init__(self, operation: str, detail: str) -> None:
        """Create a mapping error with stable operation context.

        Args:
            operation: The attempted mapping operation.
            detail: The human-readable failure detail.
        """
        super().__init__(f"Cannot {operation} Dex Value: {detail}")
        self.operation = operation
        self.detail = detail
