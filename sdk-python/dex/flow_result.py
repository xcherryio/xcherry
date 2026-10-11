# Copyright (c) 2026 Super Durable, Inc.
#
# Licensed under the Sustainable Use License 1.0.
# You may not use this file except in compliance with the License.
# See the LICENSE file in the repository root.
#
# SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any, TypeVar, cast

from dex.dexpb import dex_pb2 as pb
from dex.flow_info import FlowStatus
from dex.runtime_errors import FlowErrorType

OutputT = TypeVar("OutputT")
_Decoder = Callable[[pb.Value, type[Any]], Any]


class StepCompletion:
    """Contain one output-bearing Step completion returned by ``wait_for_flow``.

    Attributes:
        step_type: The registered Step type that produced the output.
        step_execution_id: The exact server Step execution identity.
    """

    __slots__ = ("_decoder", "_output", "_step_execution_id", "_step_type")

    def __init__(
        self,
        completion: pb.StepCompletionOutput,
        decoder: _Decoder,
    ) -> None:
        """Create a completion around an already hydrated wire value.

        Args:
            completion: The hydrated completion message returned by Dex.
            decoder: The SDK value decoder used by :meth:`decode`.
        """
        if not completion.HasField("completed_step_output"):
            raise ValueError("Step completion output is required")
        self._step_type = completion.completed_step_type
        self._step_execution_id = completion.completed_step_execution_id
        self._output = completion.completed_step_output
        self._decoder = decoder

    @property
    def step_type(self) -> str:
        """Return the registered Step type that produced this output.

        Returns:
            The registered Step type.
        """
        return self._step_type

    @property
    def step_execution_id(self) -> str:
        """Return the exact server Step execution identity.

        Returns:
            The server-assigned Step execution ID.
        """
        return self._step_execution_id

    def decode(self, output_type: type[OutputT]) -> OutputT:
        """Decode this hydrated Step output as ``output_type``.

        Args:
            output_type: The concrete Python type expected by the caller.

        Returns:
            The decoded Step output.

        Raises:
            ValueMappingError: If the output cannot be decoded as ``output_type``.
        """
        return cast(OutputT, self._decoder(self._output, output_type))


class FlowResult:
    """Describe an observed Flow status and its output-bearing completions.

    Client results are terminal. A SubFlow result can be a running snapshot when
    another AnyOf Condition won; that status is not a live backend query.

    Attributes:
        status: The observed lifecycle state.
        error_type: Dex failure category when available.
        error_message: Server failure detail when available.
        completions: An immutable sequence of Step completions.
    """

    __slots__ = ("_completions", "_error_message", "_error_type", "_status")

    def __init__(
        self,
        status: FlowStatus,
        completions: Sequence[StepCompletion],
        error_type: FlowErrorType | None = None,
        error_message: str | None = None,
    ) -> None:
        """Create an immutable Flow result snapshot.

        Args:
            status: The observed Flow status.
            completions: Output-bearing Step completions in server collection order.
            error_type: Optional Dex failure category.
            error_message: Optional server failure detail.
        """
        self._status = status
        self._completions = tuple(completions)
        self._error_type = error_type
        self._error_message = error_message

    @property
    def status(self) -> FlowStatus:
        """Return the observed Flow lifecycle state.

        Returns:
            The status captured by this result.
        """
        return self._status

    @property
    def error_type(self) -> FlowErrorType | None:
        """Return the Dex failure category, if one was reported.

        Returns:
            The failure category, or ``None`` when Dex reported none.
        """
        return self._error_type

    @property
    def error_message(self) -> str | None:
        """Return the server failure detail, if one was reported.

        Returns:
            The failure detail, or ``None`` when Dex reported none.
        """
        return self._error_message

    @property
    def is_terminal(self) -> bool:
        """Return whether this observed run can no longer execute.

        Returns:
            ``True`` for a terminal snapshot; otherwise ``False``.
        """
        return self.status not in (FlowStatus.RUNNING, FlowStatus.CONTINUED_AS_NEW)

    @property
    def completions(self) -> tuple[StepCompletion, ...]:
        """Return completions in server collection order.

        Returns:
            An immutable tuple of output-bearing Step completions.
        """
        return self._completions

    def single_output(self, output_type: type[OutputT]) -> OutputT:
        """Decode the output when exactly one completion exists.

        Args:
            output_type: The concrete Python type expected by the caller.

        Returns:
            The only decoded Step output.

        Raises:
            ValueError: If the result is nonterminal or has zero or multiple outputs.
            ValueMappingError: If the output cannot be decoded as ``output_type``.
        """
        if not self.is_terminal:
            raise ValueError("Flow result is not terminal")
        if len(self.completions) != 1:
            raise ValueError(
                f"Expected exactly one Step output, found {len(self.completions)}"
            )
        return self.completions[0].decode(output_type)


def flow_result_from_proto(result: pb.FlowResult, decoder: _Decoder) -> FlowResult:
    """Map one hydrated wire result into the public immutable representation."""
    statuses = {
        pb.FLOW_STATUS_RUNNING: FlowStatus.RUNNING,
        pb.FLOW_STATUS_COMPLETED: FlowStatus.COMPLETED,
        pb.FLOW_STATUS_FAILED: FlowStatus.FAILED,
        pb.FLOW_STATUS_CANCELED: FlowStatus.CANCELED,
        pb.FLOW_STATUS_TERMINATED: FlowStatus.TERMINATED,
        pb.FLOW_STATUS_SERVER_SIDE_TIMEOUT_INTERNAL_ONLY: FlowStatus.SERVER_SIDE_TIMEOUT_INTERNAL_ONLY,
        pb.FLOW_STATUS_CONTINUED_AS_NEW: FlowStatus.CONTINUED_AS_NEW,
    }
    errors = {
        pb.FLOW_ERROR_TYPE_UNSPECIFIED: None,
        pb.FLOW_ERROR_TYPE_STEP_DECISION_FAILING_FLOW: FlowErrorType.STEP_DECISION_FAILED,
        pb.FLOW_ERROR_TYPE_CLIENT_API_FAILING_FLOW: FlowErrorType.CLIENT_API_FAILED,
        pb.FLOW_ERROR_TYPE_WORKER_API_FAIL: FlowErrorType.WORKER_API_FAILED,
        pb.FLOW_ERROR_TYPE_INVALID_USER_FLOW_CODE: FlowErrorType.INVALID_USER_FLOW_CODE,
        pb.FLOW_ERROR_TYPE_INTERNAL: FlowErrorType.INTERNAL,
        pb.FLOW_ERROR_TYPE_FLOW_TIMEOUT: FlowErrorType.FLOW_TIMEOUT,
        pb.FLOW_ERROR_TYPE_SUB_FLOW_START_FAILED: FlowErrorType.SUB_FLOW_START_FAILED,
    }
    try:
        status = statuses[result.flow_status]
        error_type = errors[result.error_type]
    except KeyError as error:
        raise ValueError("unsupported Flow result enum") from error
    return FlowResult(
        status,
        [StepCompletion(completion, decoder) for completion in result.results],
        error_type,
        result.error_message or None,
    )
