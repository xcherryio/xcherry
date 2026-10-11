# Copyright (c) 2026 Super Durable, Inc.
#
# Licensed under the Sustainable Use License 1.0.
# You may not use this file except in compliance with the License.
# See the LICENSE file in the repository root.
#
# SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

from __future__ import annotations

from typing import Any

from dex._invocation_context import InvocationContext
from dex.condition import Condition, SubFlowCondition
from dex.context import Context
from dex.flow import Flow
from dex.flow_options import SubFlowOptions
from dex.flow_result import FlowResult


class SubFlow:
    """Create durable SubFlow Conditions and read their Execute results."""

    @staticmethod
    def run(
        flow: Flow[Any],
        input: object,
        options: SubFlowOptions | None = None,
    ) -> Condition:
        """Create a Condition that starts or reuses ``flow`` and awaits completion.

        The server starts the SubFlow after ``wait_for`` returns and retries the
        start within its configured retry budget. When the start still fails, the
        waiting Step's ``WaitForFailurePolicy`` decides the outcome. The default
        ``FAIL_FLOW`` fails the parent Flow. ``PROCEED`` runs ``execute``, and the
        SubFlow result reports ``FlowErrorType.SUB_FLOW_START_FAILED``.

        Args:
            flow: The exact target Flow instance registered by the Worker.
            input: Input accepted by the target Flow's starting Step.
            options: Optional reuse, timing, Attribute, configuration, and ID settings.

        Returns:
            A durable Condition accepted by :class:`dex.Wait`.
        """
        resolved = options or SubFlowOptions()
        return SubFlowCondition(
            condition_id=resolved.condition_id,
            flow=flow,
            input=input,
            options=resolved,
        )

    @staticmethod
    def get_condition_results(context: Context, index: int = 0) -> FlowResult:
        """Return one stable-indexed SubFlow result during Step ``execute``.

        A SubFlow that the server could not start returns ``FlowStatus.FAILED``
        with ``FlowErrorType.SUB_FLOW_START_FAILED`` and the start error as its
        error message. ``execute`` sees this result only when the Step uses
        ``WaitForFailurePolicy.PROCEED``. Check the error type to tell a start
        failure apart from a SubFlow that started and then failed::

            charge = dex.SubFlow.get_condition_results(context)
            if charge.error_type is dex.FlowErrorType.SUB_FLOW_START_FAILED:
                return dex.force_fail(f"SubFlow did not start: {charge.error_message}")

        Args:
            context: The current Dex Execute context.
            index: Zero-based SubFlow order within the surrounding Wait.

        Returns:
            A terminal result or an AnyOf loser's running snapshot.

        Raises:
            TypeError: If ``context`` is not managed by Dex.
            ValueError: If called outside Execute or ``index`` is unavailable.
        """
        return SubFlow._context(context).sub_flow_result(index)

    @staticmethod
    def get_flow_id(context: Context, index: int = 0) -> str:
        """Return the generated Flow ID for one SubFlow Condition.

        The ID remains usable after an AnyOf winner completes and may be passed to
        ``Client.stop_flow`` to stop a still-running loser.

        Args:
            context: The current Dex Execute context.
            index: Zero-based SubFlow order within the surrounding Wait.

        Returns:
            The stable server-generated SubFlow Flow ID.
        """
        return SubFlow._context(context).sub_flow_id(index)

    @staticmethod
    def _context(context: Context) -> InvocationContext:
        if not isinstance(context, InvocationContext):
            raise TypeError("SubFlow access requires a Dex invocation Context")
        return context
