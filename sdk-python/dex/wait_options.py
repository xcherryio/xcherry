# Copyright (c) 2026 Super Durable, Inc.
#
# Licensed under the Sustainable Use License 1.0.
# You may not use this file except in compliance with the License.
# See the LICENSE file in the repository root.
#
# SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from math import ceil
from time import monotonic

import grpc

from dex.runtime_errors import ErrorSubStatus, RequestTimeoutError


@dataclass(frozen=True)
class WaitForStepCompletionOptions:
    """Configure one durable Step completion wait.

    The server derives a stable Request ID from the Step execution when
    ``request_id`` is empty. Reuse an override only for the same logical wait.
    ``request_timeout`` bounds the entire SDK call across transparent transport
    reattachments. Temporal permits 10 in-flight Updates per Workflow Execution.
    ``internal_handler_timeout`` reclaims accepted waits that outlive callers and
    could consume those slots. Active callers transparently start another
    generation, which adds another Update to history.

    Attributes:
        request_id: An optional override for the server-derived stable ID.
        request_timeout: The total SDK call budget. Zero waits indefinitely.
        internal_handler_timeout: The Temporal Update handler generation lifetime.
            Set it only when abandoned waits can approach Temporal's in-flight
            Update limit. Zero disables rollover. Prefer a value longer than normal
            request timeouts and reconnect gaps because every generation counts
            toward Temporal's 2,000-Update history limit.
    """

    request_id: str = ""
    request_timeout: timedelta = timedelta(0)
    internal_handler_timeout: timedelta = timedelta(0)


@dataclass(frozen=True)
class WaitForAttributeOptions:
    """Configure one durable Attribute match wait.

    The server derives a stable Request ID from the Attribute condition when
    ``request_id`` is empty. Reuse an override only for the same logical predicate.
    ``request_timeout`` bounds the entire SDK call across transparent transport
    reattachments. Temporal permits 10 in-flight Updates per Workflow Execution.
    ``internal_handler_timeout`` reclaims accepted waits that outlive callers and
    could consume those slots. Active callers transparently start another
    generation, which adds another Update to history.

    Attributes:
        request_id: An optional override for the server-derived stable ID.
        request_timeout: The total SDK call budget. Zero waits indefinitely.
        internal_handler_timeout: The Temporal Update handler generation lifetime.
            Set it only when abandoned waits can approach Temporal's in-flight
            Update limit. Zero disables rollover. Prefer a value longer than normal
            request timeouts and reconnect gaps because every generation counts
            toward Temporal's 2,000-Update history limit.
    """

    request_id: str = ""
    request_timeout: timedelta = timedelta(0)
    internal_handler_timeout: timedelta = timedelta(0)


@dataclass(frozen=True)
class _ClientRequestAttempt:
    request_timeout_seconds: int
    transport_timeout_seconds: float | None


class _ClientRequestBudget:
    def __init__(self, request_timeout: timedelta) -> None:
        seconds = _duration_seconds32(request_timeout)
        self._deadline = monotonic() + seconds if seconds else None

    def next_attempt(self, operation: str, flow_id: str) -> _ClientRequestAttempt:
        if self._deadline is None:
            return _ClientRequestAttempt(0, None)
        remaining = self._deadline - monotonic()
        if remaining <= 0:
            raise self.timeout_error(operation, flow_id)
        return _ClientRequestAttempt(ceil(remaining), remaining)

    def has_expired(self) -> bool:
        return self._deadline is not None and monotonic() >= self._deadline

    def is_timeout(self, code: grpc.StatusCode | None) -> bool:
        # The server can reset the stream with CANCEL as the shared deadline passes.
        return (
            code in (grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.CANCELLED)
            and self.has_expired()
        )

    @staticmethod
    def timeout_error(operation: str, flow_id: str) -> RequestTimeoutError:
        return RequestTimeoutError(
            grpc.StatusCode.DEADLINE_EXCEEDED,
            ErrorSubStatus.REQUEST_TIMEOUT,
            "request timed out",
            operation,
            flow_id,
        )


def _duration_seconds32(duration: timedelta) -> int:
    seconds = duration.total_seconds()
    if seconds < 0 or not seconds.is_integer() or seconds > 2_147_483_647:
        raise ValueError("duration must be whole seconds within int32")
    return int(seconds)
