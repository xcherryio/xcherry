# Copyright (c) 2026 Super Durable, Inc.
#
# Licensed under the Sustainable Use License 1.0.
# You may not use this file except in compliance with the License.
# See the LICENSE file in the repository root.
#
# SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version
from typing import Iterable

import grpc

from dex.dexpb import dex_pb2 as pb

MINIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION = 1
MAXIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION = 3
HARD_DELETE_ATTRIBUTE_PROTOCOL_VERSION = 3


def sdk_version() -> str:
    try:
        return version("dex-python-sdk")
    except PackageNotFoundError:
        return "dev"


def negotiate_server_protocol(server_info: pb.ServerInfo, artifact_version: str) -> int:
    server_minimum = server_info.minimum_supported_protocol_version
    server_current = server_info.current_protocol_version
    if (
        MINIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION == 0
        or MAXIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION == 0
        or MINIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION
        > MAXIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION
    ):
        raise _compatibility_error(
            artifact_version,
            server_info.server_version,
            server_minimum,
            server_current,
            "Python SDK protocol interval is invalid",
        )
    if server_minimum == 0 or server_current == 0 or server_minimum > server_current:
        raise _compatibility_error(
            artifact_version,
            server_info.server_version,
            server_minimum,
            server_current,
            "Server protocol interval is invalid",
        )
    negotiated = min(server_current, MAXIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION)
    if (
        negotiated < server_minimum
        or negotiated < MINIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION
    ):
        raise _compatibility_error(
            artifact_version,
            server_info.server_version,
            server_minimum,
            server_current,
            "protocol intervals do not overlap",
        )
    return negotiated


class NegotiatedServerProtocol:
    def __init__(self) -> None:
        self._version = 0

    @property
    def version(self) -> int:
        return self._version

    def record_version(self, version: int) -> None:
        self._version = version

    def require_hard_delete_attribute_support(
        self,
        writes: Iterable[pb.AttributeWrite],
    ) -> None:
        attribute_names = [
            write.key for write in writes if write.is_hard_delete_on_null
        ]
        if (
            not attribute_names
            or self._version >= HARD_DELETE_ATTRIBUTE_PROTOCOL_VERSION
        ):
            return
        raise RuntimeError(
            "hard-delete Attribute writes require Dex Server protocol version "
            f"{HARD_DELETE_ATTRIBUTE_PROTOCOL_VERSION} or later, but the negotiated "
            f"protocol version is {self._version}: {', '.join(attribute_names)}"
        )


def server_info_request_error(
    artifact_version: str, failure: grpc.RpcError
) -> RuntimeError:
    return RuntimeError(
        f'Python SDK version "{artifact_version}" protocol '
        f"[{MINIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION},"
        f"{MAXIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION}] is incompatible "
        'with Server version "unknown" protocol [unknown,unknown]: '
        f"GetServerInfo failed: {failure}"
    )


def _compatibility_error(
    artifact_version: str,
    server_version: str,
    server_minimum: int,
    server_current: int,
    reason: str,
) -> RuntimeError:
    displayed_server_version = server_version or "unknown"
    return RuntimeError(
        f'Python SDK version "{artifact_version}" protocol '
        f"[{MINIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION},"
        f"{MAXIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION}] is incompatible with "
        f'Server version "{displayed_server_version}" protocol '
        f"[{server_minimum},{server_current}]: {reason}"
    )
