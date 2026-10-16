# Copyright (c) 2026 Super Durable, Inc.
#
# Licensed under the Sustainable Use License 1.0.
# You may not use this file except in compliance with the License.
# See the LICENSE file in the repository root.
#
# SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

"""Bearer credentials for Dex service calls."""

from __future__ import annotations

import copy
import os
import threading
import time
from pathlib import Path
from typing import Awaitable, Protocol


class Credential(Protocol):
    """Supply a bearer token for each Client or Worker service call.

    token may be called concurrently. AsyncClient and AsyncWorker accept an
    awaitable token; synchronous callers require a string. Synchronous tokens
    run on the calling thread for sync APIs and in an executor for async APIs.
    Async calls bound token lookup by the RPC deadline when one is set. Pull
    stream opens use a 30-second lookup budget. Sync calls do not bound token
    lookup separately, so synchronous implementations must return promptly.
    A raised exception fails the call before it contacts Dex. The token travels
    over Dex's plaintext gRPC connection; protect the network path.

    Example:
        >>> class FixedCredential:
        ...     def token(self) -> str:
        ...         return "my-token"
        >>> credential = FixedCredential()
        >>> options = ClientOptions(credential=credential)
    """

    def token(self) -> str | Awaitable[str]:
        """Return the current bearer token, or raise when unavailable.

        Returns:
            Token without the Bearer prefix, or an awaitable for async callers.

        Raises:
            Exception: Any failure prevents the service call from reaching Dex.
        """
        ...


class FileCredential:
    """Read a bearer token from a file and pick up rotations automatically.

    The file is read during construction. Its mtime and size are checked at most
    once per second, and changed contents are reloaded before the next call.
    Surrounding whitespace is removed. A file that becomes empty or unreadable
    fails subsequent calls until restored. Concurrent calls share a lock.
    Async callers check the file in an executor, outside the event loop.

    Example:
        >>> credential = FileCredential("/var/run/secrets/dex/token")
        >>> client = Client(registry, cache, ClientOptions(credential=credential))

    Args:
        path: Path to the token file.

    Raises:
        OSError: The file cannot be read during construction.
        ValueError: The file has no token after trimming whitespace.
        UnicodeError: The file does not contain valid UTF-8 text.
    """

    def __init__(self, path: str | os.PathLike[str]) -> None:
        """Read the initial token from path and fail if it is unavailable or empty.

        Args:
            path: File containing the bearer token.

        Raises:
            OSError: The file cannot be read.
            ValueError: The file has no token after trimming whitespace.
            UnicodeError: The file does not contain valid UTF-8 text.
        """
        self._path = Path(path)
        self._lock = threading.Lock()
        self._clock = time.monotonic
        self._token = ""
        self._mtime_ns = -1
        self._size = -1
        self._checked_at = 0.0
        self._error: OSError | UnicodeError | ValueError | None = None
        with self._lock:
            self._refresh()
            if self._error is not None:
                raise copy.copy(self._error)

    def token(self) -> str:
        """Return the current trimmed token, checking for file changes once per second.

        Returns:
            The token without the Bearer prefix.

        Raises:
            OSError: The file is no longer readable.
            ValueError: The file is empty after trimming whitespace.
            UnicodeError: The file does not contain valid UTF-8 text.
        """
        with self._lock:
            if self._clock() - self._checked_at >= 1.0:
                self._refresh()
            if self._error is not None:
                raise copy.copy(self._error)
            return self._token

    def _refresh(self) -> None:
        self._checked_at = self._clock()
        try:
            stat = self._path.stat()
            if (
                self._error is None
                and self._token
                and stat.st_mtime_ns == self._mtime_ns
                and stat.st_size == self._size
            ):
                return
            token = self._path.read_text(encoding="utf-8").strip()
            if not token:
                raise ValueError(f"credential file {self._path} is empty")
        except (OSError, UnicodeError, ValueError) as error:
            self._token = ""
            self._error = copy.copy(error)
            return
        self._token = token
        self._mtime_ns = stat.st_mtime_ns
        self._size = stat.st_size
        self._error = None
