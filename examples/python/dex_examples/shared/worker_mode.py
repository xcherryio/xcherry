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

"""Selects whether example Workers listen for push tasks or pull from Dex."""

from __future__ import annotations

import os
from enum import Enum

from dex import PullWorkerOptions

WORKER_MODE_ENVIRONMENT_VARIABLE = "DEX_EXAMPLES_WORKER_MODE"
PULL_REGISTRATION_TIMEOUT_SECONDS = 30.0


class WorkerMode(Enum):
    """How an example Worker receives tasks from Dex Server."""

    PUSH = "push"
    PULL = "pull"

    @property
    def is_pull(self) -> bool:
        return self is WorkerMode.PULL


def read_worker_mode_from_environment() -> WorkerMode:
    """Returns the mode in DEX_EXAMPLES_WORKER_MODE; empty selects push.

    Raises:
        ValueError: The variable holds a value other than push or pull.
    """
    value = os.environ.get(WORKER_MODE_ENVIRONMENT_VARIABLE, "")
    if value == "":
        return WorkerMode.PUSH
    try:
        return WorkerMode(value)
    except ValueError:
        raise ValueError(
            f"{WORKER_MODE_ENVIRONMENT_VARIABLE} must be push or pull, got {value!r}"
        ) from None


def new_example_pull_worker_options() -> PullWorkerOptions:
    """Returns pull options sized for every sample Flow sharing one Worker."""
    return PullWorkerOptions(max_concurrent_tasks=64)
