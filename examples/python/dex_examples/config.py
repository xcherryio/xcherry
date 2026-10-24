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

import os
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

from dex import StartFlowOptions

from dex_examples.products.ai_agent.models import (
    DEFAULT_CONTEXT_TOKENS,
    DEFAULT_MESSAGE_RETENTION,
    DEFAULT_MODEL,
    DEFAULT_SYSTEM_PROMPT,
)
from dex_examples.shared.worker_mode import (
    WorkerMode,
    read_worker_mode_from_environment,
)

DEFAULT_TIMEOUT = timedelta(hours=1)


@dataclass(frozen=True)
class ExamplesConfig:
    server_address: str
    worker_bind_address: str
    worker_target: str | None
    http_address: str
    blob_cache_dir: Path
    worker_mode: WorkerMode
    agent_model: str = DEFAULT_MODEL
    agent_system_prompt: str = DEFAULT_SYSTEM_PROMPT
    agent_context_tokens: int = DEFAULT_CONTEXT_TOKENS
    agent_message_retention: int = DEFAULT_MESSAGE_RETENTION
    agent_mcp_config: Path | None = None

    @staticmethod
    def from_env() -> ExamplesConfig:
        return ExamplesConfig(
            server_address=os.environ.get(
                "DEX_FLOW_SERVICE_ADDRESS", "127.0.0.1:8801"
            ),
            worker_bind_address=os.environ.get(
                "DEX_WORKER_BIND_ADDRESS", "127.0.0.1:8803"
            ),
            worker_target=os.environ.get("DEX_WORKER_TARGET") or None,
            http_address=os.environ.get("DEX_EXAMPLES_HTTP_ADDRESS", "127.0.0.1:8080"),
            blob_cache_dir=Path(
                os.environ.get(
                    "DEX_BLOB_CACHE_DIR",
                    "/tmp/dex-examples-python-blob-cache",
                )
            ),
            worker_mode=read_worker_mode_from_environment(),
            agent_model=os.environ.get("DEX_AGENT_MODEL", DEFAULT_MODEL),
            agent_system_prompt=os.environ.get(
                "DEX_AGENT_SYSTEM_PROMPT",
                DEFAULT_SYSTEM_PROMPT,
            ),
            agent_context_tokens=int(
                os.environ.get("DEX_AGENT_CONTEXT_TOKENS", DEFAULT_CONTEXT_TOKENS)
            ),
            agent_message_retention=int(
                os.environ.get(
                    "DEX_AGENT_MESSAGE_RETENTION",
                    DEFAULT_MESSAGE_RETENTION,
                )
            ),
            agent_mcp_config=(
                Path(config_path)
                if (config_path := os.environ.get("DEX_AGENT_MCP_CONFIG"))
                else None
            ),
        )


def start_options() -> StartFlowOptions:
    return StartFlowOptions(timeout=DEFAULT_TIMEOUT)
