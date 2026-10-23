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

"""Runs the sync Client/Worker showcase behind one Flask app."""

from __future__ import annotations

import sys
from pathlib import Path

# Allow `uv run python sync-python/main.py` from examples/python.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from sync_examples.app import SyncExampleApp
from sync_examples.config import SyncExamplesConfig
from sync_examples.http_app import create_app


def main() -> None:
    config = SyncExamplesConfig.from_env()
    app_state = SyncExampleApp(config)
    app_state.start_worker()
    try:
        host, port = parse_http_address(config.http_address)
        worker_description = (
            "pull Worker"
            if config.worker_mode.is_pull
            else f"Worker {app_state.worker.worker_target.address}"
        )
        print(
            f"Dex Python sync examples listening on http://{config.http_address} "
            f"({worker_description})"
        )
        create_app(app_state).run(host=host, port=port, threaded=True)
    finally:
        app_state.close()


def parse_http_address(address: str) -> tuple[str, int]:
    host, _, port_text = address.rpartition(":")
    if not _ or not port_text:
        raise ValueError(f"invalid HTTP address: {address}")
    return host or "127.0.0.1", int(port_text)


if __name__ == "__main__":
    main()
