#!/bin/bash

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

set -euo pipefail

coverage=false
if [[ "${1:-}" == "--coverage" ]]; then
  coverage=true
  shift
fi
if [[ "$#" -ne 0 ]]; then
  echo "usage: $0 [--coverage]" >&2
  exit 2
fi

worker_mode="${DEX_INTEG_WORKER_MODE:-push}"
if [[ "$worker_mode" != push && "$worker_mode" != pull ]]; then
  echo "DEX_INTEG_WORKER_MODE must be push or pull" >&2
  exit 2
fi

available_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$script_dir"

dex_port="${DEX_INTEG_DEX_PORT:-$(available_port)}"
web_port="${DEX_INTEG_WEB_PORT:-$(available_port)}"
worker_port="${DEX_INTEG_WORKER_PORT:-$(available_port)}"
dex_address="127.0.0.1:${dex_port}"
log_file="/tmp/test-go-sdk-phase5-e2e-services.log"
test_dir=$(mktemp -d)
dexcli_pid=""
cover_dir=""
coverpkg="./dex/..."
: >"$log_file"

cleanup() {
  status=$?
  if [[ -n "$dexcli_pid" ]] && kill -0 "$dexcli_pid" 2>/dev/null; then
    kill -TERM "$dexcli_pid"
    wait "$dexcli_pid" || true
  fi
  if [[ "$status" -ne 0 ]]; then
    cat "$log_file" >&2
  fi
  rm -r "$test_dir"
  if [[ -n "$cover_dir" ]]; then
    rm -rf "$cover_dir"
  fi
}
trap cleanup EXIT

run_go_test() {
  if $coverage; then
    go test \
      -covermode=atomic \
      -coverpkg="$coverpkg" \
      "$@" \
      -args \
      -test.gocoverdir="$cover_dir"
  else
    go test "$@"
  fi
}

if $coverage; then
  rm -rf coverage
  cover_dir=$(mktemp -d "$script_dir/coverage-raw.XXXXXX")
  run_go_test -count=1 -race -v ./dex -run '^TestClient'
  run_go_test -count=1 -race -v ./dex -run '^TestWorker'
fi

make -C ../cli build
mkdir -p "$test_dir/definitions"
for flow_package in webv2approval webv2reply; do
  ../cli/dexcli visualize "integ/$flow_package/workflow.go" \
    --schema-version 2.0 \
    --json \
    --out "$test_dir/definitions/$flow_package"
done
../cli/dexcli dev \
  -bind-address 127.0.0.1 \
  -dex-port "$dex_port" \
  -web-port "$web_port" \
  -open=false \
  -flow-rendering-dir "$test_dir/definitions" \
  -sqlite-db-filename "$test_dir/temporal.db" \
  >>"$log_file" 2>&1 &
dexcli_pid=$!

dex_ready=false
for _ in {1..240}; do
  if grep -q "Dex development environment is ready" "$log_file"; then
    dex_ready=true
    break
  fi
  if ! kill -0 "$dexcli_pid" 2>/dev/null; then
    echo "dexcli exited before Dex became ready" >&2
    exit 1
  fi
  sleep 0.25
done
if ! $dex_ready; then
  echo "Dex did not become ready" >&2
  exit 1
fi

DEX_FLOW_SERVICE_ADDRESS="$dex_address" \
DEX_WEB_ADDRESS="127.0.0.1:${web_port}" \
DEX_WORKER_HOST=127.0.0.1 \
DEX_WORKER_PORT="$worker_port" \
DEX_INTEG_WORKER_MODE="$worker_mode" \
  run_go_test -count=1 -race -v ./integ -run "${DEX_GO_INTEG_TEST_PATTERN:-.}"

if $coverage; then
  mkdir -p coverage
  go tool covdata textfmt -i="$cover_dir" -o=coverage/coverage.out
  go tool cover -func=coverage/coverage.out | tee coverage/coverage.txt
  go tool cover -html=coverage/coverage.out -o=coverage/index.html
fi
