#!/bin/bash

# Copyright (c) 2026 Super Durable, Inc.
#
# Licensed under the Sustainable Use License 1.0.
# You may not use this file except in compliance with the License.
# See the LICENSE file in the repository root.
#
# SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

set -euo pipefail

export DEX_INTEG_WORKER_MODE="${DEX_INTEG_WORKER_MODE:-push}"
if [[ "$DEX_INTEG_WORKER_MODE" != "push" && "$DEX_INTEG_WORKER_MODE" != "pull" ]]; then
  echo "DEX_INTEG_WORKER_MODE must be push or pull" >&2
  exit 2
fi

test_script="test:integration"
pull_only=false
if [[ "${1:-}" == "--coverage" ]]; then
  test_script="test:integration:coverage"
  shift
fi
if [[ "${1:-}" == "--pull" ]]; then
  pull_only=true
  shift
fi
if [[ "$#" -ne 0 ]]; then
  echo "usage: $0 [--coverage] [--pull]" >&2
  exit 2
fi
if $pull_only && [[ "$test_script" == "test:integration:coverage" ]]; then
  echo "--coverage and --pull cannot be combined" >&2
  exit 2
fi

available_port() {
  node -e 'const net=require("node:net");const server=net.createServer();server.listen(0,"127.0.0.1",()=>{console.log(server.address().port);server.close();});'
}

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "$script_dir/.." && pwd)
dex_port="${DEX_INTEG_DEX_PORT:-$(available_port)}"
web_port="${DEX_INTEG_WEB_PORT:-$(available_port)}"
dex_address="127.0.0.1:${dex_port}"
log_file="/tmp/test-typescript-sdk-integration-services.log"
test_dir=$(mktemp -d)
binary_dir=$(mktemp -d)
dexcli_pid=""
: >"$log_file"

cleanup() {
  status=$?
  if [[ -n "$dexcli_pid" ]] && kill -0 "$dexcli_pid" 2>/dev/null; then
    stop_process_tree "$dexcli_pid"
    wait "$dexcli_pid" || true
  fi
  if [[ "$status" -ne 0 ]]; then
    cat "$log_file" >&2
  fi
  rm -r "$test_dir" "$binary_dir"
}

stop_process_tree() {
  local parent_pid=$1
  local child_pid
  while read -r child_pid; do
    if [[ -n "$child_pid" ]]; then
      stop_process_tree "$child_pid"
    fi
  done < <(pgrep -P "$parent_pid" || true)
  kill -TERM "$parent_pid" 2>/dev/null || true
}
trap cleanup EXIT

if [[ ! -f "$repo_root/web/assets/dist/index.html" ]]; then
  (
    cd "$repo_root/web"
    npm ci
    npm run build
  )
fi

(
  cd "$repo_root/cli"
  GOWORK=off go build -trimpath -o "$binary_dir/dexcli" ./cmd/dexcli
)

"$binary_dir/dexcli" dev \
  -bind-address 127.0.0.1 \
  -dex-port "$dex_port" \
  -web-port "$web_port" \
  -open=false \
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

cd "$script_dir"
if $pull_only; then
  npm run build:native
  npm run build
  DEX_SERVER_ADDRESS="$dex_address" node --test dist/test/integ/pull-worker.integration.test.js
else
  DEX_SERVER_ADDRESS="$dex_address" npm run "$test_script"
fi
