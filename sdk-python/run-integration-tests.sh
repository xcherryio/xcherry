#!/bin/bash

# Copyright (c) 2026 Super Durable, Inc.
#
# Licensed under the Sustainable Use License 1.0.
# You may not use this file except in compliance with the License.
# See the LICENSE file in the repository root.
#
# SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

set -euo pipefail

pytest_args=(-q)
if [[ "${1:-}" == "--coverage" ]]; then
  pytest_args+=(
    --cov=dex
    --cov-branch
    --cov-config=pyproject.toml
    --cov-report=term-missing
    --cov-report=html:coverage/html
    --cov-report=xml:coverage/coverage.xml
    --cov-report=lcov:coverage/lcov.info
  )
  shift
fi
test_paths=("$@")
if [[ "${#test_paths[@]}" -eq 0 ]]; then
  test_paths=(tests/integ/test_*_runtime.py)
fi

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "$script_dir/.." && pwd)
dex_port="${DEX_INTEG_DEX_PORT:-18802}"
web_port="${DEX_INTEG_WEB_PORT:-18902}"
dex_address="127.0.0.1:${dex_port}"
log_file="/tmp/test-python-sdk-integration-services.log"
test_dir=$(mktemp -d)
binary_dir=$(mktemp -d)
dexcli_pid=""
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
  rm -r "$test_dir" "$binary_dir"
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
DEX_SERVER_ADDRESS="$dex_address" \
  uv run --frozen pytest "${pytest_args[@]}" "${test_paths[@]}"
