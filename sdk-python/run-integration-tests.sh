#!/bin/bash

# Copyright (c) 2026 Super Durable, Inc.
#
# Licensed under the Sustainable Use License 1.0.
# You may not use this file except in compliance with the License.
# See the LICENSE file in the repository root.
#
# SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

set -euo pipefail

export DEX_INTEG_ENGINE="${DEX_INTEG_ENGINE:-temporal}"
case "$DEX_INTEG_ENGINE" in
  temporal|zd) ;;
  *) echo "DEX_INTEG_ENGINE must be temporal or zd" >&2; exit 2 ;;
esac
default_worker_mode=push
if [[ "$DEX_INTEG_ENGINE" == zd ]]; then
  default_worker_mode=pull
fi
worker_mode="${DEX_INTEG_WORKER_MODE:-$default_worker_mode}"
case "$worker_mode" in
  push|pull) ;;
  *) echo "DEX_INTEG_WORKER_MODE must be push or pull" >&2; exit 2 ;;
esac
if [[ "$DEX_INTEG_ENGINE" == zd && "$worker_mode" == push ]]; then
  echo "DEX_INTEG_WORKER_MODE=push is not supported with DEX_INTEG_ENGINE=zd" >&2
  exit 2
fi

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
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
test_paths=("$@")
if [[ "${#test_paths[@]}" -eq 0 ]]; then
  test_paths=("$script_dir"/tests/integ/test_*_runtime.py)
fi

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
dev_args=(
  -bind-address 127.0.0.1
  -dex-port "$dex_port"
  -web-port "$web_port"
  -open=false
)
if [[ "$DEX_INTEG_ENGINE" == zd ]]; then
  if [[ -z "${DEX_SERVER_ZD_BINARY:-}" ]]; then
    (cd "$repo_root" && cargo build --release --locked --manifest-path server-zd/Cargo.toml -p dexzd-server --bin dex-server-zd)
    DEX_SERVER_ZD_BINARY="$repo_root/server-zd/target/release/dex-server-zd"
  fi
  dev_args+=(-engine zd -zd-binary "$DEX_SERVER_ZD_BINARY" -zd-data-dir "$test_dir/zd")
else
  dev_args+=(-sqlite-db-filename "$test_dir/temporal.db")
fi

"$binary_dir/dexcli" dev "${dev_args[@]}" >>"$log_file" 2>&1 &
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
  DEX_INTEG_WORKER_MODE="$worker_mode" \
  uv run --frozen pytest "${pytest_args[@]}" "${test_paths[@]}"
