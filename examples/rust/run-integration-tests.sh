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

keep_running=false
test_args=()
for arg in "$@"; do
  case "$arg" in
    --keep-running)
      keep_running=true
      ;;
    *)
      test_args+=("$arg")
      ;;
  esac
done

export DEX_INTEG_ENGINE="${DEX_INTEG_ENGINE:-temporal}"
if [[ "$DEX_INTEG_ENGINE" != temporal && "$DEX_INTEG_ENGINE" != zd ]]; then
  echo "DEX_INTEG_ENGINE must be temporal or zd" >&2
  exit 2
fi
default_worker_mode=push
if [[ "$DEX_INTEG_ENGINE" == zd ]]; then
  default_worker_mode=pull
fi
export DEX_EXAMPLES_WORKER_MODE="${DEX_EXAMPLES_WORKER_MODE:-$default_worker_mode}"
if [[ "$DEX_EXAMPLES_WORKER_MODE" != push && "$DEX_EXAMPLES_WORKER_MODE" != pull ]]; then
  echo "DEX_EXAMPLES_WORKER_MODE must be push or pull" >&2
  exit 2
fi
if [[ "$DEX_INTEG_ENGINE" == zd && "$DEX_EXAMPLES_WORKER_MODE" == push ]]; then
  echo "DEX_EXAMPLES_WORKER_MODE=push is not supported with DEX_INTEG_ENGINE=zd" >&2
  exit 2
fi

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "$script_dir/../.." && pwd)
if [[ -n "${DEX_SERVER_ZD_BINARY:-}" && "$DEX_SERVER_ZD_BINARY" == */* ]]; then
  DEX_SERVER_ZD_BINARY="$(cd "$(dirname "$DEX_SERVER_ZD_BINARY")" && pwd)/$(basename "$DEX_SERVER_ZD_BINARY")"
fi
entity_store_dir="$repo_root/examples/entity-store"
compose_project="dex-rust-examples-$$"
dex_port="${DEX_RUST_EXAMPLES_DEX_PORT:-19804}"
web_port="${DEX_RUST_EXAMPLES_WEB_PORT:-19904}"
dex_address="127.0.0.1:${dex_port}"
log_file="/tmp/test-rust-examples-integration-services.log"
test_dir=$(mktemp -d)
binary_dir=$(mktemp -d)
dexcli_pid=""
entity_store_started=false
: >"$log_file"

cleanup() {
  status=$?
  if $keep_running; then
    if [[ "$status" -ne 0 ]]; then
      cat "$log_file" >&2
    fi
    return
  fi
  if [[ -n "$dexcli_pid" ]] && kill -0 "$dexcli_pid" 2>/dev/null; then
    kill -TERM "$dexcli_pid"
    wait "$dexcli_pid" || true
  fi
  if $entity_store_started; then
    if ! docker compose -p "$compose_project" \
      -f "$entity_store_dir/docker-compose.yml" down --volumes >>"$log_file" 2>&1; then
      echo "failed to stop the Rust examples entity store" >&2
    fi
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

if [[ "$DEX_INTEG_ENGINE" == zd && -z "${DEX_SERVER_ZD_BINARY:-}" ]]; then
  (cd "$repo_root" && cargo build --release --locked --manifest-path server-zd/Cargo.toml -p dexzd-server --bin dex-server-zd)
  DEX_SERVER_ZD_BINARY="$repo_root/server-zd/target/release/dex-server-zd"
fi

# server-zd does not support Attribute Store sync, so zd runs skip the entity store.
if [[ "$DEX_INTEG_ENGINE" == zd ]]; then
  dev_args=(-engine zd -zd-binary "$DEX_SERVER_ZD_BINARY" -zd-data-dir "$test_dir/zd")
else
  docker compose -p "$compose_project" \
    -f "$entity_store_dir/docker-compose.yml" up --detach --wait
  entity_store_started=true
  dev_args=(
    -attribute-store-config "$entity_store_dir/attribute-store.yaml"
    -sqlite-db-filename "$test_dir/temporal.db"
  )
fi

"$binary_dir/dexcli" dev \
  "${dev_args[@]}" \
  -bind-address 127.0.0.1 \
  -dex-port "$dex_port" \
  -web-port "$web_port" \
  -open=false \
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

# The Makefile excludes the samples server-zd does not support when DEX_INTEG_ENGINE=zd.
cd "$script_dir"
export DEXCLI_PATH="$binary_dir/dexcli"
DEX_SERVER_ADDRESS="$dex_address" make integTests
DEX_SERVER_ADDRESS="$dex_address" make flowSmokeTests

if $keep_running; then
  echo ""
  echo "Dex Web:  http://127.0.0.1:${web_port}"
  echo "dexcli:   --server ${dex_address}"
  echo "Press Ctrl+C to stop dexcli dev"
  wait "$dexcli_pid"
fi
