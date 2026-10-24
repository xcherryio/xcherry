#!/bin/bash

# Copyright (c) 2022-2026 Super Durable, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

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
dex_port="${DEX_EXAMPLES_DEX_PORT:-19801}"
web_port="${DEX_EXAMPLES_WEB_PORT:-19901}"
deal_dsl_dex_port="${DEX_EXAMPLES_DEAL_DSL_DEX_PORT:-19803}"
deal_dsl_web_port="${DEX_EXAMPLES_DEAL_DSL_WEB_PORT:-19903}"
postgres_port="${DEX_EXAMPLES_POSTGRES_PORT:-19432}"
entity_store_postgres_port="${DEX_EXAMPLES_ENTITY_STORE_POSTGRES_PORT:-55432}"
default_dex_address="127.0.0.1:${dex_port}"
deal_dsl_dex_address="127.0.0.1:${deal_dsl_dex_port}"
postgres_url="postgres://deal_dsl:deal_dsl@127.0.0.1:${postgres_port}/deal_dsl?sslmode=disable"
compose_project="deal-dsl-e2e-$$"
entity_store_dir="$repo_root/examples/entity-store"
entity_store_project="entity-store-e2e-$$"
log_file="/tmp/test-go-examples-e2e-services.log"
test_dir=$(mktemp -d)
binary_dir=$(mktemp -d)
dexcli_pid=""
deal_dsl_dex_log="$test_dir/deal-dsl-dex.log"
entity_store_started=false
deal_dsl_started=false
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
  if $deal_dsl_started; then
    if ! DEAL_DSL_POSTGRES_PORT="$postgres_port" docker compose \
      -p "$compose_project" \
      -f "$script_dir/deal-dsl/docker-compose.yml" \
      down --volumes >>"$log_file" 2>&1; then
      echo "failed to stop the Deal DSL database" >&2
    fi
  fi
  if $entity_store_started; then
    if ! ENTITY_STORE_POSTGRES_PORT="$entity_store_postgres_port" docker compose -p "$entity_store_project" \
      -f "$entity_store_dir/docker-compose.yml" down --volumes >>"$log_file" 2>&1; then
      echo "failed to stop the Go examples entity store" >&2
    fi
  fi
  if [[ "$status" -ne 0 ]]; then
    cat "$log_file" >&2
    if [[ -f "$deal_dsl_dex_log" ]]; then
      cat "$deal_dsl_dex_log" >&2
    fi
  fi
  rm -r "$test_dir" "$binary_dir"
}
trap cleanup EXIT

# server-zd does not support Attribute Store sync, so zd runs skip the entity store.
if [[ "$DEX_INTEG_ENGINE" == temporal ]]; then
  ENTITY_STORE_POSTGRES_PORT="$entity_store_postgres_port" docker compose -p "$entity_store_project" \
    -f "$entity_store_dir/docker-compose.yml" up --detach --wait
  entity_store_started=true

  entity_store_config="$test_dir/entity-store.yaml"
  sed "s/localhost:55432/localhost:${entity_store_postgres_port}/" \
    "$entity_store_dir/attribute-store.yaml" >"$entity_store_config"
fi

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

if [[ "$DEX_INTEG_ENGINE" == zd ]]; then
  default_dev_args=(-engine zd -zd-binary "$DEX_SERVER_ZD_BINARY" -zd-data-dir "$test_dir/default-zd")
  deal_dsl_dev_args=(-engine zd -zd-binary "$DEX_SERVER_ZD_BINARY" -zd-data-dir "$test_dir/deal-dsl-zd")
else
  default_dev_args=(
    -attribute-store-config "$entity_store_config"
    -sqlite-db-filename "$test_dir/default-temporal.db"
  )
  deal_dsl_dev_args=(
    -attribute-store-config "$entity_store_config"
    -sqlite-db-filename "$test_dir/deal-dsl-temporal.db"
  )
fi

"$binary_dir/dexcli" dev \
  "${default_dev_args[@]}" \
  -bind-address 127.0.0.1 \
  -dex-port "$dex_port" \
  -web-port "$web_port" \
  -open=false \
  >>"$log_file" 2>&1 &
dexcli_pid=$!

export DEXCLI_PATH="$binary_dir/dexcli"

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
common_test_env=(
  DEX_FLOW_SERVICE_ADDRESS="$default_dex_address"
  DEX_WORKER_HOST=127.0.0.1
  GOCACHE="${GOCACHE:-$test_dir/gocache}"
  GOMODCACHE="${GOMODCACHE:-/tmp/dex-examples-gomodcache}"
  GOWORK=off
)
integ_status=0
env "${common_test_env[@]}" \
  go test -count=1 -race -v ./integ ${test_args[@]+"${test_args[@]}"} || integ_status=$?

if [[ -n "$dexcli_pid" ]] && kill -0 "$dexcli_pid" 2>/dev/null; then
  kill -TERM "$dexcli_pid"
  wait "$dexcli_pid" || true
fi
dexcli_pid=""

DEAL_DSL_POSTGRES_PORT="$postgres_port" docker compose \
  -p "$compose_project" \
  -f "$script_dir/deal-dsl/docker-compose.yml" \
  up -d --wait
deal_dsl_started=true

"$binary_dir/dexcli" dev \
  "${deal_dsl_dev_args[@]}" \
  -bind-address 127.0.0.1 \
  -dex-port "$deal_dsl_dex_port" \
  -web-port "$deal_dsl_web_port" \
  -open=false \
  >>"$deal_dsl_dex_log" 2>&1 &
dexcli_pid=$!

deal_dsl_dex_ready=false
for _ in {1..240}; do
  if grep -q "Dex development environment is ready" "$deal_dsl_dex_log"; then
    deal_dsl_dex_ready=true
    break
  fi
  if ! kill -0 "$dexcli_pid" 2>/dev/null; then
    echo "deal DSL dexcli exited before Dex became ready" >&2
    exit 1
  fi
  sleep 0.25
done
if ! $deal_dsl_dex_ready; then
  echo "Deal DSL Dex did not become ready" >&2
  exit 1
fi

deal_dsl_status=0
env \
DEX_FLOW_SERVICE_ADDRESS="$deal_dsl_dex_address" \
DEX_WORKER_HOST=127.0.0.1 \
GOCACHE="${GOCACHE:-/tmp/dex-examples-gocache}" \
GOMODCACHE="${GOMODCACHE:-/tmp/dex-examples-gomodcache}" \
GOWORK=off \
DEAL_DSL_POSTGRES_URL="$postgres_url" \
  go test -count=1 -race -v ./integ/dealdsl ${test_args[@]+"${test_args[@]}"} || deal_dsl_status=$?

if [[ "$integ_status" -ne 0 || "$deal_dsl_status" -ne 0 ]]; then
  exit 1
fi

if $keep_running; then
  echo ""
  echo "Dex Web:  http://127.0.0.1:${deal_dsl_web_port}"
  echo "dexcli:   --server ${deal_dsl_dex_address}"
  echo "Press Ctrl+C to stop dexcli dev"
  wait "$dexcli_pid"
fi
