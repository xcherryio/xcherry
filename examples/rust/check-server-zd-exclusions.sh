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

# Verifies that make filters exactly the server-zd exclusions and prints each reason.
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$script_dir"

fail() {
  echo "check-server-zd-exclusions: $1" >&2
  exit 1
}

list_integration_tests() {
  DEX_INTEG_ENGINE="$1" make -s integTests flowSmokeTests TEST_RUNNER_ARGS=--list
}

extract_test_names() {
  sed -nE 's/^([A-Za-z0-9_]+): test$/\1/p' | sort
}

temporal_output=$(list_integration_tests temporal)
zd_output=$(list_integration_tests zd)
all_tests=$(extract_test_names <<<"$temporal_output")
zd_tests=$(extract_test_names <<<"$zd_output")
excluded_tests=$(
  sed -nE 's/^Excluded test ([A-Za-z0-9_]+): server-zd does not support .+$/\1/p' \
    <<<"$zd_output" | sort
)

[[ -n "$all_tests" ]] || fail "no integration tests were listed"
[[ -n "$excluded_tests" ]] || fail "DEX_INTEG_ENGINE=zd printed no exclusions"
if grep -q "^Excluded test" <<<"$temporal_output"; then
  fail "DEX_INTEG_ENGINE=temporal must not exclude tests"
fi
while read -r test_name; do
  grep -qx "$test_name" <<<"$all_tests" || fail "excluded test $test_name does not exist"
  if grep -qx "$test_name" <<<"$zd_tests"; then
    fail "excluded test $test_name still runs with DEX_INTEG_ENGINE=zd"
  fi
done <<<"$excluded_tests"
expected_zd_tests=$(comm -23 <(echo "$all_tests") <(echo "$excluded_tests"))
if [[ "$zd_tests" != "$expected_zd_tests" ]]; then
  fail "DEX_INTEG_ENGINE=zd must run every test that is not excluded"
fi

echo "server-zd exclusions: $(wc -l <<<"$excluded_tests" | tr -d ' ') excluded," \
  "$(wc -l <<<"$zd_tests" | tr -d ' ') kept"
