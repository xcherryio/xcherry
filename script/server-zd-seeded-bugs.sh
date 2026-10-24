#!/usr/bin/env bash
set -euo pipefail

repository_root="$(git rev-parse --show-toplevel)"
patch_directory="$repository_root/server-zd/testdata/seeded-bugs"
scratch_root="$(mktemp -d "${TMPDIR:-/tmp}/dexzd-seeded-bugs.XXXXXX")"
scratch_worktree="$scratch_root/worktree"
seed_count=64
commands_per_seed=400
time_limit_seconds=300

cleanup() {
  if [[ -d "$scratch_worktree/.git" || -f "$scratch_worktree/.git" ]]; then
    git -C "$repository_root" worktree remove --force "$scratch_worktree"
  fi
  cargo clean --manifest-path "$repository_root/server-zd/Cargo.toml" \
    -p dexzd-apply -p dexzd-simulation >/dev/null
  rm -rf "$scratch_root"
}
trap cleanup EXIT

run_simulation() {
  local source_directory="$1"
  local log_path="$2"
  cargo clean --manifest-path "$repository_root/server-zd/Cargo.toml" \
    -p dexzd-apply -p dexzd-simulation >/dev/null
  python3 - "$source_directory/server-zd" "$log_path" "$seed_count" "$commands_per_seed" "$time_limit_seconds" "$repository_root/server-zd/target" <<'PY'
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

crate_directory, log_path, seed_count, commands_per_seed, time_limit, target_directory = sys.argv[1:]
environment = os.environ.copy()
environment.update({
    "CARGO_BUILD_JOBS": "4",
    "CARGO_TARGET_DIR": target_directory,
    "DEXZD_SIMULATION_SEEDS": seed_count,
    "DEXZD_SIMULATION_COMMANDS": commands_per_seed,
})
command = [
    "cargo", "test", "--locked", "-p", "dexzd-simulation", "--test",
    "apply_invariants", "apply_keeps_every_invariant_over_random_command_sequences",
    "--", "--exact", "--nocapture",
]
with Path(log_path).open("w") as output:
    started = time.monotonic()
    process = subprocess.Popen(
        command,
        cwd=crate_directory,
        env=environment,
        stdout=output,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    try:
        status = process.wait(timeout=int(time_limit))
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait()
        print(f"Timed out after {time.monotonic() - started:.1f}s")
        sys.exit(124)
print(f"Completed in {time.monotonic() - started:.1f}s with exit status {status}")
sys.exit(status)
PY
}

baseline_log="$scratch_root/baseline.log"
echo "Checking unpatched simulation: $seed_count seeds, $commands_per_seed commands per seed"
if ! run_simulation "$repository_root" "$baseline_log"; then
  echo "Unpatched simulation failed or exceeded ${time_limit_seconds}s:" >&2
  tail -n 35 "$baseline_log" >&2
  exit 1
fi

detected_count=0
gap_count=0
for name in wall-clock hash-map-iteration immediate-reclaim retry-stale-snapshot; do
  patch_path="$patch_directory/$name.patch"
  git -C "$repository_root" worktree add --detach "$scratch_worktree" HEAD >/dev/null
  if ! git -C "$scratch_worktree" apply --check "$patch_path"; then
    echo "Patch does not apply: $patch_path" >&2
    exit 1
  fi
  git -C "$scratch_worktree" apply "$patch_path"
  log_path="$scratch_root/$name.log"
  echo "Checking $name: $seed_count seeds, $commands_per_seed commands per seed"
  if run_simulation "$scratch_worktree" "$log_path"; then
    echo "UNDETECTED: $name passed the simulation" >&2
    gap_count=$((gap_count + 1))
  else
    run_status=$?
    if [[ "$run_status" -eq 124 ]]; then
      echo "The $name run exceeded ${time_limit_seconds}s" >&2
      gap_count=$((gap_count + 1))
    elif ! grep -Eq 'seed [0-9]+ broke an invariant at command index [0-9]+' "$log_path"; then
      echo "The $name run failed without a simulation invariant violation:" >&2
      tail -n 35 "$log_path" >&2
      gap_count=$((gap_count + 1))
    else
      grep -m 1 -E 'seed [0-9]+ broke an invariant at command index [0-9]+' "$log_path"
      detected_count=$((detected_count + 1))
    fi
  fi
  git -C "$repository_root" worktree remove --force "$scratch_worktree"
done

echo "Detected $detected_count seeded bugs; $gap_count gaps; unpatched simulation passed."
[[ "$gap_count" -eq 0 ]]
