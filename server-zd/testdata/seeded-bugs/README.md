# Seeded bug acceptance

Each patch introduces one deliberate defect in a scratch Git worktree. The
baseline and mutants use the same committed HEAD and a scratch Cargo target.
The caller's source and build tree stay unchanged. Run from the repository root:

```bash
script/server-zd-seeded-bugs.sh
```

The runner first requires the unpatched apply simulation to pass. It then applies
each active patch alone and requires its named invariant to fail. A compile
failure, timeout, or passing mutant fails acceptance. It reports every gap before
exiting nonzero.

The active budget is 64 seeds starting at `1592590336` (`0x5eed0000`), with 400
commands per seed. Each baseline or mutant run has a 300-second limit, including
compilation and shrinking. The same budget applies to every active patch.

| Patch | Deliberate defect | Expected detector | Status |
| --- | --- | --- | --- |
| `wall-clock.patch` | Apply uses the process wall clock instead of the command timestamp. | `A2-apply-twice` compares byte-identical writes. | Caught at seed `1592590336`, command 0. |
| `hash-map-iteration.patch` | Attempt snapshots emit Attributes in `HashMap` iteration order. | `A2-apply-twice` or `A2-apply-redecoded` compares byte-identical writes. | Caught at seed `1592590336`, command 17. |
| `immediate-reclaim.patch` | After the last slot holder leaves, R6 records that death point while an attempt snapshot still pins the value. It deletes the value when the final pin leaves after that death point reaches window floor W; otherwise it queues G at the old death point. | `I1-window-value-reference` must catch a retained point whose in-flight snapshot needs the deleted value. | **Gap:** the faithful mutant passed PR #839's 64 × 400 apply simulation; the runner exits nonzero. The targeted `snapshot_pin_survives_window_advance_and_collects_after_attempt_completion` integration test fails on the mutant and passes without it. |
| `retry-stale-snapshot.patch` | A retry reconstructs its prior Running snapshot and selects it when the next attempt starts; table-value pins move to the new attempt. | `S5-fresh-snapshot` must catch stale data after an intervening write. | Caught at seed `1592590336`, command 77, after a Channel message deletion changed the selected Channel info. |

The immediate-reclaim patch is the original R6 failure mode. The apply model
needs an attempt snapshot pinned across a slot overwrite and window advance,
followed by attempt completion and time travel to an in-flight point. Do not
weaken the mutant to force detection.

These patches are **pending** until the engine simulation can check them. The
apply-only runner does not apply them. Set an engine-specific seed and time
budget when the engine simulation reaches main.

| Patch | Deliberate defect | Expected engine invariant |
| --- | --- | --- |
| `ack-before-persist.patch` | A client write returns its speculative reply immediately after proposal, before the object-store log write is durable. | An acknowledged command must survive a crash and restart. |
| `skip-epoch-check.patch` | Closed-stream recovery replays batches from an older epoch after a new owner starts. | Recovered state must exclude commands voided by the epoch transition. |
| `speculative-read-leak.patch` | GetAttributes reads the speculative overlay. | A client read must see only committed state, including before and after a failed commit. |

The nightly workflow does not exist in this checkout. When
`.github/workflows/server-zd-simulation-nightly.yml` is added, its seeded-bug
acceptance step should run `script/server-zd-seeded-bugs.sh`. That step will fail
until the immediate-reclaim gap is fixed; keep this failure visible.
