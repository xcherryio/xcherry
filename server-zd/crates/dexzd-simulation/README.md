# dexzd-simulation

## Purpose

Deterministic simulation tests that run the server-zd engine against simulated object storage, clocks, Workers, and crashes.

## Apply simulation

The first stage tests `dexzd-apply` without the engine. A seeded model of clients, the owner,
and pull Workers proposes commands. `ApplyChecker` applies each command twice on the committed
state and once on a re-decoded twin, requires identical outcomes, and then checks the state
invariants: lock holders and FIFO waiter order (L1, L3, L4), lost updates on guarded keys (L2),
fresh snapshots (S5), one result per attempt (S2), value holder sets and garbage rows (I1),
history event order, the timer index, and agreement of the Flow directory and run records.

```bash
cargo test -p dexzd-simulation                                   # fixed seed set
DEXZD_SIMULATION_SEEDS=500 DEXZD_SIMULATION_COMMANDS=1000 cargo test -p dexzd-simulation
DEXZD_SIMULATION_SEED=1592590336 cargo test -p dexzd-simulation  # reproduce one seed
```

A failing seed prints its seed, the failing command index, and a shrunk command sequence that
`replay_commands` reproduces.

## Engine simulation

The second stage runs the real engine: `EngineNode`, the Flow runtime registry, and the effect
runner, assembled as the server assembles them. Everything runs on one current-thread Tokio
runtime with paused time, and every choice derives from one seed.

- **Object storage.** One shared `InMemoryObjectStore` has S3 conditional-write semantics. Each
  node process sees it through its own seeded fault layer. The layer injects slow responses,
  HTTP 5xx and 503, 409 conflicts, timeouts, and lost responses. A failed or timed-out write may
  still take effect. A lost response hides only a write that took effect; a write that failed,
  such as a 412, returns its own result. Old-epoch processes produce real 412 races.
- **Processes.** A crash first cuts the process off from storage; writes already on the wire may
  still land. The successor's local store is kept, lost (restored from the newest snapshot), or
  rolled back to an earlier copy. Some successors take over while the old-epoch process still
  runs and writes, because their wall clock is ahead. Wall clocks also jump forward and back.
- **Workers and clients.** Simulated pull Workers sit behind the engine's dispatch ports. They
  succeed, fail, hang, run slowly, send heartbeats with and without values, return the same
  attempt's result again, disconnect, or are missing. Clients start, stop, write, publish,
  delete, invoke RPCs, and read, retrying unavailable calls with the same request ID.
- **Determinism.** The engine runs frame applies, state reads, and snapshot uploads on the
  calling task (`BlockingWorkPlacement::CallingTaskForTest`), and every engine `select!` is
  biased. Every ID the engine persists comes from one `SeededEngineIdSourceForTest` that all
  processes of a seed share, so the trace keeps engine IDs as minted.

### Invariants

A shadow `ApplyChecker` collects the canonical committed log from every process's applied
frames. It runs the stage-1 checks on every new command, so the lock, snapshot, value, and
directory invariants hold on the committed history. The engine simulation adds these checks:

1. **Acknowledged writes survive.** Every write a client saw succeed is in the canonical log,
   and every recovered store equals the shadow row for row.
2. **No speculative or stale reads.** Each GetAttributes and GetChannelMessages response, as a
   whole, equals the committed state of the Flow's newest run at a frame its node applied during
   the call. The shadow keeps each client Flow's state history by frame, and the call's window
   runs from the node's applied position when the call began to the one when it returned. A
   value that a later applied write overwrote fails, as does an uncommitted one.
3. **One result per attempt.** The shadow rejects a second applied result (S2). Any frame
   applied again after recovery must reproduce its first outcome exactly.
4. **Locks.** The shadow checks L1 to L4 after every command, and the recovered store passes
   `check_flow` again.
5. **Recovered runtime equals a never-crashed one.** The recovered store equals the shadow, so
   timers, method calls, outbox rows, and hard-delete rows match. After a crash at a quiet
   point, the successor dispatches every attempt that was running again. After a calm tail,
   every due timer has fired and no method call is unfinished.

The log itself is checked too. Copies of one log position must hold the same commands, and a
partition never applies a new frame below one it already applied.

The fixed seed set also asserts that these checks had something to check. It requires every
fault kind with each effect it can have on a call, such as a timed-out write that did or did
not take effect. It requires every restart mode and Worker behavior, and crashes during
recovery. It also requires 412 races on log and lease objects. An old-epoch window opens only
when a successor's lease write takes effect; the seed set requires log and lease writes that
old-epoch processes started, and frames they applied, inside such a window. Log cleanup
deletions, snapshot restores during starts, and rejected duplicate results are required too.
So are running attempts compared after the quiet crash, and live runs in the final state.

### Not covered yet

These section 13.1 items need later stages:

- Byte-identical snapshots across dispatches of one attempt, and Step input, locals, and
  condition results unchanged across attempts (section 8.1). The shadow checks only fresh
  snapshots (S5).
- Eventual progress of lock waiters beyond the calm-tail check (section 8.3).
- Time travel rebuilds and their hashes (section 8.4); clients do not call ResetFlow.
- Value lifecycle beyond I1: I3, liveness, reads within the window, and the G byte limit
  (section 10).
- Hard-delete objects in storage (section 10.3); clients write no hard-delete Attributes.
- Worker Stream writes, SubFlows and outbox delivery, and the Worker matching layer, which the
  simulated Workers bypass.
- More than one node ID, and foreign writers of the manifest and lease.
- The planted-bug acceptance list.

### Running

```bash
cargo test -p dexzd-simulation --test engine_simulation         # 64 seeds of 120 s, 1-2 minutes
DEXZD_SIMULATION_SEEDS=1000 DEXZD_SIMULATION_SECONDS=600 \
  cargo test -p dexzd-simulation --test engine_simulation          # longer budget
DEXZD_SIMULATION_SEED=7 cargo test -p dexzd-simulation --test engine_simulation -- --nocapture
```

`DEXZD_SIMULATION_SEEDS` and `DEXZD_SIMULATION_SEED` also select the apply simulation's seeds
when both tests run. A failing seed prints its reproduction command and the last 400 trace events.
`DEXZD_SIMULATION_PRINT_EVENTS=1` prints the trace tail of a passing seed. A self-test runs seeds
twice and requires identical raw traces, final local stores, and stored objects. Lease holder
IDs are the one exception: the cluster metadata crate mints them outside the engine.
