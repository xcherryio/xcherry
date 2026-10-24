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

A failing seed prints its seed, the failing command index, and its command sequence immediately.
After shrinking, it prints the shortest sequence found that `replay_commands` reproduces.

The fixed test runs 64 seeds of 400 commands, plus every case in `regression-seeds.txt`.
Each line in that file contains a decimal seed and the command count needed to reach its failure.
The checked-in seed also runs for 1,000 commands. Nightly runs set `DEXZD_SIMULATION_BASE_SEED`
to a printed random starting seed, then use `DEXZD_SIMULATION_SEEDS` consecutive seeds and
`DEXZD_SIMULATION_COMMANDS` commands per seed. The single-seed override runs only that seed.
Manual runs accept 1–1,000 random seeds and 1–2,000 commands per seed, with at most 500,000
random commands total. The fixed regression seeds run in addition to that random budget.

When a nightly run fails, download its failure artifact. The first failing seed gets a report
immediately, before shrinking starts. Shrinking is limited to 200 replays or 30 seconds, and the
completed report replaces the initial sequence with the shortest one found. The nightly test
stops after that failure, has a 50-minute command limit, and uploads the log and reports even if
the command fails. Reproduce the failure from `server-zd/` with
`DEXZD_SIMULATION_SEED=<seed> DEXZD_SIMULATION_COMMANDS=<failing index + 1> cargo test -p dexzd-simulation`.
Add `<seed> <failing index + 1>` as a line in `regression-seeds.txt`, then run the fixed test.
The command count keeps failures beyond the default 400 commands in every PR run.
