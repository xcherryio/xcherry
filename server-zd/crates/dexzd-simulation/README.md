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
DEXZD_SIMULATION_STRICT=1 cargo test -p dexzd-simulation         # stop tolerating known apply defects
```

A failing seed prints its seed, the failing command index, and a shrunk command sequence that
`replay_commands` reproduces. `src/invariants/known_apply_defects.rs` lists the reported apply
defects that the default run tolerates; delete an entry when its fix lands.
