# testdata/v1

The baseline format of server-zd before its first release: key layout version 1, metadata
format version 1, behavior version 1, and snapshot format version 1.

## How it was produced

```bash
DEXZD_FORMAT_COMPAT_OUTPUT=$PWD/testdata/v1 \
  cargo test -p dexzd-engine --test format_compat generate_format_compat_testdata -- --ignored
```

The generator ran the real engine node with the deterministic applier, the in-memory object
store, and the in-memory local store. A scripted clock and identifiers derived from seed
`0x5eed_2026_1024` replace wall-clock time and random UUIDs, so a rerun writes the same bytes.
No effect runtime ran; the workload proposed every command itself, including timer firings,
cross-Flow deliveries, acknowledgments, and hard-delete cleanups.

One node, `node-a` with incarnation 1, served a cluster of four partitions. The cluster object
was written before the node started.

## Workload

Before the snapshot:

- `order-sync`: SYNC Steps with a Flow timeout. A WaitFor waits on a timer and a Channel; the
  timer fires. Execute heartbeats with a checkpoint, fails with a Worker error, and succeeds on
  the retry after its backoff timer fires. The success writes Attributes, records an event,
  publishes to a Channel, and moves to a Step that locks an Attribute. A Channel message is
  deleted, and a hard-delete Attribute value stays live to the end. One Attribute is longer
  than the inline threshold and lives in the Value column family.
- `profile-review`: a WaitFor on a Channel keeps the run open while a hard-delete value is
  replaced; the replaced object is deleted and the deletion committed.
- `fulfillment-parent`: a WaitFor starts a SubFlow. The child start is delivered to the child
  Flow in another partition, and the parent's outbox message is acknowledged.

Then the node took a snapshot. After the snapshot:

- The child completes, its completion wakes the parent, and both outbox messages are
  acknowledged. The parent completes gracefully.
- `order-sync`: a transactional RPC that locks an Attribute commits, then the locking Step
  runs and completes the Flow.
- `order-async`: ASYNC Steps whose events wait for the batch window. They share a log batch
  with a prompt Channel publication to `profile-review` in another partition. Step-execution
  locals are written.
- `profile-review`: the Channel message completes the wait. Execute clears the hard-delete
  value; the object is deleted and the deletion committed.
- `travel-flow`: two Steps and a client Attribute write complete the run. Time travel to the
  second Step's Execute, with writes reapplied, reopens it in run attempt 2, which completes.
- `idle-flow`: a timer wait is skipped, the Flow config is updated, and the Flow is canceled.
- `short-lived-flow`: a run with a 60-second retention completes; the retention timer fires
  and the run is purged.

Every `FlowCommand` kind of behavior version 1 appears in the log. Log cleanup did not run,
because no snapshot was older than the deletion delay, so the log keeps every batch.

## Contents

| Path | Content |
| --- | --- |
| `objects/cluster`, `objects/manifest` | Cluster object and manifest |
| `objects/registry/attribute-indexes` | Attribute index registry with one keyword index |
| `objects/log/node-a/0000000001/` | Every log batch, starting with the EpochStart at seq 1 |
| `objects/snapshot/node-a/<lineage>/` | Snapshot manifest 1 and its memory store file |
| `objects/hard-delete/` | The hard-delete value that is still live |
| `state.txt` | Committed state after the last command; see `../README.md` |
