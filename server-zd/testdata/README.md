# server-zd persistent format testdata

Each `v<N>` directory holds object storage contents and the committed state that one server-zd
version wrote. Every later version must decode them, restore them, and replay them to the same
state, as the Beta compatibility rule requires for persisted data.

Recorded versions are immutable. Never edit, regenerate, or delete a `v<N>` directory. Add the
next number instead.

## What the tests check

The `format_compat` test target in `crates/dexzd-engine/tests/format_compat/` runs in normal CI
against every `v<N>` directory:

- It decodes every recorded object with the current protobuf types. These are the cluster
  object, the manifest, the Attribute index registry, and every log batch with each `LogRecord`
  and `FlowCommand`. They also include each snapshot manifest, every row of the memory snapshot,
  and each hard-delete value. An object of an unknown kind fails the test.
- It requires every `FlowCommand` kind to appear in at least one version.
- It starts a node on the recorded objects. The node restores the snapshot and replays the log
  after it. The test then compares the committed state with `state.txt` row by row.
- It starts a second node that ignores the snapshot and replays the whole log into an empty
  store. The same comparison follows.
- It compares a digest of each version's data files with the pin in `golden_replay.rs`. Any
  changed byte fails. The README of a version is not part of the digest.
- It records the workload twice and requires identical bytes, so the generator stays usable.

A replay difference is a compatibility break. Fix the code, never the recorded data. A change
to the result of applying an already committed command needs a new behavior version.

## Layout of a version

| Path | Content |
| --- | --- |
| `objects/<key>` | One file per object storage key, byte for byte. Leases are left out: they name a random holder process and expire. |
| `state.txt` | Every committed row except NodeMeta, sorted by column family and key. |
| `README.md` | How the version was produced and what it covers. |

`state.txt` has one line per row: column family, key in hex, then the value. A value of up to
64 bytes is written as `=<hex>`. A longer value is written as `#<length>:<xxh3-128 hex>`.
NodeMeta is omitted because every restore gives the store a new lineage. PartitionMeta rows
are written without `applied` and `applied_epoch`, because a recovering node advances them to
its own EpochStart.

Snapshots use the memory store kind. RocksDB checkpoints carry random database identifiers and
file numbers, so they cannot be reproduced byte for byte. Both store kinds hold the same rows.

## When to add a version

Add `v<N+1>` in the change that does any of the following:

- changes a persistent format: `proto/dexzd/`, the RocksDB key layout, the object storage
  layout, or the snapshot format;
- adds a `FlowCommand` kind, a state table, an index kind, or an object kind;
- adds a behavior version, recorded by a workload that runs on it;
- prepares a release whose persisted data differs from the newest recorded version.

## Adding a version

Run these commands from `server-zd/`:

1. Extend `workload.rs` so that it writes the new format. Keep the files at hundreds of KiB.
2. Generate the version. The generator refuses a non-empty directory.

   ```bash
   DEXZD_FORMAT_COMPAT_OUTPUT=$PWD/testdata/v2 \
     cargo test -p dexzd-engine --test format_compat generate_format_compat_testdata -- --ignored
   ```

3. Generate it again into a temporary directory, and check that `diff -r` reports no
   difference.
4. Write `testdata/v2/README.md`.
5. Add the digest to `PINNED_VERSION_DIGESTS` in `golden_replay.rs`. The failing pin test prints
   it.
6. Run `cargo test -p dexzd-engine --test format_compat`.
