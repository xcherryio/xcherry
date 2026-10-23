# dexzd-cluster-metadata

## Purpose

Cluster object, partition manifest, and node leases that decide which server-zd node owns each partition.

## Startup sequence

Call `bootstrap_or_validate` before using a cluster. It creates the immutable cluster object once and checks partition count, hash algorithm, format version, and key layout on later starts. The returned partition hasher uses the stored key.

Create the initial manifest with `create_initial_manifest`; existing nodes read it through `read_manifest`. Call `acquire_lease` before writing the node's log stream. It acquires the lease and bumps the node epoch before returning. Keep the `LeaseHandle` alive while serving requests and watch `LeaseStatus`. Stop serving immediately when it reports `Lost` or the `ValidUntil` deadline passes. Call `shutdown` during orderly shutdown.

Manifest changes use `update_manifest` and ETag comparison. `raise_version_barrier` and `update_partition_snapshot` apply the common updates. `register` adds tenant attribute index declarations and returns `AttributeIndexConflict` if an existing key changes type.
