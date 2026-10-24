# dexzd-cluster-metadata

## Purpose

Cluster object, partition manifest, and node leases that decide which server-zd node owns each partition.

## Startup sequence

Call `bootstrap_or_validate` before using a cluster. It creates the immutable cluster object once and checks partition count, hash algorithm, format version, and key layout on later starts. The returned partition hasher uses the stored key.

Create the initial manifest with `create_initial_manifest`; existing nodes read it through `read_manifest`. Call `acquire_lease` before writing the node's log stream. It acquires the lease and bumps the node epoch before returning. While another process holds the lease, it rereads the lease every second, or every third of the lease duration if that is shorter. It takes a released lease at once and takes an expired one only after the takeover grace. Read the manifest that recovery depends on after `acquire_lease` returns; the previous holder can change it until then. Keep the `LeaseHandle` alive while serving requests and watch `LeaseStatus`. Stop serving immediately when it reports `Lost` or the `ValidUntil` deadline passes. During orderly shutdown, stop serving before calling `shutdown`; subscribers see `Lost` before the lease is released for immediate reacquisition. `shutdown` returns `LeaseReleaseError` when object storage rejects the release or the release does not finish within `LeaseHandle::RELEASE_BUDGET`; the lease then stays held until it expires. It returns `Ok` when another process already took the lease over. Dropping the handle only stops renewal; the lease then expires normally. An `acquire_lease` that fails or is cancelled after writing the lease releases it before the error returns, or in the background after cancellation.

Manifest changes use `update_manifest` and ETag comparison. `raise_version_barrier` and `update_partition_snapshot` apply the common updates. `register_attribute_indexes` adds tenant attribute index declarations and returns `AttributeIndexConflict` if an existing key changes type.
