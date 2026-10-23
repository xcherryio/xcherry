# dexzd-storage

## Purpose

Local RocksDB state store for server-zd, including atomic batch writes, state views, and snapshot upload and restore.

The synchronous `LocalStore` contract runs on the engine's dedicated blocking thread pool.
Both backends support atomic multi-column-family batches, ordered scans, and point-in-time
read views. `InMemoryLocalStore` uses ordered maps for deterministic simulation.

`RocksDbLocalStore` creates all six keyspace column families at open. Its block cache is
bounded by `RocksDbStoreOptions::block_cache_bytes` (128 MiB by default). The `value`
column family stores values at least 64 KiB in blob files and enables blob garbage
collection. Writes keep the RocksDB WAL enabled, making batches across column families
atomic after a crash. Disabling WAL in the future would also require `atomic_flush`.

Snapshots use RocksDB checkpoints or one serialized memory file. All files are uploaded
with CreateOnly under the node and lineage, and a manifest is written last. Restore
checks every file's size and xxh3-64 checksum before opening the store and assigning
a new UUIDv7 lineage. The caller supplies snapshot timestamps from its wall clock.
Retention keeps the newest three manifests by default; callers pass the retention
count and deletion delay to `retain_snapshots`.

The future `dexzd-apply::FlowStateReader` adapter belongs over `consistent_view()`.
Its contract has not been defined yet.
