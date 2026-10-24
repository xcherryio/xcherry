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

`RocksDbStoreOptions::max_total_wal_bytes` (256 MiB by default) caps the WAL files on disk.
Without it, a column family with few writes, such as partition metadata, keeps every WAL file
alive until they reach four times the memtable budget of all column families.
`start_background_compaction` compacts every column family on a store-owned thread, which
first flushes the memtables and so releases the WAL. Dropping the store cancels RocksDB
background work and joins that thread before the database closes.

Snapshots use RocksDB checkpoints or one serialized memory file. All files are uploaded
with CreateOnly under the node and lineage, and a manifest is written last. Restore
checks every file's size and xxh3-64 checksum before opening the store and assigning
a new UUIDv7 lineage. The caller supplies snapshot timestamps from its wall clock.
Run `create_snapshot` and `restore_snapshot` on the engine's blocking pool: checkpoint,
file enumeration, checksums, and local store open are synchronous even while object I/O
is asynchronous.
Retention keeps the newest three manifests by default; callers pass the retention
count and deletion delay to `retain_snapshots`. Do not run retention concurrently
with snapshot creation for the same lineage.

The future `dexzd-apply::FlowStateReader` adapter belongs over `consistent_view()`.
Its contract has not been defined yet.
