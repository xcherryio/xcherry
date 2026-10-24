// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Local RocksDB state store for server-zd, including atomic batch writes, state views, and snapshot upload and restore.

mod local_store;
mod memory_store;
mod rocksdb_store;
mod snapshot;
mod state_comparison;

pub use local_store::{
    LocalReadView, LocalStore, ScanDirection, StorageError, StorageResult, WriteBatch,
    WriteOperation,
};
pub use memory_store::InMemoryLocalStore;
pub use rocksdb_store::{RocksDbLocalStore, RocksDbStoreOptions};
pub use snapshot::{
    DEFAULT_SNAPSHOT_RETENTION, RestoredLocalStore, SnapshotIdentity, SnapshotSource,
    create_snapshot, restore_snapshot, retain_snapshots,
};
pub use state_comparison::{
    CanonicalizedStoreField, StateComparisonError, StateDifference, compare_store_views,
};

#[cfg(test)]
mod compaction_tests;
#[cfg(test)]
mod snapshot_crash_tests;
#[cfg(test)]
mod tests;
