// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Snapshot scheduling, snapshot and log retention.

mod log_cleanup;
mod ownership_fence;
mod snapshot_config;
mod snapshot_retention;
mod snapshot_service;
mod snapshot_upload_store;

pub use snapshot_config::SnapshotConfig;

pub(crate) use log_cleanup::LogCleanup;
pub(crate) use ownership_fence::OwnershipFence;
pub(crate) use snapshot_retention::{SnapshotCatalog, SnapshotRetention};
pub(crate) use snapshot_service::SnapshotService;

/// A snapshot or cleanup step failed; the service retries later.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    /// The local store could not export or record the snapshot.
    #[error("snapshot storage: {0}")]
    Storage(#[from] dexzd_storage::StorageError),
    /// Object storage failed.
    #[error("snapshot object storage: {0}")]
    ObjectStore(#[from] dexzd_object_store::ObjectStoreError),
    /// The manifest could not be updated.
    #[error("snapshot manifest update: {0}")]
    Metadata(#[from] dexzd_cluster_metadata::MetadataError),
    /// A stored snapshot manifest could not be decoded.
    #[error("snapshot manifest decode: {0}")]
    Decode(#[from] prost::DecodeError),
    /// A snapshot value was invalid.
    #[error("invalid snapshot: {0}")]
    Invalid(String),
    /// The node stopped while the snapshot was taken.
    #[error("the node stopped")]
    NodeStopped,
    /// Another epoch owns the cluster, so this process must not publish or delete anything.
    #[error("snapshot fenced: {detail}")]
    Fenced {
        /// Which epoch owns the cluster.
        detail: String,
    },
}
