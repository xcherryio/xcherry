// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Cluster object, partition manifest, and node leases that decide which server-zd node owns each partition.

mod cluster;
mod error;
mod lease;
mod manifest;
mod registry;

pub use cluster::{ClusterBootstrap, ClusterConfig, bootstrap_or_validate};
pub use error::MetadataError;
pub use lease::{
    LeaseConfig, LeaseHandle, LeaseReleaseError, LeaseStatus, acquire_lease,
    acquire_lease_with_holder_process_id,
};
pub use manifest::{
    ManifestChange, NodeConfig, bump_node_epoch, create_initial_manifest, raise_version_barrier,
    read_manifest, update_manifest, update_partition_snapshot,
};
pub use registry::{
    AttributeIndexDeclaration, read_attribute_index_registry, register_attribute_indexes,
};

/// First behavior version supported by this binary.
pub const MIN_BEHAVIOR_VERSION: u32 = 1;
/// Latest behavior version this binary can write.
pub const MAX_BEHAVIOR_VERSION: u32 = 1;
/// Persistent metadata format written by this binary.
pub const METADATA_FORMAT_VERSION: u32 = 1;
