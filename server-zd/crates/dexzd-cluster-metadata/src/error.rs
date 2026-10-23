// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_keyspace::{NodeIdentifierError, PartitionHashError};
use dexzd_object_store::ObjectStoreError;

/// Failure to read or change cluster metadata.
#[derive(Debug, thiserror::Error)]
pub enum MetadataError {
    /// Object storage could not complete an operation.
    #[error(transparent)]
    Store(#[from] ObjectStoreError),
    /// A stored protobuf could not be decoded.
    #[error("invalid {key} metadata: {source}")]
    Decode {
        /// Object key.
        key: &'static str,
        /// Protobuf decode failure.
        #[source]
        source: prost::DecodeError,
    },
    /// Cluster configuration differs from the stored cluster object.
    #[error("cluster {field} mismatch: configured {configured}, stored {stored}")]
    ClusterMismatch {
        /// Mismatched field.
        field: &'static str,
        /// Configured or binary value.
        configured: String,
        /// Stored value.
        stored: String,
    },
    /// Partition hash parameters are invalid.
    #[error(transparent)]
    PartitionHash(#[from] PartitionHashError),
    /// A configured node ID cannot appear in object keys.
    #[error(transparent)]
    InvalidNodeIdentifier(#[from] NodeIdentifierError),
    /// A metadata setting is invalid.
    #[error("invalid {field} configuration: {detail}")]
    InvalidConfig {
        /// Configured field.
        field: &'static str,
        /// Rejection reason.
        detail: String,
    },
    /// The operating system could not supply random bytes.
    #[error("OS random generator failed: {detail}")]
    RandomSource {
        /// Random source error.
        detail: String,
    },
    /// A metadata value violates the persistent format.
    #[error("invalid {key} metadata: {detail}")]
    InvalidMetadata {
        /// Object key.
        key: &'static str,
        /// Invalid content or configuration.
        detail: String,
    },
    /// This binary cannot safely update a manifest with a newer version barrier.
    #[error("unsupported version barrier: behavior {behavior_version}, format {format_version}")]
    UnsupportedVersionBarrier {
        /// Barrier behavior version.
        behavior_version: u32,
        /// Barrier format version.
        format_version: u32,
    },
    /// A required object is absent.
    #[error("{key} metadata is missing")]
    Missing {
        /// Object key.
        key: &'static str,
    },
    /// An index key was already registered with another type.
    #[error(
        "tenant {tenant_id} attribute index {index_key:?} has type {existing_type}, requested {requested_type}"
    )]
    AttributeIndexConflict {
        /// Tenant ID.
        tenant_id: u64,
        /// Index key.
        index_key: String,
        /// Existing index type.
        existing_type: i32,
        /// Requested index type.
        requested_type: i32,
    },
    /// Another process holds the requested node lease.
    #[error("node {node_id:?} lease is held by another process until {expires_at_unix_micros}")]
    LeaseHeld {
        /// Node ID.
        node_id: String,
        /// Lease expiry.
        expires_at_unix_micros: i64,
    },
    /// This process no longer holds its node lease.
    #[error("node {node_id:?} lease was lost: {detail}")]
    LeaseLost {
        /// Node ID.
        node_id: String,
        /// Why the lease was lost.
        detail: String,
    },
    /// A version or epoch cannot be increased.
    #[error("{field} overflow")]
    Overflow {
        /// Counter name.
        field: &'static str,
    },
}
