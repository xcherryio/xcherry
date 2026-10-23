// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Identifiers, partition hashing, RocksDB key encodings, and value references shared by every server-zd crate.
//!
//! Everything in this crate is a permanent format. Byte values of enums and the layout of keys
//! never change; new variants and key kinds are only appended.

mod identifiers;
mod key_layout;
mod node_identifier;
mod partition_hash;
mod value_reference;

pub use identifiers::{FlowHash, IdentifierError, PartitionId, RunId, TenantId, ValueId};
pub use key_layout::{
    ColumnFamily, FLOW_DIRECTORY_KEY_TAG, FlowKeyRange, IndexKind, KeyLayoutError, ParsedRunKey,
    RUN_KEY_TAG, RunTable, decode_partition_meta_key, flow_directory_key, flow_directory_prefix,
    parse_run_key, partition_index_key, partition_index_prefix, partition_key_range,
    partition_meta_key, run_key, run_prefix, run_table_prefix,
};
pub use node_identifier::{NodeIdentifierError, validate_node_identifier};
pub use partition_hash::{PartitionHashError, PartitionHasher};
pub use value_reference::{VALUE_REFERENCE_PREFIX, ValueReference, ValueReferenceError};

/// Current RocksDB key layout version, recorded in the cluster object.
pub const KEY_LAYOUT_VERSION: u32 = 1;
