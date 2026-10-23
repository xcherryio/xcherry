// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use bytes::Bytes;
use dexzd_clock::WallClock;
use dexzd_keyspace::{KEY_LAYOUT_VERSION, PartitionHasher};
use dexzd_object_store::{ObjectStore, ObjectStoreErrorKind, WriteCondition, WriteOutcome};
use dexzd_protocol::dexzd::v1::{ClusterObject, PartitionHashAlgorithm};
use prost::Message;
use rand::{TryRng, rngs::SysRng};
use uuid::Uuid;

use crate::{METADATA_FORMAT_VERSION, MetadataError};

const CLUSTER_KEY: &str = "cluster";

/// Immutable cluster settings supplied when a node starts.
#[derive(Debug, Clone, Copy)]
pub struct ClusterConfig {
    /// Number of partitions; defaults to 1024 and never changes after bootstrap.
    pub partition_count: u32,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            partition_count: 1024,
        }
    }
}

/// Validated cluster identity and partition hasher.
#[derive(Debug, Clone)]
pub struct ClusterBootstrap {
    /// Stored immutable cluster object.
    pub cluster: ClusterObject,
    /// Hasher configured with the cluster's persistent key.
    pub partition_hasher: PartitionHasher,
}

/// Creates the cluster once, then validates immutable settings on every start.
pub async fn bootstrap_or_validate(
    store: &dyn ObjectStore,
    clock: &dyn WallClock,
    config: ClusterConfig,
) -> Result<ClusterBootstrap, MetadataError> {
    PartitionHasher::new(&[0; 16], config.partition_count)?;
    let cluster = match store.get(CLUSTER_KEY).await? {
        Some(content) => decode_cluster(content.bytes)?,
        None => {
            let proposed = ClusterObject {
                format_version: METADATA_FORMAT_VERSION,
                cluster_id: Uuid::now_v7().as_bytes().to_vec(),
                partition_count: config.partition_count,
                partition_hash_algorithm: PartitionHashAlgorithm::Siphash24 as i32,
                partition_hash_key: random_hash_key()?.to_vec(),
                key_layout_version: KEY_LAYOUT_VERSION,
                created_at_unix_micros: clock.now_unix_micros(),
            };
            let result = store
                .put(
                    CLUSTER_KEY,
                    Bytes::from(proposed.encode_to_vec()),
                    WriteCondition::CreateOnly,
                )
                .await;
            match result {
                Ok(WriteOutcome::Written { .. }) => proposed,
                Ok(WriteOutcome::PreconditionFailed) => read_cluster(store).await?,
                Err(error) if error.kind == ObjectStoreErrorKind::OutcomeUnknown => {
                    read_cluster(store).await?
                }
                Err(error) => return Err(error.into()),
            }
        }
    };
    check_match(
        "partition count",
        config.partition_count,
        cluster.partition_count,
    )?;
    check_match(
        "partition hash algorithm",
        PartitionHashAlgorithm::Siphash24 as i32,
        cluster.partition_hash_algorithm,
    )?;
    check_match(
        "key layout version",
        KEY_LAYOUT_VERSION,
        cluster.key_layout_version,
    )?;
    check_match(
        "metadata format version",
        METADATA_FORMAT_VERSION,
        cluster.format_version,
    )?;
    let cluster_id =
        Uuid::from_slice(&cluster.cluster_id).map_err(|error| MetadataError::InvalidMetadata {
            key: CLUSTER_KEY,
            detail: format!("cluster ID is not a UUID: {error}"),
        })?;
    if cluster_id.get_version_num() != 7 {
        return Err(MetadataError::InvalidMetadata {
            key: CLUSTER_KEY,
            detail: "cluster ID is not UUIDv7".to_owned(),
        });
    }
    let partition_hasher =
        PartitionHasher::new(&cluster.partition_hash_key, cluster.partition_count)?;
    Ok(ClusterBootstrap {
        cluster,
        partition_hasher,
    })
}

async fn read_cluster(store: &dyn ObjectStore) -> Result<ClusterObject, MetadataError> {
    let content = store
        .get(CLUSTER_KEY)
        .await?
        .ok_or(MetadataError::Missing { key: CLUSTER_KEY })?;
    decode_cluster(content.bytes)
}

fn decode_cluster(bytes: Bytes) -> Result<ClusterObject, MetadataError> {
    ClusterObject::decode(bytes).map_err(|source| MetadataError::Decode {
        key: CLUSTER_KEY,
        source,
    })
}

fn check_match<T: std::fmt::Display + PartialEq>(
    field: &'static str,
    configured: T,
    stored: T,
) -> Result<(), MetadataError> {
    if configured == stored {
        return Ok(());
    }
    Err(MetadataError::ClusterMismatch {
        field,
        configured: configured.to_string(),
        stored: stored.to_string(),
    })
}

fn random_hash_key() -> Result<[u8; 16], MetadataError> {
    let mut key = [0; 16];
    SysRng
        .try_fill_bytes(&mut key)
        .map_err(|error| MetadataError::InvalidMetadata {
            key: CLUSTER_KEY,
            detail: format!("OS random generator failed: {error}"),
        })?;
    Ok(key)
}
