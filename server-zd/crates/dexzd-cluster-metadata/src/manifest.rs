// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use bytes::Bytes;
use dexzd_clock::WallClock;
use dexzd_keyspace::validate_node_identifier;
use dexzd_object_store::{ObjectStore, ObjectStoreErrorKind, WriteCondition, WriteOutcome};
use dexzd_protocol::dexzd::v1::{
    LogSegment, Manifest, ManifestNode, PartitionAssignment, SnapshotReference, VersionBarrier,
};
use prost::Message;

use crate::{MAX_BEHAVIOR_VERSION, METADATA_FORMAT_VERSION, MIN_BEHAVIOR_VERSION, MetadataError};

const MANIFEST_KEY: &str = "manifest";

/// Node details recorded in the manifest.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// Stable node ID.
    pub node_id: String,
    /// Incarnation of the node's log stream.
    pub incarnation: u32,
    /// Address used by other nodes to forward requests.
    pub internal_address: String,
}

/// Whether a manifest edit changed its content.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestChange {
    /// Persist the edited manifest.
    Changed,
    /// Return the current manifest without a write.
    Unchanged,
}

/// Reads the current manifest, if present.
pub async fn read_manifest(store: &dyn ObjectStore) -> Result<Option<Manifest>, MetadataError> {
    store
        .get(MANIFEST_KEY)
        .await?
        .map(|content| {
            Manifest::decode(content.bytes).map_err(|source| MetadataError::Decode {
                key: MANIFEST_KEY,
                source,
            })
        })
        .transpose()
}

/// Creates the first manifest with every partition owned by the specified node.
pub async fn create_initial_manifest(
    store: &dyn ObjectStore,
    clock: &dyn WallClock,
    partition_count: u32,
    node: &NodeConfig,
) -> Result<Manifest, MetadataError> {
    validate_node_identifier(&node.node_id)?;
    if !partition_count.is_power_of_two() || partition_count > 65_536 {
        return Err(MetadataError::InvalidConfig {
            field: "partition count",
            detail: format!("invalid partition count {partition_count}"),
        });
    }
    let now = clock.now_unix_micros();
    let initial_manifest = Manifest {
        version: 1,
        partition_count,
        partitions: (0..partition_count)
            .map(|partition| PartitionAssignment {
                partition,
                owner_node_id: node.node_id.clone(),
                owner_epoch: 0,
                segments: vec![LogSegment {
                    node_id: node.node_id.clone(),
                    incarnation: node.incarnation,
                    start_seq: 1,
                    end_seq: 0,
                    is_open: true,
                }],
                snapshot: None,
            })
            .collect(),
        nodes: [(
            node.node_id.clone(),
            ManifestNode {
                node_id: node.node_id.clone(),
                incarnation: node.incarnation,
                epoch: 0,
                internal_address: node.internal_address.clone(),
                joined_at_unix_micros: now,
            },
        )]
        .into(),
        version_barrier: Some(VersionBarrier {
            behavior_version: MIN_BEHAVIOR_VERSION,
            format_version: METADATA_FORMAT_VERSION,
        }),
        controller_lease: None,
        updated_at_unix_micros: now,
    };
    let manifest = loop {
        let write_result = store
            .put(
                MANIFEST_KEY,
                Bytes::from(initial_manifest.encode_to_vec()),
                WriteCondition::CreateOnly,
            )
            .await;
        match write_result {
            Ok(WriteOutcome::Written { .. }) => break initial_manifest,
            Ok(WriteOutcome::PreconditionFailed) => {
                if let Some(existing) = read_manifest(store).await? {
                    break existing;
                }
            }
            Err(error) if error.kind == ObjectStoreErrorKind::OutcomeUnknown => {
                if let Some(existing) = read_manifest(store).await? {
                    break existing;
                }
            }
            Err(error) => return Err(error.into()),
        }
    };
    ensure_version_barrier_supported(&manifest)?;
    if manifest.partition_count != partition_count
        || manifest.partitions.len() != partition_count as usize
    {
        return Err(MetadataError::InvalidMetadata {
            key: MANIFEST_KEY,
            detail: format!("partition count differs from configured {partition_count}"),
        });
    }
    Ok(manifest)
}

/// Applies a conditional manifest edit, retrying when another writer wins.
pub async fn update_manifest<F>(
    store: &dyn ObjectStore,
    clock: &dyn WallClock,
    mut edit: F,
) -> Result<Manifest, MetadataError>
where
    F: FnMut(&mut Manifest) -> Result<ManifestChange, MetadataError>,
{
    // Ambiguous writes followed by another update can apply an edit again.
    loop {
        let current_object = store
            .get(MANIFEST_KEY)
            .await?
            .ok_or(MetadataError::Missing { key: MANIFEST_KEY })?;
        let current =
            Manifest::decode(current_object.bytes).map_err(|source| MetadataError::Decode {
                key: MANIFEST_KEY,
                source,
            })?;
        ensure_version_barrier_supported(&current)?;
        let mut proposed = current.clone();
        if edit(&mut proposed)? == ManifestChange::Unchanged {
            return Ok(current);
        }
        proposed.version = current
            .version
            .checked_add(1)
            .ok_or(MetadataError::Overflow {
                field: "manifest version",
            })?;
        proposed.updated_at_unix_micros = clock.now_unix_micros();
        let condition = WriteCondition::ReplaceIfEtagMatches {
            etag: current_object.etag,
        };
        let write_result = store
            .put(
                MANIFEST_KEY,
                Bytes::from(proposed.encode_to_vec()),
                condition,
            )
            .await;
        match write_result {
            Ok(WriteOutcome::Written { .. }) => return Ok(proposed),
            Ok(WriteOutcome::PreconditionFailed) => {
                if read_manifest(store).await?.as_ref() == Some(&proposed) {
                    return Ok(proposed);
                }
            }
            Err(error) if error.kind == ObjectStoreErrorKind::OutcomeUnknown => {
                if read_manifest(store).await?.as_ref() == Some(&proposed) {
                    return Ok(proposed);
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
}

/// Raises the writer epoch for a node at each process start.
pub async fn bump_node_epoch(
    store: &dyn ObjectStore,
    clock: &dyn WallClock,
    node: &NodeConfig,
) -> Result<u64, MetadataError> {
    validate_node_identifier(&node.node_id)?;
    let manifest = update_manifest(store, clock, |manifest| {
        let now = clock.now_unix_micros();
        let node_entry = manifest
            .nodes
            .entry(node.node_id.clone())
            .or_insert(ManifestNode {
                node_id: node.node_id.clone(),
                incarnation: node.incarnation,
                epoch: 0,
                internal_address: node.internal_address.clone(),
                joined_at_unix_micros: now,
            });
        node_entry.epoch = node_entry
            .epoch
            .checked_add(1)
            .ok_or(MetadataError::Overflow {
                field: "node epoch",
            })?;
        node_entry.incarnation = node.incarnation;
        node_entry
            .internal_address
            .clone_from(&node.internal_address);
        for partition in &mut manifest.partitions {
            if partition.owner_node_id == node.node_id {
                partition.owner_epoch = node_entry.epoch;
            }
        }
        Ok(ManifestChange::Changed)
    })
    .await?;
    Ok(manifest
        .nodes
        .get(&node.node_id)
        .expect("bump_node_epoch inserted the node")
        .epoch)
}

/// Raises the behavior and format barrier without lowering either version.
pub async fn raise_version_barrier(
    store: &dyn ObjectStore,
    clock: &dyn WallClock,
    behavior_version: u32,
    format_version: u32,
) -> Result<Manifest, MetadataError> {
    if behavior_version > MAX_BEHAVIOR_VERSION || format_version > METADATA_FORMAT_VERSION {
        return Err(MetadataError::UnsupportedVersionBarrier {
            behavior_version,
            format_version,
        });
    }
    update_manifest(store, clock, |manifest| {
        let barrier = manifest
            .version_barrier
            .get_or_insert(VersionBarrier::default());
        if barrier.behavior_version >= behavior_version && barrier.format_version >= format_version
        {
            return Ok(ManifestChange::Unchanged);
        }
        barrier.behavior_version = barrier.behavior_version.max(behavior_version);
        barrier.format_version = barrier.format_version.max(format_version);
        Ok(ManifestChange::Changed)
    })
    .await
}

fn ensure_version_barrier_supported(manifest: &Manifest) -> Result<(), MetadataError> {
    if let Some(barrier) = &manifest.version_barrier
        && (barrier.behavior_version > MAX_BEHAVIOR_VERSION
            || barrier.format_version > METADATA_FORMAT_VERSION)
    {
        return Err(MetadataError::UnsupportedVersionBarrier {
            behavior_version: barrier.behavior_version,
            format_version: barrier.format_version,
        });
    }
    Ok(())
}

/// Changes one partition's complete snapshot reference.
pub async fn update_partition_snapshot(
    store: &dyn ObjectStore,
    clock: &dyn WallClock,
    partition: u32,
    snapshot: SnapshotReference,
) -> Result<Manifest, MetadataError> {
    update_manifest(store, clock, |manifest| {
        let assignment = manifest.partitions.get_mut(partition as usize).ok_or(
            MetadataError::InvalidMetadata {
                key: MANIFEST_KEY,
                detail: format!("partition {partition} is out of range"),
            },
        )?;
        if assignment.partition != partition {
            return Err(MetadataError::InvalidMetadata {
                key: MANIFEST_KEY,
                detail: format!("partition entry {partition} is misordered"),
            });
        }
        if assignment.snapshot.as_ref() == Some(&snapshot) {
            return Ok(ManifestChange::Unchanged);
        }
        assignment.snapshot = Some(snapshot.clone());
        Ok(ManifestChange::Changed)
    })
    .await
}
