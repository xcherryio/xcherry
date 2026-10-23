// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_apply::BehaviorVersion;
use dexzd_clock::WallClock;
use dexzd_cluster_metadata::{
    ClusterBootstrap, ClusterConfig, NodeConfig, bootstrap_or_validate, create_initial_manifest,
    read_manifest,
};
use dexzd_object_store::ObjectStore;
use dexzd_protocol::dexzd::v1::{Manifest, SnapshotReference};

use crate::recovery::EngineStartError;

/// The validated cluster object and manifest that a starting node works with.
pub(crate) struct ClusterMetadata {
    pub(crate) cluster: ClusterBootstrap,
    pub(crate) manifest: Manifest,
    pub(crate) behavior_version: u32,
}

impl ClusterMetadata {
    /// Creates the cluster and manifest on first start; validates both on later starts.
    pub(crate) async fn bootstrap_or_validate(
        object_store: &dyn ObjectStore,
        clock: &dyn WallClock,
        cluster_config: ClusterConfig,
        node: &NodeConfig,
    ) -> Result<Self, EngineStartError> {
        let cluster = bootstrap_or_validate(object_store, clock, cluster_config).await?;
        let partition_count = cluster.cluster.partition_count;
        let manifest = match read_manifest(object_store).await? {
            Some(manifest) => manifest,
            None => create_initial_manifest(object_store, clock, partition_count, node).await?,
        };
        if manifest.partition_count != partition_count
            || manifest.partitions.len() != partition_count as usize
        {
            return Err(EngineStartError::InvalidMetadata {
                detail: format!(
                    "manifest has {} partitions; the cluster has {partition_count}",
                    manifest.partitions.len()
                ),
            });
        }
        let behavior_version = manifest
            .version_barrier
            .as_ref()
            .map_or(BehaviorVersion::INITIAL.number(), |barrier| {
                barrier.behavior_version
            });
        if behavior_version < BehaviorVersion::MINIMUM_SUPPORTED.number()
            || behavior_version > BehaviorVersion::CURRENT.number()
        {
            return Err(EngineStartError::UnsupportedBehaviorVersion {
                barrier: behavior_version,
                newest_supported: BehaviorVersion::CURRENT.number(),
            });
        }
        Ok(Self {
            cluster,
            manifest,
            behavior_version,
        })
    }

    pub(crate) fn partition_count(&self) -> u32 {
        self.cluster.cluster.partition_count
    }

    pub(crate) fn cluster_id(&self) -> Result<[u8; 16], EngineStartError> {
        self.cluster
            .cluster
            .cluster_id
            .as_slice()
            .try_into()
            .map_err(|_| EngineStartError::InvalidMetadata {
                detail: "cluster ID must contain 16 bytes".to_owned(),
            })
    }

    /// Returns the newest snapshot of this node, when one covers every partition.
    pub(crate) fn node_snapshot(&self, node_id: &str) -> Option<&SnapshotReference> {
        let first = self.manifest.partitions.first()?.snapshot.as_ref()?;
        let is_complete = self.manifest.partitions.iter().all(|partition| {
            partition.snapshot.as_ref() == Some(first) && partition.owner_node_id == node_id
        });
        (is_complete && first.node_id == node_id).then_some(first)
    }

    /// Returns the first retained log seq of this node's open log segment.
    pub(crate) fn first_retained_seq(&self, node_id: &str, incarnation: u32) -> u64 {
        self.manifest
            .partitions
            .iter()
            .flat_map(|partition| &partition.segments)
            .filter(|segment| {
                segment.is_open && segment.node_id == node_id && segment.incarnation == incarnation
            })
            .map(|segment| segment.start_seq)
            .max()
            .unwrap_or(1)
            .max(1)
    }
}
