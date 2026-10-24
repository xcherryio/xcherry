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
    ClusterBootstrap, ClusterConfig, MetadataError, NodeConfig, bootstrap_or_validate,
    create_initial_manifest, read_manifest,
};
use dexzd_object_store::ObjectStore;
use dexzd_protocol::dexzd::v1::{Manifest, SnapshotReference};

use crate::recovery::node_streams::NodeStreams;
use crate::recovery::{EngineStartError, describe_lost_ownership};

/// The validated cluster object and manifest that a starting node works with.
pub(crate) struct ClusterMetadata {
    pub(crate) cluster: ClusterBootstrap,
    pub(crate) manifest: Manifest,
    pub(crate) behavior_version: u32,
    /// This node's recorded log streams, checked against its configured identity.
    pub(crate) streams: NodeStreams,
}

impl ClusterMetadata {
    /// Creates the cluster and manifest on first start; validates both on later starts.
    ///
    /// Rejects a node ID or incarnation that the manifest contradicts before the node takes a lease.
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
        Self::validate(cluster, manifest, node)
    }

    /// Re-reads the manifest once `epoch` holds the lease, keeping the immutable cluster object.
    /// Fails if another epoch owns it.
    pub(crate) async fn read_under_lease(
        self,
        object_store: &dyn ObjectStore,
        node: &NodeConfig,
        epoch: u64,
    ) -> Result<Self, EngineStartError> {
        let manifest = read_manifest(object_store)
            .await?
            .ok_or(MetadataError::Missing { key: "manifest" })?;
        let metadata = Self::validate(self.cluster, manifest, node)?;
        if let Some(detail) = describe_lost_ownership(&metadata.manifest, &node.node_id, epoch) {
            return Err(MetadataError::LeaseLost {
                node_id: node.node_id.clone(),
                detail,
            }
            .into());
        }
        Ok(metadata)
    }

    fn validate(
        cluster: ClusterBootstrap,
        manifest: Manifest,
        node: &NodeConfig,
    ) -> Result<Self, EngineStartError> {
        let partition_count = cluster.cluster.partition_count;
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
        let streams = NodeStreams::from_manifest(&manifest, node)?;
        Ok(Self {
            cluster,
            manifest,
            behavior_version,
            streams,
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
}
