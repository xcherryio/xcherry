// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;

use dexzd_clock::WallClock;
use dexzd_cluster_metadata::{ManifestChange, MetadataError};
use dexzd_object_store::ObjectStore;
use dexzd_protocol::dexzd::v1::Manifest;

use crate::commit::{CommitState, NodeStopReason};
use crate::recovery::describe_lost_ownership;
use crate::snapshots::SnapshotError;

/// Lets snapshot publication and cleanup act only while this process serves and owns the cluster.
pub(crate) struct OwnershipFence {
    pub(crate) object_store: Arc<dyn ObjectStore>,
    pub(crate) clock: Arc<dyn WallClock>,
    pub(crate) state: Arc<CommitState>,
    pub(crate) node_id: String,
    /// Epoch of this process.
    pub(crate) epoch: u64,
}

impl OwnershipFence {
    /// Fails once the node stopped serving.
    pub(crate) fn ensure_serving(&self) -> Result<(), SnapshotError> {
        match self.state.stop_reason() {
            Some(_) => Err(SnapshotError::NodeStopped),
            None => Ok(()),
        }
    }

    /// Applies `edit` in the same conditional write that checks serving and epoch ownership.
    ///
    /// Returns the written manifest. Fails with [`SnapshotError::NodeStopped`] after the node
    /// stopped, and with [`SnapshotError::Fenced`] after another epoch took ownership, which
    /// also stops the node.
    pub(crate) async fn update_manifest<F>(&self, mut edit: F) -> Result<Manifest, SnapshotError>
    where
        F: FnMut(&mut Manifest) -> ManifestChange,
    {
        let mut rejection = None;
        let result = dexzd_cluster_metadata::update_manifest(
            self.object_store.as_ref(),
            self.clock.as_ref(),
            |manifest| {
                if self.state.stop_reason().is_some() {
                    rejection = Some(SnapshotError::NodeStopped);
                } else if let Some(detail) =
                    describe_lost_ownership(manifest, &self.node_id, self.epoch)
                {
                    rejection = Some(SnapshotError::Fenced { detail });
                } else {
                    return Ok(edit(manifest));
                }
                Err(MetadataError::LeaseLost {
                    node_id: self.node_id.clone(),
                    detail: "the manifest update was rejected".to_owned(),
                })
            },
        )
        .await;
        match rejection {
            Some(SnapshotError::Fenced { detail }) => {
                tracing::error!(detail = %detail, "another epoch owns the cluster");
                self.state.stop(NodeStopReason::Fenced);
                Err(SnapshotError::Fenced { detail })
            }
            Some(rejection) => Err(rejection),
            None => Ok(result?),
        }
    }
}
