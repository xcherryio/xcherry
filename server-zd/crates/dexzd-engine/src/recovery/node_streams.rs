// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_cluster_metadata::{ManifestChange, NodeConfig};
use dexzd_protocol::dexzd::v1::{LogSegment, Manifest};

use crate::recovery::EngineStartError;

/// One log stream of this node that the manifest records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RecordedSegment {
    pub(crate) incarnation: u32,
    /// Lowest seq that every partition still retains.
    pub(crate) start_seq: u64,
    /// Inclusive last seq of a closed segment; `None` while the segment is open.
    pub(crate) end_seq: Option<u64>,
}

/// This node's recorded log streams, oldest first, and the incarnation it serves.
#[derive(Debug, Clone)]
pub(crate) struct NodeStreams {
    pub(crate) node_id: String,
    /// Ends with the only open segment.
    pub(crate) segments: Vec<RecordedSegment>,
    pub(crate) serving_incarnation: u32,
}

impl NodeStreams {
    /// Reads this node's streams and checks the configured node ID and incarnation against them.
    pub(crate) fn from_manifest(
        manifest: &Manifest,
        node: &NodeConfig,
    ) -> Result<Self, EngineStartError> {
        if let Some(foreign) = manifest
            .partitions
            .iter()
            .find(|partition| partition.owner_node_id != node.node_id)
        {
            return Err(EngineStartError::NodeIdentityMismatch {
                configured: node.node_id.clone(),
                recorded: foreign.owner_node_id.clone(),
                partition: foreign.partition,
            });
        }
        let mut merged: Option<Vec<RecordedSegment>> = None;
        for partition in &manifest.partitions {
            let mut segments = Vec::with_capacity(partition.segments.len());
            for segment in &partition.segments {
                if segment.node_id != node.node_id {
                    return Err(invalid_metadata(format!(
                        "partition {} has a log segment of node {:?}; a node replays only its own \
                         log streams",
                        partition.partition, segment.node_id
                    )));
                }
                segments.push(RecordedSegment {
                    incarnation: segment.incarnation,
                    start_seq: segment.start_seq,
                    end_seq: (!segment.is_open).then_some(segment.end_seq),
                });
            }
            segments.sort_by_key(|segment| segment.incarnation);
            merged = Some(match merged {
                None => segments,
                Some(previous) => {
                    merge_partition_segments(previous, &segments).ok_or_else(|| {
                        invalid_metadata(format!(
                            "partition {} records different log segments than partition 0",
                            partition.partition
                        ))
                    })?
                }
            });
        }
        let segments = merged.unwrap_or_default();
        validate_segment_order(&segments)?;
        let newest_recorded = segments
            .last()
            .expect("validate_segment_order requires a segment")
            .incarnation;
        if node.incarnation < newest_recorded {
            return Err(EngineStartError::IncarnationRegressed {
                node_id: node.node_id.clone(),
                configured: node.incarnation,
                newest_recorded,
            });
        }
        Ok(Self {
            node_id: node.node_id.clone(),
            segments,
            serving_incarnation: node.incarnation,
        })
    }

    /// Returns the open segment, the stream this node wrote last.
    pub(crate) fn open_segment(&self) -> RecordedSegment {
        *self
            .segments
            .last()
            .expect("from_manifest requires an open segment")
    }

    /// Returns whether startup seals the open segment and starts the serving incarnation.
    pub(crate) fn is_changing_incarnation(&self) -> bool {
        self.open_segment().incarnation != self.serving_incarnation
    }

    /// Closes the open segment at `end_seq` and opens the serving incarnation at seq 1.
    pub(crate) fn close_open_segment(
        &self,
        manifest: &mut Manifest,
        end_seq: u64,
    ) -> ManifestChange {
        let closing_incarnation = self.open_segment().incarnation;
        let mut change = ManifestChange::Unchanged;
        for partition in &mut manifest.partitions {
            for segment in &mut partition.segments {
                if segment.node_id == self.node_id
                    && segment.incarnation == closing_incarnation
                    && segment.is_open
                {
                    segment.is_open = false;
                    segment.end_seq = end_seq;
                    change = ManifestChange::Changed;
                }
            }
            let has_serving_segment = partition.segments.iter().any(|segment| {
                segment.node_id == self.node_id && segment.incarnation == self.serving_incarnation
            });
            if !has_serving_segment {
                partition.segments.push(LogSegment {
                    node_id: self.node_id.clone(),
                    incarnation: self.serving_incarnation,
                    start_seq: 1,
                    end_seq: 0,
                    is_open: true,
                });
                change = ManifestChange::Changed;
            }
        }
        change
    }
}

/// Describes why `epoch` of `node_id` no longer owns every partition, or returns `None`.
pub(crate) fn describe_lost_ownership(
    manifest: &Manifest,
    node_id: &str,
    epoch: u64,
) -> Option<String> {
    let recorded_epoch = manifest.nodes.get(node_id).map(|node| node.epoch);
    if recorded_epoch != Some(epoch) {
        return Some(format!(
            "node {node_id:?} epoch {epoch} was replaced; the manifest records epoch {}",
            recorded_epoch.map_or_else(|| "none".to_owned(), |epoch| epoch.to_string())
        ));
    }
    manifest
        .partitions
        .iter()
        .find(|partition| partition.owner_node_id != node_id || partition.owner_epoch != epoch)
        .map(|partition| {
            format!(
                "partition {} is owned by node {:?} epoch {}, not epoch {epoch}",
                partition.partition, partition.owner_node_id, partition.owner_epoch
            )
        })
}

/// Combines one more partition's segments, or returns `None` when the partitions disagree.
fn merge_partition_segments(
    mut merged: Vec<RecordedSegment>,
    segments: &[RecordedSegment],
) -> Option<Vec<RecordedSegment>> {
    if merged.len() != segments.len() {
        return None;
    }
    for (merged_segment, segment) in merged.iter_mut().zip(segments) {
        if merged_segment.incarnation != segment.incarnation
            || merged_segment.end_seq != segment.end_seq
        {
            return None;
        }
        // Batches below the highest start may be deleted.
        merged_segment.start_seq = merged_segment.start_seq.max(segment.start_seq);
    }
    Some(merged)
}

fn validate_segment_order(segments: &[RecordedSegment]) -> Result<(), EngineStartError> {
    let Some((open, closed)) = segments.split_last() else {
        return Err(invalid_metadata(
            "the manifest records no log segment for this node".to_owned(),
        ));
    };
    if open.end_seq.is_some() || closed.iter().any(|segment| segment.end_seq.is_none()) {
        return Err(invalid_metadata(
            "only the newest log segment of a node may be open".to_owned(),
        ));
    }
    if segments
        .windows(2)
        .any(|pair| pair[0].incarnation >= pair[1].incarnation)
    {
        return Err(invalid_metadata(
            "log segments of a node must have distinct incarnations".to_owned(),
        ));
    }
    Ok(())
}

fn invalid_metadata(detail: String) -> EngineStartError {
    EngineStartError::InvalidMetadata { detail }
}
