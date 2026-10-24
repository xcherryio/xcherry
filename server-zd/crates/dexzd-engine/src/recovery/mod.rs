// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Node startup: cluster metadata, local store, log replay, and runtime rebuild hooks.

mod cluster_metadata;
mod local_store_opening;
mod log_replay;
mod node_streams;
mod runtime_rebuild;

pub use local_store_opening::LocalStoreConfig;
pub use log_replay::RecoveryConfig;
pub use runtime_rebuild::{
    CommittedCommandSink, EngineRuntimeHooks, EngineServices, PartitionRuntimeRebuilder,
    RuntimeRebuildError,
};

pub(crate) use cluster_metadata::ClusterMetadata;
pub(crate) use local_store_opening::{LocalStoreRequirements, open_local_store};
pub(crate) use log_replay::LogReplay;
pub(crate) use node_streams::describe_lost_ownership;

use crate::commit::NodeStopReason;

/// Why an engine node could not start.
#[derive(Debug, thiserror::Error)]
pub enum EngineStartError {
    /// Cluster metadata could not be read, created, or validated.
    #[error("cluster metadata: {0}")]
    Metadata(#[from] dexzd_cluster_metadata::MetadataError),
    /// The configured node ID differs from the node that owns the cluster's partitions.
    #[error(
        "configured node ID {configured:?} differs from node {recorded:?}, which owns partition \
         {partition}; a cluster runs one node, so start it with node ID {recorded:?}"
    )]
    NodeIdentityMismatch {
        /// Node ID in the node configuration.
        configured: String,
        /// Node ID that the manifest records as the partition owner.
        recorded: String,
        /// First partition owned by the recorded node.
        partition: u32,
    },
    /// The configured incarnation precedes the newest incarnation recorded for this node.
    #[error(
        "configured incarnation {configured} of node {node_id:?} precedes its recorded \
         incarnation {newest_recorded}; configure {newest_recorded} to continue that log stream \
         or a larger value to start a new one"
    )]
    IncarnationRegressed {
        /// This node's ID.
        node_id: String,
        /// Incarnation in the node configuration.
        configured: u32,
        /// Newest incarnation of this node that the manifest records.
        newest_recorded: u32,
    },
    /// Cluster metadata contradicts this node's configuration.
    #[error("invalid cluster metadata: {detail}")]
    InvalidMetadata {
        /// What is inconsistent.
        detail: String,
    },
    /// The version barrier requires a newer binary.
    #[error("version barrier {barrier} is newer than supported version {newest_supported}")]
    UnsupportedBehaviorVersion {
        /// Behavior version in the manifest barrier.
        barrier: u32,
        /// Newest version this binary applies.
        newest_supported: u32,
    },
    /// The local store could not be opened, restored, or read.
    #[error("local store: {0}")]
    Storage(#[from] dexzd_storage::StorageError),
    /// Local file handling failed.
    #[error("local store directory: {0}")]
    Io(#[from] std::io::Error),
    /// The log stream could not be recovered.
    #[error("log stream recovery: {0}")]
    Log(#[from] dexzd_log::LogError),
    /// The local store and snapshot are older than the oldest retained log batch.
    #[error(
        "log batches of incarnation {incarnation} before {first_retained_seq} were deleted; local \
         state resumes at {resumes_at}"
    )]
    LogUnavailable {
        /// Incarnation of the log stream that lacks the batches.
        incarnation: u32,
        /// Oldest retained batch seq.
        first_retained_seq: u64,
        /// First batch seq the local state needs.
        resumes_at: u64,
    },
    /// The local store needs batches of a log stream that log cleanup already deleted.
    #[error(
        "the log stream of incarnation {incarnation} was deleted; local state resumes at seq \
         {resumes_at} of it, and the oldest retained incarnation is {oldest_retained_incarnation}"
    )]
    LogStreamDeleted {
        /// Incarnation of the deleted log stream.
        incarnation: u32,
        /// First batch seq of that stream the local state needs.
        resumes_at: u64,
        /// Oldest incarnation of this node that the manifest still records.
        oldest_retained_incarnation: u32,
    },
    /// A partition runtime could not be rebuilt.
    #[error(transparent)]
    RuntimeRebuild(#[from] RuntimeRebuildError),
    /// The node stopped during startup.
    #[error("the node stopped during startup: {reason}")]
    Stopped {
        /// Why it stopped.
        reason: NodeStopReason,
    },
    /// The configuration is invalid.
    #[error("invalid engine configuration: {detail}")]
    InvalidConfig {
        /// What is invalid.
        detail: String,
    },
}
