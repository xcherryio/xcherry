// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Typed, validated configuration for the dex-server-zd executable.

mod engine_config;
mod service_config;
mod source_config;
mod storage_config;
mod validation;

#[cfg(test)]
mod tests;

pub use engine_config::{
    ClusterConfig, CommitConfig, HistoryConfig, LeaseConfig, LimitsConfig, RecoveryConfig,
    RetentionConfig, SnapshotConfig, ValuesConfig,
};
pub use service_config::{
    ApiConfig, MatchingConfig, MetricsConfig, SearchBackend, SearchConfig, StreamStoreBackend,
    StreamStoreConfig,
};
pub use source_config::ConfigError;
pub use storage_config::{
    CredentialSource, DevConfig, DevDurabilityMode, LocalStorageConfig, ObjectStoreConfig,
    ObjectStoreCredentials, ObjectStoreLocationConfig,
};

use serde::{Deserialize, Serialize};

/// Groups every server-zd configuration section for validation and component injection.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct Config {
    /// Cluster identity and partition layout. Defaults to an unset node ID and 1024 partitions.
    pub cluster: ClusterConfig,
    /// Durable S3 storage and optional hard-delete destination. Defaults to the AWS chain and an unset bucket.
    pub object_store: ObjectStoreConfig,
    /// Rebuildable RocksDB cache settings. Defaults to an unset directory, a 256 MiB block cache, and a 256 MiB write-ahead log cap.
    pub local_storage: LocalStorageConfig,
    /// Group commit and multipart thresholds. Defaults to a 20 ms window and four in-flight writes.
    pub commit: CommitConfig,
    /// Owner lease settings. Defaults to a ten-second lease.
    pub lease: LeaseConfig,
    /// Snapshot cadence and log cleanup. Defaults to 256 MiB or ten minutes.
    pub snapshot: SnapshotConfig,
    /// Log replay read-ahead at startup. Defaults to 64 batches and 64 MiB.
    pub recovery: RecoveryConfig,
    /// Per-run semantic history window and checkpoints. Defaults to 10000 events.
    pub history: HistoryConfig,
    /// Value sizes and wire hydration behavior. Defaults to 100-byte inline values and a 16 MiB maximum.
    pub values: ValuesConfig,
    /// Closed Flow retention. Defaults to seven days.
    pub retention: RetentionConfig,
    /// Public gRPC listener and request limits. Defaults to port 8801 and 32 MiB messages.
    pub api: ApiConfig,
    /// Separate unauthenticated OpenMetrics HTTP listener. Defaults to disabled.
    pub metrics: MetricsConfig,
    /// Pull Worker matching budgets and capacities. Defaults to ten-second dispatch and five-second sticky waits.
    pub matching: MatchingConfig,
    /// Stream storage and trimming. Defaults to disabled storage.
    pub stream_store: StreamStoreConfig,
    /// Search backend and query budgets. Defaults to builtin search.
    pub search: SearchConfig,
    /// Flow, speculative state, wait, and deduplication limits.
    pub limits: LimitsConfig,
    /// Local development paths and embedded S3 durability. Defaults to unset paths and relaxed durability.
    pub dev: DevConfig,
}
