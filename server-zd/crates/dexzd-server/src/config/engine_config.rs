// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Identifies this node and the cluster's immutable partition layout.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct ClusterConfig {
    /// Defaults to empty and must be set before serving; identifies one lease owner across restarts.
    pub node_id: String,
    /// Defaults to 1024; a positive power of two used only when a cluster is created, then checked against its manifest.
    pub partition_count: u32,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            node_id: String::new(),
            partition_count: 1024,
        }
    }
}

/// Controls batching and object uploads for durable commands.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct CommitConfig {
    /// Defaults to None, which enables checks in debug builds and disables them in release builds.
    /// An explicit value overrides the build default for commit replay and time-travel checks.
    pub debug_replay_checks: Option<bool>,
    /// Defaults to 20 ms; maximum time an unsealed batch waits for more commands.
    #[serde(with = "humantime_serde")]
    pub batch_window: Duration,
    /// Defaults to 4; maximum conditional object writes in flight for one partition.
    pub max_in_flight: u32,
    /// Defaults to 4 MiB; seals a batch at this many serialized bytes unless a command itself is larger.
    pub seal_at_bytes: u64,
    /// Defaults to 8 MiB; object bodies at or above this size use multipart upload, not a hard object-size cap.
    pub multipart_threshold_bytes: u64,
}

impl Default for CommitConfig {
    fn default() -> Self {
        Self {
            debug_replay_checks: None,
            batch_window: Duration::from_millis(20),
            max_in_flight: 4,
            seal_at_bytes: 4 * 1024 * 1024,
            multipart_threshold_bytes: 8 * 1024 * 1024,
        }
    }
}

/// Controls how a partition owner renews its object-store lease.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct LeaseConfig {
    /// Defaults to 10 s; owner renews after one third of this interval and stops serving when validity ends.
    #[serde(with = "humantime_serde")]
    pub duration: Duration,
    /// Defaults to false; when true, startup fails if another unexpired owner holds the lease.
    pub fail_if_held: bool,
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            duration: Duration::from_secs(10),
            fail_if_held: false,
        }
    }
}

/// Controls snapshot creation, retention, and delayed log deletion.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct SnapshotConfig {
    /// Defaults to 256 MiB; a snapshot is requested after at least this much new log data.
    pub log_threshold_bytes: u64,
    /// Defaults to 10 min; a snapshot is requested when this time has elapsed since the previous one.
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    /// Defaults to 3; number of completed snapshot manifests retained for recovery.
    pub retained_count: u32,
    /// Defaults to 24 h; obsolete log objects wait this long in the deletion queue after snapshot coverage.
    #[serde(with = "humantime_serde")]
    pub log_deletion_delay: Duration,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            log_threshold_bytes: 256 * 1024 * 1024,
            interval: Duration::from_secs(10 * 60),
            retained_count: 3,
            log_deletion_delay: Duration::from_secs(24 * 60 * 60),
        }
    }
}

/// Controls how startup reads the log batches it replays.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct RecoveryConfig {
    /// Defaults to 64; most log batches read ahead of the batch being applied, counting GETs in
    /// flight. Replay reads about this many batches per object-store GET round trip, so 64 at a
    /// 10 ms GET latency allows about 6,400 batches per second. Must be between 1 and 1024.
    pub read_ahead_batches: u32,
    /// Defaults to 64 MiB; log object bytes that replay may hold read ahead. Each read reserves
    /// its batch's listed object size, and the reservation lasts until the batch is applied. A
    /// batch larger than this value is read alone, so the log objects held never exceed the
    /// larger of this value and the largest batch. Must be positive.
    pub read_ahead_bytes: u64,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            read_ahead_batches: 64,
            read_ahead_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Bounds reconstructable semantic history and checkpoint frequency for one run.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct HistoryConfig {
    /// Defaults to 10000; maximum semantic events in the recent reconstruction window before chunk pruning.
    /// Recorded in RunRecord at FlowStart; changes affect only later runs.
    pub window_semantic_events: u32,
    /// Defaults to 1000; number of old semantic events pruned per chunk, no greater than the window.
    pub prune_chunk_size: u32,
    /// Defaults to 1 MiB; lower bound on state bytes written before a checkpoint is considered.
    pub checkpoint_min_written_bytes: u64,
    /// Defaults to 10000; checkpoint after this many commands even when the byte threshold is not met.
    pub checkpoint_command_interval: u32,
    /// Defaults to 256 MiB per run; cap on superseded values retained for reconstruction within the window.
    /// Recorded in RunRecord at FlowStart; changes affect only later runs.
    pub max_retained_superseded_bytes: u64,
}

impl Default for HistoryConfig {
    fn default() -> Self {
        Self {
            window_semantic_events: 10_000,
            prune_chunk_size: 1_000,
            checkpoint_min_written_bytes: 1024 * 1024,
            checkpoint_command_interval: 10_000,
            max_retained_superseded_bytes: 256 * 1024 * 1024,
        }
    }
}

/// Controls value size limits, wire references, and local BlobDB placement.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct ValuesConfig {
    /// Defaults to 100 bytes; larger values are sent on the wire as references unless hydration is enabled.
    pub wire_inline_threshold_bytes: u64,
    /// Defaults to 16 MiB; maximum serialized size of one application value before an API or Worker rejection.
    pub max_value_bytes: u64,
    /// Defaults to false; when true, load referenced values before Worker dispatch instead of sending references.
    /// Client reads (GetAttributes, GetChannelMessages, GetFlowState, WaitForFlow results) and InvokeRPC,
    /// including its Worker request, then also return complete values. GetHistoryEvents keeps references,
    /// which LoadBlobs resolves.
    pub hydrate_before_dispatch: bool,
    /// Defaults to 64 KiB; values at or above this size use RocksDB BlobDB locally after committed apply.
    pub blob_db_min_value_bytes: u64,
}

impl Default for ValuesConfig {
    fn default() -> Self {
        Self {
            wire_inline_threshold_bytes: 100,
            max_value_bytes: 16 * 1024 * 1024,
            hydrate_before_dispatch: false,
            blob_db_min_value_bytes: 64 * 1024,
        }
    }
}

/// Controls how long closed Flow runs and their objects remain available.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct RetentionConfig {
    /// Defaults to 7 days; closed runs and their retained data become eligible for deletion afterward.
    #[serde(with = "humantime_serde")]
    pub closed_flow_duration: Duration,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            closed_flow_duration: Duration::from_secs(7 * 24 * 60 * 60),
        }
    }
}

/// Caps in-memory and persisted resources consumed by one Flow or one node.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct LimitsConfig {
    /// Defaults to 64 MiB; largest materialized state for one Flow, including current Attributes and Channels.
    pub max_flow_state_bytes: u64,
    /// Defaults to 10000; largest pending message count in one Channel.
    pub max_channel_messages: u32,
    /// Defaults to 1000; most concurrently active Step executions in one Flow.
    pub max_concurrent_steps_per_flow: u32,
    /// Defaults to 256; most uncommitted speculative commands retained for one Flow.
    pub max_speculative_commands_per_flow: u32,
    /// Defaults to 64 MiB; maximum speculative state and command bytes held by one node.
    pub max_speculative_bytes_per_node: u64,
    /// Defaults to 64; caps node-wide blocking state reads (1..=u32::MAX).
    /// Effect shutdown awaits in-progress reads; budget bounds a hung read.
    pub max_blocking_state_reads: u32,
    /// Defaults to 100000; maximum concurrent long-poll waiters served by one node.
    pub max_long_poll_waiters_per_node: u32,
    /// Defaults to 1000; maximum concurrent long-poll waiters attached to one Flow.
    pub max_long_poll_waiters_per_flow: u32,
    /// Defaults to 1000; number of recent request IDs retained for deduplication within one run.
    pub max_request_ids_per_run: u32,
    /// Defaults to 1000 UTF-8 bytes; maximum application-created Flow ID length.
    pub max_flow_id_bytes: u32,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_flow_state_bytes: 64 * 1024 * 1024,
            max_channel_messages: 10_000,
            max_concurrent_steps_per_flow: 1_000,
            max_speculative_commands_per_flow: 256,
            max_speculative_bytes_per_node: 64 * 1024 * 1024,
            max_blocking_state_reads: 64,
            max_long_poll_waiters_per_node: 100_000,
            max_long_poll_waiters_per_flow: 1_000,
            max_request_ids_per_run: 1_000,
            max_flow_id_bytes: 1_000,
        }
    }
}
