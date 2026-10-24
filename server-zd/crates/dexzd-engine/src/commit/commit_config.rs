// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::time::Duration;

use dexzd_log::CommitPipelineConfig;

/// Group commit, admission, and apply settings of the commit path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitConfig {
    /// Defaults to 20 ms; longest time a batch without a prompt command stays open.
    pub batch_window: Duration,
    /// Defaults to 4 MiB; a batch is sealed once its records reach this many bytes.
    pub seal_at_bytes: usize,
    /// Defaults to 10000; a batch is sealed once it holds this many records.
    pub max_batch_records: usize,
    /// Defaults to 4; most batch writes in flight to object storage at once.
    pub max_in_flight_batches: usize,
    /// Defaults to 64 MiB; encoded bytes of submitted commands that are not yet applied.
    /// Submissions beyond it fail with [`crate::CommitError::Unavailable`].
    pub max_unapplied_bytes: u32,
    /// Defaults to 10 s; deadline for writing one batch, including retries.
    pub batch_write_deadline: Duration,
    /// Defaults to 8; number of applier lanes. Partition `p` is applied by lane
    /// `p % applier_lanes`, so one partition is applied by one thread at a time.
    pub applier_lanes: usize,
    /// Defaults to 64; partition frames queued per applier lane before the commit pipeline waits.
    pub applier_queue_frames: usize,
    /// Defaults to true in debug builds and false in release builds. When true, appliers
    /// apply every command twice and compare the outcomes, and
    /// [`crate::CommandCommitter::check_speculative_outcome`] compares speculative and
    /// committed outcomes. Time-travel reconstruction checks semantic-point hashes.
    /// Any difference stops the partition.
    pub debug_replay_checks: bool,
}

impl Default for CommitConfig {
    fn default() -> Self {
        Self {
            batch_window: Duration::from_millis(20),
            seal_at_bytes: 4 * 1024 * 1024,
            max_batch_records: 10_000,
            max_in_flight_batches: 4,
            max_unapplied_bytes: 64 * 1024 * 1024,
            batch_write_deadline: Duration::from_secs(10),
            applier_lanes: 8,
            applier_queue_frames: 64,
            debug_replay_checks: cfg!(debug_assertions),
        }
    }
}

impl CommitConfig {
    pub(crate) fn pipeline_config(&self) -> CommitPipelineConfig {
        CommitPipelineConfig {
            batch_window: self.batch_window,
            target_batch_bytes: self.seal_at_bytes,
            max_batch_records: self.max_batch_records,
            max_in_flight: self.max_in_flight_batches,
            // Equal budgets keep pipeline admission from waiting: commands release
            // the engine budget only after apply, later than the pipeline's.
            max_pending_bytes: self.max_unapplied_bytes,
            admission_timeout: Duration::from_secs(1),
            head_deadline: self.batch_write_deadline,
        }
    }
}
