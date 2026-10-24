// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::time::Duration;

/// Snapshot schedule, retention, and delayed log deletion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotConfig {
    /// Defaults to 256 MiB; a snapshot starts after this many committed record bytes.
    pub log_threshold_bytes: u64,
    /// Defaults to 10 min; a snapshot starts when this long has passed since the last one.
    pub interval: Duration,
    /// Defaults to 1 s; how often the schedule is checked.
    pub check_interval: Duration,
    /// Defaults to 30 s; wait after a failed snapshot before the next attempt.
    pub retry_delay: Duration,
    /// Defaults to 3; complete snapshot manifests kept across every lineage of this node. The
    /// snapshot the manifest points to is always kept.
    pub retained_count: usize,
    /// Defaults to 24 h. Log batches are deleted only when a snapshot at least this old covers
    /// them, and older snapshots are deleted only this long after a newer one exists.
    pub deletion_delay: Duration,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            log_threshold_bytes: 256 * 1024 * 1024,
            interval: Duration::from_secs(10 * 60),
            check_interval: Duration::from_secs(1),
            retry_delay: Duration::from_secs(30),
            retained_count: 3,
            deletion_delay: Duration::from_secs(24 * 60 * 60),
        }
    }
}
