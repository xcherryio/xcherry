// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Defaults and limits for pull Worker matching.

use std::time::Duration;

/// Matching settings corresponding to the Go Server's `matching` section.
#[derive(Debug, Clone)]
pub struct MatchingConfig {
    /// Default 10 seconds. Bounds the search for an accepting Worker.
    pub dispatch_wait: Duration,
    /// Default 5 seconds. Bounds sticky queue search before Flow type fallback.
    pub sticky_wait: Duration,
    /// Default 30 seconds. Preserves same-Flow affinity within a Flow type queue.
    pub affinity_duration: Duration,
    /// Default 20 seconds. Sends pings only to idle Worker streams.
    pub ping_interval: Duration,
    /// Default 100000. Bounds registered streams on this node.
    pub max_streams: usize,
    /// Default 256. Bounds registered streams for one app_worker_id on this node.
    pub max_concurrent_tasks_cap: usize,
    /// Default 256 bytes. Bounds each queue name.
    pub max_queue_length: usize,
    /// Default 256. Bounds Flow types in one registration.
    pub max_flow_types: usize,
    /// Default 256 bytes. Bounds each registered Flow type.
    pub max_flow_type_length: usize,
}

impl Default for MatchingConfig {
    fn default() -> Self {
        Self {
            dispatch_wait: Duration::from_secs(10),
            sticky_wait: Duration::from_secs(5),
            affinity_duration: Duration::from_secs(30),
            ping_interval: Duration::from_secs(20),
            max_streams: 100_000,
            max_concurrent_tasks_cap: 256,
            max_queue_length: 256,
            max_flow_types: 256,
            max_flow_type_length: 256,
        }
    }
}
