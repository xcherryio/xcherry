// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::time::Duration;

/// Settings of the FlowService endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowServiceConfig {
    /// Defaults to this crate's package version; reported as `ServerInfo.server_version`.
    pub server_version: String,
    /// Defaults to `dev`; reported as `HealthInfo.hostname`. The server passes its node ID.
    pub node_name: String,
    /// Defaults to 60 seconds, `api.maxWaitSeconds`; the longest ReadStream wait, also for a
    /// request that asks for 0.
    pub max_wait: Duration,
    /// Defaults to 1000; SearchFlows page size when the request sets 0.
    pub default_search_page_size: usize,
}

impl Default for FlowServiceConfig {
    fn default() -> Self {
        Self {
            server_version: env!("CARGO_PKG_VERSION").to_owned(),
            node_name: "dev".to_owned(),
            max_wait: Duration::from_secs(60),
            default_search_page_size: 1000,
        }
    }
}
