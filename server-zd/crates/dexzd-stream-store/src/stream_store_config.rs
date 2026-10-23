// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

/// Configuration for one in-memory Stream Store.
#[derive(Clone, Debug)]
pub struct StreamStoreConfig {
    /// Enables writes and reads. Default false, matching the production Go Server.
    pub enabled: bool,
    /// Supplies capacity when a write has no explicit Stream capacity. Default 10 MiB.
    pub capacity_per_stream_bytes: u64,
    /// Limits the serialized Value size. Default 100 KiB.
    pub max_message_bytes: usize,
    /// Limits one reverse-list page. Default 1000.
    pub max_read_messages: usize,
    /// Starts background trimming at this percentage. Default 90.
    pub trim_trigger_percent: u8,
    /// Stops background trimming at this percentage. Default 80.
    pub trim_target_percent: u8,
}

impl Default for StreamStoreConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            capacity_per_stream_bytes: 10 * 1024 * 1024,
            max_message_bytes: 100 * 1024,
            max_read_messages: 1000,
            trim_trigger_percent: 90,
            trim_target_percent: 80,
        }
    }
}

impl StreamStoreConfig {
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if self.capacity_per_stream_bytes == 0 {
            return Err("capacity_per_stream_bytes must be positive");
        }
        if self.max_message_bytes == 0 {
            return Err("max_message_bytes must be positive");
        }
        if !(1..=1000).contains(&self.max_read_messages) {
            return Err("max_read_messages must be between 1 and 1000");
        }
        if !(1..=99).contains(&self.trim_trigger_percent) {
            return Err("trim_trigger_percent must be between 1 and 99");
        }
        if self.trim_target_percent == 0 || self.trim_target_percent >= self.trim_trigger_percent {
            return Err("trim_target_percent must be positive and below trim_trigger_percent");
        }
        Ok(())
    }
}
