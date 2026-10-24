// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_keyspace::{FlowHash, PartitionId};

use crate::ApplyError;

/// Facts about where a command is applied that are not part of the command itself.
///
/// Both values are derived from the command's (tenant ID, Flow ID) and the cluster object, so
/// they are the same on every replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApplyContext {
    /// Partition of the command's Flow.
    pub partition: PartitionId,
    /// Keyed hash of the command's (tenant ID, Flow ID); prefixes every key of the Flow.
    pub flow_hash: FlowHash,
}

/// Version of apply behavior that a command was proposed with.
///
/// Every change to the result of applying an already committed command adds a version and a
/// named predicate method here; apply branches on predicates, never on raw numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct BehaviorVersion(u32);

impl BehaviorVersion {
    /// The first behavior version.
    pub const INITIAL: Self = Self(1);
    /// Oldest version this binary can apply.
    pub const MINIMUM_SUPPORTED: Self = Self::INITIAL;
    /// Newest version this binary can apply and propose.
    pub const CURRENT: Self = Self::INITIAL;

    /// Validates the version recorded in a command.
    ///
    /// Returns [`ApplyError::UnsupportedBehaviorVersion`] when this binary cannot apply it; the
    /// partition must stop instead of guessing.
    pub fn from_command(version: u32) -> Result<Self, ApplyError> {
        let version = Self(version);
        if version < Self::MINIMUM_SUPPORTED || version > Self::CURRENT {
            return Err(ApplyError::UnsupportedBehaviorVersion { version: version.0 });
        }
        Ok(version)
    }

    /// Returns the numeric version.
    pub fn number(self) -> u32 {
        self.0
    }
}
