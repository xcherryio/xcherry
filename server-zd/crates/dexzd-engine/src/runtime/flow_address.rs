// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_keyspace::TenantId;

/// Identifies a Flow; the runtime computes its hash and partition.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FlowAddress {
    /// Tenant of the Flow.
    pub tenant_id: TenantId,
    /// Flow ID.
    pub flow_id: String,
}

impl FlowAddress {
    /// Creates an address.
    pub fn new(tenant_id: TenantId, flow_id: impl Into<String>) -> Self {
        Self {
            tenant_id,
            flow_id: flow_id.into(),
        }
    }
}
