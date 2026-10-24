// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::time::{Duration, SystemTime};

use dexzd_protocol::dex::{
    InvokeExecuteMethodRequest, InvokeWaitForMethodRequest, InvokeWorkerRpcRequest,
};

/// The unmodified Worker request carried by a pull task.
#[derive(Debug, Clone)]
pub enum DispatchTaskPayload {
    /// Runs a Step's WaitFor method.
    WaitFor(InvokeWaitForMethodRequest),
    /// Runs a Step's Execute method.
    Execute(InvokeExecuteMethodRequest),
    /// Invokes a Worker RPC.
    Rpc(InvokeWorkerRpcRequest),
}

/// Inputs for one server-generated pull task.
#[derive(Debug, Clone)]
pub struct DispatchRequest {
    /// Tenant containing the Flow and its Worker queues; use zero for the default tenant.
    pub tenant_id: u64,
    /// Flow instance ID used for same-Flow affinity.
    pub flow_id: String,
    /// Flow type queue used after an optional sticky Worker search.
    pub flow_type: String,
    /// Worker process ID preferred while the sticky wait lasts.
    pub sticky_app_worker_id: Option<String>,
    /// Worker request sent without modifying its application payload.
    pub payload: DispatchTaskPayload,
    /// Method deadline, including Worker processing time.
    pub deadline: SystemTime,
    /// Override for the 5-second sticky wait; zero skips the sticky search.
    pub sticky_wait: Option<Duration>,
    /// Override for the 10-second total Worker search.
    pub dispatch_wait: Option<Duration>,
}
