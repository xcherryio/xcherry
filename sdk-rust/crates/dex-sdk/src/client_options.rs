// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use crate::WorkerTarget;

#[derive(Clone, Debug)]
/// Configures how a [`crate::Client`] reaches Dex and identifies application workers.
///
/// [`ClientOptions::new`] connects to `127.0.0.1:8801` over plaintext gRPC and supplies no default
/// worker target. A per-Flow [`crate::FlowConfig`] can override the target.
pub struct ClientOptions {
    server_address: String,
    worker_target: Option<WorkerTarget>,
}

impl ClientOptions {
    /// Creates options with the local Dex server default and no worker target.
    pub fn new() -> Self {
        Self {
            server_address: "127.0.0.1:8801".to_string(),
            worker_target: None,
        }
    }

    /// Sets the plaintext gRPC `host:port` used for every client request.
    pub fn server_address(mut self, value: impl Into<String>) -> Self {
        self.server_address = value.into();
        self
    }

    /// Sets the Worker target advertised by newly started Flows.
    ///
    /// Use [`WorkerTarget::pull`](crate::WorkerTarget::pull) with a pull Worker. An invalid pull
    /// combination returns [`crate::SdkError::FlowDefinition`] from [`crate::Client::try_new`].
    pub fn worker_target(mut self, value: WorkerTarget) -> Self {
        self.worker_target = Some(value);
        self
    }

    pub(crate) fn server_address_value(&self) -> &str {
        &self.server_address
    }

    pub(crate) fn worker_target_value(&self) -> Option<&WorkerTarget> {
        self.worker_target.as_ref()
    }
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self::new()
    }
}
