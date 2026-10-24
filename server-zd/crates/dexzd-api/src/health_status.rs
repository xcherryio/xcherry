// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use tonic_health::ServingStatus;
use tonic_health::pb::health_server::{Health, HealthServer};
use tonic_health::server::HealthReporter;

/// Service names that `grpc.health.v1` reports; `dexcli dev` waits until all are SERVING.
pub const HEALTH_SERVICE_NAMES: [&str; 3] = ["", "dex.FlowService", "dex.WorkerTaskService"];

/// Controls the `grpc.health.v1` status of every Dex service name together.
#[derive(Clone)]
pub struct HealthStatus {
    reporter: HealthReporter,
}

impl HealthStatus {
    /// Creates the health service with every name NOT_SERVING.
    pub async fn new() -> (Self, HealthServer<impl Health>) {
        let (reporter, server) = tonic_health::server::health_reporter();
        let status = Self { reporter };
        status.set(ServingStatus::NotServing).await;
        (status, server)
    }

    /// Reports SERVING for every name.
    pub async fn set_serving(&self) {
        self.set(ServingStatus::Serving).await;
    }

    /// Reports NOT_SERVING for every name, for example during shutdown.
    pub async fn set_not_serving(&self) {
        self.set(ServingStatus::NotServing).await;
    }

    async fn set(&self, status: ServingStatus) {
        for service_name in HEALTH_SERVICE_NAMES {
            self.reporter.set_service_status(service_name, status).await;
        }
    }
}
