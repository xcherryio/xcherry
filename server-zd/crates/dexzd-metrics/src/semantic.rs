// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;

use prometheus_client::encoding::{EncodeLabelSet, EncodeMetric, MetricEncoder, NoLabelSet};
use prometheus_client::metrics::MetricType;
use prometheus_client::metrics::counter::Counter;

/// A Step method's effective durability.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum StepDurability {
    /// Synchronous Step method.
    Sync,
    /// Asynchronous Step method.
    Async,
}

label_value!(StepDurability { Sync => "sync", Async => "async" });

impl StepDurability {
    const ALL: [Self; 2] = [Self::Sync, Self::Async];

    const fn index(self) -> usize {
        match self {
            Self::Sync => 0,
            Self::Async => 1,
        }
    }
}

/// A closed Step method attempt failure class.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum StepFailureKind {
    /// Worker returned an error.
    WorkerError,
    /// Method deadline expired.
    MethodTimeout,
    /// Heartbeat deadline expired.
    HeartbeatTimeout,
    /// Worker dispatch failed.
    DispatchError,
    /// Worker response failed validation.
    InvalidResponse,
    /// Worker response used an unsupported feature.
    UnsupportedFeature,
    /// Worker response value exceeded its limit.
    ValueTooLarge,
    /// Attempt request exceeded the message limit.
    RequestTooLarge,
}

label_value!(StepFailureKind { WorkerError => "worker_error", MethodTimeout => "method_timeout", HeartbeatTimeout => "heartbeat_timeout", DispatchError => "dispatch_error", InvalidResponse => "invalid_response", UnsupportedFeature => "unsupported_feature", ValueTooLarge => "value_too_large", RequestTooLarge => "request_too_large" });

impl StepFailureKind {
    const ALL: [Self; 8] = [
        Self::WorkerError,
        Self::MethodTimeout,
        Self::HeartbeatTimeout,
        Self::DispatchError,
        Self::InvalidResponse,
        Self::UnsupportedFeature,
        Self::ValueTooLarge,
        Self::RequestTooLarge,
    ];

    const fn index(self) -> usize {
        match self {
            Self::WorkerError => 0,
            Self::MethodTimeout => 1,
            Self::HeartbeatTimeout => 2,
            Self::DispatchError => 3,
            Self::InvalidResponse => 4,
            Self::UnsupportedFeature => 5,
            Self::ValueTooLarge => 6,
            Self::RequestTooLarge => 7,
        }
    }
}

/// A closed Flow run status.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum FlowCloseStatus {
    /// Completed run.
    Completed,
    /// Failed run.
    Failed,
    /// Terminated run.
    Terminated,
    /// Canceled run.
    Canceled,
}

label_value!(FlowCloseStatus { Completed => "completed", Failed => "failed", Terminated => "terminated", Canceled => "canceled" });

impl FlowCloseStatus {
    const ALL: [Self; 4] = [
        Self::Completed,
        Self::Failed,
        Self::Terminated,
        Self::Canceled,
    ];

    const fn index(self) -> usize {
        match self {
            Self::Completed => 0,
            Self::Failed => 1,
            Self::Terminated => 2,
            Self::Canceled => 3,
        }
    }
}

#[derive(Clone, Debug, EncodeLabelSet)]
struct StepFailureLabel {
    durability: StepDurability,
    failure_kind: StepFailureKind,
}

#[derive(Clone, Debug, EncodeLabelSet)]
struct StepDurabilityLabel {
    durability: StepDurability,
}

#[derive(Clone, Debug, EncodeLabelSet)]
struct FlowCloseLabel {
    close_status: FlowCloseStatus,
}

/// Failed Step method attempts by effective durability and failure class.
#[derive(Clone, Debug)]
pub struct StepAttemptFailures {
    counts: Arc<[[Counter; 8]; 2]>,
}

impl Default for StepAttemptFailures {
    fn default() -> Self {
        Self {
            counts: Arc::new(std::array::from_fn(|_| {
                std::array::from_fn(|_| Counter::default())
            })),
        }
    }
}

impl StepAttemptFailures {
    /// Records one failed, committed Step method attempt.
    pub fn record(&self, durability: StepDurability, failure_kind: StepFailureKind) {
        self.counts[durability.index()][failure_kind.index()].inc();
    }
}

impl EncodeMetric for StepAttemptFailures {
    fn encode(&self, mut encoder: MetricEncoder) -> Result<(), std::fmt::Error> {
        for durability in StepDurability::ALL {
            for failure_kind in StepFailureKind::ALL {
                let count = self.counts[durability.index()][failure_kind.index()].get();
                if count == 0 {
                    continue;
                }
                encoder
                    .encode_family(&StepFailureLabel {
                        durability,
                        failure_kind,
                    })?
                    .encode_counter::<NoLabelSet, _, u64>(&count, None)?;
            }
        }
        Ok(())
    }

    fn metric_type(&self) -> MetricType {
        MetricType::Counter
    }
}

/// Step executions that exhausted their allowed attempts.
#[derive(Clone, Debug)]
pub struct StepRetriesExhausted {
    counts: Arc<[Counter; 2]>,
}

impl Default for StepRetriesExhausted {
    fn default() -> Self {
        Self {
            counts: Arc::new(std::array::from_fn(|_| Counter::default())),
        }
    }
}

impl StepRetriesExhausted {
    /// Records one final failed Step execution.
    pub fn record(&self, durability: StepDurability) {
        self.counts[durability.index()].inc();
    }
}

impl EncodeMetric for StepRetriesExhausted {
    fn encode(&self, mut encoder: MetricEncoder) -> Result<(), std::fmt::Error> {
        for durability in StepDurability::ALL {
            encoder
                .encode_family(&StepDurabilityLabel { durability })?
                .encode_counter::<NoLabelSet, _, u64>(
                    &self.counts[durability.index()].get(),
                    None,
                )?;
        }
        Ok(())
    }

    fn metric_type(&self) -> MetricType {
        MetricType::Counter
    }
}

/// Closed Flow runs by final status.
#[derive(Clone, Debug)]
pub struct FlowCloses {
    counts: Arc<[Counter; 4]>,
}

impl Default for FlowCloses {
    fn default() -> Self {
        Self {
            counts: Arc::new(std::array::from_fn(|_| Counter::default())),
        }
    }
}

impl FlowCloses {
    /// Records one closed, committed Flow run.
    pub fn record(&self, close_status: FlowCloseStatus) {
        self.counts[close_status.index()].inc();
    }
}

impl EncodeMetric for FlowCloses {
    fn encode(&self, mut encoder: MetricEncoder) -> Result<(), std::fmt::Error> {
        for close_status in FlowCloseStatus::ALL {
            encoder
                .encode_family(&FlowCloseLabel { close_status })?
                .encode_counter::<NoLabelSet, _, u64>(
                    &self.counts[close_status.index()].get(),
                    None,
                )?;
        }
        Ok(())
    }

    fn metric_type(&self) -> MetricType {
        MetricType::Counter
    }
}
