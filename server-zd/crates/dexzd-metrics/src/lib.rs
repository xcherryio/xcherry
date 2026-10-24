// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Own the fixed, process-local OpenMetrics registry and typed recording handles for a server-zd node.

macro_rules! label_value {
    ($name:ident { $($variant:ident => $value:expr),+ $(,)? }) => {
        impl prometheus_client::encoding::EncodeLabelValue for $name {
            fn encode(&self, encoder: &mut prometheus_client::encoding::LabelValueEncoder) -> Result<(), std::fmt::Error> {
                let value = match self { $(Self::$variant => $value,)+ };
                prometheus_client::encoding::EncodeLabelValue::encode(&value, encoder)
            }
        }
    };
}

mod api_requests;
mod commit_latency;
mod fixed_labels;
mod histogram;
mod object_requests;
#[cfg(target_os = "linux")]
mod process;
mod semantic;
mod timer_lag;
mod worker_stream;

use std::path::PathBuf;

pub use api_requests::{ApiLatency, ApiRequests, GrpcCode, RpcClass, classify_rpc};
pub use commit_latency::CommitLatency;
pub use fixed_labels::{ObjectErrorCounters, OutcomeCounters, RoleGauges};
pub use histogram::AtomicHistogram;
use histogram::{FLOW_LATENCY_BOUNDS, SCHEDULE_LATENCY_BOUNDS};
pub use object_requests::{ObjectKind, ObjectOperation, ObjectRequests, classify_object_kind};
use prometheus_client::encoding::{EncodeLabelSet, text::encode};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
pub use semantic::{
    FlowCloseStatus, FlowCloses, StepAttemptFailures, StepDurability, StepFailureKind,
    StepRetriesExhausted,
};
pub use timer_lag::TimerLag;
pub use worker_stream::{WorkerStreamEvent, WorkerStreamEvents};

/// The node's fixed set of metric handles.
#[derive(Clone, Debug)]
pub struct NodeMetrics {
    /// Current behavior barrier.
    pub behavior_version: Gauge,
    /// Current node role, with one series for each closed role.
    pub node_roles: RoleGauges,
    /// Epoch held by this node, zero when it holds none.
    pub lease_epoch: Gauge,
    /// Lease renewal attempts.
    pub lease_renewals: OutcomeCounters,
    /// Public gRPC request completions.
    pub api_requests: ApiRequests,
    /// Completed unary gRPC request latency.
    pub api_latency: ApiLatency,
    /// Durably committed batches.
    pub commit_batches: Counter,
    /// Durably committed commands.
    pub commit_records: Counter,
    /// Durably committed encoded batch bytes.
    pub commit_bytes: Counter,
    /// Time from batch seal to durable acknowledgement.
    pub commit_latency: CommitLatency,
    /// Object storage requests, including failed requests.
    pub object_requests: ObjectRequests,
    /// Failed object storage requests after SDK retries.
    pub object_errors: ObjectErrorCounters,
    /// Uploaded object bytes.
    pub object_put_bytes: Counter,
    /// Snapshot attempts.
    pub snapshots: OutcomeCounters,
    /// Duration of the last successful snapshot.
    pub snapshot_last_duration: Gauge<f64, std::sync::atomic::AtomicU64>,
    /// Bytes in the last successful snapshot.
    pub snapshot_last_bytes: Gauge,
    /// Duration of the last completed recovery or replay.
    pub recovery_last_duration: Gauge<f64, std::sync::atomic::AtomicU64>,
    /// Uncompressed command record bytes replayed during the last completed recovery.
    pub recovery_last_replayed_bytes: Gauge,
    /// Local SST and WAL bytes, updated before encoding.
    pub local_storage_bytes: Gauge,
    /// Age of the oldest due timer that has not fired.
    pub timer_lag: TimerLag,
    /// Worker tasks waiting for a poll.
    pub matching_queued_tasks: Gauge,
    /// Open Worker poll streams.
    pub matching_worker_streams: Gauge,
    /// Failed Step method attempts.
    pub step_attempt_failures: StepAttemptFailures,
    /// Step executions that exhausted their retries.
    pub step_retries_exhausted: StepRetriesExhausted,
    /// Closed Flow runs.
    pub flow_closes: FlowCloses,
    /// Run start to close latency.
    pub flow_end_to_end_latency: AtomicHistogram<8>,
    /// Committed Step task to Worker delivery latency.
    pub step_schedule_to_start_latency: AtomicHistogram<7>,
    /// Task deliveries and persistent Worker stream closes.
    pub worker_stream_events: WorkerStreamEvents,
}

/// A lifecycle role for one node.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum NodeRole {
    /// Before lease acquisition.
    Starting,
    /// Waiting for a lease.
    Standby,
    /// Replaying state under a lease.
    Recovering,
    /// Accepting public requests.
    Serving,
    /// Stopping listeners and releasing ownership.
    Draining,
}

label_value!(NodeRole { Starting => "starting", Standby => "standby", Recovering => "recovering", Serving => "serving", Draining => "draining" });

impl NodeRole {
    const ALL: [Self; 5] = [
        Self::Starting,
        Self::Standby,
        Self::Recovering,
        Self::Serving,
        Self::Draining,
    ];
}

/// A closed success or failure label.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum Outcome {
    /// Successful attempt.
    Ok,
    /// Failed attempt.
    Error,
}

label_value!(Outcome { Ok => "ok", Error => "error" });

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct OutcomeLabel {
    outcome: Outcome,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RoleLabel {
    role: NodeRole,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ObjectOperationLabel {
    operation: ObjectOperation,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct BuildLabel {
    version: BuildVersion,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum BuildVersion {
    Current,
}

label_value!(BuildVersion { Current => env!("CARGO_PKG_VERSION") });

impl NodeMetrics {
    /// Creates a registry and all handles for one process-local node.
    pub fn registered(local_store_directory: PathBuf, partition_count: usize) -> (Registry, Self) {
        let mut registry = Registry::default();
        let metrics = Self::with_partitions(partition_count);
        let build_info = Family::<BuildLabel, Gauge>::default();
        build_info
            .get_or_create(&BuildLabel {
                version: BuildVersion::Current,
            })
            .set(1);
        registry.register("dexzd_build_info", "Binary version.", build_info);
        registry.register(
            "dexzd_behavior_version",
            "Cluster behavior barrier.",
            metrics.behavior_version.clone(),
        );
        registry.register(
            "dexzd_node_role",
            "Current node lifecycle role.",
            metrics.node_roles.clone(),
        );
        registry.register(
            "dexzd_lease_epoch",
            "Held lease epoch.",
            metrics.lease_epoch.clone(),
        );
        registry.register(
            "dexzd_lease_renewals",
            "Lease renewal attempts.",
            metrics.lease_renewals.clone(),
        );
        registry.register(
            "dexzd_api_requests",
            "Completed public API requests.",
            metrics.api_requests.clone(),
        );
        registry.register(
            "dexzd_api_request_latency_seconds",
            "Completed unary public API request latency.",
            metrics.api_latency.clone(),
        );
        registry.register(
            "dexzd_commit_batches",
            "Durably committed batches.",
            metrics.commit_batches.clone(),
        );
        registry.register(
            "dexzd_commit_records",
            "Committed commands.",
            metrics.commit_records.clone(),
        );
        registry.register(
            "dexzd_commit_bytes",
            "Committed encoded batch bytes.",
            metrics.commit_bytes.clone(),
        );
        registry.register(
            "dexzd_commit_latency_seconds",
            "Time from batch seal to durable acknowledgement.",
            metrics.commit_latency.clone(),
        );
        registry.register(
            "dexzd_object_store_requests",
            "Object storage requests.",
            metrics.object_requests.clone(),
        );
        registry.register(
            "dexzd_object_store_request_errors",
            "Failed object storage requests.",
            metrics.object_errors.clone(),
        );
        registry.register(
            "dexzd_object_store_put_bytes",
            "Uploaded object bytes.",
            metrics.object_put_bytes.clone(),
        );
        registry.register(
            "dexzd_snapshots",
            "Snapshot attempts.",
            metrics.snapshots.clone(),
        );
        registry.register(
            "dexzd_snapshot_last_duration_seconds",
            "Last successful snapshot duration.",
            metrics.snapshot_last_duration.clone(),
        );
        registry.register(
            "dexzd_snapshot_last_bytes",
            "Last successful snapshot bytes.",
            metrics.snapshot_last_bytes.clone(),
        );
        registry.register(
            "dexzd_recovery_last_duration_seconds",
            "Last completed recovery duration.",
            metrics.recovery_last_duration.clone(),
        );
        registry.register(
            "dexzd_recovery_last_replayed_bytes",
            "Uncompressed command record bytes replayed by the last recovery.",
            metrics.recovery_last_replayed_bytes.clone(),
        );
        registry.register(
            "dexzd_timer_lag_seconds",
            "Age of oldest unfired due timer.",
            metrics.timer_lag.clone(),
        );
        registry.register(
            "dexzd_matching_queued_tasks",
            "Worker tasks waiting for a poll.",
            metrics.matching_queued_tasks.clone(),
        );
        registry.register(
            "dexzd_matching_worker_streams",
            "Open Worker poll streams.",
            metrics.matching_worker_streams.clone(),
        );
        registry.register(
            "dexzd_step_attempt_failures",
            "Failed Step method attempts.",
            metrics.step_attempt_failures.clone(),
        );
        registry.register(
            "dexzd_step_retries_exhausted",
            "Step executions that exhausted their attempts.",
            metrics.step_retries_exhausted.clone(),
        );
        registry.register(
            "dexzd_flow_closes",
            "Closed Flow runs.",
            metrics.flow_closes.clone(),
        );
        registry.register(
            "dexzd_flow_end_to_end_latency_seconds",
            "Committed Flow run start to close latency.",
            metrics.flow_end_to_end_latency.clone(),
        );
        registry.register(
            "dexzd_step_schedule_to_start_latency_seconds",
            "Committed Step task to Worker delivery latency.",
            metrics.step_schedule_to_start_latency.clone(),
        );
        registry.register(
            "dexzd_worker_stream_events",
            "Delivered Worker tasks and closed Worker streams.",
            metrics.worker_stream_events.clone(),
        );
        registry.register_collector(Box::new(LocalStorageCollector {
            directory: local_store_directory,
            gauge: metrics.local_storage_bytes.clone(),
        }));
        #[cfg(target_os = "linux")]
        registry.register_collector(Box::new(process::ProcessCollector::new()));
        (registry, metrics)
    }

    /// Creates unregistered handles for tests and simulations.
    pub fn unregistered() -> Self {
        Self::with_partitions(0)
    }

    fn with_partitions(partition_count: usize) -> Self {
        Self {
            behavior_version: Gauge::default(),
            node_roles: RoleGauges::default(),
            lease_epoch: Gauge::default(),
            lease_renewals: OutcomeCounters::default(),
            api_requests: ApiRequests::default(),
            api_latency: ApiLatency::default(),
            commit_batches: Counter::default(),
            commit_records: Counter::default(),
            commit_bytes: Counter::default(),
            commit_latency: CommitLatency::default(),
            object_requests: ObjectRequests::default(),
            object_errors: ObjectErrorCounters::default(),
            object_put_bytes: Counter::default(),
            snapshots: OutcomeCounters::default(),
            snapshot_last_duration: Gauge::default(),
            snapshot_last_bytes: Gauge::default(),
            recovery_last_duration: Gauge::default(),
            recovery_last_replayed_bytes: Gauge::default(),
            local_storage_bytes: Gauge::default(),
            timer_lag: TimerLag::new(partition_count),
            matching_queued_tasks: Gauge::default(),
            matching_worker_streams: Gauge::default(),
            step_attempt_failures: StepAttemptFailures::default(),
            step_retries_exhausted: StepRetriesExhausted::default(),
            flow_closes: FlowCloses::default(),
            flow_end_to_end_latency: AtomicHistogram::new(&FLOW_LATENCY_BOUNDS),
            step_schedule_to_start_latency: AtomicHistogram::new(&SCHEDULE_LATENCY_BOUNDS),
            worker_stream_events: WorkerStreamEvents::default(),
        }
    }

    /// Changes the active lifecycle role without exposing dynamic labels.
    pub fn set_role(&self, current_role: NodeRole) {
        self.node_roles.set(current_role);
    }

    /// Records one lease renewal outcome.
    pub fn record_lease_renewal(&self, outcome: Outcome) {
        self.lease_renewals.record(outcome);
    }

    /// Records one snapshot outcome.
    pub fn record_snapshot(&self, outcome: Outcome) {
        self.snapshots.record(outcome);
    }

    /// Records a failed object request.
    pub fn record_object_error(&self, operation: ObjectOperation) {
        self.object_errors.record(operation);
    }
}

#[derive(Debug)]
struct LocalStorageCollector {
    directory: PathBuf,
    gauge: Gauge,
}

impl prometheus_client::collector::Collector for LocalStorageCollector {
    fn encode(
        &self,
        mut encoder: prometheus_client::encoding::DescriptorEncoder,
    ) -> Result<(), std::fmt::Error> {
        match local_storage_bytes(&self.directory) {
            Ok(bytes) => {
                self.gauge.set(i64::try_from(bytes).unwrap_or(i64::MAX));
            }
            Err(error) => tracing::warn!(%error, "could not sample local storage bytes"),
        }
        let metric_encoder = encoder.encode_descriptor(
            "dexzd_local_storage_bytes",
            "Local SST and WAL bytes.",
            None,
            prometheus_client::metrics::MetricType::Gauge,
        )?;
        prometheus_client::encoding::EncodeMetric::encode(&self.gauge, metric_encoder)
    }
}

fn local_storage_bytes(directory: &std::path::Path) -> std::io::Result<u64> {
    let mut size = 0_u64;
    let mut pending = vec![directory.to_path_buf()];
    while let Some(path) = pending.pop() {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                pending.push(entry.path());
            } else if matches!(
                entry
                    .path()
                    .extension()
                    .and_then(|extension| extension.to_str()),
                Some("sst" | "log")
            ) {
                size = size.saturating_add(entry.metadata()?.len());
            }
        }
    }
    Ok(size)
}

/// Encodes one process-local registry in OpenMetrics text format.
pub fn encode_registry(registry: &Registry) -> Result<String, std::fmt::Error> {
    let mut output = String::new();
    encode(&mut output, registry)?;
    Ok(output)
}
