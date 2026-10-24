// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;

use prometheus_client::encoding::{EncodeLabelSet, EncodeMetric, MetricEncoder};
use prometheus_client::metrics::MetricType;
use prometheus_client::metrics::counter::Counter;

use crate::histogram::{API_LATENCY_BOUNDS, AtomicHistogram};

/// Fixed class of a public gRPC method.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum RpcClass {
    /// Mutating Flow call.
    FlowWrite,
    /// Reading Flow call.
    FlowRead,
    /// Search call.
    Search,
    /// Worker polling stream.
    WorkerPoll,
    /// Worker response call.
    WorkerRespond,
}

label_value!(RpcClass { FlowWrite => "flow_write", FlowRead => "flow_read", Search => "search", WorkerPoll => "worker_poll", WorkerRespond => "worker_respond" });

impl RpcClass {
    const ALL: [Self; 5] = [
        Self::FlowWrite,
        Self::FlowRead,
        Self::Search,
        Self::WorkerPoll,
        Self::WorkerRespond,
    ];

    const UNARY: [Self; 4] = [
        Self::FlowWrite,
        Self::FlowRead,
        Self::Search,
        Self::WorkerRespond,
    ];

    const fn index(self) -> usize {
        match self {
            Self::FlowWrite => 0,
            Self::FlowRead => 1,
            Self::Search => 2,
            Self::WorkerPoll => 3,
            Self::WorkerRespond => 4,
        }
    }
}

#[derive(Clone, Debug, EncodeLabelSet)]
struct RequestClassLabel {
    rpc_class: RpcClass,
}

/// Fixed unary request latency histograms by RPC class.
#[derive(Clone, Debug)]
pub struct ApiLatency {
    histograms: Arc<[AtomicHistogram<6>; 4]>,
}

impl Default for ApiLatency {
    fn default() -> Self {
        Self {
            histograms: Arc::new(std::array::from_fn(|_| {
                AtomicHistogram::new(&API_LATENCY_BOUNDS)
            })),
        }
    }
}

impl ApiLatency {
    /// Records one completed unary request; Worker poll streams are excluded.
    pub fn observe(&self, rpc_class: RpcClass, seconds: f64) {
        let index = match rpc_class {
            RpcClass::FlowWrite => 0,
            RpcClass::FlowRead => 1,
            RpcClass::Search => 2,
            RpcClass::WorkerPoll => return,
            RpcClass::WorkerRespond => 3,
        };
        self.histograms[index].observe(seconds);
    }
}

impl EncodeMetric for ApiLatency {
    fn encode(&self, mut encoder: MetricEncoder) -> Result<(), std::fmt::Error> {
        for (index, rpc_class) in RpcClass::UNARY.into_iter().enumerate() {
            self.histograms[index]
                .encode(encoder.encode_family(&RequestClassLabel { rpc_class })?)?;
        }
        Ok(())
    }

    fn metric_type(&self) -> MetricType {
        MetricType::Histogram
    }
}

/// One of the 17 gRPC status codes.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum GrpcCode {
    /// Successful completion.
    Ok,
    /// Client cancelled the call.
    Cancelled,
    /// Unknown failure.
    Unknown,
    /// Invalid input.
    InvalidArgument,
    /// Deadline elapsed.
    DeadlineExceeded,
    /// Requested object was absent.
    NotFound,
    /// Requested object already existed.
    AlreadyExists,
    /// Caller lacked permission.
    PermissionDenied,
    /// Capacity was exhausted.
    ResourceExhausted,
    /// A precondition failed.
    FailedPrecondition,
    /// Operation was aborted.
    Aborted,
    /// Value exceeded its range.
    OutOfRange,
    /// Method is not implemented.
    Unimplemented,
    /// Internal failure.
    Internal,
    /// Service was unavailable.
    Unavailable,
    /// Data was lost.
    DataLoss,
    /// Caller was not authenticated.
    Unauthenticated,
}

label_value!(GrpcCode {
    Ok => "ok", Cancelled => "cancelled", Unknown => "unknown",
    InvalidArgument => "invalid_argument", DeadlineExceeded => "deadline_exceeded",
    NotFound => "not_found", AlreadyExists => "already_exists",
    PermissionDenied => "permission_denied", ResourceExhausted => "resource_exhausted",
    FailedPrecondition => "failed_precondition", Aborted => "aborted",
    OutOfRange => "out_of_range", Unimplemented => "unimplemented",
    Internal => "internal", Unavailable => "unavailable", DataLoss => "data_loss",
    Unauthenticated => "unauthenticated",
});

impl GrpcCode {
    const ALL: [Self; 17] = [
        Self::Ok,
        Self::Cancelled,
        Self::Unknown,
        Self::InvalidArgument,
        Self::DeadlineExceeded,
        Self::NotFound,
        Self::AlreadyExists,
        Self::PermissionDenied,
        Self::ResourceExhausted,
        Self::FailedPrecondition,
        Self::Aborted,
        Self::OutOfRange,
        Self::Unimplemented,
        Self::Internal,
        Self::Unavailable,
        Self::DataLoss,
        Self::Unauthenticated,
    ];

    const fn index(self) -> usize {
        match self {
            Self::Ok => 0,
            Self::Cancelled => 1,
            Self::Unknown => 2,
            Self::InvalidArgument => 3,
            Self::DeadlineExceeded => 4,
            Self::NotFound => 5,
            Self::AlreadyExists => 6,
            Self::PermissionDenied => 7,
            Self::ResourceExhausted => 8,
            Self::FailedPrecondition => 9,
            Self::Aborted => 10,
            Self::OutOfRange => 11,
            Self::Unimplemented => 12,
            Self::Internal => 13,
            Self::Unavailable => 14,
            Self::DataLoss => 15,
            Self::Unauthenticated => 16,
        }
    }

    /// Converts the wire status number into the closed gRPC code set.
    pub const fn from_number(number: u8) -> Self {
        match number {
            0 => Self::Ok,
            1 => Self::Cancelled,
            2 => Self::Unknown,
            3 => Self::InvalidArgument,
            4 => Self::DeadlineExceeded,
            5 => Self::NotFound,
            6 => Self::AlreadyExists,
            7 => Self::PermissionDenied,
            8 => Self::ResourceExhausted,
            9 => Self::FailedPrecondition,
            10 => Self::Aborted,
            11 => Self::OutOfRange,
            12 => Self::Unimplemented,
            13 => Self::Internal,
            14 => Self::Unavailable,
            15 => Self::DataLoss,
            16 => Self::Unauthenticated,
            _ => Self::Unknown,
        }
    }
}

/// Maps all public FlowService and WorkerTaskService methods to one fixed class.
pub fn classify_rpc(path: &str) -> Option<RpcClass> {
    match path {
        "/dex.FlowService/GetServerInfo" => Some(RpcClass::FlowRead),
        "/dex.FlowService/StartFlow" => Some(RpcClass::FlowWrite),
        "/dex.FlowService/PublishToChannel" => Some(RpcClass::FlowWrite),
        "/dex.FlowService/GetChannelMessages" => Some(RpcClass::FlowRead),
        "/dex.FlowService/DeleteChannelMessage" => Some(RpcClass::FlowWrite),
        "/dex.FlowService/WriteStream" => Some(RpcClass::FlowWrite),
        "/dex.FlowService/ReadStream" => Some(RpcClass::FlowRead),
        "/dex.FlowService/ListStreamMessages" => Some(RpcClass::FlowRead),
        "/dex.FlowService/StopFlow" => Some(RpcClass::FlowWrite),
        "/dex.FlowService/GetAttributes" => Some(RpcClass::FlowRead),
        "/dex.FlowService/SetAttributes" => Some(RpcClass::FlowWrite),
        "/dex.FlowService/LoadBlobs" => Some(RpcClass::FlowRead),
        "/dex.FlowService/WaitForFlow" => Some(RpcClass::FlowRead),
        "/dex.FlowService/SearchFlows" => Some(RpcClass::Search),
        "/dex.FlowService/SyncAttributeIndexes" => Some(RpcClass::FlowWrite),
        "/dex.FlowService/GetFlowSummary" => Some(RpcClass::FlowRead),
        "/dex.FlowService/GetHistoryEvents" => Some(RpcClass::FlowRead),
        "/dex.FlowService/WaitForHistoryEvent" => Some(RpcClass::FlowRead),
        "/dex.FlowService/GetFlowState" => Some(RpcClass::FlowRead),
        "/dex.FlowService/ResetFlow" => Some(RpcClass::FlowWrite),
        "/dex.FlowService/InvokeRPC" => Some(RpcClass::FlowWrite),
        "/dex.FlowService/SkipTimer" => Some(RpcClass::FlowWrite),
        "/dex.FlowService/UpdateFlowConfig" => Some(RpcClass::FlowWrite),
        "/dex.FlowService/WaitForStepCompletion" => Some(RpcClass::FlowRead),
        "/dex.FlowService/WaitForAttribute" => Some(RpcClass::FlowRead),
        "/dex.FlowService/TriggerContinueAsNew" => Some(RpcClass::FlowWrite),
        "/dex.FlowService/HealthCheck" => None,
        "/dex.WorkerTaskService/PollWorkerTask" => Some(RpcClass::WorkerPoll),
        _ => None,
    }
}

#[derive(Clone, Debug, EncodeLabelSet)]
struct RequestLabel {
    rpc_class: RpcClass,
    code: GrpcCode,
}

/// Fixed atomic public-request counters, emitting only observed label pairs.
#[derive(Clone, Debug)]
pub struct ApiRequests {
    counts: Arc<[[Counter; 17]; 5]>,
}

impl Default for ApiRequests {
    fn default() -> Self {
        Self {
            counts: Arc::new(std::array::from_fn(|_| {
                std::array::from_fn(|_| Counter::default())
            })),
        }
    }
}

impl ApiRequests {
    /// Counts one completed public request without allocating or locking.
    pub fn record(&self, rpc_class: RpcClass, code: GrpcCode) {
        self.counts[rpc_class.index()][code.index()].inc();
    }
}

impl EncodeMetric for ApiRequests {
    fn encode(&self, mut encoder: MetricEncoder) -> Result<(), std::fmt::Error> {
        for rpc_class in RpcClass::ALL {
            for code in GrpcCode::ALL {
                let count = self.counts[rpc_class.index()][code.index()].get();
                if count == 0 && !(rpc_class == RpcClass::FlowWrite && code == GrpcCode::Ok) {
                    continue;
                }
                encoder.encode_family(&RequestLabel { rpc_class, code })?
                    .encode_counter::<prometheus_client::encoding::NoLabelSet, _, u64>(&count, None)?;
            }
        }
        Ok(())
    }

    fn metric_type(&self) -> MetricType {
        MetricType::Counter
    }
}

#[cfg(test)]
mod tests {
    use super::classify_rpc;

    #[test]
    fn every_public_proto_rpc_is_classified() {
        let proto = include_str!("../../../../protos/dex.proto");
        let mut service = "";
        for line in proto.lines() {
            let line = line.trim();
            if line.starts_with("service FlowService {") {
                service = "FlowService";
            } else if line.starts_with("service WorkerTaskService {") {
                service = "WorkerTaskService";
            } else if line == "}" {
                service = "";
            } else if !service.is_empty()
                && let Some(method) = line.strip_prefix("rpc ")
            {
                let method = method.split('(').next().expect("RPC has a name");
                let path = format!("/dex.{service}/{method}");
                assert!(
                    classify_rpc(&path).is_some() || path == "/dex.FlowService/HealthCheck",
                    "unmapped RPC: {path}"
                );
            }
        }
    }
}
