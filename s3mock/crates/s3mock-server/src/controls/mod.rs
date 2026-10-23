// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Deterministic fault selection and bounded request observations shared by all connections.

mod admin;
mod observation;
mod operation;

use operation::S3OperationName;

use std::collections::{BTreeMap, VecDeque};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::config::ServerConfig;
use crate::s3_error::S3Error;

pub(crate) use observation::{ObservedBody, RequestObservation};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FaultRule {
    pub(crate) operation: Option<S3OperationName>,
    pub(crate) bucket: Option<String>,
    pub(crate) key_prefix: Option<String>,
    pub(crate) has_conditional_header: Option<bool>,
    #[serde(default = "default_probability")]
    pub(crate) probability: f64,
    pub(crate) action: FaultAction,
}

fn default_probability() -> f64 {
    1.0
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum FaultAction {
    CommitThenDrop {},
    Delay { milliseconds: u64 },
    Error { status: u16 },
    FailBeforeCommit {},
    CorruptBody {},
    TruncateResponse {},
}

impl FaultAction {
    pub(crate) fn error(&self) -> Option<S3Error> {
        let status = match self {
            Self::Error { status } => *status,
            Self::FailBeforeCommit {} => 500,
            _ => return None,
        };
        let (status, code) = match status {
            409 => (http::StatusCode::CONFLICT, "ConditionalRequestConflict"),
            503 => (http::StatusCode::SERVICE_UNAVAILABLE, "SlowDown"),
            _ => (http::StatusCode::INTERNAL_SERVER_ERROR, "InternalError"),
        };
        Some(S3Error::new(status, code, "injected testing fault"))
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct RequestCounter {
    requests: u64,
    bytes_received: u64,
    bytes_sent: u64,
}

#[derive(Default, Serialize)]
pub(crate) struct Counters {
    by_operation: BTreeMap<S3OperationName, RequestCounter>,
    by_prefix: BTreeMap<String, BTreeMap<String, RequestCounter>>,
}

pub(crate) struct Controls {
    pub(crate) config: ServerConfig,
    state: Mutex<ControlState>,
    pub(crate) fault_reset: tokio::sync::watch::Sender<u64>,
}

struct ControlState {
    rules: Vec<FaultRule>,
    random_state: u64,
    counters: Counters,
    requests: VecDeque<RequestObservation>,
}

impl Controls {
    pub(crate) fn new(config: ServerConfig) -> Self {
        Self {
            state: Mutex::new(ControlState {
                rules: Vec::new(),
                random_state: config.fault_seed,
                counters: Counters::default(),
                requests: VecDeque::new(),
            }),
            fault_reset: tokio::sync::watch::channel(0).0,
            config,
        }
    }

    pub(crate) fn select_fault(&self, request: &RequestObservation) -> Option<FaultAction> {
        let mut state = self.state.lock().expect("control operations cannot panic");
        for index in 0..state.rules.len() {
            let rule = &state.rules[index];
            if rule
                .operation
                .as_ref()
                .is_some_and(|value| value != &request.operation)
                || rule
                    .bucket
                    .as_ref()
                    .is_some_and(|value| value != &request.bucket)
                || rule
                    .key_prefix
                    .as_ref()
                    .is_some_and(|value| !request.key.starts_with(value))
                || rule
                    .has_conditional_header
                    .is_some_and(|value| value != request.has_conditional_header())
            {
                continue;
            }
            let probability = rule.probability;
            // SplitMix64 includes seed zero and advances once per matching rule.
            state.random_state = state.random_state.wrapping_add(0x9e3779b97f4a7c15);
            let mut random = state.random_state;
            random = (random ^ (random >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            random = (random ^ (random >> 27)).wrapping_mul(0x94d049bb133111eb);
            random ^= random >> 31;
            let sample = ((random >> 11) as f64) / ((1_u64 << 53) as f64);
            if sample < probability {
                return Some(state.rules[index].action.clone());
            }
        }
        None
    }

    pub(crate) fn record(&self, request: RequestObservation) {
        tracing::info!(
            operation = %request.operation, bucket = %request.bucket, key = %request.key,
            if_match = ?request.if_match, if_none_match = ?request.if_none_match,
            if_modified_since = ?request.if_modified_since, if_unmodified_since = ?request.if_unmodified_since,
            status = request.status, duration_ms = request.duration_ms,
            bytes_received = request.bytes_received, bytes_sent = request.bytes_sent,
            injected_fault = ?request.injected_fault, "S3 request"
        );
        let mut prefix = request
            .key
            .split('/')
            .take(self.config.counter_prefix_depth)
            .collect::<Vec<_>>()
            .join("/");
        let mut state = self.state.lock().expect("control operations cannot panic");
        add_counter(
            state
                .counters
                .by_operation
                .entry(request.operation)
                .or_default(),
            &request,
        );
        let group_count: usize = state.counters.by_prefix.values().map(BTreeMap::len).sum();
        let bucket = if group_count >= 1024
            && !state
                .counters
                .by_prefix
                .get(&request.bucket)
                .is_some_and(|groups| groups.contains_key(&prefix))
        {
            prefix = "<overflow>".into();
            "<overflow>".to_owned()
        } else {
            request.bucket.clone()
        };
        if request.has_existing_bucket && request.operation != S3OperationName::Unsupported {
            add_counter(
                state
                    .counters
                    .by_prefix
                    .entry(bucket)
                    .or_default()
                    .entry(prefix)
                    .or_default(),
                &request,
            );
        }
        if self.config.request_log_capacity > 0 {
            if state.requests.len() == self.config.request_log_capacity {
                state.requests.pop_front();
            }
            state.requests.push_back(request);
        }
    }
}

fn add_counter(counter: &mut RequestCounter, request: &RequestObservation) {
    counter.requests += 1;
    counter.bytes_received += request.bytes_received;
    counter.bytes_sent += request.bytes_sent;
}
