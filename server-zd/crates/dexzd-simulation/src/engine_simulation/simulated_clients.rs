// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Seeded FlowService clients that retry with their request IDs and check what they read.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dexzd_api::{FlowOperationError, FlowOperations};
use dexzd_engine::FlowRuntimeRegistry;
use dexzd_keyspace::TenantId;
use dexzd_protocol::dex::{GetAttributesRequest, GetChannelMessagesRequest};
use prost::Message;
use rand::{RngExt, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::engine_simulation::committed_log::CommittedLog;
use crate::engine_simulation::event_trace::EventTrace;
use crate::engine_simulation::workload::{
    OPERATION_MARKER_PREFIX, delete_message_request, publish_request, rpc_request,
    set_attributes_request, start_flow_request, stop_request,
};
use crate::invariants::InvariantViolation;
use crate::model::CHANNEL_NAMES;

/// Flow IDs the clients use.
pub(crate) const ENGINE_FLOW_IDS: &[&str] = &["order-1", "order-2", "order-3"];
/// Longest time a client waits for one call before it treats the outcome as unknown.
const CALL_TIMEOUT: Duration = Duration::from_secs(15);
/// Calls of one operation, including retries with the same request ID.
const MAX_CALLS_PER_OPERATION: u32 = 4;

/// The node process that currently serves clients.
#[derive(Clone, Default)]
pub(crate) struct ServingNode {
    serving: Arc<Mutex<Option<(String, FlowRuntimeRegistry)>>>,
}

impl ServingNode {
    pub(crate) fn serve(&self, process_label: &str, registry: FlowRuntimeRegistry) {
        *self.lock() = Some((process_label.to_owned(), registry));
    }

    pub(crate) fn clear(&self) {
        *self.lock() = None;
    }

    fn current(&self) -> Option<(String, FlowRuntimeRegistry)> {
        self.lock().clone()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Option<(String, FlowRuntimeRegistry)>> {
        self.serving
            .lock()
            .expect("serving node lock is not poisoned")
    }
}

/// A write a client saw succeed, by operation marker.
#[derive(Debug, Clone)]
pub(crate) struct AcknowledgedWrite {
    pub(crate) marker: String,
    pub(crate) operation: &'static str,
    pub(crate) flow_id: String,
    /// The run a StartFlow reply named.
    pub(crate) started_run_id: Option<String>,
}

/// What every client of one seed recorded.
#[derive(Default)]
pub(crate) struct ClientRecords {
    pub(crate) acknowledged_writes: Vec<AcknowledgedWrite>,
    pub(crate) outcomes: BTreeMap<(&'static str, &'static str), u64>,
    pub(crate) checked_read_values: u64,
    pub(crate) first_violation: Option<InvariantViolation>,
}

enum CallOutcome<T> {
    Succeeded(T),
    Rejected,
    /// Every call timed out or was unavailable; the write may or may not have happened.
    Unknown,
}

/// One seeded client.
pub(crate) struct SimulatedClient {
    pub(crate) client_label: String,
    rng: ChaCha8Rng,
    serving: ServingNode,
    committed_log: Arc<CommittedLog>,
    records: Arc<Mutex<ClientRecords>>,
    trace: Arc<EventTrace>,
    next_operation: u64,
}

impl SimulatedClient {
    pub(crate) fn new(
        client_label: String,
        seed: u64,
        serving: ServingNode,
        committed_log: Arc<CommittedLog>,
        records: Arc<Mutex<ClientRecords>>,
        trace: Arc<EventTrace>,
    ) -> Self {
        Self {
            client_label,
            rng: ChaCha8Rng::seed_from_u64(seed),
            serving,
            committed_log,
            records,
            trace,
            next_operation: 0,
        }
    }

    /// Runs random operations until `until`, a simulated instant.
    pub(crate) async fn run_until(mut self, until: tokio::time::Instant) {
        while tokio::time::Instant::now() < until {
            let pause = Duration::from_millis(self.rng.random_range(20..800));
            tokio::time::sleep(pause).await;
            self.run_one_operation().await;
        }
    }

    async fn run_one_operation(&mut self) {
        let flow_id = ENGINE_FLOW_IDS[self.rng.random_range(0..ENGINE_FLOW_IDS.len())];
        self.next_operation += 1;
        let marker = format!(
            "{OPERATION_MARKER_PREFIX}{}-{}",
            self.client_label, self.next_operation
        );
        match self.rng.random_range(0..100) {
            0..=11 => {
                let request = start_flow_request(&mut self.rng, flow_id, &marker);
                let outcome = self
                    .call("StartFlow", flow_id, |registry| {
                        let request = request.clone();
                        async move {
                            FlowOperations::start_flow(&registry, TenantId::DEFAULT, request).await
                        }
                    })
                    .await;
                if let CallOutcome::Succeeded(response) = outcome {
                    self.acknowledge(&marker, "StartFlow", flow_id, Some(response.run_id));
                }
            }
            12..=36 => {
                let request = set_attributes_request(&mut self.rng, flow_id, &marker);
                let outcome = self
                    .call("SetAttributes", flow_id, |registry| {
                        let request = request.clone();
                        async move {
                            FlowOperations::set_attributes(&registry, TenantId::DEFAULT, request)
                                .await
                        }
                    })
                    .await;
                if let CallOutcome::Succeeded(()) = outcome {
                    self.acknowledge(&marker, "SetAttributes", flow_id, None);
                }
            }
            37..=54 => {
                let request = publish_request(&mut self.rng, flow_id, &marker);
                let outcome = self
                    .call("PublishToChannel", flow_id, |registry| {
                        let request = request.clone();
                        async move {
                            FlowOperations::publish_to_channel(
                                &registry,
                                TenantId::DEFAULT,
                                request,
                            )
                            .await
                        }
                    })
                    .await;
                if let CallOutcome::Succeeded(()) = outcome {
                    self.acknowledge(&marker, "PublishToChannel", flow_id, None);
                }
            }
            55..=60 => self.delete_a_message(flow_id, &marker).await,
            61..=63 => {
                let request = stop_request(&mut self.rng, flow_id, &marker);
                let outcome = self
                    .call("StopFlow", flow_id, |registry| {
                        let request = request.clone();
                        async move {
                            FlowOperations::stop_flow(&registry, TenantId::DEFAULT, request).await
                        }
                    })
                    .await;
                if let CallOutcome::Succeeded(()) = outcome {
                    self.acknowledge(&marker, "StopFlow", flow_id, None);
                }
            }
            64..=71 => {
                let request = rpc_request(&mut self.rng, flow_id, &marker);
                let outcome = self
                    .call("InvokeRpc", flow_id, |registry| {
                        let request = request.clone();
                        async move {
                            FlowOperations::invoke_rpc(&registry, TenantId::DEFAULT, request).await
                        }
                    })
                    .await;
                if let CallOutcome::Succeeded(_) = outcome {
                    self.acknowledge(&marker, "InvokeRpc", flow_id, None);
                }
            }
            72..=89 => self.read_attributes(flow_id).await,
            _ => {
                self.read_channel_messages(flow_id).await;
            }
        }
    }

    async fn delete_a_message(&mut self, flow_id: &str, marker: &str) {
        let Some(messages) = self.read_channel_messages(flow_id).await else {
            return;
        };
        if messages.is_empty() {
            return;
        }
        let (channel_name, message_id) = messages[self.rng.random_range(0..messages.len())].clone();
        let request = delete_message_request(flow_id, &channel_name, &message_id, marker);
        let outcome = self
            .call("DeleteChannelMessage", flow_id, |registry| {
                let request = request.clone();
                async move {
                    FlowOperations::delete_channel_message(&registry, TenantId::DEFAULT, request)
                        .await
                }
            })
            .await;
        if let CallOutcome::Succeeded(()) = outcome {
            self.acknowledge(marker, "DeleteChannelMessage", flow_id, None);
        }
    }

    /// Every value a read returns must have been committed: reads never show speculation.
    async fn read_attributes(&mut self, flow_id: &str) {
        let request = GetAttributesRequest {
            flow_id: flow_id.to_owned(),
            all_keys: true,
            ..GetAttributesRequest::default()
        };
        let outcome = self
            .call("GetAttributes", flow_id, |registry| {
                let request = request.clone();
                async move {
                    FlowOperations::get_attributes(&registry, TenantId::DEFAULT, request).await
                }
            })
            .await;
        let CallOutcome::Succeeded(response) = outcome else {
            return;
        };
        for attribute in response.attributes {
            let encoded = attribute.value.unwrap_or_default().encode_to_vec();
            let is_committed =
                self.committed_log
                    .was_attribute_committed(flow_id, &attribute.key, &encoded);
            self.check_read(flow_id, is_committed, || {
                format!(
                    "GetAttributes of Flow {flow_id} returned Attribute {} with a value no \
                     committed command wrote",
                    attribute.key
                )
            });
        }
    }

    /// Returns the Channel name and ID of each message read, after checking they were committed.
    async fn read_channel_messages(&mut self, flow_id: &str) -> Option<Vec<(String, String)>> {
        let channel_name = CHANNEL_NAMES[self.rng.random_range(0..CHANNEL_NAMES.len())];
        let request = GetChannelMessagesRequest {
            flow_id: flow_id.to_owned(),
            channel_name: channel_name.to_owned(),
            ..GetChannelMessagesRequest::default()
        };
        let outcome = self
            .call("GetChannelMessages", flow_id, |registry| {
                let request = request.clone();
                async move {
                    FlowOperations::get_channel_messages(&registry, TenantId::DEFAULT, request)
                        .await
                }
            })
            .await;
        let CallOutcome::Succeeded(response) = outcome else {
            return None;
        };
        let mut messages = Vec::new();
        for message in response.messages {
            let is_committed = self
                .committed_log
                .was_message_committed(flow_id, &message.message_id);
            self.check_read(flow_id, is_committed, || {
                format!(
                    "GetChannelMessages of Flow {flow_id} returned message {} that no committed \
                     command wrote",
                    message.message_id
                )
            });
            messages.push((message.channel_name, message.message_id));
        }
        Some(messages)
    }

    fn check_read(&self, flow_id: &str, is_committed: bool, describe: impl FnOnce() -> String) {
        let mut records = self
            .records
            .lock()
            .expect("client records lock is not poisoned");
        records.checked_read_values += 1;
        if is_committed || records.first_violation.is_some() {
            return;
        }
        let detail = describe();
        self.trace.record(&format!(
            "{} violation on Flow {flow_id}: {detail}",
            self.client_label
        ));
        records.first_violation = Some(InvariantViolation::new("speculative-read", detail));
    }

    fn acknowledge(
        &self,
        marker: &str,
        operation: &'static str,
        flow_id: &str,
        started_run_id: Option<String>,
    ) {
        self.records
            .lock()
            .expect("client records lock is not poisoned")
            .acknowledged_writes
            .push(AcknowledgedWrite {
                marker: marker.to_owned(),
                operation,
                flow_id: flow_id.to_owned(),
                started_run_id,
            });
    }

    /// Calls the serving node, retrying unavailable and timed-out calls with the same request.
    async fn call<T, F, Fut>(
        &mut self,
        operation: &'static str,
        flow_id: &str,
        mut invoke: F,
    ) -> CallOutcome<T>
    where
        F: FnMut(FlowRuntimeRegistry) -> Fut,
        Fut: Future<Output = Result<T, FlowOperationError>>,
    {
        for call_number in 1..=MAX_CALLS_PER_OPERATION {
            let Some((process_label, registry)) = self.serving.current() else {
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            };
            let result = tokio::time::timeout(CALL_TIMEOUT, invoke(registry)).await;
            let (outcome_name, detail) = match &result {
                Ok(Ok(_)) => ("ok", String::new()),
                Ok(Err(error @ FlowOperationError::Unavailable { .. })) => {
                    ("unavailable", error.to_string())
                }
                Ok(Err(error)) => ("rejected", error.to_string()),
                Err(_) => ("timeout", String::new()),
            };
            self.trace.record(&format!(
                "{} {operation} {flow_id} via {process_label} call {call_number}: {outcome_name} \
                 {detail}",
                self.client_label
            ));
            *self
                .records
                .lock()
                .expect("client records lock is not poisoned")
                .outcomes
                .entry((operation, outcome_name))
                .or_default() += 1;
            match result {
                Ok(Ok(value)) => return CallOutcome::Succeeded(value),
                Ok(Err(FlowOperationError::Unavailable { .. })) | Err(_) => {
                    let backoff = Duration::from_millis(self.rng.random_range(100..2_000));
                    tokio::time::sleep(backoff).await;
                }
                Ok(Err(_)) => return CallOutcome::Rejected,
            }
        }
        CallOutcome::Unknown
    }
}
