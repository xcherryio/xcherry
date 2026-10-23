// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Long-poll waiters (plan 9.7). Waiters register before they read committed state and are
//! woken only by committed outcomes, so a change between the read and the wait is never lost.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

use dexzd_apply::WaitableChange;
use dexzd_protocol::dex::ErrorSubStatus;
use tokio::sync::Notify;

use crate::runtime::{FlowAddress, RuntimeError, grpc_code};

/// What a waiter waits for; selects which committed changes wake it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WaitInterest {
    /// The run closes.
    FlowClose,
    /// The Attribute with this key changes.
    Attribute(String),
    /// The Step execution with this ID finishes.
    StepExecution(String),
    /// History reaches this event ID.
    HistoryEvent(i64),
}

impl WaitInterest {
    fn is_woken_by(&self, change: &WaitableChange) -> bool {
        match (self, change) {
            (_, WaitableChange::FlowClosed | WaitableChange::RunAttemptChanged) => true,
            (Self::Attribute(key), WaitableChange::AttributesChanged { keys }) => {
                keys.binary_search(key).is_ok()
            }
            (
                Self::StepExecution(step_execution_id),
                WaitableChange::StepExecutionsFinished { step_execution_ids },
            ) => step_execution_ids.binary_search(step_execution_id).is_ok(),
            (Self::HistoryEvent(event_id), WaitableChange::HistoryAppended { last_event_id }) => {
                last_event_id >= event_id
            }
            _ => false,
        }
    }
}

/// Every long-poll waiter of the node.
#[derive(Clone)]
pub(crate) struct WaiterRegistry {
    shared: Arc<WaiterShared>,
}

struct WaiterShared {
    max_waiters_per_node: usize,
    max_waiters_per_flow: usize,
    table: Mutex<WaiterTable>,
}

#[derive(Default)]
struct WaiterTable {
    waiter_count: usize,
    next_waiter_id: u64,
    flows: BTreeMap<FlowAddress, BTreeMap<u64, WaiterSlot>>,
    is_closed: bool,
}

struct WaiterSlot {
    interest: WaitInterest,
    notify: Arc<Notify>,
}

/// One registered waiter; deregisters on drop.
pub(crate) struct WaiterRegistration {
    shared: Arc<WaiterShared>,
    flow: FlowAddress,
    waiter_id: u64,
    notify: Arc<Notify>,
}

impl WaiterRegistry {
    pub(crate) fn new(max_waiters_per_node: usize, max_waiters_per_flow: usize) -> Self {
        Self {
            shared: Arc::new(WaiterShared {
                max_waiters_per_node,
                max_waiters_per_flow,
                table: Mutex::new(WaiterTable::default()),
            }),
        }
    }

    /// Registers a waiter.
    ///
    /// Returns `RESOURCE_EXHAUSTED` beyond the node or Flow limit, and `UNAVAILABLE` after
    /// [`Self::close`].
    pub(crate) fn register(
        &self,
        flow: &FlowAddress,
        interest: WaitInterest,
    ) -> Result<WaiterRegistration, RuntimeError> {
        let mut table = self.shared.lock_table();
        if table.is_closed {
            return Err(RuntimeError::Unavailable {
                detail: "long polls are closed because the node is stopping".to_owned(),
            });
        }
        if table.waiter_count >= self.shared.max_waiters_per_node {
            return Err(waiter_limit_error("node", self.shared.max_waiters_per_node));
        }
        let flow_waiter_count = table.flows.get(flow).map_or(0, BTreeMap::len);
        if flow_waiter_count >= self.shared.max_waiters_per_flow {
            return Err(waiter_limit_error("Flow", self.shared.max_waiters_per_flow));
        }
        table.next_waiter_id += 1;
        let waiter_id = table.next_waiter_id;
        let notify = Arc::new(Notify::new());
        table.flows.entry(flow.clone()).or_default().insert(
            waiter_id,
            WaiterSlot {
                interest,
                notify: Arc::clone(&notify),
            },
        );
        table.waiter_count += 1;
        Ok(WaiterRegistration {
            shared: Arc::clone(&self.shared),
            flow: flow.clone(),
            waiter_id,
            notify,
        })
    }

    /// Wakes the waiters of `flow` that `change` may satisfy.
    pub(crate) fn wake(&self, flow: &FlowAddress, change: &WaitableChange) {
        let table = self.shared.lock_table();
        let Some(waiters) = table.flows.get(flow) else {
            return;
        };
        for slot in waiters.values() {
            if slot.interest.is_woken_by(change) {
                slot.notify.notify_one();
            }
        }
    }

    /// Rejects new waiters and wakes every registered one so it observes the stop.
    pub(crate) fn close(&self) {
        let mut table = self.shared.lock_table();
        table.is_closed = true;
        for slot in table.flows.values().flat_map(BTreeMap::values) {
            slot.notify.notify_one();
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.shared.lock_table().is_closed
    }

    /// Returns the number of registered waiters on the node.
    pub(crate) fn waiter_count(&self) -> usize {
        self.shared.lock_table().waiter_count
    }
}

impl WaiterShared {
    fn lock_table(&self) -> MutexGuard<'_, WaiterTable> {
        self.table
            .lock()
            .expect("waiter table lock is poisoned only if a holder panicked")
    }
}

impl WaiterRegistration {
    /// Waits until a committed change may satisfy the waiter, or the registry closes.
    /// A wake that arrived since the last call returns immediately.
    pub(crate) async fn changed(&self) {
        self.notify.notified().await;
    }
}

impl Drop for WaiterRegistration {
    fn drop(&mut self) {
        let mut table = self.shared.lock_table();
        let Some(waiters) = table.flows.get_mut(&self.flow) else {
            return;
        };
        if waiters.remove(&self.waiter_id).is_none() {
            return;
        }
        if waiters.is_empty() {
            table.flows.remove(&self.flow);
        }
        table.waiter_count -= 1;
    }
}

fn waiter_limit_error(scope: &str, limit: usize) -> RuntimeError {
    RuntimeError::rejected(
        grpc_code::RESOURCE_EXHAUSTED,
        ErrorSubStatus::Uncategorized,
        format!("too many long-poll waiters on this {scope}; the limit is {limit}"),
    )
}
