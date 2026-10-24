// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::{Mutex, MutexGuard};

use dexzd_apply::ApplyContext;
use tokio::sync::watch;

use crate::runtime::FlowAddress;
use crate::runtime::rpc_locks::RpcLockTable;
use crate::runtime::speculative_overlay::SpeculativeOverlay;

/// One Flow's state; proposals stamp, enqueue, and apply under its lock, never held across awaits.
pub(crate) struct FlowRuntime {
    pub(crate) address: FlowAddress,
    pub(crate) context: ApplyContext,
    state: Mutex<FlowRuntimeState>,
    /// Number of overlay entries settled (committed) so far.
    settled_count: watch::Sender<u64>,
}

pub(crate) struct FlowRuntimeState {
    pub(crate) overlay: SpeculativeOverlay,
    pub(crate) rpc_locks: RpcLockTable,
    /// Number of overlay entries proposed so far.
    pub(crate) proposed_count: u64,
    settled_count: u64,
}

impl FlowRuntime {
    pub(crate) fn new(address: FlowAddress, context: ApplyContext) -> Self {
        Self {
            address,
            context,
            state: Mutex::new(FlowRuntimeState {
                overlay: SpeculativeOverlay::default(),
                rpc_locks: RpcLockTable::default(),
                proposed_count: 0,
                settled_count: 0,
            }),
            settled_count: watch::Sender::new(0),
        }
    }

    pub(crate) fn lock_state(&self) -> MutexGuard<'_, FlowRuntimeState> {
        self.state
            .lock()
            .expect("Flow runtime lock is poisoned only if a holder panicked")
    }

    /// Returns the state when no other caller holds the lock.
    pub(crate) fn try_lock_state(&self) -> Option<MutexGuard<'_, FlowRuntimeState>> {
        self.state.try_lock().ok()
    }

    /// Publishes the settled count after the caller changed the overlay.
    pub(crate) fn publish_settled(&self, state: &FlowRuntimeState) {
        self.settled_count.send_replace(state.settled_count);
    }

    pub(crate) fn subscribe_settled(&self) -> watch::Receiver<u64> {
        self.settled_count.subscribe()
    }
}

impl FlowRuntimeState {
    pub(crate) fn is_idle(&self) -> bool {
        self.overlay.is_empty() && self.rpc_locks.is_empty()
    }

    pub(crate) fn record_settled(&mut self, entry_count: usize) {
        self.settled_count += u64::try_from(entry_count).expect("entry count fits in u64");
    }
}
