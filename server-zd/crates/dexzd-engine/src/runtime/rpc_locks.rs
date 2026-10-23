// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! In-memory RPC locks and the lock checks for pending invocations (plan 8.3).

use std::collections::BTreeMap;

use dexzd_apply::FlowStateReader;
use dexzd_keyspace::{ColumnFamily, FlowHash, RunId, RunTable, run_key};
use dexzd_protocol::dexzd::v1::{LockRecord, StepInvocationKey};
use prost::Message;

use crate::runtime::RuntimeError;

/// Locks held by RPCs of the current epoch; never logged (plan 8.3).
#[derive(Debug, Default)]
pub(crate) struct RpcLockTable {
    holders: BTreeMap<(RunId, String), u64>,
    next_lock_id: u64,
}

impl RpcLockTable {
    pub(crate) fn is_empty(&self) -> bool {
        self.holders.is_empty()
    }

    pub(crate) fn is_held(&self, run_id: RunId, lock_key: &str) -> bool {
        self.holders.contains_key(&(run_id, lock_key.to_owned()))
    }

    /// Takes every key for one RPC; the caller checked that all are free.
    pub(crate) fn acquire(&mut self, run_id: RunId, lock_keys: &[String]) -> u64 {
        self.next_lock_id += 1;
        for lock_key in lock_keys {
            let previous = self
                .holders
                .insert((run_id, lock_key.clone()), self.next_lock_id);
            assert!(previous.is_none(), "RPC lock {lock_key} was already held");
        }
        self.next_lock_id
    }

    pub(crate) fn release(&mut self, lock_id: u64) {
        self.holders.retain(|_, holder| *holder != lock_id);
    }
}

/// Why a lock key is not available.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LockBlocker {
    /// An RPC holds the key.
    Rpc,
    /// A running Step attempt holds the key (committed or speculative).
    Step,
    /// An earlier pending invocation waits for the key.
    EarlierWaiter,
}

/// Returns a held key that blocks an RPC; queued Step waiters never block RPCs (plan 8.3).
pub(crate) fn find_rpc_lock_blocker(
    state: &dyn FlowStateReader,
    rpc_locks: &RpcLockTable,
    flow_hash: FlowHash,
    run_id: RunId,
    lock_keys: &[String],
) -> Result<Option<(String, LockBlocker)>, RuntimeError> {
    for lock_key in lock_keys {
        if rpc_locks.is_held(run_id, lock_key) {
            return Ok(Some((lock_key.clone(), LockBlocker::Rpc)));
        }
        let record = read_lock_record(state, flow_hash, run_id, lock_key)?;
        if record.is_some_and(|record| has_holder(&record)) {
            return Ok(Some((lock_key.clone(), LockBlocker::Step)));
        }
    }
    Ok(None)
}

/// Returns a held key or an earlier waiter that blocks the invocation.
pub(crate) fn find_invocation_lock_blocker(
    state: &dyn FlowStateReader,
    rpc_locks: &RpcLockTable,
    flow_hash: FlowHash,
    run_id: RunId,
    invocation: &StepInvocationKey,
    lock_keys: &[String],
    pending_since_seq: u64,
) -> Result<Option<(String, LockBlocker)>, RuntimeError> {
    for lock_key in lock_keys {
        if rpc_locks.is_held(run_id, lock_key) {
            return Ok(Some((lock_key.clone(), LockBlocker::Rpc)));
        }
        let Some(record) = read_lock_record(state, flow_hash, run_id, lock_key)? else {
            continue;
        };
        if has_holder(&record) && record.holder.as_ref() != Some(invocation) {
            return Ok(Some((lock_key.clone(), LockBlocker::Step)));
        }
        let has_earlier_waiter = record.waiters.iter().any(|waiter| {
            waiter.invocation.as_ref() != Some(invocation)
                && waiter.pending_since_seq < pending_since_seq
        });
        if has_earlier_waiter {
            return Ok(Some((lock_key.clone(), LockBlocker::EarlierWaiter)));
        }
    }
    Ok(None)
}

fn read_lock_record(
    state: &dyn FlowStateReader,
    flow_hash: FlowHash,
    run_id: RunId,
    lock_key: &str,
) -> Result<Option<LockRecord>, RuntimeError> {
    let key = run_key(flow_hash, run_id, RunTable::LockTable, lock_key.as_bytes());
    let Some(bytes) = state.get(ColumnFamily::Flow, &key)? else {
        return Ok(None);
    };
    LockRecord::decode(bytes.as_slice())
        .map(Some)
        .map_err(|error| RuntimeError::corrupt_row("lock", error))
}

fn has_holder(record: &LockRecord) -> bool {
    record
        .holder
        .as_ref()
        .is_some_and(|holder| !holder.step_execution_id.is_empty())
}
