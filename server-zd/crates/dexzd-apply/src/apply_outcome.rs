// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_keyspace::{ColumnFamily, RunId};
use dexzd_protocol::dexzd::v1::{CommandReply, StepInvocationKey};

use crate::StateReadError;

/// One write of an apply outcome. All writes of one outcome commit atomically.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StateWrite {
    /// Sets `key` to `value`.
    Put {
        /// Column family of the key.
        column_family: ColumnFamily,
        /// The key.
        key: Vec<u8>,
        /// The encoded row.
        value: Vec<u8>,
    },
    /// Deletes `key`.
    Delete {
        /// Column family of the key.
        column_family: ColumnFamily,
        /// The key.
        key: Vec<u8>,
    },
    /// Deletes every key in `[start, end)`.
    DeleteRange {
        /// Column family of the keys.
        column_family: ColumnFamily,
        /// First key deleted.
        start: Vec<u8>,
        /// First key kept.
        end: Vec<u8>,
    },
}

/// A change that long-poll waiters may be waiting for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WaitableChange {
    /// The run's current attempt closed.
    FlowClosed,
    /// Attributes with these keys changed.
    AttributesChanged {
        /// Changed keys, sorted.
        keys: Vec<String>,
    },
    /// These Step executions completed or were canceled.
    StepExecutionsFinished {
        /// Step execution IDs, sorted.
        step_execution_ids: Vec<String>,
    },
    /// History events up to this event ID exist.
    HistoryAppended {
        /// Largest event ID now in history.
        last_event_id: i64,
    },
    /// The run attempt changed through time travel.
    RunAttemptChanged,
}

/// Work for the runtime that follows from a committed command.
///
/// Effects are hints derived from the new state. After a restart the runtime rebuilds the
/// same work from state alone, so losing effects never loses work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyEffect {
    /// A method call is pending; the owner may start its attempt when its locks are free.
    InvocationPending {
        /// Run of the call.
        run_id: RunId,
        /// Run attempt of the call.
        run_attempt: u32,
        /// The call.
        invocation: StepInvocationKey,
        /// Attempt that would start.
        attempt: u32,
    },
    /// An attempt started; dispatch it with the snapshot stored in its Step execution.
    InvocationStarted {
        /// Run of the call.
        run_id: RunId,
        /// Run attempt of the call.
        run_attempt: u32,
        /// The call.
        invocation: StepInvocationKey,
        /// The started attempt.
        attempt: u32,
        /// Dispatch ID.
        task_id: Vec<u8>,
    },
    /// A running attempt ended without a result (canceled, Flow closed, time travel); tell the
    /// Worker to stop.
    InvocationCanceled {
        /// Run of the call.
        run_id: RunId,
        /// Dispatch ID of the canceled attempt.
        task_id: Vec<u8>,
    },
    /// Locks on these keys were released; pending calls and RPCs waiting on them may proceed.
    LocksReleased {
        /// Run of the locks.
        run_id: RunId,
        /// Released lock keys, sorted.
        lock_keys: Vec<String>,
    },
    /// A timer was armed; the timer service reads the timer index and may need to wake earlier.
    TimerArmed {
        /// Fire time in microseconds since the Unix epoch.
        fire_at_micros: i64,
    },
    /// New outbox messages wait for delivery.
    OutboxMessagesWritten {
        /// Run that owns the outbox.
        run_id: RunId,
        /// Message IDs, in write order.
        message_ids: Vec<Vec<u8>>,
    },
    /// Hard-delete objects wait for deletion from object storage.
    HardDeleteObjectsPending {
        /// Run that owned the objects.
        run_id: RunId,
        /// Object IDs.
        object_ids: Vec<String>,
    },
    /// Retention expired; delete the run's hard-delete prefix, then propose a run purge.
    RunPurgeRequested {
        /// Run to purge.
        run_id: RunId,
    },
    /// Wake long-poll waiters of the run.
    WakeWaiters {
        /// Run that changed.
        run_id: RunId,
        /// What changed.
        change: WaitableChange,
    },
}

/// Everything that applying one command produced.
#[derive(Debug, Clone, PartialEq)]
pub struct ApplyOutcome {
    /// Writes in a deterministic order; commit them in one atomic batch.
    pub writes: Vec<StateWrite>,
    /// Runtime work, in the order apply produced it.
    pub effects: Vec<ApplyEffect>,
    /// Reply for the proposer.
    pub reply: CommandReply,
    /// The run the command applied to or created, when one exists.
    pub run_id: Option<RunId>,
}

impl ApplyOutcome {
    /// Returns an outcome that changes nothing and replies with `reply`.
    pub fn rejected(reply: CommandReply) -> Self {
        Self {
            writes: Vec::new(),
            effects: Vec::new(),
            reply,
            run_id: None,
        }
    }
}

/// A failure that stops the partition: the state or the command log violates a trusted
/// invariant. Client and Worker mistakes are replies, never `ApplyError`.
#[derive(Debug, thiserror::Error)]
pub enum ApplyError {
    /// The command was proposed with a behavior version this binary cannot apply.
    #[error("behavior version {version} is not supported by this binary")]
    UnsupportedBehaviorVersion {
        /// The recorded version.
        version: u32,
    },
    /// Reading state failed.
    #[error("reading Flow state failed: {0}")]
    StateRead(#[from] StateReadError),
    /// A stored row could not be decoded.
    #[error("stored row {table} {key} is corrupt: {detail}")]
    CorruptState {
        /// Table or column family of the row.
        table: &'static str,
        /// Hex-encoded key.
        key: String,
        /// Decoder message.
        detail: String,
    },
    /// The command or state contradicts an invariant apply relies on.
    #[error("apply invariant violated: {detail}")]
    InvariantViolation {
        /// What was violated.
        detail: String,
    },
}
