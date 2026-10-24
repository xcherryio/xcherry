// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_keyspace::RunId;
use dexzd_protocol::dex::{ErrorSubStatus, FlowHistoryEvent, flow_history_event};
use dexzd_protocol::dexzd::v1::{
    CommandReply, FlowCommand, HistoryRecord, RunRecord, StepInvocationKey,
};

use crate::command_handlers::command_replies::{error_reply, grpc_code};
use crate::command_handlers::{CommandScope, HandlerOutput, ReplayedCommandOutput};
use crate::history;
use crate::state_access::{FlowTransaction, RunRows};
use crate::values::enforce_superseded_byte_cap;
use crate::values::{ValueIdAllocator, ValueLimits};
use crate::{ApplyEffect, ApplyError, WaitableChange};

/// How a command counts for history and time travel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HistoryKind {
    pub(crate) is_semantic: bool,
    pub(crate) is_external_input: bool,
}

impl HistoryKind {
    /// A command that changes Flow progress and is a time travel target.
    pub(crate) const SEMANTIC: Self = Self {
        is_semantic: true,
        is_external_input: false,
    };
    /// External input (API writes, RPC effects) that time travel may reapply.
    pub(crate) const EXTERNAL_INPUT: Self = Self {
        is_semantic: true,
        is_external_input: true,
    };
    /// Bookkeeping such as attempt starts and heartbeats.
    pub(crate) const BOOKKEEPING: Self = Self {
        is_semantic: false,
        is_external_input: false,
    };
}

/// The run a command applies to, with the per-command counters apply assigns.
pub(crate) struct RunCursor {
    pub(crate) rows: RunRows,
    pub(crate) record: RunRecord,
    /// Run seq of the command being applied.
    pub(crate) seq: u64,
    pub(crate) values: ValueIdAllocator,
    events: Vec<flow_history_event::Payload>,
    pub(crate) effects: Vec<ApplyEffect>,
    /// Set by TimeTravel: the command starts a new attempt and writes its start checkpoint.
    pub(crate) starts_attempt: bool,
    /// Set by TimeTravel: input it reapplied, recorded so later travels reapply it again.
    pub(crate) reapplied_inputs: Vec<FlowCommand>,
    /// Set by TimeTravel: method calls its reapplied input made pending for the first time.
    pub(crate) reapplied_pending_invocations: Vec<StepInvocationKey>,
}

impl RunCursor {
    /// Opens the run the command names. Returns a NotFound reply when the run does not exist
    /// or was purged, and a FailedPrecondition reply when the expected attempt differs.
    pub(crate) fn open(
        transaction: &FlowTransaction<'_>,
        scope: &CommandScope<'_>,
    ) -> Result<Result<Self, CommandReply>, ApplyError> {
        let Ok(run_id) = RunId::from_slice(&scope.command.run_id) else {
            return Err(ApplyError::InvariantViolation {
                detail: "a committed run command has no 16-byte run ID".to_owned(),
            });
        };
        let rows = RunRows::new(transaction.context().flow_hash, run_id);
        let Some(record) = rows.run_record(transaction)? else {
            return Ok(Err(flow_not_found_reply(scope.flow_id())));
        };
        if record.tenant_id != scope.tenant_id.0
            || record.flow_id != scope.flow_id()
            || record.is_purging
        {
            return Ok(Err(flow_not_found_reply(scope.flow_id())));
        }
        let expected_attempt = scope.command.expected_run_attempt;
        if expected_attempt != 0 && expected_attempt != record.current_attempt {
            return Ok(Err(error_reply(
                grpc_code::FAILED_PRECONDITION,
                ErrorSubStatus::Uncategorized,
                format!(
                    "run attempt {} changed to {} through time travel",
                    expected_attempt, record.current_attempt
                ),
            )));
        }
        Ok(Ok(Self::with_record(rows, record)))
    }

    /// Continues the TimeTravel command at `seq` for one input it reapplies.
    pub(crate) fn for_reapplied_input(
        rows: RunRows,
        record: RunRecord,
        seq: u64,
        values: ValueIdAllocator,
    ) -> Self {
        Self {
            values,
            ..Self::with_seq(rows, record, seq)
        }
    }

    /// Starts a cursor for a run created by this command.
    pub(crate) fn for_new_run(rows: RunRows, record: RunRecord) -> Self {
        Self::with_record(rows, record)
    }

    fn with_record(rows: RunRows, mut record: RunRecord) -> Self {
        let seq = record.next_run_seq.max(1);
        record.next_run_seq = seq + 1;
        Self::with_seq(rows, record, seq)
    }

    fn with_seq(rows: RunRows, record: RunRecord, seq: u64) -> Self {
        Self {
            rows,
            record,
            seq,
            values: ValueIdAllocator::new(seq),
            events: Vec::new(),
            effects: Vec::new(),
            starts_attempt: false,
            reapplied_inputs: Vec::new(),
            reapplied_pending_invocations: Vec::new(),
        }
    }

    pub(crate) fn run_id(&self) -> RunId {
        self.rows.run_id
    }

    pub(crate) fn attempt(&self) -> u32 {
        self.record.current_attempt
    }

    pub(crate) fn value_limits(&self) -> ValueLimits {
        let settings = self.record.settings.unwrap_or_default();
        ValueLimits {
            wire_inline_threshold_bytes: settings.wire_inline_threshold_bytes,
            max_value_bytes: settings.max_value_bytes,
        }
    }

    pub(crate) fn add_event(&mut self, payload: flow_history_event::Payload) {
        self.events.push(payload);
    }

    pub(crate) fn add_effect(&mut self, effect: ApplyEffect) {
        self.effects.push(effect);
    }

    pub(crate) fn wake(&mut self, change: WaitableChange) {
        let run_id = self.run_id();
        self.effects
            .push(ApplyEffect::WakeWaiters { run_id, change });
    }

    /// Records the command in history, saves the run record, and builds the handler output.
    pub(crate) fn finish(
        mut self,
        transaction: &mut FlowTransaction<'_>,
        scope: &CommandScope<'_>,
        reply: CommandReply,
        kind: HistoryKind,
    ) -> Result<HandlerOutput, ApplyError> {
        if scope.is_replay {
            return Ok(self.finish_replayed(transaction, reply));
        }
        let event_time = timestamp(scope.now_micros);
        let mut events = Vec::with_capacity(self.events.len());
        for payload in std::mem::take(&mut self.events) {
            let event_id = self.record.next_event_id.max(1);
            self.record.next_event_id = event_id + 1;
            events.push(FlowHistoryEvent {
                event_id,
                event_time: Some(event_time),
                run_attempt: i32::try_from(self.record.current_attempt).unwrap_or(i32::MAX),
                payload: Some(payload),
            });
        }
        if kind.is_semantic {
            self.record.semantic_event_count += 1;
        }
        self.record.commands_since_checkpoint += 1;
        let last_event_id = events.last().map(|event| event.event_id);
        let pending_step_methods = history::pending_step_methods(transaction, &self)?;
        self.rows.put_history_record(
            transaction,
            &HistoryRecord {
                seq: self.seq,
                attempt: self.record.current_attempt,
                timestamp_micros: scope.now_micros,
                is_semantic: kind.is_semantic,
                is_external_input: kind.is_external_input,
                command: Some(scope.command.clone()),
                events,
                state_hash: Vec::new(),
                pending_step_methods,
                reapplied_inputs: std::mem::take(&mut self.reapplied_inputs),
            },
        );
        history::after_command(transaction, &mut self, kind)?;
        self.rows.put_run_record(transaction, &self.record);
        enforce_superseded_byte_cap(transaction, &mut self)?;
        if let Some(last_event_id) = last_event_id {
            self.wake(WaitableChange::HistoryAppended { last_event_id });
        }
        Ok(HandlerOutput {
            reply,
            effects: self.effects,
            run_id: Some(self.rows.run_id),
            replayed: None,
        })
    }

    /// Saves the run record only; history, counters, and pruning belong to the recorded command.
    fn finish_replayed(
        self,
        transaction: &mut FlowTransaction<'_>,
        reply: CommandReply,
    ) -> HandlerOutput {
        self.rows.put_run_record(transaction, &self.record);
        HandlerOutput {
            reply,
            effects: self.effects,
            run_id: Some(self.rows.run_id),
            replayed: Some(ReplayedCommandOutput {
                events: self.events,
                values: self.values,
            }),
        }
    }
}

pub(crate) fn flow_not_found_reply(flow_id: &str) -> CommandReply {
    error_reply(
        grpc_code::NOT_FOUND,
        ErrorSubStatus::FlowNotExists,
        format!("Flow run is not found for flow ID {flow_id}"),
    )
}

pub(crate) fn timestamp(micros: i64) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: micros.div_euclid(1_000_000),
        nanos: i32::try_from(micros.rem_euclid(1_000_000) * 1000).unwrap_or(0),
    }
}
