// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! The Flow directory, run records, history order, and the timer index agree.

use std::collections::BTreeSet;

use dexzd_keyspace::RunId;
use dexzd_protocol::dex::FlowStatus;
use dexzd_protocol::dexzd::v1::StepExecutionPhase;
use prost::Message;

use crate::invariants::{InvariantViolation, RunContext};
use crate::row_decoding::hex;
use crate::state_view::{FlowView, RunView};

const DIRECTORY: &str = "flow-directory";
const RUN_RECORD: &str = "run-record";
const HISTORY: &str = "history-order";
const TIMER_INDEX: &str = "timer-index";

pub(crate) fn check_directory(flow_id: &str, view: &FlowView) -> Result<(), InvariantViolation> {
    let Some(directory) = &view.directory else {
        if view.runs.is_empty() {
            return Ok(());
        }
        return Err(InvariantViolation::new(
            DIRECTORY,
            format!("Flow {flow_id} has run rows but no directory row"),
        ));
    };
    let mut listed = BTreeSet::new();
    for (index, entry) in directory.runs.iter().enumerate() {
        let run_id = RunId::from_slice(&entry.run_id).map_err(|_| {
            InvariantViolation::new(
                DIRECTORY,
                format!(
                    "Flow {flow_id} lists a malformed run ID {}",
                    hex(&entry.run_id)
                ),
            )
        })?;
        if !listed.insert(run_id) {
            return Err(InvariantViolation::new(
                DIRECTORY,
                format!("Flow {flow_id} lists run {run_id} twice"),
            ));
        }
        let record = view
            .runs
            .get(&run_id)
            .and_then(|run| run.record.as_ref())
            .ok_or_else(|| {
                InvariantViolation::new(
                    DIRECTORY,
                    format!("Flow {flow_id} lists run {run_id} without a run record"),
                )
            })?;
        let is_consistent = record.status == entry.status
            && record.started_at_micros == entry.started_at_micros
            && record.closed_at_micros == entry.closed_at_micros
            && record.start_request_id == entry.start_request_id;
        if !is_consistent {
            return Err(InvariantViolation::new(
                DIRECTORY,
                format!(
                    "Flow {flow_id} run {run_id}: directory entry {entry:?} disagrees with run \
                     record (status {}, started {}, closed {}, request {:?})",
                    record.status,
                    record.started_at_micros,
                    record.closed_at_micros,
                    record.start_request_id
                ),
            ));
        }
        let is_last = index + 1 == directory.runs.len();
        if entry.status == FlowStatus::Running as i32 && !is_last {
            return Err(InvariantViolation::new(
                DIRECTORY,
                format!("Flow {flow_id} run {run_id} is running but is not the latest run"),
            ));
        }
    }
    for run_id in view.runs.keys() {
        if !listed.contains(run_id) {
            return Err(InvariantViolation::new(
                DIRECTORY,
                format!("Flow {flow_id} has rows of run {run_id}, which the directory omits"),
            ));
        }
    }
    Ok(())
}

pub(crate) fn check_run_record(
    context: &RunContext<'_>,
    run: &RunView,
) -> Result<(), InvariantViolation> {
    let Some(record) = &run.record else {
        return Err(InvariantViolation::new(
            RUN_RECORD,
            format!("{context} has rows but no run record"),
        ));
    };
    if record.run_id != context.run_id.as_bytes() || record.flow_id != context.flow_id {
        return Err(InvariantViolation::new(
            RUN_RECORD,
            format!(
                "{context} has a run record for Flow {} run {}",
                record.flow_id,
                hex(&record.run_id)
            ),
        ));
    }
    if record.current_attempt == 0 {
        return Err(InvariantViolation::new(
            RUN_RECORD,
            format!("{context} has run attempt 0"),
        ));
    }
    for attempt in 1..=record.current_attempt {
        if !run.attempts.contains_key(&attempt) {
            return Err(InvariantViolation::new(
                RUN_RECORD,
                format!("{context} has no attempt record for attempt {attempt}"),
            ));
        }
    }
    let is_running = record.status == FlowStatus::Running as i32;
    if is_running != (record.closed_at_micros == 0) {
        return Err(InvariantViolation::new(
            RUN_RECORD,
            format!(
                "{context} has status {} with close time {}",
                record.status, record.closed_at_micros
            ),
        ));
    }
    if !is_running {
        let active = run.step_executions.values().find(|step| {
            matches!(
                StepExecutionPhase::try_from(step.phase),
                Ok(StepExecutionPhase::WaitFor
                    | StepExecutionPhase::Waiting
                    | StepExecutionPhase::Execute)
            )
        });
        if let Some(step) = active {
            return Err(InvariantViolation::new(
                RUN_RECORD,
                format!(
                    "{context} is closed but Step execution {} is still in phase {}",
                    step.step_execution_id, step.phase
                ),
            ));
        }
        if let Some((lock_key, _)) = run.locks.iter().next() {
            return Err(InvariantViolation::new(
                "L3-lock-holder",
                format!("{context} is closed but lock {lock_key} still has a row"),
            ));
        }
    }
    Ok(())
}

pub(crate) fn check_history(
    context: &RunContext<'_>,
    run: &RunView,
) -> Result<(), InvariantViolation> {
    let Some(record) = &run.record else {
        return Ok(());
    };
    let mut last_event_id = 0_i64;
    for (key_seq, history) in &run.history {
        if history.seq != *key_seq || *key_seq == 0 || *key_seq >= record.next_run_seq {
            return Err(InvariantViolation::new(
                HISTORY,
                format!(
                    "{context} history key seq {key_seq} holds record seq {} (next run seq {})",
                    history.seq, record.next_run_seq
                ),
            ));
        }
        if history.attempt == 0 || history.attempt > record.current_attempt {
            return Err(InvariantViolation::new(
                HISTORY,
                format!(
                    "{context} history seq {key_seq} has attempt {} (current {})",
                    history.attempt, record.current_attempt
                ),
            ));
        }
        for event in &history.events {
            if event.event_id <= last_event_id || event.event_id >= record.next_event_id {
                return Err(InvariantViolation::new(
                    HISTORY,
                    format!(
                        "{context} history seq {key_seq} has event ID {} after {last_event_id} \
                         (next event ID {})",
                        event.event_id, record.next_event_id
                    ),
                ));
            }
            last_event_id = event.event_id;
        }
    }
    Ok(())
}

pub(crate) fn check_timers(
    context: &RunContext<'_>,
    run: &RunView,
    view: &FlowView,
) -> Result<(), InvariantViolation> {
    let index_entries: BTreeSet<(i64, &[u8])> = view
        .timer_index
        .iter()
        .filter(|entry| entry.run_id == *context.run_id)
        .map(|entry| (entry.fire_at_micros, entry.encoded_timer_id.as_slice()))
        .collect();
    let mut rows = BTreeSet::new();
    for (encoded, timer) in &run.timers {
        let encoded_record_id = timer
            .timer_id
            .as_ref()
            .map(Message::encode_to_vec)
            .unwrap_or_default();
        if encoded_record_id != *encoded {
            return Err(InvariantViolation::new(
                TIMER_INDEX,
                format!(
                    "{context} timer row {} holds TimerId {:?}",
                    hex(encoded),
                    timer.timer_id
                ),
            ));
        }
        if !index_entries.contains(&(timer.fire_at_micros, encoded.as_slice())) {
            return Err(InvariantViolation::new(
                TIMER_INDEX,
                format!(
                    "{context} timer {:?} at {} has no timer index entry",
                    timer.timer_id, timer.fire_at_micros
                ),
            ));
        }
        rows.insert((timer.fire_at_micros, encoded.as_slice()));
    }
    if let Some((fire_at, encoded)) = index_entries.difference(&rows).next() {
        return Err(InvariantViolation::new(
            TIMER_INDEX,
            format!(
                "{context} timer index entry {} at {fire_at} has no timer row",
                hex(encoded)
            ),
        ));
    }
    Ok(())
}
