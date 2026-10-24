// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Randomized invariant tests of apply over a fixed seed set.
//!
//! Set `DEXZD_SIMULATION_SEEDS` and `DEXZD_SIMULATION_COMMANDS` for a longer budget, or
//! `DEXZD_SIMULATION_SEED` for one seed.

use dexzd_keyspace::row_keys::value_row_key;
use dexzd_keyspace::{ColumnFamily, RunId, RunTable, ValueId};
use dexzd_protocol::dex::{FlowResetType, ResetFlowRequest, value};
use dexzd_protocol::dexzd::v1::{
    StoredValue, TimeTravelCommand, command_reply, flow_command::Command, stored_value,
};
use dexzd_simulation::{
    ApplyChecker, CommandGenerator, INVALID_CLIENT_INPUT_CLASSES, RunSelection, SimulationFailure,
    SimulationSummary, run_seed,
};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

const DEFAULT_SEED_COUNT: u64 = 64;
const DEFAULT_COMMANDS_PER_SEED: usize = 400;
const MAX_SEED_COUNT: u64 = 1000;
const MAX_COMMANDS_PER_SEED: usize = 2000;
const MAX_RANDOM_COMMANDS: u64 = 500_000;
/// First seed of the fixed set; any constant keeps the set reproducible.
const FIRST_SEED: u64 = 0x5eed_0000;
type RecordedAttributeValues = BTreeMap<String, (ValueId, Vec<u8>)>;
const REGRESSION_SEEDS: &str = include_str!("../regression-seeds.txt");

fn environment_number(name: &str) -> Option<u64> {
    let text = std::env::var(name).ok()?;
    Some(
        text.parse()
            .unwrap_or_else(|error| panic!("{name}={text:?} is not a number: {error}")),
    )
}

fn environment_command_count() -> usize {
    let commands =
        environment_number("DEXZD_SIMULATION_COMMANDS").unwrap_or(DEFAULT_COMMANDS_PER_SEED as u64);
    assert!(
        (1..=MAX_COMMANDS_PER_SEED as u64).contains(&commands),
        "DEXZD_SIMULATION_COMMANDS must be an integer from 1 to {MAX_COMMANDS_PER_SEED}"
    );
    commands as usize
}

#[test]
fn apply_keeps_every_invariant_over_random_command_sequences() {
    let commands = environment_command_count();
    let seeds: BTreeMap<u64, usize> = match environment_number("DEXZD_SIMULATION_SEED") {
        Some(seed) => BTreeMap::from([(seed, commands)]),
        None => {
            let count = environment_number("DEXZD_SIMULATION_SEEDS").unwrap_or(DEFAULT_SEED_COUNT);
            assert!(
                (1..=MAX_SEED_COUNT).contains(&count),
                "DEXZD_SIMULATION_SEEDS must be an integer from 1 to {MAX_SEED_COUNT}"
            );
            assert!(
                count * commands as u64 <= MAX_RANDOM_COMMANDS,
                "random simulation budget exceeds {MAX_RANDOM_COMMANDS} commands"
            );
            let first_seed = environment_number("DEXZD_SIMULATION_BASE_SEED").unwrap_or(FIRST_SEED);
            let end_seed = first_seed
                .checked_add(count)
                .expect("simulation seed range overflow");
            let mut seeds: BTreeMap<u64, usize> = (first_seed..end_seed)
                .map(|seed| (seed, commands))
                .collect();
            for (seed, regression_commands) in regression_seed_cases() {
                seeds
                    .entry(seed)
                    .and_modify(|count| *count = (*count).max(regression_commands))
                    .or_insert(regression_commands);
            }
            seeds
        }
    };
    let failure_directory = std::env::var_os("DEXZD_SIMULATION_FAILURE_DIR").map(PathBuf::from);
    let mut failing_seeds = Vec::new();
    let mut total = SimulationSummary::default();
    for (seed, command_count) in seeds {
        match run_seed(seed, command_count, |failure| {
            write_failure_report(failure_directory.as_ref(), failure);
        }) {
            Ok(summary) => total.merge(&summary),
            Err(failure) => {
                write_failure_report(failure_directory.as_ref(), &failure);
                failing_seeds.push(seed);
                break;
            }
        }
    }
    assert!(
        failing_seeds.is_empty(),
        "simulation failed for seeds: {failing_seeds:?}"
    );
    println!("simulation coverage: {total:#?}");
    for kind in [
        "FlowStart",
        "FlowStop",
        "FlowConfigUpdate",
        "AttributesSet",
        "ChannelPublish",
        "ChannelMessageDelete",
        "StepAttemptStarted",
        "StepAttemptHeartbeat",
        "StepAttemptFailed",
        "StepMethodCompleted",
        "TimersFired",
    ] {
        assert!(
            total.applied(kind) > 0,
            "the seed set never applied a {kind} command: {total:#?}"
        );
    }
    for class in INVALID_CLIENT_INPUT_CLASSES {
        assert!(
            total.size_rejections.get(class).copied().unwrap_or(0) > 0,
            "the seed set never had apply reject an {class} for its size: {total:#?}"
        );
    }
}

#[expect(clippy::print_stderr, reason = "test failures need a console report")]
fn write_failure_report(directory: Option<&PathBuf>, failure: &SimulationFailure) {
    let report = failure.to_string();
    eprintln!("{report}");
    if let Some(directory) = directory {
        std::fs::create_dir_all(directory).expect("create simulation failure directory");
        let pending_report = directory.join(format!("{}.tmp", failure.seed));
        std::fs::write(&pending_report, report).expect("write simulation failure report");
        std::fs::rename(
            pending_report,
            directory.join(format!("{}.txt", failure.seed)),
        )
        .expect("publish simulation failure report");
    }
}

fn regression_seed_cases() -> Vec<(u64, usize)> {
    REGRESSION_SEEDS
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let mut fields = line.split_whitespace();
            let seed = fields
                .next()
                .expect("regression seed is missing")
                .parse()
                .expect("regression seed is not a number");
            let command_count = fields
                .next()
                .expect("regression command count is missing")
                .parse()
                .expect("regression command count is not a number");
            assert!(fields.next().is_none(), "regression seed has extra fields");
            (seed, command_count)
        })
        .collect()
}

#[test]
fn the_same_seed_proposes_and_applies_the_same_commands() {
    let run = || {
        let mut checker = ApplyChecker::new();
        let mut generator = CommandGenerator::new(FIRST_SEED);
        let mut trace = Vec::new();
        for _ in 0..80 {
            let command = generator.next_command(&checker).expect("state decodes");
            let outcome = checker
                .apply_and_check(&command)
                .unwrap_or_else(|violation| panic!("{violation}"));
            trace.push((command, outcome));
        }
        (trace, checker.state().clone())
    };
    let (first_trace, first_state) = run();
    let (second_trace, second_state) = run();
    assert!(first_trace == second_trace, "the command traces differ");
    assert!(first_state == second_state, "the final states differ");
}

#[test]
fn overwritten_and_deleted_attributes_are_restored_with_their_values_after_travel() {
    let mut restored_overwritten = 0;
    let mut restored_deleted = 0;
    for seed in FIRST_SEED..FIRST_SEED + 16 {
        let mut checker = ApplyChecker::new();
        let mut generator = CommandGenerator::new(seed);
        let mut point_values: BTreeMap<(RunId, u64), RecordedAttributeValues> = BTreeMap::new();
        let mut explicit_deletions: BTreeMap<(RunId, String), u64> = BTreeMap::new();
        for _ in 0..DEFAULT_COMMANDS_PER_SEED {
            let command = generator.next_command(&checker).expect("state decodes");
            let pretravel_attributes = match &command.command {
                Some(Command::TimeTravel(travel)) => travel.request.as_ref().and_then(|request| {
                    (request.reason == "inspect an earlier value").then(|| {
                        let run_id = RunId::from_slice(&command.run_id).expect("generated run ID");
                        let view = checker
                            .view(&command.flow_id, RunSelection::All)
                            .expect("current state decodes");
                        let run = view.runs.get(&run_id).expect("target run exists");
                        (run_id, run.attributes.clone())
                    })
                }),
                _ => None,
            };
            let outcome = checker
                .apply_and_check(&command)
                .unwrap_or_else(|violation| panic!("{violation}"));
            let view = checker
                .view(&command.flow_id, RunSelection::All)
                .expect("applied state decodes");
            if let Some((run_id, previous_attributes)) = pretravel_attributes
                && let Some(command_reply::Reply::TimeTravel(reply)) = &outcome.reply.reply
            {
                let run = view.runs.get(&run_id).expect("traveled run exists");
                let target_seq = run
                    .attempts
                    .get(&reply.run_attempt)
                    .expect("new attempt exists")
                    .parent_seq;
                let expected_values = point_values
                    .get(&(run_id, target_seq))
                    .expect("target point was recorded");
                for (key, (expected_id, expected_bytes)) in expected_values {
                    let previous_id = previous_attributes
                        .get(key)
                        .and_then(|attribute| attribute.value.as_ref())
                        .and_then(table_value_id);
                    if previous_id == Some(*expected_id) {
                        continue;
                    }
                    if previous_attributes.get(key).is_some_and(|attribute| {
                        matches!(
                            attribute
                                .value
                                .as_ref()
                                .and_then(|value| value.kind.as_ref()),
                            Some(stored_value::Kind::HardDeleteObject(_))
                        )
                    }) {
                        continue;
                    }
                    let restored = run.attributes.get(key).expect("target Attribute restored");
                    let restored_id = restored
                        .value
                        .as_ref()
                        .and_then(table_value_id)
                        .unwrap_or_else(|| panic!("Attribute {key} at target {target_seq} restored as {restored:?}, expected value {expected_id:?}"));
                    assert_eq!(
                        restored_id, *expected_id,
                        "Attribute {key} value ID changed"
                    );
                    let value_key = value_row_key(
                        checker.context(&command.flow_id).flow_hash,
                        run_id,
                        RunTable::Value,
                        restored_id,
                    );
                    let restored_bytes = checker
                        .state()
                        .row(ColumnFamily::Value, &value_key)
                        .expect("restored Attribute value is readable");
                    assert_eq!(
                        restored_bytes, expected_bytes,
                        "Attribute {key} bytes changed"
                    );
                    if previous_attributes.contains_key(key) {
                        restored_overwritten += 1;
                    } else if explicit_deletions
                        .get(&(run_id, key.clone()))
                        .is_some_and(|deletion_seq| *deletion_seq > target_seq)
                    {
                        restored_deleted += 1;
                    }
                }
            }
            if !outcome.writes.is_empty()
                && let Some(Command::AttributesSet(set)) = &command.command
                && let Some(request) = &set.request
            {
                let run_id = RunId::from_slice(&command.run_id).expect("generated run ID");
                if let Some(run) = view.runs.get(&run_id) {
                    let deletion_seq = run.record.as_ref().expect("run record").next_run_seq - 1;
                    for write in &request.attributes {
                        let is_explicit_delete = write.value.as_ref().is_none_or(|value| {
                            matches!(value.kind, None | Some(value::Kind::NullValue(_)))
                        });
                        if is_explicit_delete && !run.attributes.contains_key(&write.key) {
                            explicit_deletions.insert((run_id, write.key.clone()), deletion_seq);
                        }
                    }
                }
            }
            for (run_id, run) in &view.runs {
                let flow_hash = checker.context(&command.flow_id).flow_hash;
                for (seq, _) in run
                    .history
                    .iter()
                    .filter(|(_, history)| history.is_semantic)
                {
                    point_values.entry((*run_id, *seq)).or_insert_with(|| {
                        run.attributes
                            .iter()
                            .filter_map(|(key, attribute)| {
                                let value_id = attribute.value.as_ref().and_then(table_value_id)?;
                                let value_key =
                                    value_row_key(flow_hash, *run_id, RunTable::Value, value_id);
                                let bytes = checker
                                    .state()
                                    .row(ColumnFamily::Value, &value_key)
                                    .expect("recorded Attribute value is readable");
                                Some((key.clone(), (value_id, bytes.to_vec())))
                            })
                            .collect()
                    });
                }
            }
        }
        if restored_overwritten > 0 && restored_deleted > 0 {
            break;
        }
    }
    assert!(
        restored_overwritten > 0,
        "no overwritten Attribute was restored"
    );
    assert!(restored_deleted > 0, "no deleted Attribute was restored");
}

fn table_value_id(stored: &StoredValue) -> Option<ValueId> {
    let stored_value::Kind::TableValue(reference) = stored.kind.as_ref()? else {
        return None;
    };
    let value_id = reference.value_id?;
    Some(ValueId {
        run_seq: value_id.run_seq,
        index_in_command: value_id.index_in_command,
    })
}

#[test]
fn pinned_snapshot_value_survives_window_advance_and_final_holder_release() {
    let mut checker = ApplyChecker::new();
    let mut generator = CommandGenerator::new(FIRST_SEED + 3);
    let mut pinned_value: Option<(RunId, ValueId, Vec<u8>)> = None;
    for _ in 0..DEFAULT_COMMANDS_PER_SEED {
        let command = generator.next_command(&checker).expect("state decodes");
        checker
            .apply_and_check(&command)
            .unwrap_or_else(|violation| panic!("{violation}"));
        let view = checker
            .view(&command.flow_id, RunSelection::All)
            .expect("applied state decodes");
        for (run_id, run) in &view.runs {
            if let Some((pinned_run_id, pinned_value_id, expected_bytes)) = pinned_value.as_ref()
                && run_id == pinned_run_id
                && let Some(birth) = run.value_births.get(pinned_value_id)
                && birth.garbage_seq > 0
            {
                let record = run.record.as_ref().expect("retained run record");
                assert!(
                    record.window_start_seq < birth.garbage_seq,
                    "window no longer retains an in-flight snapshot point"
                );
                assert!(
                    run.value_rows.contains(pinned_value_id),
                    "retained snapshot value was reclaimed at final pin release"
                );
                let value_key = value_row_key(
                    checker.context(&command.flow_id).flow_hash,
                    *run_id,
                    RunTable::Value,
                    *pinned_value_id,
                );
                assert_eq!(
                    checker.state().row(ColumnFamily::Value, &value_key),
                    Some(expected_bytes.as_slice()),
                    "retained snapshot value bytes changed"
                );
                let (_, target) = run
                    .history
                    .iter()
                    .rev()
                    .find(|(seq, history)| {
                        *seq >= record.window_start_seq
                            && *seq < birth.garbage_seq
                            && history.is_semantic
                            && history.attempt == record.current_attempt
                    })
                    .expect("retained in-flight semantic point");
                let mut travel = command.clone();
                travel.run_id = run_id.as_bytes().to_vec();
                travel.expected_run_attempt = 0;
                travel.proposed_at_micros += 1;
                travel.command_id = vec![0x83; 16];
                travel.command = Some(Command::TimeTravel(TimeTravelCommand {
                    request: Some(ResetFlowRequest {
                        flow_id: command.flow_id.clone(),
                        reset_type: FlowResetType::HistoryEventTime as i32,
                        history_event_time: (i128::from(target.timestamp_micros) * 1000)
                            .to_string(),
                        reason: "inspect a pinned snapshot value".to_owned(),
                        skip_writes_reapply: true,
                        ..ResetFlowRequest::default()
                    }),
                }));
                let outcome = checker
                    .apply_and_check(&travel)
                    .unwrap_or_else(|violation| panic!("{violation}"));
                assert!(matches!(
                    outcome.reply.reply,
                    Some(command_reply::Reply::TimeTravel(_))
                ));
                let traveled_view = checker
                    .view(&command.flow_id, RunSelection::All)
                    .expect("traveled state decodes");
                let traveled_run = traveled_view.runs.get(run_id).expect("traveled run exists");
                assert!(traveled_run.step_executions.values().any(|step| {
                    step.invocation
                        .as_ref()
                        .and_then(|invocation| invocation.snapshot.as_ref())
                        .is_some_and(|snapshot| {
                            snapshot.attributes.iter().any(|attribute| {
                                attribute.value.as_ref().and_then(table_value_id)
                                    == Some(*pinned_value_id)
                            })
                        })
                }));
                assert_eq!(
                    checker.state().row(ColumnFamily::Value, &value_key),
                    Some(expected_bytes.as_slice()),
                    "traveled snapshot value bytes changed"
                );
                return;
            }
            let window_start = run
                .record
                .as_ref()
                .map_or(0, |record| record.window_start_seq);
            for step in run.step_executions.values() {
                let Some(snapshot) = step
                    .invocation
                    .as_ref()
                    .and_then(|invocation| invocation.snapshot.as_ref())
                else {
                    continue;
                };
                for attribute in &snapshot.attributes {
                    let Some(value_id) = attribute.value.as_ref().and_then(table_value_id) else {
                        continue;
                    };
                    let Some(current) = run.attributes.get(&attribute.key) else {
                        continue;
                    };
                    let current_value_id = current.value.as_ref().and_then(table_value_id);
                    if pinned_value.is_none()
                        && current_value_id != Some(value_id)
                        && window_start > current.written_seq
                    {
                        let value_key = value_row_key(
                            checker.context(&command.flow_id).flow_hash,
                            *run_id,
                            RunTable::Value,
                            value_id,
                        );
                        let bytes = checker
                            .state()
                            .row(ColumnFamily::Value, &value_key)
                            .expect("in-flight snapshot value is readable");
                        pinned_value = Some((*run_id, value_id, bytes.to_vec()));
                    }
                }
            }
        }
    }
    panic!("no pinned snapshot value outlived its slot and reached final holder release");
}

#[test]
fn value_reclamation_follows_the_last_retained_semantic_point() {
    let mut checker = ApplyChecker::new();
    let mut generator = CommandGenerator::new(FIRST_SEED);
    let mut recorded_points = BTreeSet::new();
    let mut seen_attribute_values = BTreeSet::new();
    let mut last_attribute_reference_seq = BTreeMap::new();
    let mut has_advanced_window = false;
    let mut has_reclaimed_aged_out_value = false;
    for _ in 0..2_000 {
        let mut command = generator.next_command(&checker).expect("state decodes");
        if let Some(Command::FlowStart(start)) = command.command.as_mut() {
            let settings = start.settings.as_mut().expect("generated run settings");
            settings.history_window_semantic_events = 2;
            settings.history_prune_chunk_semantic_events = 1;
        }
        checker
            .apply_and_check(&command)
            .unwrap_or_else(|violation| panic!("{violation}"));
        let view = checker
            .view(&command.flow_id, RunSelection::All)
            .expect("applied state decodes");
        for (run_id, run) in &view.runs {
            let window_start = run
                .record
                .as_ref()
                .map_or(0, |record| record.window_start_seq);
            has_advanced_window |= window_start > 1;
            for (seq, history) in &run.history {
                if !history.is_semantic || !recorded_points.insert((*run_id, *seq)) {
                    continue;
                }
                for value_id in run
                    .attributes
                    .values()
                    .filter_map(|attribute| attribute.value.as_ref().and_then(table_value_id))
                {
                    seen_attribute_values.insert((*run_id, value_id));
                    last_attribute_reference_seq.insert((*run_id, value_id), *seq);
                }
            }
            for (recorded_run_id, value_id) in seen_attribute_values
                .iter()
                .filter(|(seen_run_id, _)| seen_run_id == run_id)
            {
                let last_seq = last_attribute_reference_seq[&(*recorded_run_id, *value_id)];
                let is_present =
                    run.value_rows.contains(value_id) && run.value_births.contains_key(value_id);
                if last_seq >= window_start {
                    assert!(
                        is_present,
                        "retained point {last_seq} lost value {value_id:?}"
                    );
                } else if !run.value_rows.contains(value_id)
                    && !run.value_births.contains_key(value_id)
                {
                    has_reclaimed_aged_out_value = true;
                }
            }
        }
        if has_advanced_window && has_reclaimed_aged_out_value {
            break;
        }
    }
    assert!(has_advanced_window, "small history window never advanced");
    assert!(
        has_reclaimed_aged_out_value,
        "no Attribute value was reclaimed after its last point aged out"
    );
}
