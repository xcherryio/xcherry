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

use std::collections::BTreeMap;

use dexzd_keyspace::row_keys::value_row_key;
use dexzd_keyspace::{ColumnFamily, RunId, RunTable, ValueId};
use dexzd_protocol::dexzd::v1::{StoredValue, command_reply, flow_command::Command, stored_value};
use dexzd_simulation::{
    ApplyChecker, CommandGenerator, INVALID_CLIENT_INPUT_CLASSES, RunSelection, SimulationSummary,
    run_seed,
};

const DEFAULT_SEED_COUNT: u64 = 64;
const DEFAULT_COMMANDS_PER_SEED: usize = 400;
/// First seed of the fixed set; any constant keeps the set reproducible.
const FIRST_SEED: u64 = 0x5eed_0000;
type RecordedAttributeValues = BTreeMap<String, (ValueId, Vec<u8>)>;

fn environment_number(name: &str) -> Option<u64> {
    let text = std::env::var(name).ok()?;
    Some(
        text.parse()
            .unwrap_or_else(|error| panic!("{name}={text:?} is not a number: {error}")),
    )
}

#[test]
fn apply_keeps_every_invariant_over_random_command_sequences() {
    let commands = environment_number("DEXZD_SIMULATION_COMMANDS")
        .map_or(DEFAULT_COMMANDS_PER_SEED, |count| count as usize);
    let seeds: Vec<u64> = match environment_number("DEXZD_SIMULATION_SEED") {
        Some(seed) => vec![seed],
        None => {
            let count = environment_number("DEXZD_SIMULATION_SEEDS").unwrap_or(DEFAULT_SEED_COUNT);
            (FIRST_SEED..FIRST_SEED + count).collect()
        }
    };
    let mut total = SimulationSummary::default();
    for seed in seeds {
        match run_seed(seed, commands) {
            Ok(summary) => total.merge(&summary),
            Err(failure) => panic!("{failure}"),
        }
    }
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
                    } else {
                        restored_deleted += 1;
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
