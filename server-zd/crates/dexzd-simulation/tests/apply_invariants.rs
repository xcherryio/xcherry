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

use dexzd_simulation::{
    ApplyChecker, CommandGenerator, INVALID_CLIENT_INPUT_CLASSES, SimulationFailure,
    SimulationSummary, run_seed,
};
use std::collections::BTreeMap;
use std::path::PathBuf;

const DEFAULT_SEED_COUNT: u64 = 64;
const DEFAULT_COMMANDS_PER_SEED: usize = 400;
const MAX_SEED_COUNT: u64 = 1000;
const MAX_COMMANDS_PER_SEED: usize = 2000;
const MAX_RANDOM_COMMANDS: u64 = 500_000;
/// First seed of the fixed set; any constant keeps the set reproducible.
const FIRST_SEED: u64 = 0x5eed_0000;
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
