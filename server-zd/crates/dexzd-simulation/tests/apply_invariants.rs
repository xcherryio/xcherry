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
//!
//! `DEXZD_SIMULATION_STRICT=1` stops tolerating the known apply defects.

use dexzd_simulation::{
    ApplyChecker, CommandGenerator, DefectTolerance, SimulationSummary, run_seed,
};

const DEFAULT_SEED_COUNT: u64 = 64;
const DEFAULT_COMMANDS_PER_SEED: usize = 400;
/// First seed of the fixed set; any constant keeps the set reproducible.
const FIRST_SEED: u64 = 0x5eed_0000;

fn environment_number(name: &str) -> Option<u64> {
    let text = std::env::var(name).ok()?;
    Some(
        text.parse()
            .unwrap_or_else(|error| panic!("{name}={text:?} is not a number: {error}")),
    )
}

fn defect_tolerance() -> DefectTolerance {
    if environment_number("DEXZD_SIMULATION_STRICT").unwrap_or(0) != 0 {
        DefectTolerance::Strict
    } else {
        DefectTolerance::TolerateKnownApplyDefects
    }
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
    let tolerance = defect_tolerance();
    let mut total = SimulationSummary::default();
    for seed in seeds {
        match run_seed(seed, commands, tolerance) {
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
}

#[test]
fn the_same_seed_proposes_and_applies_the_same_commands() {
    let run = || {
        let mut checker = ApplyChecker::new(defect_tolerance());
        let mut generator = CommandGenerator::new(FIRST_SEED, defect_tolerance());
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
