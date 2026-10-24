// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Engine-level deterministic simulation over a fixed seed set.
//!
//! Set `DEXZD_SIMULATION_SEEDS` and `DEXZD_SIMULATION_SECONDS` for a longer budget, or
//! `DEXZD_SIMULATION_SEED` for one seed.

use dexzd_simulation::{EngineSimulationConfig, EngineSimulationSummary, run_engine_seed};

const DEFAULT_SEED_COUNT: u64 = 8;
const DEFAULT_ACTIVE_SECONDS: u64 = 120;
/// First seed of the fixed set; any constant keeps the set reproducible.
const FIRST_SEED: u64 = 0xe5e5_0000;

fn environment_number(name: &str) -> Option<u64> {
    let text = std::env::var(name).ok()?;
    Some(
        text.parse()
            .unwrap_or_else(|error| panic!("{name}={text:?} is not a number: {error}")),
    )
}

fn configured() -> (Vec<u64>, EngineSimulationConfig) {
    let config = EngineSimulationConfig {
        active_seconds: environment_number("DEXZD_SIMULATION_SECONDS")
            .unwrap_or(DEFAULT_ACTIVE_SECONDS),
        ..EngineSimulationConfig::default()
    };
    let seeds = match environment_number("DEXZD_SIMULATION_SEED") {
        Some(seed) => vec![seed],
        None => {
            let count = environment_number("DEXZD_SIMULATION_SEEDS").unwrap_or(DEFAULT_SEED_COUNT);
            (FIRST_SEED..FIRST_SEED + count).collect()
        }
    };
    (seeds, config)
}

#[test]
fn the_engine_keeps_every_invariant_through_faults_and_crashes() {
    let (seeds, config) = configured();
    let mut total = EngineSimulationSummary::default();
    for seed in seeds {
        match run_engine_seed(seed, &config) {
            Ok(summary) => total.merge(&summary),
            Err(failure) => panic!("{failure}"),
        }
    }
    if std::env::var_os("DEXZD_SIMULATION_PRINT_EVENTS").is_some() {
        for event in &total.recent_events {
            println!("{event}");
        }
    }
    total.recent_events.clear();
    println!("engine simulation coverage: {total:#?}");
}
