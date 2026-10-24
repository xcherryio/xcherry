// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Engine simulation over a fixed seed set; the crate README lists its environment variables.

use dexzd_simulation::{EngineSimulationConfig, EngineSimulationSummary, run_engine_seed};

const DEFAULT_SEED_COUNT: u64 = 64;
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

/// Returns the seeds to run, the configuration, and whether one seed was requested.
fn configured() -> (Vec<u64>, EngineSimulationConfig, bool) {
    let config = EngineSimulationConfig {
        active_seconds: environment_number("DEXZD_SIMULATION_SECONDS")
            .unwrap_or(DEFAULT_ACTIVE_SECONDS),
        ..EngineSimulationConfig::default()
    };
    match environment_number("DEXZD_SIMULATION_SEED") {
        Some(seed) => (vec![seed], config, true),
        None => {
            let count = environment_number("DEXZD_SIMULATION_SEEDS").unwrap_or(DEFAULT_SEED_COUNT);
            ((FIRST_SEED..FIRST_SEED + count).collect(), config, false)
        }
    }
}

#[test]
fn the_engine_keeps_every_invariant_through_faults_and_crashes() {
    let (seeds, config, is_single_seed) = configured();
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
    if is_single_seed {
        return;
    }
    assert_covered(&total);
}

/// The fixed seed set must exercise every fault, crash, and Worker behavior the harness models.
fn assert_covered(total: &EngineSimulationSummary) {
    let count = |counts: &std::collections::BTreeMap<String, u64>, name: &str| {
        counts.get(name).copied().unwrap_or(0)
    };
    for restart in ["kept", "lost", "rolled-back"] {
        assert!(
            count(&total.restarts_by_local_store, restart) > 0,
            "no restart with a {restart} local store: {total:#?}"
        );
    }
    for fault in [
        "SlowResponse",
        "ServerError",
        "Throttled",
        "ConditionalRequestConflict",
        "Timeout",
        "ResponseLost",
    ] {
        assert!(
            count(&total.injected_faults, fault) > 0,
            "no injected {fault}: {total:#?}"
        );
    }
    for behavior in [
        "Succeed",
        "Fail",
        "Hang",
        "Slow",
        "RepeatResult",
        "DisconnectBeforeOutput",
        "DisconnectAfterOutput",
        "NoWorker",
        "Hold",
        "HeartbeatWithValue",
        "HeartbeatWithoutValue",
        "RpcSucceed",
        "RpcFail",
        "RpcTimeout",
    ] {
        assert!(
            count(&total.worker_behaviors, behavior) > 0,
            "no Worker behaved as {behavior}: {total:#?}"
        );
    }
    for kind in [
        "FlowStart",
        "FlowStop",
        "AttributesSet",
        "ChannelPublish",
        "ChannelMessageDelete",
        "RpcCommitted",
        "StepAttemptStarted",
        "StepAttemptHeartbeat",
        "StepAttemptFailed",
        "StepMethodCompleted",
        "TimersFired",
        "RunPurge",
    ] {
        assert!(
            count(&total.applied_by_kind, kind) > 0,
            "the seed set never committed a {kind} command: {total:#?}"
        );
    }
    for (counts, name) in [
        (&total.precondition_failed_puts, "log"),
        (&total.precondition_failed_puts, "lease"),
        (&total.deleted_objects, "log"),
        (&total.snapshot_reads_during_start, "lost"),
        (&total.snapshot_reads_during_start, "takeover"),
    ] {
        assert!(
            count(counts, name) > 0,
            "missing {name} in a coverage count: {total:#?}"
        );
    }
    let rejected_results = count(&total.rejected_by_kind, "StepMethodCompleted")
        + count(&total.rejected_by_kind, "StepAttemptFailed");
    let checks = [
        ("rejected duplicate results", rejected_results),
        ("old-epoch frames", total.old_epoch_frames),
        (
            "attempts compared after the quiet crash",
            total.attempts_compared_after_quiet_crash,
        ),
        ("runs in the final state", total.final_runs as u64),
        ("old-epoch overlaps", total.old_epoch_overlaps),
        ("crashes during start", total.crashes_during_start),
        ("self-stopped processes", total.self_stopped_processes),
        ("clock jumps", total.clock_jumps),
        ("acknowledged writes", total.acknowledged_writes),
        ("checked read values", total.checked_read_values),
        ("replayed frames", total.replayed_frames),
        ("state comparisons", total.state_comparisons),
        ("dispatch comparisons", total.dispatch_comparisons),
    ];
    for (name, value) in checks {
        assert!(value > 0, "the seed set had no {name}: {total:#?}");
    }
}

#[test]
fn one_seed_produces_the_same_event_trace_twice() {
    let config = EngineSimulationConfig {
        active_seconds: 60,
        calm_tail_seconds: 30,
    };
    for seed in [FIRST_SEED, FIRST_SEED + 1, FIRST_SEED + 2] {
        let first = run_engine_seed(seed, &config).unwrap_or_else(|failure| panic!("{failure}"));
        let second = run_engine_seed(seed, &config).unwrap_or_else(|failure| panic!("{failure}"));
        if first.trace_hash != second.trace_hash {
            let divergence = first
                .recent_events
                .iter()
                .zip(&second.recent_events)
                .find(|(left, right)| left != right);
            panic!(
                "seed {seed} produced different traces: {} events hash {:x}, then {} events hash \
                 {:x}; first difference among the last events: {divergence:?}",
                first.trace_events, first.trace_hash, second.trace_events, second.trace_hash
            );
        }
        assert!(
            first.trace_events > 300,
            "seed {seed} produced only {} events",
            first.trace_events
        );
    }
}
