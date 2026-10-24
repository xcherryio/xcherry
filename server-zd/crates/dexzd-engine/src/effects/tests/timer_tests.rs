// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::time::Duration;

use dexzd_apply::ApplyEffect;
use dexzd_keyspace::row_keys::{decode_timer_index_key, timer_index_key, timer_key};
use dexzd_keyspace::{ColumnFamily, RunId};
use dexzd_protocol::dexzd::v1::{
    TimerId, TimerRecord, WaitConditionTimerId, flow_command, timer_id,
};
use prost::Message;

use super::{CommandOverride, EffectHarness, is_timers_fired, ok_reply, settings, start_request};
use crate::effects::EffectsConfig;
use crate::runtime::RuntimeEffectSink;
use dexzd_clock::WallClock;

const MILLI_MICROS: i64 = 1000;

pub(super) fn wait_timer(index: u32) -> TimerId {
    TimerId {
        run_attempt: 1,
        kind: Some(timer_id::Kind::WaitCondition(WaitConditionTimerId {
            step_execution_id: "ChargeStep-1".to_owned(),
            timer_index: index,
        })),
    }
}

/// Writes a timer row and its index entry as apply would.
pub(super) fn seed_timer(
    harness: &EffectHarness,
    flow_id: &str,
    run_id: RunId,
    timer: &TimerId,
    fire_at_micros: i64,
) {
    let run = harness.run_location(flow_id, run_id);
    let encoded = timer.encode_to_vec();
    let mut state = harness.runtime.state.lock().expect("lock");
    state.put_message(
        ColumnFamily::Flow,
        timer_key(run.flow_hash, run.run_id, &encoded),
        &TimerRecord {
            timer_id: Some(timer.clone()),
            fire_at_micros,
        },
    );
    state.rows.insert(
        (
            ColumnFamily::PartitionIndex,
            timer_index_key(
                harness.partition_of(flow_id),
                fire_at_micros,
                run.flow_hash,
                run.run_id,
                &encoded,
            ),
        ),
        Vec::new(),
    );
}

/// Simulates apply of TimersFired: removes the fired timers from the index.
pub(super) fn timers_fired_override() -> CommandOverride {
    Box::new(|command, state| {
        let Some(flow_command::Command::TimersFired(fired)) = &command.command else {
            return None;
        };
        let encoded: Vec<Vec<u8>> = fired.timers.iter().map(Message::encode_to_vec).collect();
        state.rows.retain(|(column_family, key), _| {
            *column_family != ColumnFamily::PartitionIndex
                || decode_timer_index_key(key).is_none_or(|entry| {
                    entry.run_id.as_bytes().as_slice() != command.run_id.as_slice()
                        || !encoded.contains(&entry.encoded_timer_id)
                })
        });
        Some(ok_reply())
    })
}

fn fired_commands(harness: &EffectHarness) -> Vec<(Vec<u8>, Vec<TimerId>, i64)> {
    harness
        .runtime
        .commands_matching(is_timers_fired)
        .into_iter()
        .filter_map(|recorded| match recorded.command.command {
            Some(flow_command::Command::TimersFired(fired)) => Some((
                recorded.command.run_id,
                fired.timers,
                recorded.command.proposed_at_micros,
            )),
            _ => None,
        })
        .collect()
}

pub(super) async fn sleep_until_micros(harness: &EffectHarness, micros: i64) {
    let wait = u64::try_from(micros - harness.clock.now_unix_micros()).unwrap_or(0);
    tokio::time::sleep(Duration::from_micros(wait)).await;
}

#[tokio::test(start_paused = true)]
async fn due_timers_fire_in_time_order_merged_per_run_and_never_early() {
    let harness = EffectHarness::new();
    harness.rebuild_all_partitions().await;
    harness.runtime.add_override(timers_fired_override());
    let first_run = harness.start_flow(start_request("order-1"), settings());
    let second_run = harness.start_flow(start_request("order-2"), settings());
    let base = harness.clock.now_unix_micros();
    seed_timer(
        &harness,
        "order-1",
        first_run,
        &wait_timer(0),
        base + 5000 * MILLI_MICROS,
    );
    seed_timer(
        &harness,
        "order-1",
        first_run,
        &wait_timer(1),
        base + 5000 * MILLI_MICROS,
    );
    seed_timer(
        &harness,
        "order-2",
        second_run,
        &wait_timer(0),
        base + 3000 * MILLI_MICROS,
    );
    seed_timer(
        &harness,
        "order-1",
        first_run,
        &wait_timer(2),
        base + 8000 * MILLI_MICROS,
    );
    for flow_id in ["order-1", "order-2"] {
        harness.runner.handle(
            &EffectHarness::flow(flow_id),
            &[ApplyEffect::TimerArmed {
                fire_at_micros: base + 3000 * MILLI_MICROS,
            }],
            true,
        );
    }

    sleep_until_micros(&harness, base + 2999 * MILLI_MICROS).await;
    harness.settle().await;
    assert!(fired_commands(&harness).is_empty(), "no timer fires early");
    sleep_until_micros(&harness, base + 3000 * MILLI_MICROS).await;
    harness.settle().await;
    sleep_until_micros(&harness, base + 5000 * MILLI_MICROS).await;
    harness.settle().await;
    sleep_until_micros(&harness, base + 8000 * MILLI_MICROS).await;
    harness.settle().await;

    let fired = fired_commands(&harness);
    assert_eq!(fired.len(), 3);
    assert_eq!(fired[0].0, second_run.as_bytes().to_vec());
    assert_eq!(fired[0].1, vec![wait_timer(0)]);
    assert_eq!(fired[1].0, first_run.as_bytes().to_vec());
    assert_eq!(fired[1].1, vec![wait_timer(0), wait_timer(1)]);
    assert_eq!(fired[2].1, vec![wait_timer(2)]);
    for ((_, _, proposed_at), fire_at) in fired.iter().zip([3000, 5000, 8000]) {
        assert!(*proposed_at >= base + fire_at * MILLI_MICROS);
    }
    harness.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn timers_beyond_the_lookahead_are_loaded_by_a_later_pass() {
    let config = EffectsConfig {
        timer_lookahead: Duration::from_secs(60),
        ..EffectsConfig::default()
    };
    let harness = EffectHarness::with_config(super::DurableState::new(), config);
    harness.rebuild_all_partitions().await;
    harness.runtime.add_override(timers_fired_override());
    let run_id = harness.start_flow(start_request("order-1"), settings());
    let base = harness.clock.now_unix_micros();
    // No TimerArmed notification: only the periodic rescan can find this timer.
    seed_timer(
        &harness,
        "order-1",
        run_id,
        &wait_timer(0),
        base + 150 * 1_000_000,
    );
    let seeded_fired = |harness: &EffectHarness| {
        fired_commands(harness)
            .into_iter()
            .filter(|(_, timers, _)| timers.contains(&wait_timer(0)))
            .count()
    };
    sleep_until_micros(&harness, base + 149 * 1_000_000).await;
    harness.settle().await;
    assert_eq!(seeded_fired(&harness), 0);
    sleep_until_micros(&harness, base + 151 * 1_000_000).await;
    harness.settle().await;
    assert_eq!(seeded_fired(&harness), 1);
    harness.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_timer_left_in_the_index_is_retried_after_the_delay_not_in_a_loop() {
    let harness = EffectHarness::new();
    harness.rebuild_all_partitions().await;
    // Apply accepts TimersFired but this timer stays in the index.
    harness
        .runtime
        .add_override(super::ok_override(is_timers_fired));
    let run_id = harness.start_flow(start_request("order-1"), settings());
    let base = harness.clock.now_unix_micros();
    seed_timer(
        &harness,
        "order-1",
        run_id,
        &wait_timer(0),
        base + 1000 * MILLI_MICROS,
    );
    harness.runner.handle(
        &EffectHarness::flow("order-1"),
        &[ApplyEffect::TimerArmed {
            fire_at_micros: base + 1000 * MILLI_MICROS,
        }],
        true,
    );
    sleep_until_micros(&harness, base + 1500 * MILLI_MICROS).await;
    harness.settle().await;
    assert_eq!(fired_commands(&harness).len(), 1);
    sleep_until_micros(&harness, base + 2100 * MILLI_MICROS).await;
    harness.settle().await;
    assert_eq!(fired_commands(&harness).len(), 2);
    harness.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn due_timers_behind_a_full_page_of_timers_in_backoff_still_fire() {
    let config = EffectsConfig {
        timer_load_limit: 2,
        ..EffectsConfig::default()
    };
    let harness = EffectHarness::with_config(super::DurableState::new(), config);
    harness.rebuild_all_partitions().await;
    harness.runtime.add_override(timers_fired_override());
    let run_id = harness.start_flow(start_request("order-1"), settings());
    // Timers of a missing run stay in the index; each pass puts them back into backoff.
    let missing_run = RunId::from_slice(&[3; 16]).expect("16-byte run ID");
    let base = harness.clock.now_unix_micros();
    for index in 0..3 {
        seed_timer(
            &harness,
            "order-1",
            missing_run,
            &wait_timer(index),
            base - i64::from(3 - index) * MILLI_MICROS,
        );
    }
    seed_timer(&harness, "order-1", run_id, &wait_timer(0), base);
    harness.runner.handle(
        &EffectHarness::flow("order-1"),
        &[ApplyEffect::TimerArmed {
            fire_at_micros: base,
        }],
        true,
    );
    sleep_until_micros(&harness, base + 10 * MILLI_MICROS).await;
    harness.settle().await;

    let fired = fired_commands(&harness);
    assert_eq!(fired.len(), 1, "{fired:?}");
    assert_eq!(fired[0].0, run_id.as_bytes().to_vec());
    assert_eq!(fired[0].1, vec![wait_timer(0)]);
    harness.shutdown().await;
}
