// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! An expired deadline wins over a Worker event or state read ready at once, and outlasts a
//! stalled read.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use dexzd_apply::ApplyEffect;
use dexzd_protocol::dex::{InvokeWaitForMethodResponse, StepOptions};
use dexzd_protocol::dexzd::v1::{StepAttemptFailureKind, flow_command};
use tokio::time::Instant;

use super::{
    DurableState, EffectHarness, FakeDispatch, is_error_reply, is_method_completed, settings,
    start_request,
};
use crate::effects::EffectsConfig;

/// Enough Flows that an unbiased choice between ready branches picks the deadline every time
/// only with probability 2^-32.
const FLOW_COUNT: usize = 32;

/// How a Worker event and its expired deadline become ready together.
#[derive(Clone, Copy)]
enum Readiness {
    /// The driver fires the timer first, so each attempt wakes with both the timer and the
    /// event ready.
    TimerFiredFirst,
    /// The event arrives first, so each attempt sees it before the driver fires the timer.
    EventBeforeTimerFires,
}

/// Starts [`FLOW_COUNT`] Flows with `options` and returns their first dispatches, which all
/// start at the returned instant.
async fn start_flows(
    harness: &EffectHarness,
    options: StepOptions,
) -> (Instant, Vec<FakeDispatch>) {
    let started_at = Instant::now();
    let mut dispatches = Vec::with_capacity(FLOW_COUNT);
    for index in 0..FLOW_COUNT {
        let mut request = start_request(&format!("order-{index}"));
        request.step_options = Some(options.clone());
        harness.start_flow(request, settings());
        dispatches.push(harness.dispatcher.next_dispatch().await);
    }
    assert_eq!(
        Instant::now(),
        started_at,
        "every attempt starts its timeouts together"
    );
    (started_at, dispatches)
}

/// Sleeps until 100ms before `deadline`, then makes `send` and the expired deadline ready
/// together in the order `readiness` names.
async fn send_at_the_deadline(deadline: Instant, readiness: Readiness, send: impl Fn()) {
    tokio::time::sleep_until(deadline - Duration::from_millis(100)).await;
    match readiness {
        Readiness::TimerFiredFirst => {
            // Advancing yields once, so the driver fires and queues the attempts before `send`.
            tokio::time::advance(Duration::from_millis(200)).await;
            send();
        }
        Readiness::EventBeforeTimerFires => {
            send();
            tokio::time::advance(Duration::from_millis(200)).await;
        }
    }
}

/// Asserts that every Flow's first attempt failed with `kind`, and that no result committed.
fn assert_every_first_attempt_failed(harness: &EffectHarness, kind: StepAttemptFailureKind) {
    assert!(
        harness
            .runtime
            .commands_matching(is_method_completed)
            .is_empty(),
        "no result after an expired deadline is proposed"
    );
    let failures: Vec<_> = harness
        .runtime
        .commands_matching(|command| matches!(command, flow_command::Command::StepAttemptFailed(_)))
        .into_iter()
        .filter_map(|recorded| match recorded.command.command {
            Some(flow_command::Command::StepAttemptFailed(failure))
                if !is_error_reply(&recorded.reply) && failure.attempt == 1 =>
            {
                Some(failure.kind)
            }
            _ => None,
        })
        .collect();
    assert_eq!(failures, vec![kind as i32; FLOW_COUNT]);
}

async fn assert_a_result_at_the_method_deadline_times_out(readiness: Readiness) {
    let harness = EffectHarness::new();
    harness.rebuild_all_partitions().await;
    let options = StepOptions {
        wait_for_timeout_seconds: 5,
        heartbeat_timeout_seconds: 600,
        ..StepOptions::default()
    };
    let (started_at, dispatches) = start_flows(&harness, options).await;

    send_at_the_deadline(started_at + Duration::from_secs(5), readiness, || {
        for dispatch in &dispatches {
            dispatch.complete_wait_for(InvokeWaitForMethodResponse::default());
        }
    })
    .await;
    harness.settle().await;
    assert_every_first_attempt_failed(&harness, StepAttemptFailureKind::MethodTimeout);
    assert!(
        dispatches
            .iter()
            .all(|dispatch| dispatch.is_cancelled.load(Ordering::SeqCst))
    );
    harness.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_worker_result_ready_with_the_fired_method_timer_fails_with_method_timeout() {
    assert_a_result_at_the_method_deadline_times_out(Readiness::TimerFiredFirst).await;
}

#[tokio::test(start_paused = true)]
async fn a_worker_result_after_the_method_deadline_fails_before_the_timer_fires() {
    assert_a_result_at_the_method_deadline_times_out(Readiness::EventBeforeTimerFires).await;
}

async fn assert_a_frame_at_the_heartbeat_deadline_times_out(readiness: Readiness) {
    let harness = EffectHarness::new();
    harness.rebuild_all_partitions().await;
    let options = StepOptions {
        heartbeat_timeout_seconds: 10,
        ..StepOptions::default()
    };
    // The fake Worker receives each task at dispatch, which starts its heartbeat timeout.
    let (started_at, dispatches) = start_flows(&harness, options).await;

    send_at_the_deadline(started_at + Duration::from_secs(10), readiness, || {
        for dispatch in &dispatches {
            dispatch.send_wait_for_heartbeat(None);
        }
    })
    .await;
    harness.settle().await;
    assert_every_first_attempt_failed(&harness, StepAttemptFailureKind::HeartbeatTimeout);
    harness.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_worker_frame_ready_with_the_fired_heartbeat_timer_fails_with_heartbeat_timeout() {
    assert_a_frame_at_the_heartbeat_deadline_times_out(Readiness::TimerFiredFirst).await;
}

#[tokio::test(start_paused = true)]
async fn a_worker_frame_after_the_heartbeat_deadline_fails_before_the_timer_fires() {
    assert_a_frame_at_the_heartbeat_deadline_times_out(Readiness::EventBeforeTimerFires).await;
}

/// Starts one Flow and returns the gate's guard once its first state read is held on a blocking
/// thread.
async fn start_with_a_held_state_read(harness: &EffectHarness) -> super::OpenStateReadGateOnDrop {
    let held_reads = harness.runtime.state_read_gate.close();
    let mut request = start_request("order-1");
    request.step_options = Some(StepOptions {
        wait_for_lock_attribute_keys: vec!["inventory".to_owned()],
        wait_for_timeout_seconds: 30,
        ..StepOptions::default()
    });
    harness.start_flow(request, settings());
    harness.yield_to_tasks().await;
    harness.runtime.state_read_gate.wait_for_held_reads(1);
    held_reads
}

/// Asserts that only the first attempt failed, by method timeout before any dispatch, releasing
/// its lock.
fn assert_the_first_attempt_timed_out_before_dispatch(harness: &EffectHarness) {
    let failures = harness.runtime.commands_matching(|command| {
        matches!(command, flow_command::Command::StepAttemptFailed(_))
    });
    assert_eq!(failures.len(), 1);
    let Some(flow_command::Command::StepAttemptFailed(failure)) = &failures[0].command.command
    else {
        panic!("expected an attempt failure");
    };
    assert_eq!(failure.attempt, 1);
    assert_eq!(failure.kind, StepAttemptFailureKind::MethodTimeout as i32);
    assert_eq!(failure.server_detail, "method timeout of 30s expired");
    assert_eq!(harness.dispatcher.dispatch_count.load(Ordering::SeqCst), 0);
    let published_effects = harness.runtime.published_effects.lock().expect("lock");
    assert!(published_effects.iter().any(|effect| matches!(
        effect,
        ApplyEffect::LocksReleased { lock_keys, .. } if lock_keys == &["inventory".to_owned()]
    )));
}

/// Asserts that the timed-out attempt's late read dispatches nothing and the retry runs.
async fn assert_only_the_retry_is_dispatched(harness: &EffectHarness) {
    let retry = harness.dispatcher.next_dispatch().await;
    assert_eq!(retry.context().attempt, 2);
    assert_eq!(harness.dispatcher.dispatch_count.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn a_stalled_first_state_read_cannot_hold_off_the_method_timeout() {
    let harness = EffectHarness::new();
    harness.rebuild_all_partitions().await;
    let held_reads = start_with_a_held_state_read(&harness).await;

    // A held read stops paused time from advancing by itself.
    tokio::time::advance(Duration::from_millis(29_900)).await;
    harness.yield_to_tasks().await;
    assert!(
        harness
            .runtime
            .commands_matching(|command| matches!(
                command,
                flow_command::Command::StepAttemptFailed(_)
            ))
            .is_empty()
    );
    tokio::time::advance(Duration::from_millis(200)).await;
    harness.yield_to_tasks().await;
    assert_eq!(harness.runtime.state_read_gate.held_reads(), 1);
    assert_the_first_attempt_timed_out_before_dispatch(&harness);

    held_reads.open_and_wait_for_reads();
    assert_only_the_retry_is_dispatched(&harness).await;
    harness.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_first_state_read_ready_with_the_fired_method_timer_fails_with_method_timeout() {
    let harness = EffectHarness::new();
    harness.rebuild_all_partitions().await;
    let held_reads = start_with_a_held_state_read(&harness).await;

    // Advancing yields once, so the driver fires and queues the attempt before the read ends.
    tokio::time::advance(Duration::from_secs(30)).await;
    held_reads.open_and_wait_for_reads();
    // Lets the released read finish on its blocking thread before the attempt runs.
    std::thread::sleep(Duration::from_millis(20));
    harness.yield_to_tasks().await;
    assert_the_first_attempt_timed_out_before_dispatch(&harness);

    assert_only_the_retry_is_dispatched(&harness).await;
    harness.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn effect_shutdown_waits_for_a_state_read_abandoned_at_the_method_deadline() {
    let harness = EffectHarness::new();
    harness.rebuild_all_partitions().await;
    let held_reads = start_with_a_held_state_read(&harness).await;
    tokio::time::advance(Duration::from_millis(30_100)).await;
    harness.yield_to_tasks().await;
    assert_the_first_attempt_timed_out_before_dispatch(&harness);
    assert_eq!(harness.runtime.state_read_gate.held_reads(), 1);

    let gate = Arc::clone(&harness.runtime.state_read_gate);
    let shutdown = tokio::spawn(harness.shutdown());
    for _ in 0..200 {
        tokio::task::yield_now().await;
    }
    assert!(
        !shutdown.is_finished(),
        "shutdown waits for the abandoned read"
    );
    held_reads.open_and_wait_for_reads();
    shutdown.await.expect("shutdown completes");
    assert_eq!(gate.open_reader_count(), 0, "no reader outlives shutdown");
}

#[tokio::test(start_paused = true)]
async fn an_attempt_without_a_free_blocking_read_slot_times_out_without_reading() {
    let config = EffectsConfig {
        max_blocking_state_reads: 1,
        ..EffectsConfig::default()
    };
    let harness = EffectHarness::with_config(DurableState::new(), config);
    harness.rebuild_all_partitions().await;
    let held_reads = start_with_a_held_state_read(&harness).await;
    let mut request = start_request("order-2");
    request.step_options = Some(StepOptions {
        wait_for_timeout_seconds: 5,
        ..StepOptions::default()
    });
    harness.start_flow(request, settings());
    harness.yield_to_tasks().await;
    assert_eq!(
        harness.runtime.state_read_gate.open_reader_count(),
        1,
        "the second attempt opens no reader while the stalled read holds the only slot"
    );

    // A held read stops paused time from advancing by itself.
    tokio::time::advance(Duration::from_millis(5_100)).await;
    harness.yield_to_tasks().await;
    let failures: Vec<_> = harness
        .runtime
        .commands_matching(|command| matches!(command, flow_command::Command::StepAttemptFailed(_)))
        .into_iter()
        .filter_map(|recorded| match recorded.command.command {
            Some(flow_command::Command::StepAttemptFailed(failure)) => {
                Some((recorded.command.flow_id, failure.kind))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        failures,
        vec![(
            "order-2".to_owned(),
            StepAttemptFailureKind::MethodTimeout as i32
        )]
    );
    assert_eq!(harness.runtime.state_read_gate.held_reads(), 1);

    held_reads.open_and_wait_for_reads();
    let mut dispatched_flow_ids = BTreeSet::new();
    for _ in 0..2 {
        let dispatch = harness.dispatcher.next_dispatch().await;
        dispatched_flow_ids.insert(dispatch.context().flow_id);
    }
    assert_eq!(
        dispatched_flow_ids,
        BTreeSet::from(["order-1".to_owned(), "order-2".to_owned()])
    );
    harness.shutdown().await;
}
