// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Races between proposals, commits, and readers that run on separate threads.

use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use dexzd_keyspace::{ColumnFamily, RunId, RunTable, run_key};
use dexzd_protocol::dexzd::v1::LockRecord;
use prost::Message;

use crate::runtime::speculative_overlay::OverlayEntry;
use crate::runtime::tests::fixtures::{
    attributes_set_command, flow, int_value, large_outcome, locking_attempt_command,
    locking_invocation, start_flow,
};
use crate::runtime::tests::test_support::RuntimeHarness;
use crate::runtime::{CommitUrgency, PendingInvocation, RuntimeConfig, RuntimeError, grpc_code};

const LOCK_KEY: &str = "balance";

/// Proposes a Step attempt that locks [`LOCK_KEY`] and leaves it uncommitted.
async fn harness_with_uncommitted_step_lock() -> (Arc<RuntimeHarness>, RunId) {
    let harness = Arc::new(RuntimeHarness::new());
    let run_id = start_flow(&harness, "order-1").await.unwrap();
    harness
        .registry
        .propose(
            flow("order-1"),
            locking_attempt_command("order-1", run_id, LOCK_KEY),
            CommitUrgency::Batched,
        )
        .await
        .unwrap();
    assert_eq!(harness.committer.pending_count(), 1);
    (harness, run_id)
}

/// Runs `read` on another thread and commits the pending Step start right after `read` captured
/// its committed snapshot.
fn read_during_commit<T: Send + 'static>(
    harness: &Arc<RuntimeHarness>,
    read: impl FnOnce(&RuntimeHarness) -> T + Send + 'static,
) -> T {
    let (captured_sender, captured_receiver) = mpsc::channel();
    let (proceed_sender, proceed_receiver) = mpsc::channel::<()>();
    *harness.committer.view_hook.lock().unwrap() = Some(Box::new(move || {
        captured_sender.send(()).unwrap();
        // Times out when the commit waits for the Flow lock this reader holds.
        match proceed_receiver.recv_timeout(Duration::from_millis(200)) {
            Ok(()) | Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => panic!("the test stopped early"),
        }
    }));
    let reader = std::thread::spawn({
        let harness = Arc::clone(harness);
        move || read(&harness)
    });
    captured_receiver
        .recv_timeout(Duration::from_secs(5))
        .expect("the reader opens a committed view");
    assert!(harness.commit_next());
    // The reader is gone when the commit waited for the reader's Flow lock.
    if proceed_sender.send(()).is_err() {
        tracing::trace!("reader already finished");
    }
    reader.join().unwrap()
}

#[tokio::test]
async fn rpc_lock_check_sees_a_step_lock_committed_while_the_snapshot_opens() {
    let (harness, run_id) = harness_with_uncommitted_step_lock().await;
    let result = read_during_commit(&harness, move |harness| {
        let lease = harness.registry.lease(&flow("order-1"));
        harness
            .registry
            .acquire_rpc_locks(lease, run_id, &[LOCK_KEY.to_owned()])
            .map(drop)
    });
    let error = result.expect_err("a running Step holds the key");
    assert_eq!(error.grpc_code(), grpc_code::ABORTED, "{error:?}");
}

#[tokio::test]
async fn invocation_check_sees_a_step_lock_committed_while_the_snapshot_opens() {
    let (harness, run_id) = harness_with_uncommitted_step_lock().await;
    let can_start = read_during_commit(&harness, move |harness| {
        let other = locking_invocation("other");
        harness.registry.can_start_invocation(
            &flow("order-1"),
            &PendingInvocation {
                run_id,
                invocation: other,
                lock_keys: vec![LOCK_KEY.to_owned()],
                pending_since_seq: 9,
            },
        )
    });
    assert!(!can_start.unwrap(), "a running Step holds the key");
}

#[tokio::test]
async fn speculative_reader_sees_a_step_lock_committed_while_the_snapshot_opens() {
    let (harness, run_id) = harness_with_uncommitted_step_lock().await;
    let reader = read_during_commit(&harness, |harness| {
        harness.registry.flow_state_reader(&flow("order-1"), true)
    })
    .unwrap();
    let context = harness.registry.flow_context(&flow("order-1"));
    let key = run_key(
        context.flow_hash,
        run_id,
        RunTable::LockTable,
        LOCK_KEY.as_bytes(),
    );
    let bytes = reader
        .get(ColumnFamily::Flow, &key)
        .unwrap()
        .expect("the lock row is visible");
    let record = LockRecord::decode(bytes.as_slice()).unwrap();
    assert_eq!(record.holder, Some(locking_invocation(LOCK_KEY)));
}

/// Blocks each apply call until `expected` calls arrived or the timeout passed.
struct ApplyRendezvous {
    arrivals: Mutex<usize>,
    arrived: Condvar,
    expected: usize,
}

impl ApplyRendezvous {
    fn wait(&self) {
        let mut arrivals = self.arrivals.lock().unwrap();
        *arrivals += 1;
        self.arrived.notify_all();
        let (arrivals, _) = self
            .arrived
            .wait_timeout_while(arrivals, Duration::from_millis(300), |arrivals| {
                *arrivals < self.expected
            })
            .unwrap();
        drop(arrivals);
    }
}

#[test]
fn concurrent_proposals_on_different_flows_cannot_overrun_the_node_byte_limit() {
    const FLOW_COUNT: usize = 4;
    let config = RuntimeConfig {
        max_speculative_bytes_per_node: 1,
        speculation_capacity_wait: Duration::from_millis(50),
        ..RuntimeConfig::default()
    };
    let harness = Arc::new(RuntimeHarness::with_config(&config));
    let setup = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let run_ids: Vec<RunId> = (0..FLOW_COUNT)
        .map(|index| {
            setup
                .block_on(start_flow(&harness, &format!("order-{index}")))
                .unwrap()
        })
        .collect();
    assert_eq!(harness.registry.uncommitted_bytes(), 0);
    let rendezvous = Arc::new(ApplyRendezvous {
        arrivals: Mutex::new(0),
        arrived: Condvar::new(),
        expected: FLOW_COUNT,
    });
    *harness.applier.apply_hook.lock().unwrap() = Some({
        let rendezvous = Arc::clone(&rendezvous);
        Arc::new(move || rendezvous.wait())
    });

    let proposers: Vec<_> = run_ids
        .into_iter()
        .enumerate()
        .map(|(index, run_id)| {
            let harness = Arc::clone(&harness);
            std::thread::spawn(move || {
                let flow_id = format!("order-{index}");
                let command = attributes_set_command(&flow_id, run_id, "count", int_value(1));
                tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build()
                    .unwrap()
                    .block_on(harness.registry.propose(
                        flow(&flow_id),
                        command,
                        CommitUrgency::Batched,
                    ))
                    .map(drop)
            })
        })
        .collect();
    let results: Vec<Result<(), RuntimeError>> = proposers
        .into_iter()
        .map(|proposer| proposer.join().unwrap())
        .collect();

    let admitted = results.iter().filter(|result| result.is_ok()).count();
    assert_eq!(admitted, 1, "{results:?}");
    for error in results.iter().filter_map(|result| result.as_ref().err()) {
        assert!(
            matches!(error, RuntimeError::Unavailable { .. }),
            "{error:?}"
        );
    }
    assert_eq!(harness.committer.pending_count(), 1);
}

#[test]
fn concurrent_small_commands_overshoot_by_at_most_one_outcome_per_flow() {
    const FLOW_COUNT: usize = 4;
    let config = RuntimeConfig {
        max_speculative_bytes_per_node: 4096,
        speculation_capacity_wait: Duration::from_millis(50),
        ..RuntimeConfig::default()
    };
    let harness = Arc::new(RuntimeHarness::with_config(&config));
    let setup = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let run_ids: Vec<RunId> = (0..FLOW_COUNT)
        .map(|index| {
            setup
                .block_on(start_flow(&harness, &format!("order-{index}")))
                .unwrap()
        })
        .collect();
    let mut scripted = harness.applier.scripted.lock().unwrap();
    for run_id in &run_ids {
        scripted.push_back(Ok(large_outcome(*run_id)));
    }
    drop(scripted);
    let rendezvous = Arc::new(ApplyRendezvous {
        arrivals: Mutex::new(0),
        arrived: Condvar::new(),
        expected: FLOW_COUNT,
    });
    *harness.applier.apply_hook.lock().unwrap() = Some({
        let rendezvous = Arc::clone(&rendezvous);
        Arc::new(move || rendezvous.wait())
    });

    let proposers: Vec<_> = run_ids
        .iter()
        .copied()
        .enumerate()
        .map(|(index, run_id)| {
            let harness = Arc::clone(&harness);
            std::thread::spawn(move || {
                let flow_id = format!("order-{index}");
                let command = attributes_set_command(&flow_id, run_id, "count", int_value(1));
                tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build()
                    .unwrap()
                    .block_on(harness.registry.propose(
                        flow(&flow_id),
                        command,
                        CommitUrgency::Batched,
                    ))
                    .map(drop)
            })
        })
        .collect();
    let results: Vec<Result<(), RuntimeError>> = proposers
        .into_iter()
        .map(|proposer| proposer.join().unwrap())
        .collect();
    *harness.applier.apply_hook.lock().unwrap() = None;

    assert!(results.iter().all(Result::is_ok), "{results:?}");
    let limit = config.max_speculative_bytes_per_node;
    let largest_entry_bytes = harness
        .submitted()
        .iter()
        .rev()
        .take(FLOW_COUNT)
        .map(|command| OverlayEntry::new(command, large_outcome(run_ids[0])).size_bytes)
        .max()
        .unwrap();
    let retained = harness.registry.uncommitted_bytes();
    assert!(retained > limit, "{retained}");
    assert!(
        retained <= limit + FLOW_COUNT * largest_entry_bytes,
        "{retained}"
    );

    let error = setup
        .block_on(harness.registry.propose(
            flow("order-0"),
            attributes_set_command("order-0", run_ids[0], "count", int_value(2)),
            CommitUrgency::Batched,
        ))
        .unwrap_err();
    assert!(
        matches!(error, RuntimeError::Unavailable { .. }),
        "{error:?}"
    );
}
