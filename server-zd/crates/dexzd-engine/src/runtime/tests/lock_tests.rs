// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;
use std::time::Duration;

use dexzd_keyspace::{ColumnFamily, RunId, RunTable, run_key};
use dexzd_protocol::dex::{InvokeRpcRequest, InvokeWorkerRpcResponse};
use dexzd_protocol::dexzd::v1::{LockRecord, LockWaiterRecord, StepInvocationKey, StepMethod};
use prost::Message;
use tokio::sync::Notify;

use crate::runtime::tests::fixtures::{
    TENANT, flow, locking_attempt_command, locking_invocation, start_flow, string_value,
};
use crate::runtime::tests::test_support::RuntimeHarness;
use crate::runtime::{CommitUrgency, PendingInvocation, RuntimeError, WorkerRpcResult, grpc_code};

fn rpc_request(flow_id: &str, lock_keys: &[&str]) -> InvokeRpcRequest {
    InvokeRpcRequest {
        flow_id: flow_id.to_owned(),
        rpc_name: "approveOrder".to_owned(),
        request_id: "rpc-1".to_owned(),
        lock_attribute_keys: lock_keys.iter().map(|key| (*key).to_owned()).collect(),
        ..InvokeRpcRequest::default()
    }
}

fn output_result(text: &str) -> WorkerRpcResult {
    WorkerRpcResult {
        response: InvokeWorkerRpcResponse {
            output: Some(string_value(text)),
            ..InvokeWorkerRpcResponse::default()
        },
        app_worker_id: "worker-1".to_owned(),
        pull_routing: None,
    }
}

fn assert_aborted(error: &RuntimeError) {
    assert_eq!(error.grpc_code(), grpc_code::ABORTED, "{error:?}");
}

fn seed_lock_record(harness: &RuntimeHarness, run_id: RunId, lock_key: &str, record: &LockRecord) {
    let context = harness.registry.flow_context(&flow("order-1"));
    harness.store.put(
        ColumnFamily::Flow,
        run_key(
            context.flow_hash,
            run_id,
            RunTable::LockTable,
            lock_key.as_bytes(),
        ),
        record.encode_to_vec(),
    );
}

fn pending(
    run_id: RunId,
    invocation: &StepInvocationKey,
    lock_keys: &[String],
    pending_since_seq: u64,
) -> PendingInvocation {
    PendingInvocation {
        run_id,
        invocation: invocation.clone(),
        lock_keys: lock_keys.to_vec(),
        pending_since_seq,
    }
}

fn waiter_invocation(step_execution_id: &str) -> StepInvocationKey {
    StepInvocationKey {
        step_execution_id: step_execution_id.to_owned(),
        method: StepMethod::Execute as i32,
    }
}

#[tokio::test(start_paused = true)]
async fn rpc_conflicts_with_a_speculative_step_lock_holder() {
    let harness = RuntimeHarness::new();
    let run_id = start_flow(&harness, "order-1").await.unwrap();
    harness
        .registry
        .propose(
            flow("order-1"),
            locking_attempt_command("order-1", run_id, "balance"),
            CommitUrgency::Batched,
        )
        .await
        .unwrap();

    let error = harness
        .drive(
            harness
                .registry
                .invoke_rpc(TENANT, rpc_request("order-1", &["balance"])),
        )
        .await
        .unwrap_err();
    assert_aborted(&error);
    assert!(
        harness.rpc_invoker.invocations.lock().unwrap().is_empty(),
        "the Worker is never called on a conflict"
    );
}

#[tokio::test(start_paused = true)]
async fn rpc_conflicts_with_another_rpc_and_releases_when_the_call_is_dropped() {
    let harness = Arc::new(RuntimeHarness::new());
    start_flow(&harness, "order-1").await.unwrap();
    let gate = Arc::new(Notify::new());
    *harness.rpc_invoker.gate.lock().unwrap() = Some(Arc::clone(&gate));
    harness
        .rpc_invoker
        .results
        .lock()
        .unwrap()
        .push_back(Ok(output_result("first")));

    let mut holder = Box::pin(
        harness
            .registry
            .invoke_rpc(TENANT, rpc_request("order-1", &["balance"])),
    );
    tokio::select! {
        _ = &mut holder => panic!("the first RPC waits at the gate"),
        () = harness.rpc_invoker.entered.notified() => {}
    }

    let error = harness
        .registry
        .invoke_rpc(TENANT, rpc_request("order-1", &["other", "balance"]))
        .await
        .unwrap_err();
    assert_aborted(&error);

    let mut unrelated = Box::pin(
        harness
            .registry
            .invoke_rpc(TENANT, rpc_request("order-1", &["unrelated"])),
    );
    tokio::select! {
        _ = &mut unrelated => panic!("waits at the gate too"),
        () = harness.rpc_invoker.entered.notified() => {}
    }

    drop(holder);
    drop(unrelated);
    *harness.rpc_invoker.gate.lock().unwrap() = None;
    harness.rpc_invoker.results.lock().unwrap().clear();
    harness
        .rpc_invoker
        .results
        .lock()
        .unwrap()
        .push_back(Ok(output_result("second")));
    let response = harness
        .drive(
            harness
                .registry
                .invoke_rpc(TENANT, rpc_request("order-1", &["balance"])),
        )
        .await
        .unwrap();
    assert_eq!(response.output, Some(string_value("second")));
}

#[tokio::test(start_paused = true)]
async fn rpc_ignores_queued_step_waiters_but_invocations_respect_fifo() {
    let harness = RuntimeHarness::new();
    let run_id = start_flow(&harness, "order-1").await.unwrap();
    seed_lock_record(
        &harness,
        run_id,
        "balance",
        &LockRecord {
            holder: None,
            holder_attempt: 0,
            waiters: vec![LockWaiterRecord {
                invocation: Some(waiter_invocation("Earlier-1")),
                pending_since_seq: 5,
            }],
        },
    );
    let address = flow("order-1");
    let later = waiter_invocation("Later-1");
    let lock_keys = vec!["balance".to_owned()];

    assert!(
        !harness
            .registry
            .can_start_invocation(&address, &pending(run_id, &later, &lock_keys, 9))
            .unwrap(),
        "a later waiter cannot pass an earlier one"
    );
    assert!(
        harness
            .registry
            .can_start_invocation(
                &address,
                &pending(run_id, &waiter_invocation("Earlier-1"), &lock_keys, 5)
            )
            .unwrap()
    );

    harness
        .rpc_invoker
        .results
        .lock()
        .unwrap()
        .push_back(Ok(output_result("read")));
    let response = harness
        .drive(
            harness
                .registry
                .invoke_rpc(TENANT, rpc_request("order-1", &["balance"])),
        )
        .await
        .unwrap();
    assert_eq!(response.output, Some(string_value("read")));
}

#[tokio::test(start_paused = true)]
async fn invocation_start_is_blocked_by_an_rpc_lock_and_proposes_once_free() {
    let harness = Arc::new(RuntimeHarness::new());
    let run_id = start_flow(&harness, "order-1").await.unwrap();
    let address = flow("order-1");
    let gate = Arc::new(Notify::new());
    *harness.rpc_invoker.gate.lock().unwrap() = Some(Arc::clone(&gate));
    harness
        .rpc_invoker
        .results
        .lock()
        .unwrap()
        .push_back(Ok(output_result("done")));
    let rpc = harness
        .registry
        .invoke_rpc(TENANT, rpc_request("order-1", &["balance"]));
    tokio::pin!(rpc);
    tokio::select! {
        _ = &mut rpc => panic!("waits at the gate"),
        () = harness.rpc_invoker.entered.notified() => {}
    }

    let lock_keys = vec!["balance".to_owned()];
    let invocation = locking_invocation("balance");
    let blocked = harness
        .registry
        .propose_invocation_start(
            &address,
            &pending(run_id, &invocation, &lock_keys, 1),
            locking_attempt_command("order-1", run_id, "balance"),
            CommitUrgency::Prompt,
        )
        .await
        .unwrap();
    assert!(blocked.is_none(), "an RPC holds the key");

    gate.notify_one();
    let response = harness.drive(rpc).await.unwrap();
    assert_eq!(response.output, Some(string_value("done")));

    let started = harness
        .registry
        .propose_invocation_start(
            &address,
            &pending(run_id, &invocation, &lock_keys, 1),
            locking_attempt_command("order-1", run_id, "balance"),
            CommitUrgency::Prompt,
        )
        .await
        .unwrap();
    assert!(started.is_some());
    assert!(
        !harness
            .registry
            .can_start_invocation(
                &address,
                &pending(run_id, &waiter_invocation("Other-1"), &lock_keys, 2)
            )
            .unwrap(),
        "the speculative holder blocks other invocations"
    );
}

#[tokio::test(start_paused = true)]
async fn stalled_worker_rpc_ends_at_its_deadline_and_releases_its_locks() {
    let harness = RuntimeHarness::new();
    let run_id = start_flow(&harness, "order-1").await.unwrap();
    *harness.rpc_invoker.gate.lock().unwrap() = Some(Arc::new(Notify::new()));
    let mut request = rpc_request("order-1", &["balance"]);
    request.timeout_seconds = 1;

    let error = tokio::time::timeout(
        Duration::from_secs(5),
        harness.registry.invoke_rpc(TENANT, request),
    )
    .await
    .expect("the RPC ends at its deadline")
    .unwrap_err();
    assert_eq!(error.grpc_code(), grpc_code::DEADLINE_EXCEEDED, "{error:?}");

    let lock_keys = vec!["balance".to_owned()];
    let started = harness
        .registry
        .propose_invocation_start(
            &flow("order-1"),
            &pending(run_id, &locking_invocation("balance"), &lock_keys, 1),
            locking_attempt_command("order-1", run_id, "balance"),
            CommitUrgency::Prompt,
        )
        .await
        .unwrap();
    assert!(started.is_some(), "the expired RPC released its lock");
}

#[tokio::test(start_paused = true)]
async fn equal_sequence_waiters_start_in_queue_order() {
    let harness = RuntimeHarness::new();
    let run_id = start_flow(&harness, "order-1").await.unwrap();
    let first = locking_invocation("balance");
    let second = waiter_invocation("Second-1");
    seed_lock_record(
        &harness,
        run_id,
        "balance",
        &LockRecord {
            holder: None,
            holder_attempt: 0,
            waiters: vec![
                LockWaiterRecord {
                    invocation: Some(first.clone()),
                    pending_since_seq: 7,
                },
                LockWaiterRecord {
                    invocation: Some(second.clone()),
                    pending_since_seq: 7,
                },
            ],
        },
    );
    let address = flow("order-1");
    let lock_keys = vec!["balance".to_owned()];

    let blocked = harness
        .registry
        .propose_invocation_start(
            &address,
            &pending(run_id, &second, &lock_keys, 7),
            locking_attempt_command("order-1", run_id, "balance"),
            CommitUrgency::Prompt,
        )
        .await
        .unwrap();
    assert!(
        blocked.is_none(),
        "the second queued waiter cannot pass the first"
    );
    let started = harness
        .registry
        .propose_invocation_start(
            &address,
            &pending(run_id, &first, &lock_keys, 7),
            locking_attempt_command("order-1", run_id, "balance"),
            CommitUrgency::Prompt,
        )
        .await
        .unwrap();
    assert!(started.is_some());
}
