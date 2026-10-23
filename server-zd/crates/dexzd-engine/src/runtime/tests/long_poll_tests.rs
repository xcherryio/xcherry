// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::time::Duration;

use dexzd_keyspace::{ColumnFamily, RunTable, run_key};
use dexzd_protocol::dex::{
    AttributeMatch, AttributeMatchOperator, ErrorSubStatus, FlowStatus, StopFlowRequest,
    WaitForAttributeRequest, WaitForFlowRequest, WaitForHistoryEventRequest,
    WaitForStepCompletionRequest,
};
use dexzd_protocol::dexzd::v1::{
    AttributeRecord, FlowCommand, FlowStopCommand, StepExecutionPhase, StepExecutionRecord,
    StoredValue, StoredValueReference, flow_command, stored_value,
};
use prost::Message;

use crate::runtime::tests::fixtures::{
    TENANT, flow, int_value, set_attribute_request, start_flow, string_value,
};
use crate::runtime::tests::test_support::RuntimeHarness;
use crate::runtime::{CommitUrgency, RuntimeConfig, RuntimeError, grpc_code};

fn wait_for_flow_request(flow_id: &str, wait_time_seconds: i32) -> WaitForFlowRequest {
    WaitForFlowRequest {
        flow_id: flow_id.to_owned(),
        wait_time_seconds,
        needs_results: true,
        ..WaitForFlowRequest::default()
    }
}

fn stop_command(flow_id: &str, run_id: dexzd_keyspace::RunId) -> FlowCommand {
    FlowCommand {
        flow_id: flow_id.to_owned(),
        run_id: run_id.as_bytes().to_vec(),
        command: Some(flow_command::Command::FlowStop(FlowStopCommand {
            request: Some(StopFlowRequest {
                flow_id: flow_id.to_owned(),
                stop_type: 2,
                ..StopFlowRequest::default()
            }),
        })),
        ..FlowCommand::default()
    }
}

fn attribute_wait(
    flow_id: &str,
    operator: AttributeMatchOperator,
    operand: i64,
) -> WaitForAttributeRequest {
    WaitForAttributeRequest {
        flow_id: flow_id.to_owned(),
        r#match: Some(AttributeMatch {
            key: "count".to_owned(),
            operator: operator as i32,
            operand: Some(int_value(operand)),
        }),
        ..WaitForAttributeRequest::default()
    }
}

async fn assert_pending<F: std::future::Future>(future: std::pin::Pin<&mut F>) {
    tokio::select! {
        _ = future => panic!("the long poll must still wait"),
        () = tokio::time::sleep(Duration::from_millis(10)) => {}
    }
}

#[tokio::test(start_paused = true)]
async fn wait_for_flow_wakes_only_on_the_committed_close() {
    let harness = RuntimeHarness::new();
    let run_id = start_flow(&harness, "order-1").await.unwrap();
    let wait = harness
        .registry
        .wait_for_flow(TENANT, wait_for_flow_request("order-1", 30));
    tokio::pin!(wait);
    assert_pending(wait.as_mut()).await;
    assert_eq!(harness.registry.long_poll_waiter_count(), 1);

    harness
        .registry
        .propose(
            flow("order-1"),
            stop_command("order-1", run_id),
            CommitUrgency::Batched,
        )
        .await
        .unwrap();
    assert_pending(wait.as_mut()).await;

    harness.commit_all();
    let result = wait.await.unwrap();
    assert_eq!(result.flow_status, FlowStatus::Terminated as i32);
    assert_eq!(harness.registry.long_poll_waiter_count(), 0);
}

#[tokio::test(start_paused = true)]
async fn wait_for_flow_times_out_with_long_poll_time_out() {
    let harness = RuntimeHarness::new();
    start_flow(&harness, "order-1").await.unwrap();
    let error = harness
        .registry
        .wait_for_flow(TENANT, wait_for_flow_request("order-1", 2))
        .await
        .unwrap_err();
    assert_eq!(error.grpc_code(), grpc_code::DEADLINE_EXCEEDED);
    assert_eq!(error.sub_status(), ErrorSubStatus::LongPollTimeOut);
    let error = harness
        .registry
        .wait_for_flow(TENANT, wait_for_flow_request("missing", 2))
        .await
        .unwrap_err();
    assert_eq!(error.sub_status(), ErrorSubStatus::FlowNotExists);
}

#[tokio::test(start_paused = true)]
async fn wait_for_attribute_returns_the_first_committed_match() {
    let harness = RuntimeHarness::new();
    start_flow(&harness, "order-1").await.unwrap();
    let wait = harness.registry.wait_for_attribute(
        TENANT,
        attribute_wait("order-1", AttributeMatchOperator::GreaterThanOrEqual, 5),
    );
    tokio::pin!(wait);
    assert_pending(wait.as_mut()).await;

    harness
        .drive(harness.registry.set_attributes(
            TENANT,
            set_attribute_request("order-1", "count", int_value(3)),
        ))
        .await
        .unwrap();
    assert_pending(wait.as_mut()).await;
    let mut write = set_attribute_request("order-1", "count", int_value(7));
    write.request_id = "second".to_owned();
    harness
        .drive(harness.registry.set_attributes(TENANT, write))
        .await
        .unwrap();
    let response = wait.await.unwrap();
    assert_eq!(response.matched_value, Some(int_value(7)));

    let mismatched_type = harness.registry.wait_for_attribute(
        TENANT,
        WaitForAttributeRequest {
            request_timeout_seconds: 1,
            ..attribute_wait("order-1", AttributeMatchOperator::Equal, 7)
        },
    );
    assert!(mismatched_type.await.is_ok());
}

#[tokio::test(start_paused = true)]
async fn wait_for_attribute_rejects_values_above_the_inline_threshold() {
    let harness = RuntimeHarness::new();
    let run_id = start_flow(&harness, "order-1").await.unwrap();
    let context = harness.registry.flow_context(&flow("order-1"));
    harness.store.put(
        ColumnFamily::Flow,
        run_key(context.flow_hash, run_id, RunTable::Attribute, b"count"),
        AttributeRecord {
            value: Some(StoredValue {
                kind: Some(stored_value::Kind::TableValue(
                    StoredValueReference::default(),
                )),
            }),
            ..AttributeRecord::default()
        }
        .encode_to_vec(),
    );
    let error = harness
        .registry
        .wait_for_attribute(
            TENANT,
            attribute_wait("order-1", AttributeMatchOperator::Equal, 1),
        )
        .await
        .unwrap_err();
    assert_eq!(error.grpc_code(), grpc_code::FAILED_PRECONDITION);

    harness
        .drive(harness.registry.set_attributes(
            TENANT,
            set_attribute_request("order-1", "count", string_value("text")),
        ))
        .await
        .unwrap();
    let error = harness
        .registry
        .wait_for_attribute(
            TENANT,
            WaitForAttributeRequest {
                request_timeout_seconds: 2,
                ..attribute_wait("order-1", AttributeMatchOperator::Equal, 1)
            },
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.sub_status(),
        ErrorSubStatus::RequestTimeout,
        "a type mismatch keeps waiting until the request times out"
    );
}

#[tokio::test(start_paused = true)]
async fn wait_for_step_completion_distinguishes_request_and_call_timeouts() {
    let config = RuntimeConfig {
        long_poll_default_wait: Duration::from_secs(5),
        ..RuntimeConfig::default()
    };
    let harness = RuntimeHarness::with_config(&config);
    let run_id = start_flow(&harness, "order-1").await.unwrap();
    let request = |request_timeout_seconds| WaitForStepCompletionRequest {
        flow_id: "order-1".to_owned(),
        step_type: "Approve".to_owned(),
        step_execution_number: "1".to_owned(),
        request_timeout_seconds,
        ..WaitForStepCompletionRequest::default()
    };
    let error = harness
        .registry
        .wait_for_step_completion(TENANT, request(2))
        .await
        .unwrap_err();
    assert_eq!(error.sub_status(), ErrorSubStatus::RequestTimeout);
    let error = harness
        .registry
        .wait_for_step_completion(TENANT, request(0))
        .await
        .unwrap_err();
    assert_eq!(error.sub_status(), ErrorSubStatus::LongPollTimeOut);

    let context = harness.registry.flow_context(&flow("order-1"));
    harness.store.put(
        ColumnFamily::Flow,
        run_key(
            context.flow_hash,
            run_id,
            RunTable::StepExecution,
            b"Approve-1",
        ),
        StepExecutionRecord {
            step_execution_id: "Approve-1".to_owned(),
            phase: StepExecutionPhase::Canceled as i32,
            ..StepExecutionRecord::default()
        }
        .encode_to_vec(),
    );
    harness
        .registry
        .wait_for_step_completion(TENANT, request(0))
        .await
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn wait_for_history_event_reports_available_events_and_closed_runs() {
    let harness = RuntimeHarness::new();
    let run_id = start_flow(&harness, "order-1").await.unwrap();
    let request = |next_internal_event_id| WaitForHistoryEventRequest {
        flow_id: "order-1".to_owned(),
        run_id: run_id.to_string(),
        next_internal_event_id,
    };
    let available = harness
        .registry
        .wait_for_history_event(TENANT, request(1))
        .await
        .unwrap();
    assert!(available.event_available);
    assert_eq!(available.available_internal_event_id, 1);

    let wait = harness.registry.wait_for_history_event(TENANT, request(5));
    tokio::pin!(wait);
    assert_pending(wait.as_mut()).await;
    harness
        .drive(harness.registry.stop_flow(
            TENANT,
            StopFlowRequest {
                flow_id: "order-1".to_owned(),
                ..StopFlowRequest::default()
            },
        ))
        .await
        .unwrap();
    let closed = wait.await.unwrap();
    assert!(!closed.event_available);
    assert_eq!(closed.flow_status, FlowStatus::Canceled as i32);
}

#[tokio::test(start_paused = true)]
async fn waiter_limits_reject_registrations_and_free_slots_on_drop() {
    let config = RuntimeConfig {
        max_long_poll_waiters_per_flow: 1,
        max_long_poll_waiters_per_node: 2,
        ..RuntimeConfig::default()
    };
    let harness = RuntimeHarness::with_config(&config);
    start_flow(&harness, "order-1").await.unwrap();
    start_flow(&harness, "order-2").await.unwrap();
    start_flow(&harness, "order-3").await.unwrap();

    let first = harness
        .registry
        .wait_for_flow(TENANT, wait_for_flow_request("order-1", 30));
    tokio::pin!(first);
    assert_pending(first.as_mut()).await;
    let error = harness
        .registry
        .wait_for_flow(TENANT, wait_for_flow_request("order-1", 30))
        .await
        .unwrap_err();
    assert_eq!(error.grpc_code(), grpc_code::RESOURCE_EXHAUSTED);

    let mut second = Box::pin(
        harness
            .registry
            .wait_for_flow(TENANT, wait_for_flow_request("order-2", 30)),
    );
    assert_pending(second.as_mut()).await;
    let error = harness
        .registry
        .wait_for_flow(TENANT, wait_for_flow_request("order-3", 30))
        .await
        .unwrap_err();
    assert_eq!(
        error.grpc_code(),
        grpc_code::RESOURCE_EXHAUSTED,
        "node limit"
    );

    drop(second);
    assert_eq!(harness.registry.long_poll_waiter_count(), 1);
    let mut third = Box::pin(
        harness
            .registry
            .wait_for_flow(TENANT, wait_for_flow_request("order-3", 30)),
    );
    assert_pending(third.as_mut()).await;
}

#[tokio::test(start_paused = true)]
async fn closing_or_halting_ends_waiting_long_polls() {
    let harness = RuntimeHarness::new();
    start_flow(&harness, "order-1").await.unwrap();
    let wait = harness
        .registry
        .wait_for_flow(TENANT, wait_for_flow_request("order-1", 30));
    tokio::pin!(wait);
    assert_pending(wait.as_mut()).await;
    harness.registry.close_long_polls();
    let error = wait.await.unwrap_err();
    assert!(
        matches!(error, RuntimeError::Unavailable { .. }),
        "{error:?}"
    );

    let halted = RuntimeHarness::new();
    start_flow(&halted, "order-1").await.unwrap();
    let wait = halted
        .registry
        .wait_for_flow(TENANT, wait_for_flow_request("order-1", 30));
    tokio::pin!(wait);
    assert_pending(wait.as_mut()).await;
    halted.registry.halt("test halt".to_owned());
    let error = wait.await.unwrap_err();
    assert!(
        matches!(error, RuntimeError::NodeStopping { .. }),
        "{error:?}"
    );
}
