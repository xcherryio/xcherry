// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Worker requests whose values are above the inline threshold: every reference a Worker
//! receives resolves through LoadBlobs at once, also when ASYNC dispatch runs before commit.

use std::time::Duration;

use dexzd_protocol::dex::flow_service_client::FlowServiceClient;
use dexzd_protocol::dex::{
    AttributeWrite, FlowConfig, FlowStartOptions, FlowStatus, InvokeExecuteMethodOutput,
    InvokeExecuteMethodRequest, InvokeExecuteMethodResponse, LoadBlobRequestEntry,
    LoadBlobsRequest, PollWorkerTask, RetryPolicy, StartFlowRequest, StepDecision, StepDurability,
    StepMovement, StepOptions, Value, WaitForFlowRequest, WorkerTarget,
    invoke_execute_method_output, poll_worker_task, poll_worker_task_frame, value,
};
use tonic::transport::Channel;

use crate::harness::{DevServer, ScriptedWorker, force_complete};

const FLOW_TYPE: &str = "ValueReferenceFlow";
const CHAIN_STEP_TYPE: &str = "ChainStep";

/// A 2 second batch window keeps ASYNC commands uncommitted while their next Step runs.
const SLOW_BATCHED_COMMIT_CONFIG: &str = "commit:\n  batchWindow: 2s\n";

/// An ASYNC Step whose input and Attribute were written by a still-uncommitted command must
/// still succeed on its only attempt.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn async_step_receives_uncommitted_values_on_its_only_attempt() {
    let server = DevServer::start_with_config_overrides(Some(SLOW_BATCHED_COMMIT_CONFIG)).await;
    let mut flow_client = server.flow_client();
    let mut worker = ScriptedWorker::start(server.worker_client(), FLOW_TYPE).await;
    let flow_id = "async-uncommitted-values";
    start_chain_flow(
        &mut flow_client,
        flow_id,
        text_value(&"s".repeat(64)),
        StepDurability::Async,
    )
    .await;

    let first_task = worker.next_task().await;
    let first = execute_request(&first_task);
    assert_eq!(attempt_of(first), 1);
    let second_input = text_value(&"i".repeat(4096));
    let profile = text_value(&"p".repeat(4096));
    worker.reply(
        first_task.task_id.clone(),
        execute_output(InvokeExecuteMethodResponse {
            upsert_attributes: vec![AttributeWrite {
                key: "profile".to_owned(),
                value: Some(profile.clone()),
                ..AttributeWrite::default()
            }],
            step_decision: Some(go_to_chain_step(
                second_input.clone(),
                StepDurability::Async,
            )),
            ..InvokeExecuteMethodResponse::default()
        }),
    );

    let second_task = worker.next_task().await;
    let second = execute_request(&second_task);
    assert_eq!(
        attempt_of(second),
        1,
        "the second Step runs on its first and only attempt"
    );
    assert_eq!(
        load_value(&mut flow_client, flow_id, second.step_input.as_ref()).await,
        second_input,
        "the Worker resolves the Step input"
    );
    let received_profile = second
        .attributes
        .iter()
        .find(|attribute| attribute.key == "profile")
        .and_then(|attribute| attribute.value.as_ref());
    assert_eq!(
        load_value(&mut flow_client, flow_id, received_profile).await,
        profile,
        "the Worker resolves the Attribute"
    );
    worker.reply(
        second_task.task_id.clone(),
        execute_output(InvokeExecuteMethodResponse {
            step_decision: Some(force_complete(text_value("done"))),
            ..InvokeExecuteMethodResponse::default()
        }),
    );

    assert_flow_completed(&mut flow_client, flow_id).await;
    worker.stop().await;
    server.shutdown().await;
}

/// When inlining uncommitted values would exceed the Worker message limit, dispatch waits for
/// the commit and sends value references that LoadBlobs resolves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oversized_uncommitted_values_are_sent_as_references_after_commit() {
    let server = DevServer::start_with_config_overrides(Some(&format!(
        "{SLOW_BATCHED_COMMIT_CONFIG}api:\n  grpcMaxMessageBytes: 1048576\nvalues:\n  maxValueBytes: 524288\n"
    )))
    .await;
    let mut flow_client = server.flow_client();
    let mut worker = ScriptedWorker::start(server.worker_client(), FLOW_TYPE).await;
    let flow_id = "async-oversized-uncommitted-values";
    start_chain_flow(
        &mut flow_client,
        flow_id,
        text_value("start"),
        StepDurability::Async,
    )
    .await;
    let first_attribute = text_value(&"a".repeat(400 * 1024));
    let second_attribute = text_value(&"b".repeat(400 * 1024));
    let third_input = text_value(&"c".repeat(400 * 1024));

    let first_task = worker.next_task().await;
    worker.reply(
        first_task.task_id.clone(),
        execute_output(InvokeExecuteMethodResponse {
            upsert_attributes: vec![attribute_write("first", &first_attribute)],
            step_decision: Some(go_to_chain_step(text_value("next"), StepDurability::Async)),
            ..InvokeExecuteMethodResponse::default()
        }),
    );
    let second_task = worker.next_task().await;
    assert_eq!(attempt_of(execute_request(&second_task)), 1);
    let second_replied_at = tokio::time::Instant::now();
    worker.reply(
        second_task.task_id.clone(),
        execute_output(InvokeExecuteMethodResponse {
            upsert_attributes: vec![attribute_write("second", &second_attribute)],
            step_decision: Some(go_to_chain_step(third_input.clone(), StepDurability::Async)),
            ..InvokeExecuteMethodResponse::default()
        }),
    );

    let third_task = worker.next_task().await;
    let dispatch_delay = second_replied_at.elapsed();
    let third = execute_request(&third_task);
    assert_eq!(attempt_of(third), 1);
    assert!(
        is_value_reference(third.step_input.as_ref()),
        "an oversized request falls back to value references"
    );
    assert!(
        dispatch_delay >= Duration::from_secs(1),
        "the fallback waits for the 2 second batch to commit, but dispatch took {dispatch_delay:?}"
    );
    assert_eq!(
        load_value(&mut flow_client, flow_id, third.step_input.as_ref()).await,
        third_input
    );
    for (key, expected) in [("first", &first_attribute), ("second", &second_attribute)] {
        let received = third
            .attributes
            .iter()
            .find(|attribute| attribute.key == key)
            .and_then(|attribute| attribute.value.as_ref());
        assert_eq!(
            &load_value(&mut flow_client, flow_id, received).await,
            expected,
            "Attribute {key}"
        );
    }
    worker.reply(
        third_task.task_id.clone(),
        execute_output(InvokeExecuteMethodResponse {
            step_decision: Some(force_complete(text_value("done"))),
            ..InvokeExecuteMethodResponse::default()
        }),
    );

    assert_flow_completed(&mut flow_client, flow_id).await;
    worker.stop().await;
    server.shutdown().await;
}

/// A chain of SYNC Steps with 4 KiB inputs stays close to the inline case; the bound is loose
/// enough for shared CI runners.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sync_steps_with_value_references_dispatch_promptly() {
    const STEP_COUNT: usize = 30;
    let server = DevServer::start().await;
    let mut flow_client = server.flow_client();
    let mut worker = ScriptedWorker::start(server.worker_client(), FLOW_TYPE).await;
    let flow_id = "sync-value-references";
    let payload = text_value(&"x".repeat(4096));
    start_chain_flow(
        &mut flow_client,
        flow_id,
        payload.clone(),
        StepDurability::Sync,
    )
    .await;

    let mut step_gaps = Vec::with_capacity(STEP_COUNT);
    let mut replied_at = None;
    for index in 0..STEP_COUNT {
        let task = worker.next_task().await;
        let request = execute_request(&task);
        assert_eq!(attempt_of(request), 1);
        assert_eq!(
            load_value(&mut flow_client, flow_id, request.step_input.as_ref()).await,
            payload
        );
        if let Some(replied_at) = replied_at {
            step_gaps.push(tokio::time::Instant::now().duration_since(replied_at));
        }
        let decision = if index + 1 == STEP_COUNT {
            force_complete(text_value("done"))
        } else {
            go_to_chain_step(payload.clone(), StepDurability::Sync)
        };
        worker.reply(
            task.task_id.clone(),
            execute_output(InvokeExecuteMethodResponse {
                step_decision: Some(decision),
                ..InvokeExecuteMethodResponse::default()
            }),
        );
        replied_at = Some(tokio::time::Instant::now());
    }
    assert_flow_completed(&mut flow_client, flow_id).await;

    step_gaps.sort();
    let median_gap = step_gaps[step_gaps.len() / 2];
    assert!(
        median_gap < Duration::from_millis(250),
        "median Step gap {median_gap:?} over {} gaps: {step_gaps:?}",
        step_gaps.len()
    );
    worker.stop().await;
    server.shutdown().await;
}

async fn start_chain_flow(
    flow_client: &mut FlowServiceClient<Channel>,
    flow_id: &str,
    input: Value,
    durability: StepDurability,
) {
    flow_client
        .start_flow(StartFlowRequest {
            flow_id: flow_id.to_owned(),
            flow_type: FLOW_TYPE.to_owned(),
            start_step_type: CHAIN_STEP_TYPE.to_owned(),
            step_input: Some(input),
            step_options: Some(chain_step_options(durability)),
            flow_start_options: Some(FlowStartOptions {
                flow_config_override: Some(FlowConfig {
                    worker_target: Some(WorkerTarget {
                        is_pull_mode: true,
                        ..WorkerTarget::default()
                    }),
                    ..FlowConfig::default()
                }),
                ..FlowStartOptions::default()
            }),
            request_id: format!("{flow_id}-start"),
            ..StartFlowRequest::default()
        })
        .await
        .expect("StartFlow succeeds");
}

/// Execute-only options with a single attempt, so any failure fails the Flow.
fn chain_step_options(durability: StepDurability) -> StepOptions {
    StepOptions {
        skip_wait_for: true,
        execute_durability_override: durability as i32,
        execute_retry_policy: Some(RetryPolicy {
            maximum_attempts: 1,
            ..RetryPolicy::default()
        }),
        ..StepOptions::default()
    }
}

fn go_to_chain_step(input: Value, durability: StepDurability) -> StepDecision {
    StepDecision {
        next_steps: vec![StepMovement {
            step_type: CHAIN_STEP_TYPE.to_owned(),
            step_input: Some(input),
            step_options: Some(chain_step_options(durability)),
            ..StepMovement::default()
        }],
        ..StepDecision::default()
    }
}

fn execute_request(task: &PollWorkerTask) -> &InvokeExecuteMethodRequest {
    match task.request.as_ref() {
        Some(poll_worker_task::Request::Execute(request)) => request,
        other => panic!("expected an Execute task, got {other:?}"),
    }
}

fn attempt_of(request: &InvokeExecuteMethodRequest) -> i32 {
    request
        .context
        .as_ref()
        .expect("an Execute request has a context")
        .attempt
}

fn execute_output(response: InvokeExecuteMethodResponse) -> poll_worker_task_frame::Output {
    poll_worker_task_frame::Output::Execute(InvokeExecuteMethodOutput {
        output: Some(invoke_execute_method_output::Output::Result(response)),
    })
}

fn attribute_write(key: &str, value: &Value) -> AttributeWrite {
    AttributeWrite {
        key: key.to_owned(),
        value: Some(value.clone()),
        ..AttributeWrite::default()
    }
}

fn is_value_reference(value: Option<&Value>) -> bool {
    matches!(
        value.and_then(|value| value.kind.as_ref()),
        Some(
            value::Kind::InternalBlobIdForStringValue(_)
                | value::Kind::InternalBlobIdForObjValue(_)
        )
    )
}

/// Returns `value`, resolving a value reference through LoadBlobs as an SDK Worker does; an
/// omitted reference fails the test, as it fails the SDK's attempt.
async fn load_value(
    flow_client: &mut FlowServiceClient<Channel>,
    flow_id: &str,
    value: Option<&Value>,
) -> Value {
    let value = value.expect("the request carries the value").clone();
    let reference = match value.kind.as_ref() {
        Some(
            value::Kind::InternalBlobIdForStringValue(reference)
            | value::Kind::InternalBlobIdForObjValue(reference),
        ) => reference.clone(),
        _ => return value,
    };
    let loaded = flow_client
        .load_blobs(LoadBlobsRequest {
            entries: vec![LoadBlobRequestEntry {
                flow_id: flow_id.to_owned(),
                blob_value: Some(value),
            }],
        })
        .await
        .expect("LoadBlobs succeeds")
        .into_inner();
    loaded
        .values
        .get(&reference)
        .cloned()
        .unwrap_or_else(|| panic!("LoadBlobs omitted the value reference {reference}"))
}

async fn assert_flow_completed(flow_client: &mut FlowServiceClient<Channel>, flow_id: &str) {
    let result = flow_client
        .wait_for_flow(WaitForFlowRequest {
            flow_id: flow_id.to_owned(),
            wait_time_seconds: 30,
            ..WaitForFlowRequest::default()
        })
        .await
        .expect("WaitForFlow succeeds")
        .into_inner();
    assert_eq!(
        result.flow_status,
        FlowStatus::Completed as i32,
        "{result:?}"
    );
}

fn text_value(text: &str) -> Value {
    Value {
        kind: Some(value::Kind::StringValue(text.to_owned())),
    }
}
