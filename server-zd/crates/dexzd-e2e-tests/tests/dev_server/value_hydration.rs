// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! `values.hydrateBeforeDispatch`: complete values, or value references when it is off.

use std::time::Duration;

use dexzd_protocol::dex::flow_service_client::FlowServiceClient;
use dexzd_protocol::dex::{
    AttributeWrite, ChannelCondition, ChannelMessage, EncodedObject, FlowConfig, FlowStartOptions,
    FlowStatus, GetAttributesRequest, GetChannelMessagesRequest, GetFlowStateRequest,
    GetFlowStateResponse, GetHistoryEventsRequest, InvokeExecuteMethodOutput,
    InvokeExecuteMethodResponse, InvokeRpcRequest, InvokeWaitForMethodOutput,
    InvokeWaitForMethodResponse, InvokeWorkerRpcResponse, Kv, PublishToChannelRequest,
    StartFlowRequest, StepOptions, Value, WaitForFlowRequest, WaitingCondition,
    WaitingConditionType, WorkerTarget, invoke_execute_method_output,
    invoke_wait_for_method_output, poll_worker_task, poll_worker_task_frame, value,
};
use prost::Message;
use tonic::transport::Channel;

use crate::harness::{DevServer, ScriptedWorker, force_complete};

const FLOW_TYPE: &str = "HydrationFlow";
const FLOW_ID: &str = "hydration-flow-1";
const VALUE_REFERENCE_PREFIX: &[u8] = b"zd1|";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hydration_delivers_complete_values_to_clients_and_workers() {
    check_value_delivery(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn without_hydration_large_values_are_value_references() {
    check_value_delivery(false).await;
}

/// Runs one Flow with a 10-byte inline threshold and checks every value it reads or receives.
async fn check_value_delivery(is_hydrating: bool) {
    let server = DevServer::start_with_config_overrides(Some(&format!(
        "values:\n  wireInlineThresholdBytes: 10\n  hydrateBeforeDispatch: {is_hydrating}\n"
    )))
    .await;
    let mut flow_client = server.flow_client();
    let mut worker = ScriptedWorker::start(server.worker_client(), FLOW_TYPE).await;
    let expected = ValueExpectation { is_hydrating };

    let step_input = string_value(&large_text("step-input"));
    let profile = Value {
        kind: Some(value::Kind::ObjValue(EncodedObject {
            encoding: "json".to_owned(),
            payload: format!(r#"{{"name":"{}"}}"#, large_text("profile")).into_bytes(),
        })),
    };
    let started = flow_client
        .start_flow(StartFlowRequest {
            flow_id: FLOW_ID.to_owned(),
            flow_type: FLOW_TYPE.to_owned(),
            start_step_type: "CollectStep".to_owned(),
            step_input: Some(step_input.clone()),
            step_options: Some(StepOptions {
                execute_load_channel_names: vec!["notes".to_owned()],
                ..StepOptions::default()
            }),
            flow_start_options: Some(FlowStartOptions {
                attributes: vec![AttributeWrite {
                    key: "profile".to_owned(),
                    value: Some(profile.clone()),
                    ..AttributeWrite::default()
                }],
                flow_config_override: Some(FlowConfig {
                    worker_target: Some(WorkerTarget {
                        is_pull_mode: true,
                        ..WorkerTarget::default()
                    }),
                    ..FlowConfig::default()
                }),
                ..FlowStartOptions::default()
            }),
            request_id: "hydration-start".to_owned(),
            ..StartFlowRequest::default()
        })
        .await
        .expect("StartFlow succeeds")
        .into_inner();

    let note = string_value(&large_text("note"));
    publish(&mut flow_client, "notes", &note).await;

    let wait_for_task = worker.next_task().await;
    let Some(poll_worker_task::Request::WaitFor(wait_for)) = wait_for_task.request.as_ref() else {
        panic!("the first task is WaitFor, got {wait_for_task:?}");
    };
    expected.value(
        wait_for.step_input.as_ref(),
        &step_input,
        "WaitFor step input",
    );
    expected.key_value(
        &wait_for.attributes,
        "profile",
        &profile,
        "WaitFor Attribute",
    );
    expected.encoded_references(&wait_for_task, "WaitFor task");
    let cursor = string_value(&large_text("cursor"));
    worker.reply(
        wait_for_task.task_id.clone(),
        poll_worker_task_frame::Output::WaitFor(InvokeWaitForMethodOutput {
            output: Some(invoke_wait_for_method_output::Output::Result(
                InvokeWaitForMethodResponse {
                    waiting_condition: Some(WaitingCondition {
                        waiting_condition_type: WaitingConditionType::AllCompleted as i32,
                        channel_conditions: vec![ChannelCondition {
                            condition_id: "order-arrived".to_owned(),
                            channel_name: "orders".to_owned(),
                            ..ChannelCondition::default()
                        }],
                        ..WaitingCondition::default()
                    }),
                    upsert_step_exe_locals: vec![Kv {
                        key: "cursor".to_owned(),
                        value: Some(cursor.clone()),
                    }],
                    ..InvokeWaitForMethodResponse::default()
                },
            )),
        }),
    );

    // The Step now waits for "orders", so no task holds the Worker during these reads.
    let state = wait_for_waiting_step(&mut flow_client).await;
    expected.key_value(
        &state.attributes,
        "profile",
        &profile,
        "GetFlowState Attribute",
    );
    let active_step = &state.active_step_executions[0];
    expected.value(
        active_step
            .movement
            .as_ref()
            .and_then(|movement| movement.step_input.as_ref()),
        &step_input,
        "GetFlowState step input",
    );
    expected.key_value(
        &active_step.step_execution_locals,
        "cursor",
        &cursor,
        "GetFlowState Step local",
    );
    expected.value(
        state.pending_channel_messages["notes"].messages[0]
            .value
            .as_ref(),
        &note,
        "GetFlowState pending message",
    );
    expected.encoded_references(&state, "GetFlowState response");

    let attributes = flow_client
        .get_attributes(GetAttributesRequest {
            flow_id: FLOW_ID.to_owned(),
            all_keys: true,
            ..GetAttributesRequest::default()
        })
        .await
        .expect("GetAttributes succeeds")
        .into_inner();
    expected.key_value(&attributes.attributes, "profile", &profile, "GetAttributes");
    expected.encoded_references(&attributes, "GetAttributes response");

    let notes = flow_client
        .get_channel_messages(GetChannelMessagesRequest {
            flow_id: FLOW_ID.to_owned(),
            channel_name: "notes".to_owned(),
            ..GetChannelMessagesRequest::default()
        })
        .await
        .expect("GetChannelMessages succeeds")
        .into_inner();
    expected.value(
        notes.messages[0].value.as_ref(),
        &note,
        "GetChannelMessages",
    );
    expected.encoded_references(&notes, "GetChannelMessages response");

    let rpc_output = string_value(&large_text("rpc-output"));
    let mut rpc_client = flow_client.clone();
    let rpc_call = tokio::spawn(async move {
        rpc_client
            .invoke_rpc(InvokeRpcRequest {
                flow_id: FLOW_ID.to_owned(),
                rpc_name: "listNotes".to_owned(),
                load_channel_names: vec!["notes".to_owned()],
                timeout_seconds: 30,
                request_id: "hydration-rpc".to_owned(),
                ..InvokeRpcRequest::default()
            })
            .await
    });
    let rpc_task = worker.next_task().await;
    let Some(poll_worker_task::Request::Rpc(rpc)) = rpc_task.request.as_ref() else {
        panic!("the second task is the RPC, got {rpc_task:?}");
    };
    expected.key_value(&rpc.attributes, "profile", &profile, "RPC Attribute");
    expected.value(
        rpc.loaded_channel_messages["notes"].messages[0]
            .value
            .as_ref(),
        &note,
        "RPC loaded message",
    );
    expected.encoded_references(&rpc_task, "RPC task");
    worker.reply(
        rpc_task.task_id.clone(),
        poll_worker_task_frame::Output::Rpc(InvokeWorkerRpcResponse {
            output: Some(rpc_output.clone()),
            ..InvokeWorkerRpcResponse::default()
        }),
    );
    let rpc_response = rpc_call
        .await
        .expect("the RPC call task finishes")
        .expect("InvokeRPC succeeds")
        .into_inner();
    assert_eq!(rpc_response.output, Some(rpc_output));

    let order = string_value(&large_text("order"));
    publish(&mut flow_client, "orders", &order).await;
    let execute_task = worker.next_task().await;
    let Some(poll_worker_task::Request::Execute(execute)) = execute_task.request.as_ref() else {
        panic!("the third task is Execute, got {execute_task:?}");
    };
    expected.value(
        execute.step_input.as_ref(),
        &step_input,
        "Execute step input",
    );
    expected.key_value(
        &execute.attributes,
        "profile",
        &profile,
        "Execute Attribute",
    );
    expected.key_value(
        &execute.step_exe_locals,
        "cursor",
        &cursor,
        "Execute Step local",
    );
    let channel_results = &execute
        .condition_results
        .as_ref()
        .expect("Execute carries condition results")
        .channel_results;
    expected.value(
        channel_results[0].values.first(),
        &order,
        "Execute channel condition result",
    );
    expected.value(
        execute.loaded_channel_messages["notes"].messages[0]
            .value
            .as_ref(),
        &note,
        "Execute loaded message",
    );
    expected.encoded_references(&execute_task, "Execute task");
    let close_output = string_value(&large_text("close-output"));
    worker.reply(
        execute_task.task_id.clone(),
        poll_worker_task_frame::Output::Execute(InvokeExecuteMethodOutput {
            output: Some(invoke_execute_method_output::Output::Result(
                InvokeExecuteMethodResponse {
                    step_decision: Some(force_complete(close_output.clone())),
                    ..InvokeExecuteMethodResponse::default()
                },
            )),
        }),
    );

    let result = flow_client
        .wait_for_flow(WaitForFlowRequest {
            flow_id: FLOW_ID.to_owned(),
            needs_results: true,
            wait_time_seconds: 30,
            ..WaitForFlowRequest::default()
        })
        .await
        .expect("WaitForFlow succeeds")
        .into_inner();
    assert_eq!(result.flow_status, FlowStatus::Completed as i32);
    expected.value(
        result.results[0].completed_step_output.as_ref(),
        &close_output,
        "WaitForFlow result",
    );
    let closed_state = get_flow_state(&mut flow_client).await;
    expected.value(
        closed_state.completed_steps[0]
            .completed_step_output
            .as_ref(),
        &close_output,
        "GetFlowState completed Step",
    );
    expected.encoded_references(&closed_state, "closed GetFlowState response");

    // History keeps value references in both modes, as the Go Server's history does.
    let history = flow_client
        .get_history_events(GetHistoryEventsRequest {
            flow_id: FLOW_ID.to_owned(),
            run_id: started.run_id,
            ..GetHistoryEventsRequest::default()
        })
        .await
        .expect("GetHistoryEvents succeeds")
        .into_inner();
    assert!(
        has_value_reference(&history.encode_to_vec()),
        "history events keep value references"
    );

    worker.stop().await;
    server.shutdown().await;
}

/// Checks a received value against the original: equal when hydrating, otherwise a value
/// reference of the original's kind.
struct ValueExpectation {
    is_hydrating: bool,
}

impl ValueExpectation {
    fn value(&self, received: Option<&Value>, original: &Value, what: &str) {
        let received = received.unwrap_or_else(|| panic!("{what} is missing"));
        if self.is_hydrating {
            assert_eq!(received, original, "{what} is the complete value");
            return;
        }
        let reference = match (&original.kind, &received.kind) {
            (
                Some(value::Kind::StringValue(_)),
                Some(value::Kind::InternalBlobIdForStringValue(reference)),
            )
            | (
                Some(value::Kind::ObjValue(_)),
                Some(value::Kind::InternalBlobIdForObjValue(reference)),
            ) => reference,
            _ => panic!("{what} is a value reference of the original kind, got {received:?}"),
        };
        assert!(
            reference.as_bytes().starts_with(VALUE_REFERENCE_PREFIX),
            "{what} is a server-zd value reference, got {reference}"
        );
    }

    fn key_value(&self, key_values: &[Kv], key: &str, original: &Value, what: &str) {
        let received = key_values
            .iter()
            .find(|key_value| key_value.key == key)
            .and_then(|key_value| key_value.value.as_ref());
        self.value(received, original, what);
    }

    /// Checks that `message` contains a value reference exactly when hydration is off.
    fn encoded_references(&self, message: &impl Message, what: &str) {
        assert_eq!(
            has_value_reference(&message.encode_to_vec()),
            !self.is_hydrating,
            "{what} contains value references only without hydration"
        );
    }
}

async fn publish(
    flow_client: &mut FlowServiceClient<Channel>,
    channel_name: &str,
    message: &Value,
) {
    flow_client
        .publish_to_channel(PublishToChannelRequest {
            flow_id: FLOW_ID.to_owned(),
            messages: vec![ChannelMessage {
                channel_name: channel_name.to_owned(),
                value: Some(message.clone()),
                ..ChannelMessage::default()
            }],
            ..PublishToChannelRequest::default()
        })
        .await
        .expect("PublishToChannel succeeds");
}

/// Polls GetFlowState until the Step has registered its wait and the Step local.
async fn wait_for_waiting_step(
    flow_client: &mut FlowServiceClient<Channel>,
) -> GetFlowStateResponse {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let state = get_flow_state(flow_client).await;
            let is_waiting = state
                .active_step_executions
                .first()
                .is_some_and(|step| !step.step_execution_locals.is_empty());
            if is_waiting {
                return state;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the Step waits within 30 seconds")
}

async fn get_flow_state(flow_client: &mut FlowServiceClient<Channel>) -> GetFlowStateResponse {
    flow_client
        .get_flow_state(GetFlowStateRequest {
            flow_id: FLOW_ID.to_owned(),
            ..GetFlowStateRequest::default()
        })
        .await
        .expect("GetFlowState succeeds")
        .into_inner()
}

fn has_value_reference(bytes: &[u8]) -> bool {
    bytes
        .windows(VALUE_REFERENCE_PREFIX.len())
        .any(|window| window == VALUE_REFERENCE_PREFIX)
}

fn large_text(label: &str) -> String {
    format!("{label}-").repeat(16)
}

fn string_value(text: &str) -> Value {
    Value {
        kind: Some(value::Kind::StringValue(text.to_owned())),
    }
}
