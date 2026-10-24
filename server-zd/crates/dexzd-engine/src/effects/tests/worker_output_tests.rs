// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Worker output beyond the result's state changes: Step Stream writes, heartbeat checkpoints
//! of failing ASYNC attempts, and channel message IDs of Step results.

use std::collections::BTreeSet;

use dexzd_protocol::dex::{
    ChannelMessage, InvokeExecuteMethodResponse, InvokeWaitForMethodResponse, RetryPolicy,
    StepDurability, StepOptions, StepStreamWrite, Value,
};
use dexzd_protocol::dexzd::v1::{flow_command, step_method_completed_command};

use super::dispatch_tests::force_complete;
use super::{EffectHarness, is_method_completed, settings, start_request, string_value};
use crate::runtime::CommitUrgency;

const PROGRESS_STREAM: &str = "progress";

fn progress_write(text: &str) -> StepStreamWrite {
    StepStreamWrite {
        stream_name: PROGRESS_STREAM.to_owned(),
        stream_capacity_bytes: 1 << 20,
        value: Some(string_value(text)),
    }
}

/// Returns the retained progress Stream messages of `flow_id` as (Value, source), oldest first.
async fn progress_messages(harness: &EffectHarness, flow_id: &str) -> Vec<(Value, String)> {
    let page = harness
        .stream_store
        .list(0, "OrderFlow", flow_id, PROGRESS_STREAM, 100, "")
        .await
        .expect("the progress Stream lists");
    page.messages
        .into_iter()
        .rev()
        .map(|message| (message.value, message.source))
        .collect()
}

fn publication(message_id: &str) -> ChannelMessage {
    ChannelMessage {
        channel_name: "events".to_owned(),
        value: Some(string_value("payload")),
        message_id: message_id.to_owned(),
    }
}

#[tokio::test(start_paused = true)]
async fn step_stream_writes_reach_the_stream_store_in_frame_order_including_failed_attempts() {
    let harness = EffectHarness::new();
    harness.rebuild_all_partitions().await;
    harness.start_flow(start_request("order-1"), settings());

    let first = harness.dispatcher.next_dispatch().await;
    first.send_stream_write(progress_write("wait-for-1"));
    first.send_stream_write(StepStreamWrite {
        value: None,
        ..progress_write("")
    });
    first.send_stream_write(progress_write("wait-for-2"));
    first.fail_with_worker_error("retry after progress");
    let second = harness.dispatcher.next_dispatch().await;
    assert_eq!(second.context().attempt, 2);
    second.send_stream_write(progress_write("wait-for-3"));
    second.complete_wait_for(InvokeWaitForMethodResponse::default());
    let execute = harness.dispatcher.next_dispatch().await;
    execute.send_stream_write(progress_write("execute-1"));
    execute.complete_execute(force_complete());
    harness.settle().await;

    let source = "#ChargeStep-1".to_owned();
    assert_eq!(
        progress_messages(&harness, "order-1").await,
        ["wait-for-1", "wait-for-2", "wait-for-3", "execute-1"]
            .map(|text| (string_value(text), source.clone()))
            .to_vec(),
        "a write without a Value is dropped and later writes still arrive"
    );
    assert!(
        harness
            .runtime
            .commands_matching(|command| matches!(
                command,
                flow_command::Command::StepAttemptHeartbeat(_)
            ))
            .is_empty(),
        "a Stream write keeps the checkpoint"
    );
    harness.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn failing_async_attempts_propose_their_checkpoint_before_the_failure() {
    let harness = EffectHarness::new();
    harness.rebuild_all_partitions().await;
    let mut request = start_request("order-1");
    request.step_options = Some(StepOptions {
        wait_for_durability_override: StepDurability::Async as i32,
        wait_for_retry_policy: Some(RetryPolicy {
            initial_interval_seconds: 1,
            maximum_attempts: 4,
            ..RetryPolicy::default()
        }),
        ..StepOptions::default()
    });
    harness.start_flow(request, settings());

    // C2: ASYNC has no local phase, so the first attempt's checkpoint also carries over.
    let mut previous_checkpoint = None;
    for attempt in 1..=4 {
        let dispatch = harness.dispatcher.next_dispatch().await;
        let context = dispatch.context();
        assert_eq!(context.attempt, attempt);
        assert_eq!(
            context.last_heartbeat_value, previous_checkpoint,
            "attempt {attempt} receives the previous attempt's checkpoint"
        );
        let checkpoint = string_value(&format!("attempt-{attempt}"));
        dispatch.send_wait_for_heartbeat(Some(checkpoint.clone()));
        dispatch.send_stream_write(progress_write(&format!("attempt-{attempt}")));
        // The result arrives within the heartbeat batch window.
        if attempt < 4 {
            dispatch.fail_with_worker_error("retry");
        } else {
            dispatch.complete_wait_for(InvokeWaitForMethodResponse::default());
        }
        previous_checkpoint = Some(checkpoint);
    }
    harness.dispatcher.next_dispatch().await;

    let attempt_commands: Vec<(&str, u32, CommitUrgency)> = harness
        .runtime
        .commands
        .lock()
        .expect("lock")
        .iter()
        .filter_map(|recorded| match recorded.command.command.as_ref()? {
            flow_command::Command::StepAttemptHeartbeat(heartbeat) => {
                Some(("heartbeat", heartbeat.attempt, recorded.urgency))
            }
            flow_command::Command::StepAttemptFailed(failure) => {
                Some(("failed", failure.attempt, recorded.urgency))
            }
            flow_command::Command::StepMethodCompleted(completed) => {
                Some(("completed", completed.attempt, recorded.urgency))
            }
            _ => None,
        })
        .collect();
    let batched = CommitUrgency::Batched;
    assert_eq!(
        attempt_commands,
        vec![
            ("heartbeat", 1, batched),
            ("failed", 1, batched),
            ("heartbeat", 2, batched),
            ("failed", 2, batched),
            ("heartbeat", 3, batched),
            ("failed", 3, batched),
            ("heartbeat", 4, batched),
            ("completed", 4, batched),
        ]
    );
    let source = "#ChargeStep-1".to_owned();
    assert_eq!(
        progress_messages(&harness, "order-1").await,
        (1..=4)
            .map(|attempt| (string_value(&format!("attempt-{attempt}")), source.clone()))
            .collect::<Vec<_>>()
    );
    harness.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn every_step_result_publication_gets_a_new_uuid_v7_message_id() {
    let harness = EffectHarness::new();
    harness.rebuild_all_partitions().await;
    harness.start_flow(start_request("order-1"), settings());

    let wait_for = harness.dispatcher.next_dispatch().await;
    wait_for.complete_wait_for(InvokeWaitForMethodResponse {
        publish_to_channel: vec![publication("worker-chosen"), publication("")],
        ..InvokeWaitForMethodResponse::default()
    });
    let execute = harness.dispatcher.next_dispatch().await;
    execute.complete_execute(InvokeExecuteMethodResponse {
        publish_to_channel: vec![publication("worker-chosen")],
        ..force_complete()
    });
    harness.settle().await;

    let message_ids: Vec<String> = harness
        .runtime
        .commands_matching(is_method_completed)
        .into_iter()
        .flat_map(|recorded| match recorded.command.command {
            Some(flow_command::Command::StepMethodCompleted(completed)) => {
                match completed.response.expect("the result has a response") {
                    step_method_completed_command::Response::WaitFor(response) => {
                        response.publish_to_channel
                    }
                    step_method_completed_command::Response::Execute(response) => {
                        response.publish_to_channel
                    }
                }
            }
            _ => Vec::new(),
        })
        .map(|message| message.message_id)
        .collect();
    assert_eq!(message_ids.len(), 3);
    for message_id in &message_ids {
        let parsed = uuid::Uuid::parse_str(message_id).expect("the message ID is a UUID");
        assert_eq!(parsed.get_version_num(), 7);
        assert_eq!(message_id, &parsed.hyphenated().to_string());
    }
    assert_eq!(
        message_ids.iter().collect::<BTreeSet<_>>().len(),
        3,
        "every publication gets its own ID, replacing the Worker's"
    );
    harness.shutdown().await;
}
