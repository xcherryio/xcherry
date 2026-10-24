// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use dexzd_apply::{ApplyContext, ApplyEffect, FlowStateReader};
use dexzd_keyspace::row_keys::{outbox_key, run_singleton_key};
use dexzd_keyspace::{ColumnFamily, RunId, RunTable};
use dexzd_object_store::{
    FaultAction, FaultInjector, InMemoryObjectStore, ObjectStore, ObjectStoreOperation,
    WriteCondition,
};
use dexzd_protocol::dex::{
    FlowResult, FlowStatus, InvokeWaitForMethodResponse, StepOptions, StopFlowRequest, StopType,
    SubFlowCondition, Value, WaitingCondition, WaitingConditionType,
};
use dexzd_protocol::dexzd::v1::{
    ChildFlowCompletedMessage, CommandErrorReply, CommandReply, CrossFlowEndpoint, FlowCommand,
    FlowStopCommand, InboxMessageCommand, OutboxRecord, RunRecord, RunSettings, command_reply,
    flow_command, inbox_message_command, outbox_record,
};
use prost::Message;
use tokio::sync::Notify;

use super::hard_delete_tests::set_hard_delete_attribute;
use super::{
    DurableState, EffectHarness, RecordedCommand, is_error_reply, ok_override, ok_reply, settings,
    start_request, string_value,
};
use crate::effects::{EffectsConfig, decode_hard_delete_value};
use crate::runtime::read_operations::state_rows::read_run;
use crate::runtime::{CommitUrgency, RuntimeEffectSink, RuntimeError};

const MESSAGE_ID: [u8; 16] = [9; 16];

/// Lets a fixed number of Lists of one run's hard-delete prefix through, then fails the rest.
struct PurgeStepGate {
    run_prefix: std::sync::Mutex<Option<String>>,
    allowed_lists: AtomicUsize,
    entered: Notify,
}

impl PurgeStepGate {
    fn new() -> Self {
        Self {
            run_prefix: std::sync::Mutex::new(None),
            allowed_lists: AtomicUsize::new(usize::MAX),
            entered: Notify::new(),
        }
    }

    fn gate_run(&self, prefix: String, allowed_lists: usize) {
        *self.run_prefix.lock().expect("gate lock") = Some(prefix);
        self.allowed_lists.store(allowed_lists, Ordering::SeqCst);
    }

    fn open(&self) {
        self.allowed_lists.store(usize::MAX, Ordering::SeqCst);
    }
}

impl FaultInjector for PurgeStepGate {
    fn action(&self, operation: ObjectStoreOperation, key: &str) -> FaultAction {
        let is_run_listing = operation == ObjectStoreOperation::List
            && self
                .run_prefix
                .lock()
                .expect("gate lock")
                .as_ref()
                .is_some_and(|prefix| key == prefix);
        if !is_run_listing {
            return FaultAction::Proceed;
        }
        let is_allowed = self
            .allowed_lists
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |allowed| {
                allowed.checked_sub(1)
            })
            .is_ok();
        if is_allowed {
            return FaultAction::Proceed;
        }
        self.entered.notify_one();
        FaultAction::FailBeforeEffect
    }
}

fn seed_outbox_message(
    harness: &EffectHarness,
    parent_run: RunId,
    child_run: RunId,
    acknowledged_at_micros: i64,
) {
    let run = harness.run_location("child", child_run);
    let record = OutboxRecord {
        message_id: MESSAGE_ID.to_vec(),
        target: Some(CrossFlowEndpoint {
            tenant_id: 0,
            flow_id: "parent".to_owned(),
            run_id: parent_run.as_bytes().to_vec(),
            run_attempt: 1,
        }),
        payload: Some(outbox_record::Payload::ChildCompleted(
            ChildFlowCompletedMessage {
                child_flow_id: "child".to_owned(),
                child_run_id: child_run.as_bytes().to_vec(),
                child_run_attempt: 1,
                result: Some(FlowResult {
                    flow_status: FlowStatus::Completed as i32,
                    ..FlowResult::default()
                }),
                parent: None,
            },
        )),
        written_seq: 3,
        acknowledged_at_micros,
        held_values: Vec::new(),
    };
    harness.runtime.state.lock().expect("lock").put_message(
        ColumnFamily::Flow,
        outbox_key(run.flow_hash, run.run_id, &MESSAGE_ID),
        &record,
    );
}

fn written_effect(run_id: RunId) -> ApplyEffect {
    ApplyEffect::OutboxMessagesWritten {
        run_id,
        message_ids: vec![MESSAGE_ID.to_vec()],
    }
}

#[tokio::test(start_paused = true)]
async fn outbox_messages_are_delivered_to_the_target_then_acknowledged_despite_outages() {
    let harness = EffectHarness::new();
    harness.rebuild_all_partitions().await;
    let parent_run = harness.start_flow(start_request("parent"), settings());
    let child_run = harness.start_flow(start_request("child"), settings());
    harness.runtime.add_override(ok_override(|command| {
        matches!(
            command,
            flow_command::Command::InboxMessage(_) | flow_command::Command::OutboxAck(_)
        )
    }));
    seed_outbox_message(&harness, parent_run, child_run, 0);
    harness
        .runtime
        .unavailable_proposals
        .store(3, Ordering::SeqCst);
    harness.runner.handle(
        &EffectHarness::flow("child"),
        &[written_effect(child_run)],
        true,
    );
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    harness.settle().await;

    let inbox = harness
        .runtime
        .commands_matching(|command| matches!(command, flow_command::Command::InboxMessage(_)));
    assert_eq!(inbox.len(), 1);
    assert_eq!(inbox[0].command.flow_id, "parent");
    assert_eq!(inbox[0].command.run_id, parent_run.as_bytes().to_vec());
    let Some(flow_command::Command::InboxMessage(message)) = &inbox[0].command.command else {
        panic!("expected an inbox message");
    };
    assert_eq!(message.message_id, MESSAGE_ID.to_vec());
    let source = message.source.as_ref().expect("source");
    assert_eq!(source.flow_id, "child");
    assert_eq!(source.run_id, child_run.as_bytes().to_vec());
    assert_eq!(source.run_attempt, 1);
    assert!(matches!(
        message.payload,
        Some(inbox_message_command::Payload::ChildCompleted(_))
    ));

    let acks = harness
        .runtime
        .commands_matching(|command| matches!(command, flow_command::Command::OutboxAck(_)));
    assert_eq!(acks.len(), 1);
    assert_eq!(acks[0].command.flow_id, "child");
    let Some(flow_command::Command::OutboxAck(ack)) = &acks[0].command.command else {
        panic!("expected an ack");
    };
    assert_eq!(ack.message_ids, vec![MESSAGE_ID.to_vec()]);
    harness.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn acknowledged_outbox_messages_are_not_redelivered() {
    let harness = EffectHarness::new();
    harness.rebuild_all_partitions().await;
    let parent_run = harness.start_flow(start_request("parent"), settings());
    let child_run = harness.start_flow(start_request("child"), settings());
    seed_outbox_message(&harness, parent_run, child_run, 1);
    harness.runner.handle(
        &EffectHarness::flow("child"),
        &[written_effect(child_run)],
        true,
    );
    harness.settle().await;
    assert!(
        harness
            .runtime
            .commands_matching(|command| matches!(command, flow_command::Command::InboxMessage(_)))
            .is_empty()
    );
    harness.shutdown().await;
}

pub(super) fn mark_purging(harness: &EffectHarness, flow_id: &str, run_id: RunId) {
    let run = harness.run_location(flow_id, run_id);
    let key = run_singleton_key(run.flow_hash, run.run_id, RunTable::RunRecord);
    let mut state = harness.runtime.state.lock().expect("lock");
    let bytes = state
        .rows
        .get(&(ColumnFamily::Flow, key.clone()))
        .expect("run record")
        .clone();
    let mut record = RunRecord::decode(bytes.as_slice()).expect("decodes");
    record.is_purging = true;
    state.put_message(ColumnFamily::Flow, key, &record);
}

pub(super) async fn put_object(harness: &EffectHarness, key: &str) {
    harness
        .durable
        .objects
        .put(key, Bytes::from_static(b"x"), WriteCondition::Unconditional)
        .await
        .expect("put succeeds");
}

#[tokio::test(start_paused = true)]
async fn retention_purge_deletes_the_hard_delete_prefix_then_proposes_run_purge() {
    let harness = EffectHarness::new();
    harness.rebuild_all_partitions().await;
    harness.runtime.add_override(ok_override(|command| {
        matches!(command, flow_command::Command::RunPurge(_))
    }));
    let run_id = harness.start_flow(start_request("order-1"), settings());
    let other_run = harness.start_flow(start_request("order-2"), settings());
    let owner = harness
        .hard_delete_objects
        .owner_of(&EffectHarness::flow("order-1"), run_id);
    let other_owner = harness
        .hard_delete_objects
        .owner_of(&EffectHarness::flow("order-2"), other_run);
    put_object(&harness, &owner.object_key("referenced")).await;
    put_object(&harness, &owner.object_key("orphan-of-lost-disk")).await;
    put_object(&harness, &other_owner.object_key("kept")).await;
    mark_purging(&harness, "order-1", run_id);

    harness.runner.handle(
        &EffectHarness::flow("order-1"),
        &[ApplyEffect::RunPurgeRequested { run_id }],
        true,
    );
    harness.settle().await;

    let remaining = harness
        .durable
        .objects
        .list("hard-delete/", None, 1000)
        .await
        .expect("list succeeds");
    let keys: Vec<String> = remaining
        .objects
        .into_iter()
        .map(|object| object.key)
        .collect();
    assert_eq!(keys, vec![other_owner.object_key("kept")]);
    let purges = harness
        .runtime
        .commands_matching(|command| matches!(command, flow_command::Command::RunPurge(_)));
    assert_eq!(purges.len(), 1);
    assert_eq!(purges[0].command.run_id, run_id.as_bytes().to_vec());
    assert_eq!(purges[0].reply, ok_reply());
    harness.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn purge_requests_for_runs_not_purging_and_speculative_external_effects_do_nothing() {
    let harness = EffectHarness::new();
    harness.rebuild_all_partitions().await;
    let parent_run = harness.start_flow(start_request("parent"), settings());
    let child_run = harness.start_flow(start_request("child"), settings());
    seed_outbox_message(&harness, parent_run, child_run, 0);
    harness.runner.handle(
        &EffectHarness::flow("parent"),
        &[ApplyEffect::RunPurgeRequested { run_id: parent_run }],
        true,
    );
    mark_purging(&harness, "child", child_run);
    harness.runner.handle(
        &EffectHarness::flow("child"),
        &[
            ApplyEffect::RunPurgeRequested { run_id: child_run },
            written_effect(child_run),
        ],
        false,
    );
    harness.settle().await;
    assert!(
        harness
            .runtime
            .commands_matching(|command| matches!(
                command,
                flow_command::Command::RunPurge(_) | flow_command::Command::InboxMessage(_)
            ))
            .is_empty()
    );
    harness.shutdown().await;
}

/// Child Flow ID that apply derives for the parent's first SubFlow condition.
const CHILD_FLOW_ID: &str = "SubFlow:parent-ChargeStep-1-0";

fn child_settings_config() -> EffectsConfig {
    EffectsConfig {
        child_run_settings: RunSettings {
            retention_after_close_micros: 3 * 24 * 3600 * super::SECOND_MICROS,
            ..settings()
        },
        ..EffectsConfig::default()
    }
}

/// Starts "parent" and answers its WaitFor with one SubFlow condition; apply writes the
/// child start into the parent's outbox.
async fn start_parent_waiting_for_a_child(
    harness: &EffectHarness,
    run_settings: RunSettings,
) -> RunId {
    let parent_run = harness.start_flow(start_request("parent"), run_settings);
    harness
        .dispatcher
        .next_dispatch()
        .await
        .complete_wait_for(InvokeWaitForMethodResponse {
            waiting_condition: Some(WaitingCondition {
                waiting_condition_type: WaitingConditionType::AllCompleted as i32,
                sub_flow_conditions: vec![SubFlowCondition {
                    condition_id: "child".to_owned(),
                    sub_flow_type: "ChildFlow".to_owned(),
                    start_step_type: "ChildStep".to_owned(),
                    step_options: Some(StepOptions {
                        skip_wait_for: true,
                        ..StepOptions::default()
                    }),
                    ..SubFlowCondition::default()
                }],
                ..WaitingCondition::default()
            }),
            ..InvokeWaitForMethodResponse::default()
        });
    harness.settle().await;
    parent_run
}

fn inbox_messages(harness: &EffectHarness) -> Vec<(InboxMessageCommand, CommandReply)> {
    harness
        .runtime
        .commands_matching(|command| matches!(command, flow_command::Command::InboxMessage(_)))
        .into_iter()
        .filter_map(|recorded| match recorded.command.command {
            Some(flow_command::Command::InboxMessage(message)) => Some((message, recorded.reply)),
            _ => None,
        })
        .collect()
}

fn outbox_acks(harness: &EffectHarness) -> Vec<RecordedCommand> {
    harness
        .runtime
        .commands_matching(|command| matches!(command, flow_command::Command::OutboxAck(_)))
}

fn read_outbox_row(harness: &EffectHarness, parent_run: RunId, message_id: &[u8]) -> OutboxRecord {
    let run = harness.run_location("parent", parent_run);
    let bytes = harness
        .runtime
        .snapshot()
        .get(
            ColumnFamily::Flow,
            &outbox_key(run.flow_hash, run.run_id, message_id),
        )
        .expect("memory reads never fail")
        .expect("the outbox row exists");
    OutboxRecord::decode(bytes.as_slice()).expect("the outbox row decodes")
}

fn child_run_exists(harness: &EffectHarness, new_run_id: &[u8]) -> bool {
    let run = harness.run_location(
        CHILD_FLOW_ID,
        RunId::from_slice(new_run_id).expect("16-byte run ID"),
    );
    harness
        .runtime
        .snapshot()
        .get(
            ColumnFamily::Flow,
            &run_singleton_key(run.flow_hash, run.run_id, RunTable::RunRecord),
        )
        .expect("memory reads never fail")
        .is_some()
}

fn unavailable_reply() -> CommandReply {
    CommandReply {
        reply: Some(command_reply::Reply::Error(CommandErrorReply {
            grpc_code: 14,
            message: "latest child run is being purged".to_owned(),
            ..CommandErrorReply::default()
        })),
    }
}

/// Replies UNAVAILABLE to the first `count` InboxMessage commands, then lets apply run.
fn unavailable_inbox_override(count: usize) -> super::CommandOverride {
    let remaining = Arc::new(AtomicUsize::new(count));
    Box::new(move |command, _| {
        let is_inbox = matches!(
            command.command,
            Some(flow_command::Command::InboxMessage(_))
        );
        let is_rejected = is_inbox
            && remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                    left.checked_sub(1)
                })
                .is_ok();
        is_rejected.then(unavailable_reply)
    })
}

#[tokio::test(start_paused = true)]
async fn child_starts_are_delivered_with_a_new_run_id_and_the_configured_settings() {
    let config = child_settings_config();
    let harness = EffectHarness::with_config(DurableState::new(), config.clone());
    harness.rebuild_all_partitions().await;
    let parent_run = start_parent_waiting_for_a_child(&harness, settings()).await;

    let inbox = inbox_messages(&harness);
    assert_eq!(inbox.len(), 1);
    let (message, reply) = &inbox[0];
    assert_eq!(reply, &ok_reply());
    assert_eq!(message.new_run_id.len(), 16);
    assert_eq!(message.settings, Some(config.child_run_settings));
    assert!(matches!(
        message.payload,
        Some(inbox_message_command::Payload::ChildStart(_))
    ));
    let source = message.source.as_ref().expect("source");
    assert_eq!(source.flow_id, "parent");
    assert_eq!(source.run_id, parent_run.as_bytes().to_vec());
    assert_eq!(source.run_attempt, 1);

    let child_run = RunId::from_slice(&message.new_run_id).expect("16-byte run ID");
    let child = harness.run_record(CHILD_FLOW_ID, child_run);
    assert_eq!(child.settings, Some(config.child_run_settings));
    assert_eq!(child.status, FlowStatus::Running as i32);
    assert_eq!(outbox_acks(&harness).len(), 1);
    assert_ne!(
        read_outbox_row(&harness, parent_run, &message.message_id).acknowledged_at_micros,
        0
    );
    harness.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_redelivered_child_start_with_a_new_run_id_starts_the_child_only_once() {
    let config = child_settings_config();
    let first = EffectHarness::with_config(DurableState::new(), config.clone());
    // The ack is lost: it replies OK but never reaches the parent's state.
    first.runtime.add_override(ok_override(|command| {
        matches!(command, flow_command::Command::OutboxAck(_))
    }));
    first.rebuild_all_partitions().await;
    let parent_run = start_parent_waiting_for_a_child(&first, settings()).await;
    let first_delivery = inbox_messages(&first).remove(0).0;
    assert!(child_run_exists(&first, &first_delivery.new_run_id));
    let durable = first.durable.clone();
    first.shutdown().await;

    let second = EffectHarness::with_config(durable, config);
    second.rebuild_all_partitions().await;
    second.settle().await;
    let redeliveries = inbox_messages(&second);
    assert_eq!(redeliveries.len(), 1);
    let (redelivery, reply) = &redeliveries[0];
    assert_eq!(reply, &ok_reply(), "a duplicate is acknowledged");
    assert_eq!(redelivery.message_id, first_delivery.message_id);
    assert_ne!(redelivery.new_run_id, first_delivery.new_run_id);
    assert!(child_run_exists(&second, &first_delivery.new_run_id));
    assert!(
        !child_run_exists(&second, &redelivery.new_run_id),
        "inbox deduplication runs before the new run ID is used"
    );
    assert_eq!(outbox_acks(&second).len(), 1);
    assert_ne!(
        read_outbox_row(&second, parent_run, &redelivery.message_id).acknowledged_at_micros,
        0
    );
    second.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn receiver_errors_are_retried_without_acknowledging_and_survive_a_restart() {
    let first = EffectHarness::new();
    first
        .runtime
        .add_override(unavailable_inbox_override(usize::MAX));
    first.rebuild_all_partitions().await;
    let parent_run = start_parent_waiting_for_a_child(&first, settings()).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    first.settle().await;
    let rejected = inbox_messages(&first);
    assert!(
        rejected.len() >= 3,
        "errors are retried: {}",
        rejected.len()
    );
    assert!(
        outbox_acks(&first).is_empty(),
        "an error reply is never acknowledged"
    );
    let message_id = rejected[0].0.message_id.clone();
    assert_eq!(
        read_outbox_row(&first, parent_run, &message_id).acknowledged_at_micros,
        0
    );
    let durable = first.durable.clone();
    first.shutdown().await;

    let second = EffectHarness::with_config(durable, EffectsConfig::default());
    second.runtime.add_override(unavailable_inbox_override(1));
    second.rebuild_all_partitions().await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    second.settle().await;
    let deliveries = inbox_messages(&second);
    assert_eq!(deliveries.len(), 2);
    assert_eq!(deliveries[0].1, unavailable_reply());
    assert_eq!(deliveries[1].1, ok_reply());
    assert!(child_run_exists(&second, &deliveries[1].0.new_run_id));
    assert_eq!(outbox_acks(&second).len(), 1);
    assert_ne!(
        read_outbox_row(&second, parent_run, &message_id).acknowledged_at_micros,
        0
    );
    second.shutdown().await;
}

const SHORT_RETENTION: Duration = Duration::from_secs(60);

fn short_retention_settings() -> RunSettings {
    RunSettings {
        retention_after_close_micros: 60 * super::SECOND_MICROS,
        ..settings()
    }
}

fn terminate_run(harness: &EffectHarness, flow_id: &str, run_id: RunId) {
    let command = FlowCommand {
        flow_id: flow_id.to_owned(),
        run_id: run_id.as_bytes().to_vec(),
        command: Some(flow_command::Command::FlowStop(FlowStopCommand {
            request: Some(StopFlowRequest {
                flow_id: flow_id.to_owned(),
                stop_type: StopType::Terminate as i32,
                ..StopFlowRequest::default()
            }),
        })),
        ..FlowCommand::default()
    };
    let reply = harness
        .runtime
        .commit(
            &EffectHarness::flow(flow_id),
            command,
            CommitUrgency::Prompt,
        )
        .expect("stop commits");
    assert!(!is_error_reply(&reply), "stop failed: {reply:?}");
}

fn run_record_if_exists(
    harness: &EffectHarness,
    flow_id: &str,
    run_id: RunId,
) -> Option<RunRecord> {
    let run = harness.run_location(flow_id, run_id);
    harness
        .runtime
        .snapshot()
        .get(
            ColumnFamily::Flow,
            &run_singleton_key(run.flow_hash, run.run_id, RunTable::RunRecord),
        )
        .expect("memory reads never fail")
        .map(|bytes| RunRecord::decode(bytes.as_slice()).expect("the run record decodes"))
}

async fn hard_delete_value(harness: &EffectHarness, object_key: &str) -> Option<Value> {
    harness
        .durable
        .objects
        .get(object_key)
        .await
        .expect("object reads succeed")
        .map(|content| decode_hard_delete_value(object_key, &content.bytes).expect("decodes"))
}

fn client_read_error(harness: &EffectHarness, flow_id: &str) -> Option<RuntimeError> {
    let flow = EffectHarness::flow(flow_id);
    let flow_hash = harness.hasher.flow_hash(flow.tenant_id, flow_id);
    let context = ApplyContext {
        partition: harness.hasher.partition_of(flow_hash),
        flow_hash,
    };
    read_run(&harness.runtime.snapshot(), context, &flow, "").err()
}

fn assert_hidden_from_client_reads(harness: &EffectHarness, flow_id: &str) {
    let error = client_read_error(harness, flow_id).expect("a purging run is unreadable");
    assert_eq!(error.grpc_code(), crate::runtime::grpc_code::NOT_FOUND);
    assert_eq!(
        error.sub_status(),
        dexzd_protocol::dex::ErrorSubStatus::FlowNotExists
    );
}

fn run_purges(harness: &EffectHarness) -> Vec<RecordedCommand> {
    harness
        .runtime
        .commands_matching(|command| matches!(command, flow_command::Command::RunPurge(_)))
}

/// Replies UNAVAILABLE to every InboxMessage while `is_rejecting` is set.
fn switchable_unavailable_inbox_override(is_rejecting: Arc<AtomicBool>) -> super::CommandOverride {
    Box::new(move |command, _| {
        let is_inbox = matches!(
            command.command,
            Some(flow_command::Command::InboxMessage(_))
        );
        (is_inbox && is_rejecting.load(Ordering::SeqCst)).then(unavailable_reply)
    })
}

#[tokio::test(start_paused = true)]
async fn a_pending_outbox_postpones_retention_before_any_hard_delete_object_is_deleted() {
    let harness = EffectHarness::new();
    let is_child_start_rejected = Arc::new(AtomicBool::new(true));
    harness
        .runtime
        .add_override(switchable_unavailable_inbox_override(Arc::clone(
            &is_child_start_rejected,
        )));
    harness.rebuild_all_partitions().await;
    let parent_run = start_parent_waiting_for_a_child(&harness, short_retention_settings()).await;
    let object_id =
        set_hard_delete_attribute(&harness, "parent", parent_run, "secret", "classified").await;
    let object_key = harness
        .hard_delete_objects
        .owner_of(&EffectHarness::flow("parent"), parent_run)
        .object_key(&object_id);
    terminate_run(&harness, "parent", parent_run);
    let first_deadline = harness
        .run_record("parent", parent_run)
        .retention_deadline_micros;

    tokio::time::sleep(SHORT_RETENTION + Duration::from_secs(1)).await;
    harness.settle().await;
    assert!(
        outbox_acks(&harness).is_empty(),
        "the child start is still unacknowledged"
    );
    assert_eq!(
        hard_delete_value(&harness, &object_key).await,
        Some(string_value("classified")),
        "postponing retention keeps every hard-delete value"
    );
    let postponed = harness.run_record("parent", parent_run);
    assert!(!postponed.is_purging, "a postponed run stays readable");
    assert!(postponed.retention_deadline_micros > first_deadline);
    assert!(client_read_error(&harness, "parent").is_none());
    assert!(run_purges(&harness).is_empty());

    is_child_start_rejected.store(false, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(3600)).await;
    harness.settle().await;
    assert_eq!(outbox_acks(&harness).len(), 1);
    let purges = run_purges(&harness);
    assert_eq!(purges.len(), 1);
    assert_eq!(purges[0].reply, ok_reply());
    assert!(run_record_if_exists(&harness, "parent", parent_run).is_none());
    assert_eq!(hard_delete_value(&harness, &object_key).await, None);
    harness.shutdown().await;
}

/// Where the node stops in the retention purge sequence.
#[derive(Clone, Copy, PartialEq, Debug)]
enum PurgeCrashPoint {
    BeforeRetentionDecision,
    BeforeObjectDeletion,
    DuringObjectDeletion,
    BeforeRunPurgeCommit,
    AfterRunPurgeCommit,
}

/// Stops the node at `crash_point`, restarts it from durable state, and checks the purge finishes.
async fn assert_purge_recovers_after_a_crash(crash_point: PurgeCrashPoint) {
    let gate = Arc::new(PurgeStepGate::new());
    let durable = DurableState {
        objects: Arc::new(InMemoryObjectStore::with_injector(gate.clone())),
        ..DurableState::new()
    };
    let first = EffectHarness::with_config(durable.clone(), EffectsConfig::default());
    first.rebuild_all_partitions().await;
    let run_id = first.start_flow(start_request("order-1"), short_retention_settings());
    let owner = first
        .hard_delete_objects
        .owner_of(&EffectHarness::flow("order-1"), run_id);
    let mut object_keys = Vec::new();
    for (key, text) in [("secret", "classified"), ("token", "opaque")] {
        let object_id = set_hard_delete_attribute(&first, "order-1", run_id, key, text).await;
        object_keys.push(owner.object_key(&object_id));
    }
    terminate_run(&first, "order-1", run_id);
    let allowed_lists = match crash_point {
        PurgeCrashPoint::BeforeObjectDeletion | PurgeCrashPoint::DuringObjectDeletion => Some(0),
        PurgeCrashPoint::BeforeRunPurgeCommit => Some(1),
        PurgeCrashPoint::BeforeRetentionDecision | PurgeCrashPoint::AfterRunPurgeCommit => None,
    };
    if let Some(allowed_lists) = allowed_lists {
        gate.gate_run(owner.object_key_prefix(), allowed_lists);
    }
    if crash_point != PurgeCrashPoint::BeforeRetentionDecision {
        tokio::time::sleep(SHORT_RETENTION + Duration::from_secs(1)).await;
    }
    if allowed_lists.is_some() {
        gate.entered.notified().await;
        assert!(first.run_record("order-1", run_id).is_purging);
        assert_hidden_from_client_reads(&first, "order-1");
        assert!(run_purges(&first).is_empty());
    }
    match crash_point {
        PurgeCrashPoint::BeforeRetentionDecision => {
            assert!(!first.run_record("order-1", run_id).is_purging);
            assert!(client_read_error(&first, "order-1").is_none());
        }
        PurgeCrashPoint::BeforeObjectDeletion => {
            for object_key in &object_keys {
                assert!(hard_delete_value(&first, object_key).await.is_some());
            }
        }
        PurgeCrashPoint::DuringObjectDeletion => {
            // A partly deleted prefix, as when a batch delete stops midway.
            first
                .durable
                .objects
                .delete(&object_keys[0])
                .await
                .expect("delete succeeds");
            assert!(hard_delete_value(&first, &object_keys[1]).await.is_some());
        }
        PurgeCrashPoint::BeforeRunPurgeCommit => {
            for object_key in &object_keys {
                assert_eq!(hard_delete_value(&first, object_key).await, None);
            }
        }
        PurgeCrashPoint::AfterRunPurgeCommit => {
            first.settle().await;
            assert_eq!(run_purges(&first).len(), 1);
            assert!(run_record_if_exists(&first, "order-1", run_id).is_none());
        }
    }
    first.shutdown().await;

    gate.open();
    let second = EffectHarness::with_config(durable, EffectsConfig::default());
    second.rebuild_all_partitions().await;
    tokio::time::sleep(SHORT_RETENTION + Duration::from_secs(1)).await;
    second.settle().await;
    let purges = run_purges(&second);
    if crash_point == PurgeCrashPoint::AfterRunPurgeCommit {
        assert!(purges.is_empty(), "a committed purge leaves no work");
    } else {
        assert_eq!(purges.len(), 1, "{crash_point:?}");
        assert_eq!(purges[0].reply, ok_reply());
    }
    assert!(run_record_if_exists(&second, "order-1", run_id).is_none());
    assert_hidden_from_client_reads(&second, "order-1");
    let remaining = second
        .durable
        .objects
        .list(&owner.object_key_prefix(), None, 1000)
        .await
        .expect("list succeeds");
    assert!(remaining.objects.is_empty(), "{crash_point:?}");
    second.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_crash_before_the_retention_decision_commits_purges_after_restart() {
    assert_purge_recovers_after_a_crash(PurgeCrashPoint::BeforeRetentionDecision).await;
}

#[tokio::test(start_paused = true)]
async fn a_crash_after_the_purge_decision_before_object_deletion_purges_after_restart() {
    assert_purge_recovers_after_a_crash(PurgeCrashPoint::BeforeObjectDeletion).await;
}

#[tokio::test(start_paused = true)]
async fn a_crash_during_object_deletion_purges_after_restart() {
    assert_purge_recovers_after_a_crash(PurgeCrashPoint::DuringObjectDeletion).await;
}

#[tokio::test(start_paused = true)]
async fn a_crash_after_object_deletion_before_run_purge_commits_purges_after_restart() {
    assert_purge_recovers_after_a_crash(PurgeCrashPoint::BeforeRunPurgeCommit).await;
}

#[tokio::test(start_paused = true)]
async fn a_crash_after_run_purge_commits_leaves_no_purge_work() {
    assert_purge_recovers_after_a_crash(PurgeCrashPoint::AfterRunPurgeCommit).await;
}
