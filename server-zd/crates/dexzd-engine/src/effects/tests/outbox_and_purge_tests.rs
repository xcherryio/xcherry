// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::atomic::Ordering;

use bytes::Bytes;
use dexzd_apply::ApplyEffect;
use dexzd_keyspace::row_keys::{outbox_key, run_singleton_key};
use dexzd_keyspace::{ColumnFamily, RunId, RunTable};
use dexzd_object_store::{ObjectStore, WriteCondition};
use dexzd_protocol::dex::{FlowResult, FlowStatus};
use dexzd_protocol::dexzd::v1::{
    ChildFlowCompletedMessage, CrossFlowEndpoint, OutboxRecord, RunRecord, flow_command,
    inbox_message_command, outbox_record,
};
use prost::Message;

use super::{EffectHarness, ok_override, ok_reply, settings, start_request};
use crate::runtime::RuntimeEffectSink;

const MESSAGE_ID: [u8; 16] = [9; 16];

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
