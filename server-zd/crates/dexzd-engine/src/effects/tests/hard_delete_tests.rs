// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_keyspace::row_keys::{HARD_DELETE_PENDING_OBJECT, hard_delete_current_key};
use dexzd_keyspace::{ColumnFamily, IndexKind, RunId, RunTable, run_table_prefix};
use dexzd_object_store::ObjectStore;
use dexzd_protocol::dex::{
    AttributeWrite, InvokeWaitForMethodResponse, SetAttributesRequest, Value,
};
use dexzd_protocol::dexzd::v1::{
    AttributesSetCommand, FlowCommand, HardDeleteObjectRecord, flow_command,
    step_method_completed_command,
};
use dexzd_storage::LocalReadView;
use prost::Message;

use super::{
    EffectHarness, error_reply, is_error_reply, is_method_completed, ok_reply, settings,
    start_request, string_value,
};
use crate::effects::state_reads::decode_row;
use crate::effects::{MethodRequest, decode_hard_delete_value, hard_delete_placeholder_value};
use crate::runtime::CommitUrgency;

fn hard_delete_write(key: &str, value: Value) -> AttributeWrite {
    AttributeWrite {
        key: key.to_owned(),
        value: Some(value),
        is_hard_delete_on_null: true,
        ..AttributeWrite::default()
    }
}

/// Sets a hard-delete Attribute through the client path: upload, then commit.
pub(super) async fn set_hard_delete_attribute(
    harness: &EffectHarness,
    flow_id: &str,
    run_id: RunId,
    key: &str,
    text: &str,
) -> String {
    let mut writes = vec![hard_delete_write(key, string_value(text))];
    let flow = EffectHarness::flow(flow_id);
    let uploads = harness
        .hard_delete_objects
        .upload_attribute_writes(
            harness.hard_delete_objects.owner_of(&flow, run_id),
            &mut writes,
        )
        .await
        .expect("upload succeeds");
    let object_id = uploads.references[0].object_id.clone();
    let command = FlowCommand {
        flow_id: flow_id.to_owned(),
        run_id: run_id.as_bytes().to_vec(),
        command: Some(flow_command::Command::AttributesSet(AttributesSetCommand {
            request: Some(SetAttributesRequest {
                flow_id: flow_id.to_owned(),
                attributes: writes,
                request_id: uuid::Uuid::now_v7().to_string(),
                ..SetAttributesRequest::default()
            }),
            hard_delete_objects: uploads.references.clone(),
            request_fingerprint: vec![1],
        })),
        ..FlowCommand::default()
    };
    let reply = harness
        .runtime
        .commit(&flow, command, CommitUrgency::Prompt)
        .expect("set commits");
    assert!(!is_error_reply(&reply), "set failed: {reply:?}");
    harness
        .hard_delete_objects
        .finish_uploads(uploads, true)
        .await
        .expect("finish succeeds");
    object_id
}

fn pending_upload_count(harness: &EffectHarness) -> usize {
    harness
        .durable
        .local_store
        .node_meta()
        .expect("read node meta")
        .expect("node meta exists")
        .pending_hard_delete_uploads
        .len()
}

async fn object_value(
    harness: &EffectHarness,
    flow_id: &str,
    run_id: RunId,
    object_id: &str,
) -> Option<Value> {
    let owner = harness
        .hard_delete_objects
        .owner_of(&EffectHarness::flow(flow_id), run_id);
    let key = owner.object_key(object_id);
    harness
        .durable
        .objects
        .get(&key)
        .await
        .expect("object read succeeds")
        .map(|content| decode_hard_delete_value(&key, &content.bytes).expect("decodes"))
}

#[tokio::test(start_paused = true)]
async fn result_hard_delete_writes_are_uploaded_before_the_command_and_reach_the_next_attempt() {
    let harness = EffectHarness::new();
    harness.rebuild_all_partitions().await;
    let run_id = harness.start_flow(start_request("order-1"), settings());
    let wait_for = harness.dispatcher.next_dispatch().await;
    wait_for.complete_wait_for(InvokeWaitForMethodResponse {
        upsert_attributes: vec![hard_delete_write("secret", string_value("card-4242"))],
        ..InvokeWaitForMethodResponse::default()
    });
    let execute = harness.dispatcher.next_dispatch().await;

    let completed = harness.runtime.commands_matching(is_method_completed);
    let Some(flow_command::Command::StepMethodCompleted(command)) = &completed[0].command.command
    else {
        panic!("expected a completion");
    };
    let Some(step_method_completed_command::Response::WaitFor(response)) = &command.response else {
        panic!("expected a WaitFor response");
    };
    assert_eq!(
        response.upsert_attributes[0].value,
        Some(hard_delete_placeholder_value()),
        "the value never enters the log"
    );
    let reference = &command.hard_delete_objects[0];
    assert_eq!(reference.write_index, 0);
    assert!(reference.is_string_value);
    assert_eq!(
        object_value(&harness, "order-1", run_id, &reference.object_id).await,
        Some(string_value("card-4242"))
    );
    assert_eq!(pending_upload_count(&harness), 0);

    let MethodRequest::Execute(request) = execute.method_request() else {
        panic!("expected an Execute request");
    };
    let secret = request
        .attributes
        .iter()
        .find(|attribute| attribute.key == "secret")
        .expect("the hard-delete Attribute is hydrated");
    assert_eq!(secret.value, Some(string_value("card-4242")));
    harness.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn rejected_results_delete_their_uploaded_objects() {
    let harness = EffectHarness::new();
    harness.rebuild_all_partitions().await;
    let run_id = harness.start_flow(start_request("order-1"), settings());
    harness.runtime.add_override(Box::new(|command, _| {
        command
            .command
            .as_ref()
            .is_some_and(is_method_completed)
            .then(|| error_reply("stale attempt"))
    }));
    let wait_for = harness.dispatcher.next_dispatch().await;
    wait_for.complete_wait_for(InvokeWaitForMethodResponse {
        upsert_attributes: vec![hard_delete_write("secret", string_value("card-4242"))],
        ..InvokeWaitForMethodResponse::default()
    });
    harness.settle().await;
    let owner = harness
        .hard_delete_objects
        .owner_of(&EffectHarness::flow("order-1"), run_id);
    let listing = harness
        .durable
        .objects
        .list(&owner.object_key_prefix(), None, 1000)
        .await
        .expect("list succeeds");
    assert!(listing.objects.is_empty());
    assert_eq!(pending_upload_count(&harness), 0);
    harness.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn overwritten_hard_delete_objects_are_deleted_then_acknowledged() {
    let harness = EffectHarness::new();
    harness.rebuild_all_partitions().await;
    harness.runtime.add_override(Box::new(|command, state| {
        let Some(flow_command::Command::HardDeleteObjectsDeleted(deleted)) = &command.command
        else {
            return None;
        };
        // Simulates apply: drop the pending rows and their index entries.
        state
            .rows
            .retain(|(column_family, key), value| match column_family {
                ColumnFamily::Flow => HardDeleteObjectRecord::decode(value.as_slice())
                    .ok()
                    .and_then(|record| record.object)
                    .is_none_or(|object| {
                        !deleted.object_ids.contains(&object.object_id)
                            || !key.ends_with(object.object_id.as_bytes())
                    }),
                ColumnFamily::PartitionIndex => {
                    key.get(2) != Some(&(IndexKind::HardDeletePending as u8))
                }
                _ => true,
            });
        Some(ok_reply())
    }));
    let run_id = harness.start_flow(start_request("order-1"), settings());
    let first = set_hard_delete_attribute(&harness, "order-1", run_id, "secret", "v1").await;
    let second = set_hard_delete_attribute(&harness, "order-1", run_id, "secret", "v2").await;
    harness.settle().await;

    assert_eq!(
        object_value(&harness, "order-1", run_id, &first).await,
        None
    );
    assert_eq!(
        object_value(&harness, "order-1", run_id, &second).await,
        Some(string_value("v2"))
    );
    let deleted = harness.runtime.commands_matching(|command| {
        matches!(command, flow_command::Command::HardDeleteObjectsDeleted(_))
    });
    let Some(flow_command::Command::HardDeleteObjectsDeleted(command)) =
        &deleted[0].command.command
    else {
        panic!("expected HardDeleteObjectsDeleted");
    };
    assert_eq!(command.object_ids, vec![first]);
    let run = harness.run_location("order-1", run_id);
    let mut pending_prefix =
        run_table_prefix(run.flow_hash, run.run_id, RunTable::HardDeleteObject);
    pending_prefix.push(HARD_DELETE_PENDING_OBJECT);
    assert!(
        harness
            .runtime
            .snapshot()
            .rows
            .keys()
            .all(|(_, key)| !key.starts_with(&pending_prefix))
    );
    let current_key = hard_delete_current_key(run.flow_hash, run.run_id, "secret");
    let current: HardDeleteObjectRecord = decode_row(
        "HardDeleteObject",
        &current_key,
        harness
            .runtime
            .snapshot()
            .rows
            .get(&(ColumnFamily::Flow, current_key.clone()))
            .expect("current object row"),
    )
    .expect("decodes");
    assert_eq!(current.object.expect("object").object_id, second);
    harness.shutdown().await;
}
