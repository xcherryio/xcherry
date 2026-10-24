// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::time::Duration;

use std::sync::{Arc, Mutex};

use dexzd_apply::FlowStateReader;
use dexzd_keyspace::row_keys::{
    hard_delete_pending_key, outbox_key, run_item_index_key, run_singleton_key,
};
use dexzd_keyspace::{ColumnFamily, IndexKind, RunTable, partition_index_prefix};
use dexzd_object_store::{FaultAction, ObjectStore, ObjectStoreOperation};
use dexzd_protocol::dex::{
    AttributeWrite, CloseDecision, CloseDecisionType, FlowStatus, InvokeExecuteMethodResponse,
    InvokeWaitForMethodResponse, StepDecision, StepOptions,
};
use dexzd_protocol::dexzd::v1::{
    ChildFlowCompletedMessage, CrossFlowEndpoint, HardDeleteObjectRecord,
    HardDeleteObjectReference, OutboxRecord, PendingHardDeleteUpload, flow_command, outbox_record,
};
use dexzd_storage::LocalStore;
use prost::Message;

use super::hard_delete_tests::set_hard_delete_attribute;
use super::outbox_and_purge_tests::put_object;
use super::{
    DurableState, EffectHarness, SECOND_MICROS, SwitchableFault, is_method_completed, ok_override,
    ok_reply, settings, start_request, string_value,
};
use crate::effects::{
    EffectError, EffectsConfig, HardDeleteUploadLedger, MethodRequest, NodeMetaUploadLedger,
};

fn restart(harness_durable: &DurableState) -> EffectHarness {
    EffectHarness::with_config(harness_durable.clone(), EffectsConfig::default())
}

fn encoded_request(request: &MethodRequest) -> Vec<u8> {
    match request {
        MethodRequest::WaitFor(request) => request.encode_to_vec(),
        MethodRequest::Execute(request) => request.encode_to_vec(),
    }
}

#[tokio::test(start_paused = true)]
async fn a_restart_redispatches_running_attempts_with_the_same_attempt_and_request() {
    let first = EffectHarness::new();
    first.rebuild_all_partitions().await;
    let run_id = first.start_flow(start_request("order-1"), settings());
    let before = first.dispatcher.next_dispatch().await;
    let task_id = first
        .step_execution("order-1", run_id, "ChargeStep-1")
        .invocation
        .expect("invocation")
        .task_id;
    let durable = first.durable.clone();
    first.shutdown().await;
    assert!(
        before
            .is_cancelled
            .load(std::sync::atomic::Ordering::SeqCst),
        "shutdown cancels the Worker task"
    );

    tokio::time::sleep(Duration::from_secs(30)).await;
    let second = restart(&durable);
    second.rebuild_all_partitions().await;
    let after = second.dispatcher.next_dispatch().await;
    assert_eq!(
        encoded_request(after.method_request()),
        encoded_request(before.method_request()),
        "S1: a redispatch sends the same snapshot"
    );
    assert_eq!(after.context().attempt, 1, "D8: no retry is consumed");
    let invocation = second
        .step_execution("order-1", run_id, "ChargeStep-1")
        .invocation
        .expect("invocation");
    assert_eq!(invocation.task_id, task_id);
    assert!(
        second
            .runtime
            .commands_matching(|command| matches!(
                command,
                flow_command::Command::StepAttemptStarted(_)
            ))
            .is_empty()
    );
    after.complete_wait_for(InvokeWaitForMethodResponse::default());
    let execute = second.dispatcher.next_dispatch().await;
    assert!(matches!(
        execute.method_request(),
        MethodRequest::Execute(_)
    ));
    second.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_restart_starts_pending_invocations() {
    let first = EffectHarness::new();
    first.rebuild_all_partitions().await;
    first
        .runtime
        .busy_lock_keys
        .lock()
        .expect("lock")
        .insert("balance".to_owned());
    let mut request = start_request("order-1");
    request.step_options = Some(StepOptions {
        wait_for_lock_attribute_keys: vec!["balance".to_owned()],
        ..StepOptions::default()
    });
    first.start_flow(request, settings());
    first.settle().await;
    let durable = first.durable.clone();
    first.shutdown().await;

    let second = restart(&durable);
    second.rebuild_all_partitions().await;
    let dispatch = second.dispatcher.next_dispatch().await;
    assert_eq!(dispatch.context().attempt, 1);
    second.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_restart_fires_overdue_timers_at_once() {
    let first = EffectHarness::new();
    first.rebuild_all_partitions().await;
    first.start_flow(start_request("order-1"), settings());
    first
        .dispatcher
        .next_dispatch()
        .await
        .fail_with_worker_error("transient");
    first.settle().await;
    let durable = first.durable.clone();
    first.shutdown().await;

    tokio::time::sleep(Duration::from_secs(60)).await;
    let second = restart(&durable);
    second.rebuild_all_partitions().await;
    let retry = tokio::time::timeout(
        Duration::from_millis(100),
        second.dispatcher.next_dispatch(),
    )
    .await
    .expect("the overdue backoff timer fires during recovery");
    assert_eq!(retry.context().attempt, 2);
    second.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_restart_deletes_uploads_whose_command_never_committed() {
    let first = EffectHarness::new();
    first.rebuild_all_partitions().await;
    let run_id = first.start_flow(start_request("order-1"), settings());
    let flow = EffectHarness::flow("order-1");
    let owner = first.hard_delete_objects.owner_of(&flow, run_id);

    // Uploaded, but the owner crashed before proposing.
    let mut orphan_writes = vec![AttributeWrite {
        key: "secret".to_owned(),
        value: Some(string_value("lost")),
        is_hard_delete_on_null: true,
        ..AttributeWrite::default()
    }];
    let orphan = first
        .hard_delete_objects
        .upload_attribute_writes(owner, &mut orphan_writes)
        .await
        .expect("upload succeeds");
    let orphan_key = owner.object_key(&orphan.references[0].object_id);
    // Committed, but the owner crashed before clearing its record.
    let committed_id = set_hard_delete_attribute(&first, "order-1", run_id, "token", "kept").await;
    first
        .durable
        .local_store
        .put_node_meta(&{
            let mut meta =
                dexzd_storage::LocalReadView::node_meta(first.durable.local_store.as_ref())
                    .expect("read")
                    .expect("exists");
            meta.pending_hard_delete_uploads.push(
                dexzd_protocol::dexzd::v1::PendingHardDeleteUpload {
                    object_key: owner.object_key(&committed_id),
                    uploaded_at_unix_micros: 1,
                },
            );
            meta
        })
        .expect("write node meta");
    let durable = first.durable.clone();
    first.shutdown().await;

    let second = restart(&durable);
    second.rebuild_all_partitions().await;
    assert!(
        durable
            .objects
            .get(&orphan_key)
            .await
            .expect("get")
            .is_none(),
        "the orphan upload is deleted"
    );
    assert!(
        durable
            .objects
            .get(&owner.object_key(&committed_id))
            .await
            .expect("get")
            .is_some(),
        "the committed upload is kept"
    );
    let meta = dexzd_storage::LocalReadView::node_meta(durable.local_store.as_ref())
        .expect("read")
        .expect("exists");
    assert!(meta.pending_hard_delete_uploads.is_empty());
    second.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_restart_resumes_outbox_deliveries_and_hard_delete_deletions() {
    let first = EffectHarness::new();
    let child_run = first.start_flow(start_request("child"), settings());
    let parent_run = first.start_flow(start_request("parent"), settings());
    let child = first.run_location("child", child_run);
    let child_partition = first.partition_of("child");
    let owner = first
        .hard_delete_objects
        .owner_of(&EffectHarness::flow("child"), child_run);
    put_object(&first, &owner.object_key("replaced")).await;
    {
        let mut state = first.runtime.state.lock().expect("lock");
        state.put_message(
            ColumnFamily::Flow,
            outbox_key(child.flow_hash, child.run_id, &[5; 16]),
            &OutboxRecord {
                message_id: vec![5; 16],
                target: Some(CrossFlowEndpoint {
                    tenant_id: 0,
                    flow_id: "parent".to_owned(),
                    run_id: parent_run.as_bytes().to_vec(),
                    run_attempt: 1,
                }),
                payload: Some(outbox_record::Payload::ChildCompleted(
                    ChildFlowCompletedMessage::default(),
                )),
                ..OutboxRecord::default()
            },
        );
        state.rows.insert(
            (
                ColumnFamily::PartitionIndex,
                run_item_index_key(
                    child_partition,
                    IndexKind::OutboxPending,
                    child.flow_hash,
                    child.run_id,
                    &[5; 16],
                ),
            ),
            Vec::new(),
        );
        state.put_message(
            ColumnFamily::Flow,
            hard_delete_pending_key(child.flow_hash, child.run_id, "replaced"),
            &HardDeleteObjectRecord {
                object: Some(HardDeleteObjectReference {
                    object_id: "replaced".to_owned(),
                    ..HardDeleteObjectReference::default()
                }),
                written_seq: 2,
            },
        );
        state.rows.insert(
            (
                ColumnFamily::PartitionIndex,
                run_item_index_key(
                    child_partition,
                    IndexKind::HardDeletePending,
                    child.flow_hash,
                    child.run_id,
                    b"replaced",
                ),
            ),
            Vec::new(),
        );
    }
    let durable = first.durable.clone();
    first.shutdown().await;

    let second = restart(&durable);
    second.runtime.add_override(ok_override(|command| {
        matches!(
            command,
            flow_command::Command::InboxMessage(_) | flow_command::Command::OutboxAck(_)
        )
    }));
    second.runtime.add_override(Box::new(|command, state| {
        let Some(flow_command::Command::HardDeleteObjectsDeleted(_)) = &command.command else {
            return None;
        };
        state.rows.retain(|_, value| {
            HardDeleteObjectRecord::decode(value.as_slice()).is_err() || value.is_empty()
        });
        Some(super::ok_reply())
    }));
    second.rebuild_all_partitions().await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    second.settle().await;

    let kinds: Vec<&'static str> = second
        .runtime
        .commands
        .lock()
        .expect("lock")
        .iter()
        .filter_map(|recorded| match recorded.command.command.as_ref()? {
            flow_command::Command::InboxMessage(_) => Some("inbox"),
            flow_command::Command::OutboxAck(_) => Some("ack"),
            flow_command::Command::HardDeleteObjectsDeleted(_) => Some("hard-delete"),
            _ => None,
        })
        .collect();
    for kind in ["inbox", "ack", "hard-delete"] {
        assert!(kinds.contains(&kind), "{kind} missing from {kinds:?}");
    }
    assert!(
        durable
            .objects
            .get(&owner.object_key("replaced"))
            .await
            .expect("get")
            .is_none()
    );
    second.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_redispatch_reads_the_current_hard_delete_object_when_the_snapshot_object_is_gone() {
    let first = EffectHarness::new();
    first.rebuild_all_partitions().await;
    let mut request = start_request("order-1");
    request.step_options = Some(StepOptions {
        skip_wait_for: true,
        ..StepOptions::default()
    });
    let run_id = first.start_flow(request, settings());
    first
        .dispatcher
        .next_dispatch()
        .await
        .complete_execute(InvokeExecuteMethodResponse {
            upsert_attributes: vec![AttributeWrite {
                key: "secret".to_owned(),
                value: Some(string_value("v1")),
                is_hard_delete_on_null: true,
                ..AttributeWrite::default()
            }],
            step_decision: Some(StepDecision {
                next_steps: vec![dexzd_protocol::dex::StepMovement {
                    step_type: "ShipStep".to_owned(),
                    step_options: Some(StepOptions {
                        skip_wait_for: true,
                        ..StepOptions::default()
                    }),
                    ..dexzd_protocol::dex::StepMovement::default()
                }],
                ..StepDecision::default()
            }),
            ..InvokeExecuteMethodResponse::default()
        });
    let ship = first.dispatcher.next_dispatch().await;
    assert_eq!(ship.context().step_execution_id, "ShipStep-1");
    let durable = first.durable.clone();
    first.shutdown().await;

    let second = restart(&durable);
    // Overwrite the Attribute while the attempt runs; the replaced object is deleted.
    set_hard_delete_attribute(&second, "order-1", run_id, "secret", "v2").await;
    second.settle().await;
    second.rebuild_all_partitions().await;
    let redispatch = second.dispatcher.next_dispatch().await;
    let MethodRequest::Execute(request) = redispatch.method_request() else {
        panic!("expected an Execute request");
    };
    let secret = request
        .attributes
        .iter()
        .find(|attribute| attribute.key == "secret")
        .expect("the Attribute is present");
    assert_eq!(secret.value, Some(string_value("v2")));
    redispatch.complete_execute(InvokeExecuteMethodResponse {
        step_decision: Some(StepDecision {
            close_decision: Some(CloseDecision {
                close_decision_type: CloseDecisionType::ForceComplete as i32,
                ..CloseDecision::default()
            }),
            ..StepDecision::default()
        }),
        ..InvokeExecuteMethodResponse::default()
    });
    second.settle().await;
    assert_eq!(
        second.runtime.commands_matching(is_method_completed).len(),
        1
    );
    assert_eq!(
        second.run_record("order-1", run_id).status,
        FlowStatus::Completed as i32
    );
    second.shutdown().await;
}

fn force_complete() -> InvokeExecuteMethodResponse {
    InvokeExecuteMethodResponse {
        step_decision: Some(StepDecision {
            close_decision: Some(CloseDecision {
                close_decision_type: CloseDecisionType::ForceComplete as i32,
                ..CloseDecision::default()
            }),
            ..StepDecision::default()
        }),
        ..InvokeExecuteMethodResponse::default()
    }
}

#[tokio::test(start_paused = true)]
async fn a_crash_after_retention_expiry_is_purged_from_the_retention_index_on_restart() {
    let (durable, object_fault) = SwitchableFault::install(
        vec![ObjectStoreOperation::List, ObjectStoreOperation::Delete],
        FaultAction::FailBeforeEffect,
    );
    let first = EffectHarness::with_config(durable, EffectsConfig::default());
    first.rebuild_all_partitions().await;
    let mut request = start_request("expired");
    request.step_options = Some(StepOptions {
        skip_wait_for: true,
        ..StepOptions::default()
    });
    let mut run_settings = settings();
    run_settings.retention_after_close_micros = 10 * SECOND_MICROS;
    let run_id = first.start_flow(request, run_settings);
    let owner = first
        .hard_delete_objects
        .owner_of(&EffectHarness::flow("expired"), run_id);
    put_object(&first, &owner.object_key("leftover")).await;
    first
        .dispatcher
        .next_dispatch()
        .await
        .complete_execute(force_complete());
    first.settle().await;
    assert_eq!(
        first.run_record("expired", run_id).status,
        FlowStatus::Completed as i32
    );
    let partition = first.partition_of("expired");
    let retention_prefix = partition_index_prefix(partition, IndexKind::Retention);
    let retention_rows = |harness: &EffectHarness| {
        harness
            .runtime
            .snapshot()
            .rows
            .keys()
            .filter(|(column_family, key)| {
                *column_family == ColumnFamily::PartitionIndex && key.starts_with(&retention_prefix)
            })
            .count()
    };
    assert_eq!(
        retention_rows(&first),
        1,
        "close writes the Retention index"
    );

    // The owner crashes after TimersFired committed and before the purge finished.
    object_fault.set_enabled(true);
    tokio::time::sleep(Duration::from_secs(12)).await;
    first.settle().await;
    assert!(first.run_record("expired", run_id).is_purging);
    assert!(
        first
            .runtime
            .commands_matching(|command| matches!(command, flow_command::Command::RunPurge(_)))
            .is_empty()
    );
    let durable = first.durable.clone();
    first.shutdown().await;
    object_fault.set_enabled(false);

    let second = restart(&durable);
    second.rebuild_all_partitions().await;
    second.settle().await;
    let purges = second
        .runtime
        .commands_matching(|command| matches!(command, flow_command::Command::RunPurge(_)));
    assert_eq!(purges.len(), 1);
    assert_eq!(purges[0].reply, ok_reply());
    let run = second.run_location("expired", run_id);
    assert!(
        second
            .runtime
            .snapshot()
            .get(
                ColumnFamily::Flow,
                &run_singleton_key(run.flow_hash, run.run_id, RunTable::RunRecord),
            )
            .expect("memory reads never fail")
            .is_none(),
        "apply deleted the run"
    );
    assert_eq!(
        retention_rows(&second),
        0,
        "RunPurge removes the Retention index"
    );
    let remaining = durable
        .objects
        .list(&owner.object_key_prefix(), None, 1000)
        .await
        .expect("list succeeds");
    assert!(remaining.objects.is_empty());
    second.shutdown().await;
}

/// Ledger whose reads fail, as a local metadata read error would.
struct UnreadableUploadLedger {
    ledger: NodeMetaUploadLedger,
}

impl HardDeleteUploadLedger for UnreadableUploadLedger {
    fn record_pending_uploads(
        &self,
        uploads: &[PendingHardDeleteUpload],
    ) -> Result<(), EffectError> {
        self.ledger.record_pending_uploads(uploads)
    }

    fn clear_pending_uploads(&self, object_keys: &[String]) -> Result<(), EffectError> {
        self.ledger.clear_pending_uploads(object_keys)
    }

    fn pending_uploads(&self) -> Result<Vec<PendingHardDeleteUpload>, EffectError> {
        Err(EffectError::CorruptState {
            table: "NodeMeta",
            key: "node".to_owned(),
            detail: "injected read failure".to_owned(),
        })
    }
}

#[tokio::test(start_paused = true)]
async fn an_unreadable_upload_ledger_fails_startup_and_the_next_restart_recovers() {
    let first = EffectHarness::new();
    first.rebuild_all_partitions().await;
    let run_id = first.start_flow(start_request("order-1"), settings());
    let owner = first
        .hard_delete_objects
        .owner_of(&EffectHarness::flow("order-1"), run_id);
    let mut orphan_writes = vec![AttributeWrite {
        key: "secret".to_owned(),
        value: Some(string_value("lost")),
        is_hard_delete_on_null: true,
        ..AttributeWrite::default()
    }];
    let orphan = first
        .hard_delete_objects
        .upload_attribute_writes(owner, &mut orphan_writes)
        .await
        .expect("upload succeeds");
    let orphan_key = owner.object_key(&orphan.references[0].object_id);
    let durable = first.durable.clone();
    first.shutdown().await;

    let ledger = Arc::new(UnreadableUploadLedger {
        ledger: NodeMetaUploadLedger::new(
            Arc::clone(&durable.local_store) as _,
            Arc::new(Mutex::new(())),
        ),
    });
    let second = EffectHarness::with_ledger(durable.clone(), EffectsConfig::default(), ledger);
    let error = second
        .try_rebuild_all_partitions()
        .await
        .expect_err("rebuild fails without the startup uploads");
    assert!(
        matches!(error, EffectError::StartupUploadsUnreadable { .. }),
        "{error}"
    );
    second.shutdown().await;
    assert!(
        durable
            .objects
            .get(&orphan_key)
            .await
            .expect("get")
            .is_some()
    );

    let third = restart(&durable);
    third.rebuild_all_partitions().await;
    assert!(
        durable
            .objects
            .get(&orphan_key)
            .await
            .expect("get")
            .is_none(),
        "the next restart deletes the orphan"
    );
    third.shutdown().await;
}
