// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use dexzd_apply::ApplyOutcome;
use dexzd_keyspace::RunId;
use dexzd_protocol::dex::{
    AttributeWrite, EncodedObject, GetAttributesRequest, InvokeRpcRequest, InvokeWorkerRpcResponse,
    Kv, SetAttributesRequest, StartFlowRequest, Value, value,
};
use dexzd_protocol::dexzd::v1::{
    AttributesSetCommand, CommandReply, FlowCommand, StartFlowReply, command_reply, flow_command,
};
use prost::Message;
use tokio::sync::Notify;
use xxhash_rust::xxh3::xxh3_128;

use crate::runtime::tests::fixtures::{
    TENANT, flow, set_attribute_request, start_flow, start_request, string_value,
};
use crate::runtime::tests::test_support::RuntimeHarness;
use crate::runtime::{
    CommitError, HardDeleteUploadError, HardDeleteUploadOutcome, RuntimeConfig, WorkerRpcResult,
    grpc_code,
};

fn blob_store_harness() -> RuntimeHarness {
    let mut config = RuntimeConfig::default();
    config.validation.has_blob_store = true;
    RuntimeHarness::with_config(&config)
}

fn hard_delete_write(key: &str, value: Value) -> AttributeWrite {
    AttributeWrite {
        key: key.to_owned(),
        value: Some(value),
        is_hard_delete_on_null: true,
        ..AttributeWrite::default()
    }
}

fn plain_write(key: &str, value: Value) -> AttributeWrite {
    AttributeWrite {
        key: key.to_owned(),
        value: Some(value),
        ..AttributeWrite::default()
    }
}

fn object_value(payload: &str) -> Value {
    Value {
        kind: Some(value::Kind::ObjValue(EncodedObject {
            encoding: "json".to_owned(),
            payload: payload.as_bytes().to_vec(),
        })),
    }
}

fn placeholder_value() -> Option<Value> {
    Some(Value { kind: None })
}

fn hard_delete_start_request(flow_id: &str) -> StartFlowRequest {
    let mut request = start_request(flow_id);
    request.flow_start_options.as_mut().unwrap().attributes = vec![
        plain_write("status", string_value("new")),
        hard_delete_write("card", string_value("4111-1111")),
    ];
    request
}

fn hard_delete_set_request(flow_id: &str) -> SetAttributesRequest {
    let mut request = set_attribute_request(flow_id, "status", string_value("paid"));
    request.attributes.push(hard_delete_write(
        "address",
        object_value(r#"{"city":"Paris"}"#),
    ));
    request
}

fn rpc_request(flow_id: &str) -> InvokeRpcRequest {
    InvokeRpcRequest {
        flow_id: flow_id.to_owned(),
        rpc_name: "updateCard".to_owned(),
        request_id: "rpc-1".to_owned(),
        ..InvokeRpcRequest::default()
    }
}

fn script_hard_delete_rpc_result(harness: &RuntimeHarness) {
    harness
        .rpc_invoker
        .results
        .lock()
        .unwrap()
        .push_back(Ok(WorkerRpcResult {
            response: InvokeWorkerRpcResponse {
                output: Some(string_value("updated")),
                upsert_attributes: vec![hard_delete_write("card", string_value("5500-0000"))],
                ..InvokeWorkerRpcResponse::default()
            },
            app_worker_id: "worker-1".to_owned(),
            pull_routing: None,
        }));
}

fn submitted_attributes_set(harness: &RuntimeHarness) -> (FlowCommand, AttributesSetCommand) {
    let last = harness.submitted().last().cloned().unwrap();
    let Some(flow_command::Command::AttributesSet(set)) = last.command.clone() else {
        panic!("expected AttributesSet, got {last:?}");
    };
    (last, set)
}

fn submitted_attributes_set_count(harness: &RuntimeHarness) -> usize {
    harness
        .submitted()
        .iter()
        .filter(|command| {
            matches!(
                command.command,
                Some(flow_command::Command::AttributesSet(_))
            )
        })
        .count()
}

fn upload_error() -> HardDeleteUploadError {
    HardDeleteUploadError {
        detail: "object storage outage".to_owned(),
    }
}

fn finished_outcomes(harness: &RuntimeHarness) -> Vec<HardDeleteUploadOutcome> {
    harness
        .hard_delete_uploader
        .finished()
        .into_iter()
        .map(|(_, outcome)| outcome)
        .collect()
}

#[tokio::test(start_paused = true)]
async fn start_flow_uploads_hard_delete_values_under_the_new_run_before_proposing() {
    let harness = blob_store_harness();
    let request = hard_delete_start_request("order-1");

    let response = harness
        .drive(harness.registry.start_flow(TENANT, request.clone()))
        .await
        .unwrap();

    let submitted = harness.submitted();
    assert_eq!(submitted.len(), 1);
    let Some(flow_command::Command::FlowStart(start)) = submitted[0].command.clone() else {
        panic!("expected FlowStart");
    };
    let new_run_id = RunId::from_slice(&start.new_run_id).unwrap();
    assert_eq!(new_run_id.to_string(), response.run_id);
    let uploads = harness.hard_delete_uploader.uploads.lock().unwrap().clone();
    assert_eq!(uploads.len(), 1);
    assert_eq!(uploads[0].0, flow("order-1"));
    assert_eq!(uploads[0].1, new_run_id, "objects belong to the new run");
    assert_eq!(start.hard_delete_objects.len(), 1);
    assert_eq!(start.hard_delete_objects[0].write_index, 1);
    assert!(start.hard_delete_objects[0].is_string_value);
    let attributes = start
        .request
        .as_ref()
        .and_then(|request| request.flow_start_options.as_ref())
        .map(|options| options.attributes.clone())
        .unwrap();
    assert_eq!(attributes[0].value, Some(string_value("new")));
    assert_eq!(
        attributes[1].value,
        placeholder_value(),
        "the uploaded value never enters the log"
    );
    assert_eq!(
        start.request_fingerprint,
        xxh3_128(&request.encode_to_vec()).to_be_bytes().to_vec(),
        "the fingerprint covers the original values"
    );
    let finished = harness.hard_delete_uploader.finished();
    assert_eq!(finished.len(), 1);
    assert_eq!(finished[0].0.references, start.hard_delete_objects);
    assert_eq!(finished[0].1, HardDeleteUploadOutcome::Committed);
    assert!(!harness.registry.halted().is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn set_attributes_uploads_hard_delete_values_for_the_run_it_targets() {
    let harness = blob_store_harness();
    let run_id = start_flow(&harness, "order-1").await.unwrap();
    assert!(
        harness
            .hard_delete_uploader
            .uploads
            .lock()
            .unwrap()
            .is_empty(),
        "commands without hard-delete values upload nothing"
    );

    harness
        .drive(
            harness
                .registry
                .set_attributes(TENANT, hard_delete_set_request("order-1")),
        )
        .await
        .unwrap();

    let (command, set) = submitted_attributes_set(&harness);
    assert_eq!(command.run_id, run_id.as_bytes().to_vec());
    let uploads = harness.hard_delete_uploader.uploads.lock().unwrap().clone();
    assert_eq!(uploads.len(), 1);
    assert_eq!(uploads[0].1, run_id);
    assert_eq!(set.hard_delete_objects.len(), 1);
    assert_eq!(set.hard_delete_objects[0].write_index, 1);
    assert!(!set.hard_delete_objects[0].is_string_value);
    let request = set.request.unwrap();
    assert_eq!(request.attributes[0].value, Some(string_value("paid")));
    assert_eq!(request.attributes[1].value, placeholder_value());
    assert_eq!(
        set.request_fingerprint,
        xxh3_128(&hard_delete_set_request("order-1").encode_to_vec())
            .to_be_bytes()
            .to_vec()
    );
    assert_eq!(
        finished_outcomes(&harness),
        vec![HardDeleteUploadOutcome::Committed]
    );
}

#[tokio::test(start_paused = true)]
async fn committed_rpc_effects_upload_hard_delete_values_for_the_snapshot_run() {
    let harness = blob_store_harness();
    let run_id = start_flow(&harness, "order-1").await.unwrap();
    script_hard_delete_rpc_result(&harness);

    let response = harness
        .drive(harness.registry.invoke_rpc(TENANT, rpc_request("order-1")))
        .await
        .unwrap();

    assert_eq!(response.output, Some(string_value("updated")));
    let last = harness.submitted().last().cloned().unwrap();
    assert_eq!(last.run_id, run_id.as_bytes().to_vec());
    let Some(flow_command::Command::RpcCommitted(rpc)) = last.command else {
        panic!("expected RpcCommitted");
    };
    let uploads = harness.hard_delete_uploader.uploads.lock().unwrap().clone();
    assert_eq!(uploads.len(), 1);
    assert_eq!(uploads[0].1, run_id);
    assert_eq!(rpc.hard_delete_objects.len(), 1);
    assert_eq!(rpc.hard_delete_objects[0].write_index, 0);
    assert_eq!(
        rpc.response.unwrap().upsert_attributes[0].value,
        placeholder_value()
    );
    assert_eq!(
        finished_outcomes(&harness),
        vec![HardDeleteUploadOutcome::Committed]
    );
}

#[tokio::test(start_paused = true)]
async fn failed_upload_replies_unavailable_and_proposes_nothing() {
    let harness = blob_store_harness();
    harness
        .hard_delete_uploader
        .upload_errors
        .lock()
        .unwrap()
        .push_back(upload_error());
    let start_error = harness
        .drive(
            harness
                .registry
                .start_flow(TENANT, hard_delete_start_request("order-1")),
        )
        .await
        .unwrap_err();
    assert_eq!(
        start_error.grpc_code(),
        grpc_code::UNAVAILABLE,
        "{start_error:?}"
    );
    assert!(harness.submitted().is_empty());

    start_flow(&harness, "order-1").await.unwrap();
    harness
        .hard_delete_uploader
        .upload_errors
        .lock()
        .unwrap()
        .extend([upload_error(), upload_error()]);
    let set_error = harness
        .drive(
            harness
                .registry
                .set_attributes(TENANT, hard_delete_set_request("order-1")),
        )
        .await
        .unwrap_err();
    assert_eq!(
        set_error.grpc_code(),
        grpc_code::UNAVAILABLE,
        "{set_error:?}"
    );
    script_hard_delete_rpc_result(&harness);
    let rpc_error = harness
        .drive(harness.registry.invoke_rpc(TENANT, rpc_request("order-1")))
        .await
        .unwrap_err();
    assert_eq!(
        rpc_error.grpc_code(),
        grpc_code::UNAVAILABLE,
        "{rpc_error:?}"
    );

    assert_eq!(
        harness.submitted().len(),
        1,
        "only the plain FlowStart is proposed"
    );
    assert!(
        harness.hard_delete_uploader.finished().is_empty(),
        "a failed upload leaves nothing to finish"
    );
    assert!(!harness.registry.halted().is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn hard_delete_value_without_an_uploaded_object_never_reaches_apply() {
    let harness = blob_store_harness();
    *harness
        .hard_delete_uploader
        .skipped_write_index
        .lock()
        .unwrap() = Some(1);
    let mut request = start_request("order-1");
    request.flow_start_options.as_mut().unwrap().attributes = vec![
        hard_delete_write("card", string_value("4111-1111")),
        hard_delete_write("pin", string_value("1234")),
    ];

    let error = harness
        .drive(harness.registry.start_flow(TENANT, request))
        .await
        .unwrap_err();

    assert_eq!(error.grpc_code(), grpc_code::INTERNAL, "{error:?}");
    assert!(harness.submitted().is_empty());
    assert!(harness.applier.applied.lock().unwrap().is_empty());
    let finished = harness.hard_delete_uploader.finished();
    assert_eq!(finished.len(), 1);
    assert_eq!(finished[0].0.references.len(), 1);
    assert_eq!(finished[0].1, HardDeleteUploadOutcome::NotCommitted);
    assert!(!harness.registry.halted().is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn rejected_commands_finish_their_uploads_as_not_committed() {
    let harness = blob_store_harness();
    start_flow(&harness, "order-1").await.unwrap();
    let mut request = hard_delete_set_request("order-1");
    request.run_id = RunId(uuid::Uuid::now_v7().into_bytes()).to_string();
    let set_error = harness
        .drive(harness.registry.set_attributes(TENANT, request))
        .await
        .unwrap_err();
    assert_eq!(set_error.grpc_code(), grpc_code::NOT_FOUND, "{set_error:?}");
    assert_eq!(
        submitted_attributes_set_count(&harness),
        1,
        "apply rejected it"
    );

    // A rejection may carry a non-error reply, such as an ignored already-started error.
    let existing_run_id = RunId(uuid::Uuid::now_v7().into_bytes());
    let existing_run = ApplyOutcome::rejected(CommandReply {
        reply: Some(command_reply::Reply::StartFlow(StartFlowReply {
            run_id: existing_run_id.as_bytes().to_vec(),
            is_existing_run: true,
        })),
    });
    // The speculative apply and the committed apply each take one scripted outcome.
    harness
        .applier
        .scripted
        .lock()
        .unwrap()
        .extend([Ok(existing_run.clone()), Ok(existing_run)]);
    let response = harness
        .drive(
            harness
                .registry
                .start_flow(TENANT, hard_delete_start_request("order-2")),
        )
        .await
        .unwrap();
    assert_eq!(response.run_id, existing_run_id.to_string());

    assert_eq!(
        finished_outcomes(&harness),
        vec![
            HardDeleteUploadOutcome::NotCommitted,
            HardDeleteUploadOutcome::NotCommitted
        ]
    );
    assert!(!harness.registry.halted().is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn command_the_pipeline_never_accepted_finishes_as_not_committed() {
    let harness = blob_store_harness();
    start_flow(&harness, "order-1").await.unwrap();
    harness
        .committer
        .submit_errors
        .lock()
        .unwrap()
        .push_back(CommitError::Unavailable {
            detail: "commit queue is full".to_owned(),
        });

    let error = harness
        .drive(
            harness
                .registry
                .set_attributes(TENANT, hard_delete_set_request("order-1")),
        )
        .await
        .unwrap_err();

    assert_eq!(error.grpc_code(), grpc_code::UNAVAILABLE, "{error:?}");
    assert_eq!(submitted_attributes_set_count(&harness), 0);
    assert_eq!(
        finished_outcomes(&harness),
        vec![HardDeleteUploadOutcome::NotCommitted]
    );
}

#[tokio::test(start_paused = true)]
async fn failed_commit_finishes_uploads_as_unknown() {
    let harness = blob_store_harness();
    start_flow(&harness, "order-1").await.unwrap();
    let operation = harness
        .registry
        .set_attributes(TENANT, hard_delete_set_request("order-1"));
    tokio::pin!(operation);
    tokio::select! {
        _ = &mut operation => panic!("the write waits for its commit"),
        () = tokio::time::sleep(Duration::from_millis(10)) => {}
    }
    assert_eq!(harness.committer.pending_count(), 1);
    assert!(harness.hard_delete_uploader.finished().is_empty());

    assert!(harness.fail_next(CommitError::Unavailable {
        detail: "log append failed".to_owned(),
    }));
    let error = operation.await.unwrap_err();

    assert_eq!(error.grpc_code(), grpc_code::UNAVAILABLE, "{error:?}");
    assert!(harness.registry.halted().is_cancelled());
    assert_eq!(
        finished_outcomes(&harness),
        vec![HardDeleteUploadOutcome::Unknown],
        "the command may be durable, so recovery decides"
    );
}

#[tokio::test(start_paused = true)]
async fn current_run_change_during_the_upload_proposes_nothing() {
    let harness = blob_store_harness();
    let first_run_id = start_flow(&harness, "order-1").await.unwrap();
    let gate = Arc::new(Notify::new());
    *harness.hard_delete_uploader.gate.lock().unwrap() = Some(Arc::clone(&gate));
    let operation = harness
        .registry
        .set_attributes(TENANT, hard_delete_set_request("order-1"));
    tokio::pin!(operation);
    tokio::select! {
        _ = &mut operation => panic!("the upload waits for the gate"),
        () = harness.hard_delete_uploader.entered.notified() => {}
    }

    let second_run_id = start_flow(&harness, "order-1").await.unwrap();
    gate.notify_one();
    let error = harness.drive(operation).await.unwrap_err();

    assert_ne!(first_run_id, second_run_id);
    assert_eq!(error.grpc_code(), grpc_code::UNAVAILABLE, "{error:?}");
    let uploads = harness.hard_delete_uploader.uploads.lock().unwrap().clone();
    assert_eq!(uploads[0].1, first_run_id);
    assert_eq!(submitted_attributes_set_count(&harness), 0);
    assert_eq!(
        finished_outcomes(&harness),
        vec![HardDeleteUploadOutcome::NotCommitted]
    );
}

#[tokio::test(start_paused = true)]
async fn apply_stores_uploaded_hard_delete_values_that_get_attributes_reads_back() {
    let harness = blob_store_harness();
    harness
        .applier
        .uses_real_apply
        .store(true, Ordering::SeqCst);

    harness
        .drive(
            harness
                .registry
                .start_flow(TENANT, hard_delete_start_request("order-1")),
        )
        .await
        .unwrap();
    harness
        .drive(
            harness
                .registry
                .set_attributes(TENANT, hard_delete_set_request("order-1")),
        )
        .await
        .unwrap();
    assert!(
        !harness.registry.halted().is_cancelled(),
        "apply found an object for every hard-delete write"
    );

    let uploads = harness.hard_delete_uploader.uploads.lock().unwrap().clone();
    let finished = harness.hard_delete_uploader.finished();
    for ((_, _, writes), (uploaded, outcome)) in uploads.iter().zip(&finished) {
        assert_eq!(*outcome, HardDeleteUploadOutcome::Committed);
        for reference in &uploaded.references {
            let write_index = usize::try_from(reference.write_index).unwrap();
            harness.hard_delete_reader.values.lock().unwrap().insert(
                reference.object_id.clone(),
                writes[write_index].value.clone().unwrap(),
            );
        }
    }
    let response = harness
        .registry
        .get_attributes(
            TENANT,
            GetAttributesRequest {
                flow_id: "order-1".to_owned(),
                all_keys: true,
                ..GetAttributesRequest::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        response.attributes,
        vec![
            Kv {
                key: "address".to_owned(),
                value: Some(object_value(r#"{"city":"Paris"}"#)),
            },
            Kv {
                key: "card".to_owned(),
                value: Some(string_value("4111-1111")),
            },
            Kv {
                key: "status".to_owned(),
                value: Some(string_value("paid")),
            },
        ]
    );
}
