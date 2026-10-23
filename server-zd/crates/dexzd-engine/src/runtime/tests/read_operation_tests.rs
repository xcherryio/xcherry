// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_keyspace::{ColumnFamily, RunId, RunTable, ValueId, ValueReference, run_key};
use dexzd_protocol::dex::{
    ChannelMessage, EncodedObject, ErrorSubStatus, FlowStatus, GetAttributesRequest,
    GetChannelMessagesRequest, GetFlowStateRequest, GetFlowSummaryRequest, LoadBlobRequestEntry,
    LoadBlobsRequest, PublishToChannelRequest, Value, value,
};
use dexzd_protocol::dexzd::v1::{
    AttributeRecord, HardDeleteObjectReference, PermissionSetRecord, StoredValue, StoredValueId,
    StoredValueReference, ValueRecord, stored_value,
};
use prost::Message;

use crate::CommitUrgency;
use crate::runtime::grpc_code;
use crate::runtime::tests::fixtures::{
    TENANT, attributes_set_command, flow, set_attribute_request, start_flow, string_value,
};
use crate::runtime::tests::test_support::RuntimeHarness;

fn attributes_request(flow_id: &str, keys: &[&str], all_keys: bool) -> GetAttributesRequest {
    GetAttributesRequest {
        flow_id: flow_id.to_owned(),
        keys: keys.iter().map(|key| (*key).to_owned()).collect(),
        all_keys,
        ..GetAttributesRequest::default()
    }
}

fn seed_attribute(harness: &RuntimeHarness, run_id: RunId, key: &str, stored: StoredValue) {
    let context = harness.registry.flow_context(&flow("order-1"));
    harness.store.put(
        ColumnFamily::Flow,
        run_key(
            context.flow_hash,
            run_id,
            RunTable::Attribute,
            key.as_bytes(),
        ),
        AttributeRecord {
            value: Some(stored),
            ..AttributeRecord::default()
        }
        .encode_to_vec(),
    );
}

fn seed_table_value(harness: &RuntimeHarness, run_id: RunId, value_id: ValueId, value: Value) {
    let context = harness.registry.flow_context(&flow("order-1"));
    harness.store.put(
        ColumnFamily::Value,
        run_key(
            context.flow_hash,
            run_id,
            RunTable::Value,
            &value_id.to_key_suffix(),
        ),
        ValueRecord { value: Some(value) }.encode_to_vec(),
    );
}

fn table_reference(value_id: ValueId) -> StoredValue {
    StoredValue {
        kind: Some(stored_value::Kind::TableValue(StoredValueReference {
            value_id: Some(StoredValueId {
                run_seq: value_id.run_seq,
                index_in_command: value_id.index_in_command,
            }),
            size_bytes: 500,
            is_string_value: true,
        })),
    }
}

#[tokio::test(start_paused = true)]
async fn get_attributes_returns_sorted_committed_values_references_and_hard_delete_content() {
    let harness = RuntimeHarness::new();
    let run_id = start_flow(&harness, "order-1").await.unwrap();
    harness
        .drive(harness.registry.set_attributes(
            TENANT,
            set_attribute_request("order-1", "zeta", string_value("last")),
        ))
        .await
        .unwrap();
    let value_id = ValueId {
        run_seq: 3,
        index_in_command: 0,
    };
    seed_attribute(&harness, run_id, "big", table_reference(value_id));
    seed_attribute(
        &harness,
        run_id,
        "secret",
        StoredValue {
            kind: Some(stored_value::Kind::HardDeleteObject(
                HardDeleteObjectReference {
                    object_id: "object-1".to_owned(),
                    is_string_value: true,
                    ..HardDeleteObjectReference::default()
                },
            )),
        },
    );
    harness
        .hard_delete_reader
        .values
        .lock()
        .unwrap()
        .insert("object-1".to_owned(), string_value("classified"));
    harness
        .registry
        .propose(
            flow("order-1"),
            attributes_set_command("order-1", run_id, "speculative", string_value("hidden")),
            CommitUrgency::Batched,
        )
        .await
        .unwrap();

    let response = harness
        .registry
        .get_attributes(
            TENANT,
            attributes_request("order-1", &["zeta", "missing", "big", "zeta"], false),
        )
        .await
        .unwrap();
    let keys: Vec<_> = response
        .attributes
        .iter()
        .map(|kv| kv.key.as_str())
        .collect();
    assert_eq!(keys, ["big", "zeta"]);
    let reference = ValueReference { run_id, value_id }.to_string();
    assert_eq!(
        response.attributes[0].value,
        Some(Value {
            kind: Some(value::Kind::InternalBlobIdForStringValue(reference)),
        })
    );

    let all = harness
        .registry
        .get_attributes(TENANT, attributes_request("order-1", &[], true))
        .await
        .unwrap();
    let keys: Vec<_> = all.attributes.iter().map(|kv| kv.key.as_str()).collect();
    assert_eq!(
        keys,
        ["big", "secret", "zeta"],
        "speculative writes stay invisible"
    );
    assert_eq!(all.attributes[1].value, Some(string_value("classified")));

    harness.hard_delete_reader.values.lock().unwrap().clear();
    let deleted = harness
        .registry
        .get_attributes(TENANT, attributes_request("order-1", &["secret"], false))
        .await
        .unwrap();
    assert!(
        deleted.attributes.is_empty(),
        "a deleted hard-delete value is omitted"
    );
}

#[tokio::test(start_paused = true)]
async fn get_attributes_includes_the_work_queue_permission_projection() {
    let harness = RuntimeHarness::new();
    let run_id = start_flow(&harness, "order-1").await.unwrap();
    let context = harness.registry.flow_context(&flow("order-1"));
    harness.store.put(
        ColumnFamily::Flow,
        run_key(context.flow_hash, run_id, RunTable::PermissionSet, &[]),
        PermissionSetRecord {
            permissions: vec!["approve".to_owned(), "a<b".to_owned()],
        }
        .encode_to_vec(),
    );
    let response = harness
        .registry
        .get_attributes(TENANT, attributes_request("order-1", &[], true))
        .await
        .unwrap();
    assert_eq!(response.attributes.len(), 1);
    assert_eq!(response.attributes[0].key, "DexWorkQueuePermissions");
    assert_eq!(
        response.attributes[0].value,
        Some(Value {
            kind: Some(value::Kind::ObjValue(EncodedObject {
                encoding: "json".to_owned(),
                payload: br#"["approve","a\u003cb"]"#.to_vec(),
            })),
        })
    );
}

#[tokio::test(start_paused = true)]
async fn reads_of_unknown_flows_and_runs_are_not_found() {
    let harness = RuntimeHarness::new();
    start_flow(&harness, "order-1").await.unwrap();
    let error = harness
        .registry
        .get_attributes(TENANT, attributes_request("missing", &["a"], false))
        .await
        .unwrap_err();
    assert_eq!(error.sub_status(), ErrorSubStatus::FlowNotExists);
    let mut request = attributes_request("order-1", &["a"], false);
    request.run_id = RunId([3; 16]).to_string();
    let error = harness
        .registry
        .get_attributes(TENANT, request)
        .await
        .unwrap_err();
    assert_eq!(error.sub_status(), ErrorSubStatus::FlowNotExists);
    let error = harness
        .registry
        .get_attributes(TENANT, attributes_request("", &[], true))
        .await
        .unwrap_err();
    assert_eq!(error.grpc_code(), grpc_code::INVALID_ARGUMENT);
}

#[tokio::test(start_paused = true)]
async fn get_channel_messages_returns_pending_messages_of_one_channel_in_order() {
    let harness = RuntimeHarness::new();
    start_flow(&harness, "order-1").await.unwrap();
    let message = |channel: &str, text: &str| ChannelMessage {
        channel_name: channel.to_owned(),
        value: Some(string_value(text)),
        message_id: String::new(),
    };
    harness
        .drive(harness.registry.publish_to_channel(
            TENANT,
            PublishToChannelRequest {
                flow_id: "order-1".to_owned(),
                messages: vec![
                    message("orders", "first"),
                    message("orders-archive", "other"),
                    message("orders", "second"),
                ],
                ..PublishToChannelRequest::default()
            },
        ))
        .await
        .unwrap();
    let response = harness
        .registry
        .get_channel_messages(
            TENANT,
            GetChannelMessagesRequest {
                flow_id: "order-1".to_owned(),
                channel_name: "orders".to_owned(),
                ..GetChannelMessagesRequest::default()
            },
        )
        .await
        .unwrap();
    let values: Vec<_> = response
        .messages
        .iter()
        .map(|message| message.value.clone().unwrap())
        .collect();
    assert_eq!(values, [string_value("first"), string_value("second")]);
    assert!(
        response
            .messages
            .iter()
            .all(|message| !message.message_id.is_empty())
    );
}

#[tokio::test(start_paused = true)]
async fn get_flow_summary_reports_run_identity_status_and_times() {
    let harness = RuntimeHarness::new();
    let run_id = start_flow(&harness, "order-1").await.unwrap();
    let response = harness
        .registry
        .get_flow_summary(
            TENANT,
            GetFlowSummaryRequest {
                flow_id: "order-1".to_owned(),
                ..GetFlowSummaryRequest::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        response.flow_execution_id.unwrap().run_id,
        run_id.to_string()
    );
    assert_eq!(response.first_run_id, run_id.to_string());
    assert_eq!(response.request_id, "start-order-1");
    assert_eq!(response.flow_type, "OrderFlow");
    assert_eq!(response.flow_status, FlowStatus::Running as i32);
    assert_eq!(response.start_time.unwrap().seconds, 1);
    assert!(response.close_time.is_none());
    assert_eq!(response.run_attempt, 1);
}

#[tokio::test(start_paused = true)]
async fn load_blobs_checks_the_owning_flow_and_omits_missing_values() {
    let harness = RuntimeHarness::new();
    let run_id = start_flow(&harness, "order-1").await.unwrap();
    start_flow(&harness, "order-2").await.unwrap();
    let present = ValueId {
        run_seq: 4,
        index_in_command: 1,
    };
    let missing = ValueId {
        run_seq: 5,
        index_in_command: 0,
    };
    seed_table_value(&harness, run_id, present, string_value("large"));
    let entry = |flow_id: &str, value_id: ValueId| LoadBlobRequestEntry {
        flow_id: flow_id.to_owned(),
        blob_value: Some(Value {
            kind: Some(value::Kind::InternalBlobIdForObjValue(
                ValueReference { run_id, value_id }.to_string(),
            )),
        }),
    };

    let response = harness
        .registry
        .load_blobs(
            TENANT,
            LoadBlobsRequest {
                entries: vec![entry("order-1", present), entry("order-1", missing)],
            },
        )
        .await
        .unwrap();
    assert_eq!(response.values.len(), 1);
    assert_eq!(
        response.values[&ValueReference {
            run_id,
            value_id: present
        }
        .to_string()],
        string_value("large")
    );

    let error = harness
        .registry
        .load_blobs(
            TENANT,
            LoadBlobsRequest {
                entries: vec![entry("order-2", present)],
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error.grpc_code(), grpc_code::INVALID_ARGUMENT, "{error:?}");

    let malformed = LoadBlobRequestEntry {
        flow_id: "order-1".to_owned(),
        blob_value: Some(Value {
            kind: Some(value::Kind::InternalBlobIdForObjValue(
                "blob-123".to_owned(),
            )),
        }),
    };
    let error = harness
        .registry
        .load_blobs(
            TENANT,
            LoadBlobsRequest {
                entries: vec![malformed],
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error.grpc_code(), grpc_code::INVALID_ARGUMENT);
}

#[tokio::test(start_paused = true)]
async fn read_model_operations_report_unimplemented_until_the_read_model_lands() {
    let harness = RuntimeHarness::new();
    let error = harness
        .registry
        .get_flow_state(
            TENANT,
            GetFlowStateRequest {
                flow_id: "order-1".to_owned(),
                ..GetFlowStateRequest::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error.grpc_code(), grpc_code::UNIMPLEMENTED);
}
