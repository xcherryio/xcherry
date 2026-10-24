// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;

use dexzd_apply::{
    ApplyContext, ApplyEffect, ApplyOutcome, FlowStateReader, StateReadError, StateRow, StateWrite,
    apply_flow_command,
};
use dexzd_keyspace::row_keys::{
    channel_message_key, history_record_key, request_dedup_key, run_named_key, run_singleton_key,
    value_row_key,
};
use dexzd_keyspace::{
    ColumnFamily, PartitionHasher, RunId, RunTable, TenantId, ValueId, run_table_prefix,
};
use dexzd_protocol::dex::{
    ActionPermissionMapping, ActionPermissionMappings, AttributeWrite, ChannelMessage,
    ChannelMessageDeletion, ErrorSubStatus, FlowStatus, FlowTimeoutPolicy, InvokeRpcRequest,
    InvokeWorkerRpcResponse, Kv, SetAttributesRequest, StartFlowRequest, StepDecision,
    StepMovement, StepOptions, StopFlowRequest, Value, WaitForMethodFailurePolicy, WorkerTarget,
    flow_history_event, value,
};
use dexzd_protocol::dexzd::v1::{
    AttributeRecord, AttributesSetCommand, ChannelMessageRecord, CommandReply, FlowCommand,
    FlowStartCommand, HistoryRecord, RpcCommittedCommand, RunRecord, RunSettings,
    StepExecutionRecord, ValueBirthRecord, command_reply, flow_command::Command,
};
use prost::Message;

#[derive(Default)]
struct MemoryState {
    rows: BTreeMap<(ColumnFamily, Vec<u8>), Vec<u8>>,
}

impl FlowStateReader for MemoryState {
    fn get(
        &self,
        column_family: ColumnFamily,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StateReadError> {
        Ok(self.rows.get(&(column_family, key.to_vec())).cloned())
    }

    fn scan(
        &self,
        column_family: ColumnFamily,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<StateRow>, StateReadError> {
        Ok(self
            .rows
            .range((column_family, start.to_vec())..(column_family, end.to_vec()))
            .take(limit)
            .map(|((_, key), value)| (key.clone(), value.clone()))
            .collect())
    }
}

impl MemoryState {
    fn commit(&mut self, outcome: &ApplyOutcome) {
        for write in &outcome.writes {
            match write {
                StateWrite::Put {
                    column_family,
                    key,
                    value,
                } => {
                    self.rows
                        .insert((*column_family, key.clone()), value.clone());
                }
                StateWrite::Delete { column_family, key } => {
                    self.rows.remove(&(*column_family, key.clone()));
                }
                StateWrite::DeleteRange {
                    column_family,
                    start,
                    end,
                } => {
                    let keys: Vec<_> = self
                        .rows
                        .range((*column_family, start.clone())..(*column_family, end.clone()))
                        .map(|(key, _)| key.clone())
                        .collect();
                    for key in keys {
                        self.rows.remove(&key);
                    }
                }
            }
        }
    }

    fn row<RowMessage: Message + Default>(
        &self,
        column_family: ColumnFamily,
        key: &[u8],
    ) -> Option<RowMessage> {
        self.rows
            .get(&(column_family, key.to_vec()))
            .map(|bytes| RowMessage::decode(bytes.as_slice()).expect("state row decodes"))
    }
}

struct Harness {
    state: MemoryState,
    context: ApplyContext,
    run_id: Vec<u8>,
    now_micros: i64,
}

impl Harness {
    fn new(dedup_capacity: u32, max_value_bytes: u64) -> Self {
        let hasher = PartitionHasher::new(&[7; 16], 1024).expect("valid partition hasher");
        let flow_hash = hasher.flow_hash(TenantId::DEFAULT, "rpc-flow");
        let mut harness = Self {
            state: MemoryState::default(),
            context: ApplyContext {
                partition: hasher.partition_of(flow_hash),
                flow_hash,
            },
            run_id: vec![1; 16],
            now_micros: 1_800_000_000_000_000,
        };
        let start = harness.command(Command::FlowStart(FlowStartCommand {
            new_run_id: harness.run_id.clone(),
            request: Some(StartFlowRequest {
                flow_id: "rpc-flow".to_owned(),
                flow_type: "RpcFlow".to_owned(),
                start_step_type: "StartStep".to_owned(),
                request_id: "start-1".to_owned(),
                ..StartFlowRequest::default()
            }),
            settings: Some(RunSettings {
                wire_inline_threshold_bytes: 100,
                max_value_bytes,
                request_dedup_capacity: dedup_capacity,
                ..RunSettings::default()
            }),
            resolved_timeout_policy: FlowTimeoutPolicy::Fail as i32,
            worker_target: Some(WorkerTarget {
                is_pull_mode: true,
                ..WorkerTarget::default()
            }),
            ..FlowStartCommand::default()
        }));
        let started = harness.apply(start);
        assert!(matches!(
            started.reply.reply,
            Some(command_reply::Reply::StartFlow(_))
        ));
        harness
    }

    fn run_id(&self) -> RunId {
        RunId::from_slice(&self.run_id).expect("valid run ID")
    }

    fn command(&mut self, body: Command) -> FlowCommand {
        self.now_micros += 1000;
        FlowCommand {
            flow_id: "rpc-flow".to_owned(),
            run_id: if matches!(body, Command::FlowStart(_)) {
                Vec::new()
            } else {
                self.run_id.clone()
            },
            expected_run_attempt: 0,
            epoch: 1,
            behavior_version: 1,
            proposed_at_micros: self.now_micros,
            command_id: vec![2; 16],
            tenant_id: 0,
            command: Some(body),
        }
    }

    fn apply(&mut self, command: FlowCommand) -> ApplyOutcome {
        let outcome =
            apply_flow_command(&self.context, &self.state, &command).expect("apply succeeds");
        let replay =
            apply_flow_command(&self.context, &self.state, &command).expect("replay succeeds");
        assert_eq!(outcome.writes, replay.writes);
        assert_eq!(outcome.effects, replay.effects);
        assert_eq!(outcome.reply, replay.reply);
        self.state.commit(&outcome);
        outcome
    }

    fn rpc(
        &mut self,
        request: InvokeRpcRequest,
        response: InvokeWorkerRpcResponse,
        fingerprint: &[u8],
    ) -> ApplyOutcome {
        let is_transactional = request.is_transactional || !request.lock_attribute_keys.is_empty();
        let command = self.command(Command::RpcCommitted(RpcCommittedCommand {
            request: Some(request),
            response: Some(response),
            app_worker_id: "worker-rpc".to_owned(),
            request_fingerprint: fingerprint.to_vec(),
            is_transactional,
            ..RpcCommittedCommand::default()
        }));
        self.apply(command)
    }

    fn run_record(&self) -> RunRecord {
        self.state
            .row(
                ColumnFamily::Flow,
                &run_singleton_key(self.context.flow_hash, self.run_id(), RunTable::RunRecord),
            )
            .expect("run record exists")
    }
}

fn request(request_id: &str) -> InvokeRpcRequest {
    InvokeRpcRequest {
        flow_id: "rpc-flow".to_owned(),
        rpc_name: "updateOrder".to_owned(),
        request_id: request_id.to_owned(),
        ..InvokeRpcRequest::default()
    }
}

fn string_value(text: &str) -> Value {
    Value {
        kind: Some(value::Kind::StringValue(text.to_owned())),
    }
}

fn assert_error(reply: &CommandReply, code: i32, sub_status: ErrorSubStatus) {
    let Some(command_reply::Reply::Error(error)) = &reply.reply else {
        panic!("expected error reply, got {reply:?}");
    };
    assert_eq!(error.grpc_code, code);
    assert_eq!(error.sub_status, sub_status as i32);
}

#[test]
fn plain_rpc_applies_writes_publications_permissions_and_history() {
    let mut harness = Harness::new(1000, 16 << 20);
    let response = InvokeWorkerRpcResponse {
        output: Some(string_value("done")),
        upsert_attributes: vec![AttributeWrite {
            key: "status".to_owned(),
            value: Some(string_value("ready")),
            ..AttributeWrite::default()
        }],
        publish_to_channel: vec![ChannelMessage {
            channel_name: "updates".to_owned(),
            message_id: "message-1".to_owned(),
            value: Some(string_value("published")),
        }],
        action_permission_mappings: Some(ActionPermissionMappings {
            mappings: vec![ActionPermissionMapping {
                attribute_key: "status".to_owned(),
                equal_values: vec![string_value("ready")],
                required_permission: "read-order".to_owned(),
            }],
        }),
        ..InvokeWorkerRpcResponse::default()
    };
    let outcome = harness.rpc(request("rpc-1"), response, &[1]);
    assert!(matches!(
        outcome.reply.reply,
        Some(command_reply::Reply::Rpc(_))
    ));
    let attribute: AttributeRecord = harness
        .state
        .row(
            ColumnFamily::Flow,
            &run_named_key(
                harness.context.flow_hash,
                harness.run_id(),
                RunTable::Attribute,
                "status",
            ),
        )
        .expect("attribute exists");
    assert!(attribute.value.is_some());
    let message: ChannelMessageRecord = harness
        .state
        .row(
            ColumnFamily::Flow,
            &channel_message_key(
                harness.context.flow_hash,
                harness.run_id(),
                "updates",
                2_000_000,
            ),
        )
        .expect("publication exists");
    assert_eq!(message.message_id, "message-1");
    let permissions: AttributeRecord = harness
        .state
        .row(
            ColumnFamily::Flow,
            &run_named_key(
                harness.context.flow_hash,
                harness.run_id(),
                RunTable::Attribute,
                "DexWorkQueuePermissions",
            ),
        )
        .expect("permissions exist");
    let dexzd_protocol::dexzd::v1::stored_value::Kind::InlineValue(value) = permissions
        .value
        .expect("permissions have a value")
        .kind
        .expect("permissions have a stored kind")
    else {
        panic!("permissions are stored inline");
    };
    assert_eq!(
        value.kind,
        Some(value::Kind::ObjValue(dexzd_protocol::dex::EncodedObject {
            encoding: "json".to_owned(),
            payload: b"[\"read-order\"]".to_vec(),
        }))
    );
    let search_document = dexzd_apply::read_model::get_search_document(
        &harness.state,
        harness.context.flow_hash,
        harness.run_id(),
    )
    .expect("search document read succeeds")
    .expect("search document exists");
    assert_eq!(search_document.work_queue_permissions, ["read-order"]);
    assert_eq!(harness.run_record().sticky_app_worker_id, "");
    let history: HistoryRecord = harness
        .state
        .row(
            ColumnFamily::History,
            &history_record_key(harness.context.flow_hash, harness.run_id(), 2),
        )
        .expect("RPC history exists");
    assert!(history.is_external_input);
    let Some(flow_history_event::Payload::RpcExecutionCompleted(event)) =
        history.events[0].payload.as_ref()
    else {
        panic!("expected RPC history event");
    };
    assert!(event.input.is_none());
    assert!(event.output.is_none());
}

#[test]
fn transactional_rpc_checks_all_deletions_before_writing() {
    let mut harness = Harness::new(1000, 16 << 20);
    harness.rpc(
        request("publishUpdate"),
        InvokeWorkerRpcResponse {
            publish_to_channel: vec![ChannelMessage {
                channel_name: "updates".to_owned(),
                message_id: "present".to_owned(),
                value: Some(string_value("queued")),
            }],
            ..InvokeWorkerRpcResponse::default()
        },
        &[1],
    );
    let mut rpc_request = request("rpc-1");
    rpc_request.is_transactional = true;
    let response = InvokeWorkerRpcResponse {
        upsert_attributes: vec![AttributeWrite {
            key: "status".to_owned(),
            value: Some(string_value("ready")),
            ..AttributeWrite::default()
        }],
        delete_from_channel: vec![
            ChannelMessageDeletion {
                channel_name: "updates".to_owned(),
                message_id: "present".to_owned(),
            },
            ChannelMessageDeletion {
                channel_name: "updates".to_owned(),
                message_id: "missing".to_owned(),
            },
        ],
        ..InvokeWorkerRpcResponse::default()
    };
    let outcome = harness.rpc(rpc_request, response, &[1]);
    assert_error(&outcome.reply, 5, ErrorSubStatus::ChannelMessageNotFound);
    assert!(outcome.writes.is_empty());
    assert!(outcome.effects.is_empty());
    assert_eq!(harness.run_record().next_run_seq, 3);
    assert!(
        harness
            .state
            .row::<ChannelMessageRecord>(
                ColumnFamily::Flow,
                &channel_message_key(
                    harness.context.flow_hash,
                    harness.run_id(),
                    "updates",
                    2_000_000
                ),
            )
            .is_some()
    );
}

#[test]
fn transactional_rpc_rejects_duplicate_deletion_target_without_writes() {
    let mut harness = Harness::new(1000, 16 << 20);
    harness.rpc(
        request("publishUpdate"),
        InvokeWorkerRpcResponse {
            publish_to_channel: vec![ChannelMessage {
                channel_name: "updates".to_owned(),
                message_id: "m1".to_owned(),
                value: Some(string_value("queued")),
            }],
            ..InvokeWorkerRpcResponse::default()
        },
        &[1],
    );
    let mut rpc_request = request("rpc-1");
    rpc_request.is_transactional = true;
    let deletion = ChannelMessageDeletion {
        channel_name: "updates".to_owned(),
        message_id: "m1".to_owned(),
    };
    let outcome = harness.rpc(
        rpc_request,
        InvokeWorkerRpcResponse {
            upsert_attributes: vec![AttributeWrite {
                key: "status".to_owned(),
                value: Some(string_value("ready")),
                ..AttributeWrite::default()
            }],
            delete_from_channel: vec![deletion.clone(), deletion],
            ..InvokeWorkerRpcResponse::default()
        },
        &[2],
    );
    assert_error(&outcome.reply, 5, ErrorSubStatus::ChannelMessageNotFound);
    assert!(outcome.writes.is_empty());
    assert!(outcome.effects.is_empty());
    assert_eq!(harness.run_record().next_run_seq, 3);
    assert!(
        harness
            .state
            .row::<AttributeRecord>(
                ColumnFamily::Flow,
                &run_named_key(
                    harness.context.flow_hash,
                    harness.run_id(),
                    RunTable::Attribute,
                    "status"
                ),
            )
            .is_none()
    );
    assert!(
        harness
            .state
            .row::<ChannelMessageRecord>(
                ColumnFamily::Flow,
                &channel_message_key(
                    harness.context.flow_hash,
                    harness.run_id(),
                    "updates",
                    2_000_000
                ),
            )
            .is_some()
    );
}

#[test]
fn rpc_request_ids_replay_the_reply_and_reject_changed_fingerprints() {
    let mut harness = Harness::new(1000, 16 << 20);
    let first = harness.rpc(
        request("rpc-1"),
        InvokeWorkerRpcResponse {
            output: Some(string_value("done")),
            ..InvokeWorkerRpcResponse::default()
        },
        &[1],
    );
    let same = harness.rpc(request("rpc-1"), InvokeWorkerRpcResponse::default(), &[1]);
    assert!(same.writes.is_empty());
    assert_eq!(same.reply, first.reply);
    let different = harness.rpc(request("rpc-1"), InvokeWorkerRpcResponse::default(), &[2]);
    assert_error(&different.reply, 3, ErrorSubStatus::Uncategorized);
    assert!(different.writes.is_empty());
}

#[test]
fn rpc_step_decision_cancels_existing_steps_and_sources_new_steps() {
    let mut harness = Harness::new(1000, 16 << 20);
    let outcome = harness.rpc(
        request("rpc-1"),
        InvokeWorkerRpcResponse {
            step_decision: Some(StepDecision {
                cancel_step_types: vec!["StartStep".to_owned()],
                next_steps: vec![StepMovement {
                    step_type: "FollowupStep".to_owned(),
                    step_input: Some(string_value("next")),
                    ..StepMovement::default()
                }],
                ..StepDecision::default()
            }),
            ..InvokeWorkerRpcResponse::default()
        },
        &[1],
    );
    assert!(outcome.effects.iter().any(|effect| matches!(
        effect,
        ApplyEffect::InvocationPending { invocation, .. }
            if invocation.step_execution_id == "FollowupStep-1"
    )));
    let first: StepExecutionRecord = harness
        .state
        .row(
            ColumnFamily::Flow,
            &run_named_key(
                harness.context.flow_hash,
                harness.run_id(),
                RunTable::StepExecution,
                "StartStep-1",
            ),
        )
        .expect("first Step exists");
    assert_eq!(
        first.phase,
        dexzd_protocol::dexzd::v1::StepExecutionPhase::Canceled as i32
    );
    let next: StepExecutionRecord = harness
        .state
        .row(
            ColumnFamily::Flow,
            &run_named_key(
                harness.context.flow_hash,
                harness.run_id(),
                RunTable::StepExecution,
                "FollowupStep-1",
            ),
        )
        .expect("next Step exists");
    assert_eq!(next.from_step_execution_id, "__rpc/updateOrder");
}

#[test]
fn locked_rpc_records_input_output_and_sticky_worker() {
    let mut harness = Harness::new(1000, 16 << 20);
    let mut rpc_request = request("rpc-1");
    rpc_request.input = Some(string_value("input"));
    rpc_request.lock_attribute_keys = vec!["status".to_owned()];
    let outcome = harness.rpc(
        rpc_request,
        InvokeWorkerRpcResponse {
            output: Some(string_value("output")),
            ..InvokeWorkerRpcResponse::default()
        },
        &[1],
    );
    assert!(matches!(
        outcome.reply.reply,
        Some(command_reply::Reply::Rpc(_))
    ));
    assert_eq!(harness.run_record().sticky_app_worker_id, "worker-rpc");
    let history: HistoryRecord = harness
        .state
        .row(
            ColumnFamily::History,
            &history_record_key(harness.context.flow_hash, harness.run_id(), 2),
        )
        .expect("RPC history exists");
    let Some(flow_history_event::Payload::RpcExecutionCompleted(event)) =
        history.events[0].payload.as_ref()
    else {
        panic!("expected RPC history event");
    };
    assert_eq!(event.input, Some(string_value("input")));
    assert_eq!(event.output, Some(string_value("output")));
}

#[test]
fn owner_resolved_transaction_mode_overrides_request_flags() {
    let mut harness = Harness::new(1000, 16 << 20);
    let mut rpc_request = request("rpc-1");
    rpc_request.input = Some(string_value("input"));
    let command = harness.command(Command::RpcCommitted(RpcCommittedCommand {
        request: Some(rpc_request),
        response: Some(InvokeWorkerRpcResponse {
            output: Some(string_value("output")),
            ..InvokeWorkerRpcResponse::default()
        }),
        app_worker_id: "worker-rpc".to_owned(),
        request_fingerprint: vec![1],
        is_transactional: true,
        ..RpcCommittedCommand::default()
    }));
    let outcome = harness.apply(command);
    assert!(matches!(
        outcome.reply.reply,
        Some(command_reply::Reply::Rpc(_))
    ));
    assert_eq!(harness.run_record().sticky_app_worker_id, "worker-rpc");
    let history: HistoryRecord = harness
        .state
        .row(
            ColumnFamily::History,
            &history_record_key(harness.context.flow_hash, harness.run_id(), 2),
        )
        .expect("RPC history exists");
    let Some(flow_history_event::Payload::RpcExecutionCompleted(event)) =
        history.events[0].payload.as_ref()
    else {
        panic!("expected RPC history event");
    };
    assert_eq!(event.input, Some(string_value("input")));
    assert_eq!(event.output, Some(string_value("output")));
}

#[test]
fn rpc_attempt_mismatch_and_closed_flow_reject_without_writes() {
    let mut harness = Harness::new(1000, 16 << 20);
    let mut stale = harness.command(Command::RpcCommitted(RpcCommittedCommand {
        request: Some(request("rpc-1")),
        response: Some(InvokeWorkerRpcResponse::default()),
        ..RpcCommittedCommand::default()
    }));
    stale.expected_run_attempt = 2;
    if let Some(Command::RpcCommitted(rpc)) = stale.command.as_mut()
        && let Some(request) = rpc.request.as_mut()
    {
        request.is_transactional = true;
    }
    let mismatched = harness.apply(stale);
    assert_error(&mismatched.reply, 9, ErrorSubStatus::Uncategorized);
    assert!(mismatched.writes.is_empty());
    let stop = harness.command(Command::FlowStop(
        dexzd_protocol::dexzd::v1::FlowStopCommand {
            request: Some(StopFlowRequest::default()),
        },
    ));
    harness.apply(stop);
    assert_eq!(harness.run_record().status, FlowStatus::Canceled as i32);
    let closed = harness.rpc(request("rpc-2"), InvokeWorkerRpcResponse::default(), &[2]);
    assert_error(&closed.reply, 5, ErrorSubStatus::FlowNotExists);
    assert!(closed.writes.is_empty());
}

#[test]
fn invalid_rpc_output_discards_earlier_effects() {
    let mut harness = Harness::new(1000, 64);
    let response = InvokeWorkerRpcResponse {
        output: Some(string_value(&"x".repeat(100))),
        upsert_attributes: vec![AttributeWrite {
            key: "status".to_owned(),
            value: Some(string_value("ready")),
            ..AttributeWrite::default()
        }],
        ..InvokeWorkerRpcResponse::default()
    };
    let outcome = harness.rpc(request("rpc-1"), response, &[1]);
    assert_error(&outcome.reply, 3, ErrorSubStatus::Uncategorized);
    assert!(outcome.writes.is_empty());
    assert!(outcome.effects.is_empty());
    assert_eq!(harness.run_record().next_run_seq, 2);
    assert!(
        harness
            .state
            .row::<AttributeRecord>(
                ColumnFamily::Flow,
                &run_named_key(
                    harness.context.flow_hash,
                    harness.run_id(),
                    RunTable::Attribute,
                    "status"
                ),
            )
            .is_none()
    );
}

#[test]
fn invalid_recorded_event_value_discards_the_rpc() {
    let mut harness = Harness::new(1000, 64);
    let outcome = harness.rpc(
        request("rpc-1"),
        InvokeWorkerRpcResponse {
            record_events: vec![Kv {
                key: "note".to_owned(),
                value: Some(string_value(&"x".repeat(100))),
            }],
            ..InvokeWorkerRpcResponse::default()
        },
        &[1],
    );
    assert_error(&outcome.reply, 3, ErrorSubStatus::Uncategorized);
    assert!(outcome.writes.is_empty());
}

#[test]
fn large_record_event_and_permission_comparison_create_no_value_rows() {
    let mut harness = Harness::new(1000, 16 << 20);
    let large_value = string_value(&"x".repeat(500));
    let outcome = harness.rpc(
        request("rpc-1"),
        InvokeWorkerRpcResponse {
            record_events: vec![Kv {
                key: "note".to_owned(),
                value: Some(large_value.clone()),
            }],
            action_permission_mappings: Some(ActionPermissionMappings {
                mappings: vec![ActionPermissionMapping {
                    attribute_key: "status".to_owned(),
                    equal_values: vec![large_value],
                    required_permission: "read-order".to_owned(),
                }],
            }),
            ..InvokeWorkerRpcResponse::default()
        },
        &[1],
    );
    assert!(matches!(
        outcome.reply.reply,
        Some(command_reply::Reply::Rpc(_))
    ));
    assert!(
        !harness
            .state
            .rows
            .keys()
            .any(|(family, _)| *family == ColumnFamily::Value)
    );
    for table in [RunTable::ValueBirth, RunTable::ValueGarbage] {
        let prefix = run_table_prefix(harness.context.flow_hash, harness.run_id(), table);
        assert!(
            !harness
                .state
                .rows
                .keys()
                .any(|(family, key)| { *family == ColumnFamily::Flow && key.starts_with(&prefix) })
        );
    }
}

#[test]
fn zero_dedup_capacity_keeps_rpc_output_live() {
    let mut harness = Harness::new(0, 16 << 20);
    let first = harness.rpc(
        request("rpc-1"),
        InvokeWorkerRpcResponse {
            output: Some(string_value(&"x".repeat(500))),
            ..InvokeWorkerRpcResponse::default()
        },
        &[1],
    );
    let Some(command_reply::Reply::Rpc(reply)) = &first.reply.reply else {
        panic!("expected RPC reply");
    };
    let Some(dexzd_protocol::dexzd::v1::stored_value::Kind::TableValue(reference)) = reply
        .output
        .as_ref()
        .and_then(|output| output.kind.as_ref())
    else {
        panic!("expected table value");
    };
    let value_id = reference.value_id.expect("value ID");
    let value_id = ValueId {
        run_seq: value_id.run_seq,
        index_in_command: value_id.index_in_command,
    };
    assert!(
        harness
            .state
            .row::<dexzd_protocol::dexzd::v1::RequestDedupRecord>(
                ColumnFamily::Flow,
                &request_dedup_key(
                    harness.context.flow_hash,
                    harness.run_id(),
                    dexzd_keyspace::row_keys::RequestKind::InvokeRpc,
                    "rpc-1",
                ),
            )
            .is_some()
    );
    let birth: ValueBirthRecord = harness
        .state
        .row(
            ColumnFamily::Flow,
            &value_row_key(
                harness.context.flow_hash,
                harness.run_id(),
                RunTable::ValueBirth,
                value_id,
            ),
        )
        .expect("value birth exists");
    assert_eq!(birth.garbage_seq, 0);
    let holder_prefix = dexzd_keyspace::row_keys::value_holder_key(
        harness.context.flow_hash,
        harness.run_id(),
        value_id,
        &[],
    );
    assert!(
        harness.state.rows.keys().any(|(family, key)| {
            *family == ColumnFamily::Flow && key.starts_with(&holder_prefix)
        })
    );
}

#[test]
fn rpc_rejects_unsupported_step_failure_policy() {
    let mut harness = Harness::new(1000, 16 << 20);
    let outcome = harness.rpc(
        request("rpc-1"),
        InvokeWorkerRpcResponse {
            step_decision: Some(StepDecision {
                next_steps: vec![StepMovement {
                    step_type: "FollowupStep".to_owned(),
                    step_options: Some(StepOptions {
                        wait_for_failure_policy: WaitForMethodFailurePolicy::ProceedOnFailure
                            as i32,
                        ..StepOptions::default()
                    }),
                    ..StepMovement::default()
                }],
                ..StepDecision::default()
            }),
            ..InvokeWorkerRpcResponse::default()
        },
        &[1],
    );
    assert_error(&outcome.reply, 3, ErrorSubStatus::Uncategorized);
    assert!(outcome.writes.is_empty());
}

#[test]
fn nontransactional_rpc_ignores_missing_deletions() {
    let mut harness = Harness::new(1000, 16 << 20);
    let outcome = harness.rpc(
        request("rpc-1"),
        InvokeWorkerRpcResponse {
            delete_from_channel: vec![ChannelMessageDeletion {
                channel_name: "updates".to_owned(),
                message_id: "missing".to_owned(),
            }],
            ..InvokeWorkerRpcResponse::default()
        },
        &[1],
    );
    assert!(matches!(
        outcome.reply.reply,
        Some(command_reply::Reply::Rpc(_))
    ));
    assert!(!outcome.writes.is_empty());
}

#[test]
fn another_request_kind_evicts_and_releases_the_old_rpc_output_holder() {
    let mut harness = Harness::new(1, 16 << 20);
    let first = harness.rpc(
        request("rpc-1"),
        InvokeWorkerRpcResponse {
            output: Some(string_value(&"x".repeat(500))),
            ..InvokeWorkerRpcResponse::default()
        },
        &[1],
    );
    let Some(command_reply::Reply::Rpc(first_reply)) = &first.reply.reply else {
        panic!("expected RPC reply");
    };
    let stored = first_reply.output.as_ref().expect("stored output");
    let Some(dexzd_protocol::dexzd::v1::stored_value::Kind::TableValue(reference)) = &stored.kind
    else {
        panic!("expected table value");
    };
    let value_id = reference.value_id.expect("value ID");
    let set = harness.command(Command::AttributesSet(AttributesSetCommand {
        request: Some(SetAttributesRequest {
            request_id: "set-1".to_owned(),
            ..SetAttributesRequest::default()
        }),
        request_fingerprint: vec![2],
        ..AttributesSetCommand::default()
    }));
    harness.apply(set);
    assert!(
        harness
            .state
            .row::<dexzd_protocol::dexzd::v1::RequestDedupRecord>(
                ColumnFamily::Flow,
                &request_dedup_key(
                    harness.context.flow_hash,
                    harness.run_id(),
                    dexzd_keyspace::row_keys::RequestKind::InvokeRpc,
                    "rpc-1"
                ),
            )
            .is_none()
    );
    let holder_prefix = dexzd_keyspace::row_keys::value_holder_key(
        harness.context.flow_hash,
        harness.run_id(),
        dexzd_keyspace::ValueId {
            run_seq: value_id.run_seq,
            index_in_command: value_id.index_in_command,
        },
        &[],
    );
    assert!(
        !harness
            .state
            .rows
            .keys()
            .any(|(family, key)| *family == ColumnFamily::Flow && key.starts_with(&holder_prefix))
    );
}
