// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Command-driven checks of the search projection and public read model.

use std::collections::BTreeMap;

use dexzd_apply::read_model::{
    ReadModelError, get_attributes, get_channel_messages, get_flow_state, get_flow_summary,
    get_history_events, get_search_document, is_flow_closed, is_history_event_available,
    is_step_execution_finished, list_search_documents, load_blobs, match_attribute, resolve_run_id,
};
use dexzd_apply::{
    ApplyContext, ApplyOutcome, FlowStateReader, StateReadError, StateRow, StateWrite,
    apply_flow_command,
};
use dexzd_keyspace::row_keys::{flow_list_index_key, tenant_run_index_key};
use dexzd_keyspace::{ColumnFamily, IndexKind, PartitionHasher, RunId, TenantId};
use dexzd_protocol::dex::{
    ActionPermissionMapping, ActionPermissionMappings, AttributeMatch, AttributeMatchOperator,
    AttributeWrite, ChannelMessage, CloseDecision, CloseDecisionType, FlowConfig, FlowStartOptions,
    FlowStatus, IndexConfig, IndexType, InvokeExecuteMethodResponse, InvokeWaitForMethodResponse,
    LoadBlobRequestEntry, PublishToChannelRequest, SetAttributesRequest, StartFlowRequest,
    StepDecision, StepOptions, StopFlowRequest, StopType, UpdateFlowConfigRequest, Value,
    WorkerTarget, flow_history_event, value,
};
use dexzd_protocol::dexzd::v1::{
    AttributesSetCommand, ChannelPublishCommand, FlowCommand, FlowConfigUpdateCommand,
    FlowStartCommand, FlowStopCommand, HardDeleteObjectReference, RunSettings,
    StepAttemptStartedCommand, StepInvocationKey, StepMethod, StepMethodCompletedCommand,
    flow_command::Command, step_method_completed_command,
};

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
                    let removed: Vec<_> = self
                        .rows
                        .range((*column_family, start.clone())..(*column_family, end.clone()))
                        .map(|(key, _)| key.clone())
                        .collect();
                    for key in removed {
                        self.rows.remove(&key);
                    }
                }
            }
        }
    }
}

struct Harness {
    state: MemoryState,
    context: ApplyContext,
    run_id: RunId,
    now_micros: i64,
    behavior_version: u32,
}

impl Harness {
    fn new() -> Self {
        let hasher = PartitionHasher::new(&[7; 16], 1024).expect("valid partition hasher");
        let flow_hash = hasher.flow_hash(TenantId::DEFAULT, "order-1");
        Self {
            state: MemoryState::default(),
            context: ApplyContext {
                partition: hasher.partition_of(flow_hash),
                flow_hash,
            },
            run_id: RunId([1; 16]),
            now_micros: 1_800_000_000_000_000,
            behavior_version: 2,
        }
    }

    fn apply(&mut self, body: Command) -> ApplyOutcome {
        self.now_micros += 1000;
        let is_start = matches!(body, Command::FlowStart(_));
        let command = FlowCommand {
            flow_id: "order-1".to_owned(),
            run_id: if is_start {
                Vec::new()
            } else {
                self.run_id.as_bytes().to_vec()
            },
            behavior_version: self.behavior_version,
            proposed_at_micros: self.now_micros,
            tenant_id: 0,
            command: Some(body),
            ..FlowCommand::default()
        };
        let outcome =
            apply_flow_command(&self.context, &self.state, &command).expect("the command applies");
        let replay = apply_flow_command(&self.context, &self.state, &command)
            .expect("the same command replays");
        assert_eq!(outcome.writes, replay.writes);
        self.state.commit(&outcome);
        outcome
    }

    fn start(&mut self, options: FlowStartOptions, step_options: Option<StepOptions>) {
        let request = StartFlowRequest {
            flow_id: "order-1".to_owned(),
            flow_type: "OrderFlow".to_owned(),
            start_step_type: "ChargeStep".to_owned(),
            step_input: Some(string_value("input")),
            step_options,
            request_id: "start-1".to_owned(),
            flow_start_options: Some(options),
            ..StartFlowRequest::default()
        };
        let outcome = self.apply(Command::FlowStart(FlowStartCommand {
            new_run_id: self.run_id.as_bytes().to_vec(),
            request: Some(request),
            settings: Some(RunSettings {
                wire_inline_threshold_bytes: 100,
                max_value_bytes: 16 << 20,
                retention_after_close_micros: 7 * 24 * 3600 * 1_000_000,
                request_dedup_capacity: 1000,
                ..RunSettings::default()
            }),
            worker_target: Some(WorkerTarget {
                is_pull_mode: true,
                ..WorkerTarget::default()
            }),
            ..FlowStartCommand::default()
        }));
        assert!(outcome.reply.reply.is_some());
    }

    fn set_attributes(
        &mut self,
        attributes: Vec<AttributeWrite>,
        mappings: Option<ActionPermissionMappings>,
        hard_delete_objects: Vec<HardDeleteObjectReference>,
    ) {
        self.apply(Command::AttributesSet(AttributesSetCommand {
            request: Some(SetAttributesRequest {
                attributes,
                action_permission_mappings: mappings,
                ..SetAttributesRequest::default()
            }),
            hard_delete_objects,
            ..AttributesSetCommand::default()
        }));
    }

    fn start_attempt(&mut self, method: StepMethod) {
        self.apply(Command::StepAttemptStarted(StepAttemptStartedCommand {
            invocation: Some(StepInvocationKey {
                step_execution_id: "ChargeStep-1".to_owned(),
                method: method as i32,
            }),
            attempt: 1,
            task_id: vec![2; 16],
        }));
    }

    fn complete_wait_for(&mut self) {
        self.apply(Command::StepMethodCompleted(StepMethodCompletedCommand {
            invocation: Some(StepInvocationKey {
                step_execution_id: "ChargeStep-1".to_owned(),
                method: StepMethod::WaitFor as i32,
            }),
            attempt: 1,
            response: Some(step_method_completed_command::Response::WaitFor(
                InvokeWaitForMethodResponse::default(),
            )),
            ..StepMethodCompletedCommand::default()
        }));
    }

    fn complete_execute(&mut self, decision: StepDecision) {
        self.apply(Command::StepMethodCompleted(StepMethodCompletedCommand {
            invocation: Some(StepInvocationKey {
                step_execution_id: "ChargeStep-1".to_owned(),
                method: StepMethod::Execute as i32,
            }),
            attempt: 1,
            response: Some(step_method_completed_command::Response::Execute(
                InvokeExecuteMethodResponse {
                    step_decision: Some(decision),
                    ..InvokeExecuteMethodResponse::default()
                },
            )),
            ..StepMethodCompletedCommand::default()
        }));
    }

    fn stop(&mut self) {
        self.apply(Command::FlowStop(FlowStopCommand {
            request: Some(StopFlowRequest {
                stop_type: StopType::Cancel as i32,
                ..StopFlowRequest::default()
            }),
        }));
    }
}

fn string_value(text: &str) -> Value {
    Value {
        kind: Some(value::Kind::StringValue(text.to_owned())),
    }
}

fn index_config(index_key: &str) -> IndexConfig {
    IndexConfig {
        enable: true,
        index_key: index_key.to_owned(),
        ..IndexConfig::default()
    }
}

#[test]
fn search_document_tracks_attributes_permissions_active_steps_and_status() {
    let mut harness = Harness::new();
    harness.start(
        FlowStartOptions {
            attributes: vec![AttributeWrite {
                key: "state".to_owned(),
                value: Some(string_value("new")),
                index_config: Some(index_config("ReviewState")),
                ..AttributeWrite::default()
            }],
            ..FlowStartOptions::default()
        },
        None,
    );
    let mut document =
        get_search_document(&harness.state, harness.context.flow_hash, harness.run_id)
            .unwrap()
            .unwrap();
    assert_eq!(document.version, 1);
    assert_eq!(document.active_step_types, ["ChargeStep"]);
    assert_eq!(document.attributes[0].index_key, "ReviewState");
    assert_eq!(document.attributes[0].index_type, IndexType::Keyword as i32);
    let search_key = tenant_run_index_key(
        harness.context.partition,
        IndexKind::SearchDocument,
        TenantId::DEFAULT,
        harness.context.flow_hash,
        harness.run_id,
    );
    assert_eq!(
        harness
            .state
            .rows
            .get(&(ColumnFamily::PartitionIndex, search_key)),
        Some(&1_u64.to_be_bytes().to_vec())
    );
    harness.set_attributes(
        vec![
            AttributeWrite {
                key: "state".to_owned(),
                value: Some(string_value("ready")),
                index_config: Some(index_config("ReviewState")),
                ..AttributeWrite::default()
            },
            AttributeWrite {
                key: "oversized".to_owned(),
                value: Some(string_value(&"x".repeat(256 * 1024 + 1))),
                index_config: Some(index_config("Oversized")),
                ..AttributeWrite::default()
            },
            AttributeWrite {
                key: "largest_allowed".to_owned(),
                value: Some(string_value(&"x".repeat(256 * 1024))),
                index_config: Some(index_config("LargestAllowed")),
                ..AttributeWrite::default()
            },
            AttributeWrite {
                key: "count".to_owned(),
                value: Some(Value {
                    kind: Some(value::Kind::IntValue(7)),
                }),
                index_config: Some(index_config("Count")),
                ..AttributeWrite::default()
            },
        ],
        Some(ActionPermissionMappings {
            mappings: vec![ActionPermissionMapping {
                attribute_key: "state".to_owned(),
                equal_values: vec![string_value("ready")],
                required_permission: "approve".to_owned(),
            }],
        }),
        Vec::new(),
    );
    document = get_search_document(&harness.state, harness.context.flow_hash, harness.run_id)
        .unwrap()
        .unwrap();
    assert_eq!(document.version, 2);
    assert_eq!(document.attributes.len(), 3);
    assert_eq!(document.attributes[0].index_key, "Count");
    assert_eq!(document.attributes[0].index_type, IndexType::Int as i32);
    assert_eq!(document.attributes[1].index_key, "LargestAllowed");
    assert_eq!(document.attributes[2].index_key, "ReviewState");
    assert_eq!(document.work_queue_permissions, ["approve"]);
    let all = get_attributes(
        &harness.state,
        harness.context.flow_hash,
        harness.run_id,
        &[],
        true,
    )
    .unwrap();
    assert_eq!(all.response.attributes[0].key, "DexWorkQueuePermissions");
    assert_eq!(
        all.response.attributes[0].value.as_ref().unwrap().kind,
        Some(value::Kind::ObjValue(dexzd_protocol::dex::EncodedObject {
            encoding: "json".to_owned(),
            payload: b"[\"approve\"]".to_vec(),
        }))
    );
    harness.stop();
    document = get_search_document(&harness.state, harness.context.flow_hash, harness.run_id)
        .unwrap()
        .unwrap();
    assert_eq!(document.version, 3);
    assert_eq!(document.status, FlowStatus::Canceled as i32);
    assert!(document.active_step_types.is_empty());
    assert!(document.closed_at_micros > 0);
    let old_list_key = flow_list_index_key(
        harness.context.partition,
        TenantId::DEFAULT,
        "OrderFlow",
        FlowStatus::Running as u8,
        harness.context.flow_hash,
        harness.run_id,
    );
    assert!(
        !harness
            .state
            .rows
            .contains_key(&(ColumnFamily::PartitionIndex, old_list_key))
    );
    assert_eq!(
        list_search_documents(
            &harness.state,
            harness.context.partition,
            TenantId::DEFAULT,
            Some("OrderFlow"),
            10,
        )
        .unwrap(),
        vec![document]
    );
}

#[test]
fn search_document_applies_index_writes_in_order_and_validates_typed_values() {
    let mut harness = Harness::new();
    harness.start(
        FlowStartOptions {
            attributes: vec![AttributeWrite {
                key: "first".to_owned(),
                value: Some(string_value("original")),
                index_config: Some(index_config("Shared")),
                ..AttributeWrite::default()
            }],
            ..FlowStartOptions::default()
        },
        None,
    );
    let array_config = IndexConfig {
        r#type: IndexType::KeywordArray as i32,
        ..index_config("Tags")
    };
    let datetime_config = IndexConfig {
        r#type: IndexType::Datetime as i32,
        ..index_config("DueAt")
    };
    harness.set_attributes(
        vec![
            AttributeWrite {
                key: "second".to_owned(),
                value: Some(string_value("before")),
                index_config: Some(index_config("Shared")),
                ..AttributeWrite::default()
            },
            AttributeWrite {
                key: "third".to_owned(),
                value: Some(string_value("latest")),
                index_config: Some(index_config("Shared")),
                ..AttributeWrite::default()
            },
            AttributeWrite {
                key: "tags".to_owned(),
                value: Some(string_value("[\"red\", \"blue\"]")),
                index_config: Some(array_config.clone()),
                ..AttributeWrite::default()
            },
            AttributeWrite {
                key: "due".to_owned(),
                value: Some(string_value("2026-10-23T12:34:56Z")),
                index_config: Some(datetime_config.clone()),
                ..AttributeWrite::default()
            },
        ],
        None,
        Vec::new(),
    );
    let mut document =
        get_search_document(&harness.state, harness.context.flow_hash, harness.run_id)
            .unwrap()
            .unwrap();
    assert_eq!(document.attributes.len(), 3);
    assert_eq!(document.attributes[0].index_key, "DueAt");
    assert_eq!(document.attributes[1].index_key, "Shared");
    assert_eq!(document.attributes[1].value, Some(string_value("latest")));
    assert_eq!(document.attributes[2].index_key, "Tags");
    assert_eq!(
        document.attributes[2].value,
        Some(Value {
            kind: Some(value::Kind::ObjValue(dexzd_protocol::dex::EncodedObject {
                encoding: "json".to_owned(),
                payload: b"[\"red\",\"blue\"]".to_vec(),
            })),
        })
    );
    harness.set_attributes(
        vec![
            AttributeWrite {
                key: "third".to_owned(),
                value: Some(string_value("unindexed")),
                ..AttributeWrite::default()
            },
            AttributeWrite {
                key: "tags".to_owned(),
                value: Some(string_value("not an array")),
                index_config: Some(array_config),
                ..AttributeWrite::default()
            },
            AttributeWrite {
                key: "due".to_owned(),
                value: Some(string_value("not a date")),
                index_config: Some(datetime_config),
                ..AttributeWrite::default()
            },
        ],
        None,
        Vec::new(),
    );
    document = get_search_document(&harness.state, harness.context.flow_hash, harness.run_id)
        .unwrap()
        .unwrap();
    assert_eq!(document.attributes[1].value, Some(string_value("latest")));
    assert_eq!(
        document.attributes[2].index_type,
        IndexType::KeywordArray as i32
    );
    harness.set_attributes(
        vec![AttributeWrite {
            key: "third".to_owned(),
            value: None,
            index_config: Some(index_config("Shared")),
            ..AttributeWrite::default()
        }],
        None,
        Vec::new(),
    );
    document = get_search_document(&harness.state, harness.context.flow_hash, harness.run_id)
        .unwrap()
        .unwrap();
    assert_eq!(document.attributes.len(), 2);
    assert!(
        document
            .attributes
            .iter()
            .all(|value| value.index_key != "Shared")
    );
}

#[test]
fn active_step_search_mode_respects_skip_wait_for() {
    let mut harness = Harness::new();
    harness.start(
        FlowStartOptions::default(),
        Some(StepOptions {
            skip_wait_for: true,
            ..StepOptions::default()
        }),
    );
    let document = get_search_document(&harness.state, harness.context.flow_hash, harness.run_id)
        .unwrap()
        .unwrap();
    assert!(document.active_step_types.is_empty());
    harness.apply(Command::FlowConfigUpdate(FlowConfigUpdateCommand {
        request: Some(UpdateFlowConfigRequest {
            flow_config: Some(FlowConfig {
                active_step_search_mode: Some(
                    dexzd_protocol::dex::ActiveStepSearchMode::EnabledForAll as i32,
                ),
                ..FlowConfig::default()
            }),
            ..UpdateFlowConfigRequest::default()
        }),
    }));
    let document = get_search_document(&harness.state, harness.context.flow_hash, harness.run_id)
        .unwrap()
        .unwrap();
    assert_eq!(document.active_step_types, ["ChargeStep"]);
}

#[test]
fn earlier_behavior_version_keeps_its_history_and_search_writes() {
    let mut harness = Harness::new();
    harness.behavior_version = 1;
    harness.start(FlowStartOptions::default(), None);
    assert_eq!(
        get_search_document(&harness.state, harness.context.flow_hash, harness.run_id).unwrap(),
        None
    );
    harness.stop();
    let history = get_history_events(
        &harness.state,
        harness.context.flow_hash,
        harness.run_id,
        1,
        0,
        &[],
        0,
    )
    .unwrap();
    assert_eq!(history.events.len(), 2);
    assert!(matches!(
        history.events[1].payload,
        Some(flow_history_event::Payload::FlowClosed(_))
    ));
    assert_eq!(
        get_search_document(&harness.state, harness.context.flow_hash, harness.run_id).unwrap(),
        None
    );
}

#[test]
fn search_projection_seeds_indexed_attributes_when_a_run_adopts_new_behavior() {
    let mut harness = Harness::new();
    harness.behavior_version = 1;
    harness.start(
        FlowStartOptions {
            attributes: vec![AttributeWrite {
                key: "state".to_owned(),
                value: Some(string_value("created")),
                index_config: Some(index_config("ReviewState")),
                ..AttributeWrite::default()
            }],
            ..FlowStartOptions::default()
        },
        None,
    );
    harness.behavior_version = 2;
    harness.set_attributes(
        vec![AttributeWrite {
            key: "other".to_owned(),
            value: Some(string_value("new")),
            ..AttributeWrite::default()
        }],
        None,
        Vec::new(),
    );
    let document = get_search_document(&harness.state, harness.context.flow_hash, harness.run_id)
        .unwrap()
        .unwrap();
    assert_eq!(document.attributes.len(), 1);
    assert_eq!(document.attributes[0].index_key, "ReviewState");
    assert_eq!(document.attributes[0].value, Some(string_value("created")));
}

#[test]
fn read_model_returns_sorted_values_channels_and_wait_predicates() {
    let mut harness = Harness::new();
    harness.start(
        FlowStartOptions {
            attributes: vec![
                AttributeWrite {
                    key: "z".to_owned(),
                    value: Some(Value {
                        kind: Some(value::Kind::IntValue(7)),
                    }),
                    ..AttributeWrite::default()
                },
                AttributeWrite {
                    key: "a".to_owned(),
                    value: Some(string_value("first")),
                    ..AttributeWrite::default()
                },
            ],
            ..FlowStartOptions::default()
        },
        None,
    );
    let run_id = resolve_run_id(
        &harness.state,
        harness.context.flow_hash,
        TenantId::DEFAULT,
        "order-1",
        "",
    )
    .unwrap();
    assert_eq!(run_id, Some(harness.run_id));
    let summary =
        get_flow_summary(&harness.state, harness.context.flow_hash, harness.run_id).unwrap();
    assert_eq!(summary.flow_type, "OrderFlow");
    assert_eq!(summary.request_id, "start-1");
    assert_eq!(summary.run_attempt, 1);
    assert!(summary.close_time.is_none());
    assert!(!is_flow_closed(&harness.state, harness.context.flow_hash, harness.run_id).unwrap());
    assert!(
        !is_step_execution_finished(
            &harness.state,
            harness.context.flow_hash,
            harness.run_id,
            "ChargeStep",
            1,
        )
        .unwrap()
    );

    harness.set_attributes(
        vec![AttributeWrite {
            key: "report".to_owned(),
            value: Some(string_value(&"x".repeat(500))),
            ..AttributeWrite::default()
        }],
        None,
        Vec::new(),
    );
    let attributes = get_attributes(
        &harness.state,
        harness.context.flow_hash,
        harness.run_id,
        &[
            "z".to_owned(),
            "report".to_owned(),
            "a".to_owned(),
            "z".to_owned(),
        ],
        false,
    )
    .unwrap();
    assert_eq!(
        attributes
            .response
            .attributes
            .iter()
            .map(|attribute| attribute.key.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "report", "z"]
    );
    let report_reference = attributes.response.attributes[1].value.clone().unwrap();
    assert!(matches!(
        report_reference.kind,
        Some(value::Kind::InternalBlobIdForStringValue(_))
    ));
    let loaded = load_blobs(
        &harness.state,
        harness.context.flow_hash,
        &[LoadBlobRequestEntry {
            flow_id: "order-1".to_owned(),
            blob_value: Some(report_reference.clone()),
        }],
    )
    .unwrap();
    let reference_text = match report_reference.kind.as_ref().unwrap() {
        value::Kind::InternalBlobIdForStringValue(text) => text,
        other => panic!("unexpected reference {other:?}"),
    };
    assert_eq!(
        loaded.values[reference_text],
        string_value(&"x".repeat(500))
    );
    assert!(matches!(
        match_attribute(
            &harness.state,
            harness.context.flow_hash,
            harness.run_id,
            &AttributeMatch {
                key: "report".to_owned(),
                operator: AttributeMatchOperator::Equal as i32,
                operand: Some(string_value("anything")),
            },
        ),
        Err(ReadModelError::UnsupportedAttributeValue { .. })
    ));
    let matched = match_attribute(
        &harness.state,
        harness.context.flow_hash,
        harness.run_id,
        &AttributeMatch {
            key: "z".to_owned(),
            operator: AttributeMatchOperator::GreaterThanOrEqual as i32,
            operand: Some(Value {
                kind: Some(value::Kind::IntValue(7)),
            }),
        },
    )
    .unwrap();
    assert_eq!(matched.unwrap().kind, Some(value::Kind::IntValue(7)));
    assert!(
        match_attribute(
            &harness.state,
            harness.context.flow_hash,
            harness.run_id,
            &AttributeMatch {
                key: "z".to_owned(),
                operator: AttributeMatchOperator::NotEqual as i32,
                operand: Some(string_value("7")),
            },
        )
        .unwrap()
        .is_none()
    );

    harness.set_attributes(
        vec![AttributeWrite {
            key: "secret".to_owned(),
            value: Some(string_value("private")),
            is_hard_delete_on_null: true,
            ..AttributeWrite::default()
        }],
        None,
        vec![HardDeleteObjectReference {
            write_index: 0,
            object_id: "object-1".to_owned(),
            size_bytes: 7,
            is_string_value: true,
        }],
    );
    let secret = get_attributes(
        &harness.state,
        harness.context.flow_hash,
        harness.run_id,
        &["secret".to_owned()],
        false,
    )
    .unwrap();
    assert_eq!(secret.hard_delete_attributes.len(), 1);
    assert_eq!(
        secret.hard_delete_attributes[0].object.object_id,
        "object-1"
    );
    assert!(secret.response.attributes[0].value.is_none());

    harness.apply(Command::ChannelPublish(ChannelPublishCommand {
        request: Some(PublishToChannelRequest {
            messages: vec![
                ChannelMessage {
                    channel_name: "updates".to_owned(),
                    message_id: "message-1".to_owned(),
                    value: Some(string_value("one")),
                },
                ChannelMessage {
                    channel_name: "updates".to_owned(),
                    message_id: "message-2".to_owned(),
                    value: Some(string_value("two")),
                },
            ],
            ..PublishToChannelRequest::default()
        }),
    }));
    let channel = get_channel_messages(
        &harness.state,
        harness.context.flow_hash,
        harness.run_id,
        "updates",
    )
    .unwrap();
    assert_eq!(
        channel
            .messages
            .iter()
            .map(|message| message.message_id.as_str())
            .collect::<Vec<_>>(),
        vec!["message-1", "message-2"]
    );
    let state = get_flow_state(&harness.state, harness.context.flow_hash, harness.run_id).unwrap();
    assert_eq!(state.attributes[0].key, "a");
    assert_eq!(state.active_step_executions.len(), 1);
    assert_eq!(state.pending_channel_messages["updates"].messages.len(), 2);

    harness.start_attempt(StepMethod::WaitFor);
    harness.complete_wait_for();
    harness.start_attempt(StepMethod::Execute);
    harness.complete_execute(StepDecision::default());
    assert!(
        is_step_execution_finished(
            &harness.state,
            harness.context.flow_hash,
            harness.run_id,
            "ChargeStep",
            1,
        )
        .unwrap()
    );
    harness.stop();
    assert!(is_flow_closed(&harness.state, harness.context.flow_hash, harness.run_id).unwrap());
    let closed_summary =
        get_flow_summary(&harness.state, harness.context.flow_hash, harness.run_id).unwrap();
    assert!(closed_summary.close_time.is_some());
}

#[test]
fn history_pages_a_full_flow_and_allocates_pending_ids_at_close() {
    let mut harness = Harness::new();
    harness.start(FlowStartOptions::default(), None);
    harness.start_attempt(StepMethod::WaitFor);
    harness.complete_wait_for();
    harness.start_attempt(StepMethod::Execute);
    harness.complete_execute(StepDecision {
        close_decision: Some(CloseDecision {
            close_decision_type: CloseDecisionType::ForceComplete as i32,
            close_input: Some(string_value("done")),
            ..CloseDecision::default()
        }),
        ..StepDecision::default()
    });
    let first = get_history_events(
        &harness.state,
        harness.context.flow_hash,
        harness.run_id,
        0,
        2,
        &[],
        0,
    )
    .unwrap();
    assert_eq!(
        first
            .events
            .iter()
            .map(|event| event.event_id)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(first.next_internal_event_id, 3);
    assert!(!first.next_page_token.is_empty());
    let second = get_history_events(
        &harness.state,
        harness.context.flow_hash,
        harness.run_id,
        0,
        2,
        &first.next_page_token,
        0,
    )
    .unwrap();
    assert_eq!(
        second
            .events
            .iter()
            .map(|event| event.event_id)
            .collect::<Vec<_>>(),
        [3, 4]
    );
    assert!(second.next_page_token.is_empty());
    let all = get_history_events(
        &harness.state,
        harness.context.flow_hash,
        harness.run_id,
        0,
        0,
        &[],
        0,
    )
    .unwrap();
    assert!(matches!(
        all.events[0].payload,
        Some(flow_history_event::Payload::FlowStartedOrContinued(_))
    ));
    assert!(matches!(
        all.events[1].payload,
        Some(flow_history_event::Payload::StepWaitForCompleted(_))
    ));
    assert!(matches!(
        all.events[2].payload,
        Some(flow_history_event::Payload::StepExecuteCompleted(_))
    ));
    assert!(matches!(
        all.events[3].payload,
        Some(flow_history_event::Payload::FlowClosed(_))
    ));
    assert!(all.events.iter().all(|event| event.run_attempt == 1));
    let wait =
        is_history_event_available(&harness.state, harness.context.flow_hash, harness.run_id, 3)
            .unwrap();
    assert!(wait.event_available);
    assert_eq!(wait.available_internal_event_id, 3);

    for is_running in [false, true] {
        let mut pending = Harness::new();
        pending.start(FlowStartOptions::default(), None);
        if is_running {
            pending.start_attempt(StepMethod::WaitFor);
        }
        pending.stop();
        let events = get_history_events(
            &pending.state,
            pending.context.flow_hash,
            pending.run_id,
            0,
            0,
            &[],
            0,
        )
        .unwrap()
        .events;
        assert_eq!(
            events
                .iter()
                .map(|event| event.event_id)
                .collect::<Vec<_>>(),
            [1, 2, 3]
        );
        let Some(flow_history_event::Payload::StepWaitForPending(method)) = &events[1].payload
        else {
            panic!("close must project a pending WaitFor call");
        };
        assert_eq!(
            method.phase,
            if is_running {
                dexzd_protocol::dex::PendingStepMethodPhase::Started as i32
            } else {
                dexzd_protocol::dex::PendingStepMethodPhase::Scheduled as i32
            }
        );
        assert!(matches!(
            events[2].payload,
            Some(flow_history_event::Payload::FlowClosed(_))
        ));
    }
}
