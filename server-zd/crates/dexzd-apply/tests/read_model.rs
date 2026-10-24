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
    ReadModelError, StepCompletionState, get_attributes, get_channel_messages, get_flow_state,
    get_flow_summary, get_history_events, get_search_document, is_flow_closed,
    is_history_event_available, list_search_documents, load_blobs, match_attribute, resolve_run_id,
    step_completion_state,
};
use dexzd_apply::{
    ApplyContext, ApplyOutcome, FlowStateReader, StateReadError, StateRow, StateWrite,
    apply_flow_command,
};
use dexzd_keyspace::row_keys::{
    flow_list_index_key, run_named_key, run_singleton_key, tenant_run_index_key,
};
use dexzd_keyspace::{ColumnFamily, IndexKind, PartitionHasher, RunId, RunTable, TenantId};
use dexzd_protocol::dex::{
    ActionPermissionMapping, ActionPermissionMappings, ActiveStepPhase, AttributeMatch,
    AttributeMatchOperator, AttributeWrite, ChannelMessage, CloseDecision, CloseDecisionType,
    ConditionStatus, FlowConfig, FlowStartOptions, FlowStatus, FlowTimeoutPolicy, IndexConfig,
    IndexType, InvokeExecuteMethodResponse, InvokeWaitForMethodResponse, LoadBlobRequestEntry,
    PublishToChannelRequest, PullRouting, PullRoutingSource, RetryPolicy, SetAttributesRequest,
    StartFlowRequest, StepDecision, StepDurability, StepMethodEventContext, StepMethodOptions,
    StepMovement, StepOptions, StopFlowRequest, StopType, TimerCondition, TimerResult,
    UpdateFlowConfigRequest, Value, WaitingCondition, WaitingConditionType, WorkerErrorResponse,
    WorkerTarget, flow_history_event, value,
};
use dexzd_protocol::dexzd::v1::{
    AttributesSetCommand, ChannelPublishCommand, ConditionResultsRecord, FlowCommand,
    FlowConfigUpdateCommand, FlowStartCommand, FlowStopCommand, FlowTimeoutTimerId,
    HardDeleteObjectReference, RetryBackoffTimerId, RunRecord, RunSettings,
    StepAttemptFailedCommand, StepAttemptFailureKind, StepAttemptStartedCommand,
    StepExecutionRecord, StepInvocationKey, StepMethod, StepMethodCompletedCommand,
    StoredFlowResult, TimerId, TimersFiredCommand, WaitConditionTimerId, flow_command::Command,
    step_method_completed_command, timer_id,
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
    flow_id: String,
    run_id: RunId,
    now_micros: i64,
}

impl Harness {
    fn new() -> Self {
        Self::new_for_flow("order-1", RunId([1; 16]))
    }

    fn new_for_flow(flow_id: &str, run_id: RunId) -> Self {
        let hasher = PartitionHasher::new(&[7; 16], 1024).expect("valid partition hasher");
        let flow_hash = hasher.flow_hash(TenantId::DEFAULT, flow_id);
        Self {
            state: MemoryState::default(),
            context: ApplyContext {
                partition: hasher.partition_of(flow_hash),
                flow_hash,
            },
            flow_id: flow_id.to_owned(),
            run_id,
            now_micros: 1_800_000_000_000_000,
        }
    }

    fn apply(&mut self, body: Command) -> ApplyOutcome {
        self.now_micros += 1000;
        let is_start = matches!(body, Command::FlowStart(_));
        let command = FlowCommand {
            flow_id: self.flow_id.clone(),
            run_id: if is_start {
                Vec::new()
            } else {
                self.run_id.as_bytes().to_vec()
            },
            behavior_version: 1,
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
        self.start_with_timeout(options, step_options, 0);
    }

    fn start_with_timeout(
        &mut self,
        options: FlowStartOptions,
        step_options: Option<StepOptions>,
        flow_timeout_seconds: i32,
    ) {
        let request = StartFlowRequest {
            flow_id: self.flow_id.clone(),
            flow_type: "OrderFlow".to_owned(),
            start_step_type: "ChargeStep".to_owned(),
            step_input: Some(string_value("input")),
            step_options,
            flow_timeout_seconds,
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
                max_run_state_bytes: 1 << 30,
                max_active_step_executions: 10_000,
                history_window_semantic_events: 10_000,
                history_prune_chunk_semantic_events: 1000,
                max_retained_superseded_bytes: 256 << 20,
                ..RunSettings::default()
            }),
            resolved_timeout_policy: FlowTimeoutPolicy::Fail as i32,
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
        self.start_attempt_for("ChargeStep-1", method);
    }

    fn start_attempt_for(&mut self, step_execution_id: &str, method: StepMethod) {
        self.start_attempt_with_routing(step_execution_id, method, None);
    }

    fn start_attempt_with_routing(
        &mut self,
        step_execution_id: &str,
        method: StepMethod,
        pull_routing: Option<PullRouting>,
    ) {
        self.apply(Command::StepAttemptStarted(StepAttemptStartedCommand {
            invocation: Some(StepInvocationKey {
                step_execution_id: step_execution_id.to_owned(),
                method: method as i32,
            }),
            attempt: 1,
            task_id: vec![2; 16],
            pull_routing,
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
        self.complete_execute_for("ChargeStep-1", decision);
    }

    fn complete_execute_for(&mut self, step_execution_id: &str, decision: StepDecision) {
        self.apply(Command::StepMethodCompleted(StepMethodCompletedCommand {
            invocation: Some(StepInvocationKey {
                step_execution_id: step_execution_id.to_owned(),
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
        self.stop_with(StopType::Cancel);
    }

    fn stop_with(&mut self, stop_type: StopType) {
        self.apply(Command::FlowStop(FlowStopCommand {
            request: Some(StopFlowRequest {
                stop_type: stop_type as i32,
                ..StopFlowRequest::default()
            }),
        }));
    }

    fn set_step_condition_results(&mut self, results: ConditionResultsRecord) {
        let key = run_named_key(
            self.context.flow_hash,
            self.run_id,
            RunTable::StepExecution,
            "ChargeStep-1",
        );
        let bytes = self
            .state
            .rows
            .get_mut(&(ColumnFamily::Flow, key))
            .expect("Step row exists");
        let mut step = StepExecutionRecord::decode(bytes.as_slice()).expect("valid Step row");
        step.condition_results = Some(results);
        *bytes = step.encode_to_vec();
    }

    fn mark_run_purging(&mut self) {
        let key = run_singleton_key(self.context.flow_hash, self.run_id, RunTable::RunRecord);
        let bytes = self
            .state
            .rows
            .get_mut(&(ColumnFamily::Flow, key))
            .expect("run row exists");
        let mut run = RunRecord::decode(bytes.as_slice()).expect("valid run row");
        run.is_purging = true;
        *bytes = run.encode_to_vec();
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

fn work_queue_permissions_write(permissions: &[&str]) -> AttributeWrite {
    AttributeWrite {
        key: "DexWorkQueuePermissions".to_owned(),
        value: Some(Value {
            kind: Some(value::Kind::ObjValue(dexzd_protocol::dex::EncodedObject {
                encoding: "json".to_owned(),
                payload: serde_json::to_vec(permissions)
                    .expect("a string slice serializes into JSON"),
            })),
        }),
        index_config: Some(IndexConfig {
            r#type: IndexType::KeywordArray as i32,
            ..index_config("DexWorkQueuePermissions")
        }),
        ..AttributeWrite::default()
    }
}

fn action_permission_mapping(
    attribute_key: &str,
    equal_value: Value,
    required_permission: &str,
) -> ActionPermissionMapping {
    ActionPermissionMapping {
        attribute_key: attribute_key.to_owned(),
        equal_values: vec![equal_value],
        required_permission: required_permission.to_owned(),
    }
}

fn assert_work_queue_permissions(harness: &Harness, expected: &[&str]) {
    let expected: Vec<String> = expected
        .iter()
        .map(|permission| (*permission).to_owned())
        .collect();
    let document = get_search_document(&harness.state, harness.context.flow_hash, harness.run_id)
        .expect("search document read succeeds")
        .expect("search document exists");
    assert_eq!(document.work_queue_permissions, expected);
    let indexed = document
        .attributes
        .iter()
        .find(|attribute| attribute.index_key == "DexWorkQueuePermissions");
    let attributes = get_attributes(
        &harness.state,
        harness.context.flow_hash,
        harness.run_id,
        &["DexWorkQueuePermissions".to_owned()],
        false,
    )
    .expect("Attribute read succeeds");
    if expected.is_empty() {
        assert!(indexed.is_none());
        assert!(attributes.response.attributes.is_empty());
    } else {
        let indexed = indexed.expect("permissions are indexed");
        assert_eq!(indexed.index_type, IndexType::KeywordArray as i32);
        let expected_value =
            work_queue_permissions_write(&expected.iter().map(String::as_str).collect::<Vec<_>>())
                .value;
        assert_eq!(indexed.value.as_ref(), expected_value.as_ref());
        assert_eq!(attributes.response.attributes.len(), 1);
        assert_eq!(
            attributes.response.attributes[0].value.as_ref(),
            expected_value.as_ref()
        );
    }
}

#[test]
fn initial_work_queue_permissions_attribute_is_searchable() {
    let mut harness = Harness::new();
    harness.start(
        FlowStartOptions {
            attributes: vec![work_queue_permissions_write(&[
                "permission-x",
                "permission-z",
            ])],
            ..FlowStartOptions::default()
        },
        None,
    );
    assert_work_queue_permissions(&harness, &["permission-x", "permission-z"]);
    let permission_set_key = run_singleton_key(
        harness.context.flow_hash,
        harness.run_id,
        RunTable::PermissionSet,
    );
    assert!(
        !harness
            .state
            .rows
            .contains_key(&(ColumnFamily::Flow, permission_set_key))
    );
}

#[test]
fn action_permission_projection_follows_attribute_state_matrix() {
    let mut harness = Harness::new();
    harness.start(
        FlowStartOptions {
            attributes: vec![
                AttributeWrite {
                    key: "first".to_owned(),
                    value: Some(string_value("A")),
                    ..AttributeWrite::default()
                },
                AttributeWrite {
                    key: "second".to_owned(),
                    value: Some(string_value("C")),
                    ..AttributeWrite::default()
                },
                work_queue_permissions_write(&["permission-x", "permission-z"]),
            ],
            ..FlowStartOptions::default()
        },
        None,
    );
    assert_work_queue_permissions(&harness, &["permission-x", "permission-z"]);
    let mappings = ActionPermissionMappings {
        mappings: vec![
            action_permission_mapping("first", string_value("A"), "permission-x"),
            action_permission_mapping("first", string_value("B"), "permission-y"),
            action_permission_mapping("first", string_value("A"), "permission-x"),
            action_permission_mapping("second", string_value("C"), "permission-z"),
        ],
    };
    for (first, second) in [
        (Some("B"), "C"),
        (Some("D"), "C"),
        (Some("D"), "D"),
        (None, "C"),
        (Some("A"), "C"),
    ] {
        harness.set_attributes(
            vec![
                AttributeWrite {
                    key: "first".to_owned(),
                    value: Some(first.map_or_else(
                        || Value {
                            kind: Some(value::Kind::NullValue(0)),
                        },
                        string_value,
                    )),
                    ..AttributeWrite::default()
                },
                AttributeWrite {
                    key: "second".to_owned(),
                    value: Some(string_value(second)),
                    ..AttributeWrite::default()
                },
            ],
            Some(mappings.clone()),
            Vec::new(),
        );
        assert_work_queue_permissions(&harness, &["permission-x", "permission-y", "permission-z"]);
    }
    let version_before_unchanged_mapping =
        get_search_document(&harness.state, harness.context.flow_hash, harness.run_id)
            .unwrap()
            .unwrap()
            .version;
    harness.set_attributes(Vec::new(), Some(mappings.clone()), Vec::new());
    assert_eq!(
        get_search_document(&harness.state, harness.context.flow_hash, harness.run_id)
            .unwrap()
            .unwrap()
            .version,
        version_before_unchanged_mapping
    );
    assert_work_queue_permissions(&harness, &["permission-x", "permission-y", "permission-z"]);
    harness.set_attributes(
        vec![work_queue_permissions_write(&["permission-y"])],
        None,
        Vec::new(),
    );
    assert_work_queue_permissions(&harness, &["permission-y"]);
    harness.set_attributes(Vec::new(), Some(mappings), Vec::new());
    assert_work_queue_permissions(&harness, &["permission-x", "permission-y", "permission-z"]);
}

#[test]
fn invalid_work_queue_permissions_are_rewritten_by_mappings() {
    let mut harness = Harness::new();
    harness.start(
        FlowStartOptions {
            attributes: vec![AttributeWrite {
                key: "state".to_owned(),
                value: Some(string_value("ready")),
                ..AttributeWrite::default()
            }],
            ..FlowStartOptions::default()
        },
        None,
    );
    let mut invalid = work_queue_permissions_write(&[]);
    invalid.value = Some(string_value("not JSON"));
    harness.set_attributes(vec![invalid.clone()], None, Vec::new());
    harness.set_attributes(
        Vec::new(),
        Some(ActionPermissionMappings {
            mappings: vec![action_permission_mapping(
                "state",
                string_value("ready"),
                "permission-ready",
            )],
        }),
        Vec::new(),
    );
    assert_work_queue_permissions(&harness, &["permission-ready"]);
    invalid.value = Some(Value {
        kind: Some(value::Kind::ObjValue(dexzd_protocol::dex::EncodedObject {
            encoding: "json".to_owned(),
            payload: b"{bad".to_vec(),
        })),
    });
    harness.set_attributes(vec![invalid], None, Vec::new());
    harness.set_attributes(
        Vec::new(),
        Some(ActionPermissionMappings::default()),
        Vec::new(),
    );
    assert_work_queue_permissions(&harness, &[]);
}

#[test]
fn action_permission_mappings_match_only_equal_scalar_values() {
    let mut harness = Harness::new();
    harness.start(
        FlowStartOptions {
            attributes: vec![
                AttributeWrite {
                    key: "flag".to_owned(),
                    value: Some(Value {
                        kind: Some(value::Kind::BoolValue(true)),
                    }),
                    ..AttributeWrite::default()
                },
                AttributeWrite {
                    key: "count".to_owned(),
                    value: Some(Value {
                        kind: Some(value::Kind::IntValue(42)),
                    }),
                    ..AttributeWrite::default()
                },
                AttributeWrite {
                    key: "ratio".to_owned(),
                    value: Some(Value {
                        kind: Some(value::Kind::DoubleValue(3.5)),
                    }),
                    ..AttributeWrite::default()
                },
                AttributeWrite {
                    key: "object".to_owned(),
                    value: work_queue_permissions_write(&["x"]).value,
                    ..AttributeWrite::default()
                },
            ],
            ..FlowStartOptions::default()
        },
        None,
    );
    let object_value = work_queue_permissions_write(&["x"]).value.unwrap();
    harness.set_attributes(
        Vec::new(),
        Some(ActionPermissionMappings {
            mappings: vec![
                action_permission_mapping(
                    "flag",
                    Value {
                        kind: Some(value::Kind::BoolValue(true)),
                    },
                    "scalar.bool",
                ),
                action_permission_mapping(
                    "count",
                    Value {
                        kind: Some(value::Kind::IntValue(42)),
                    },
                    "scalar.int",
                ),
                action_permission_mapping(
                    "ratio",
                    Value {
                        kind: Some(value::Kind::DoubleValue(3.5)),
                    },
                    "scalar.float",
                ),
                action_permission_mapping("object", object_value, "object.ignored"),
            ],
        }),
        Vec::new(),
    );
    assert_work_queue_permissions(&harness, &["scalar.bool", "scalar.float", "scalar.int"]);
}

#[test]
fn step_method_attribute_writes_project_action_permissions() {
    let mut harness = Harness::new();
    harness.start(FlowStartOptions::default(), None);
    let mappings = ActionPermissionMappings {
        mappings: vec![
            action_permission_mapping("stage", string_value("waiting"), "step.waiting"),
            action_permission_mapping("stage", string_value("executed"), "step.executed"),
        ],
    };
    harness.start_attempt(StepMethod::WaitFor);
    harness.apply(Command::StepMethodCompleted(StepMethodCompletedCommand {
        invocation: Some(StepInvocationKey {
            step_execution_id: "ChargeStep-1".to_owned(),
            method: StepMethod::WaitFor as i32,
        }),
        attempt: 1,
        response: Some(step_method_completed_command::Response::WaitFor(
            InvokeWaitForMethodResponse {
                upsert_attributes: vec![AttributeWrite {
                    key: "stage".to_owned(),
                    value: Some(string_value("waiting")),
                    ..AttributeWrite::default()
                }],
                action_permission_mappings: Some(mappings.clone()),
                ..InvokeWaitForMethodResponse::default()
            },
        )),
        ..StepMethodCompletedCommand::default()
    }));
    assert_work_queue_permissions(&harness, &["step.waiting"]);
    harness.start_attempt(StepMethod::Execute);
    harness.apply(Command::StepMethodCompleted(StepMethodCompletedCommand {
        invocation: Some(StepInvocationKey {
            step_execution_id: "ChargeStep-1".to_owned(),
            method: StepMethod::Execute as i32,
        }),
        attempt: 1,
        response: Some(step_method_completed_command::Response::Execute(
            InvokeExecuteMethodResponse {
                upsert_attributes: vec![AttributeWrite {
                    key: "stage".to_owned(),
                    value: Some(string_value("executed")),
                    ..AttributeWrite::default()
                }],
                action_permission_mappings: Some(mappings),
                ..InvokeExecuteMethodResponse::default()
            },
        )),
        ..StepMethodCompletedCommand::default()
    }));
    assert_work_queue_permissions(&harness, &["step.executed", "step.waiting"]);
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
    assert_eq!(document.attributes.len(), 4);
    assert_eq!(document.attributes[0].index_key, "Count");
    assert_eq!(document.attributes[0].index_type, IndexType::Int as i32);
    assert_eq!(document.attributes[1].index_key, "DexWorkQueuePermissions");
    assert_eq!(
        document.attributes[1].index_type,
        IndexType::KeywordArray as i32
    );
    assert_eq!(document.attributes[2].index_key, "LargestAllowed");
    assert_eq!(document.attributes[3].index_key, "ReviewState");
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
fn search_projection_matches_go_scalar_conversions_and_skips_bookkeeping_writes() {
    let mut harness = Harness::new();
    harness.start(
        FlowStartOptions {
            attributes: vec![AttributeWrite {
                key: "state".to_owned(),
                value: Some(string_value("original")),
                index_config: Some(index_config("Shared")),
                ..AttributeWrite::default()
            }],
            ..FlowStartOptions::default()
        },
        None,
    );
    harness.set_attributes(
        vec![
            AttributeWrite {
                key: "due".to_owned(),
                value: Some(string_value("2026-10-23T20:00:00+08:00")),
                index_config: Some(IndexConfig {
                    r#type: IndexType::Datetime as i32,
                    ..index_config("DueAt")
                }),
                ..AttributeWrite::default()
            },
            AttributeWrite {
                key: "flag".to_owned(),
                value: Some(string_value("TRUE")),
                index_config: Some(IndexConfig {
                    r#type: IndexType::Bool as i32,
                    ..index_config("Flag")
                }),
                ..AttributeWrite::default()
            },
            AttributeWrite {
                key: "size".to_owned(),
                value: Some(Value {
                    kind: Some(value::Kind::DoubleValue(1e21)),
                }),
                index_config: Some(IndexConfig {
                    r#type: IndexType::Keyword as i32,
                    ..index_config("Size")
                }),
                ..AttributeWrite::default()
            },
            AttributeWrite {
                key: "unbounded".to_owned(),
                value: Some(Value {
                    kind: Some(value::Kind::DoubleValue(f64::INFINITY)),
                }),
                index_config: Some(IndexConfig {
                    r#type: IndexType::Keyword as i32,
                    ..index_config("Unbounded")
                }),
                ..AttributeWrite::default()
            },
            AttributeWrite {
                key: "disabled".to_owned(),
                value: Some(string_value("0")),
                index_config: Some(IndexConfig {
                    r#type: IndexType::Bool as i32,
                    ..index_config("Disabled")
                }),
                ..AttributeWrite::default()
            },
            AttributeWrite {
                key: "state".to_owned(),
                value: Some(Value { kind: None }),
                index_config: Some(index_config("Shared")),
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
    let indexed = |key: &str| {
        document
            .attributes
            .iter()
            .find(|attribute| attribute.index_key == key)
            .and_then(|attribute| attribute.value.clone())
    };
    assert_eq!(indexed("DueAt"), Some(string_value("2026-10-23T12:00:00Z")));
    assert_eq!(
        indexed("Flag"),
        Some(Value {
            kind: Some(value::Kind::BoolValue(true))
        })
    );
    assert_eq!(indexed("Size"), Some(string_value("1e+21")));
    assert_eq!(indexed("Unbounded"), Some(string_value("+Inf")));
    assert_eq!(
        indexed("Disabled"),
        Some(Value {
            kind: Some(value::Kind::BoolValue(false))
        })
    );
    assert_eq!(indexed("Shared"), Some(string_value("original")));
    let version = document.version;
    harness.start_attempt(StepMethod::WaitFor);
    document = get_search_document(&harness.state, harness.context.flow_hash, harness.run_id)
        .unwrap()
        .unwrap();
    assert_eq!(document.version, version);
    harness.set_attributes(
        vec![AttributeWrite {
            key: "state".to_owned(),
            value: None,
            index_config: Some(index_config("Shared")),
            is_hard_delete_on_null: true,
            ..AttributeWrite::default()
        }],
        None,
        Vec::new(),
    );
    document = get_search_document(&harness.state, harness.context.flow_hash, harness.run_id)
        .unwrap()
        .unwrap();
    assert!(
        document
            .attributes
            .iter()
            .all(|value| value.index_key != "Shared")
    );
}

#[test]
fn search_listing_omits_runs_being_purged() {
    let mut harness = Harness::new();
    harness.start(FlowStartOptions::default(), None);
    harness.mark_run_purging();
    assert!(
        list_search_documents(
            &harness.state,
            harness.context.partition,
            TenantId::DEFAULT,
            None,
            10,
        )
        .unwrap()
        .is_empty()
    );
}

#[test]
fn permission_attribute_uses_go_json_escaping() {
    let mut harness = Harness::new();
    harness.start(FlowStartOptions::default(), None);
    harness.set_attributes(
        vec![AttributeWrite {
            key: "state".to_owned(),
            value: Some(string_value("ready")),
            ..AttributeWrite::default()
        }],
        Some(ActionPermissionMappings {
            mappings: vec![ActionPermissionMapping {
                attribute_key: "state".to_owned(),
                equal_values: vec![string_value("ready")],
                required_permission: "<>&\u{2028}\u{2029}".to_owned(),
            }],
        }),
        Vec::new(),
    );
    let attributes = get_attributes(
        &harness.state,
        harness.context.flow_hash,
        harness.run_id,
        &["DexWorkQueuePermissions".to_owned()],
        false,
    )
    .unwrap();
    let Some(value::Kind::ObjValue(object)) = attributes.response.attributes[0]
        .value
        .as_ref()
        .and_then(|value| value.kind.as_ref())
    else {
        panic!("permissions Attribute must be JSON");
    };
    assert_eq!(object.payload, b"[\"\\u003c\\u003e\\u0026\\u2028\\u2029\"]");
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
fn version_one_start_writes_search_document_and_index_rows() {
    let mut harness = Harness::new();
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
    let document = get_search_document(&harness.state, harness.context.flow_hash, harness.run_id)
        .unwrap()
        .unwrap();
    assert_eq!(document.version, 1);
    assert_eq!(document.attributes.len(), 1);
    assert_eq!(document.attributes[0].index_key, "ReviewState");
    assert_eq!(document.attributes[0].value, Some(string_value("created")));
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
    let list_key = flow_list_index_key(
        harness.context.partition,
        TenantId::DEFAULT,
        "OrderFlow",
        FlowStatus::Running as u8,
        harness.context.flow_hash,
        harness.run_id,
    );
    assert!(
        harness
            .state
            .rows
            .contains_key(&(ColumnFamily::PartitionIndex, list_key))
    );
    harness.start_attempt(StepMethod::WaitFor);
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
    assert_eq!(history.events.len(), 3);
    assert!(matches!(
        history.events[1].payload,
        Some(flow_history_event::Payload::StepWaitForPending(_))
    ));
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
        step_completion_state(
            &harness.state,
            harness.context.flow_hash,
            harness.run_id,
            "ChargeStep",
            1,
        )
        .unwrap()
            == StepCompletionState::NotCompleted
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
        &PartitionHasher::new(&[7; 16], 1024).unwrap(),
        TenantId::DEFAULT,
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
    let secret_state =
        get_flow_state(&harness.state, harness.context.flow_hash, harness.run_id).unwrap();
    let secret_reference = secret_state
        .attributes
        .iter()
        .find(|attribute| attribute.key == "secret")
        .and_then(|attribute| attribute.value.clone())
        .expect("hard-delete Attribute remains visible as a reference");
    assert!(matches!(
        secret_reference.kind.as_ref(),
        Some(value::Kind::InternalBlobIdForStringValue(reference)) if reference.starts_with("hd1|")
    ));
    assert!(
        load_blobs(
            &harness.state,
            &PartitionHasher::new(&[7; 16], 1024).unwrap(),
            TenantId::DEFAULT,
            &[LoadBlobRequestEntry {
                flow_id: "order-1".to_owned(),
                blob_value: Some(secret_reference),
            }],
        )
        .unwrap()
        .values
        .is_empty()
    );

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
        step_completion_state(
            &harness.state,
            harness.context.flow_hash,
            harness.run_id,
            "ChargeStep",
            1,
        )
        .unwrap()
            == StepCompletionState::Completed
    );
    harness.stop();
    assert!(is_flow_closed(&harness.state, harness.context.flow_hash, harness.run_id).unwrap());
    let closed_summary =
        get_flow_summary(&harness.state, harness.context.flow_hash, harness.run_id).unwrap();
    assert!(closed_summary.close_time.is_some());
}

#[test]
fn active_step_keeps_wait_for_routing_until_it_finishes() {
    let mut harness = Harness::new();
    harness.start(FlowStartOptions::default(), None);
    harness.start_attempt(StepMethod::WaitFor);
    let wait_for_routing = PullRouting {
        sticky_app_worker_id: "wait-worker".to_owned(),
        served_by_app_worker_id: "wait-worker".to_owned(),
        source: PullRoutingSource::StickyQueue as i32,
    };
    harness.apply(Command::StepMethodCompleted(StepMethodCompletedCommand {
        invocation: Some(StepInvocationKey {
            step_execution_id: "ChargeStep-1".to_owned(),
            method: StepMethod::WaitFor as i32,
        }),
        attempt: 1,
        response: Some(step_method_completed_command::Response::WaitFor(
            InvokeWaitForMethodResponse {
                waiting_condition: Some(WaitingCondition {
                    waiting_condition_type: WaitingConditionType::AllCompleted as i32,
                    timer_conditions: vec![TimerCondition {
                        condition_id: "delay".to_owned(),
                        duration_seconds: 60,
                        ..TimerCondition::default()
                    }],
                    ..WaitingCondition::default()
                }),
                ..InvokeWaitForMethodResponse::default()
            },
        )),
        app_worker_id: "wait-worker".to_owned(),
        pull_routing: Some(wait_for_routing.clone()),
        ..StepMethodCompletedCommand::default()
    }));

    let state = get_flow_state(&harness.state, harness.context.flow_hash, harness.run_id).unwrap();
    let waiting_step = &state.active_step_executions[0];
    assert_eq!(waiting_step.phase, ActiveStepPhase::Waiting as i32);
    assert_eq!(
        waiting_step.wait_for_pull_routing,
        Some(wait_for_routing.clone())
    );
    assert_eq!(
        wait_for_routing.served_by_app_worker_id.as_str(),
        state.sticky_app_worker_id.as_str()
    );
    assert!(waiting_step.execute_pull_routing.is_none());

    harness.apply(Command::TimersFired(TimersFiredCommand {
        timers: vec![TimerId {
            run_attempt: 1,
            kind: Some(timer_id::Kind::WaitCondition(WaitConditionTimerId {
                step_execution_id: "ChargeStep-1".to_owned(),
                timer_index: 0,
            })),
        }],
    }));
    harness.start_attempt(StepMethod::Execute);
    let state = get_flow_state(&harness.state, harness.context.flow_hash, harness.run_id).unwrap();
    let executing_step = &state.active_step_executions[0];
    assert_eq!(executing_step.phase, ActiveStepPhase::Active as i32);
    assert_eq!(executing_step.wait_for_pull_routing, Some(wait_for_routing));
    assert!(executing_step.execute_pull_routing.is_none());

    harness.complete_execute(StepDecision::default());
    let step_key = run_named_key(
        harness.context.flow_hash,
        harness.run_id,
        RunTable::StepExecution,
        "ChargeStep-1",
    );
    let step_row = harness
        .state
        .get(ColumnFamily::Flow, &step_key)
        .unwrap()
        .expect("Step row exists");
    let step = StepExecutionRecord::decode(step_row.as_slice()).unwrap();
    assert!(step.wait_for_pull_routing.is_none());
}

fn dispatch_routing(sticky_app_worker_id: &str) -> PullRouting {
    PullRouting {
        sticky_app_worker_id: sticky_app_worker_id.to_owned(),
        ..PullRouting::default()
    }
}

fn served_routing(app_worker_id: &str) -> PullRouting {
    PullRouting {
        sticky_app_worker_id: String::new(),
        served_by_app_worker_id: app_worker_id.to_owned(),
        source: PullRoutingSource::FlowTypeQueue as i32,
    }
}

#[test]
fn running_attempts_report_their_dispatch_routing_until_a_result_replaces_it() {
    let mut harness = Harness::new();
    harness.start(FlowStartOptions::default(), None);
    harness.start_attempt_with_routing(
        "ChargeStep-1",
        StepMethod::WaitFor,
        Some(dispatch_routing("")),
    );
    let state = get_flow_state(&harness.state, harness.context.flow_hash, harness.run_id).unwrap();
    let step = &state.active_step_executions[0];
    assert_eq!(step.wait_for_pull_routing, Some(dispatch_routing("")));
    assert!(step.execute_pull_routing.is_none());

    harness.apply(Command::StepMethodCompleted(StepMethodCompletedCommand {
        invocation: Some(StepInvocationKey {
            step_execution_id: "ChargeStep-1".to_owned(),
            method: StepMethod::WaitFor as i32,
        }),
        attempt: 1,
        response: Some(step_method_completed_command::Response::WaitFor(
            InvokeWaitForMethodResponse::default(),
        )),
        app_worker_id: "worker-a".to_owned(),
        pull_routing: Some(served_routing("worker-a")),
        ..StepMethodCompletedCommand::default()
    }));
    let state = get_flow_state(&harness.state, harness.context.flow_hash, harness.run_id).unwrap();
    let step = &state.active_step_executions[0];
    assert_eq!(step.wait_for_pull_routing, Some(served_routing("worker-a")));
    assert!(
        step.execute_pull_routing.is_none(),
        "Execute is not running"
    );

    harness.start_attempt_with_routing(
        "ChargeStep-1",
        StepMethod::Execute,
        Some(dispatch_routing("worker-a")),
    );
    let state = get_flow_state(&harness.state, harness.context.flow_hash, harness.run_id).unwrap();
    let step = &state.active_step_executions[0];
    assert_eq!(step.wait_for_pull_routing, Some(served_routing("worker-a")));
    assert_eq!(
        step.execute_pull_routing,
        Some(dispatch_routing("worker-a"))
    );
}

#[test]
fn a_result_rejected_by_apply_keeps_its_routing_on_the_attempt_failure() {
    let mut harness = Harness::new();
    harness.start(FlowStartOptions::default(), None);
    harness.start_attempt(StepMethod::WaitFor);
    let unknown_reference = Value {
        kind: Some(value::Kind::InternalBlobIdForStringValue(
            "zd1|not-a-reference".to_owned(),
        )),
    };
    harness.apply(Command::StepMethodCompleted(StepMethodCompletedCommand {
        invocation: Some(StepInvocationKey {
            step_execution_id: "ChargeStep-1".to_owned(),
            method: StepMethod::WaitFor as i32,
        }),
        attempt: 1,
        response: Some(step_method_completed_command::Response::WaitFor(
            InvokeWaitForMethodResponse {
                upsert_attributes: vec![AttributeWrite {
                    key: "report".to_owned(),
                    value: Some(unknown_reference),
                    ..AttributeWrite::default()
                }],
                ..InvokeWaitForMethodResponse::default()
            },
        )),
        app_worker_id: "worker-a".to_owned(),
        pull_routing: Some(served_routing("worker-a")),
        ..StepMethodCompletedCommand::default()
    }));
    let events = get_history_events(
        &harness.state,
        harness.context.flow_hash,
        harness.run_id,
        0,
        0,
        &[],
        0,
    )
    .unwrap()
    .events;
    let failure_routings: Vec<_> = events
        .iter()
        .filter_map(|event| match event.payload.as_ref() {
            Some(flow_history_event::Payload::StepWaitForFailed(failed)) => Some(
                failed
                    .output
                    .as_ref()
                    .and_then(|output| output.failure.as_ref())
                    .and_then(|failure| failure.pull_routing.clone()),
            ),
            _ => None,
        })
        .collect();
    assert_eq!(failure_routings, vec![Some(served_routing("worker-a"))]);
}

#[test]
fn a_step_canceled_by_cancel_close_is_reported_as_completed() {
    let mut harness = Harness::new();
    harness.start(FlowStartOptions::default(), None);
    harness.start_attempt(StepMethod::WaitFor);
    harness.stop();
    assert_eq!(
        step_completion_state(
            &harness.state,
            harness.context.flow_hash,
            harness.run_id,
            "ChargeStep",
            1,
        )
        .unwrap(),
        StepCompletionState::Completed
    );
    assert_eq!(
        step_completion_state(
            &harness.state,
            harness.context.flow_hash,
            harness.run_id,
            "ChargeStep",
            2,
        )
        .unwrap(),
        StepCompletionState::EndedWithoutCompletion
    );
}

#[test]
fn a_step_canceled_by_a_decision_is_completed_even_after_terminate() {
    let mut harness = Harness::new();
    harness.start(
        FlowStartOptions::default(),
        Some(StepOptions {
            skip_wait_for: true,
            ..StepOptions::default()
        }),
    );
    harness.start_attempt(StepMethod::Execute);
    harness.complete_execute(StepDecision {
        next_steps: vec![
            StepMovement {
                step_type: "CancelStep".to_owned(),
                ..StepMovement::default()
            },
            StepMovement {
                step_type: "DecidingStep".to_owned(),
                step_options: Some(StepOptions {
                    skip_wait_for: true,
                    ..StepOptions::default()
                }),
                ..StepMovement::default()
            },
        ],
        ..StepDecision::default()
    });
    harness.start_attempt_for("DecidingStep-1", StepMethod::Execute);
    harness.complete_execute_for(
        "DecidingStep-1",
        StepDecision {
            cancel_step_types: vec!["CancelStep".to_owned()],
            ..StepDecision::default()
        },
    );
    assert!(!is_flow_closed(&harness.state, harness.context.flow_hash, harness.run_id).unwrap());
    assert_eq!(
        step_completion_state(
            &harness.state,
            harness.context.flow_hash,
            harness.run_id,
            "CancelStep",
            1,
        )
        .unwrap(),
        StepCompletionState::Completed
    );
    harness.stop_with(StopType::Terminate);
    assert_eq!(
        step_completion_state(
            &harness.state,
            harness.context.flow_hash,
            harness.run_id,
            "CancelStep",
            1,
        )
        .unwrap(),
        StepCompletionState::Completed
    );
}

#[test]
fn a_step_canceled_by_terminate_close_ends_without_completion() {
    let mut harness = Harness::new();
    harness.start(FlowStartOptions::default(), None);
    harness.start_attempt(StepMethod::WaitFor);
    harness.stop_with(StopType::Terminate);
    assert_eq!(
        step_completion_state(
            &harness.state,
            harness.context.flow_hash,
            harness.run_id,
            "ChargeStep",
            1,
        )
        .unwrap(),
        StepCompletionState::EndedWithoutCompletion
    );
    harness.apply(Command::FlowConfigUpdate(FlowConfigUpdateCommand::default()));
    assert_eq!(
        step_completion_state(
            &harness.state,
            harness.context.flow_hash,
            harness.run_id,
            "ChargeStep",
            1,
        )
        .unwrap(),
        StepCompletionState::EndedWithoutCompletion
    );
}

#[test]
fn a_failed_step_ends_without_completion() {
    let mut harness = Harness::new();
    harness.start(FlowStartOptions::default(), None);
    assert_eq!(
        step_completion_state(
            &harness.state,
            harness.context.flow_hash,
            harness.run_id,
            "ChargeStep",
            1,
        )
        .unwrap(),
        StepCompletionState::NotCompleted
    );
    harness.start_attempt(StepMethod::WaitFor);
    harness.apply(Command::StepAttemptFailed(StepAttemptFailedCommand {
        invocation: Some(StepInvocationKey {
            step_execution_id: "ChargeStep-1".to_owned(),
            method: StepMethod::WaitFor as i32,
        }),
        attempt: 1,
        kind: 1,
        worker_error: Some(WorkerErrorResponse {
            detail: "unrecoverable".to_owned(),
            ..WorkerErrorResponse::default()
        }),
        is_non_retryable: true,
        ..StepAttemptFailedCommand::default()
    }));
    assert_eq!(
        step_completion_state(
            &harness.state,
            harness.context.flow_hash,
            harness.run_id,
            "ChargeStep",
            1,
        )
        .unwrap(),
        StepCompletionState::EndedWithoutCompletion
    );
}

#[test]
fn attribute_wait_rejects_a_stored_nonfinite_double_for_any_operand() {
    let mut harness = Harness::new();
    harness.start(FlowStartOptions::default(), None);
    harness.set_attributes(
        vec![AttributeWrite {
            key: "ratio".to_owned(),
            value: Some(Value {
                kind: Some(value::Kind::DoubleValue(f64::NAN)),
            }),
            ..AttributeWrite::default()
        }],
        None,
        Vec::new(),
    );
    assert!(matches!(
        match_attribute(
            &harness.state,
            harness.context.flow_hash,
            harness.run_id,
            &AttributeMatch {
                key: "ratio".to_owned(),
                operator: AttributeMatchOperator::Equal as i32,
                operand: Some(string_value("not-a-number")),
            },
        ),
        Err(ReadModelError::UnsupportedAttributeValue { .. })
    ));
}

#[test]
fn flow_state_orders_active_steps_by_type_then_execution_number() {
    let mut harness = Harness::new();
    harness.start(FlowStartOptions::default(), None);
    harness.start_attempt(StepMethod::WaitFor);
    harness.complete_wait_for();
    harness.start_attempt(StepMethod::Execute);
    harness.complete_execute(StepDecision {
        next_steps: vec![
            StepMovement {
                step_type: "ZStep".to_owned(),
                ..StepMovement::default()
            },
            StepMovement {
                step_type: "AStep".to_owned(),
                ..StepMovement::default()
            },
            StepMovement {
                step_type: "ZStep".to_owned(),
                ..StepMovement::default()
            },
        ],
        ..StepDecision::default()
    });
    let state = get_flow_state(&harness.state, harness.context.flow_hash, harness.run_id).unwrap();
    assert_eq!(
        state
            .active_step_executions
            .iter()
            .map(|step| step.step_execution_id.as_str())
            .collect::<Vec<_>>(),
        ["AStep-1", "ZStep-1", "ZStep-2"]
    );
    harness.stop();
    let events = get_history_events(
        &harness.state,
        harness.context.flow_hash,
        harness.run_id,
        0,
        0,
        &[],
        0,
    )
    .unwrap()
    .events;
    let pending: Vec<_> = events
        .iter()
        .filter_map(|event| match event.payload.as_ref() {
            Some(flow_history_event::Payload::StepWaitForPending(method)) => Some((
                event.event_id,
                method.context.as_ref().unwrap().step_execution_id.as_str(),
            )),
            _ => None,
        })
        .collect();
    assert_eq!(pending, [(4, "AStep-1"), (5, "ZStep-1"), (6, "ZStep-2")]);
}

#[test]
fn load_blobs_resolves_each_entry_under_its_own_flow() {
    let mut first = Harness::new();
    first.start(FlowStartOptions::default(), None);
    first.set_attributes(
        vec![AttributeWrite {
            key: "blob".to_owned(),
            value: Some(string_value(&"a".repeat(500))),
            ..AttributeWrite::default()
        }],
        None,
        Vec::new(),
    );
    let first_reference = get_attributes(
        &first.state,
        first.context.flow_hash,
        first.run_id,
        &["blob".to_owned()],
        false,
    )
    .unwrap()
    .response
    .attributes[0]
        .value
        .clone()
        .unwrap();
    let mut second = Harness::new_for_flow("order-2", RunId([2; 16]));
    second.state = first.state;
    second.start(FlowStartOptions::default(), None);
    second.set_attributes(
        vec![AttributeWrite {
            key: "blob".to_owned(),
            value: Some(string_value(&"b".repeat(500))),
            ..AttributeWrite::default()
        }],
        None,
        Vec::new(),
    );
    let second_reference = get_attributes(
        &second.state,
        second.context.flow_hash,
        second.run_id,
        &["blob".to_owned()],
        false,
    )
    .unwrap()
    .response
    .attributes[0]
        .value
        .clone()
        .unwrap();
    let hasher = PartitionHasher::new(&[7; 16], 1024).unwrap();
    let entries = [
        LoadBlobRequestEntry {
            flow_id: "order-1".to_owned(),
            blob_value: Some(first_reference.clone()),
        },
        LoadBlobRequestEntry {
            flow_id: "order-2".to_owned(),
            blob_value: Some(second_reference),
        },
    ];
    let loaded = load_blobs(&second.state, &hasher, TenantId::DEFAULT, &entries).unwrap();
    assert_eq!(loaded.values.len(), 2);
    assert!(
        loaded
            .values
            .values()
            .any(|value| value == &string_value(&"a".repeat(500)))
    );
    assert!(
        loaded
            .values
            .values()
            .any(|value| value == &string_value(&"b".repeat(500)))
    );
    let mismatch = load_blobs(
        &second.state,
        &hasher,
        TenantId::DEFAULT,
        &[LoadBlobRequestEntry {
            flow_id: "order-2".to_owned(),
            blob_value: Some(first_reference),
        }],
    );
    assert!(matches!(
        mismatch,
        Err(ReadModelError::InvalidRequest { .. })
    ));
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
        let scheduled_at = pending.now_micros;
        if is_running {
            pending.start_attempt(StepMethod::WaitFor);
        }
        let started_at = if is_running {
            pending.now_micros
        } else {
            scheduled_at
        };
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
        assert!(!method.input.as_ref().unwrap().unavailable);
        assert_eq!(
            method.input.as_ref().unwrap().step_input,
            Some(string_value("input"))
        );
        assert_eq!(
            method.context,
            Some(StepMethodEventContext {
                step_execution_id: "ChargeStep-1".to_owned(),
                from_step_execution_id: "__start__".to_owned(),
                step_type: "ChargeStep".to_owned(),
                durability: StepDurability::Sync as i32,
                final_attempt: 1,
                started_time: Some(test_timestamp(started_at)),
                duration: Some(test_duration(started_at, pending.now_micros)),
                method_options: Some(StepMethodOptions {
                    timeout_seconds: 7_200,
                    heartbeat_timeout_seconds: 60,
                    retry_policy: Some(RetryPolicy {
                        initial_interval_seconds: 1,
                        backoff_coefficient: 2.0,
                        maximum_interval_seconds: 100,
                        maximum_attempts: 0,
                        total_duration_seconds: 14_400,
                    }),
                }),
                ..StepMethodEventContext::default()
            })
        );
        assert!(matches!(
            events[2].payload,
            Some(flow_history_event::Payload::FlowClosed(_))
        ));
    }
}

#[test]
fn completed_method_contexts_include_effective_options_and_timing() {
    let mut harness = Harness::new();
    harness.start(
        FlowStartOptions {
            flow_config_override: Some(FlowConfig {
                step_durability: Some(StepDurability::Async as i32),
                ..FlowConfig::default()
            }),
            ..FlowStartOptions::default()
        },
        Some(StepOptions {
            wait_for_timeout_seconds: 17,
            heartbeat_timeout_seconds: 9,
            wait_for_durability_override: StepDurability::Sync as i32,
            wait_for_retry_policy: Some(RetryPolicy {
                maximum_attempts: 3,
                ..RetryPolicy::default()
            }),
            execute_retry_policy: Some(RetryPolicy {
                maximum_interval_seconds: 25,
                ..RetryPolicy::default()
            }),
            ..StepOptions::default()
        }),
    );
    harness.start_attempt(StepMethod::WaitFor);
    let wait_for_started_at = harness.now_micros;
    let wait_for_routing = PullRouting {
        sticky_app_worker_id: "worker-a".to_owned(),
        served_by_app_worker_id: "worker-a".to_owned(),
        source: PullRoutingSource::StickyQueue as i32,
    };
    harness.apply(Command::StepMethodCompleted(StepMethodCompletedCommand {
        invocation: Some(StepInvocationKey {
            step_execution_id: "ChargeStep-1".to_owned(),
            method: StepMethod::WaitFor as i32,
        }),
        attempt: 1,
        response: Some(step_method_completed_command::Response::WaitFor(
            InvokeWaitForMethodResponse::default(),
        )),
        app_worker_id: "worker-a".to_owned(),
        pull_routing: Some(wait_for_routing.clone()),
        ..StepMethodCompletedCommand::default()
    }));
    harness.start_attempt(StepMethod::Execute);
    let execute_started_at = harness.now_micros;
    let execute_routing = PullRouting {
        sticky_app_worker_id: "worker-a".to_owned(),
        served_by_app_worker_id: "worker-b".to_owned(),
        source: PullRoutingSource::FlowTypeQueue as i32,
    };
    harness.apply(Command::StepMethodCompleted(StepMethodCompletedCommand {
        invocation: Some(StepInvocationKey {
            step_execution_id: "ChargeStep-1".to_owned(),
            method: StepMethod::Execute as i32,
        }),
        attempt: 1,
        response: Some(step_method_completed_command::Response::Execute(
            InvokeExecuteMethodResponse {
                step_decision: Some(StepDecision::default()),
                ..InvokeExecuteMethodResponse::default()
            },
        )),
        app_worker_id: "worker-b".to_owned(),
        pull_routing: Some(execute_routing.clone()),
        ..StepMethodCompletedCommand::default()
    }));
    let events = get_history_events(
        &harness.state,
        harness.context.flow_hash,
        harness.run_id,
        0,
        0,
        &[],
        0,
    )
    .unwrap()
    .events;
    let Some(flow_history_event::Payload::StepWaitForCompleted(wait_for)) = &events[1].payload
    else {
        panic!("WaitFor completion event exists");
    };
    assert_eq!(
        wait_for.context,
        Some(StepMethodEventContext {
            step_execution_id: "ChargeStep-1".to_owned(),
            from_step_execution_id: "__start__".to_owned(),
            step_type: "ChargeStep".to_owned(),
            durability: StepDurability::Sync as i32,
            final_attempt: 1,
            started_time: Some(test_timestamp(wait_for_started_at)),
            duration: Some(prost_types::Duration {
                seconds: 0,
                nanos: 1_000_000,
            }),
            method_options: Some(StepMethodOptions {
                timeout_seconds: 17,
                heartbeat_timeout_seconds: 9,
                retry_policy: Some(RetryPolicy {
                    initial_interval_seconds: 1,
                    backoff_coefficient: 2.0,
                    maximum_interval_seconds: 100,
                    maximum_attempts: 3,
                    total_duration_seconds: 14_400,
                }),
            }),
            pull_routing: Some(wait_for_routing),
            ..StepMethodEventContext::default()
        })
    );
    let Some(flow_history_event::Payload::StepExecuteCompleted(execute)) = &events[2].payload
    else {
        panic!("Execute completion event exists");
    };
    assert_eq!(
        execute.context,
        Some(StepMethodEventContext {
            step_execution_id: "ChargeStep-1".to_owned(),
            from_step_execution_id: "__start__".to_owned(),
            step_type: "ChargeStep".to_owned(),
            durability: StepDurability::Async as i32,
            final_attempt: 1,
            started_time: Some(test_timestamp(execute_started_at)),
            duration: Some(prost_types::Duration {
                seconds: 0,
                nanos: 1_000_000,
            }),
            method_options: Some(StepMethodOptions {
                timeout_seconds: 7_200,
                heartbeat_timeout_seconds: 9,
                retry_policy: Some(RetryPolicy {
                    initial_interval_seconds: 1,
                    backoff_coefficient: 2.0,
                    maximum_interval_seconds: 25,
                    maximum_attempts: 0,
                    total_duration_seconds: 14_400,
                }),
            }),
            pull_routing: Some(execute_routing),
            ..StepMethodEventContext::default()
        })
    );
}

#[test]
fn method_context_keeps_durability_from_the_method_schedule() {
    let mut harness = Harness::new();
    harness.start(
        FlowStartOptions {
            flow_config_override: Some(FlowConfig {
                step_durability: Some(StepDurability::Async as i32),
                ..FlowConfig::default()
            }),
            ..FlowStartOptions::default()
        },
        None,
    );
    let update_durability = |durability| {
        Command::FlowConfigUpdate(FlowConfigUpdateCommand {
            request: Some(UpdateFlowConfigRequest {
                flow_config: Some(FlowConfig {
                    step_durability: Some(durability as i32),
                    ..FlowConfig::default()
                }),
                ..UpdateFlowConfigRequest::default()
            }),
        })
    };
    harness.apply(update_durability(StepDurability::Sync));
    harness.start_attempt(StepMethod::WaitFor);
    harness.complete_wait_for();
    harness.apply(update_durability(StepDurability::Async));
    harness.start_attempt(StepMethod::Execute);
    harness.complete_execute(StepDecision::default());
    let events = get_history_events(
        &harness.state,
        harness.context.flow_hash,
        harness.run_id,
        0,
        0,
        &[],
        0,
    )
    .unwrap()
    .events;
    let Some(flow_history_event::Payload::StepWaitForCompleted(wait_for)) = &events[1].payload
    else {
        panic!("WaitFor completion event exists");
    };
    assert_eq!(
        wait_for.context.as_ref().unwrap().durability,
        StepDurability::Async as i32
    );
    let Some(flow_history_event::Payload::StepExecuteCompleted(execute)) = &events[2].payload
    else {
        panic!("Execute completion event exists");
    };
    assert_eq!(
        execute.context.as_ref().unwrap().durability,
        StepDurability::Sync as i32
    );
}

#[test]
fn retried_method_contexts_keep_the_previous_failure_and_final_start() {
    let mut harness = Harness::new();
    harness.start(
        FlowStartOptions::default(),
        Some(StepOptions {
            wait_for_retry_policy: Some(RetryPolicy {
                maximum_attempts: 2,
                ..RetryPolicy::default()
            }),
            execute_retry_policy: Some(RetryPolicy {
                maximum_attempts: 2,
                ..RetryPolicy::default()
            }),
            ..StepOptions::default()
        }),
    );
    let invocation_key = |method| StepInvocationKey {
        step_execution_id: "ChargeStep-1".to_owned(),
        method: method as i32,
    };
    let failure_routing = |served_by: &str| PullRouting {
        sticky_app_worker_id: "worker-a".to_owned(),
        served_by_app_worker_id: served_by.to_owned(),
        source: PullRoutingSource::FlowTypeQueue as i32,
    };
    let failure_command = |method, attempt, detail: &str| {
        Command::StepAttemptFailed(StepAttemptFailedCommand {
            invocation: Some(invocation_key(method)),
            attempt,
            kind: StepAttemptFailureKind::WorkerError as i32,
            worker_error: Some(WorkerErrorResponse {
                detail: detail.to_owned(),
                ..WorkerErrorResponse::default()
            }),
            pull_routing: Some(failure_routing(detail)),
            ..StepAttemptFailedCommand::default()
        })
    };
    let retry_timer = |method| {
        Command::TimersFired(TimersFiredCommand {
            timers: vec![TimerId {
                run_attempt: 1,
                kind: Some(timer_id::Kind::RetryBackoff(RetryBackoffTimerId {
                    invocation: Some(invocation_key(method)),
                    next_attempt: 2,
                })),
            }],
        })
    };
    let start_second_attempt = |method| {
        Command::StepAttemptStarted(StepAttemptStartedCommand {
            invocation: Some(invocation_key(method)),
            attempt: 2,
            task_id: vec![3; 16],
            pull_routing: None,
        })
    };

    harness.start_attempt(StepMethod::WaitFor);
    let wait_for_started_at = harness.now_micros;
    harness.apply(failure_command(StepMethod::WaitFor, 1, "wait retry"));
    harness.apply(retry_timer(StepMethod::WaitFor));
    harness.apply(start_second_attempt(StepMethod::WaitFor));
    let wait_for_retry_started_at = harness.now_micros;
    harness.apply(Command::StepMethodCompleted(StepMethodCompletedCommand {
        invocation: Some(invocation_key(StepMethod::WaitFor)),
        attempt: 2,
        response: Some(step_method_completed_command::Response::WaitFor(
            InvokeWaitForMethodResponse::default(),
        )),
        ..StepMethodCompletedCommand::default()
    }));
    let wait_for_completed_at = harness.now_micros;

    harness.start_attempt(StepMethod::Execute);
    let execute_started_at = harness.now_micros;
    harness.apply(failure_command(StepMethod::Execute, 1, "execute retry"));
    harness.apply(retry_timer(StepMethod::Execute));
    harness.apply(start_second_attempt(StepMethod::Execute));
    let execute_retry_started_at = harness.now_micros;
    harness.apply(failure_command(StepMethod::Execute, 2, "execute final"));
    let execute_failed_at = harness.now_micros;

    let events = get_history_events(
        &harness.state,
        harness.context.flow_hash,
        harness.run_id,
        0,
        0,
        &[],
        0,
    )
    .unwrap()
    .events;
    let Some(flow_history_event::Payload::StepWaitForCompleted(wait_for)) = &events[1].payload
    else {
        panic!("retried WaitFor completion event exists");
    };
    let wait_for_context = wait_for.context.as_ref().unwrap();
    assert_eq!(wait_for_context.step_execution_id, "ChargeStep-1");
    assert_eq!(wait_for_context.from_step_execution_id, "__start__");
    assert_eq!(wait_for_context.step_type, "ChargeStep");
    assert_eq!(wait_for_context.durability, StepDurability::Sync as i32);
    assert_eq!(wait_for_context.final_attempt, 2);
    assert_ne!(wait_for_started_at, wait_for_retry_started_at);
    assert_eq!(
        wait_for_context.started_time,
        Some(test_timestamp(wait_for_retry_started_at))
    );
    assert_eq!(
        wait_for_context.duration,
        Some(test_duration(
            wait_for_retry_started_at,
            wait_for_completed_at
        ))
    );
    assert_eq!(
        wait_for_context.method_options,
        Some(expected_default_method_options(2))
    );
    let wait_for_failure = wait_for_context.last_failure_info.as_ref().unwrap();
    assert_eq!(
        wait_for_failure
            .details
            .as_ref()
            .unwrap()
            .original_worker_error_detail,
        "wait retry"
    );
    assert_eq!(wait_for_failure.attempt, 1);
    assert_eq!(
        wait_for_failure.pull_routing,
        Some(failure_routing("wait retry"))
    );
    assert!(wait_for_context.pull_routing.is_none());

    let Some(flow_history_event::Payload::StepExecuteFailed(execute)) = &events[2].payload else {
        panic!("retried Execute failure event exists");
    };
    let execute_context = execute.context.as_ref().unwrap();
    assert_eq!(execute_context.step_execution_id, "ChargeStep-1");
    assert_eq!(execute_context.from_step_execution_id, "__start__");
    assert_eq!(execute_context.step_type, "ChargeStep");
    assert_eq!(execute_context.durability, StepDurability::Sync as i32);
    assert_eq!(execute_context.final_attempt, 2);
    assert_ne!(execute_started_at, execute_retry_started_at);
    assert_eq!(
        execute_context.started_time,
        Some(test_timestamp(execute_retry_started_at))
    );
    assert_eq!(
        execute_context.duration,
        Some(test_duration(execute_retry_started_at, execute_failed_at))
    );
    assert_eq!(
        execute_context.method_options,
        Some(expected_default_method_options(2))
    );
    let previous_failure = execute_context.last_failure_info.as_ref().unwrap();
    assert_eq!(
        previous_failure
            .details
            .as_ref()
            .unwrap()
            .original_worker_error_detail,
        "execute retry"
    );
    assert_eq!(previous_failure.attempt, 1);
    assert_eq!(
        previous_failure.pull_routing,
        Some(failure_routing("execute retry"))
    );
    assert_eq!(
        execute_context.pull_routing,
        Some(failure_routing("execute final"))
    );
    let final_failure = execute.output.as_ref().unwrap().failure.as_ref().unwrap();
    assert_eq!(
        final_failure
            .details
            .as_ref()
            .unwrap()
            .original_worker_error_detail,
        "execute final"
    );
    assert_eq!(final_failure.attempt, 2);
}

#[test]
fn exhausted_first_attempt_has_no_previous_failure_in_history_context() {
    let mut harness = Harness::new();
    harness.start(FlowStartOptions::default(), None);
    harness.start_attempt(StepMethod::WaitFor);
    let started_at = harness.now_micros;
    harness.apply(Command::StepAttemptFailed(StepAttemptFailedCommand {
        invocation: Some(StepInvocationKey {
            step_execution_id: "ChargeStep-1".to_owned(),
            method: StepMethod::WaitFor as i32,
        }),
        attempt: 1,
        kind: StepAttemptFailureKind::WorkerError as i32,
        is_non_retryable: true,
        worker_error: Some(WorkerErrorResponse {
            detail: "first failure".to_owned(),
            ..WorkerErrorResponse::default()
        }),
        ..StepAttemptFailedCommand::default()
    }));
    let events = get_history_events(
        &harness.state,
        harness.context.flow_hash,
        harness.run_id,
        0,
        0,
        &[],
        0,
    )
    .unwrap()
    .events;
    let Some(flow_history_event::Payload::StepWaitForFailed(failed)) = &events[1].payload else {
        panic!("first attempt failure event exists");
    };
    let context = failed.context.as_ref().unwrap();
    assert_eq!(context.final_attempt, 1);
    assert_eq!(context.started_time, Some(test_timestamp(started_at)));
    assert_eq!(
        context.duration,
        Some(test_duration(started_at, harness.now_micros))
    );
    assert!(context.last_failure_info.is_none());
    let failure = failed.output.as_ref().unwrap().failure.as_ref().unwrap();
    assert_eq!(failure.attempt, 1);
    assert_eq!(
        failure
            .details
            .as_ref()
            .unwrap()
            .original_worker_error_detail,
        "first failure"
    );
}

#[test]
fn pending_retry_context_reports_the_last_started_attempt() {
    for is_retry_running in [false, true] {
        let mut harness = Harness::new();
        harness.start(
            FlowStartOptions::default(),
            Some(StepOptions {
                wait_for_retry_policy: Some(RetryPolicy {
                    maximum_attempts: 3,
                    ..RetryPolicy::default()
                }),
                ..StepOptions::default()
            }),
        );
        let first_attempt_at = harness.now_micros;
        harness.start_attempt(StepMethod::WaitFor);
        let invocation = StepInvocationKey {
            step_execution_id: "ChargeStep-1".to_owned(),
            method: StepMethod::WaitFor as i32,
        };
        harness.apply(Command::StepAttemptFailed(StepAttemptFailedCommand {
            invocation: Some(invocation.clone()),
            attempt: 1,
            kind: StepAttemptFailureKind::WorkerError as i32,
            worker_error: Some(WorkerErrorResponse {
                detail: "retry later".to_owned(),
                ..WorkerErrorResponse::default()
            }),
            ..StepAttemptFailedCommand::default()
        }));
        harness.apply(Command::TimersFired(TimersFiredCommand {
            timers: vec![TimerId {
                run_attempt: 1,
                kind: Some(timer_id::Kind::RetryBackoff(RetryBackoffTimerId {
                    invocation: Some(invocation.clone()),
                    next_attempt: 2,
                })),
            }],
        }));
        if is_retry_running {
            harness.apply(Command::StepAttemptStarted(StepAttemptStartedCommand {
                invocation: Some(invocation),
                attempt: 2,
                task_id: vec![3; 16],
                pull_routing: None,
            }));
        }
        let expected_started_at = if is_retry_running {
            harness.now_micros
        } else {
            first_attempt_at
        };
        harness.stop();
        let events = get_history_events(
            &harness.state,
            harness.context.flow_hash,
            harness.run_id,
            0,
            0,
            &[],
            0,
        )
        .unwrap()
        .events;
        let Some(flow_history_event::Payload::StepWaitForPending(pending)) = &events[1].payload
        else {
            panic!("retry pending event exists");
        };
        assert_eq!(
            pending.phase,
            if is_retry_running {
                dexzd_protocol::dex::PendingStepMethodPhase::Started as i32
            } else {
                dexzd_protocol::dex::PendingStepMethodPhase::Scheduled as i32
            }
        );
        let context = pending.context.as_ref().unwrap();
        assert_eq!(context.step_execution_id, "ChargeStep-1");
        assert_eq!(context.from_step_execution_id, "__start__");
        assert_eq!(context.step_type, "ChargeStep");
        assert_eq!(context.durability, StepDurability::Sync as i32);
        assert_eq!(context.final_attempt, if is_retry_running { 2 } else { 1 });
        assert_eq!(
            context.started_time,
            Some(test_timestamp(expected_started_at))
        );
        assert_eq!(
            context.duration,
            Some(test_duration(expected_started_at, harness.now_micros))
        );
        assert_eq!(
            context.method_options,
            Some(expected_default_method_options(3))
        );
        let previous_failure = context.last_failure_info.as_ref().unwrap();
        assert_eq!(
            previous_failure
                .details
                .as_ref()
                .unwrap()
                .original_worker_error_detail,
            "retry later"
        );
        assert_eq!(previous_failure.attempt, 1);
        assert!(context.pull_routing.is_none());
    }
}

fn expected_default_method_options(maximum_attempts: i32) -> StepMethodOptions {
    StepMethodOptions {
        timeout_seconds: 7_200,
        heartbeat_timeout_seconds: 60,
        retry_policy: Some(RetryPolicy {
            initial_interval_seconds: 1,
            backoff_coefficient: 2.0,
            maximum_interval_seconds: 100,
            maximum_attempts,
            total_duration_seconds: 14_400,
        }),
    }
}

fn test_timestamp(micros: i64) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: micros.div_euclid(1_000_000),
        nanos: i32::try_from(micros.rem_euclid(1_000_000) * 1000)
            .expect("fractional microseconds fit i32"),
    }
}

fn test_duration(started_at_micros: i64, ended_at_micros: i64) -> prost_types::Duration {
    let duration_micros = ended_at_micros - started_at_micros;
    prost_types::Duration {
        seconds: duration_micros / 1_000_000,
        nanos: i32::try_from(duration_micros % 1_000_000).expect("fractional microseconds fit i32")
            * 1000,
    }
}

#[test]
fn failed_close_drops_pending_step_method_events() {
    let mut harness = Harness::new();
    harness.start(FlowStartOptions::default(), None);
    harness.start_attempt(StepMethod::WaitFor);
    harness.apply(Command::FlowStop(FlowStopCommand {
        request: Some(StopFlowRequest {
            stop_type: StopType::Fail as i32,
            ..StopFlowRequest::default()
        }),
    }));
    let events = get_history_events(
        &harness.state,
        harness.context.flow_hash,
        harness.run_id,
        0,
        0,
        &[],
        0,
    )
    .unwrap()
    .events;
    assert_eq!(events.len(), 2);
    assert!(matches!(
        events[1].payload,
        Some(flow_history_event::Payload::FlowClosed(_))
    ));
}

#[test]
fn fail_policy_flow_timeout_drops_pending_step_method_events() {
    let mut harness = Harness::new();
    harness.start_with_timeout(FlowStartOptions::default(), None, 30);
    harness.start_attempt(StepMethod::WaitFor);
    harness.apply(Command::TimersFired(TimersFiredCommand {
        timers: vec![TimerId {
            run_attempt: 1,
            kind: Some(timer_id::Kind::FlowTimeout(FlowTimeoutTimerId {})),
        }],
    }));
    let summary =
        get_flow_summary(&harness.state, harness.context.flow_hash, harness.run_id).unwrap();
    assert_eq!(summary.flow_status, FlowStatus::Failed as i32);
    let events = get_history_events(
        &harness.state,
        harness.context.flow_hash,
        harness.run_id,
        0,
        0,
        &[],
        0,
    )
    .unwrap()
    .events;
    assert_eq!(events.len(), 2);
    assert!(matches!(
        events[1].payload,
        Some(flow_history_event::Payload::FlowClosed(_))
    ));
}

#[test]
fn pending_execute_includes_condition_results_and_state_only_lists_completed_conditions() {
    let mut harness = Harness::new();
    harness.start(FlowStartOptions::default(), None);
    harness.start_attempt(StepMethod::WaitFor);
    harness.complete_wait_for();
    harness.set_step_condition_results(ConditionResultsRecord {
        timer_results: vec![
            TimerResult {
                condition_id: "pending-timer".to_owned(),
                condition_status: ConditionStatus::Waiting as i32,
            },
            TimerResult {
                condition_id: "done-timer".to_owned(),
                condition_status: ConditionStatus::Completed as i32,
            },
        ],
        sub_flow_results: vec![
            StoredFlowResult {
                flow_status: FlowStatus::Running as i32,
                ..StoredFlowResult::default()
            },
            StoredFlowResult {
                flow_status: FlowStatus::Completed as i32,
                ..StoredFlowResult::default()
            },
        ],
        ..ConditionResultsRecord::default()
    });
    harness.start_attempt(StepMethod::Execute);
    let execute_started_at = harness.now_micros;
    let state = get_flow_state(&harness.state, harness.context.flow_hash, harness.run_id).unwrap();
    let completed = state.active_step_executions[0]
        .completed_conditions
        .as_ref()
        .unwrap();
    assert_eq!(completed.completed_timer_conditions.len(), 1);
    assert!(completed.completed_timer_conditions.contains_key(&1));
    assert_eq!(completed.completed_sub_flow_results.len(), 1);
    assert!(completed.completed_sub_flow_results.contains_key(&1));
    harness.stop();
    let events = get_history_events(
        &harness.state,
        harness.context.flow_hash,
        harness.run_id,
        0,
        0,
        &[],
        0,
    )
    .unwrap()
    .events;
    let Some(flow_history_event::Payload::StepExecutePending(method)) = &events[2].payload else {
        panic!("close must project a pending Execute call");
    };
    let context = method.context.as_ref().unwrap();
    assert_eq!(context.step_execution_id, "ChargeStep-1");
    assert_eq!(context.from_step_execution_id, "__start__");
    assert_eq!(context.step_type, "ChargeStep");
    assert_eq!(context.durability, StepDurability::Sync as i32);
    assert_eq!(context.final_attempt, 1);
    assert_eq!(
        context.started_time,
        Some(test_timestamp(execute_started_at))
    );
    assert_eq!(
        context.duration,
        Some(test_duration(execute_started_at, harness.now_micros))
    );
    assert_eq!(
        context.method_options,
        Some(expected_default_method_options(0))
    );
    assert!(context.last_failure_info.is_none());
    assert!(context.pull_routing.is_none());
    let input = method.input.as_ref().unwrap();
    assert!(!input.unavailable);
    assert_eq!(
        input
            .condition_results
            .as_ref()
            .unwrap()
            .timer_results
            .len(),
        2
    );
    assert_eq!(
        input
            .condition_results
            .as_ref()
            .unwrap()
            .sub_flow_results
            .len(),
        2
    );
}
