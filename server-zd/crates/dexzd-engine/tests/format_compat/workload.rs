// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use bytes::Bytes;
use dexzd_apply::{ApplyContext, ApplyEffect, ApplyOutcome, FlowStateReader};
use dexzd_clock::WallClock;
use dexzd_engine::{
    CommitTicket, CommitUrgency, CommittedCommand, EngineNode, HardDeleteObjectOwner,
    default_run_settings, encode_hard_delete_value, hard_delete_placeholder_value,
};
use dexzd_keyspace::row_keys::prefix_range;
use dexzd_keyspace::{ColumnFamily, PartitionHasher, RunId, RunTable, TenantId, run_table_prefix};
use dexzd_object_store::{InMemoryObjectStore, ObjectStore, WriteCondition, WriteOutcome};
use dexzd_protocol::dex::{
    AttributeWrite, ChannelCondition, ChannelMessage, CloseDecision, CloseDecisionType,
    DeleteChannelMessageRequest, FlowConfig, FlowResetStepMethod, FlowResetType, FlowStartOptions,
    FlowTimeoutPolicy, InvokeExecuteMethodResponse, InvokeRpcRequest, InvokeWaitForMethodResponse,
    InvokeWorkerRpcResponse, Kv, PublishToChannelRequest, ResetFlowRequest, RetryPolicy,
    SetAttributesRequest, SkipTimerRequest, StartFlowRequest, StepDecision, StepDurability,
    StepMovement, StepOptions, StopFlowRequest, StopType, SubFlowCondition, TimerCondition,
    UpdateFlowConfigRequest, Value, WaitingCondition, WaitingConditionType, WorkerErrorResponse,
    WorkerTarget, value,
};
use dexzd_protocol::dexzd::v1::{
    AttributesSetCommand, ChannelMessageDeleteCommand, ChannelPublishCommand, FlowCommand,
    FlowConfigUpdateCommand, FlowStartCommand, FlowStopCommand, HardDeleteObjectReference,
    HardDeleteObjectsDeletedCommand, InboxMessageCommand, OutboxAckCommand, OutboxRecord,
    RpcCommittedCommand, RunPurgeCommand, RunSettings, StepAttemptFailedCommand,
    StepAttemptFailureKind, StepAttemptHeartbeatCommand, StepAttemptStartedCommand,
    StepInvocationKey, StepMethod, StepMethodCompletedCommand, TimeTravelCommand, TimerRecord,
    TimerSkipCommand, TimersFiredCommand, command_reply, flow_command::Command,
    inbox_message_command, outbox_record, step_attempt_heartbeat_command,
    step_method_completed_command, timer_id,
};
use prost::Message;
use xxhash_rust::xxh3::xxh3_128;

use crate::scripted_clock::ScriptedClock;
use crate::seeded_id_source::SeededIdSource;

/// Seed of every identifier the workload generates.
pub(crate) const WORKLOAD_SEED: u64 = 0x5eed_2026_1024;
/// Scripted time between two commands.
const COMMAND_SPACING_MICROS: i64 = 250_000;
const SECOND_MICROS: i64 = 1_000_000;
const WORKER_ID: &str = "worker-a";
const SUB_FLOW_PARENT: &str = "fulfillment-parent";
/// Longer than the 100-byte inline threshold, so it lives in the Value column family.
const LONG_PROFILE_TEXT: &str = "Preferred carrier: ground. Deliver to the side entrance. \
    Leave with the front desk when nobody answers. Signature required for orders above 500.";

/// Drives a scripted workload through the real commit pipeline; every command must apply.
pub(crate) struct Workload<'a> {
    pub(crate) node: &'a EngineNode,
    pub(crate) clock: &'a ScriptedClock,
    pub(crate) ids: &'a SeededIdSource,
    pub(crate) object_store: &'a InMemoryObjectStore,
    pub(crate) hasher: &'a PartitionHasher,
}

/// A started run of one Flow.
#[derive(Debug, Clone)]
struct FlowRun {
    flow_id: String,
    run_id: RunId,
}

/// Runs whose later commands follow the snapshot.
pub(crate) struct OpenRuns {
    order: FlowRun,
    profile: FlowRun,
    profile_secret_object: String,
    parent: FlowRun,
    child: FlowRun,
}

impl Workload<'_> {
    /// Commands whose results the snapshot captures.
    pub(crate) async fn run_before_snapshot(&self) -> OpenRuns {
        let order = self.order_waits_for_timer_and_channel_then_retries().await;
        let (profile, profile_secret_object) = self.profile_rewrites_hard_delete_values().await;
        let (parent, child) = self.parent_starts_sub_flow().await;
        OpenRuns {
            order,
            profile,
            profile_secret_object,
            parent,
            child,
        }
    }

    /// Commands that only the log after the snapshot holds.
    pub(crate) async fn run_after_snapshot(&self, open: OpenRuns) {
        self.sub_flow_completes_and_wakes_parent(&open.parent, &open.child)
            .await;
        self.order_ships_under_lock_after_locked_rpc(&open.order)
            .await;
        self.async_steps_share_batches_with_another_partition(&open.profile)
            .await;
        self.profile_clears_hard_delete_value(&open.profile, &open.profile_secret_object)
            .await;
        self.travel_back_to_second_step().await;
        self.skip_timer_update_config_and_stop().await;
        self.purge_after_retention().await;
    }

    /// SYNC Steps: a timer and Channel WaitFor, a failed then retried Execute, then a locking Step.
    async fn order_waits_for_timer_and_channel_then_retries(&self) -> FlowRun {
        let order = self
            .start_flow(
                StartFlowRequest {
                    flow_id: "order-sync".to_owned(),
                    flow_type: "OrderFlow".to_owned(),
                    flow_timeout_seconds: 3600,
                    flow_timeout_policy: FlowTimeoutPolicy::Fail as i32,
                    start_step_type: "ChargeStep".to_owned(),
                    step_input: Some(string_value("order 1001")),
                    step_options: Some(StepOptions {
                        execute_retry_policy: Some(RetryPolicy {
                            initial_interval_seconds: 2,
                            backoff_coefficient: 2.0,
                            maximum_interval_seconds: 60,
                            maximum_attempts: 3,
                            ..RetryPolicy::default()
                        }),
                        ..StepOptions::default()
                    }),
                    flow_start_options: Some(FlowStartOptions {
                        attributes: vec![
                            attribute("status", string_value("new")),
                            attribute("profile", string_value(LONG_PROFILE_TEXT)),
                        ],
                        ..FlowStartOptions::default()
                    }),
                    request_id: "start-order-sync".to_owned(),
                },
                default_run_settings(),
            )
            .await;
        self.start_attempt(&order, "ChargeStep-1", StepMethod::WaitFor, 1)
            .await;
        self.complete_wait_for(
            &order,
            "ChargeStep-1",
            1,
            InvokeWaitForMethodResponse {
                waiting_condition: Some(WaitingCondition {
                    waiting_condition_type: WaitingConditionType::AllCompleted as i32,
                    timer_conditions: vec![TimerCondition {
                        condition_id: "settle".to_owned(),
                        duration_seconds: 30,
                        ..TimerCondition::default()
                    }],
                    channel_conditions: vec![ChannelCondition {
                        condition_id: "payment".to_owned(),
                        channel_name: "payments".to_owned(),
                        ..ChannelCondition::default()
                    }],
                    ..WaitingCondition::default()
                }),
                ..InvokeWaitForMethodResponse::default()
            },
        )
        .await;
        self.publish(
            &order,
            vec![
                channel_message("payments", "payment-1", "paid 42.00"),
                channel_message("audit", "audit-1", "charge requested"),
            ],
        )
        .await;
        self.fire_due_timers(&order, 31 * SECOND_MICROS).await;
        self.start_attempt(&order, "ChargeStep-1", StepMethod::Execute, 1)
            .await;
        self.commit(
            &order,
            Command::StepAttemptHeartbeat(StepAttemptHeartbeatCommand {
                invocation: Some(invocation("ChargeStep-1", StepMethod::Execute)),
                attempt: 1,
                checkpoint: Some(step_attempt_heartbeat_command::Checkpoint::Value(
                    string_value("authorized"),
                )),
            }),
        )
        .await;
        self.commit(
            &order,
            Command::StepAttemptFailed(StepAttemptFailedCommand {
                invocation: Some(invocation("ChargeStep-1", StepMethod::Execute)),
                attempt: 1,
                kind: StepAttemptFailureKind::WorkerError as i32,
                worker_error: Some(WorkerErrorResponse {
                    detail: "card network timed out".to_owned(),
                    error_type: "CardNetworkTimeout".to_owned(),
                    grpc_status: 14,
                    ..WorkerErrorResponse::default()
                }),
                ..StepAttemptFailedCommand::default()
            }),
        )
        .await;
        self.fire_due_timers(&order, 3 * SECOND_MICROS).await;
        self.start_attempt(&order, "ChargeStep-1", StepMethod::Execute, 2)
            .await;
        self.complete_execute(
            &order,
            "ChargeStep-1",
            2,
            InvokeExecuteMethodResponse {
                step_decision: Some(StepDecision {
                    next_steps: vec![StepMovement {
                        step_type: "ShipStep".to_owned(),
                        step_input: Some(string_value("ship order 1001")),
                        step_options: Some(StepOptions {
                            skip_wait_for: true,
                            execute_lock_attribute_keys: vec!["status".to_owned()],
                            ..StepOptions::default()
                        }),
                        ..StepMovement::default()
                    }],
                    ..StepDecision::default()
                }),
                upsert_attributes: vec![attribute("status", string_value("charged"))],
                record_events: vec![Kv {
                    key: "receipt".to_owned(),
                    value: Some(string_value("receipt 77")),
                }],
                publish_to_channel: vec![channel_message("audit", "audit-2", "charged")],
                ..InvokeExecuteMethodResponse::default()
            },
        )
        .await;
        let delete = DeleteChannelMessageRequest {
            flow_id: order.flow_id.clone(),
            channel_name: "audit".to_owned(),
            message_id: "audit-1".to_owned(),
            request_id: "delete-audit-1".to_owned(),
            ..DeleteChannelMessageRequest::default()
        };
        self.commit(
            &order,
            Command::ChannelMessageDelete(ChannelMessageDeleteCommand {
                request_fingerprint: request_fingerprint(&delete),
                request: Some(delete),
            }),
        )
        .await;
        // Kept to the end, so the recorded objects include a live hard-delete value.
        self.set_attributes(
            &order,
            vec![hard_delete_attribute(
                "payment-token",
                string_value("tok_4242"),
            )],
            "set-payment-token",
        )
        .await;
        order
    }

    /// A Channel WaitFor keeps the run open while hard-delete values are replaced.
    ///
    /// Returns the run and its current secret's object ID.
    async fn profile_rewrites_hard_delete_values(&self) -> (FlowRun, String) {
        let profile = self
            .start_flow(
                StartFlowRequest {
                    flow_id: "profile-review".to_owned(),
                    flow_type: "ProfileFlow".to_owned(),
                    start_step_type: "ReviewStep".to_owned(),
                    request_id: "start-profile-review".to_owned(),
                    ..StartFlowRequest::default()
                },
                default_run_settings(),
            )
            .await;
        self.start_attempt(&profile, "ReviewStep-1", StepMethod::WaitFor, 1)
            .await;
        self.complete_wait_for(
            &profile,
            "ReviewStep-1",
            1,
            InvokeWaitForMethodResponse {
                waiting_condition: Some(WaitingCondition {
                    waiting_condition_type: WaitingConditionType::AnyCompleted as i32,
                    channel_conditions: vec![ChannelCondition {
                        condition_id: "approval".to_owned(),
                        channel_name: "approvals".to_owned(),
                        ..ChannelCondition::default()
                    }],
                    ..WaitingCondition::default()
                }),
                ..InvokeWaitForMethodResponse::default()
            },
        )
        .await;
        let first_secret = self
            .set_attributes(
                &profile,
                vec![
                    hard_delete_attribute("secret", string_value("tax id 12-3456789")),
                    attribute("reviewer", string_value("unassigned")),
                ],
                "set-secret-1",
            )
            .await;
        let second_secret = self
            .set_attributes(
                &profile,
                vec![hard_delete_attribute(
                    "secret",
                    string_value("tax id 98-7654321"),
                )],
                "set-secret-2",
            )
            .await;
        self.delete_hard_delete_objects(&profile, &first_secret)
            .await;
        let current = second_secret
            .into_iter()
            .next()
            .expect("the second write uploads one object");
        (profile, current)
    }

    /// A parent WaitFor starts a child Flow in another partition.
    async fn parent_starts_sub_flow(&self) -> (FlowRun, FlowRun) {
        let parent = self
            .start_flow(
                StartFlowRequest {
                    flow_id: SUB_FLOW_PARENT.to_owned(),
                    flow_type: "ParentFlow".to_owned(),
                    start_step_type: "SpawnStep".to_owned(),
                    request_id: "start-parent-flow".to_owned(),
                    ..StartFlowRequest::default()
                },
                default_run_settings(),
            )
            .await;
        self.start_attempt(&parent, "SpawnStep-1", StepMethod::WaitFor, 1)
            .await;
        self.complete_wait_for(
            &parent,
            "SpawnStep-1",
            1,
            InvokeWaitForMethodResponse {
                waiting_condition: Some(WaitingCondition {
                    waiting_condition_type: WaitingConditionType::AllCompleted as i32,
                    sub_flow_conditions: vec![SubFlowCondition {
                        condition_id: "child".to_owned(),
                        sub_flow_type: "ChildFlow".to_owned(),
                        start_step_type: "ChildStep".to_owned(),
                        step_input: Some(string_value("child input")),
                        step_options: Some(skip_wait_for()),
                        ..SubFlowCondition::default()
                    }],
                    ..WaitingCondition::default()
                }),
                ..InvokeWaitForMethodResponse::default()
            },
        )
        .await;
        let start = self.single_outbox_record(&parent).await;
        let Some(outbox_record::Payload::ChildStart(child_start)) = start.payload.clone() else {
            panic!("the parent outbox holds a child start");
        };
        let child_flow_id = child_start.child_flow_id.clone();
        assert_ne!(
            self.context(&child_flow_id).partition,
            self.context(SUB_FLOW_PARENT).partition,
            "the child Flow lives in another partition"
        );
        let child_run_id = self.next_run_id();
        let source = child_start
            .parent
            .as_ref()
            .and_then(|reference| reference.parent.clone());
        let child_start_command = Command::InboxMessage(InboxMessageCommand {
            message_id: start.message_id.clone(),
            source,
            payload: Some(inbox_message_command::Payload::ChildStart(child_start)),
            new_run_id: child_run_id.as_bytes().to_vec(),
            settings: Some(default_run_settings()),
        });
        self.commit_to(&child_flow_id, &[], child_start_command)
            .await;
        self.commit(
            &parent,
            Command::OutboxAck(OutboxAckCommand {
                message_ids: vec![start.message_id],
            }),
        )
        .await;
        let child = FlowRun {
            flow_id: child_flow_id,
            run_id: child_run_id,
        };
        (parent, child)
    }

    async fn sub_flow_completes_and_wakes_parent(&self, parent: &FlowRun, child: &FlowRun) {
        self.start_attempt(child, "ChildStep-1", StepMethod::Execute, 1)
            .await;
        self.complete_execute(
            child,
            "ChildStep-1",
            1,
            close_response(CloseDecisionType::ForceComplete, "child done"),
        )
        .await;
        let completion = self.single_outbox_record(child).await;
        let Some(outbox_record::Payload::ChildCompleted(completed)) = completion.payload.clone()
        else {
            panic!("the child outbox holds its completion");
        };
        let source = dexzd_protocol::dexzd::v1::CrossFlowEndpoint {
            tenant_id: 0,
            flow_id: child.flow_id.clone(),
            run_id: completed.child_run_id.clone(),
            run_attempt: completed.child_run_attempt,
        };
        self.commit(
            parent,
            Command::InboxMessage(InboxMessageCommand {
                message_id: completion.message_id.clone(),
                source: Some(source),
                payload: Some(inbox_message_command::Payload::ChildCompleted(completed)),
                ..InboxMessageCommand::default()
            }),
        )
        .await;
        self.commit(
            child,
            Command::OutboxAck(OutboxAckCommand {
                message_ids: vec![completion.message_id],
            }),
        )
        .await;
        self.start_attempt(parent, "SpawnStep-1", StepMethod::Execute, 1)
            .await;
        self.complete_execute(
            parent,
            "SpawnStep-1",
            1,
            close_response(CloseDecisionType::GracefulComplete, "parent done"),
        )
        .await;
    }

    /// A locked transactional RPC writes the Attribute that the next Step then locks.
    async fn order_ships_under_lock_after_locked_rpc(&self, order: &FlowRun) {
        let rpc = InvokeRpcRequest {
            flow_id: order.flow_id.clone(),
            run_id: order.run_id.to_string(),
            rpc_name: "updateShippingAddress".to_owned(),
            input: Some(string_value("1 Main St")),
            lock_attribute_keys: vec!["status".to_owned()],
            request_id: "rpc-address-1".to_owned(),
            ..InvokeRpcRequest::default()
        };
        let outcome = self
            .commit(
                order,
                Command::RpcCommitted(RpcCommittedCommand {
                    request_fingerprint: request_fingerprint(&rpc),
                    request: Some(rpc),
                    response: Some(InvokeWorkerRpcResponse {
                        output: Some(string_value("address updated")),
                        upsert_attributes: vec![
                            attribute("address", string_value("1 Main St")),
                            attribute("status", string_value("address confirmed")),
                        ],
                        ..InvokeWorkerRpcResponse::default()
                    }),
                    app_worker_id: WORKER_ID.to_owned(),
                    is_transactional: true,
                    ..RpcCommittedCommand::default()
                }),
            )
            .await;
        assert!(
            matches!(outcome.reply.reply, Some(command_reply::Reply::Rpc(_))),
            "the RPC commits"
        );
        self.start_attempt(order, "ShipStep-1", StepMethod::Execute, 1)
            .await;
        self.complete_execute(
            order,
            "ShipStep-1",
            1,
            close_response(CloseDecisionType::ForceComplete, "shipped"),
        )
        .await;
    }

    /// ASYNC Step events wait for the batch window, so they share batches with prompt commands
    /// of another partition.
    async fn async_steps_share_batches_with_another_partition(&self, profile: &FlowRun) {
        let order = self
            .start_flow(
                StartFlowRequest {
                    flow_id: "order-async".to_owned(),
                    flow_type: "OrderFlow".to_owned(),
                    start_step_type: "ReserveStep".to_owned(),
                    step_options: Some(skip_wait_for()),
                    flow_start_options: Some(FlowStartOptions {
                        flow_config_override: Some(FlowConfig {
                            step_durability: Some(StepDurability::Async as i32),
                            ..FlowConfig::default()
                        }),
                        ..FlowStartOptions::default()
                    }),
                    request_id: "start-order-async".to_owned(),
                    ..StartFlowRequest::default()
                },
                default_run_settings(),
            )
            .await;
        assert_ne!(
            self.context(&order.flow_id).partition,
            self.context(&profile.flow_id).partition,
            "the ASYNC Flow and the profile Flow live in different partitions"
        );
        let batched = vec![
            self.submit(
                &order,
                self.start_attempt_command("ReserveStep-1", StepMethod::Execute, 1),
                CommitUrgency::Batched,
            ),
            self.submit(
                &order,
                execute_completed(
                    "ReserveStep-1",
                    1,
                    InvokeExecuteMethodResponse {
                        step_decision: Some(StepDecision {
                            next_steps: vec![StepMovement {
                                step_type: "NotifyStep".to_owned(),
                                step_options: Some(skip_wait_for()),
                                ..StepMovement::default()
                            }],
                            ..StepDecision::default()
                        }),
                        upsert_step_exe_locals: vec![Kv {
                            key: "reservation".to_owned(),
                            value: Some(string_value("bin 7")),
                        }],
                        ..InvokeExecuteMethodResponse::default()
                    },
                ),
                CommitUrgency::Batched,
            ),
            self.submit(
                &order,
                self.start_attempt_command("NotifyStep-1", StepMethod::Execute, 1),
                CommitUrgency::Batched,
            ),
            self.submit(
                profile,
                publish_command(vec![channel_message("approvals", "approval-1", "approved")]),
                CommitUrgency::Prompt,
            ),
        ];
        self.wait_applied(batched).await;
        let closing = vec![self.submit(
            &order,
            execute_completed(
                "NotifyStep-1",
                1,
                close_response(CloseDecisionType::GracefulComplete, "notified"),
            ),
            CommitUrgency::Batched,
        )];
        self.wait_applied(closing).await;
    }

    async fn profile_clears_hard_delete_value(&self, profile: &FlowRun, secret_object: &str) {
        self.start_attempt(profile, "ReviewStep-1", StepMethod::Execute, 1)
            .await;
        let outcome = self
            .complete_execute(
                profile,
                "ReviewStep-1",
                1,
                InvokeExecuteMethodResponse {
                    upsert_attributes: vec![
                        AttributeWrite {
                            key: "secret".to_owned(),
                            value: Some(null_value()),
                            is_hard_delete_on_null: true,
                            ..AttributeWrite::default()
                        },
                        attribute("reviewer", string_value("auditor 9")),
                    ],
                    step_decision: Some(force_complete("reviewed")),
                    ..InvokeExecuteMethodResponse::default()
                },
            )
            .await;
        assert!(
            outcome.effects.iter().any(|effect| matches!(
                effect,
                ApplyEffect::HardDeleteObjectsPending { object_ids, .. }
                    if object_ids == &vec![secret_object.to_owned()]
            )),
            "clearing the secret schedules its object for deletion"
        );
        self.delete_hard_delete_objects(profile, &[secret_object.to_owned()])
            .await;
    }

    /// Time travel to the second Step's Execute reopens the closed run in a new attempt.
    async fn travel_back_to_second_step(&self) {
        let travel = self
            .start_flow(
                StartFlowRequest {
                    flow_id: "travel-flow".to_owned(),
                    flow_type: "TravelFlow".to_owned(),
                    start_step_type: "FirstStep".to_owned(),
                    step_options: Some(skip_wait_for()),
                    request_id: "start-travel-flow".to_owned(),
                    ..StartFlowRequest::default()
                },
                default_run_settings(),
            )
            .await;
        self.start_attempt(&travel, "FirstStep-1", StepMethod::Execute, 1)
            .await;
        self.complete_execute(
            &travel,
            "FirstStep-1",
            1,
            InvokeExecuteMethodResponse {
                step_decision: Some(StepDecision {
                    next_steps: vec![StepMovement {
                        step_type: "SecondStep".to_owned(),
                        step_options: Some(skip_wait_for()),
                        ..StepMovement::default()
                    }],
                    ..StepDecision::default()
                }),
                upsert_attributes: vec![attribute("stage", string_value("first"))],
                ..InvokeExecuteMethodResponse::default()
            },
        )
        .await;
        self.set_attributes(
            &travel,
            vec![attribute("note", string_value("external write"))],
            "set-note-1",
        )
        .await;
        self.start_attempt(&travel, "SecondStep-1", StepMethod::Execute, 1)
            .await;
        self.complete_execute(
            &travel,
            "SecondStep-1",
            1,
            close_response(CloseDecisionType::ForceComplete, "first pass"),
        )
        .await;
        let outcome = self
            .commit(
                &travel,
                Command::TimeTravel(TimeTravelCommand {
                    request: Some(ResetFlowRequest {
                        flow_id: travel.flow_id.clone(),
                        reset_type: FlowResetType::StepExecutionId as i32,
                        reason: "rerun the second step".to_owned(),
                        step_execution_id: "SecondStep-1".to_owned(),
                        step_method: FlowResetStepMethod::Execute as i32,
                        skip_writes_reapply: false,
                        ..ResetFlowRequest::default()
                    }),
                }),
            )
            .await;
        assert!(
            matches!(
                outcome.reply.reply,
                Some(command_reply::Reply::TimeTravel(ref reply)) if reply.run_attempt == 2
            ),
            "time travel starts run attempt 2"
        );
        let (step_execution_id, attempt) = pending_execute(&outcome);
        self.start_attempt(&travel, &step_execution_id, StepMethod::Execute, attempt)
            .await;
        self.complete_execute(
            &travel,
            &step_execution_id,
            attempt,
            close_response(CloseDecisionType::ForceComplete, "second pass"),
        )
        .await;
    }

    /// TimerSkip, FlowConfigUpdate, and FlowStop on one waiting run.
    async fn skip_timer_update_config_and_stop(&self) {
        let idle = self
            .start_flow(
                StartFlowRequest {
                    flow_id: "idle-flow".to_owned(),
                    flow_type: "IdleFlow".to_owned(),
                    start_step_type: "IdleStep".to_owned(),
                    request_id: "start-idle-flow".to_owned(),
                    ..StartFlowRequest::default()
                },
                default_run_settings(),
            )
            .await;
        self.start_attempt(&idle, "IdleStep-1", StepMethod::WaitFor, 1)
            .await;
        self.complete_wait_for(
            &idle,
            "IdleStep-1",
            1,
            InvokeWaitForMethodResponse {
                waiting_condition: Some(WaitingCondition {
                    waiting_condition_type: WaitingConditionType::AllCompleted as i32,
                    timer_conditions: vec![TimerCondition {
                        condition_id: "nap".to_owned(),
                        duration_seconds: 600,
                        ..TimerCondition::default()
                    }],
                    ..WaitingCondition::default()
                }),
                ..InvokeWaitForMethodResponse::default()
            },
        )
        .await;
        self.commit(
            &idle,
            Command::TimerSkip(TimerSkipCommand {
                request: Some(SkipTimerRequest {
                    flow_id: idle.flow_id.clone(),
                    step_execution_id: "IdleStep-1".to_owned(),
                    timer_condition_index: Some(0),
                    ..SkipTimerRequest::default()
                }),
            }),
        )
        .await;
        self.commit(
            &idle,
            Command::FlowConfigUpdate(FlowConfigUpdateCommand {
                request: Some(UpdateFlowConfigRequest {
                    flow_id: idle.flow_id.clone(),
                    flow_config: Some(FlowConfig {
                        step_durability: Some(StepDurability::Async as i32),
                        ..FlowConfig::default()
                    }),
                    ..UpdateFlowConfigRequest::default()
                }),
            }),
        )
        .await;
        self.commit(
            &idle,
            Command::FlowStop(FlowStopCommand {
                request: Some(StopFlowRequest {
                    flow_id: idle.flow_id.clone(),
                    reason: "no longer needed".to_owned(),
                    stop_type: StopType::Cancel as i32,
                    ..StopFlowRequest::default()
                }),
            }),
        )
        .await;
    }

    /// A short retention fires after close, and the purge deletes the run.
    async fn purge_after_retention(&self) {
        let short_lived = self
            .start_flow(
                StartFlowRequest {
                    flow_id: "short-lived-flow".to_owned(),
                    flow_type: "ShortLivedFlow".to_owned(),
                    start_step_type: "OnlyStep".to_owned(),
                    step_options: Some(skip_wait_for()),
                    request_id: "start-short-lived-flow".to_owned(),
                    ..StartFlowRequest::default()
                },
                RunSettings {
                    retention_after_close_micros: 60 * SECOND_MICROS,
                    ..default_run_settings()
                },
            )
            .await;
        self.start_attempt(&short_lived, "OnlyStep-1", StepMethod::Execute, 1)
            .await;
        self.complete_execute(
            &short_lived,
            "OnlyStep-1",
            1,
            close_response(CloseDecisionType::ForceComplete, "done"),
        )
        .await;
        let fired = self.fire_due_timers(&short_lived, 61 * SECOND_MICROS).await;
        assert!(
            fired
                .effects
                .iter()
                .any(|effect| matches!(effect, ApplyEffect::RunPurgeRequested { .. })),
            "the retention timer requests the purge"
        );
        self.commit(&short_lived, Command::RunPurge(RunPurgeCommand {}))
            .await;
    }

    async fn start_flow(&self, request: StartFlowRequest, settings: RunSettings) -> FlowRun {
        let flow_id = request.flow_id.clone();
        let run_id = self.next_run_id();
        let resolved_timeout_policy =
            if request.flow_timeout_seconds > 0 && request.flow_timeout_policy == 0 {
                FlowTimeoutPolicy::Fail as i32
            } else {
                request.flow_timeout_policy
            };
        let outcome = self
            .commit_to(
                &flow_id,
                &[],
                Command::FlowStart(FlowStartCommand {
                    new_run_id: run_id.as_bytes().to_vec(),
                    request_fingerprint: request_fingerprint(&request),
                    request: Some(request),
                    settings: Some(settings),
                    resolved_timeout_policy,
                    worker_target: Some(WorkerTarget {
                        is_pull_mode: true,
                        ..WorkerTarget::default()
                    }),
                    hard_delete_objects: Vec::new(),
                }),
            )
            .await;
        assert!(
            matches!(
                outcome.reply.reply,
                Some(command_reply::Reply::StartFlow(ref reply))
                    if reply.run_id == run_id.as_bytes() && !reply.is_existing_run
            ),
            "{flow_id} starts a new run"
        );
        FlowRun { flow_id, run_id }
    }

    async fn start_attempt(
        &self,
        run: &FlowRun,
        step_execution_id: &str,
        method: StepMethod,
        attempt: u32,
    ) {
        let outcome = self
            .commit(
                run,
                self.start_attempt_command(step_execution_id, method, attempt),
            )
            .await;
        assert!(
            outcome
                .effects
                .iter()
                .any(|effect| matches!(effect, ApplyEffect::InvocationStarted { .. })),
            "{step_execution_id} attempt {attempt} starts"
        );
    }

    fn start_attempt_command(
        &self,
        step_execution_id: &str,
        method: StepMethod,
        attempt: u32,
    ) -> Command {
        Command::StepAttemptStarted(StepAttemptStartedCommand {
            invocation: Some(invocation(step_execution_id, method)),
            attempt,
            task_id: self.next_uuid().as_bytes().to_vec(),
            pull_routing: None,
        })
    }

    async fn complete_wait_for(
        &self,
        run: &FlowRun,
        step_execution_id: &str,
        attempt: u32,
        response: InvokeWaitForMethodResponse,
    ) -> ApplyOutcome {
        self.commit(
            run,
            Command::StepMethodCompleted(StepMethodCompletedCommand {
                invocation: Some(invocation(step_execution_id, StepMethod::WaitFor)),
                attempt,
                response: Some(step_method_completed_command::Response::WaitFor(response)),
                app_worker_id: WORKER_ID.to_owned(),
                ..StepMethodCompletedCommand::default()
            }),
        )
        .await
    }

    async fn complete_execute(
        &self,
        run: &FlowRun,
        step_execution_id: &str,
        attempt: u32,
        response: InvokeExecuteMethodResponse,
    ) -> ApplyOutcome {
        self.commit(run, execute_completed(step_execution_id, attempt, response))
            .await
    }

    /// Uploads hard-delete values as the runtime does and commits the writes.
    ///
    /// Returns the uploaded object IDs.
    async fn set_attributes(
        &self,
        run: &FlowRun,
        mut attributes: Vec<AttributeWrite>,
        request_id: &str,
    ) -> Vec<String> {
        let request = SetAttributesRequest {
            flow_id: run.flow_id.clone(),
            attributes: attributes.clone(),
            request_id: request_id.to_owned(),
            ..SetAttributesRequest::default()
        };
        let request_fingerprint = request_fingerprint(&request);
        let hard_delete_objects = self.upload_hard_delete_values(run, &mut attributes).await;
        let object_ids = hard_delete_objects
            .iter()
            .map(|reference| reference.object_id.clone())
            .collect();
        self.commit(
            run,
            Command::AttributesSet(AttributesSetCommand {
                request: Some(SetAttributesRequest {
                    attributes,
                    ..request
                }),
                hard_delete_objects,
                request_fingerprint,
            }),
        )
        .await;
        object_ids
    }

    async fn upload_hard_delete_values(
        &self,
        run: &FlowRun,
        attributes: &mut [AttributeWrite],
    ) -> Vec<HardDeleteObjectReference> {
        let owner = HardDeleteObjectOwner {
            tenant_id: TenantId::DEFAULT,
            flow_hash: self.hasher.flow_hash(TenantId::DEFAULT, &run.flow_id),
            run_id: run.run_id,
        };
        let mut references = Vec::new();
        for (write_index, write) in attributes.iter_mut().enumerate() {
            let is_string_value = match write.value.as_ref().and_then(|value| value.kind.as_ref()) {
                Some(value::Kind::StringValue(_)) if write.is_hard_delete_on_null => true,
                _ => continue,
            };
            let object_id = self.next_uuid().hyphenated().to_string();
            let body = encode_hard_delete_value(write.value.as_ref().expect("checked above"));
            let size_bytes = body.len() as u64;
            let outcome = self
                .object_store
                .put(
                    &owner.object_key(&object_id),
                    body,
                    WriteCondition::CreateOnly,
                )
                .await
                .expect("upload a hard-delete value");
            assert!(matches!(outcome, WriteOutcome::Written { .. }));
            write.value = Some(hard_delete_placeholder_value());
            references.push(HardDeleteObjectReference {
                write_index: u32::try_from(write_index).expect("few writes"),
                object_id,
                size_bytes,
                is_string_value,
            });
        }
        references
    }

    /// Deletes objects from object storage and commits the deletion, as the effect runtime does.
    async fn delete_hard_delete_objects(&self, run: &FlowRun, object_ids: &[String]) {
        let owner = HardDeleteObjectOwner {
            tenant_id: TenantId::DEFAULT,
            flow_hash: self.hasher.flow_hash(TenantId::DEFAULT, &run.flow_id),
            run_id: run.run_id,
        };
        let keys: Vec<String> = object_ids
            .iter()
            .map(|object_id| owner.object_key(object_id))
            .collect();
        self.object_store
            .delete_many(&keys)
            .await
            .expect("delete hard-delete objects");
        self.commit(
            run,
            Command::HardDeleteObjectsDeleted(HardDeleteObjectsDeletedCommand {
                object_ids: object_ids.to_vec(),
            }),
        )
        .await;
    }

    async fn publish(&self, run: &FlowRun, messages: Vec<ChannelMessage>) {
        self.commit(run, publish_command(messages)).await;
    }

    /// Advances the clock by `elapsed_micros` and fires every timer of the run that is due.
    async fn fire_due_timers(&self, run: &FlowRun, elapsed_micros: i64) -> ApplyOutcome {
        self.clock.advance(elapsed_micros);
        let now = self.clock.now_unix_micros();
        let timers: Vec<_> = self
            .run_rows::<TimerRecord>(run, RunTable::Timer)
            .await
            .into_iter()
            .filter(|timer| timer.fire_at_micros <= now)
            .filter_map(|timer| timer.timer_id)
            .collect();
        assert!(!timers.is_empty(), "a timer of {} is due", run.flow_id);
        assert!(
            timers
                .iter()
                .all(|timer| !matches!(timer.kind, Some(timer_id::Kind::FlowTimeout(_)))),
            "the Flow timeout never fires in this workload"
        );
        self.commit(run, Command::TimersFired(TimersFiredCommand { timers }))
            .await
    }

    async fn single_outbox_record(&self, run: &FlowRun) -> OutboxRecord {
        let mut records: Vec<OutboxRecord> = self
            .run_rows::<OutboxRecord>(run, RunTable::Outbox)
            .await
            .into_iter()
            .filter(|record| record.acknowledged_at_micros == 0)
            .collect();
        assert_eq!(
            records.len(),
            1,
            "{} has one unacknowledged message",
            run.flow_id
        );
        records.remove(0)
    }

    async fn run_rows<RowMessage: Message + Default>(
        &self,
        run: &FlowRun,
        table: RunTable,
    ) -> Vec<RowMessage> {
        let flow_hash = self.hasher.flow_hash(TenantId::DEFAULT, &run.flow_id);
        let (start, end) = prefix_range(&run_table_prefix(flow_hash, run.run_id, table));
        let view = self
            .node
            .committer()
            .committed_view()
            .expect("open a committed view");
        let rows = tokio::task::spawn_blocking(move || {
            view.scan(ColumnFamily::Flow, &start, &end, usize::MAX)
                .expect("scan committed rows")
        })
        .await
        .expect("the scan task completes");
        rows.into_iter()
            .map(|(_, value)| RowMessage::decode(value.as_slice()).expect("committed rows decode"))
            .collect()
    }

    async fn commit(&self, run: &FlowRun, body: Command) -> ApplyOutcome {
        self.commit_to(&run.flow_id, run.run_id.as_bytes(), body)
            .await
    }

    async fn commit_to(&self, flow_id: &str, run_id: &[u8], body: Command) -> ApplyOutcome {
        let ticket = self.submit_to(flow_id, run_id, body, CommitUrgency::Prompt);
        let mut outcomes = self.wait_applied(vec![ticket]).await;
        outcomes.remove(0)
    }

    fn submit(&self, run: &FlowRun, body: Command, urgency: CommitUrgency) -> CommitTicket {
        self.submit_to(&run.flow_id, run.run_id.as_bytes(), body, urgency)
    }

    fn submit_to(
        &self,
        flow_id: &str,
        run_id: &[u8],
        body: Command,
        urgency: CommitUrgency,
    ) -> CommitTicket {
        self.clock.advance(COMMAND_SPACING_MICROS);
        let command = FlowCommand {
            flow_id: flow_id.to_owned(),
            run_id: run_id.to_vec(),
            command: Some(body),
            ..FlowCommand::default()
        };
        let (_, ticket) = self
            .node
            .committer()
            .submit(self.context(flow_id), command, urgency)
            .expect("the node accepts the command");
        ticket
    }

    /// Waits for every ticket and requires each command to apply with writes.
    async fn wait_applied(&self, tickets: Vec<CommitTicket>) -> Vec<ApplyOutcome> {
        let mut outcomes = Vec::with_capacity(tickets.len());
        for ticket in tickets {
            let CommittedCommand { outcome, position } =
                ticket.wait().await.expect("the command commits");
            assert!(
                !matches!(outcome.reply.reply, Some(command_reply::Reply::Error(_))),
                "the command in batch {position:?} applies: {:?}",
                outcome.reply
            );
            assert!(
                !outcome.writes.is_empty(),
                "the command in batch {position:?} changes state"
            );
            outcomes.push(outcome);
        }
        outcomes
    }

    fn context(&self, flow_id: &str) -> ApplyContext {
        let flow_hash = self.hasher.flow_hash(TenantId::DEFAULT, flow_id);
        ApplyContext {
            partition: self.hasher.partition_of(flow_hash),
            flow_hash,
        }
    }

    fn next_run_id(&self) -> RunId {
        RunId(self.next_uuid().into_bytes())
    }

    fn next_uuid(&self) -> uuid::Uuid {
        self.ids.next_uuid_v7()
    }
}

/// Returns the 16-byte big-endian xxh3-128 of the request, as the runtime computes it.
fn request_fingerprint(request: &impl Message) -> Vec<u8> {
    xxh3_128(&request.encode_to_vec()).to_be_bytes().to_vec()
}

fn pending_execute(outcome: &ApplyOutcome) -> (String, u32) {
    outcome
        .effects
        .iter()
        .find_map(|effect| match effect {
            ApplyEffect::InvocationPending {
                invocation,
                attempt,
                ..
            } if invocation.method == StepMethod::Execute as i32 => {
                Some((invocation.step_execution_id.clone(), *attempt))
            }
            _ => None,
        })
        .expect("time travel leaves an Execute pending")
}

fn execute_completed(
    step_execution_id: &str,
    attempt: u32,
    response: InvokeExecuteMethodResponse,
) -> Command {
    Command::StepMethodCompleted(StepMethodCompletedCommand {
        invocation: Some(invocation(step_execution_id, StepMethod::Execute)),
        attempt,
        response: Some(step_method_completed_command::Response::Execute(response)),
        app_worker_id: WORKER_ID.to_owned(),
        ..StepMethodCompletedCommand::default()
    })
}

fn publish_command(messages: Vec<ChannelMessage>) -> Command {
    Command::ChannelPublish(ChannelPublishCommand {
        request: Some(PublishToChannelRequest {
            messages,
            ..PublishToChannelRequest::default()
        }),
    })
}

fn close_response(
    close_decision_type: CloseDecisionType,
    output: &str,
) -> InvokeExecuteMethodResponse {
    InvokeExecuteMethodResponse {
        step_decision: Some(StepDecision {
            close_decision: Some(CloseDecision {
                close_decision_type: close_decision_type as i32,
                close_input: Some(string_value(output)),
                ..CloseDecision::default()
            }),
            ..StepDecision::default()
        }),
        ..InvokeExecuteMethodResponse::default()
    }
}

fn force_complete(output: &str) -> StepDecision {
    close_response(CloseDecisionType::ForceComplete, output)
        .step_decision
        .expect("the close response has a decision")
}

fn invocation(step_execution_id: &str, method: StepMethod) -> StepInvocationKey {
    StepInvocationKey {
        step_execution_id: step_execution_id.to_owned(),
        method: method as i32,
    }
}

fn skip_wait_for() -> StepOptions {
    StepOptions {
        skip_wait_for: true,
        ..StepOptions::default()
    }
}

fn attribute(key: &str, value: Value) -> AttributeWrite {
    AttributeWrite {
        key: key.to_owned(),
        value: Some(value),
        ..AttributeWrite::default()
    }
}

fn hard_delete_attribute(key: &str, value: Value) -> AttributeWrite {
    AttributeWrite {
        is_hard_delete_on_null: true,
        ..attribute(key, value)
    }
}

fn channel_message(channel_name: &str, message_id: &str, text: &str) -> ChannelMessage {
    ChannelMessage {
        channel_name: channel_name.to_owned(),
        value: Some(string_value(text)),
        message_id: message_id.to_owned(),
    }
}

fn string_value(text: &str) -> Value {
    Value {
        kind: Some(value::Kind::StringValue(text.to_owned())),
    }
}

fn null_value() -> Value {
    Value {
        kind: Some(value::Kind::NullValue(0)),
    }
}

/// Puts `body` at `key`; used for metadata the workload records directly.
pub(crate) async fn put_new_object(store: &InMemoryObjectStore, key: &str, body: Vec<u8>) {
    let outcome = store
        .put(key, Bytes::from(body), WriteCondition::CreateOnly)
        .await
        .expect("write a recorded object");
    assert!(
        matches!(outcome, WriteOutcome::Written { .. }),
        "{key} is new"
    );
}
