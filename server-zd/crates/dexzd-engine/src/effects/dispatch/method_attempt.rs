// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! One running method attempt: reads its snapshot, dispatches it, follows Worker frames and the
//! owner-memory method and heartbeat timeouts, and proposes its result or failure.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dexzd_keyspace::{ColumnFamily, RunTable, TenantId, run_key};
use dexzd_protocol::dex::{
    self, DispatchTaskErrorReason, PollWorkerTaskFrame, PullRouting, PullRoutingSource, Value,
    WorkerErrorResponse, invoke_execute_method_output, invoke_wait_for_method_output,
    poll_worker_task_frame,
};
use dexzd_protocol::dexzd::v1::{
    FlowCommand, HardDeleteObjectRecord, StepAttemptFailedCommand, StepAttemptFailureKind,
    StepAttemptHeartbeatCommand, StepMethod, flow_command, step_attempt_heartbeat_command,
};
use dexzd_worker_dispatch::{DispatchEvent, DispatchOutcome};
use tokio::time::{Instant, sleep_until};
use tokio_util::sync::CancellationToken;

use crate::effects::dispatch::MethodDispatchRequest;
use crate::effects::dispatch::worker_result::{
    MethodResult, WorkerResultCheck, check_worker_result,
};
use crate::effects::effect_runtime::{EffectFlow, EffectUrgency};
use crate::effects::hard_delete_objects::HardDeleteObjectOwner;
use crate::effects::method_request::{
    MethodAttemptKey, MethodAttemptState, MethodRequest, build_method_request,
    read_method_attempt_state, resolve_method_durability,
};
use crate::effects::state_reads::{HARD_DELETE_CURRENT_OBJECT_TAG, RunLocation, decode_row};
use crate::effects::{EffectContext, EffectError};

const DEFAULT_METHOD_TIMEOUT: Duration = Duration::from_secs(2 * 60 * 60);
const DEFAULT_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(60);

/// A method attempt task's inputs.
pub(crate) struct MethodAttempt {
    pub(crate) context: Arc<EffectContext>,
    pub(crate) flow: EffectFlow,
    pub(crate) run: RunLocation,
    pub(crate) key: MethodAttemptKey,
    pub(crate) task_id: Vec<u8>,
    /// Canceled by `InvocationCanceled` or node shutdown.
    pub(crate) cancel: CancellationToken,
}

/// How the attempt's dispatch ended, before it becomes a command.
enum AttemptEnd {
    Result(Box<PollWorkerTaskFrame>, String),
    Failure(AttemptFailure),
    Canceled,
}

struct AttemptFailure {
    kind: StepAttemptFailureKind,
    worker_error: Option<WorkerErrorResponse>,
    server_detail: String,
    is_non_retryable: bool,
    served_by_app_worker_id: String,
}

impl AttemptFailure {
    fn server(kind: StepAttemptFailureKind, detail: impl Into<String>) -> Self {
        Self {
            kind,
            worker_error: None,
            server_detail: detail.into(),
            is_non_retryable: false,
            served_by_app_worker_id: String::new(),
        }
    }
}

impl MethodAttempt {
    /// Runs the attempt to its end. Errors are logged; the attempt is rebuilt after a restart.
    pub(crate) async fn run(self) {
        if let Err(error) = self.run_attempt().await {
            tracing::warn!(
                %error,
                run_id = %self.key.run_id,
                step_execution_id = %self.key.step_execution_id,
                attempt = self.key.attempt,
                "method attempt stopped with an error"
            );
        }
    }

    async fn run_attempt(&self) -> Result<(), EffectError> {
        let state = {
            let reader = self.context.runtime.flow_state_reader(&self.flow, true);
            read_method_attempt_state(reader.as_ref(), self.run, &self.key, &self.task_id)?
        };
        let Some(state) = state else {
            tracing::debug!(run_id = %self.key.run_id, "method attempt is no longer running");
            return Ok(());
        };
        let urgency = match resolve_method_durability(
            &state.run_record,
            &state.step_execution,
            self.key.method,
        ) {
            dex::StepDurability::Async => EffectUrgency::Batched,
            _ => EffectUrgency::Prompt,
        };
        let hard_delete_values = self.read_hard_delete_values(&state).await?;
        let request = build_method_request(&state, &hard_delete_values)?;
        let end = if request.encoded_len() > self.context.config.max_worker_request_bytes {
            AttemptEnd::Failure(AttemptFailure::server(
                StepAttemptFailureKind::RequestTooLarge,
                format!(
                    "Worker request of {} bytes exceeds the {} byte message limit",
                    request.encoded_len(),
                    self.context.config.max_worker_request_bytes
                ),
            ))
        } else {
            self.dispatch_and_follow(&state, request, urgency).await
        };
        let sticky_app_worker_id = state.run_record.sticky_app_worker_id.clone();
        match end {
            AttemptEnd::Canceled => Ok(()),
            AttemptEnd::Failure(failure) => {
                self.propose_failure(failure, &sticky_app_worker_id, urgency)
                    .await;
                Ok(())
            }
            AttemptEnd::Result(frame, app_worker_id) => {
                self.complete(&state, *frame, app_worker_id, urgency).await
            }
        }
    }

    /// Reads hard-delete Attribute values; a deleted object is retried once with the current
    /// object of its Attribute, then omitted.
    async fn read_hard_delete_values(
        &self,
        state: &MethodAttemptState,
    ) -> Result<BTreeMap<String, Value>, EffectError> {
        let owner = self.object_owner(state);
        let mut values = BTreeMap::new();
        for (attribute_key, object) in state.hard_delete_attributes() {
            let objects = &self.context.hard_delete_objects;
            if let Some(value) = objects.read_value(owner, &object.object_id).await? {
                values.insert(attribute_key, value);
                continue;
            }
            let current_object_id = self.current_hard_delete_object_id(&attribute_key)?;
            if let Some(object_id) = current_object_id.filter(|id| *id != object.object_id)
                && let Some(value) = objects.read_value(owner, &object_id).await?
            {
                values.insert(attribute_key, value);
            }
        }
        Ok(values)
    }

    fn current_hard_delete_object_id(
        &self,
        attribute_key: &str,
    ) -> Result<Option<String>, EffectError> {
        let mut suffix = vec![HARD_DELETE_CURRENT_OBJECT_TAG];
        suffix.extend_from_slice(attribute_key.as_bytes());
        let key = run_key(
            self.run.flow_hash,
            self.run.run_id,
            RunTable::HardDeleteObject,
            &suffix,
        );
        let reader = self.context.runtime.flow_state_reader(&self.flow, false);
        let Some(bytes) = reader.get(ColumnFamily::Flow, &key)? else {
            return Ok(None);
        };
        let record: HardDeleteObjectRecord = decode_row("HardDeleteObject", &key, &bytes)?;
        Ok(record.object.map(|object| object.object_id))
    }

    fn object_owner(&self, state: &MethodAttemptState) -> HardDeleteObjectOwner {
        HardDeleteObjectOwner {
            tenant_id: TenantId(state.run_record.tenant_id),
            flow_hash: self.run.flow_hash,
            run_id: self.run.run_id,
        }
    }

    async fn dispatch_and_follow(
        &self,
        state: &MethodAttemptState,
        request: MethodRequest,
        urgency: EffectUrgency,
    ) -> AttemptEnd {
        let (method_timeout, heartbeat_timeout) = method_timeouts(state, self.key.method);
        let started = Instant::now();
        let method_deadline = started + method_timeout;
        let wall_deadline = unix_micros_to_system_time(
            self.context
                .clock
                .now_unix_micros()
                .saturating_add(i64::try_from(method_timeout.as_micros()).unwrap_or(i64::MAX)),
        );
        let sticky = &state.run_record.sticky_app_worker_id;
        let dispatch_request = MethodDispatchRequest {
            tenant_id: state.run_record.tenant_id,
            flow_id: state.run_record.flow_id.clone(),
            flow_type: state.run_record.flow_type.clone(),
            sticky_app_worker_id: (!sticky.is_empty()).then(|| sticky.clone()),
            request,
            deadline: wall_deadline,
        };
        let mut session = match self.context.dispatcher.dispatch(dispatch_request) {
            Ok(session) => session,
            Err(error) => {
                return AttemptEnd::Failure(AttemptFailure::server(
                    StepAttemptFailureKind::DispatchError,
                    error.detail,
                ));
            }
        };
        // Matching may wait for a Worker before the Worker can heartbeat.
        let mut heartbeat_deadline = started + self.context.config.dispatch_wait + heartbeat_timeout;
        let mut checkpoints = CheckpointTracker::new(state.invocation().heartbeat_checkpoint.is_some(), {
            let initial = build_method_request(state, &BTreeMap::new())
                .ok()
                .and_then(|request| match request {
                    MethodRequest::WaitFor(request) => request.context,
                    MethodRequest::Execute(request) => request.context,
                })
                .and_then(|context| context.last_heartbeat_value);
            initial
        });
        loop {
            let flush_at = checkpoints.flush_at;
            tokio::select! {
                () = self.cancel.cancelled() => {
                    session.cancel();
                    return AttemptEnd::Canceled;
                }
                () = sleep_until(method_deadline) => {
                    session.cancel();
                    return AttemptEnd::Failure(AttemptFailure::server(
                        StepAttemptFailureKind::MethodTimeout,
                        format!("method timeout of {}s expired", method_timeout.as_secs()),
                    ));
                }
                () = sleep_until(heartbeat_deadline) => {
                    session.cancel();
                    return AttemptEnd::Failure(AttemptFailure::server(
                        StepAttemptFailureKind::HeartbeatTimeout,
                        format!("no heartbeat within {}s", heartbeat_timeout.as_secs()),
                    ));
                }
                () = sleep_until_optional(flush_at) => {
                    if let Some(checkpoint) = checkpoints.take_pending() {
                        self.propose_heartbeat(checkpoint, urgency).await;
                    }
                }
                event = session.next_event() => {
                    match event {
                        None => {
                            return AttemptEnd::Failure(AttemptFailure::server(
                                StepAttemptFailureKind::DispatchError,
                                "dispatch ended without an outcome",
                            ));
                        }
                        Some(DispatchEvent::Output(frame)) => {
                            heartbeat_deadline = Instant::now() + heartbeat_timeout;
                            if let Some(change) = heartbeat_change(&frame) {
                                checkpoints.observe(change, self.context.config.heartbeat_batch_window);
                            }
                        }
                        Some(DispatchEvent::Completed(outcome)) => {
                            if let Some(checkpoint) = checkpoints.take_pending() {
                                self.propose_heartbeat(checkpoint, urgency).await;
                            }
                            return attempt_end(outcome);
                        }
                    }
                }
            }
        }
    }

    async fn propose_heartbeat(&self, checkpoint: Option<Value>, urgency: EffectUrgency) {
        let checkpoint = match checkpoint {
            Some(value) => step_attempt_heartbeat_command::Checkpoint::Value(value),
            None => step_attempt_heartbeat_command::Checkpoint::IsCleared(true),
        };
        let command = self.flow_command(flow_command::Command::StepAttemptHeartbeat(
            StepAttemptHeartbeatCommand {
                invocation: Some(self.key.invocation()),
                attempt: self.key.attempt,
                checkpoint: Some(checkpoint),
            },
        ));
        // A lost checkpoint only means the next attempt sees an older value.
        if let Err(error) = self
            .context
            .runtime
            .propose_speculative(&self.flow, command, urgency)
            .await
        {
            tracing::warn!(%error, run_id = %self.key.run_id, "proposing a heartbeat failed");
        }
    }

    async fn complete(
        &self,
        state: &MethodAttemptState,
        frame: PollWorkerTaskFrame,
        app_worker_id: String,
        urgency: EffectUrgency,
    ) -> Result<(), EffectError> {
        let sticky_app_worker_id = state.run_record.sticky_app_worker_id.clone();
        let max_value_bytes = match state.run_record.settings.as_ref() {
            Some(settings) if settings.max_value_bytes > 0 => settings.max_value_bytes,
            _ => self.context.config.default_max_value_bytes,
        };
        let check = check_worker_result(
            frame,
            self.key.method,
            &self.context.validation_config,
            max_value_bytes,
        );
        let mut result = match check {
            WorkerResultCheck::Valid(result) => result,
            WorkerResultCheck::Invalid {
                kind,
                detail,
                is_non_retryable,
            } => {
                let failure = AttemptFailure {
                    kind,
                    worker_error: None,
                    server_detail: detail,
                    is_non_retryable,
                    served_by_app_worker_id: app_worker_id,
                };
                self.propose_failure(failure, &sticky_app_worker_id, urgency)
                    .await;
                return Ok(());
            }
        };
        let uploads = self
            .context
            .hard_delete_objects
            .upload_attribute_writes(self.object_owner(state), result.attribute_writes_mut())
            .await?;
        let command = self.flow_command(flow_command::Command::StepMethodCompleted(
            result.into_command(
                self.key.invocation(),
                self.key.attempt,
                app_worker_id.clone(),
                uploads.references.clone(),
                pull_routing(&sticky_app_worker_id, &app_worker_id),
            ),
        ));
        let reply = self
            .context
            .propose_until_committed(&self.flow, command, urgency)
            .await;
        let is_committed = reply.as_ref().is_some_and(|reply| !reply.is_error());
        if reply.is_some() || !uploads.is_empty() {
            // Without a reply the node is stopping; recovery finishes the recorded uploads.
            if reply.is_some() {
                self.context
                    .hard_delete_objects
                    .finish_uploads(uploads, is_committed)
                    .await?;
            }
        }
        Ok(())
    }

    async fn propose_failure(
        &self,
        failure: AttemptFailure,
        sticky_app_worker_id: &str,
        urgency: EffectUrgency,
    ) {
        tracing::info!(
            run_id = %self.key.run_id,
            step_execution_id = %self.key.step_execution_id,
            attempt = self.key.attempt,
            kind = ?failure.kind,
            detail = %failure.server_detail,
            "method attempt failed"
        );
        let command = self.flow_command(flow_command::Command::StepAttemptFailed(
            StepAttemptFailedCommand {
                invocation: Some(self.key.invocation()),
                attempt: self.key.attempt,
                kind: failure.kind as i32,
                worker_error: failure.worker_error,
                server_detail: failure.server_detail,
                is_non_retryable: failure.is_non_retryable,
                pull_routing: Some(pull_routing(
                    sticky_app_worker_id,
                    &failure.served_by_app_worker_id,
                )),
            },
        ));
        // A stale failure is rejected by apply; nothing else to do with the reply.
        let _reply = self
            .context
            .propose_until_committed(&self.flow, command, urgency)
            .await;
    }

    fn flow_command(&self, command: flow_command::Command) -> FlowCommand {
        FlowCommand {
            flow_id: self.flow.flow_id.clone(),
            run_id: self.key.run_id.as_bytes().to_vec(),
            expected_run_attempt: self.key.run_attempt,
            tenant_id: self.flow.tenant_id.0,
            command: Some(command),
            ..FlowCommand::default()
        }
    }
}

/// Returns the attempt's method timeout and heartbeat timeout, with the Go Server defaults.
fn method_timeouts(state: &MethodAttemptState, method: i32) -> (Duration, Duration) {
    let options = state.step_execution.options.clone().unwrap_or_default();
    let method_timeout_seconds = if method == StepMethod::WaitFor as i32 {
        options.wait_for_timeout_seconds
    } else {
        options.execute_timeout_seconds
    };
    let method_timeout = positive_seconds(method_timeout_seconds).unwrap_or(DEFAULT_METHOD_TIMEOUT);
    let heartbeat_timeout =
        positive_seconds(options.heartbeat_timeout_seconds).unwrap_or(DEFAULT_HEARTBEAT_TIMEOUT);
    (method_timeout, heartbeat_timeout)
}

fn positive_seconds(seconds: i32) -> Option<Duration> {
    u64::try_from(seconds)
        .ok()
        .filter(|seconds| *seconds > 0)
        .map(Duration::from_secs)
}

/// Converts a dispatch outcome into the attempt's end.
fn attempt_end(outcome: DispatchOutcome) -> AttemptEnd {
    match outcome {
        DispatchOutcome::WorkerResult {
            frame,
            app_worker_id,
        } => AttemptEnd::Result(Box::new(frame), app_worker_id),
        DispatchOutcome::WorkerError {
            frame,
            app_worker_id,
        } => {
            let worker_error = match frame.output {
                Some(poll_worker_task_frame::Output::WorkerError(error)) => error,
                _ => WorkerErrorResponse {
                    detail: "Worker returned an error frame without an error".to_owned(),
                    ..WorkerErrorResponse::default()
                },
            };
            AttemptEnd::Failure(AttemptFailure {
                kind: StepAttemptFailureKind::WorkerError,
                server_detail: String::new(),
                worker_error: Some(worker_error),
                is_non_retryable: false,
                served_by_app_worker_id: app_worker_id,
            })
        }
        DispatchOutcome::DispatchError { reason, detail } => {
            let reason_name = DispatchTaskErrorReason::as_str_name(&reason);
            AttemptEnd::Failure(AttemptFailure::server(
                StepAttemptFailureKind::DispatchError,
                format!("{reason_name}: {detail}"),
            ))
        }
        DispatchOutcome::Cancelled => AttemptEnd::Canceled,
        DispatchOutcome::DeadlineExceeded => AttemptEnd::Failure(AttemptFailure::server(
            StepAttemptFailureKind::MethodTimeout,
            "method deadline elapsed before the Worker result",
        )),
    }
}

/// A checkpoint change carried by a Worker frame: `Some(value)` sets it, `None` clears it.
type CheckpointChange = Option<Value>;

/// Returns the checkpoint change of an explicit heartbeat; stream writes keep the checkpoint.
fn heartbeat_change(frame: &PollWorkerTaskFrame) -> Option<CheckpointChange> {
    match frame.output.as_ref()? {
        poll_worker_task_frame::Output::WaitFor(output) => match output.output.as_ref()? {
            invoke_wait_for_method_output::Output::Heartbeat(heartbeat) => {
                Some(heartbeat.value.clone())
            }
            _ => None,
        },
        poll_worker_task_frame::Output::Execute(output) => match output.output.as_ref()? {
            invoke_execute_method_output::Output::Heartbeat(heartbeat) => {
                Some(heartbeat.value.clone())
            }
            _ => None,
        },
        _ => None,
    }
}

/// Coalesces checkpoint changes within the batch window and proposes only real changes.
struct CheckpointTracker {
    last_proposed: CheckpointChange,
    pending: Option<CheckpointChange>,
    flush_at: Option<Instant>,
}

impl CheckpointTracker {
    fn new(has_checkpoint: bool, initial: CheckpointChange) -> Self {
        Self {
            last_proposed: if has_checkpoint { initial } else { None },
            pending: None,
            flush_at: None,
        }
    }

    fn observe(&mut self, change: CheckpointChange, batch_window: Duration) {
        if change == self.last_proposed {
            self.pending = None;
            self.flush_at = None;
            return;
        }
        self.pending = Some(change);
        self.flush_at.get_or_insert_with(|| Instant::now() + batch_window);
    }

    fn take_pending(&mut self) -> Option<CheckpointChange> {
        self.flush_at = None;
        let change = self.pending.take()?;
        self.last_proposed = change.clone();
        Some(change)
    }
}

async fn sleep_until_optional(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

fn pull_routing(sticky_app_worker_id: &str, served_by_app_worker_id: &str) -> PullRouting {
    let source = if served_by_app_worker_id.is_empty() {
        PullRoutingSource::Unspecified
    } else if served_by_app_worker_id == sticky_app_worker_id {
        PullRoutingSource::StickyQueue
    } else {
        PullRoutingSource::FlowTypeQueue
    };
    PullRouting {
        sticky_app_worker_id: sticky_app_worker_id.to_owned(),
        served_by_app_worker_id: served_by_app_worker_id.to_owned(),
        source: source as i32,
    }
}

fn unix_micros_to_system_time(micros: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_micros(u64::try_from(micros).unwrap_or(0))
}

/// Whether a reply rejected the command.
trait ReplyOutcome {
    fn is_error(&self) -> bool;
}

impl ReplyOutcome for dexzd_protocol::dexzd::v1::CommandReply {
    fn is_error(&self) -> bool {
        matches!(
            self.reply,
            Some(dexzd_protocol::dexzd::v1::command_reply::Reply::Error(_))
        )
    }
}

impl MethodResult {
    fn attribute_writes_mut(&mut self) -> &mut [dex::AttributeWrite] {
        match self {
            Self::WaitFor(response) => &mut response.upsert_attributes,
            Self::Execute(response) => &mut response.upsert_attributes,
        }
    }
}
