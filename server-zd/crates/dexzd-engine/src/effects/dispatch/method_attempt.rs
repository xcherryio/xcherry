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
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dexzd_keyspace::row_keys::hard_delete_current_key;
use dexzd_keyspace::{ColumnFamily, TenantId};
use dexzd_protocol::dex::{
    self, DispatchTaskErrorReason, PollWorkerTaskFrame, PullRouting, PullRoutingSource,
    StepStreamWrite, Value, WorkerErrorResponse, invoke_execute_method_output,
    invoke_wait_for_method_output, poll_worker_task_frame,
};
use dexzd_protocol::dexzd::v1::{
    FlowCommand, HardDeleteObjectRecord, StepAttemptFailedCommand, StepAttemptFailureKind,
    StepAttemptHeartbeatCommand, flow_command, step_attempt_heartbeat_command,
};
use dexzd_stream_store::WriteStreamInput;
use dexzd_worker_dispatch::{DispatchEvent, DispatchOutcome};
use tokio::time::{Instant, sleep_until};
use tokio_util::sync::CancellationToken;

use crate::effects::dispatch::MethodDispatchRequest;
use crate::effects::dispatch::worker_result::{
    MethodResult, WorkerResultCheck, check_worker_result,
};
use crate::effects::hard_delete_objects::HardDeleteObjectOwner;
use crate::effects::method_request::{
    MethodAttemptKey, MethodAttemptState, MethodRequest, build_method_request,
    heartbeat_checkpoint_value, read_method_attempt_state, resolve_method_durability,
};
use crate::effects::state_reads::{RunLocation, decode_row};
use crate::effects::{EffectContext, EffectError};
use crate::runtime::{CommitUrgency, FlowAddress};

const DEFAULT_METHOD_TIMEOUT: Duration = Duration::from_secs(2 * 60 * 60);
const DEFAULT_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(60);

/// A method attempt task's inputs.
pub(crate) struct MethodAttempt {
    pub(crate) context: Arc<EffectContext>,
    pub(crate) flow: FlowAddress,
    pub(crate) run: RunLocation,
    pub(crate) key: MethodAttemptKey,
    pub(crate) task_id: Vec<u8>,
    /// The method's configured timeout in seconds from the start effect; 0 means the default.
    pub(crate) method_timeout_seconds: i32,
    /// Canceled by `InvocationCanceled` or node shutdown.
    pub(crate) cancel: CancellationToken,
}

/// How the attempt's dispatch ended, before it becomes a command.
enum AttemptEnd {
    Result(Box<PollWorkerTaskFrame>, String),
    Failure(AttemptFailure),
    Canceled,
}

/// Outcome of preparing the Worker request.
enum PreparedRequest {
    Ready(Box<MethodRequest>),
    Ended(AttemptEnd),
}

/// Outcome of work raced against the method deadline and cancellation.
enum BeforeDeadline<T> {
    Done(T),
    Canceled,
    TimedOut,
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
    /// Runs the attempt to its end. A server-side error fails the attempt so that it retries;
    /// otherwise the invocation would stay running until a restart.
    pub(crate) async fn run(self) {
        let Err(error) = self.run_attempt().await else {
            return;
        };
        if matches!(error, EffectError::Stopped) || self.cancel.is_cancelled() {
            return;
        }
        tracing::warn!(
            %error,
            run_id = %self.key.run_id,
            step_execution_id = %self.key.step_execution_id,
            attempt = self.key.attempt,
            "method attempt failed on the server"
        );
        let failure = AttemptFailure::server(
            StepAttemptFailureKind::DispatchError,
            format!("server error: {error}"),
        );
        self.propose_failure(failure, "", CommitUrgency::Prompt)
            .await;
    }

    async fn run_attempt(&self) -> Result<(), EffectError> {
        // The owner-memory method timeout covers preparation, so slow state or object reads count.
        let method_deadline = MethodDeadline::start(self.method_timeout_seconds);
        let read = self.retry_transient(|| self.read_attempt_state());
        let state = match self.before_method_deadline(&method_deadline, read).await {
            BeforeDeadline::Canceled => return Ok(()),
            BeforeDeadline::TimedOut => {
                // Without state, the failure carries no sticky Worker and commits promptly.
                self.propose_failure(method_deadline.timeout_failure(), "", CommitUrgency::Prompt)
                    .await;
                return Ok(());
            }
            BeforeDeadline::Done(state) => state?,
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
            dex::StepDurability::Async => CommitUrgency::Batched,
            _ => CommitUrgency::Prompt,
        };
        let timeouts = AttemptTimeouts::new(&state, method_deadline);
        let end = match self.prepare_request(&state, &timeouts).await? {
            PreparedRequest::Ready(request) => {
                self.dispatch_and_follow(&state, *request, &timeouts).await
            }
            PreparedRequest::Ended(end) => end,
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
                self.complete(&state, *frame, app_worker_id, &timeouts.method, urgency)
                    .await
            }
        }
    }

    /// Runs `operation` until the method deadline or cancellation, giving the deadline priority.
    ///
    /// Returns `TimedOut` once the deadline has expired, even when `operation` finished in the
    /// same poll or before the deadline timer fired; returns `Canceled` when the attempt was
    /// canceled first; otherwise returns the operation's output.
    async fn before_method_deadline<T>(
        &self,
        deadline: &MethodDeadline,
        operation: impl Future<Output = T>,
    ) -> BeforeDeadline<T> {
        tokio::select! {
            biased;
            () = sleep_until(deadline.expires_at) => BeforeDeadline::TimedOut,
            () = self.cancel.cancelled() => BeforeDeadline::Canceled,
            output = operation => {
                if deadline.has_expired() {
                    BeforeDeadline::TimedOut
                } else {
                    BeforeDeadline::Done(output)
                }
            }
        }
    }

    /// Reads the attempt's state on a blocking thread so that a stalled read cannot hold off
    /// the method deadline.
    ///
    /// Returns the state, `None` when the attempt is no longer running with this dispatch ID,
    /// or the read's error.
    async fn read_attempt_state(&self) -> Result<Option<MethodAttemptState>, EffectError> {
        let flow = self.flow.clone();
        let run = self.run;
        let key = self.key.clone();
        let task_id = self.task_id.clone();
        self.read_off_task(move |context| {
            let reader = context.runtime.flow_state_reader(&flow, true)?;
            read_method_attempt_state(reader.as_ref(), run, &key, &task_id)
        })
        .await
    }

    /// Runs a synchronous state read on the blocking pool.
    ///
    /// Returns the read's result, or [`EffectError::Stopped`] when the runtime shut down before
    /// the read ran. A panic in `read` resumes on this task.
    async fn read_off_task<T: Send + 'static>(
        &self,
        read: impl FnOnce(&EffectContext) -> Result<T, EffectError> + Send + 'static,
    ) -> Result<T, EffectError> {
        let context = Arc::clone(&self.context);
        // Dropping the handle at the deadline detaches the read; it ends with its store call.
        let handle = tokio::task::spawn_blocking(move || read(&context));
        match handle.await {
            Ok(result) => result,
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            Err(error) => {
                tracing::debug!(%error, run_id = %self.key.run_id, "state read did not run");
                Err(EffectError::Stopped)
            }
        }
    }

    /// Reads the hard-delete values and builds the Worker request before the method deadline.
    ///
    /// Returns the request, or the attempt's end when it was canceled, timed out, or is too large.
    async fn prepare_request(
        &self,
        state: &MethodAttemptState,
        timeouts: &AttemptTimeouts,
    ) -> Result<PreparedRequest, EffectError> {
        let read = self.retry_transient(|| self.read_hard_delete_values(state));
        let hard_delete_values = match self.before_method_deadline(&timeouts.method, read).await {
            BeforeDeadline::Canceled => return Ok(PreparedRequest::Ended(AttemptEnd::Canceled)),
            BeforeDeadline::TimedOut => {
                return Ok(PreparedRequest::Ended(AttemptEnd::Failure(
                    timeouts.method.timeout_failure(),
                )));
            }
            BeforeDeadline::Done(values) => values?,
        };
        let request = build_method_request(state, &hard_delete_values)?;
        if request.encoded_len() > self.context.config.max_worker_request_bytes {
            return Ok(PreparedRequest::Ended(AttemptEnd::Failure(
                AttemptFailure::server(
                    StepAttemptFailureKind::RequestTooLarge,
                    format!(
                        "Worker request of {} bytes exceeds the {} byte message limit",
                        request.encoded_len(),
                        self.context.config.max_worker_request_bytes
                    ),
                ),
            )));
        }
        Ok(PreparedRequest::Ready(Box::new(request)))
    }

    /// Runs `operation` until it succeeds or fails permanently, retrying transient failures.
    ///
    /// Returns [`EffectError::Stopped`] when the attempt is canceled while it waits.
    async fn retry_transient<T, Operation, OperationFuture>(
        &self,
        mut operation: Operation,
    ) -> Result<T, EffectError>
    where
        Operation: FnMut() -> OperationFuture,
        OperationFuture: Future<Output = Result<T, EffectError>>,
    {
        let mut backoff = self.context.retry_backoff();
        loop {
            match operation().await {
                Err(error) if error.is_retryable() => {
                    tracing::warn!(%error, run_id = %self.key.run_id, "retrying a transient attempt failure");
                    if !backoff.wait(&self.cancel).await {
                        return Err(EffectError::Stopped);
                    }
                }
                result => return result,
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
            let current_object_id = self.current_hard_delete_object_id(&attribute_key).await?;
            if let Some(object_id) = current_object_id.filter(|id| *id != object.object_id)
                && let Some(value) = objects.read_value(owner, &object_id).await?
            {
                values.insert(attribute_key, value);
            }
        }
        Ok(values)
    }

    async fn current_hard_delete_object_id(
        &self,
        attribute_key: &str,
    ) -> Result<Option<String>, EffectError> {
        let key = hard_delete_current_key(self.run.flow_hash, self.run.run_id, attribute_key);
        let flow = self.flow.clone();
        self.read_off_task(move |context| {
            let reader = context.runtime.flow_state_reader(&flow, false)?;
            let Some(bytes) = reader.get(ColumnFamily::Flow, &key)? else {
                return Ok(None);
            };
            let record: HardDeleteObjectRecord = decode_row("HardDeleteObject", &key, &bytes)?;
            Ok(record.object.map(|object| object.object_id))
        })
        .await
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
        timeouts: &AttemptTimeouts,
    ) -> AttemptEnd {
        // Building the request takes time, and an expired attempt must not reach a Worker.
        if timeouts.method.has_expired() {
            return AttemptEnd::Failure(timeouts.method.timeout_failure());
        }
        let method_deadline = timeouts.method.expires_at;
        let started = Instant::now();
        let remaining = method_deadline.saturating_duration_since(started);
        let wall_deadline = unix_micros_to_system_time(
            self.context
                .clock
                .now_unix_micros()
                .saturating_add(i64::try_from(remaining.as_micros()).unwrap_or(i64::MAX)),
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
        // Like the Go Server, the heartbeat timeout runs only while a Worker holds the task.
        let mut heartbeat_deadline = None;
        let mut checkpoints = CheckpointTracker {
            last_proposed: heartbeat_checkpoint_value(state),
            pending: None,
            flush_at: None,
        };
        loop {
            let flush_at = checkpoints.flush_at;
            tokio::select! {
                biased;
                () = sleep_until(method_deadline) => {
                    session.cancel();
                    return AttemptEnd::Failure(timeouts.method.timeout_failure());
                }
                () = sleep_until_optional(heartbeat_deadline) => {
                    session.cancel();
                    return AttemptEnd::Failure(timeouts.heartbeat_timeout_failure());
                }
                () = self.cancel.cancelled() => {
                    session.cancel();
                    return AttemptEnd::Canceled;
                }
                () = sleep_until_optional(flush_at) => {
                    if let Some(failure) = timeouts.expired_failure(heartbeat_deadline) {
                        session.cancel();
                        return AttemptEnd::Failure(failure);
                    }
                    if let Some(checkpoint) = checkpoints.take_pending() {
                        self.propose_heartbeat(checkpoint).await;
                    }
                }
                event = session.next_event() => {
                    // A timer fires up to a driver tick late; an event after its deadline loses.
                    if let Some(failure) = timeouts.expired_failure(heartbeat_deadline) {
                        session.cancel();
                        return AttemptEnd::Failure(failure);
                    }
                    match event {
                        None => {
                            return AttemptEnd::Failure(AttemptFailure::server(
                                StepAttemptFailureKind::DispatchError,
                                "dispatch ended without an outcome",
                            ));
                        }
                        Some(DispatchEvent::Assigned { .. }) => {
                            heartbeat_deadline = Some(Instant::now() + timeouts.heartbeat_timeout);
                        }
                        Some(DispatchEvent::Unassigned) => heartbeat_deadline = None,
                        Some(DispatchEvent::Output(frame)) => {
                            heartbeat_deadline = Some(Instant::now() + timeouts.heartbeat_timeout);
                            if let Some(change) = heartbeat_change(&frame) {
                                checkpoints.observe(change, self.context.config.heartbeat_batch_window);
                            } else if let Some(write) = into_step_stream_write(frame) {
                                self.write_step_stream(state, write).await;
                            }
                        }
                        Some(DispatchEvent::Completed(outcome)) => {
                            if let Some(checkpoint) = checkpoints.take_pending() {
                                self.propose_heartbeat(checkpoint).await;
                            }
                            return attempt_end(outcome);
                        }
                    }
                }
            }
        }
    }

    async fn propose_heartbeat(&self, checkpoint: Option<Value>) {
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
            .propose_speculative(&self.flow, command, CommitUrgency::Batched)
            .await
        {
            tracing::warn!(%error, run_id = %self.key.run_id, "proposing a heartbeat failed");
        }
    }

    /// Appends a Step Stream write in frame order; a rejected write is logged and never fails
    /// the attempt.
    async fn write_step_stream(&self, state: &MethodAttemptState, write: StepStreamWrite) {
        let Some(value) = write.value else {
            tracing::warn!(
                run_id = %self.key.run_id,
                stream_name = %write.stream_name,
                "Worker Stream write without a Value was dropped"
            );
            return;
        };
        let input = WriteStreamInput {
            tenant_id: state.run_record.tenant_id,
            flow_id: state.run_record.flow_id.clone(),
            flow_type: state.run_record.flow_type.clone(),
            stream_name: write.stream_name,
            stream_capacity_bytes: Some(write.stream_capacity_bytes),
            value,
            source: String::new(),
        };
        // The store logs every dropped write itself.
        let _is_written = self
            .context
            .stream_store
            .write_worker_best_effort(input, &self.key.step_execution_id)
            .await;
    }

    async fn complete(
        &self,
        state: &MethodAttemptState,
        frame: PollWorkerTaskFrame,
        app_worker_id: String,
        method_deadline: &MethodDeadline,
        urgency: CommitUrgency,
    ) -> Result<(), EffectError> {
        let sticky_app_worker_id = state.run_record.sticky_app_worker_id.clone();
        let max_value_bytes = match state.run_record.settings.as_ref() {
            Some(settings) if settings.max_value_bytes > 0 => settings.max_value_bytes,
            _ => self.context.config.default_max_value_bytes,
        };
        let flow_config = state.run_record.flow_config.clone().unwrap_or_default();
        let check = check_worker_result(
            frame,
            self.key.method,
            &self.context.validation_config,
            &flow_config,
            max_value_bytes,
        );
        let mut result = match check {
            WorkerResultCheck::Valid(result) => *result,
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
        result.assign_channel_message_ids();
        let mut backoff = self.context.retry_backoff();
        let uploads = loop {
            let upload = self
                .context
                .hard_delete_objects
                .upload_attribute_writes(self.object_owner(state), result.attribute_writes_mut())
                .await;
            match upload {
                Ok(uploads) => break uploads,
                Err(error) if error.is_retryable() && method_deadline.has_expired() => {
                    tracing::warn!(%error, run_id = %self.key.run_id, "hard-delete uploads failed after the method deadline");
                    let failure = method_deadline.timeout_failure();
                    self.propose_failure(failure, &sticky_app_worker_id, urgency)
                        .await;
                    return Ok(());
                }
                Err(error) if error.is_retryable() => {
                    tracing::warn!(%error, run_id = %self.key.run_id, "retrying hard-delete uploads");
                    if !backoff.wait(&self.cancel).await {
                        return Err(EffectError::Stopped);
                    }
                }
                Err(error) => return Err(error),
            }
        };
        // Like the Go Server's backends, a result whose processing ends after the deadline loses.
        if method_deadline.has_expired() {
            let failure = method_deadline.timeout_failure();
            self.propose_failure(failure, &sticky_app_worker_id, urgency)
                .await;
            return self
                .context
                .hard_delete_objects
                .finish_uploads(uploads, false)
                .await;
        }
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
        // Without a reply the node is stopping; recovery finishes the recorded uploads.
        let Some(reply) = reply else {
            return Ok(());
        };
        self.context
            .hard_delete_objects
            .finish_uploads(uploads, !reply.is_error())
            .await
    }

    async fn propose_failure(
        &self,
        failure: AttemptFailure,
        sticky_app_worker_id: &str,
        urgency: CommitUrgency,
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

/// The attempt's owner-memory timeouts.
struct AttemptTimeouts {
    method: MethodDeadline,
    heartbeat_timeout: Duration,
}

impl AttemptTimeouts {
    /// Returns the attempt's method timeout and heartbeat timeout, with the Go Server defaults.
    fn new(state: &MethodAttemptState, method: MethodDeadline) -> Self {
        let heartbeat_timeout_seconds = state
            .step_execution
            .options
            .as_ref()
            .map_or(0, |options| options.heartbeat_timeout_seconds);
        let heartbeat_timeout =
            positive_seconds(heartbeat_timeout_seconds).unwrap_or(DEFAULT_HEARTBEAT_TIMEOUT);
        Self {
            method,
            heartbeat_timeout,
        }
    }

    /// Returns the failure of an expired method or heartbeat deadline, checked against the clock
    /// rather than the timers; the method timeout comes first.
    fn expired_failure(&self, heartbeat_deadline: Option<Instant>) -> Option<AttemptFailure> {
        if self.method.has_expired() {
            return Some(self.method.timeout_failure());
        }
        heartbeat_deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
            .then(|| self.heartbeat_timeout_failure())
    }

    fn heartbeat_timeout_failure(&self) -> AttemptFailure {
        AttemptFailure::server(
            StepAttemptFailureKind::HeartbeatTimeout,
            format!("no heartbeat within {}s", self.heartbeat_timeout.as_secs()),
        )
    }
}

/// The method timeout, started before the attempt's first state read.
struct MethodDeadline {
    method_timeout: Duration,
    /// When the method timeout expires, counted from the attempt's start.
    expires_at: Instant,
}

impl MethodDeadline {
    /// Starts the configured method timeout now, or the Go Server default when it is not positive.
    fn start(method_timeout_seconds: i32) -> Self {
        let method_timeout =
            positive_seconds(method_timeout_seconds).unwrap_or(DEFAULT_METHOD_TIMEOUT);
        Self {
            method_timeout,
            expires_at: Instant::now() + method_timeout,
        }
    }

    /// Returns true once the method timeout has expired, even before its timer fires.
    fn has_expired(&self) -> bool {
        Instant::now() >= self.expires_at
    }

    fn timeout_failure(&self) -> AttemptFailure {
        AttemptFailure::server(
            StepAttemptFailureKind::MethodTimeout,
            format!(
                "method timeout of {}s expired",
                self.method_timeout.as_secs()
            ),
        )
    }
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
        DispatchOutcome::ServerClosing => AttemptEnd::Failure(AttemptFailure::server(
            StepAttemptFailureKind::DispatchError,
            "matching server is closing",
        )),
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

/// Returns the Step Stream write that a Worker frame carries.
fn into_step_stream_write(frame: PollWorkerTaskFrame) -> Option<StepStreamWrite> {
    match frame.output? {
        poll_worker_task_frame::Output::WaitFor(output) => match output.output? {
            invoke_wait_for_method_output::Output::StreamWrite(write) => Some(write),
            _ => None,
        },
        poll_worker_task_frame::Output::Execute(output) => match output.output? {
            invoke_execute_method_output::Output::StreamWrite(write) => Some(write),
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
    fn observe(&mut self, change: CheckpointChange, batch_window: Duration) {
        if change == self.last_proposed {
            self.pending = None;
            self.flush_at = None;
            return;
        }
        self.pending = Some(change);
        self.flush_at
            .get_or_insert_with(|| Instant::now() + batch_window);
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

/// Returns the routing of a dispatch to the run's sticky Worker or its Flow type queue; an
/// empty `served_by_app_worker_id` means no Worker served it yet.
pub(crate) fn pull_routing(
    sticky_app_worker_id: &str,
    served_by_app_worker_id: &str,
) -> PullRouting {
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
