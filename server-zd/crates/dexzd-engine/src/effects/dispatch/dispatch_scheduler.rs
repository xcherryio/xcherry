// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Tracks pending and running method attempts for the effect router: lock-checked starts,
//! invocations parked on busy locks, running attempt tasks, and cancellation.

use std::collections::BTreeMap;
use std::sync::Arc;

use dexzd_keyspace::RunId;
use dexzd_protocol::dex::PullRouting;
use dexzd_protocol::dexzd::v1::StepInvocationKey;
use tokio_util::sync::CancellationToken;

use crate::effects::dispatch::method_attempt::{MethodAttempt, pull_routing};
use crate::effects::effect_runtime::{EffectRuntimeError, InvocationStartOutcome};
use crate::effects::method_request::MethodAttemptKey;
use crate::effects::state_reads::read_run_record;
use crate::effects::{EffectContext, EffectError, RouterMessage};
use crate::runtime::FlowAddress;

/// Result of one lock-checked start, reported back to the router.
#[derive(Debug)]
pub(crate) enum InvocationStartResult {
    Started,
    LocksBusy,
    Stale,
    Stopped,
}

struct RunningAttempt {
    cancel: CancellationToken,
}

struct StartingInvocation {
    /// Lock releases of the run already seen when the start began.
    lock_release_count: u64,
}

/// Dispatch state owned by the effect router task.
pub(crate) struct DispatchScheduler {
    context: Arc<EffectContext>,
    starting: BTreeMap<MethodAttemptKey, StartingInvocation>,
    parked: BTreeMap<RunId, BTreeMap<MethodAttemptKey, FlowAddress>>,
    lock_release_counts: BTreeMap<RunId, u64>,
    /// Running attempts by task ID, then by attempt key; time travel may reuse a task ID.
    running: BTreeMap<Vec<u8>, BTreeMap<MethodAttemptKey, RunningAttempt>>,
}

impl DispatchScheduler {
    pub(crate) fn new(context: Arc<EffectContext>) -> Self {
        Self {
            context,
            starting: BTreeMap::new(),
            parked: BTreeMap::new(),
            lock_release_counts: BTreeMap::new(),
            running: BTreeMap::new(),
        }
    }

    /// Starts a pending invocation unless a start of the same attempt is in flight.
    pub(crate) fn start_pending_invocation(&mut self, flow: FlowAddress, key: MethodAttemptKey) {
        if self.starting.contains_key(&key) {
            return;
        }
        let lock_release_count = self.lock_release_count(key.run_id);
        self.starting
            .insert(key.clone(), StartingInvocation { lock_release_count });
        let context = Arc::clone(&self.context);
        self.context.tracker.spawn(async move {
            let result = try_start_until_answered(&context, &flow, &key).await;
            context.report(RouterMessage::InvocationStartFinished { flow, key, result });
        });
    }

    /// Records the answer of a lock-checked start.
    pub(crate) fn finish_start(
        &mut self,
        flow: FlowAddress,
        key: MethodAttemptKey,
        result: InvocationStartResult,
    ) {
        let Some(starting) = self.starting.remove(&key) else {
            return;
        };
        if !matches!(result, InvocationStartResult::LocksBusy) {
            return;
        }
        if starting.lock_release_count != self.lock_release_count(key.run_id) {
            // Locks were released while the start was in flight; try again now.
            self.start_pending_invocation(flow, key);
            return;
        }
        self.parked.entry(key.run_id).or_default().insert(key, flow);
    }

    /// Retries the run's invocations that waited for locks.
    pub(crate) fn release_locks(&mut self, run_id: RunId) {
        *self.lock_release_counts.entry(run_id).or_insert(0) += 1;
        for (key, flow) in self.parked.remove(&run_id).unwrap_or_default() {
            self.start_pending_invocation(flow, key);
        }
    }

    /// Retries every parked invocation; in-memory RPC locks release without an effect.
    pub(crate) fn retry_parked_invocations(&mut self) {
        let parked = std::mem::take(&mut self.parked);
        for (key, flow) in parked.into_values().flatten() {
            self.start_pending_invocation(flow, key);
        }
    }

    /// Runs a started attempt unless the same attempt of the same run attempt already runs.
    ///
    /// A start of a finished attempt still spawns; the attempt ends at once unless state
    /// shows it running.
    pub(crate) fn run_started_attempt(
        &mut self,
        flow: FlowAddress,
        key: MethodAttemptKey,
        task_id: Vec<u8>,
        method_timeout_seconds: i32,
    ) {
        let attempts = self.running.entry(task_id.clone()).or_default();
        if attempts.contains_key(&key) {
            return;
        }
        let cancel = self.context.shutdown.child_token();
        attempts.insert(
            key.clone(),
            RunningAttempt {
                cancel: cancel.clone(),
            },
        );
        let run = self.context.run_location(&flow, key.run_id);
        let attempt = MethodAttempt {
            context: Arc::clone(&self.context),
            flow,
            run,
            key: key.clone(),
            task_id: task_id.clone(),
            method_timeout_seconds,
            cancel,
        };
        let context = Arc::clone(&self.context);
        self.context.tracker.spawn(async move {
            attempt.run().await;
            context.report(RouterMessage::AttemptFinished { task_id, key });
        });
    }

    /// Stops the attempts dispatched as `task_id`; their Workers receive a cancel request.
    pub(crate) fn cancel_attempt(&mut self, task_id: &[u8]) {
        for attempt in self
            .running
            .get(task_id)
            .into_iter()
            .flat_map(BTreeMap::values)
        {
            attempt.cancel.cancel();
        }
    }

    pub(crate) fn finish_attempt(&mut self, task_id: Vec<u8>, key: &MethodAttemptKey) {
        if let Some(attempts) = self.running.get_mut(&task_id) {
            attempts.remove(key);
            if attempts.is_empty() {
                self.running.remove(&task_id);
            }
        }
    }

    pub(crate) fn has_parked_invocations(&self) -> bool {
        !self.parked.is_empty()
    }

    fn lock_release_count(&self, run_id: RunId) -> u64 {
        self.lock_release_counts.get(&run_id).copied().unwrap_or(0)
    }
}

/// Asks the runtime to start the attempt, retrying while the runtime or state is unavailable.
async fn try_start_until_answered(
    context: &EffectContext,
    flow: &FlowAddress,
    key: &MethodAttemptKey,
) -> InvocationStartResult {
    let invocation: StepInvocationKey = key.invocation();
    let task_id = context.id_source.dispatch_task_id().as_bytes().to_vec();
    let mut backoff = context.retry_backoff();
    loop {
        let outcome = match read_dispatch_pull_routing(context, flow, key) {
            Ok(Some(pull_routing)) => context
                .runtime
                .try_start_invocation(
                    flow,
                    key.run_id,
                    key.run_attempt,
                    &invocation,
                    key.attempt,
                    task_id.clone(),
                    pull_routing,
                )
                .await
                .map_err(EffectError::Runtime),
            Ok(None) => return InvocationStartResult::Stale,
            Err(error) => Err(error),
        };
        match outcome {
            Ok(InvocationStartOutcome::Started) => return InvocationStartResult::Started,
            Ok(InvocationStartOutcome::LocksBusy) => return InvocationStartResult::LocksBusy,
            Ok(InvocationStartOutcome::Stale) => return InvocationStartResult::Stale,
            Err(EffectError::Runtime(EffectRuntimeError::NodeStopping { .. })) => {
                return InvocationStartResult::Stopped;
            }
            Err(error) => {
                // A corrupt run record also waits: dropping the start would strand the call.
                if error.is_retryable() {
                    tracing::debug!(%error, run_id = %key.run_id, "invocation start is waiting for the runtime");
                } else {
                    tracing::warn!(%error, run_id = %key.run_id, "invocation start cannot read the run");
                }
                if !backoff.wait(&context.shutdown).await {
                    return InvocationStartResult::Stopped;
                }
            }
        }
    }
}

/// Returns the routing that the start records: the run's sticky Worker and no serving Worker.
///
/// Returns `None` when the run no longer exists, or [`EffectError`] when its state is unreadable.
fn read_dispatch_pull_routing(
    context: &EffectContext,
    flow: &FlowAddress,
    key: &MethodAttemptKey,
) -> Result<Option<PullRouting>, EffectError> {
    let reader = context.runtime.flow_state_reader(flow, true)?;
    let run = context.run_location(flow, key.run_id);
    let run_record = read_run_record(reader.as_ref(), run)?;
    Ok(run_record.map(|record| pull_routing(&record.sticky_app_worker_id, "")))
}
