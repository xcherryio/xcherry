// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! One shared in-memory object store with S3 semantics, seen by each node through a fault layer.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use dexzd_object_store::{
    ByteRange, InMemoryObjectStore, ObjectContent, ObjectListing, ObjectMetadata, ObjectStore,
    ObjectStoreError, ObjectStoreErrorKind, ObjectStoreOperation, ObjectStoreResult,
    WriteCondition, WriteOutcome,
};
use rand::{RngExt, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::engine_simulation::event_trace::EventTrace;

/// A fault the layer injects into one call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum InjectedFault {
    /// The response arrives late.
    SlowResponse,
    /// HTTP 5xx: reads fail without effect; writes may or may not take effect.
    ServerError,
    /// HTTP 503 SlowDown before any effect.
    Throttled,
    /// HTTP 409 for a concurrent conditional request, before any effect.
    ConditionalRequestConflict,
    /// The client gives up after a delay; a write may or may not take effect.
    Timeout,
    /// The call takes effect, but its response is lost.
    ResponseLost,
}

impl InjectedFault {
    pub(crate) const ALL: [Self; 6] = [
        Self::SlowResponse,
        Self::ServerError,
        Self::Throttled,
        Self::ConditionalRequestConflict,
        Self::Timeout,
        Self::ResponseLost,
    ];
}

/// What an injected fault did to its call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum FaultEffect {
    /// The call completed normally, only late.
    Delayed,
    /// The call failed before it reached storage.
    FailedBeforeEffect,
    /// A read ran, but the caller got an error instead of its result.
    ReadDiscarded,
    /// A write took effect, and the caller got an unknown outcome.
    WrittenOutcomeUnknown,
    /// A write did not take effect, and the caller got an unknown outcome.
    NotWrittenOutcomeUnknown,
}

/// What happens to one call, drawn when the call starts so timing never changes the draws.
#[derive(Debug, Clone, Copy)]
struct CallPlan {
    fault: Option<InjectedFault>,
    delay: Duration,
    /// Whether a failing or interrupted write still takes effect.
    is_effect_applied: bool,
}

struct FaultState {
    rng: ChaCha8Rng,
    fault_probability: f64,
}

/// What processes did to object storage, so coverage checks can show the risky paths ran.
#[derive(Debug, Default)]
pub(crate) struct StorageObservations {
    /// Faults by kind and by what they did to the call, counted once the call's effect is known.
    pub(crate) injected_faults: BTreeMap<(InjectedFault, FaultEffect), u64>,
    /// Conditional writes the store rejected, by key class.
    pub(crate) precondition_failed_puts: BTreeMap<&'static str, u64>,
    /// Objects deleted, by key class.
    pub(crate) deleted_objects: BTreeMap<&'static str, u64>,
    /// Snapshot manifest reads during a start, by how the start found its local store.
    pub(crate) snapshot_reads_during_start: BTreeMap<String, u64>,
    pub(crate) old_epoch: OldEpochWindows,
}

/// Old-epoch processes before and after a successor's lease write took effect.
#[derive(Debug, Default)]
pub(crate) struct OldEpochWindows {
    /// Old-epoch processes whose successor has not written the lease yet.
    awaiting_successor: BTreeSet<String>,
    /// Old-epoch processes that kept running after their successor's lease write took effect.
    superseded: BTreeSet<String>,
    /// Successor lease writes that took effect while an old-epoch process still ran.
    pub(crate) overlaps: u64,
    /// Writes superseded processes started, by key class.
    pub(crate) write_attempts: BTreeMap<&'static str, u64>,
    /// Frames superseded processes applied.
    pub(crate) frames: u64,
}

impl OldEpochWindows {
    /// Records that `process_label` keeps running while a successor starts.
    pub(crate) fn await_successor(&mut self, process_label: &str) {
        self.awaiting_successor.insert(process_label.to_owned());
    }

    /// Records a lease write of `writer` that took effect; returns the processes it superseded.
    fn record_lease_written(&mut self, writer: &str) -> Vec<String> {
        let superseded: Vec<String> = self
            .awaiting_successor
            .iter()
            .filter(|process_label| *process_label != writer)
            .cloned()
            .collect();
        for process_label in &superseded {
            self.awaiting_successor.remove(process_label);
            self.superseded.insert(process_label.clone());
            self.overlaps += 1;
        }
        superseded
    }

    fn record_write_attempt(&mut self, process_label: &str, key_class: &'static str) {
        if self.superseded.contains(process_label) {
            *self.write_attempts.entry(key_class).or_default() += 1;
        }
    }

    /// Counts a frame that `process_label` applied, if its successor already holds the lease.
    pub(crate) fn record_frame(&mut self, process_label: &str) {
        if self.superseded.contains(process_label) {
            self.frames += 1;
        }
    }
}

/// One node process's view of the shared store; [`Self::cut_off`] makes its calls hang like a
/// crash.
pub(crate) struct NodeObjectStore {
    shared: Arc<InMemoryObjectStore>,
    process_label: String,
    /// How this process's start found its local store, such as `kept` or `lost`.
    start_kind: String,
    faults: Mutex<FaultState>,
    is_cut_off: AtomicBool,
    is_serving: AtomicBool,
    trace: Arc<EventTrace>,
    observations: Arc<Mutex<StorageObservations>>,
}

impl NodeObjectStore {
    pub(crate) fn new(
        shared: Arc<InMemoryObjectStore>,
        process_label: String,
        start_kind: String,
        fault_seed: u64,
        trace: Arc<EventTrace>,
        observations: Arc<Mutex<StorageObservations>>,
    ) -> Self {
        Self {
            shared,
            process_label,
            start_kind,
            faults: Mutex::new(FaultState {
                rng: ChaCha8Rng::seed_from_u64(fault_seed),
                fault_probability: 0.0,
            }),
            is_cut_off: AtomicBool::new(false),
            is_serving: AtomicBool::new(false),
            trace,
            observations,
        }
    }

    /// Records that the process finished starting.
    pub(crate) fn mark_serving(&self) {
        self.is_serving.store(true, Ordering::SeqCst);
    }

    fn observe(&self, record: impl FnOnce(&mut StorageObservations)) {
        record(
            &mut self
                .observations
                .lock()
                .expect("storage observation lock is not poisoned"),
        );
    }

    /// Sets the probability that a call gets a fault; zero turns faults off.
    pub(crate) fn set_fault_probability(&self, probability: f64) {
        self.faults
            .lock()
            .expect("fault lock is not poisoned")
            .fault_probability = probability;
    }

    /// Makes every later call of this process hang, as when the process dies.
    pub(crate) fn cut_off(&self) {
        self.is_cut_off.store(true, Ordering::SeqCst);
    }

    fn is_cut_off(&self) -> bool {
        self.is_cut_off.load(Ordering::SeqCst)
    }

    fn plan_call(&self, operation: ObjectStoreOperation, key: &str) -> CallPlan {
        let mut faults = self.faults.lock().expect("fault lock is not poisoned");
        let probability = faults.fault_probability;
        let rng = &mut faults.rng;
        let has_fault = probability > 0.0 && rng.random_bool(probability);
        let fault =
            has_fault.then(|| InjectedFault::ALL[rng.random_range(0..InjectedFault::ALL.len())]);
        let delay = match fault {
            Some(InjectedFault::SlowResponse) => Duration::from_millis(rng.random_range(50..3_000)),
            Some(InjectedFault::Timeout) => Duration::from_millis(rng.random_range(1_000..8_000)),
            _ => Duration::from_millis(rng.random_range(1..20)),
        };
        let is_effect_applied = rng.random_bool(0.5);
        drop(faults);
        self.trace.record(&format!(
            "{} object-store {operation} {key} {fault:?} applied={is_effect_applied}",
            self.process_label
        ));
        CallPlan {
            fault,
            delay,
            is_effect_applied,
        }
    }

    fn observe_fault(&self, fault: InjectedFault, effect: FaultEffect) {
        self.observe(|observations| {
            *observations
                .injected_faults
                .entry((fault, effect))
                .or_default() += 1;
        });
    }

    /// Runs one read under a new plan. `effect` performs the read on the shared store.
    async fn run_read<T, F, Fut>(
        &self,
        operation: ObjectStoreOperation,
        key: &str,
        effect: F,
    ) -> ObjectStoreResult<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = ObjectStoreResult<T>>,
    {
        if self.is_cut_off() {
            return std::future::pending().await;
        }
        let plan = self.plan_call(operation, key);
        tokio::time::sleep(plan.delay).await;
        if self.is_cut_off() {
            return std::future::pending().await;
        }
        let Some(fault) = plan.fault else {
            return effect().await;
        };
        let (effect_kind, result) = match fault {
            InjectedFault::SlowResponse => (FaultEffect::Delayed, effect().await),
            InjectedFault::ResponseLost => {
                let discarded = effect().await;
                drop(discarded);
                (
                    FaultEffect::ReadDiscarded,
                    Err(injected_error(
                        operation,
                        key,
                        ObjectStoreErrorKind::Transient,
                        "injected lost response",
                    )),
                )
            }
            _ => (
                FaultEffect::FailedBeforeEffect,
                Err(rejection_before_effect(operation, key, fault)),
            ),
        };
        self.observe_fault(fault, effect_kind);
        result
    }

    /// Runs one write under a new plan; see [`Self::run_write_with_plan`].
    async fn run_write<T, F, Fut>(
        &self,
        operation: ObjectStoreOperation,
        key: &str,
        effect: F,
        is_written: fn(&ObjectStoreResult<T>) -> bool,
    ) -> ObjectStoreResult<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = ObjectStoreResult<T>>,
    {
        if self.is_cut_off() {
            return std::future::pending().await;
        }
        let process_label = &self.process_label;
        self.observe(|observations| {
            observations
                .old_epoch
                .record_write_attempt(process_label, key_class(key));
        });
        let plan = self.plan_call(operation, key);
        self.run_write_with_plan(plan, operation, key, effect, is_written)
            .await
    }

    /// Runs one write under `plan`. `effect` performs it on the shared store, and `is_written`
    /// tells whether its result means the write took effect.
    ///
    /// Returns an unknown outcome only for a write that may have taken effect; a lost response
    /// of a write that did not take effect returns the store's own result.
    async fn run_write_with_plan<T, F, Fut>(
        &self,
        plan: CallPlan,
        operation: ObjectStoreOperation,
        key: &str,
        effect: F,
        is_written: fn(&ObjectStoreResult<T>) -> bool,
    ) -> ObjectStoreResult<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = ObjectStoreResult<T>>,
    {
        tokio::time::sleep(plan.delay).await;
        if self.is_cut_off() {
            // The request is on the wire: it may still land after the process died.
            if plan.is_effect_applied {
                let landed = self.perform_write(key, effect, is_written).await;
                drop(landed);
            }
            return std::future::pending().await;
        }
        let Some(fault) = plan.fault else {
            return self.perform_write(key, effect, is_written).await;
        };
        match fault {
            InjectedFault::SlowResponse => {
                self.observe_fault(fault, FaultEffect::Delayed);
                self.perform_write(key, effect, is_written).await
            }
            InjectedFault::Throttled | InjectedFault::ConditionalRequestConflict => {
                self.observe_fault(fault, FaultEffect::FailedBeforeEffect);
                Err(rejection_before_effect(operation, key, fault))
            }
            InjectedFault::ServerError | InjectedFault::Timeout => {
                let is_applied = plan.is_effect_applied
                    && is_written(&self.perform_write(key, effect, is_written).await);
                let effect_kind = if is_applied {
                    FaultEffect::WrittenOutcomeUnknown
                } else {
                    FaultEffect::NotWrittenOutcomeUnknown
                };
                self.observe_fault(fault, effect_kind);
                Err(injected_error(
                    operation,
                    key,
                    ObjectStoreErrorKind::OutcomeUnknown,
                    "injected HTTP 500 or client timeout",
                ))
            }
            InjectedFault::ResponseLost => {
                let outcome = self.perform_write(key, effect, is_written).await;
                if !is_written(&outcome) {
                    return outcome;
                }
                self.observe_fault(fault, FaultEffect::WrittenOutcomeUnknown);
                Err(injected_error(
                    operation,
                    key,
                    ObjectStoreErrorKind::OutcomeUnknown,
                    "injected lost response",
                ))
            }
        }
    }

    /// Performs a write on the shared store and records a lease write that took effect.
    async fn perform_write<T, F, Fut>(
        &self,
        key: &str,
        effect: F,
        is_written: fn(&ObjectStoreResult<T>) -> bool,
    ) -> ObjectStoreResult<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = ObjectStoreResult<T>>,
    {
        let outcome = effect().await;
        if key_class(key) == "lease" && is_written(&outcome) {
            let mut superseded = Vec::new();
            self.observe(|observations| {
                superseded = observations
                    .old_epoch
                    .record_lease_written(&self.process_label);
            });
            for old_process in superseded {
                self.trace.record(&format!(
                    "{} holds the lease while {old_process} still runs",
                    self.process_label
                ));
            }
        }
        outcome
    }
}

/// The transient error of a fault that fails a call before it reaches storage.
fn rejection_before_effect(
    operation: ObjectStoreOperation,
    key: &str,
    fault: InjectedFault,
) -> ObjectStoreError {
    let detail = match fault {
        InjectedFault::Throttled => "injected HTTP 503 SlowDown",
        InjectedFault::ConditionalRequestConflict => "injected HTTP 409 ConditionalRequestConflict",
        _ => "injected HTTP 500 or client timeout",
    };
    injected_error(operation, key, ObjectStoreErrorKind::Transient, detail)
}

fn is_put_written(outcome: &ObjectStoreResult<WriteOutcome>) -> bool {
    matches!(outcome, Ok(WriteOutcome::Written { .. }))
}

fn is_delete_done(outcome: &ObjectStoreResult<()>) -> bool {
    outcome.is_ok()
}

/// The first segment of an object key: `log`, `lease`, `manifest`, `snapshot`, and so on.
fn key_class(key: &str) -> &'static str {
    match key.split('/').next() {
        Some("log") => "log",
        Some("lease") => "lease",
        Some("manifest") => "manifest",
        Some("snapshot") => "snapshot",
        Some("cluster") => "cluster",
        _ => "other",
    }
}

fn injected_error(
    operation: ObjectStoreOperation,
    key: &str,
    kind: ObjectStoreErrorKind,
    detail: &str,
) -> ObjectStoreError {
    ObjectStoreError::new(operation, key, kind, detail)
}

#[async_trait]
impl ObjectStore for NodeObjectStore {
    async fn put(
        &self,
        key: &str,
        body: Bytes,
        condition: WriteCondition,
    ) -> ObjectStoreResult<WriteOutcome> {
        let outcome = self
            .run_write(
                ObjectStoreOperation::Put,
                key,
                || self.shared.put(key, body, condition),
                is_put_written,
            )
            .await;
        if let Ok(WriteOutcome::PreconditionFailed) = outcome {
            self.observe(|observations| {
                *observations
                    .precondition_failed_puts
                    .entry(key_class(key))
                    .or_default() += 1;
            });
        }
        outcome
    }

    /// Reads the file synchronously: `tokio::fs` runs on blocking threads, which break determinism.
    async fn put_file(
        &self,
        key: &str,
        path: &Path,
        condition: WriteCondition,
    ) -> ObjectStoreResult<WriteOutcome> {
        let body = std::fs::read(path).map_err(|error| {
            ObjectStoreError::new(
                ObjectStoreOperation::Put,
                key,
                ObjectStoreErrorKind::Permanent,
                format!("reading {}: {error}", path.display()),
            )
        })?;
        self.put(key, Bytes::from(body), condition).await
    }

    async fn get(&self, key: &str) -> ObjectStoreResult<Option<ObjectContent>> {
        let is_snapshot_manifest = key.starts_with("snapshot/") && key.contains("/manifest/");
        if is_snapshot_manifest && !self.is_serving.load(Ordering::SeqCst) {
            self.observe(|observations| {
                *observations
                    .snapshot_reads_during_start
                    .entry(self.start_kind.clone())
                    .or_default() += 1;
            });
        }
        self.run_read(ObjectStoreOperation::Get, key, || self.shared.get(key))
            .await
    }

    async fn get_range(
        &self,
        key: &str,
        range: ByteRange,
    ) -> ObjectStoreResult<Option<ObjectContent>> {
        self.run_read(ObjectStoreOperation::Get, key, || {
            self.shared.get_range(key, range)
        })
        .await
    }

    /// Writes the file synchronously, through a temporary file renamed on success.
    async fn get_to_file(&self, key: &str, path: &Path) -> ObjectStoreResult<Option<u64>> {
        let Some(content) = self.get(key).await? else {
            return Ok(None);
        };
        let file_error = |error: std::io::Error| {
            ObjectStoreError::new(
                ObjectStoreOperation::Get,
                key,
                ObjectStoreErrorKind::Permanent,
                format!("writing {}: {error}", path.display()),
            )
        };
        let mut temporary = path.as_os_str().to_owned();
        temporary.push(".download");
        std::fs::write(&temporary, &content.bytes).map_err(file_error)?;
        std::fs::rename(&temporary, path).map_err(file_error)?;
        Ok(Some(content.object_size))
    }

    async fn head(&self, key: &str) -> ObjectStoreResult<Option<ObjectMetadata>> {
        self.run_read(ObjectStoreOperation::Head, key, || self.shared.head(key))
            .await
    }

    async fn delete(&self, key: &str) -> ObjectStoreResult<()> {
        self.run_write(
            ObjectStoreOperation::Delete,
            key,
            || self.shared.delete(key),
            is_delete_done,
        )
        .await?;
        self.observe(|observations| {
            *observations
                .deleted_objects
                .entry(key_class(key))
                .or_default() += 1;
        });
        Ok(())
    }

    async fn delete_many(&self, keys: &[String]) -> ObjectStoreResult<()> {
        let first_key = keys.first().map_or("", String::as_str);
        self.run_write(
            ObjectStoreOperation::Delete,
            first_key,
            || self.shared.delete_many(keys),
            is_delete_done,
        )
        .await?;
        self.observe(|observations| {
            for key in keys {
                *observations
                    .deleted_objects
                    .entry(key_class(key))
                    .or_default() += 1;
            }
        });
        Ok(())
    }

    async fn list(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        max_keys: u32,
    ) -> ObjectStoreResult<ObjectListing> {
        self.run_read(ObjectStoreOperation::List, prefix, || {
            self.shared.list(prefix, start_after, max_keys)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use bytes::Bytes;
    use dexzd_object_store::{
        InMemoryObjectStore, ObjectStore, ObjectStoreErrorKind, ObjectStoreOperation,
        WriteCondition, WriteOutcome,
    };

    use super::{
        CallPlan, FaultEffect, InjectedFault, NodeObjectStore, StorageObservations, is_put_written,
    };
    use crate::engine_simulation::event_trace::EventTrace;

    const KEY: &str = "log/node-a/0000000001/00000000000000000001";

    fn paused_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .expect("a current-thread runtime with paused time builds")
    }

    fn layer_for(
        process_label: &str,
        shared: &Arc<InMemoryObjectStore>,
        observations: &Arc<Mutex<StorageObservations>>,
    ) -> NodeObjectStore {
        NodeObjectStore::new(
            Arc::clone(shared),
            process_label.to_owned(),
            "first".to_owned(),
            0,
            Arc::new(EventTrace::new(tokio::time::Instant::now())),
            Arc::clone(observations),
        )
    }

    /// Runs `test` on a paused current-thread runtime with a layer over an empty shared store.
    fn with_layer(test: impl AsyncFnOnce(&NodeObjectStore, &InMemoryObjectStore)) {
        paused_runtime().block_on(async {
            let shared = Arc::new(InMemoryObjectStore::new());
            let observations = Arc::new(Mutex::new(StorageObservations::default()));
            let layer = layer_for("p1", &shared, &observations);
            test(&layer, &shared).await;
        });
    }

    fn plan(fault: InjectedFault, is_effect_applied: bool) -> CallPlan {
        CallPlan {
            fault: Some(fault),
            delay: Duration::from_millis(5),
            is_effect_applied,
        }
    }

    async fn put_with_plan(
        layer: &NodeObjectStore,
        shared: &InMemoryObjectStore,
        plan: CallPlan,
        body: &'static [u8],
    ) -> dexzd_object_store::ObjectStoreResult<WriteOutcome> {
        layer
            .run_write_with_plan(
                plan,
                ObjectStoreOperation::Put,
                KEY,
                || shared.put(KEY, Bytes::from_static(body), WriteCondition::CreateOnly),
                is_put_written,
            )
            .await
    }

    fn observed(layer: &NodeObjectStore) -> Vec<(InjectedFault, FaultEffect)> {
        layer
            .observations
            .lock()
            .expect("storage observation lock is not poisoned")
            .injected_faults
            .keys()
            .copied()
            .collect()
    }

    #[test]
    fn a_lost_response_of_a_failed_precondition_returns_the_failure() {
        with_layer(async |layer, shared| {
            let first = shared
                .put(
                    KEY,
                    Bytes::from_static(b"first"),
                    WriteCondition::CreateOnly,
                )
                .await;
            assert!(matches!(first, Ok(WriteOutcome::Written { .. })));
            let outcome = put_with_plan(
                layer,
                shared,
                plan(InjectedFault::ResponseLost, true),
                b"second",
            )
            .await;
            assert!(matches!(outcome, Ok(WriteOutcome::PreconditionFailed)));
            assert!(observed(layer).is_empty());
        });
    }

    #[test]
    fn a_lost_response_of_a_write_is_an_unknown_outcome() {
        with_layer(async |layer, shared| {
            let outcome = put_with_plan(
                layer,
                shared,
                plan(InjectedFault::ResponseLost, true),
                b"only",
            )
            .await;
            let error = outcome.expect_err("the response was lost");
            assert_eq!(error.kind, ObjectStoreErrorKind::OutcomeUnknown);
            let stored = shared.get(KEY).await.expect("the shared store reads");
            assert_eq!(
                stored.map(|object| object.bytes),
                Some(Bytes::from_static(b"only"))
            );
            assert_eq!(
                observed(layer),
                vec![(
                    InjectedFault::ResponseLost,
                    FaultEffect::WrittenOutcomeUnknown
                )]
            );
        });
    }

    #[test]
    fn a_timed_out_failed_precondition_is_labelled_not_written() {
        with_layer(async |layer, shared| {
            let first = shared
                .put(
                    KEY,
                    Bytes::from_static(b"first"),
                    WriteCondition::CreateOnly,
                )
                .await;
            assert!(matches!(first, Ok(WriteOutcome::Written { .. })));
            let outcome =
                put_with_plan(layer, shared, plan(InjectedFault::Timeout, true), b"second").await;
            let error = outcome.expect_err("the client timed out");
            assert_eq!(error.kind, ObjectStoreErrorKind::OutcomeUnknown);
            assert_eq!(
                observed(layer),
                vec![(
                    InjectedFault::Timeout,
                    FaultEffect::NotWrittenOutcomeUnknown
                )]
            );
        });
    }

    #[test]
    fn old_epoch_counts_start_when_the_successor_lease_write_takes_effect() {
        paused_runtime().block_on(async {
            let shared = Arc::new(InMemoryObjectStore::new());
            let observations = Arc::new(Mutex::new(StorageObservations::default()));
            let old = layer_for("p1", &shared, &observations);
            let successor = layer_for("p2", &shared, &observations);
            let old_epoch = |observations: &Mutex<StorageObservations>| {
                let observations = observations
                    .lock()
                    .expect("storage observation lock is not poisoned");
                let windows = &observations.old_epoch;
                (
                    windows.overlaps,
                    windows.write_attempts.get("log").copied().unwrap_or(0),
                    windows.frames,
                )
            };
            observations
                .lock()
                .expect("storage observation lock is not poisoned")
                .old_epoch
                .await_successor("p1");
            let early = old
                .put(
                    KEY,
                    Bytes::from_static(b"early"),
                    WriteCondition::CreateOnly,
                )
                .await;
            assert!(matches!(early, Ok(WriteOutcome::Written { .. })));
            record_frame(&observations, "p1");
            assert_eq!(old_epoch(&observations), (0, 0, 0));

            let lease = successor
                .put(
                    "lease/node-a",
                    Bytes::from_static(b"p2"),
                    WriteCondition::CreateOnly,
                )
                .await;
            assert!(matches!(lease, Ok(WriteOutcome::Written { .. })));
            let late = old
                .put(KEY, Bytes::from_static(b"late"), WriteCondition::CreateOnly)
                .await;
            assert!(matches!(late, Ok(WriteOutcome::PreconditionFailed)));
            record_frame(&observations, "p1");
            assert_eq!(old_epoch(&observations), (1, 1, 1));
        });
    }

    fn record_frame(observations: &Mutex<StorageObservations>, process_label: &str) {
        observations
            .lock()
            .expect("storage observation lock is not poisoned")
            .old_epoch
            .record_frame(process_label);
    }
}
