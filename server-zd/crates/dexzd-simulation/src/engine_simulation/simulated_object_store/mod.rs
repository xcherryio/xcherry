// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! One shared in-memory object store with S3 semantics, seen by each node through a fault layer.

mod landing_writes;
mod storage_observations;

use std::future::Future;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use dexzd_object_store::{
    ByteRange, ObjectContent, ObjectListing, ObjectMetadata, ObjectStore, ObjectStoreError,
    ObjectStoreErrorKind, ObjectStoreOperation, ObjectStoreResult, WriteCondition, WriteOutcome,
};
use rand::{RngExt, SeedableRng};
use rand_chacha::ChaCha8Rng;
use tokio::sync::watch;
use tokio::time::Instant;

pub(crate) use landing_writes::SharedObjectStorage;
use landing_writes::{CrashLanding, WriteLanding};
pub(crate) use storage_observations::StorageObservations;

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
    /// How a write lands if its process dies before it reaches storage.
    crash_landing: CrashLanding,
}

struct FaultState {
    rng: ChaCha8Rng,
    fault_probability: f64,
}

/// One node process's view of the shared store; [`Self::cut_off`] makes its calls hang like a
/// crash.
pub(crate) struct NodeObjectStore {
    storage: Arc<SharedObjectStorage>,
    process_label: String,
    /// How this process's start found its local store, such as `kept` or `lost`.
    start_kind: String,
    faults: Mutex<FaultState>,
    cut_off: watch::Sender<bool>,
    is_serving: AtomicBool,
    unlanded_log_writes: Arc<AtomicUsize>,
}

impl NodeObjectStore {
    pub(crate) fn new(
        storage: Arc<SharedObjectStorage>,
        process_label: String,
        start_kind: String,
        fault_seed: u64,
    ) -> Self {
        Self {
            storage,
            process_label,
            start_kind,
            faults: Mutex::new(FaultState {
                rng: ChaCha8Rng::seed_from_u64(fault_seed),
                fault_probability: 0.0,
            }),
            cut_off: watch::Sender::new(false),
            is_serving: AtomicBool::new(false),
            unlanded_log_writes: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Records that the process finished starting.
    pub(crate) fn mark_serving(&self) {
        self.is_serving.store(true, Ordering::SeqCst);
        self.storage.mark_serving(&self.process_label);
    }

    /// Log writes of this process that have not reached storage yet.
    pub(crate) fn unlanded_log_writes(&self) -> usize {
        self.unlanded_log_writes.load(Ordering::SeqCst)
    }

    fn observe(&self, record: impl FnOnce(&mut StorageObservations)) {
        self.storage.observe(record);
    }

    /// Sets the probability that a call gets a fault; zero turns faults off.
    pub(crate) fn set_fault_probability(&self, probability: f64) {
        self.faults
            .lock()
            .expect("fault lock is not poisoned")
            .fault_probability = probability;
    }

    /// Makes every later call of this process hang, as when the process dies; writes already
    /// sent still land.
    pub(crate) fn cut_off(&self) {
        self.cut_off.send_replace(true);
    }

    fn is_cut_off(&self) -> bool {
        *self.cut_off.borrow()
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
        let crash_landing_kind = rng.random_range(0..3);
        let linger = Duration::from_millis(rng.random_range(0..3_000));
        drop(faults);
        self.storage.trace.record(&format!(
            "{} object-store {operation} {key} {fault:?} applied={is_effect_applied}",
            self.process_label
        ));
        CallPlan {
            fault,
            delay,
            is_effect_applied,
            crash_landing: match crash_landing_kind {
                0 => CrashLanding::AtDeadline,
                1 => CrashLanding::AfterSuccessorLease(linger),
                _ => CrashLanding::AfterSuccessorServes(linger / 10),
            },
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
        T: Send + 'static,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ObjectStoreResult<T>> + Send + 'static,
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
    /// A write that reaches storage runs on its own task, so it lands even if this process dies.
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
        T: Send + 'static,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ObjectStoreResult<T>> + Send + 'static,
    {
        let reaches_storage = match plan.fault {
            None | Some(InjectedFault::SlowResponse | InjectedFault::ResponseLost) => true,
            Some(InjectedFault::ServerError | InjectedFault::Timeout) => plan.is_effect_applied,
            Some(InjectedFault::Throttled | InjectedFault::ConditionalRequestConflict) => false,
        };
        let deadline = Instant::now() + plan.delay;
        let landing = reaches_storage.then(|| {
            WriteLanding {
                storage: Arc::clone(&self.storage),
                process_label: self.process_label.clone(),
                key: key.to_owned(),
                deadline,
                crash_landing: plan.crash_landing,
                cut_off: self.cut_off.subscribe(),
                unlanded_log_writes: (key_class(key) == "log")
                    .then(|| Arc::clone(&self.unlanded_log_writes)),
            }
            .send(effect, is_written)
        });
        tokio::time::sleep_until(deadline).await;
        if self.is_cut_off() {
            return std::future::pending().await;
        }
        let outcome = match landing {
            Some(landing) => match landing.await {
                Ok(outcome) => Some(outcome),
                // Only the end of the seed drops a landing.
                Err(_) => return std::future::pending().await,
            },
            None => None,
        };
        let Some(fault) = plan.fault else {
            return outcome.expect("a write without a fault reaches storage");
        };
        match fault {
            InjectedFault::SlowResponse => {
                self.observe_fault(fault, FaultEffect::Delayed);
                outcome.expect("a slow write reaches storage")
            }
            InjectedFault::Throttled | InjectedFault::ConditionalRequestConflict => {
                self.observe_fault(fault, FaultEffect::FailedBeforeEffect);
                Err(rejection_before_effect(operation, key, fault))
            }
            InjectedFault::ServerError | InjectedFault::Timeout => {
                let effect_kind = if outcome.as_ref().is_some_and(is_written) {
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
                let outcome = outcome.expect("a write with a lost response reaches storage");
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
pub(super) fn key_class(key: &str) -> &'static str {
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
        let store = Arc::clone(&self.storage.store);
        let owned_key = key.to_owned();
        let outcome = self
            .run_write(
                ObjectStoreOperation::Put,
                key,
                move || async move { store.put(&owned_key, body, condition).await },
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
        self.run_read(ObjectStoreOperation::Get, key, || {
            self.storage.store.get(key)
        })
        .await
    }

    async fn get_range(
        &self,
        key: &str,
        range: ByteRange,
    ) -> ObjectStoreResult<Option<ObjectContent>> {
        self.run_read(ObjectStoreOperation::Get, key, || {
            self.storage.store.get_range(key, range)
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
        self.run_read(ObjectStoreOperation::Head, key, || {
            self.storage.store.head(key)
        })
        .await
    }

    async fn delete(&self, key: &str) -> ObjectStoreResult<()> {
        let store = Arc::clone(&self.storage.store);
        let owned_key = key.to_owned();
        self.run_write(
            ObjectStoreOperation::Delete,
            key,
            move || async move { store.delete(&owned_key).await },
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
        let store = Arc::clone(&self.storage.store);
        let owned_keys = keys.to_vec();
        self.run_write(
            ObjectStoreOperation::Delete,
            first_key,
            move || async move { store.delete_many(&owned_keys).await },
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
            self.storage.store.list(prefix, start_after, max_keys)
        })
        .await
    }
}

#[cfg(test)]
mod tests;
