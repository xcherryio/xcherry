// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! One shared in-memory object store with S3 semantics, seen by each node through a fault layer.

use std::collections::BTreeMap;
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

/// One node process's view of the shared store; [`Self::cut_off`] makes its calls hang like a crash.
pub(crate) struct NodeObjectStore {
    shared: Arc<InMemoryObjectStore>,
    process_label: String,
    faults: Mutex<FaultState>,
    is_cut_off: AtomicBool,
    trace: Arc<EventTrace>,
    injected_faults: Arc<Mutex<BTreeMap<InjectedFault, u64>>>,
}

impl NodeObjectStore {
    pub(crate) fn new(
        shared: Arc<InMemoryObjectStore>,
        process_label: String,
        fault_seed: u64,
        trace: Arc<EventTrace>,
        injected_faults: Arc<Mutex<BTreeMap<InjectedFault, u64>>>,
    ) -> Self {
        Self {
            shared,
            process_label,
            faults: Mutex::new(FaultState {
                rng: ChaCha8Rng::seed_from_u64(fault_seed),
                fault_probability: 0.0,
            }),
            is_cut_off: AtomicBool::new(false),
            trace,
            injected_faults,
        }
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
        if let Some(fault) = fault {
            *self
                .injected_faults
                .lock()
                .expect("fault count lock is not poisoned")
                .entry(fault)
                .or_default() += 1;
        }
        self.trace.record(&format!(
            "{} object-store {operation} {} {fault:?} applied={is_effect_applied}",
            self.process_label,
            without_content_hash(key)
        ));
        CallPlan {
            fault,
            delay,
            is_effect_applied,
        }
    }

    /// Runs one call under its plan. `effect` performs the call on the shared store.
    async fn run_call<T, F, Fut>(
        &self,
        operation: ObjectStoreOperation,
        key: &str,
        is_write: bool,
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
            // The request is on the wire: it may still land after the process died.
            if is_write && plan.is_effect_applied {
                let landed = effect().await;
                drop(landed);
            }
            return std::future::pending().await;
        }
        let failure_kind = if is_write {
            ObjectStoreErrorKind::OutcomeUnknown
        } else {
            ObjectStoreErrorKind::Transient
        };
        match plan.fault {
            None | Some(InjectedFault::SlowResponse) => effect().await,
            Some(InjectedFault::Throttled) => Err(injected_error(
                operation,
                key,
                ObjectStoreErrorKind::Transient,
                "injected HTTP 503 SlowDown",
            )),
            Some(InjectedFault::ConditionalRequestConflict) => Err(injected_error(
                operation,
                key,
                ObjectStoreErrorKind::Transient,
                "injected HTTP 409 ConditionalRequestConflict",
            )),
            Some(InjectedFault::ServerError | InjectedFault::Timeout) => {
                if is_write && plan.is_effect_applied {
                    let applied = effect().await;
                    drop(applied);
                }
                Err(injected_error(
                    operation,
                    key,
                    failure_kind,
                    "injected HTTP 500 or client timeout",
                ))
            }
            Some(InjectedFault::ResponseLost) => {
                let applied = effect().await;
                drop(applied);
                Err(injected_error(
                    operation,
                    key,
                    failure_kind,
                    "injected lost response",
                ))
            }
        }
    }
}

/// Snapshot file keys end with a hash of content that holds engine UUIDs; the trace omits it.
fn without_content_hash(key: &str) -> &str {
    match key.rsplit_once('.') {
        Some((stem, hash))
            if key.starts_with("snapshot/")
                && hash.len() == 16
                && hash.bytes().all(|byte| byte.is_ascii_hexdigit()) =>
        {
            stem
        }
        _ => key,
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
        self.run_call(ObjectStoreOperation::Put, key, true, || {
            self.shared.put(key, body, condition)
        })
        .await
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
        self.run_call(ObjectStoreOperation::Get, key, false, || {
            self.shared.get(key)
        })
        .await
    }

    async fn get_range(
        &self,
        key: &str,
        range: ByteRange,
    ) -> ObjectStoreResult<Option<ObjectContent>> {
        self.run_call(ObjectStoreOperation::Get, key, false, || {
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
        self.run_call(ObjectStoreOperation::Head, key, false, || {
            self.shared.head(key)
        })
        .await
    }

    async fn delete(&self, key: &str) -> ObjectStoreResult<()> {
        self.run_call(ObjectStoreOperation::Delete, key, true, || {
            self.shared.delete(key)
        })
        .await
    }

    async fn delete_many(&self, keys: &[String]) -> ObjectStoreResult<()> {
        let first_key = keys.first().map_or("", String::as_str);
        self.run_call(ObjectStoreOperation::Delete, first_key, true, || {
            self.shared.delete_many(keys)
        })
        .await
    }

    async fn list(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        max_keys: u32,
    ) -> ObjectStoreResult<ObjectListing> {
        self.run_call(ObjectStoreOperation::List, prefix, false, || {
            self.shared.list(prefix, start_after, max_keys)
        })
        .await
    }
}
