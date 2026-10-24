// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Writes on their way to storage, owned apart from their callers so they can land after a crash.

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dexzd_object_store::{InMemoryObjectStore, ObjectStore, ObjectStoreResult};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::engine_simulation::event_trace::EventTrace;
use crate::engine_simulation::simulated_object_store::key_class;
use crate::engine_simulation::simulated_object_store::storage_observations::StorageObservations;

/// What the fault layers of every process share.
pub(crate) struct SharedObjectStorage {
    pub(crate) store: Arc<InMemoryObjectStore>,
    pub(crate) trace: Arc<EventTrace>,
    pub(crate) observations: Arc<Mutex<StorageObservations>>,
    /// The process whose lease write landed last.
    lease_writer: watch::Sender<Option<String>>,
    /// The process that started serving last.
    serving_process: watch::Sender<Option<String>>,
    /// Writes that have not landed yet, including those of processes that died.
    in_flight: Mutex<JoinSet<()>>,
}

impl SharedObjectStorage {
    pub(crate) fn new(
        store: Arc<InMemoryObjectStore>,
        trace: Arc<EventTrace>,
        observations: Arc<Mutex<StorageObservations>>,
    ) -> Self {
        Self {
            store,
            trace,
            observations,
            lease_writer: watch::Sender::new(None),
            serving_process: watch::Sender::new(None),
            in_flight: Mutex::new(JoinSet::new()),
        }
    }

    pub(super) fn observe(&self, record: impl FnOnce(&mut StorageObservations)) {
        record(
            &mut self
                .observations
                .lock()
                .expect("storage observation lock is not poisoned"),
        );
    }

    pub(super) fn mark_serving(&self, process_label: &str) {
        self.serving_process
            .send_replace(Some(process_label.to_owned()));
    }

    /// Drops the writes that never landed, at the end of a seed.
    pub(crate) fn abort_in_flight_writes(&self) {
        self.in_flight
            .lock()
            .expect("in-flight write lock is not poisoned")
            .abort_all();
    }

    fn spawn_landing(&self, landing: impl Future<Output = ()> + Send + 'static) {
        let mut in_flight = self
            .in_flight
            .lock()
            .expect("in-flight write lock is not poisoned");
        while let Some(finished) = in_flight.try_join_next() {
            if let Err(error) = finished
                && error.is_panic()
            {
                std::panic::resume_unwind(error.into_panic());
            }
        }
        in_flight.spawn(landing);
    }
}

/// When a write lands if its process dies before it reaches storage.
#[derive(Debug, Clone, Copy)]
pub(super) enum CrashLanding {
    /// At the time it would have landed anyway.
    AtDeadline,
    /// This long after another process's lease write lands, so during the successor's recovery.
    AfterSuccessorLease(Duration),
    /// This long after another process starts serving, so before or after its next log write.
    AfterSuccessorServes(Duration),
}

/// One write between its caller and storage.
pub(super) struct WriteLanding {
    pub(super) storage: Arc<SharedObjectStorage>,
    pub(super) process_label: String,
    pub(super) key: String,
    /// When the write reaches storage while its process lives.
    pub(super) deadline: Instant,
    pub(super) crash_landing: CrashLanding,
    /// Turns true, or closes, when the process dies.
    pub(super) cut_off: watch::Receiver<bool>,
    /// The process's count of log writes that have not landed, for log writes.
    pub(super) unlanded_log_writes: Option<Arc<AtomicUsize>>,
}

impl WriteLanding {
    /// Starts the write on its own task and returns where its result arrives.
    pub(super) fn send<T, F, Fut>(
        self,
        effect: F,
        is_written: fn(&ObjectStoreResult<T>) -> bool,
    ) -> oneshot::Receiver<ObjectStoreResult<T>>
    where
        T: Send + 'static,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ObjectStoreResult<T>> + Send + 'static,
    {
        let (sender, receiver) = oneshot::channel();
        if let Some(unlanded) = &self.unlanded_log_writes {
            unlanded.fetch_add(1, Ordering::SeqCst);
        }
        let storage = Arc::clone(&self.storage);
        storage.spawn_landing(self.land(effect, is_written, sender));
        receiver
    }

    async fn land<T, F, Fut>(
        mut self,
        effect: F,
        is_written: fn(&ObjectStoreResult<T>) -> bool,
        sender: oneshot::Sender<ObjectStoreResult<T>>,
    ) where
        F: FnOnce() -> Fut,
        Fut: Future<Output = ObjectStoreResult<T>>,
    {
        let has_died = tokio::select! {
            biased;
            () = tokio::time::sleep_until(self.deadline) => false,
            // A closed channel means the process is gone too.
            _ = self.cut_off.wait_for(|is_cut_off| *is_cut_off) => true,
        };
        if has_died {
            self.wait_after_crash().await;
        }
        let outcome = effect().await;
        if let Some(unlanded) = &self.unlanded_log_writes {
            unlanded.fetch_sub(1, Ordering::SeqCst);
        }
        if is_written(&outcome) {
            self.record_landed(has_died).await;
        }
        // The caller is gone when its process died; the write landed anyway.
        let _caller_gone = sender.send(outcome);
    }

    async fn wait_after_crash(&self) {
        let (mut successor, linger) = match self.crash_landing {
            CrashLanding::AtDeadline => {
                tokio::time::sleep_until(self.deadline).await;
                return;
            }
            CrashLanding::AfterSuccessorLease(linger) => {
                (self.storage.lease_writer.subscribe(), linger)
            }
            CrashLanding::AfterSuccessorServes(linger) => {
                (self.storage.serving_process.subscribe(), linger)
            }
        };
        let process_label = &self.process_label;
        // A closed channel only happens at the end of the seed, which drops the landing anyway.
        let _is_seed_ending = successor
            .wait_for(|other| other.as_ref().is_some_and(|other| other != process_label))
            .await
            .is_err();
        tokio::time::sleep(linger).await;
    }

    async fn record_landed(&self, has_died: bool) {
        let class = key_class(&self.key);
        if has_died {
            self.storage.observe(|observations| {
                *observations
                    .writes_landed_after_crash
                    .entry(class)
                    .or_default() += 1;
            });
            self.storage.trace.record(&format!(
                "{} write {} landed after the process died",
                self.process_label, self.key
            ));
        }
        match class {
            "lease" => self.record_lease_written(),
            "log" => self.record_log_batch_landed().await,
            _ => {}
        }
    }

    fn record_lease_written(&self) {
        let mut superseded = Vec::new();
        self.storage.observe(|observations| {
            superseded = observations
                .old_epoch
                .record_lease_written(&self.process_label);
        });
        for old_process in superseded {
            self.storage.trace.record(&format!(
                "{} holds the lease while {old_process} still runs",
                self.process_label
            ));
        }
        self.storage
            .lease_writer
            .send_replace(Some(self.process_label.clone()));
    }

    /// Counts an EpochStart that landed below a batch already in its stream.
    async fn record_log_batch_landed(&self) {
        let store = &self.storage.store;
        let Ok(Some(object)) = store.get(&self.key).await else {
            return;
        };
        let Ok(batch) = dexzd_log::decode_batch(&object.bytes) else {
            return;
        };
        if batch.header.epoch_start.is_none() {
            return;
        }
        let Some((stream_prefix, _)) = self.key.rsplit_once('/') else {
            return;
        };
        let later = store
            .list(&format!("{stream_prefix}/"), Some(&self.key), 1)
            .await;
        if later.is_ok_and(|listing| !listing.objects.is_empty()) {
            self.storage.observe(|observations| {
                observations.epoch_starts_below_landed_batches += 1;
            });
            self.storage.trace.record(&format!(
                "{} EpochStart {} landed below a later batch",
                self.process_label, self.key
            ));
        }
    }
}
