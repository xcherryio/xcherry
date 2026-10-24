// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use dexzd_object_store::{
    InMemoryObjectStore, ObjectStore, ObjectStoreErrorKind, ObjectStoreOperation, WriteCondition,
    WriteOutcome,
};

use super::{
    CallPlan, CrashLanding, FaultEffect, InjectedFault, NodeObjectStore, SharedObjectStorage,
    StorageObservations, is_put_written,
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

/// Shared storage over an empty store; call it inside the runtime.
fn shared_storage() -> Arc<SharedObjectStorage> {
    Arc::new(SharedObjectStorage::new(
        Arc::new(InMemoryObjectStore::new()),
        Arc::new(EventTrace::new(tokio::time::Instant::now())),
        Arc::new(Mutex::new(StorageObservations::default())),
    ))
}

fn layer_for(process_label: &str, storage: &Arc<SharedObjectStorage>) -> Arc<NodeObjectStore> {
    Arc::new(NodeObjectStore::new(
        Arc::clone(storage),
        process_label.to_owned(),
        "first".to_owned(),
        0,
    ))
}

/// Runs `test` on a paused current-thread runtime with a layer over an empty shared store.
fn with_layer(test: impl AsyncFnOnce(&NodeObjectStore, &InMemoryObjectStore)) {
    paused_runtime().block_on(async {
        let storage = shared_storage();
        let layer = layer_for("p1", &storage);
        test(&layer, &storage.store).await;
    });
}

fn plan(fault: Option<InjectedFault>, is_effect_applied: bool) -> CallPlan {
    CallPlan {
        fault,
        delay: Duration::from_millis(5),
        is_effect_applied,
        crash_landing: CrashLanding::AfterSuccessorLease(Duration::from_millis(100)),
    }
}

async fn put_with_plan(
    layer: &NodeObjectStore,
    plan: CallPlan,
    body: &'static [u8],
) -> dexzd_object_store::ObjectStoreResult<WriteOutcome> {
    let store = Arc::clone(&layer.storage.store);
    layer
        .run_write_with_plan(
            plan,
            ObjectStoreOperation::Put,
            KEY,
            move || async move {
                store
                    .put(KEY, Bytes::from_static(body), WriteCondition::CreateOnly)
                    .await
            },
            is_put_written,
        )
        .await
}

fn observe<T>(layer: &NodeObjectStore, read: impl FnOnce(&StorageObservations) -> T) -> T {
    read(
        &layer
            .storage
            .observations
            .lock()
            .expect("storage observation lock is not poisoned"),
    )
}

fn observed_faults(layer: &NodeObjectStore) -> Vec<(InjectedFault, FaultEffect)> {
    observe(layer, |observations| {
        observations.injected_faults.keys().copied().collect()
    })
}

async fn put_directly(store: &InMemoryObjectStore, key: &str, body: &'static [u8]) {
    let outcome = store
        .put(key, Bytes::from_static(body), WriteCondition::CreateOnly)
        .await;
    assert!(matches!(outcome, Ok(WriteOutcome::Written { .. })));
}

#[test]
fn a_lost_response_of_a_failed_precondition_returns_the_failure() {
    with_layer(async |layer, shared| {
        put_directly(shared, KEY, b"first").await;
        let outcome = put_with_plan(
            layer,
            plan(Some(InjectedFault::ResponseLost), true),
            b"second",
        )
        .await;
        assert!(matches!(outcome, Ok(WriteOutcome::PreconditionFailed)));
        assert!(observed_faults(layer).is_empty());
    });
}

#[test]
fn a_lost_response_of_a_write_is_an_unknown_outcome() {
    with_layer(async |layer, shared| {
        let outcome = put_with_plan(
            layer,
            plan(Some(InjectedFault::ResponseLost), true),
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
            observed_faults(layer),
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
        put_directly(shared, KEY, b"first").await;
        let outcome =
            put_with_plan(layer, plan(Some(InjectedFault::Timeout), true), b"second").await;
        let error = outcome.expect_err("the client timed out");
        assert_eq!(error.kind, ObjectStoreErrorKind::OutcomeUnknown);
        assert_eq!(
            observed_faults(layer),
            vec![(
                InjectedFault::Timeout,
                FaultEffect::NotWrittenOutcomeUnknown
            )]
        );
    });
}

#[test]
fn a_write_lands_after_its_process_and_caller_are_gone() {
    paused_runtime().block_on(async {
        let storage = shared_storage();
        let dead = layer_for("p1", &storage);
        let successor = layer_for("p2", &storage);
        let caller_layer = Arc::clone(&dead);
        let caller =
            tokio::spawn(
                async move { put_with_plan(&caller_layer, plan(None, true), b"late").await },
            );
        tokio::time::sleep(Duration::from_millis(1)).await;
        dead.cut_off();
        caller.abort();
        assert!(caller.await.is_err_and(|error| error.is_cancelled()));
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert!(storage.store.get(KEY).await.expect("reads").is_none());

        let lease = successor
            .put(
                "lease/node-a",
                Bytes::from_static(b"p2"),
                WriteCondition::CreateOnly,
            )
            .await;
        assert!(matches!(lease, Ok(WriteOutcome::Written { .. })));
        tokio::time::sleep(Duration::from_millis(200)).await;
        let landed = storage.store.get(KEY).await.expect("reads");
        assert_eq!(
            landed.map(|object| object.bytes),
            Some(Bytes::from_static(b"late"))
        );
        let landed_after_crash = observe(&dead, |observations| {
            observations.writes_landed_after_crash.get("log").copied()
        });
        assert_eq!(landed_after_crash, Some(1));
    });
}

#[test]
fn old_epoch_counts_start_when_the_successor_lease_write_takes_effect() {
    paused_runtime().block_on(async {
        let storage = shared_storage();
        let old = layer_for("p1", &storage);
        let successor = layer_for("p2", &storage);
        let old_epoch = || {
            observe(&old, |observations| {
                let windows = &observations.old_epoch;
                (
                    windows.overlaps,
                    windows.write_attempts.get("log").copied().unwrap_or(0),
                    windows.frames,
                )
            })
        };
        let record_frame = || {
            storage
                .observations
                .lock()
                .expect("storage observation lock is not poisoned")
                .old_epoch
                .record_frame("p1");
        };
        storage
            .observations
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
        record_frame();
        assert_eq!(old_epoch(), (0, 0, 0));

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
        record_frame();
        assert_eq!(old_epoch(), (1, 1, 1));
    });
}
