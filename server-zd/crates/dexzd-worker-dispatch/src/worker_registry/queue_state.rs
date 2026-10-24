// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Queue lifetime and bounded same-Flow affinity.

use super::*;
use prometheus_client::metrics::gauge::Gauge;

pub(super) struct QueuedTaskMetric {
    gauge: Gauge,
    is_waiting: bool,
}

impl QueuedTaskMetric {
    pub(super) fn new(gauge: Gauge) -> Self {
        gauge.inc();
        Self {
            gauge,
            is_waiting: true,
        }
    }

    pub(super) fn set_waiting(&mut self, should_wait: bool) {
        if should_wait && !self.is_waiting {
            self.gauge.inc();
        } else if !should_wait && self.is_waiting {
            self.gauge.dec();
        }
        self.is_waiting = should_wait;
    }
}

impl Drop for QueuedTaskMetric {
    fn drop(&mut self) {
        if self.is_waiting {
            self.gauge.dec();
        }
    }
}

impl Drop for QueueLease {
    fn drop(&mut self) {
        self.dispatch.release_queue(&self.key);
    }
}

impl QueueState {
    pub(super) fn new() -> Self {
        let (changes, _) = watch::channel(0);
        Self {
            stream_ids: HashSet::new(),
            idle: VecDeque::new(),
            affinity: HashMap::new(),
            affinity_order: VecDeque::new(),
            changes,
            users: 0,
        }
    }

    pub(super) fn signal(&self) {
        self.changes.send_modify(|generation| *generation += 1);
    }

    pub(super) fn remember(&mut self, flow_id: &str, worker_id: &str, duration: Duration) {
        if flow_id.is_empty() {
            return;
        }
        let now = Instant::now();
        let expiry = now + duration;
        self.affinity
            .insert(flow_id.to_owned(), (worker_id.to_owned(), expiry));
        self.affinity_order.push_back((flow_id.to_owned(), expiry));
        while let Some((oldest_id, oldest_expiry)) = self.affinity_order.front() {
            let current = self
                .affinity
                .get(oldest_id)
                .map(|(_, current_expiry)| *current_expiry);
            if current == Some(*oldest_expiry) && *oldest_expiry > now {
                break;
            }
            if current == Some(*oldest_expiry) {
                self.affinity.remove(oldest_id);
            }
            self.affinity_order.pop_front();
        }
        while self.affinity.len() > 10_000 {
            let (oldest_id, oldest_expiry) = self
                .affinity_order
                .pop_front()
                .expect("affinity order has current entries");
            if self
                .affinity
                .get(&oldest_id)
                .is_some_and(|(_, current_expiry)| *current_expiry == oldest_expiry)
            {
                self.affinity.remove(&oldest_id);
            }
        }
        if self.affinity_order.len() > 20_000 {
            let affinity = &self.affinity;
            self.affinity_order.retain(|(flow_id, expiry)| {
                affinity
                    .get(flow_id)
                    .is_some_and(|(_, current_expiry)| current_expiry == expiry)
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dexzd_clock::SystemWallClock;
    use dexzd_protocol::dex::InvokeWaitForMethodRequest;

    #[tokio::test]
    async fn empty_queue_leases_and_absent_sticky_queues_are_removed() {
        let dispatch = WorkerDispatch::new(MatchingConfig::default(), Arc::new(SystemWallClock));
        let lease = dispatch.acquire_queue(0, "Flow");
        assert_eq!(
            dispatch
                .inner
                .registry
                .lock()
                .expect("registry lock")
                .queues
                .len(),
            1
        );
        drop(lease);
        assert!(
            dispatch
                .inner
                .registry
                .lock()
                .expect("registry lock")
                .queues
                .is_empty()
        );
        for index in 0..64 {
            let mut handle = dispatch
                .dispatch(DispatchRequest {
                    tenant_id: 0,
                    flow_id: format!("flow-{index}"),
                    flow_type: "Flow".to_owned(),
                    sticky_app_worker_id: Some(format!("absent-{index}")),
                    payload: DispatchTaskPayload::WaitFor(InvokeWaitForMethodRequest::default()),
                    deadline: SystemTime::now() + Duration::from_secs(1),
                    sticky_wait: Some(Duration::ZERO),
                    dispatch_wait: Some(Duration::from_millis(1)),
                })
                .expect("dispatch starts");
            assert!(matches!(
                handle.next().await,
                Some(DispatchEvent::Completed(DispatchOutcome::DispatchError {
                    reason: DispatchTaskErrorReason::NoWorkerAvailable,
                    ..
                }))
            ));
        }
        assert!(
            dispatch
                .inner
                .registry
                .lock()
                .expect("registry lock")
                .queues
                .is_empty()
        );
    }

    #[test]
    fn last_worker_disconnect_removes_its_queue() {
        let dispatch = WorkerDispatch::new(MatchingConfig::default(), Arc::new(SystemWallClock));
        let (output, _) = mpsc::unbounded_channel();
        let (close, _) = watch::channel(false);
        let worker = Arc::new(WorkerStream {
            id: Uuid::now_v7(),
            tenant_id: 7,
            queue: "Flow".to_owned(),
            app_worker_id: "worker".to_owned(),
            output,
            close,
            state: Mutex::new(WorkerState::default()),
        });
        {
            let mut registry = dispatch.inner.registry.lock().expect("registry lock");
            let queue = registry
                .queues
                .entry((7, "Flow".to_owned()))
                .or_insert_with(QueueState::new);
            queue.stream_ids.insert(worker.id);
            queue.idle.push_back(worker.id);
            queue.users += 1;
            registry.worker_counts.insert((7, "worker".to_owned()), 1);
            registry.streams.insert(worker.id, worker.clone());
        }
        dispatch.disconnect(worker.id);
        let registry = dispatch.inner.registry.lock().expect("registry lock");
        assert!(registry.queues.is_empty());
        assert!(registry.worker_counts.is_empty());
    }

    #[test]
    fn affinity_order_evicts_old_flows_and_compacts_repeated_updates() {
        let mut queue = QueueState::new();
        for index in 0..10_001 {
            queue.remember(&format!("flow-{index}"), "worker", Duration::from_secs(30));
        }
        assert_eq!(queue.affinity.len(), 10_000);
        assert!(!queue.affinity.contains_key("flow-0"));
        assert!(queue.affinity.contains_key("flow-10000"));
        for _ in 0..25_000 {
            queue.remember("hot-flow", "worker", Duration::from_secs(30));
        }
        assert!(queue.affinity_order.len() <= 20_000);
    }
}
