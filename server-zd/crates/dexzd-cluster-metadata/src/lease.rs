// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use dexzd_clock::WallClock;
use dexzd_keyspace::validate_node_identifier;
use dexzd_metrics::{NodeMetrics, Outcome};
use dexzd_object_store::{ObjectStore, ObjectStoreErrorKind, WriteCondition, WriteOutcome};
use dexzd_protocol::dexzd::v1::NodeLease;
use prost::Message;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use uuid::Uuid;

use crate::{MetadataError, NodeConfig, bump_node_epoch};

const RELEASED_LEASE_EXPIRY_UNIX_MICROS: i64 = i64::MIN;
const RELEASE_RETRY_DELAY: Duration = Duration::from_millis(10);
/// Longest wait between reads of a lease another process holds.
const HELD_LEASE_POLL_LIMIT: Duration = Duration::from_secs(1);

/// Timing and contention policy for a node lease.
#[derive(Debug, Clone)]
pub struct LeaseConfig {
    /// Lease duration; defaults to ten seconds.
    pub duration: Duration,
    /// Additional wait after an old lease expires before takeover.
    pub takeover_grace: Duration,
    /// Return immediately when another live process holds the lease.
    pub fail_fast: bool,
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            duration: Duration::from_secs(10),
            takeover_grace: Duration::from_secs(1),
            fail_fast: false,
        }
    }
}

impl LeaseConfig {
    /// Acquires the lease, or returns `None` if cancellation arrives before its write begins.
    pub async fn acquire_until_cancelled(
        &self,
        store: Arc<dyn ObjectStore>,
        clock: Arc<dyn WallClock>,
        node: &NodeConfig,
        cancellation: &mut watch::Receiver<bool>,
    ) -> Result<Option<LeaseHandle>, MetadataError> {
        self.acquire_until_cancelled_with_holder_process_id(
            store,
            clock,
            node,
            cancellation,
            Uuid::new_v4().to_string(),
        )
        .await
    }

    /// Acquires the lease like [`Self::acquire_until_cancelled`], naming `holder_process_id` as
    /// its holder.
    pub async fn acquire_until_cancelled_with_holder_process_id(
        &self,
        store: Arc<dyn ObjectStore>,
        clock: Arc<dyn WallClock>,
        node: &NodeConfig,
        cancellation: &mut watch::Receiver<bool>,
        holder_process_id: String,
    ) -> Result<Option<LeaseHandle>, MetadataError> {
        acquire_lease_inner(
            store,
            clock,
            node,
            self,
            Some(cancellation),
            holder_process_id,
            None,
        )
        .await
    }

    /// Acquires a process-specific lease and records its renewal outcomes.
    pub async fn acquire_until_cancelled_with_metrics(
        &self,
        store: Arc<dyn ObjectStore>,
        clock: Arc<dyn WallClock>,
        node: &NodeConfig,
        cancellation: &mut watch::Receiver<bool>,
        holder_process_id: String,
        metrics: NodeMetrics,
    ) -> Result<Option<LeaseHandle>, MetadataError> {
        acquire_lease_inner(
            store,
            clock,
            node,
            self,
            Some(cancellation),
            holder_process_id,
            Some(metrics),
        )
        .await
    }
}

/// Local monotonic lease validity observed by the engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseStatus {
    /// Serving is allowed only until this instant.
    ValidUntil(Instant),
    /// Serving must stop immediately.
    Lost(String),
}

/// Why this process could not confirm its node lease release; the lease stays held until expiry.
#[derive(Debug, thiserror::Error)]
pub enum LeaseReleaseError {
    /// The renewal task ended abnormally, so the state needed for the release write is gone.
    #[error("node {node_id:?} lease renewal task failed before the release: {detail}")]
    RenewalTaskFailed {
        /// Node ID.
        node_id: String,
        /// How the task ended.
        detail: String,
    },
    /// Object storage rejected the release write or its follow-up read.
    #[error("releasing node {node_id:?} lease failed: {source}")]
    Rejected {
        /// Node ID.
        node_id: String,
        /// The failed metadata operation.
        #[source]
        source: MetadataError,
    },
    /// Stopping renewal and writing the release did not finish within the budget.
    #[error("releasing node {node_id:?} lease did not finish within {budget:?}")]
    TimedOut {
        /// Node ID.
        node_id: String,
        /// The exceeded budget, [`LeaseHandle::RELEASE_BUDGET`].
        budget: Duration,
    },
}

/// Owns lease renewal and publishes lease validity.
///
/// Dropping it unreleased leaves the lease to expire, like a crash.
pub struct LeaseHandle {
    node_id: String,
    epoch: u64,
    status: watch::Receiver<LeaseStatus>,
    renewal_task: Option<JoinHandle<LeaseOwner>>,
    shutdown_sender: Option<oneshot::Sender<()>>,
}

impl LeaseHandle {
    /// Longest time [`LeaseHandle::shutdown`] spends stopping renewal and writing the release.
    pub const RELEASE_BUDGET: Duration = Duration::from_secs(2);

    /// Returns the node epoch assigned after lease acquisition.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Subscribes to local monotonic lease validity.
    pub fn subscribe(&self) -> watch::Receiver<LeaseStatus> {
        self.status.clone()
    }

    /// Stops renewal and releases the lease after the node stops serving.
    /// Subscribers observe `Lost` before the release write.
    ///
    /// Returns `Ok` once this process no longer holds the lease: the release was written, or another
    /// process had already taken the lease over. Returns [`LeaseReleaseError`] when the release could
    /// not be confirmed within [`LeaseHandle::RELEASE_BUDGET`]; the lease then stays held until it
    /// expires.
    pub async fn shutdown(mut self) -> Result<(), LeaseReleaseError> {
        let stopped =
            tokio::time::timeout(Self::RELEASE_BUDGET, self.stop_renewal_and_release()).await;
        stopped.unwrap_or_else(|_| {
            Err(LeaseReleaseError::TimedOut {
                node_id: self.node_id.clone(),
                budget: Self::RELEASE_BUDGET,
            })
        })
    }

    async fn stop_renewal_and_release(&mut self) -> Result<(), LeaseReleaseError> {
        if let Some(sender) = self.shutdown_sender.take()
            && sender.send(()).is_err()
        {
            tracing::debug!("lease renewal task stopped before shutdown");
        }
        let task = self
            .renewal_task
            .as_mut()
            .expect("only shutdown takes the renewal task, and it consumes the handle");
        let owner_result = task.await;
        drop(self.renewal_task.take());
        let owner = owner_result.map_err(|error| LeaseReleaseError::RenewalTaskFailed {
            node_id: self.node_id.clone(),
            detail: error.to_string(),
        })?;
        drop(owner.status.send_replace(LeaseStatus::Lost(
            "lease released during shutdown".to_owned(),
        )));
        owner
            .release()
            .await
            .map_err(|source| LeaseReleaseError::Rejected {
                node_id: self.node_id.clone(),
                source,
            })
    }
}

impl Drop for LeaseHandle {
    fn drop(&mut self) {
        if let Some(task) = self.renewal_task.take() {
            task.abort();
        }
    }
}

struct LeaseOwner {
    metrics: Option<NodeMetrics>,
    store: Arc<dyn ObjectStore>,
    clock: Arc<dyn WallClock>,
    key: String,
    lease: NodeLease,
    etag: String,
    duration: Duration,
    last_request_sent: Instant,
    current_valid_until: Instant,
    status: watch::Sender<LeaseStatus>,
}

/// Acquires a process-specific node lease, raises its epoch, and starts renewal.
///
/// Polls a held lease at most every second, so a released lease is taken at once.
/// Each wait is randomized between half and all of that interval, so waiters spread their reads.
pub async fn acquire_lease(
    store: Arc<dyn ObjectStore>,
    clock: Arc<dyn WallClock>,
    node: &NodeConfig,
    config: &LeaseConfig,
) -> Result<LeaseHandle, MetadataError> {
    acquire_lease_with_holder_process_id(store, clock, node, config, Uuid::new_v4().to_string())
        .await
}

/// Acquires the lease like [`acquire_lease`], naming `holder_process_id` as its holder.
pub async fn acquire_lease_with_holder_process_id(
    store: Arc<dyn ObjectStore>,
    clock: Arc<dyn WallClock>,
    node: &NodeConfig,
    config: &LeaseConfig,
    holder_process_id: String,
) -> Result<LeaseHandle, MetadataError> {
    let acquired =
        acquire_lease_inner(store, clock, node, config, None, holder_process_id, None).await?;
    Ok(acquired.expect("a lease acquisition without cancellation must complete"))
}

async fn acquire_lease_inner(
    store: Arc<dyn ObjectStore>,
    clock: Arc<dyn WallClock>,
    node: &NodeConfig,
    config: &LeaseConfig,
    mut cancellation: Option<&mut watch::Receiver<bool>>,
    process_id: String,
    metrics: Option<NodeMetrics>,
) -> Result<Option<LeaseHandle>, MetadataError> {
    let duration_micros =
        i64::try_from(config.duration.as_micros()).map_err(|_| MetadataError::InvalidConfig {
            field: "lease duration",
            detail: "lease duration exceeds i64 microseconds".to_owned(),
        })?;
    if duration_micros <= 0 || config.duration / 3 == Duration::ZERO {
        return Err(MetadataError::InvalidConfig {
            field: "lease duration",
            detail: "lease duration must permit a positive renewal interval".to_owned(),
        });
    }
    validate_node_identifier(&node.node_id)?;
    let grace_micros = i64::try_from(config.takeover_grace.as_micros()).map_err(|_| {
        MetadataError::InvalidConfig {
            field: "lease takeover grace",
            detail: "takeover grace exceeds i64 microseconds".to_owned(),
        }
    })?;
    let key = format!("lease/{}", node.node_id);
    let held_lease_poll_interval = HELD_LEASE_POLL_LIMIT.min(config.duration / 3);
    let mut has_reported_wait = false;
    loop {
        if cancellation
            .as_ref()
            .is_some_and(|receiver| *receiver.borrow())
        {
            return Ok(None);
        }
        let existing = store.get(&key).await?;
        let previous = existing
            .as_ref()
            .map(|content| {
                NodeLease::decode(content.bytes.clone()).map_err(|source| MetadataError::Decode {
                    key: "lease",
                    source,
                })
            })
            .transpose()?;
        let sent_at = Instant::now();
        let now = clock.now_unix_micros();
        if let Some(previous) = &previous {
            if previous.node_id != node.node_id {
                return Err(MetadataError::InvalidMetadata {
                    key: "lease",
                    detail: "lease node ID disagrees with its object key".to_owned(),
                });
            }
            let available_at = previous.expires_at_unix_micros.saturating_add(grace_micros);
            if now < available_at {
                if config.fail_fast {
                    return Err(MetadataError::LeaseHeld {
                        node_id: node.node_id.clone(),
                        expires_at_unix_micros: previous.expires_at_unix_micros,
                    });
                }
                if !has_reported_wait {
                    tracing::info!(
                        node_id = %node.node_id,
                        holder_process_id = %previous.holder_process_id,
                        expires_at_unix_micros = previous.expires_at_unix_micros,
                        "another process holds the node lease; waiting for its release or expiry"
                    );
                    has_reported_wait = true;
                }
                let until_available = Duration::from_micros((available_at - now) as u64);
                let poll_delay = jittered_poll_delay(held_lease_poll_interval);
                let delay = until_available.min(poll_delay);
                if let Some(cancellation_receiver) = cancellation.as_mut() {
                    tokio::select! {
                        () = tokio::time::sleep(delay) => {}
                        changed = cancellation_receiver.changed() => {
                            if changed.is_ok() && *cancellation_receiver.borrow() {
                                return Ok(None);
                            }
                            if changed.is_err() {
                                tokio::time::sleep(delay).await;
                            }
                        }
                    }
                } else {
                    tokio::time::sleep(delay).await;
                }
                continue;
            }
        }
        if cancellation
            .as_ref()
            .is_some_and(|receiver| *receiver.borrow())
        {
            return Ok(None);
        }
        let version = previous
            .as_ref()
            .map_or(0, |lease| lease.version)
            .checked_add(1)
            .ok_or(MetadataError::Overflow {
                field: "node lease version",
            })?;
        let lease = NodeLease {
            node_id: node.node_id.clone(),
            incarnation: node.incarnation,
            epoch: previous.as_ref().map_or(0, |lease| lease.epoch),
            holder_process_id: process_id.clone(),
            expires_at_unix_micros: now.checked_add(duration_micros).ok_or(
                MetadataError::Overflow {
                    field: "lease expiry",
                },
            )?,
            duration_micros,
            version,
        };
        let condition = existing.map_or(WriteCondition::CreateOnly, |content| {
            WriteCondition::ReplaceIfEtagMatches { etag: content.etag }
        });
        let write_result = store
            .put(&key, Bytes::from(lease.encode_to_vec()), condition)
            .await;
        let etag = match write_result {
            Ok(WriteOutcome::Written { etag }) => etag,
            Ok(WriteOutcome::PreconditionFailed) => continue,
            Err(error) if error.kind == ObjectStoreErrorKind::OutcomeUnknown => {
                let current = store.get(&key).await?;
                match current {
                    Some(content)
                        if NodeLease::decode(content.bytes.clone()).map_err(|source| {
                            MetadataError::Decode {
                                key: "lease",
                                source,
                            }
                        })? == lease =>
                    {
                        content.etag
                    }
                    _ => continue,
                }
            }
            Err(error) => return Err(error.into()),
        };
        let valid_until = sent_at + config.duration;
        let (status, mut receiver) = watch::channel(LeaseStatus::ValidUntil(valid_until));
        let mut pending = PendingLease {
            owner: Some(LeaseOwner {
                metrics: metrics.clone(),
                store,
                clock,
                key,
                lease,
                etag,
                duration: config.duration,
                last_request_sent: sent_at,
                current_valid_until: valid_until,
                status,
            }),
        };
        let epoch = match pending.owner_mut().finish_acquisition(node).await {
            Ok(epoch) => epoch,
            Err(error) => return Err(pending.release_after(error).await),
        };
        let owner = pending.take_owner();
        drop(receiver.borrow_and_update());
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let renewal_task = tokio::spawn(async move { owner.run(shutdown_receiver).await });
        return Ok(Some(LeaseHandle {
            node_id: node.node_id.clone(),
            epoch,
            status: receiver,
            renewal_task: Some(renewal_task),
            shutdown_sender: Some(shutdown_sender),
        }));
    }
}

/// A written lease still being acquired; dropping it releases the lease in the background.
struct PendingLease {
    owner: Option<LeaseOwner>,
}

impl PendingLease {
    fn owner_mut(&mut self) -> &mut LeaseOwner {
        self.owner
            .as_mut()
            .expect("the owner stays until the acquisition finishes")
    }

    /// Disarms the guard and returns the owner.
    fn take_owner(&mut self) -> LeaseOwner {
        self.owner
            .take()
            .expect("the owner stays until the acquisition finishes")
    }

    /// Releases the lease, then returns `error`. The release continues if the caller is cancelled.
    async fn release_after(mut self, error: MetadataError) -> MetadataError {
        let owner = self.take_owner();
        let release_task = tokio::spawn(release_unfinished_acquisition(owner));
        if let Err(join_error) = release_task.await {
            tracing::error!(
                error = %join_error,
                "the node lease release task failed after an unfinished acquisition"
            );
        }
        error
    }
}

impl Drop for PendingLease {
    fn drop(&mut self) {
        let Some(owner) = self.owner.take() else {
            return;
        };
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                // Detached on purpose: the release must outlive the cancelled acquisition.
                let release_task = runtime.spawn(release_unfinished_acquisition(owner));
                drop(release_task);
            }
            Err(_) => tracing::error!(
                node_id = %owner.lease.node_id,
                "no async runtime can release the node lease; it stays held until it expires"
            ),
        }
    }
}

/// Releases the lease of a failed or cancelled acquisition within the release budget.
async fn release_unfinished_acquisition(owner: LeaseOwner) {
    let node_id = owner.lease.node_id.clone();
    match tokio::time::timeout(LeaseHandle::RELEASE_BUDGET, owner.release()).await {
        Ok(Ok(())) => tracing::info!(
            %node_id,
            "released the node lease after an unfinished acquisition"
        ),
        Ok(Err(error)) => tracing::error!(
            %node_id,
            %error,
            "the node lease stays held after an unfinished acquisition"
        ),
        Err(_) => tracing::error!(
            %node_id,
            budget_milliseconds = LeaseHandle::RELEASE_BUDGET.as_millis(),
            "releasing the node lease after an unfinished acquisition timed out"
        ),
    }
}

impl LeaseOwner {
    async fn run(mut self, mut shutdown_receiver: oneshot::Receiver<()>) -> Self {
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown_receiver => return self,
                () = tokio::time::sleep_until(self.last_request_sent + self.duration / 3) => {}
            }
            let renewal_result = tokio::select! {
                biased;
                _ = &mut shutdown_receiver => return self,
                result = self.renew_before_deadline() => result,
            };
            if let Err(error) = renewal_result {
                drop(
                    self.status
                        .send_replace(LeaseStatus::Lost(error.to_string())),
                );
                return self;
            }
        }
    }

    /// Raises the node epoch and renews once, both within the first lease period.
    ///
    /// Returns the raised epoch, or the error that ended the acquisition.
    async fn finish_acquisition(&mut self, node: &NodeConfig) -> Result<u64, MetadataError> {
        if Instant::now() >= self.current_valid_until {
            return Err(self.lost("acquisition completed after local lease deadline"));
        }
        let epoch = bump_node_epoch(self.store.as_ref(), self.clock.as_ref(), node).await?;
        if Instant::now() >= self.current_valid_until {
            return Err(self.lost("epoch update completed after local lease deadline"));
        }
        self.lease.epoch = epoch;
        self.renew_before_deadline().await?;
        Ok(epoch)
    }

    /// Writes an expired copy of the lease.
    ///
    /// Returns `Ok` once this process no longer holds the lease, including when another process
    /// already took it over, and the storage or decode error otherwise.
    async fn release(&self) -> Result<(), MetadataError> {
        let mut current_lease = self.lease.clone();
        let mut current_etag = self.etag.clone();
        loop {
            let mut released = current_lease.clone();
            released.version = released
                .version
                .checked_add(1)
                .ok_or(MetadataError::Overflow {
                    field: "node lease version",
                })?;
            released.expires_at_unix_micros = RELEASED_LEASE_EXPIRY_UNIX_MICROS;
            let outcome = self
                .store
                .put(
                    &self.key,
                    Bytes::from(released.encode_to_vec()),
                    WriteCondition::ReplaceIfEtagMatches { etag: current_etag },
                )
                .await;
            match outcome {
                Ok(WriteOutcome::Written { .. }) => return Ok(()),
                Ok(WriteOutcome::PreconditionFailed) => {}
                Err(error) if error.kind == ObjectStoreErrorKind::OutcomeUnknown => {}
                Err(error) => return Err(error.into()),
            }
            let Some(content) = self.store.get(&self.key).await? else {
                return Ok(());
            };
            let stored_lease =
                NodeLease::decode(content.bytes).map_err(|source| MetadataError::Decode {
                    key: "lease",
                    source,
                })?;
            if stored_lease == released
                || (stored_lease.holder_process_id == self.lease.holder_process_id
                    && stored_lease.expires_at_unix_micros == RELEASED_LEASE_EXPIRY_UNIX_MICROS)
            {
                return Ok(());
            }
            if stored_lease.holder_process_id != self.lease.holder_process_id {
                tracing::info!(
                    node_id = %self.lease.node_id,
                    "another process took the node lease over before the release"
                );
                return Ok(());
            }
            current_lease = stored_lease;
            current_etag = content.etag;
            tokio::time::sleep(RELEASE_RETRY_DELAY).await;
        }
    }

    async fn renew_before_deadline(&mut self) -> Result<(), MetadataError> {
        let result = match tokio::time::timeout_at(self.current_valid_until, self.renew()).await {
            Ok(result) => result,
            Err(_) => Err(self.lost("renewal exceeded local lease deadline")),
        };
        if let Some(metrics) = &self.metrics {
            metrics.record_lease_renewal(if result.is_ok() {
                Outcome::Ok
            } else {
                Outcome::Error
            });
        }
        result
    }

    async fn renew(&mut self) -> Result<(), MetadataError> {
        let mut proposed = self.lease.clone();
        proposed.version = proposed
            .version
            .checked_add(1)
            .ok_or(MetadataError::Overflow {
                field: "node lease version",
            })?;
        let sent_at = Instant::now();
        proposed.expires_at_unix_micros = self
            .clock
            .now_unix_micros()
            .checked_add(proposed.duration_micros)
            .ok_or(MetadataError::Overflow {
                field: "lease expiry",
            })?;
        let condition = WriteCondition::ReplaceIfEtagMatches {
            etag: self.etag.clone(),
        };
        let result = self
            .store
            .put(&self.key, Bytes::from(proposed.encode_to_vec()), condition)
            .await;
        let etag = match result {
            Ok(WriteOutcome::Written { etag }) => etag,
            Ok(WriteOutcome::PreconditionFailed) => {
                return Err(self.lost("renewal precondition failed"));
            }
            Err(error) if error.kind == ObjectStoreErrorKind::OutcomeUnknown => {
                let current = self.store.get(&self.key).await?;
                match current {
                    Some(content)
                        if NodeLease::decode(content.bytes.clone()).map_err(|source| {
                            MetadataError::Decode {
                                key: "lease",
                                source,
                            }
                        })? == proposed =>
                    {
                        content.etag
                    }
                    _ => return Err(self.lost("renewal outcome could not be confirmed")),
                }
            }
            Err(error) => return Err(error.into()),
        };
        let valid_until = sent_at + self.duration;
        if Instant::now() >= self.current_valid_until {
            return Err(self.lost("renewal completed after previous lease deadline"));
        }
        if Instant::now() >= valid_until {
            return Err(self.lost("renewal completed after local lease deadline"));
        }
        self.lease = proposed;
        self.etag = etag;
        self.last_request_sent = sent_at;
        self.current_valid_until = valid_until;
        drop(
            self.status
                .send_replace(LeaseStatus::ValidUntil(valid_until)),
        );
        Ok(())
    }

    fn lost(&self, detail: &str) -> MetadataError {
        MetadataError::LeaseLost {
            node_id: self.lease.node_id.clone(),
            detail: detail.to_owned(),
        }
    }
}

/// Returns a random delay between half of `interval` and all of it.
fn jittered_poll_delay(interval: Duration) -> Duration {
    interval - rand::random_range(Duration::ZERO..=interval / 2)
}
