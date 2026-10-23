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
use dexzd_object_store::{ObjectStore, ObjectStoreErrorKind, WriteCondition, WriteOutcome};
use dexzd_protocol::dexzd::v1::NodeLease;
use prost::Message;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use uuid::Uuid;

use crate::{MetadataError, NodeConfig, bump_node_epoch};

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

/// Local monotonic lease validity observed by the engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseStatus {
    /// Serving is allowed only until this instant.
    ValidUntil(Instant),
    /// Serving must stop immediately.
    Lost(String),
}

/// Owns lease renewal and publishes lease validity.
pub struct LeaseHandle {
    epoch: u64,
    status: watch::Receiver<LeaseStatus>,
    renewal_task: Option<JoinHandle<()>>,
}

impl LeaseHandle {
    /// Returns the node epoch assigned after lease acquisition.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Subscribes to local monotonic lease validity.
    pub fn subscribe(&self) -> watch::Receiver<LeaseStatus> {
        self.status.clone()
    }

    /// Stops lease renewal before shutting down the node.
    pub async fn shutdown(mut self) {
        if let Some(task) = self.renewal_task.take() {
            task.abort();
            if let Err(error) = task.await
                && !error.is_cancelled()
            {
                tracing::warn!(error = %error, "lease renewal task failed during shutdown");
            }
        }
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
pub async fn acquire_lease(
    store: Arc<dyn ObjectStore>,
    clock: Arc<dyn WallClock>,
    node: &NodeConfig,
    config: &LeaseConfig,
) -> Result<LeaseHandle, MetadataError> {
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
    let process_id = Uuid::new_v4().to_string();
    loop {
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
                tokio::time::sleep(Duration::from_micros((available_at - now) as u64)).await;
                continue;
            }
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
        if Instant::now() >= valid_until {
            return Err(MetadataError::LeaseLost {
                node_id: node.node_id.clone(),
                detail: "acquisition completed after local lease deadline".to_owned(),
            });
        }
        let (status, mut receiver) = watch::channel(LeaseStatus::ValidUntil(valid_until));
        let mut owner = LeaseOwner {
            store,
            clock,
            key,
            lease,
            etag,
            duration: config.duration,
            last_request_sent: sent_at,
            current_valid_until: valid_until,
            status,
        };
        let epoch = bump_node_epoch(owner.store.as_ref(), owner.clock.as_ref(), node).await?;
        if Instant::now() >= valid_until {
            return Err(MetadataError::LeaseLost {
                node_id: node.node_id.clone(),
                detail: "epoch update completed after local lease deadline".to_owned(),
            });
        }
        owner.lease.epoch = epoch;
        owner.renew_before_deadline().await?;
        drop(receiver.borrow_and_update());
        let renewal_task = tokio::spawn(async move { owner.run().await });
        return Ok(LeaseHandle {
            epoch,
            status: receiver,
            renewal_task: Some(renewal_task),
        });
    }
}

impl LeaseOwner {
    async fn run(mut self) {
        loop {
            tokio::time::sleep_until(self.last_request_sent + self.duration / 3).await;
            if let Err(error) = self.renew_before_deadline().await {
                drop(
                    self.status
                        .send_replace(LeaseStatus::Lost(error.to_string())),
                );
                return;
            }
        }
    }

    async fn renew_before_deadline(&mut self) -> Result<(), MetadataError> {
        match tokio::time::timeout_at(self.current_valid_until, self.renew()).await {
            Ok(result) => result,
            Err(_) => Err(self.lost("renewal exceeded local lease deadline")),
        }
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
