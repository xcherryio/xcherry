// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, RwLock, RwLockReadGuard};

use dexzd_keyspace::PartitionId;
use tokio::sync::{OwnedSemaphorePermit, watch};

use crate::runtime::{CommitError, CommitTicketSender};

/// Why a node stopped serving.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeStopReason {
    /// Orderly shutdown was requested.
    Shutdown,
    /// The node lease was lost or expired; another process may own the log stream.
    LeaseLost {
        /// Lease status detail.
        detail: String,
    },
    /// A newer epoch wrote to this node's log stream.
    Fenced,
    /// The commit pipeline failed; uncommitted commands are lost and the node must recover.
    CommitPipelineFailed {
        /// Failure detail.
        detail: String,
    },
    /// The local store rejected a write; its state is no longer trusted.
    LocalStoreFailed {
        /// Store error.
        detail: String,
    },
}

impl fmt::Display for NodeStopReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Shutdown => formatter.write_str("shutdown requested"),
            Self::LeaseLost { detail } => write!(formatter, "node lease lost: {detail}"),
            Self::Fenced => formatter.write_str("log stream fenced by a newer epoch"),
            Self::CommitPipelineFailed { detail } => {
                write!(formatter, "commit pipeline failed: {detail}")
            }
            Self::LocalStoreFailed { detail } => write!(formatter, "local store failed: {detail}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NodeState {
    Recovering,
    Serving,
    Stopped(NodeStopReason),
}

pub(crate) struct PendingCommand {
    pub(crate) responder: CommitTicketSender,
    /// Releases the command's bytes from the unapplied budget when dropped.
    pub(crate) _admission: OwnedSemaphorePermit,
}

#[derive(Default)]
struct PendingCommands {
    by_command_id: BTreeMap<Vec<u8>, PendingCommand>,
    stop_reason: Option<NodeStopReason>,
}

/// Node serving state, halted partitions, and submitted commands that wait for apply.
pub(crate) struct CommitState {
    lifecycle: watch::Sender<NodeState>,
    pending: Mutex<PendingCommands>,
    /// Appliers hold it for reading while they write; `stop` holds it for writing.
    store_writes: RwLock<()>,
    halted_partitions: Vec<AtomicBool>,
}

impl CommitState {
    pub(crate) fn new(partition_count: u32) -> Self {
        let (lifecycle, _) = watch::channel(NodeState::Recovering);
        Self {
            lifecycle,
            pending: Mutex::new(PendingCommands::default()),
            store_writes: RwLock::new(()),
            halted_partitions: (0..partition_count)
                .map(|_| AtomicBool::new(false))
                .collect(),
        }
    }

    pub(crate) fn partition_count(&self) -> u32 {
        u32::try_from(self.halted_partitions.len()).expect("partition count fits u32")
    }

    pub(crate) fn start_serving(&self) -> bool {
        self.lifecycle.send_if_modified(|state| {
            if *state != NodeState::Recovering {
                return false;
            }
            *state = NodeState::Serving;
            true
        })
    }

    /// Stops the node once; later reasons are ignored. Fails every pending command.
    ///
    /// Waits for in-progress applier writes, so never call it while holding [`Self::begin_store_write`].
    pub(crate) fn stop(&self, reason: NodeStopReason) {
        let is_first_stop = {
            let _store_writes = self
                .store_writes
                .write()
                .expect("store write lock is not poisoned");
            self.lifecycle.send_if_modified(|state| {
                if matches!(state, NodeState::Stopped(_)) {
                    return false;
                }
                *state = NodeState::Stopped(reason.clone());
                true
            })
        };
        if !is_first_stop {
            return;
        }
        if reason == NodeStopReason::Shutdown {
            tracing::info!("engine node stopping");
        } else {
            tracing::error!(reason = %reason, "engine node stopping after a fatal failure");
        }
        let drained = {
            let mut pending = self.lock_pending();
            pending.stop_reason = Some(reason.clone());
            std::mem::take(&mut pending.by_command_id)
        };
        for (_, command) in drained {
            command.responder.resolve(Err(CommitError::NodeStopping {
                detail: reason.to_string(),
            }));
        }
    }

    /// Admits one applier write to the local store, or returns `None` once the node stopped.
    ///
    /// A failed ticket must never be followed by a write, so `stop` waits for admitted writes.
    pub(crate) fn begin_store_write(&self) -> Option<RwLockReadGuard<'_, ()>> {
        let store_write = self
            .store_writes
            .read()
            .expect("store write lock is not poisoned");
        self.stop_reason().is_none().then_some(store_write)
    }

    pub(crate) fn check_serving(&self) -> Result<(), CommitError> {
        match &*self.lifecycle.borrow() {
            NodeState::Recovering => Err(CommitError::Unavailable {
                detail: "the node is recovering".to_owned(),
            }),
            NodeState::Serving => Ok(()),
            NodeState::Stopped(reason) => Err(CommitError::NodeStopping {
                detail: reason.to_string(),
            }),
        }
    }

    pub(crate) fn stop_reason(&self) -> Option<NodeStopReason> {
        match &*self.lifecycle.borrow() {
            NodeState::Stopped(reason) => Some(reason.clone()),
            NodeState::Recovering | NodeState::Serving => None,
        }
    }

    pub(crate) async fn wait_stopped(&self) -> NodeStopReason {
        let mut receiver = self.lifecycle.subscribe();
        let state = receiver
            .wait_for(|state| matches!(state, NodeState::Stopped(_)))
            .await
            .expect("the sender lives as long as self");
        match &*state {
            NodeState::Stopped(reason) => reason.clone(),
            NodeState::Recovering | NodeState::Serving => {
                unreachable!("wait_for returned a stopped state")
            }
        }
    }

    pub(crate) fn register(
        &self,
        command_id: Vec<u8>,
        command: PendingCommand,
    ) -> Result<(), CommitError> {
        let mut pending = self.lock_pending();
        if let Some(reason) = &pending.stop_reason {
            let error = CommitError::NodeStopping {
                detail: reason.to_string(),
            };
            command.responder.resolve(Err(error.clone()));
            return Err(error);
        }
        pending.by_command_id.insert(command_id, command);
        Ok(())
    }

    pub(crate) fn take(&self, command_id: &[u8]) -> Option<PendingCommand> {
        self.lock_pending().by_command_id.remove(command_id)
    }

    pub(crate) fn halt_partition(&self, partition: PartitionId) {
        self.halted_partitions[usize::from(partition.0)].store(true, Ordering::Release);
    }

    pub(crate) fn is_partition_halted(&self, partition: PartitionId) -> bool {
        self.halted_partitions[usize::from(partition.0)].load(Ordering::Acquire)
    }

    pub(crate) fn halted_partitions(&self) -> Vec<PartitionId> {
        self.halted_partitions
            .iter()
            .enumerate()
            .filter(|(_, is_halted)| is_halted.load(Ordering::Acquire))
            .map(|(index, _)| PartitionId(u16::try_from(index).expect("partition fits u16")))
            .collect()
    }

    fn lock_pending(&self) -> std::sync::MutexGuard<'_, PendingCommands> {
        self.pending
            .lock()
            .expect("pending command lock is not poisoned")
    }
}

/// The error for commands of a partition that stopped applying after an apply failure.
pub(crate) fn partition_stopped_error(partition: PartitionId) -> CommitError {
    CommitError::Unavailable {
        detail: format!("partition {} stopped applying commands", partition.0),
    }
}
