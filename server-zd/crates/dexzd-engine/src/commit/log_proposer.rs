// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use dexzd_keyspace::PartitionId;
use dexzd_log::{CommitPipeline, CommittedBatch, LogError, ProposalFailure};
use dexzd_protocol::dexzd::v1::LogPosition;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::commit::command_committer::LogProposal;
use crate::commit::commit_state::{CommitState, NodeStopReason};
use crate::commit::partition_applier::{PartitionApplierPool, PartitionFrame};

const PROPOSAL_RETRY_DELAY: Duration = Duration::from_millis(10);

/// Feeds submitted records to the commit pipeline in submission order.
///
/// Owns the pipeline: returning drops it, which stops its writer task.
pub(crate) async fn run_log_proposer(
    pipeline: CommitPipeline,
    mut proposals: mpsc::UnboundedReceiver<LogProposal>,
    state: Arc<CommitState>,
    cancellation: CancellationToken,
) {
    let mut tickets = FuturesUnordered::new();
    loop {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => return,
            Some(result) = tickets.next(), if !tickets.is_empty() => {
                if let Some(reason) = proposal_failure_reason(result) {
                    state.stop(reason);
                    return;
                }
            }
            proposal = proposals.recv() => {
                let Some(proposal) = proposal else { return };
                match propose_until_accepted(&pipeline, proposal, &state, &cancellation).await {
                    Some(ticket) => tickets.push(ticket),
                    None => return,
                }
            }
        }
    }
}

async fn propose_until_accepted(
    pipeline: &CommitPipeline,
    proposal: LogProposal,
    state: &CommitState,
    cancellation: &CancellationToken,
) -> Option<tokio::sync::oneshot::Receiver<Result<u64, ProposalFailure>>> {
    loop {
        if cancellation.is_cancelled() || state.stop_reason().is_some() {
            return None;
        }
        // A timed-out proposal was never enqueued, so retrying keeps submission order.
        match pipeline
            .propose(
                u32::from(proposal.partition.0),
                proposal.record.clone(),
                proposal.waits_for_commit,
            )
            .await
        {
            Ok(ticket) => return Some(ticket.committed),
            Err(LogError::Retryable) => {
                tracing::warn!(
                    partition = proposal.partition.0,
                    "commit pipeline admission timed out; retrying"
                );
                tokio::time::sleep(PROPOSAL_RETRY_DELAY).await;
            }
            Err(error) => {
                state.stop(NodeStopReason::CommitPipelineFailed {
                    detail: error.to_string(),
                });
                return None;
            }
        }
    }
}

fn proposal_failure_reason(
    result: Result<Result<u64, ProposalFailure>, tokio::sync::oneshot::error::RecvError>,
) -> Option<NodeStopReason> {
    match result {
        Ok(Ok(_seq)) => None,
        Ok(Err(ProposalFailure::Fenced)) => Some(NodeStopReason::Fenced),
        Ok(Err(ProposalFailure::Unavailable)) => Some(NodeStopReason::CommitPipelineFailed {
            detail: "object storage did not accept a log batch before its deadline".to_owned(),
        }),
        Err(_) => Some(NodeStopReason::CommitPipelineFailed {
            detail: "commit pipeline dropped a proposal".to_owned(),
        }),
    }
}

/// Routes committed batches to the partition appliers in log order.
pub(crate) struct CommittedBatchRouter {
    pub(crate) appliers: Arc<PartitionApplierPool>,
    pub(crate) state: Arc<CommitState>,
    pub(crate) node_id: String,
    pub(crate) incarnation: u32,
    pub(crate) progress: Arc<CommitProgress>,
}

/// Counters that the snapshot service reads to schedule snapshots.
#[derive(Debug, Default)]
pub(crate) struct CommitProgress {
    /// Record bytes of every batch routed since the node started.
    pub(crate) committed_log_bytes: AtomicU64,
    /// Highest batch seq whose frames are all queued to the appliers.
    pub(crate) last_routed_seq: AtomicU64,
}

impl CommittedBatchRouter {
    pub(crate) async fn run(
        self,
        mut batches: mpsc::Receiver<CommittedBatch>,
        cancellation: CancellationToken,
    ) {
        loop {
            let batch = tokio::select! {
                biased;
                () = cancellation.cancelled() => return,
                batch = batches.recv() => batch,
            };
            // The pipeline fails every waiting proposal before it closes this channel, so
            // the proposer reports the precise stop reason.
            let Some(batch) = batch else { return };
            if !self.route(batch).await {
                self.state.stop(NodeStopReason::CommitPipelineFailed {
                    detail: "partition appliers stopped".to_owned(),
                });
                return;
            }
        }
    }

    async fn route(&self, batch: CommittedBatch) -> bool {
        let mut batch_bytes = 0_u64;
        for (partition, records) in batch.records_by_partition {
            batch_bytes += records
                .iter()
                .map(|record| record.len() as u64)
                .sum::<u64>();
            let frame = PartitionFrame {
                partition: PartitionId(
                    u16::try_from(partition).expect("the committer proposes u16 partitions"),
                ),
                position: LogPosition {
                    node_id: self.node_id.clone(),
                    incarnation: self.incarnation,
                    seq: batch.seq,
                },
                records,
                is_replayed: false,
            };
            if self.appliers.enqueue(frame).await.is_err() {
                return false;
            }
        }
        self.progress
            .committed_log_bytes
            .fetch_add(batch_bytes, Ordering::Relaxed);
        self.progress
            .last_routed_seq
            .store(batch.seq, Ordering::Release);
        true
    }
}
