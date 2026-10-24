// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use dexzd_clock::WallClock;
use dexzd_keyspace::validate_node_identifier;
use dexzd_object_store::{ObjectStore, ObjectStoreErrorKind, WriteCondition, WriteOutcome};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Instant, sleep, sleep_until, timeout, timeout_at};
use xxhash_rust::xxh3::xxh3_128;

use crate::batch_codec::{
    BatchContent, BatchRecord, LogError, MAX_RECORD_LENGTH, MAX_UNCOMPRESSED_FRAME_LENGTH,
    StreamIdentity, decode_batch, encode_batch, encoded_record_length,
};
use crate::stream_recovery::validate_batch_identity;

#[derive(Clone, Debug)]
pub struct CommitPipelineConfig {
    pub batch_window: Duration,
    pub target_batch_bytes: usize,
    pub max_batch_records: usize,
    pub max_in_flight: usize,
    pub max_pending_bytes: u32,
    pub admission_timeout: Duration,
    pub head_deadline: Duration,
}

impl Default for CommitPipelineConfig {
    fn default() -> Self {
        Self {
            batch_window: Duration::from_millis(20),
            target_batch_bytes: 4 * 1024 * 1024,
            max_batch_records: 10_000,
            max_in_flight: 4,
            max_pending_bytes: 64 * 1024 * 1024,
            admission_timeout: Duration::from_secs(1),
            head_deadline: Duration::from_secs(10),
        }
    }
}

#[derive(Debug)]
pub struct ProposalTicket {
    pub id: u64,
    pub committed: oneshot::Receiver<Result<u64, ProposalFailure>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProposalFailure {
    /// A newer writer owns the stream.
    Fenced,
    /// An older writer raced with this batch; recover before retrying.
    Restart,
    /// The write outcome may be unknown; recover before retrying.
    Unavailable,
}

#[derive(Debug)]
pub struct CommittedBatch {
    pub seq: u64,
    pub epoch: u64,
    pub records_by_partition: BTreeMap<u32, Vec<Bytes>>,
    pub proposal_tickets: Vec<u64>,
}

pub struct CommitPipeline {
    proposals: mpsc::Sender<Proposal>,
    admission: Arc<Semaphore>,
    next_ticket: AtomicU64,
    config: CommitPipelineConfig,
    task: JoinHandle<()>,
}

impl CommitPipeline {
    pub fn start(
        store: Arc<dyn ObjectStore>,
        clock: Arc<dyn WallClock>,
        stream: StreamIdentity,
        epoch: u64,
        next_seq: u64,
        config: CommitPipelineConfig,
    ) -> Result<(Self, mpsc::Receiver<CommittedBatch>), LogError> {
        if epoch == 0
            || next_seq == 0
            || config.max_in_flight == 0
            || config.max_batch_records == 0
            || config.target_batch_bytes == 0
            || config.max_pending_bytes == 0
        {
            return Err(LogError::Invalid(
                "invalid commit pipeline configuration".into(),
            ));
        }
        validate_node_identifier(&stream.node_id)
            .map_err(|error| LogError::Invalid(error.to_string()))?;
        let (proposals, receiver) = mpsc::channel(config.max_in_flight * 2);
        let (committed_sender, committed_receiver) = mpsc::channel(config.max_in_flight * 2);
        let admission = Arc::new(Semaphore::new(config.max_pending_bytes as usize));
        let runtime = PipelineRuntime {
            store,
            clock,
            stream,
            epoch,
            config: config.clone(),
            committed_sender,
        };
        let task = tokio::spawn(run_pipeline(runtime, next_seq, receiver));
        Ok((
            Self {
                proposals,
                admission,
                next_ticket: AtomicU64::new(1),
                config,
                task,
            },
            committed_receiver,
        ))
    }

    pub async fn propose(
        &self,
        partition: u32,
        record: Bytes,
        waits_for_commit: bool,
    ) -> Result<ProposalTicket, LogError> {
        if record.len() > MAX_RECORD_LENGTH {
            return Err(LogError::Invalid("record exceeds maximum length".into()));
        }
        let requested = u32::try_from(record.len().max(1))
            .map_err(|_| LogError::Invalid("record exceeds maximum length".into()))?;
        if requested > self.config.max_pending_bytes {
            return Err(LogError::Invalid(
                "record exceeds pending byte capacity".into(),
            ));
        }
        let permit = timeout(
            self.config.admission_timeout,
            self.admission.clone().acquire_many_owned(requested),
        )
        .await
        .map_err(|_| LogError::Retryable)?
        .map_err(|_| LogError::Retryable)?;
        let id = self.next_ticket.fetch_add(1, Ordering::Relaxed);
        let (sender, committed) = oneshot::channel();
        let proposal = Proposal {
            id,
            partition,
            record,
            waits_for_commit,
            sender,
            permit,
        };
        timeout(self.config.admission_timeout, self.proposals.send(proposal))
            .await
            .map_err(|_| LogError::Retryable)?
            .map_err(|_| LogError::Retryable)?;
        Ok(ProposalTicket { id, committed })
    }
}

impl Drop for CommitPipeline {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Proposal {
    id: u64,
    partition: u32,
    record: Bytes,
    waits_for_commit: bool,
    sender: oneshot::Sender<Result<u64, ProposalFailure>>,
    permit: OwnedSemaphorePermit,
}

#[derive(Default)]
struct PendingBatch {
    proposals: Vec<Proposal>,
    bytes: usize,
    has_waiter: bool,
    partition_frame_bytes: BTreeMap<u32, usize>,
}

impl PendingBatch {
    fn can_accept(&self, proposal: &Proposal) -> bool {
        self.partition_frame_bytes
            .get(&proposal.partition)
            .copied()
            .unwrap_or(0)
            + encoded_record_length(proposal.record.len())
            <= MAX_UNCOMPRESSED_FRAME_LENGTH
    }

    fn add(&mut self, proposal: Proposal) {
        self.bytes += proposal.record.len();
        self.has_waiter |= proposal.waits_for_commit;
        *self
            .partition_frame_bytes
            .entry(proposal.partition)
            .or_default() += encoded_record_length(proposal.record.len());
        self.proposals.push(proposal);
    }

    fn is_empty(&self) -> bool {
        self.proposals.is_empty()
    }
}

struct InFlightBatch {
    batch: PendingBatch,
    outcome: Option<Result<(), LogError>>,
}

struct PipelineRuntime {
    store: Arc<dyn ObjectStore>,
    clock: Arc<dyn WallClock>,
    stream: StreamIdentity,
    epoch: u64,
    config: CommitPipelineConfig,
    committed_sender: mpsc::Sender<CommittedBatch>,
}

async fn run_pipeline(
    runtime: PipelineRuntime,
    mut next_seq: u64,
    mut receiver: mpsc::Receiver<Proposal>,
) {
    let PipelineRuntime {
        store,
        clock,
        stream,
        epoch,
        config,
        committed_sender,
    } = runtime;
    let mut open = PendingBatch::default();
    let mut open_deadline = None;
    let mut ready = VecDeque::new();
    let mut in_flight = BTreeMap::<u64, InFlightBatch>::new();
    let mut writes = JoinSet::<(u64, Result<(), LogError>)>::new();
    let mut receiver_closed = false;
    loop {
        if !open.is_empty()
            && (open.bytes >= config.target_batch_bytes
                || open.proposals.len() >= config.max_batch_records
                || (open.has_waiter && in_flight.len() < config.max_in_flight))
        {
            ready.push_back(std::mem::take(&mut open));
            open_deadline = None;
        }
        while in_flight.len() < config.max_in_flight {
            let Some(batch) = ready.pop_front() else {
                break;
            };
            let seq = next_seq;
            let Some(incremented) = next_seq.checked_add(1) else {
                fail_all(
                    &mut open,
                    &mut ready,
                    &mut in_flight,
                    ProposalFailure::Unavailable,
                );
                return;
            };
            next_seq = incremented;
            let content = BatchContent {
                stream: stream.clone(),
                epoch,
                seq,
                sealed_at_unix_micros: clock.now_unix_micros(),
                records: batch
                    .proposals
                    .iter()
                    .map(|proposal| BatchRecord {
                        partition: proposal.partition,
                        bytes: proposal.record.clone(),
                    })
                    .collect(),
                epoch_start_writer: None,
            };
            let body = match encode_batch(&content) {
                Ok(body) => body,
                Err(_) => {
                    fail_all(
                        &mut open,
                        &mut ready,
                        &mut in_flight,
                        ProposalFailure::Unavailable,
                    );
                    fail_batch(batch, ProposalFailure::Unavailable);
                    return;
                }
            };
            let store = store.clone();
            let stream = stream.clone();
            let head_deadline = config.head_deadline;
            writes.spawn(async move {
                (
                    seq,
                    write_batch(store.as_ref(), &stream, epoch, seq, body, head_deadline).await,
                )
            });
            in_flight.insert(
                seq,
                InFlightBatch {
                    batch,
                    outcome: None,
                },
            );
        }
        if receiver_closed && open.is_empty() && ready.is_empty() && in_flight.is_empty() {
            return;
        }
        let deadline = open_deadline.unwrap_or_else(|| Instant::now() + config.batch_window);
        // Biased for reproducible simulation; in-flight writes are bounded and the deadline fires
        // once per batch, so proposals cannot starve.
        tokio::select! {
            biased;
            result = writes.join_next(), if !writes.is_empty() => {
                match result {
                    Some(Ok((seq, outcome))) => {
                        if let Some(batch) = in_flight.get_mut(&seq) { batch.outcome = Some(outcome); }
                    }
                    _ => {
                        fail_all(&mut open, &mut ready, &mut in_flight, ProposalFailure::Unavailable);
                        return;
                    }
                }
            }
            () = sleep_until(deadline), if open_deadline.is_some() => {
                ready.push_back(std::mem::take(&mut open));
                open_deadline = None;
            }
            maybe_proposal = receiver.recv(), if !receiver_closed => {
                match maybe_proposal {
                    Some(proposal) => {
                        if !open.is_empty() && (!open.can_accept(&proposal)
                            || open.bytes.saturating_add(proposal.record.len()) > config.target_batch_bytes)
                        {
                            ready.push_back(std::mem::take(&mut open));
                            open_deadline = None;
                        }
                        if open.is_empty() { open_deadline = Some(Instant::now() + config.batch_window); }
                        open.add(proposal);
                    }
                    None => {
                        receiver_closed = true;
                        if !open.is_empty() { ready.push_back(std::mem::take(&mut open)); open_deadline = None; }
                    }
                }
            }
        }
        while let Some((&seq, first)) = in_flight.first_key_value() {
            if first.outcome.is_none() {
                break;
            }
            let Some(mut finished) = in_flight.remove(&seq) else {
                break;
            };
            let outcome = finished
                .outcome
                .take()
                .expect("completed batch has an outcome");
            match outcome {
                Ok(()) => {
                    let mut records_by_partition = BTreeMap::<u32, Vec<Bytes>>::new();
                    let mut tickets = Vec::with_capacity(finished.batch.proposals.len());
                    for proposal in &finished.batch.proposals {
                        records_by_partition
                            .entry(proposal.partition)
                            .or_default()
                            .push(proposal.record.clone());
                        tickets.push(proposal.id);
                    }
                    if committed_sender
                        .send(CommittedBatch {
                            seq,
                            epoch,
                            records_by_partition,
                            proposal_tickets: tickets,
                        })
                        .await
                        .is_err()
                    {
                        fail_batch(finished.batch, ProposalFailure::Unavailable);
                        fail_all(
                            &mut open,
                            &mut ready,
                            &mut in_flight,
                            ProposalFailure::Unavailable,
                        );
                        return;
                    }
                    for proposal in finished.batch.proposals {
                        if proposal.sender.send(Ok(seq)).is_err() {
                            tracing::debug!(
                                ticket_id = proposal.id,
                                "proposal ticket receiver dropped"
                            );
                        }
                        drop(proposal.permit);
                    }
                }
                Err(LogError::Fenced) => {
                    fail_batch(finished.batch, ProposalFailure::Fenced);
                    fail_all(
                        &mut open,
                        &mut ready,
                        &mut in_flight,
                        ProposalFailure::Fenced,
                    );
                    return;
                }
                Err(LogError::Restart) => {
                    fail_batch(finished.batch, ProposalFailure::Restart);
                    fail_all(
                        &mut open,
                        &mut ready,
                        &mut in_flight,
                        ProposalFailure::Restart,
                    );
                    return;
                }
                Err(_) => {
                    fail_batch(finished.batch, ProposalFailure::Unavailable);
                    fail_all(
                        &mut open,
                        &mut ready,
                        &mut in_flight,
                        ProposalFailure::Unavailable,
                    );
                    return;
                }
            }
        }
    }
}

fn fail_all(
    open: &mut PendingBatch,
    ready: &mut VecDeque<PendingBatch>,
    in_flight: &mut BTreeMap<u64, InFlightBatch>,
    failure: ProposalFailure,
) {
    fail_batch(std::mem::take(open), failure);
    for batch in ready.drain(..) {
        fail_batch(batch, failure);
    }
    for (_, batch) in std::mem::take(in_flight) {
        fail_batch(batch.batch, failure);
    }
}

fn fail_batch(batch: PendingBatch, failure: ProposalFailure) {
    for proposal in batch.proposals {
        if proposal.sender.send(Err(failure)).is_err() {
            tracing::debug!(ticket_id = proposal.id, "proposal ticket receiver dropped");
        }
        drop(proposal.permit);
    }
}

async fn write_batch(
    store: &dyn ObjectStore,
    stream: &StreamIdentity,
    epoch: u64,
    seq: u64,
    body: Bytes,
    timeout_duration: Duration,
) -> Result<(), LogError> {
    let key = stream.batch_key(seq);
    let trailer = xxh3_128(&body[..body.len() - 16]);
    let deadline = Instant::now() + timeout_duration;
    let mut backoff = Duration::from_millis(10);
    loop {
        if Instant::now() >= deadline {
            return Err(LogError::Deadline);
        }
        let should_read_back = match timeout_at(
            deadline,
            store.put(&key, body.clone(), WriteCondition::CreateOnly),
        )
        .await
        {
            Err(_) => return Err(LogError::Deadline),
            Ok(result) => match result {
                Ok(WriteOutcome::Written { .. }) => return Ok(()),
                Ok(WriteOutcome::PreconditionFailed) => true,
                Err(error) if error.kind == ObjectStoreErrorKind::OutcomeUnknown => true,
                Err(error) if error.kind == ObjectStoreErrorKind::Transient => false,
                Err(error) => return Err(error.into()),
            },
        };
        if should_read_back {
            match timeout_at(deadline, store.get(&key)).await {
                Err(_) => return Err(LogError::Deadline),
                Ok(result) => match result {
                    Ok(Some(object)) => {
                        let batch = decode_batch(&object.bytes)?;
                        validate_batch_identity(&batch, stream, seq)?;
                        if batch.trailer == trailer {
                            return Ok(());
                        }
                        if batch.header.epoch < epoch {
                            return Err(LogError::Restart);
                        }
                        return Err(LogError::Fenced);
                    }
                    Ok(None) => {}
                    Err(error) if error.kind == ObjectStoreErrorKind::Transient => {}
                    Err(error) => return Err(error.into()),
                },
            }
        }
        sleep(backoff.min(deadline.saturating_duration_since(Instant::now()))).await;
        backoff = (backoff * 2).min(Duration::from_millis(500));
    }
}
