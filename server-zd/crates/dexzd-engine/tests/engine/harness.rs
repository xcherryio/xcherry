// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use dexzd_apply::{ApplyContext, ApplyError, ApplyOutcome, FlowStateReader, StateWrite};
use dexzd_clock::WallClock;
use dexzd_cluster_metadata::{ClusterConfig, LeaseConfig, NodeConfig};
use dexzd_engine::{
    CommandApplier, CommitConfig, CommitError, CommitTicket, CommitUrgency, CommittedCommandSink,
    CommittedFlowCommand, CommittedStateView, EngineNode, EngineNodeConfig, EngineNodeDependencies,
    EngineRuntimeHooks, EngineServices, EngineStartError, LocalStoreConfig,
    PartitionRuntimeRebuilder, RecoveryConfig, RuntimeRebuildError, SnapshotConfig,
};
use dexzd_keyspace::{
    ColumnFamily, PartitionHasher, PartitionId, TenantId, flow_directory_key, flow_directory_prefix,
};
use dexzd_object_store::{
    FaultAction, FaultInjector, InMemoryObjectStore, ObjectStore, ObjectStoreOperation,
};
use dexzd_protocol::dexzd::v1::{
    AttributesSetCommand, CommandReply, FlowCommand, LogPosition, Manifest, NodeLease, flow_command,
};
use futures::future::BoxFuture;
use prost::Message;
use tokio::sync::Notify;
use tokio::time::Instant;

pub(crate) const PARTITION_COUNT: u32 = 4;
pub(crate) const FAILING_FLOW: &str = "fails-to-apply";
pub(crate) const PANICKING_FLOW: &str = "panics-in-apply";
pub(crate) const NONDETERMINISTIC_FLOW: &str = "applies-nondeterministically";
pub(crate) const BLOCKING_FLOW: &str = "blocks-in-apply";

/// Wall clock that follows Tokio's paused clock, with an adjustable offset.
pub(crate) struct TestClock {
    start: Instant,
    offset_micros: AtomicI64,
}

impl TestClock {
    pub(crate) fn new() -> Self {
        Self {
            start: Instant::now(),
            offset_micros: AtomicI64::new(1_800_000_000_000_000),
        }
    }

    pub(crate) fn shift(&self, micros: i64) {
        self.offset_micros.fetch_add(micros, Ordering::SeqCst);
    }
}

impl WallClock for TestClock {
    fn now_unix_micros(&self) -> i64 {
        self.offset_micros.load(Ordering::SeqCst)
            + i64::try_from(self.start.elapsed().as_micros()).expect("test time fits i64")
    }
}

/// Counts commands per Flow: each command increments a counter row and appends an event row.
///
/// Reading the event rows with a scan checks that commands of one frame see earlier writes.
#[derive(Default)]
pub(crate) struct CountingApplier {
    applied: Mutex<Vec<FlowCommand>>,
    nondeterministic_counter: AtomicU64,
    /// Holds commands of [`BLOCKING_FLOW`] until opened.
    pub(crate) gate: ApplyGate,
}

impl CountingApplier {
    pub(crate) fn applied_commands(&self) -> Vec<FlowCommand> {
        self.applied.lock().expect("applied lock").clone()
    }
}

/// Blocks an apply call on its blocking-pool thread until the test opens it.
#[derive(Default)]
pub(crate) struct ApplyGate {
    has_entered: AtomicBool,
    is_open: Mutex<bool>,
    opened: Condvar,
}

impl ApplyGate {
    /// Yields instead of sleeping: paused time does not advance while a blocking task runs.
    pub(crate) async fn wait_until_entered(&self) {
        while !self.has_entered.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    }

    pub(crate) fn open(&self) {
        *self.is_open.lock().expect("gate lock") = true;
        self.opened.notify_all();
    }

    fn pass(&self) {
        self.has_entered.store(true, Ordering::SeqCst);
        let is_open = self.is_open.lock().expect("gate lock");
        drop(
            self.opened
                .wait_while(is_open, |is_open| !*is_open)
                .expect("gate lock"),
        );
    }
}

impl CommandApplier for CountingApplier {
    fn apply(
        &self,
        context: &ApplyContext,
        state: &dyn FlowStateReader,
        command: &FlowCommand,
    ) -> Result<ApplyOutcome, ApplyError> {
        self.applied
            .lock()
            .expect("applied lock")
            .push(command.clone());
        match command.flow_id.as_str() {
            FAILING_FLOW => {
                return Err(ApplyError::InvariantViolation {
                    detail: "scripted failure".to_owned(),
                });
            }
            PANICKING_FLOW => panic!("scripted apply panic"),
            BLOCKING_FLOW => self.gate.pass(),
            _ => {}
        }
        let counter_key = counter_key(context, &command.flow_id);
        let count = state
            .get(ColumnFamily::Flow, &counter_key)?
            .map_or(0, |bytes| decode_u64(&bytes));
        let prefix = flow_directory_prefix(context.flow_hash);
        let mut prefix_end = prefix.clone();
        *prefix_end.last_mut().expect("prefix is not empty") += 1;
        let rows = state.scan(ColumnFamily::Flow, &prefix, &prefix_end, usize::MAX)?;
        let expected_rows = if count == 0 { 0 } else { count as usize + 1 };
        if rows.len() != expected_rows {
            return Err(ApplyError::InvariantViolation {
                detail: format!("expected {expected_rows} rows, found {}", rows.len()),
            });
        }
        let next = count + 1;
        let mut event_key = counter_key.clone();
        event_key.push(0xFF);
        event_key.extend_from_slice(&next.to_be_bytes());
        let event_value = if command.flow_id == NONDETERMINISTIC_FLOW {
            self.nondeterministic_counter
                .fetch_add(1, Ordering::SeqCst)
                .to_be_bytes()
                .to_vec()
        } else {
            command.proposed_at_micros.to_be_bytes().to_vec()
        };
        Ok(ApplyOutcome {
            writes: vec![
                StateWrite::Put {
                    column_family: ColumnFamily::Flow,
                    key: counter_key,
                    value: next.to_be_bytes().to_vec(),
                },
                StateWrite::Put {
                    column_family: ColumnFamily::Flow,
                    key: event_key,
                    value: event_value,
                },
            ],
            effects: Vec::new(),
            reply: CommandReply::default(),
            run_id: None,
        })
    }
}

fn counter_key(context: &ApplyContext, flow_id: &str) -> Vec<u8> {
    flow_directory_key(context.flow_hash, TenantId::DEFAULT, flow_id)
}

pub(crate) fn decode_u64(bytes: &[u8]) -> u64 {
    u64::from_be_bytes(bytes.try_into().expect("counter has eight bytes"))
}

/// Records rebuilt partitions, every committed command, and every frame end.
#[derive(Default)]
pub(crate) struct RecordingRuntime {
    rebuilt: Mutex<Vec<PartitionId>>,
    committed: Mutex<Vec<CommittedFlowCommand>>,
    sink_events: Mutex<Vec<(PartitionId, SinkEvent)>>,
    failing_partition: Option<PartitionId>,
    is_frame_end_gated: bool,
    /// Holds every `finish_frame` call after it is recorded, when frame ends are gated.
    pub(crate) frame_end_gate: ApplyGate,
}

/// One call the committed command sink received, with the frame position it carried.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SinkEvent {
    Applied(LogPosition),
    FrameFinished(LogPosition),
}

impl RecordingRuntime {
    pub(crate) fn failing_rebuild_of(partition: PartitionId) -> Self {
        Self {
            failing_partition: Some(partition),
            ..Self::default()
        }
    }

    pub(crate) fn with_gated_frame_ends() -> Self {
        Self {
            is_frame_end_gated: true,
            ..Self::default()
        }
    }

    pub(crate) fn rebuilt_partitions(&self) -> Vec<PartitionId> {
        self.rebuilt.lock().expect("rebuilt lock").clone()
    }

    pub(crate) fn committed_commands(&self) -> Vec<CommittedFlowCommand> {
        self.committed.lock().expect("committed lock").clone()
    }

    /// Returns the command IDs of `flow_id` in the order they were published.
    pub(crate) fn committed_command_ids(&self, flow_id: &str) -> Vec<Vec<u8>> {
        self.committed_commands()
            .into_iter()
            .filter(|committed| committed.command.flow_id == flow_id)
            .map(|committed| committed.command.command_id)
            .collect()
    }

    /// Returns the sink calls for `partition` in the order they arrived.
    pub(crate) fn sink_events_of(&self, partition: PartitionId) -> Vec<SinkEvent> {
        self.sink_events
            .lock()
            .expect("sink event lock")
            .iter()
            .filter(|(event_partition, _)| *event_partition == partition)
            .map(|(_, event)| event.clone())
            .collect()
    }

    fn record_sink_event(&self, partition: PartitionId, event: SinkEvent) {
        self.sink_events
            .lock()
            .expect("sink event lock")
            .push((partition, event));
    }
}

impl CommittedCommandSink for RecordingRuntime {
    fn apply_committed(&self, committed: CommittedFlowCommand) {
        self.record_sink_event(
            committed.context.partition,
            SinkEvent::Applied(committed.committed.position.clone()),
        );
        self.committed
            .lock()
            .expect("committed lock")
            .push(committed);
    }

    fn finish_frame(&self, partition: PartitionId, position: &LogPosition) {
        self.record_sink_event(partition, SinkEvent::FrameFinished(position.clone()));
        if self.is_frame_end_gated {
            self.frame_end_gate.pass();
        }
    }
}

/// Asserts that every frame ends with one `FrameFinished` right after its last command.
///
/// Returns the number of frames in `events`.
pub(crate) fn assert_frames_finish_after_their_commands(events: &[SinkEvent]) -> usize {
    let mut open_frame: Option<&LogPosition> = None;
    let mut finished_frames = 0;
    for event in events {
        match event {
            SinkEvent::Applied(position) => {
                if let Some(open) = open_frame {
                    assert_eq!(open, position, "a frame ended without finish_frame");
                }
                open_frame = Some(position);
            }
            SinkEvent::FrameFinished(position) => {
                assert_eq!(
                    open_frame,
                    Some(position),
                    "finish_frame did not follow a command of its frame"
                );
                open_frame = None;
                finished_frames += 1;
            }
        }
    }
    assert_eq!(open_frame, None, "the last frame has no finish_frame");
    finished_frames
}

impl PartitionRuntimeRebuilder for RecordingRuntime {
    fn rebuild_partition(
        &self,
        partition: PartitionId,
    ) -> BoxFuture<'_, Result<(), RuntimeRebuildError>> {
        Box::pin(async move {
            if self.failing_partition == Some(partition) {
                return Err(RuntimeRebuildError {
                    partition,
                    detail: "scripted rebuild failure".to_owned(),
                });
            }
            self.rebuilt.lock().expect("rebuilt lock").push(partition);
            Ok(())
        })
    }
}

/// One armed delay: the next `operation` on a key that `matches_prefix` or equals `key`.
struct DelayedOperation {
    operation: ObjectStoreOperation,
    key: String,
    matches_prefix: bool,
    delay: Duration,
}

/// Fails writes under a key prefix, fails writes of one key after some succeed, or delays one
/// operation, once switched on. Records every operation it sees.
#[derive(Default)]
pub(crate) struct SwitchableFaults {
    failing_put_prefix: Mutex<Option<String>>,
    /// Key whose writes fail once the remaining count of allowed writes is zero.
    limited_put: Mutex<Option<(String, usize)>>,
    delayed: Mutex<Option<DelayedOperation>>,
    /// Key prefix and delay applied to every read under it.
    delayed_gets: Mutex<Option<(String, Duration)>>,
    delayed_entered: Notify,
    operations: Mutex<Vec<(ObjectStoreOperation, String)>>,
}

impl SwitchableFaults {
    pub(crate) fn fail_puts_under(&self, prefix: &str) {
        *self.failing_put_prefix.lock().expect("fault lock") = Some(prefix.to_owned());
    }

    /// Lets `allowed` more writes of exactly `key` succeed, then fails the rest.
    pub(crate) fn fail_puts_of_after(&self, key: &str, allowed: usize) {
        *self.limited_put.lock().expect("fault lock") = Some((key.to_owned(), allowed));
    }

    pub(crate) fn clear(&self) {
        *self.failing_put_prefix.lock().expect("fault lock") = None;
        *self.limited_put.lock().expect("fault lock") = None;
        *self.delayed.lock().expect("fault lock") = None;
        *self.delayed_gets.lock().expect("fault lock") = None;
    }

    /// Delays every read of a key under `prefix` by `delay` until cleared.
    pub(crate) fn delay_every_get_under(&self, prefix: &str, delay: Duration) {
        *self.delayed_gets.lock().expect("fault lock") = Some((prefix.to_owned(), delay));
    }

    /// Delays the next read of exactly `key` by `delay`.
    pub(crate) fn delay_next_get_of(&self, key: &str, delay: Duration) {
        self.arm_delay(ObjectStoreOperation::Get, key, false, delay);
    }

    /// Delays the next read of a key under `prefix` by `delay`.
    pub(crate) fn delay_next_get_under(&self, prefix: &str, delay: Duration) {
        self.arm_delay(ObjectStoreOperation::Get, prefix, true, delay);
    }

    /// Delays the next write of a key under `prefix` by `delay`.
    pub(crate) fn delay_next_put_under(&self, prefix: &str, delay: Duration) {
        self.arm_delay(ObjectStoreOperation::Put, prefix, true, delay);
    }

    /// Waits until the operation armed by a `delay_next_*` call starts waiting.
    pub(crate) async fn wait_until_delayed_operation_entered(&self) {
        self.delayed_entered.notified().await;
    }

    /// Counts recorded operations of one kind whose key satisfies `is_counted`.
    pub(crate) fn count_operations(
        &self,
        operation: ObjectStoreOperation,
        is_counted: impl Fn(&str) -> bool,
    ) -> usize {
        self.operations
            .lock()
            .expect("fault lock")
            .iter()
            .filter(|(recorded, key)| *recorded == operation && is_counted(key))
            .count()
    }

    /// Counts recorded operations of one kind per key, for keys under `prefix`.
    pub(crate) fn count_operations_per_key(
        &self,
        operation: ObjectStoreOperation,
        prefix: &str,
    ) -> BTreeMap<String, usize> {
        let mut counts = BTreeMap::new();
        for (recorded, key) in self.operations.lock().expect("fault lock").iter() {
            if *recorded == operation && key.starts_with(prefix) {
                *counts.entry(key.clone()).or_insert(0) += 1;
            }
        }
        counts
    }

    fn arm_delay(
        &self,
        operation: ObjectStoreOperation,
        key: &str,
        matches_prefix: bool,
        delay: Duration,
    ) {
        *self.delayed.lock().expect("fault lock") = Some(DelayedOperation {
            operation,
            key: key.to_owned(),
            matches_prefix,
            delay,
        });
    }
}

impl FaultInjector for SwitchableFaults {
    fn action(&self, operation: ObjectStoreOperation, key: &str) -> FaultAction {
        self.operations
            .lock()
            .expect("fault lock")
            .push((operation, key.to_owned()));
        let armed = self.delayed.lock().expect("fault lock").take_if(|delayed| {
            delayed.operation == operation
                && if delayed.matches_prefix {
                    key.starts_with(&delayed.key)
                } else {
                    key == delayed.key
                }
        });
        if let Some(delayed) = armed {
            self.delayed_entered.notify_one();
            return FaultAction::Delay(delayed.delay);
        }
        if operation == ObjectStoreOperation::Get
            && let Some((prefix, delay)) = &*self.delayed_gets.lock().expect("fault lock")
            && key.starts_with(prefix.as_str())
        {
            return FaultAction::Delay(*delay);
        }
        if operation != ObjectStoreOperation::Put {
            return FaultAction::Proceed;
        }
        if let Some((limited_key, allowed)) = &mut *self.limited_put.lock().expect("fault lock")
            && limited_key == key
        {
            if *allowed == 0 {
                return FaultAction::FailBeforeEffect;
            }
            *allowed -= 1;
        }
        let failing = self.failing_put_prefix.lock().expect("fault lock");
        match failing.as_deref() {
            Some(prefix) if key.starts_with(prefix) => FaultAction::FailBeforeEffect,
            _ => FaultAction::Proceed,
        }
    }
}

/// Object storage, clock, and applier shared by every node start of one test.
pub(crate) struct TestCluster {
    pub(crate) object_store: Arc<InMemoryObjectStore>,
    pub(crate) faults: Arc<SwitchableFaults>,
    pub(crate) clock: Arc<TestClock>,
    pub(crate) hasher: PartitionHasher,
}

impl TestCluster {
    pub(crate) fn new() -> Self {
        let faults = Arc::new(SwitchableFaults::default());
        let clock = Arc::new(TestClock::new());
        Self {
            object_store: Arc::new(InMemoryObjectStore::with_injector_and_clock(
                faults.clone(),
                clock.clone(),
            )),
            faults,
            clock,
            hasher: PartitionHasher::new(&[0; 16], PARTITION_COUNT).expect("valid hasher"),
        }
    }

    pub(crate) fn config(&self, local_store: LocalStoreConfig) -> EngineNodeConfig {
        EngineNodeConfig {
            node: NodeConfig {
                node_id: "node-a".to_owned(),
                incarnation: 1,
                internal_address: "127.0.0.1:9000".to_owned(),
            },
            cluster: ClusterConfig {
                partition_count: PARTITION_COUNT,
            },
            lease: LeaseConfig::default(),
            local_store,
            commit: CommitConfig {
                applier_lanes: 2,
                ..CommitConfig::default()
            },
            snapshots: SnapshotConfig::default(),
            recovery: RecoveryConfig::default(),
        }
    }

    pub(crate) async fn start(
        &mut self,
        config: &EngineNodeConfig,
        applier: Arc<CountingApplier>,
        runtime: Arc<RecordingRuntime>,
    ) -> Result<TestNode, EngineStartError> {
        let factory_runtime = runtime.clone();
        let node = EngineNode::start(
            config,
            EngineNodeDependencies {
                object_store: self.object_store.clone(),
                clock: self.clock.clone(),
                applier: applier.clone(),
            },
            move |_services: EngineServices| EngineRuntimeHooks {
                committed_commands: factory_runtime.clone(),
                rebuilder: factory_runtime,
            },
        )
        .await?;
        let cluster = dexzd_cluster_metadata::bootstrap_or_validate(
            self.object_store.as_ref(),
            self.clock.as_ref(),
            config.cluster,
        )
        .await
        .expect("cluster exists");
        self.hasher = cluster.partition_hasher;
        Ok(TestNode {
            node,
            applier,
            runtime,
            hasher: self.hasher.clone(),
        })
    }

    pub(crate) async fn start_in_memory(&mut self) -> TestNode {
        let config = self.config(LocalStoreConfig::InMemory);
        self.start_with(&config).await.expect("node starts")
    }

    /// Starts a node with a fresh applier and runtime.
    pub(crate) async fn start_with(
        &mut self,
        config: &EngineNodeConfig,
    ) -> Result<TestNode, EngineStartError> {
        self.start(
            config,
            Arc::new(CountingApplier::default()),
            Arc::new(RecordingRuntime::default()),
        )
        .await
    }

    pub(crate) async fn cluster_manifest(&self) -> Manifest {
        dexzd_cluster_metadata::read_manifest(self.object_store.as_ref())
            .await
            .expect("read cluster manifest")
            .expect("cluster manifest exists")
    }

    /// Replaces the node lease with an expired one, as a successor's takeover would.
    pub(crate) async fn expire_lease_of(&self, node_id: &str) {
        let key = format!("lease/{node_id}");
        let current = self
            .object_store
            .get(&key)
            .await
            .expect("read lease")
            .expect("lease exists");
        let mut lease = NodeLease::decode(current.bytes).expect("decode lease");
        lease.holder_process_id = "taken-over-by-a-test".to_owned();
        lease.expires_at_unix_micros = 0;
        lease.version += 1;
        self.object_store
            .put(
                &key,
                lease.encode_to_vec().into(),
                dexzd_object_store::WriteCondition::Unconditional,
            )
            .await
            .expect("overwrite lease");
    }
}

pub(crate) struct TestNode {
    pub(crate) node: EngineNode,
    pub(crate) applier: Arc<CountingApplier>,
    pub(crate) runtime: Arc<RecordingRuntime>,
    pub(crate) hasher: PartitionHasher,
}

impl TestNode {
    pub(crate) fn context(&self, flow_id: &str) -> ApplyContext {
        let flow_hash = self.hasher.flow_hash(TenantId::DEFAULT, flow_id);
        ApplyContext {
            partition: self.hasher.partition_of(flow_hash),
            flow_hash,
        }
    }

    pub(crate) fn submit(
        &self,
        flow_id: &str,
        urgency: CommitUrgency,
    ) -> Result<(FlowCommand, CommitTicket), CommitError> {
        self.node
            .committer()
            .submit(self.context(flow_id), flow_command(flow_id), urgency)
    }

    /// Submits `count` prompt commands and waits for each to commit.
    pub(crate) async fn commit_prompt(&self, flow_id: &str, count: usize) {
        for _ in 0..count {
            let (_, ticket) = self
                .submit(flow_id, CommitUrgency::Prompt)
                .expect("submit succeeds");
            ticket.wait().await.expect("command commits");
        }
    }

    /// Returns the committed counter of `flow_id`.
    pub(crate) async fn committed_count(&self, flow_id: &str) -> u64 {
        let view = self
            .node
            .committer()
            .committed_view()
            .expect("committed view opens");
        self.count_in_view(view, flow_id).await
    }

    /// Returns the counter of `flow_id` as `view` sees it.
    pub(crate) async fn count_in_view(&self, view: CommittedStateView, flow_id: &str) -> u64 {
        let key = counter_key(&self.context(flow_id), flow_id);
        tokio::task::spawn_blocking(move || {
            view.get(ColumnFamily::Flow, &key)
                .expect("committed read succeeds")
                .map_or(0, |bytes| decode_u64(&bytes))
        })
        .await
        .expect("read task completes")
    }

    /// Returns a Flow ID whose partition differs from every partition in `taken`.
    pub(crate) fn flow_in_other_partition(&self, taken: &[&str]) -> String {
        let taken_partitions: Vec<PartitionId> = taken
            .iter()
            .map(|flow| self.context(flow).partition)
            .collect();
        (0..)
            .map(|index| format!("flow-{index}"))
            .find(|flow| !taken_partitions.contains(&self.context(flow).partition))
            .expect("some Flow ID uses another partition")
    }
}

pub(crate) fn flow_command(flow_id: &str) -> FlowCommand {
    FlowCommand {
        flow_id: flow_id.to_owned(),
        command: Some(flow_command::Command::AttributesSet(
            AttributesSetCommand::default(),
        )),
        ..FlowCommand::default()
    }
}

pub(crate) fn is_partition_stopped(error: &CommitError) -> bool {
    matches!(error, CommitError::Unavailable { detail } if detail.contains("stopped applying"))
}
