// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use dexzd_apply::{
    ApplyContext, ApplyEffect, ApplyError, ApplyOutcome, FlowStateReader, StateWrite,
};
use dexzd_clock::WallClock;
use dexzd_cluster_metadata::{ClusterConfig, LeaseConfig, NodeConfig};
use dexzd_engine::{
    CommandApplier, CommitConfig, CommitError, CommitTicket, CommitUrgency, CommittedCommandSink,
    CommittedFlowCommand, EngineNode, EngineNodeConfig, EngineNodeDependencies, EngineRuntimeHooks,
    EngineServices, EngineStartError, LocalStoreConfig, PartitionRuntimeRebuilder,
    RuntimeRebuildError, SnapshotConfig,
};
use dexzd_keyspace::{
    ColumnFamily, PartitionHasher, PartitionId, TenantId, flow_directory_key, flow_directory_prefix,
};
use dexzd_object_store::{FaultAction, FaultInjector, InMemoryObjectStore, ObjectStoreOperation};
use dexzd_protocol::dexzd::v1::{AttributesSetCommand, CommandReply, FlowCommand, flow_command};
use futures::future::BoxFuture;
use tokio::time::Instant;

pub(crate) const PARTITION_COUNT: u32 = 4;
pub(crate) const FAILING_FLOW: &str = "fails-to-apply";
pub(crate) const PANICKING_FLOW: &str = "panics-in-apply";
pub(crate) const NONDETERMINISTIC_FLOW: &str = "applies-nondeterministically";

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
}

impl CountingApplier {
    pub(crate) fn applied_commands(&self) -> Vec<FlowCommand> {
        self.applied.lock().expect("applied lock").clone()
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
        let mut effects = Vec::new();
        if command.flow_id == NONDETERMINISTIC_FLOW {
            effects.push(ApplyEffect::TimerArmed {
                fire_at_micros: i64::try_from(
                    self.nondeterministic_counter.fetch_add(1, Ordering::SeqCst),
                )
                .expect("counter fits i64"),
            });
        }
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
                    value: command.proposed_at_micros.to_be_bytes().to_vec(),
                },
            ],
            effects,
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

/// Records rebuilt partitions and every committed command.
#[derive(Default)]
pub(crate) struct RecordingRuntime {
    rebuilt: Mutex<Vec<PartitionId>>,
    committed: Mutex<Vec<CommittedFlowCommand>>,
    failing_partition: Option<PartitionId>,
}

impl RecordingRuntime {
    pub(crate) fn failing_rebuild_of(partition: PartitionId) -> Self {
        Self {
            failing_partition: Some(partition),
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
}

impl CommittedCommandSink for RecordingRuntime {
    fn apply_committed(&self, committed: CommittedFlowCommand) {
        self.committed
            .lock()
            .expect("committed lock")
            .push(committed);
    }
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

/// Fails every write under a key prefix once switched on.
#[derive(Default)]
pub(crate) struct SwitchableFaults {
    failing_put_prefix: Mutex<Option<String>>,
}

impl SwitchableFaults {
    pub(crate) fn fail_puts_under(&self, prefix: &str) {
        *self.failing_put_prefix.lock().expect("fault lock") = Some(prefix.to_owned());
    }
}

impl FaultInjector for SwitchableFaults {
    fn action(&self, operation: ObjectStoreOperation, key: &str) -> FaultAction {
        let failing = self.failing_put_prefix.lock().expect("fault lock");
        match failing.as_deref() {
            Some(prefix) if operation == ObjectStoreOperation::Put && key.starts_with(prefix) => {
                FaultAction::FailBeforeEffect
            }
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
        Self {
            object_store: Arc::new(InMemoryObjectStore::with_injector(faults.clone())),
            faults,
            clock: Arc::new(TestClock::new()),
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
        self.start(
            &config,
            Arc::new(CountingApplier::default()),
            Arc::new(RecordingRuntime::default()),
        )
        .await
        .expect("node starts")
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
        let view = self.node.committer().committed_view();
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
