// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! The recovered-store check of a store that applied fewer frames than the shadow, as during an
//! old-epoch overlap.

use std::sync::{Arc, Mutex};

use dexzd_engine::{CommittedCommand, CommittedFlowCommand, EngineLocalStore};
use dexzd_keyspace::ColumnFamily;
use dexzd_protocol::dexzd::v1::{LogPosition, PartitionMeta, flow_command::Command};
use dexzd_storage::{InMemoryLocalStore, LocalStore, WriteBatch};
use tokio::time::Instant;

use super::CommittedLog;
use crate::apply_checker::ApplyChecker;
use crate::engine_simulation::engine_invariants::{all_flow_ids, check_recovered_flows};
use crate::engine_simulation::event_trace::EventTrace;
use crate::engine_simulation::simulated_object_store::StorageObservations;
use crate::invariants::InvariantViolation;
use crate::memory_state::MemoryFlowState;
use crate::model::CommandGenerator;

/// Any constant keeps the command sequence reproducible.
const SEED: u64 = 0x5eed_0000;
const MAX_COMMANDS: u64 = 2_000;
const PROCESS_LABEL: &str = "p1";

/// The old-epoch process applies one frame per command; the successor's store lags one frame.
#[test]
fn a_store_behind_an_old_epoch_frame_is_checked_with_the_window_points_of_its_position() {
    let log = CommittedLog::new(
        Arc::new(EventTrace::new(Instant::now())),
        Arc::new(Mutex::new(StorageObservations::default())),
    );
    let mut engine = ApplyChecker::new();
    let mut generator = CommandGenerator::new(SEED);
    let mut has_lagging_store_needing_pruned_point = false;
    let mut has_newer_point_than_position = false;
    for seq in 1..=MAX_COMMANDS {
        let lagging_state = engine.state().clone();
        let mut command = generator
            .next_command(&engine)
            .expect("committed state decodes");
        if let Some(Command::FlowStart(start)) = command.command.as_mut() {
            let settings = start.settings.as_mut().expect("generated run settings");
            settings.history_window_semantic_events = 2;
            settings.history_prune_chunk_semantic_events = 1;
        }
        let outcome = engine
            .apply_and_check(&command)
            .unwrap_or_else(|violation| panic!("{violation}"));
        let context = engine.context(&command.flow_id);
        let committed = CommittedFlowCommand {
            command,
            context,
            committed: CommittedCommand {
                outcome,
                position: log_position(seq),
            },
        };
        log.observe(PROCESS_LABEL, &committed);
        log.finish_frame(PROCESS_LABEL, context.partition, &log_position(seq));
        assert_eq!(log.first_violation(), None);

        let lagging_store = store_at(&engine, &lagging_state, seq - 1);
        let lagging_has_older_points = check_recovered_flows(&lagging_store, &log)
            .unwrap_or_else(|violation| panic!("lagging store at command {seq}: {violation}"));
        let current_store = store_at(&engine, engine.state(), seq);
        let current_has_older_points = check_recovered_flows(&current_store, &log)
            .unwrap_or_else(|violation| panic!("current store at command {seq}: {violation}"));
        assert!(
            !current_has_older_points,
            "a store at the newest frame got older window points at command {seq}"
        );

        // The newest position's points are what the check used before; the shadow pruned some.
        if is_missing_window_point(check_recovered_flows(
            &store_at(&engine, &lagging_state, seq),
            &log,
        )) {
            assert!(lagging_has_older_points, "command {seq}");
            has_lagging_store_needing_pruned_point = true;
        }
        has_newer_point_than_position |= is_missing_window_point(check_recovered_flows(
            &store_at(&engine, engine.state(), seq - 1),
            &log,
        ));
        if has_lagging_store_needing_pruned_point && has_newer_point_than_position {
            return;
        }
    }
    panic!(
        "a lagging store needed a pruned point: {has_lagging_store_needing_pruned_point}; a \
         store held a point newer than its position: {has_newer_point_than_position}"
    );
}

fn log_position(seq: u64) -> LogPosition {
    LogPosition {
        node_id: "node-1".to_owned(),
        incarnation: 1,
        seq,
    }
}

/// Returns a store holding `state` whose partitions applied the frame at `applied_seq`.
fn store_at(engine: &ApplyChecker, state: &MemoryFlowState, applied_seq: u64) -> EngineLocalStore {
    let mut batch = WriteBatch::new();
    for column_family in ColumnFamily::ALL {
        for (key, value) in state.rows_with_prefix(column_family, &[]) {
            batch.put(column_family, key, value);
        }
    }
    for flow_id in &all_flow_ids(engine) {
        let meta = PartitionMeta {
            partition: u32::from(engine.context(flow_id).partition.0),
            applied: Some(log_position(applied_seq)),
            ..PartitionMeta::default()
        };
        batch
            .put_partition_meta(&meta)
            .expect("partition metadata encodes");
    }
    let store = InMemoryLocalStore::new();
    store
        .write(batch)
        .expect("the in-memory store accepts writes");
    EngineLocalStore::Memory(store)
}

/// Returns whether the check failed only because a semantic point has no recorded state.
fn is_missing_window_point(result: Result<bool, InvariantViolation>) -> bool {
    match result {
        Ok(_) => false,
        Err(violation)
            if violation.invariant == "I1-window-value-reference"
                && violation.detail.contains("has no recorded state") =>
        {
            true
        }
        Err(violation) => panic!("unexpected violation: {violation}"),
    }
}
