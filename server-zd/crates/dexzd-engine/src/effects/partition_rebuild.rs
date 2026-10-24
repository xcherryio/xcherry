// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Recovery: rebuilds a partition's pending effect work from committed state alone, so a
//! restarted owner behaves like one that never stopped.

use std::collections::BTreeMap;

use dexzd_apply::{ApplyEffect, FlowStateReader};
use dexzd_keyspace::{ColumnFamily, IndexKind, PartitionId, TenantId, partition_index_prefix};
use dexzd_protocol::dexzd::v1::{StepInvocationKey, StepInvocationPhase};

use crate::effects::method_request::configured_method_timeout_seconds;
use crate::effects::state_reads::{
    RunLocation, parse_retention_index_key, parse_run_item_index_key, read_run_record,
    read_step_executions, scan_all,
};
use crate::effects::{EffectContext, EffectError, RouterMessage};
use crate::runtime::FlowAddress;

/// Rebuilds the partition's work and submits it to the router as committed effects.
pub(crate) async fn rebuild_partition(
    context: &EffectContext,
    partition: PartitionId,
) -> Result<(), EffectError> {
    let startup_uploads = context
        .startup_pending_uploads
        .as_deref()
        .map_err(|detail| EffectError::StartupUploadsUnreadable {
            detail: detail.clone(),
        })?;
    let reader = context.runtime.committed_state_reader()?;
    context
        .hard_delete_objects
        .recover_pending_uploads(partition, startup_uploads, reader)
        .await?;
    let reader = context.runtime.committed_state_reader()?;
    let mut work: BTreeMap<RunLocation, (FlowAddress, Vec<ApplyEffect>)> = BTreeMap::new();
    rebuild_invocations(reader.as_ref(), partition, &mut work)?;
    rebuild_outbox_messages(reader.as_ref(), partition, &mut work)?;
    rebuild_hard_delete_deletions(reader.as_ref(), partition, &mut work)?;
    rebuild_run_purges(reader.as_ref(), partition, &mut work)?;
    let effect_count: usize = work.values().map(|(_, effects)| effects.len()).sum();
    for (flow, effects) in work.into_values() {
        context.report(RouterMessage::Effects {
            flow,
            effects,
            is_committed: true,
            scheduled_at: None,
        });
    }
    tracing::info!(%partition, effect_count, "rebuilt partition effects");
    Ok(())
}

/// Pending invocations start again; running attempts are redispatched with the same attempt.
fn rebuild_invocations(
    reader: &dyn FlowStateReader,
    partition: PartitionId,
    work: &mut BTreeMap<RunLocation, (FlowAddress, Vec<ApplyEffect>)>,
) -> Result<(), EffectError> {
    let prefix = partition_index_prefix(partition, IndexKind::ActiveWork);
    for (key, _) in scan_all(reader, ColumnFamily::PartitionIndex, &prefix)? {
        let (run, _) = parse_run_item_index_key(partition, IndexKind::ActiveWork, &key)?;
        let Some((flow, run_attempt)) = flow_of_run(reader, run)? else {
            continue;
        };
        let mut effects = Vec::new();
        for step_execution in read_step_executions(reader, run)? {
            let Some(invocation) = step_execution.invocation else {
                continue;
            };
            let key = StepInvocationKey {
                step_execution_id: step_execution.step_execution_id.clone(),
                method: invocation.method,
            };
            if invocation.phase == StepInvocationPhase::Pending as i32 {
                effects.push(ApplyEffect::InvocationPending {
                    run_id: run.run_id,
                    run_attempt,
                    invocation: key,
                    attempt: invocation.attempt,
                });
            } else if invocation.phase == StepInvocationPhase::Running as i32 {
                effects.push(ApplyEffect::InvocationStarted {
                    run_id: run.run_id,
                    run_attempt,
                    invocation: key,
                    attempt: invocation.attempt,
                    task_id: invocation.task_id,
                    method_timeout_seconds: configured_method_timeout_seconds(
                        step_execution.options.as_ref(),
                        invocation.method,
                    ),
                });
            }
        }
        add_effects(work, run, flow, effects);
    }
    Ok(())
}

fn rebuild_outbox_messages(
    reader: &dyn FlowStateReader,
    partition: PartitionId,
    work: &mut BTreeMap<RunLocation, (FlowAddress, Vec<ApplyEffect>)>,
) -> Result<(), EffectError> {
    let prefix = partition_index_prefix(partition, IndexKind::OutboxPending);
    let mut message_ids: BTreeMap<RunLocation, Vec<Vec<u8>>> = BTreeMap::new();
    for (key, _) in scan_all(reader, ColumnFamily::PartitionIndex, &prefix)? {
        let (run, message_id) =
            parse_run_item_index_key(partition, IndexKind::OutboxPending, &key)?;
        message_ids
            .entry(run)
            .or_default()
            .push(message_id.to_vec());
    }
    for (run, message_ids) in message_ids {
        if let Some((flow, _)) = flow_of_run(reader, run)? {
            let effect = ApplyEffect::OutboxMessagesWritten {
                run_id: run.run_id,
                message_ids,
            };
            add_effects(work, run, flow, vec![effect]);
        }
    }
    Ok(())
}

fn rebuild_hard_delete_deletions(
    reader: &dyn FlowStateReader,
    partition: PartitionId,
    work: &mut BTreeMap<RunLocation, (FlowAddress, Vec<ApplyEffect>)>,
) -> Result<(), EffectError> {
    let prefix = partition_index_prefix(partition, IndexKind::HardDeletePending);
    let mut object_ids: BTreeMap<RunLocation, Vec<String>> = BTreeMap::new();
    for (key, _) in scan_all(reader, ColumnFamily::PartitionIndex, &prefix)? {
        let (run, object_id) =
            parse_run_item_index_key(partition, IndexKind::HardDeletePending, &key)?;
        object_ids
            .entry(run)
            .or_default()
            .push(String::from_utf8_lossy(object_id).into_owned());
    }
    for (run, object_ids) in object_ids {
        if let Some((flow, _)) = flow_of_run(reader, run)? {
            let effect = ApplyEffect::HardDeleteObjectsPending {
                run_id: run.run_id,
                object_ids,
            };
            add_effects(work, run, flow, vec![effect]);
        }
    }
    Ok(())
}

/// Runs whose retention expired and whose purge started before the restart.
fn rebuild_run_purges(
    reader: &dyn FlowStateReader,
    partition: PartitionId,
    work: &mut BTreeMap<RunLocation, (FlowAddress, Vec<ApplyEffect>)>,
) -> Result<(), EffectError> {
    let prefix = partition_index_prefix(partition, IndexKind::Retention);
    for (key, _) in scan_all(reader, ColumnFamily::PartitionIndex, &prefix)? {
        let (_, run) = parse_retention_index_key(partition, &key)?;
        let Some(run_record) = read_run_record(reader, run)? else {
            continue;
        };
        if !run_record.is_purging {
            continue;
        }
        let flow = FlowAddress {
            tenant_id: TenantId(run_record.tenant_id),
            flow_id: run_record.flow_id,
        };
        let effect = ApplyEffect::RunPurgeRequested { run_id: run.run_id };
        add_effects(work, run, flow, vec![effect]);
    }
    Ok(())
}

/// Returns the run's Flow and current run attempt, or `None` when the run is gone.
fn flow_of_run(
    reader: &dyn FlowStateReader,
    run: RunLocation,
) -> Result<Option<(FlowAddress, u32)>, EffectError> {
    Ok(read_run_record(reader, run)?.map(|record| {
        (
            FlowAddress {
                tenant_id: TenantId(record.tenant_id),
                flow_id: record.flow_id,
            },
            record.current_attempt,
        )
    }))
}

fn add_effects(
    work: &mut BTreeMap<RunLocation, (FlowAddress, Vec<ApplyEffect>)>,
    run: RunLocation,
    flow: FlowAddress,
    effects: Vec<ApplyEffect>,
) {
    if effects.is_empty() {
        return;
    }
    work.entry(run)
        .or_insert_with(|| (flow, Vec::new()))
        .1
        .extend(effects);
}
