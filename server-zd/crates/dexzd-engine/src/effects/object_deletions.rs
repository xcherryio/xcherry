// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Object storage deletions that precede a command: hard-delete objects replaced by a committed
//! write, and a run's whole hard-delete prefix before its retention purge.

use dexzd_keyspace::row_keys::HARD_DELETE_PENDING_OBJECT;
use dexzd_keyspace::{ColumnFamily, RunId, RunTable, run_table_prefix};
use dexzd_protocol::dexzd::v1::{
    FlowCommand, HardDeleteObjectRecord, HardDeleteObjectsDeletedCommand, RunPurgeCommand,
    command_reply, flow_command,
};

use crate::effects::hard_delete_objects::HardDeleteObjectOwner;
use crate::effects::state_reads::{decode_row, read_run_record, scan_all};
use crate::effects::{EffectContext, EffectError};
use crate::runtime::{CommitUrgency, FlowAddress};

/// Deletes the run's hard-delete objects that wait for deletion, then proposes
/// `HardDeleteObjectsDeleted`; repeats until committed state has none left.
pub(crate) async fn delete_pending_hard_delete_objects(
    context: &EffectContext,
    flow: &FlowAddress,
    run_id: RunId,
) -> Result<(), EffectError> {
    let run = context.run_location(flow, run_id);
    let owner = HardDeleteObjectOwner {
        tenant_id: flow.tenant_id,
        flow_hash: run.flow_hash,
        run_id,
    };
    loop {
        let object_ids = {
            let reader = context.runtime.flow_state_reader(flow, false)?;
            let mut prefix =
                run_table_prefix(run.flow_hash, run.run_id, RunTable::HardDeleteObject);
            prefix.push(HARD_DELETE_PENDING_OBJECT);
            let mut object_ids = Vec::new();
            for (key, value) in scan_all(reader.as_ref(), ColumnFamily::Flow, &prefix)? {
                let record: HardDeleteObjectRecord = decode_row("HardDeleteObject", &key, &value)?;
                if let Some(object) = record.object {
                    object_ids.push(object.object_id);
                }
            }
            object_ids
        };
        if object_ids.is_empty() {
            return Ok(());
        }
        let mut backoff = context.retry_backoff();
        while let Err(error) = context
            .hard_delete_objects
            .delete_objects(owner, &object_ids)
            .await
        {
            tracing::warn!(%error, %run_id, "deleting hard-delete objects failed; retrying");
            if !backoff.wait(&context.shutdown).await {
                return Err(EffectError::Stopped);
            }
        }
        let mut sorted_object_ids = object_ids;
        object_ids_sorted_and_unique(&mut sorted_object_ids);
        let command = FlowCommand {
            flow_id: flow.flow_id.clone(),
            run_id: run_id.as_bytes().to_vec(),
            tenant_id: flow.tenant_id.0,
            command: Some(flow_command::Command::HardDeleteObjectsDeleted(
                HardDeleteObjectsDeletedCommand {
                    object_ids: sorted_object_ids,
                },
            )),
            ..FlowCommand::default()
        };
        let reply = context
            .propose_until_committed(flow, command, CommitUrgency::Prompt)
            .await
            .ok_or(EffectError::Stopped)?;
        if let Some(command_reply::Reply::Error(error)) = reply.reply {
            tracing::warn!(%run_id, message = %error.message, "HardDeleteObjectsDeleted was rejected");
            return Ok(());
        }
    }
}

/// Deletes the run's hard-delete object prefix, then proposes `RunPurge`.
pub(crate) async fn purge_run(
    context: &EffectContext,
    flow: &FlowAddress,
    run_id: RunId,
) -> Result<(), EffectError> {
    let run = context.run_location(flow, run_id);
    let is_purging = {
        let reader = context.runtime.flow_state_reader(flow, false)?;
        read_run_record(reader.as_ref(), run)?.is_some_and(|record| record.is_purging)
    };
    if !is_purging {
        return Ok(());
    }
    let owner = HardDeleteObjectOwner {
        tenant_id: flow.tenant_id,
        flow_hash: run.flow_hash,
        run_id,
    };
    let mut backoff = context.retry_backoff();
    while let Err(error) = context.hard_delete_objects.delete_run_prefix(owner).await {
        tracing::warn!(%error, %run_id, "deleting the run's hard-delete prefix failed; retrying");
        if !backoff.wait(&context.shutdown).await {
            return Err(EffectError::Stopped);
        }
    }
    let command = FlowCommand {
        flow_id: flow.flow_id.clone(),
        run_id: run_id.as_bytes().to_vec(),
        tenant_id: flow.tenant_id.0,
        command: Some(flow_command::Command::RunPurge(RunPurgeCommand {})),
        ..FlowCommand::default()
    };
    let reply = context
        .propose_until_committed(flow, command, CommitUrgency::Prompt)
        .await
        .ok_or(EffectError::Stopped)?;
    if let Some(command_reply::Reply::Error(error)) = reply.reply {
        tracing::warn!(%run_id, message = %error.message, "RunPurge was rejected");
    }
    Ok(())
}

fn object_ids_sorted_and_unique(object_ids: &mut Vec<String>) {
    object_ids.sort();
    object_ids.dedup();
}
