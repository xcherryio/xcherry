// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_keyspace::row_keys::{
    RequestKind, attempt_record_key, channel_message_key, channel_name_prefix, history_record_key,
    history_row_metadata_key, prefix_range, request_dedup_key, request_dedup_order_key,
    run_named_key, run_singleton_key,
};
use dexzd_keyspace::{
    ColumnFamily, FlowHash, RunId, RunTable, TenantId, flow_directory_key, run_table_prefix,
};
use dexzd_protocol::dexzd::v1::{
    AttemptRecord, AttributeRecord, ChannelMessageRecord, ChannelMetaRecord, FlowDirectoryRecord,
    HistoryRecord, HistoryRowMetadata, LockRecord, PermissionSetRecord, RequestDedupRecord,
    RunRecord, SearchDocumentRecord, StepCounterRecord, StepExecutionRecord,
};
use prost::Message;

use crate::ApplyError;
use crate::state_access::FlowTransaction;
use crate::values::{ValueHolder, release_stored_value};

/// Upper bound for scans that read a whole table of one run.
pub(crate) const WHOLE_TABLE: usize = usize::MAX;

/// Typed access to the rows of one Flow's directory.
pub(crate) fn read_flow_directory(
    transaction: &FlowTransaction<'_>,
    tenant_id: TenantId,
    flow_id: &str,
) -> Result<FlowDirectoryRecord, ApplyError> {
    let key = flow_directory_key(transaction.context().flow_hash, tenant_id, flow_id);
    Ok(transaction
        .get_message(ColumnFamily::Flow, &key, "flow directory")?
        .unwrap_or_default())
}

pub(crate) fn write_flow_directory(
    transaction: &mut FlowTransaction<'_>,
    tenant_id: TenantId,
    flow_id: &str,
    directory: &FlowDirectoryRecord,
) {
    let key = flow_directory_key(transaction.context().flow_hash, tenant_id, flow_id);
    if directory.runs.is_empty() {
        transaction.delete(ColumnFamily::Flow, key);
    } else {
        transaction.put_message(ColumnFamily::Flow, key, directory);
    }
}

/// Typed access to the rows of one run.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RunRows {
    pub(crate) flow_hash: FlowHash,
    pub(crate) run_id: RunId,
}

impl RunRows {
    pub(crate) fn new(flow_hash: FlowHash, run_id: RunId) -> Self {
        Self { flow_hash, run_id }
    }

    pub(crate) fn run_record(
        &self,
        transaction: &FlowTransaction<'_>,
    ) -> Result<Option<RunRecord>, ApplyError> {
        let key = run_singleton_key(self.flow_hash, self.run_id, RunTable::RunRecord);
        transaction.get_message(ColumnFamily::Flow, &key, "run record")
    }

    pub(crate) fn put_run_record(&self, transaction: &mut FlowTransaction<'_>, record: &RunRecord) {
        let key = run_singleton_key(self.flow_hash, self.run_id, RunTable::RunRecord);
        transaction.put_message(ColumnFamily::Flow, key, record);
    }

    pub(crate) fn put_attempt_record(
        &self,
        transaction: &mut FlowTransaction<'_>,
        record: &AttemptRecord,
    ) {
        let key = attempt_record_key(self.flow_hash, self.run_id, record.attempt);
        transaction.put_message(ColumnFamily::Flow, key, record);
    }

    pub(crate) fn attempt_record(
        &self,
        transaction: &FlowTransaction<'_>,
        attempt: u32,
    ) -> Result<Option<AttemptRecord>, ApplyError> {
        let key = attempt_record_key(self.flow_hash, self.run_id, attempt);
        transaction.get_message(ColumnFamily::Flow, &key, "attempt record")
    }

    pub(crate) fn attribute(
        &self,
        transaction: &FlowTransaction<'_>,
        attribute_key: &str,
    ) -> Result<Option<AttributeRecord>, ApplyError> {
        let key = run_named_key(
            self.flow_hash,
            self.run_id,
            RunTable::Attribute,
            attribute_key,
        );
        transaction.get_message(ColumnFamily::Flow, &key, "attribute")
    }

    pub(crate) fn put_attribute(
        &self,
        transaction: &mut FlowTransaction<'_>,
        attribute_key: &str,
        record: &AttributeRecord,
    ) {
        let key = run_named_key(
            self.flow_hash,
            self.run_id,
            RunTable::Attribute,
            attribute_key,
        );
        transaction.put_message(ColumnFamily::Flow, key, record);
    }

    pub(crate) fn delete_attribute(
        &self,
        transaction: &mut FlowTransaction<'_>,
        attribute_key: &str,
    ) {
        let key = run_named_key(
            self.flow_hash,
            self.run_id,
            RunTable::Attribute,
            attribute_key,
        );
        transaction.delete(ColumnFamily::Flow, key);
    }

    /// Every Attribute of the current attempt in key order, keyed by Attribute key.
    pub(crate) fn attributes(
        &self,
        transaction: &FlowTransaction<'_>,
    ) -> Result<Vec<(String, AttributeRecord)>, ApplyError> {
        let prefix = run_table_prefix(self.flow_hash, self.run_id, RunTable::Attribute);
        self.scan_named(transaction, ColumnFamily::Flow, &prefix, "attribute")
    }

    pub(crate) fn step_execution(
        &self,
        transaction: &FlowTransaction<'_>,
        step_execution_id: &str,
    ) -> Result<Option<StepExecutionRecord>, ApplyError> {
        let key = run_named_key(
            self.flow_hash,
            self.run_id,
            RunTable::StepExecution,
            step_execution_id,
        );
        transaction.get_message(ColumnFamily::Flow, &key, "step execution")
    }

    pub(crate) fn put_step_execution(
        &self,
        transaction: &mut FlowTransaction<'_>,
        record: &StepExecutionRecord,
    ) {
        let key = run_named_key(
            self.flow_hash,
            self.run_id,
            RunTable::StepExecution,
            &record.step_execution_id,
        );
        transaction.put_message(ColumnFamily::Flow, key, record);
    }

    /// Every Step execution of the current attempt, in Step execution ID order.
    pub(crate) fn step_executions(
        &self,
        transaction: &FlowTransaction<'_>,
    ) -> Result<Vec<(String, StepExecutionRecord)>, ApplyError> {
        let prefix = run_table_prefix(self.flow_hash, self.run_id, RunTable::StepExecution);
        self.scan_named(transaction, ColumnFamily::Flow, &prefix, "step execution")
    }

    pub(crate) fn step_counter(
        &self,
        transaction: &FlowTransaction<'_>,
        step_type: &str,
    ) -> Result<StepCounterRecord, ApplyError> {
        let key = run_named_key(
            self.flow_hash,
            self.run_id,
            RunTable::StepCounter,
            step_type,
        );
        Ok(transaction
            .get_message(ColumnFamily::Flow, &key, "step counter")?
            .unwrap_or_default())
    }

    pub(crate) fn put_step_counter(
        &self,
        transaction: &mut FlowTransaction<'_>,
        step_type: &str,
        record: &StepCounterRecord,
    ) {
        let key = run_named_key(
            self.flow_hash,
            self.run_id,
            RunTable::StepCounter,
            step_type,
        );
        transaction.put_message(ColumnFamily::Flow, key, record);
    }

    pub(crate) fn lock(
        &self,
        transaction: &FlowTransaction<'_>,
        lock_key: &str,
    ) -> Result<LockRecord, ApplyError> {
        let key = run_named_key(self.flow_hash, self.run_id, RunTable::LockTable, lock_key);
        Ok(transaction
            .get_message(ColumnFamily::Flow, &key, "lock")?
            .unwrap_or_default())
    }

    pub(crate) fn put_lock(
        &self,
        transaction: &mut FlowTransaction<'_>,
        lock_key: &str,
        record: &LockRecord,
    ) {
        let key = run_named_key(self.flow_hash, self.run_id, RunTable::LockTable, lock_key);
        let is_unused = record.holder.is_none() && record.waiters.is_empty();
        if is_unused {
            transaction.delete(ColumnFamily::Flow, key);
        } else {
            transaction.put_message(ColumnFamily::Flow, key, record);
        }
    }

    pub(crate) fn channel_meta(
        &self,
        transaction: &FlowTransaction<'_>,
        channel_name: &str,
    ) -> Result<ChannelMetaRecord, ApplyError> {
        let key = run_named_key(
            self.flow_hash,
            self.run_id,
            RunTable::ChannelMeta,
            channel_name,
        );
        Ok(transaction
            .get_message(ColumnFamily::Flow, &key, "channel meta")?
            .unwrap_or_default())
    }

    pub(crate) fn put_channel_meta(
        &self,
        transaction: &mut FlowTransaction<'_>,
        channel_name: &str,
        record: &ChannelMetaRecord,
    ) {
        let key = run_named_key(
            self.flow_hash,
            self.run_id,
            RunTable::ChannelMeta,
            channel_name,
        );
        if record.pending_count == 0 && record.claimed_count == 0 {
            transaction.delete(ColumnFamily::Flow, key);
        } else {
            transaction.put_message(ColumnFamily::Flow, key, record);
        }
    }

    /// Every channel with pending or claimed messages, keyed by channel name.
    pub(crate) fn channel_metas(
        &self,
        transaction: &FlowTransaction<'_>,
    ) -> Result<Vec<(String, ChannelMetaRecord)>, ApplyError> {
        let prefix = run_table_prefix(self.flow_hash, self.run_id, RunTable::ChannelMeta);
        self.scan_named(transaction, ColumnFamily::Flow, &prefix, "channel meta")
    }

    pub(crate) fn put_channel_message(
        &self,
        transaction: &mut FlowTransaction<'_>,
        record: &ChannelMessageRecord,
    ) {
        let key = channel_message_key(
            self.flow_hash,
            self.run_id,
            &record.channel_name,
            record.enqueued_seq,
        );
        transaction.put_message(ColumnFamily::Flow, key, record);
    }

    pub(crate) fn delete_channel_message(
        &self,
        transaction: &mut FlowTransaction<'_>,
        record: &ChannelMessageRecord,
    ) {
        let key = channel_message_key(
            self.flow_hash,
            self.run_id,
            &record.channel_name,
            record.enqueued_seq,
        );
        transaction.delete(ColumnFamily::Flow, key);
    }

    /// Pending messages of one channel in FIFO order.
    pub(crate) fn channel_messages(
        &self,
        transaction: &FlowTransaction<'_>,
        channel_name: &str,
        limit: usize,
    ) -> Result<Vec<ChannelMessageRecord>, ApplyError> {
        let mut prefix = run_table_prefix(self.flow_hash, self.run_id, RunTable::ChannelMessage);
        prefix.extend_from_slice(&channel_name_prefix(channel_name));
        let (start, end) = prefix_range(&prefix);
        Ok(transaction
            .scan_messages(ColumnFamily::Flow, &start, &end, limit, "channel message")?
            .into_iter()
            .map(|(_, record)| record)
            .collect())
    }

    pub(crate) fn permission_set(
        &self,
        transaction: &FlowTransaction<'_>,
    ) -> Result<PermissionSetRecord, ApplyError> {
        let key = run_singleton_key(self.flow_hash, self.run_id, RunTable::PermissionSet);
        Ok(transaction
            .get_message(ColumnFamily::Flow, &key, "permission set")?
            .unwrap_or_default())
    }

    pub(crate) fn put_permission_set(
        &self,
        transaction: &mut FlowTransaction<'_>,
        record: &PermissionSetRecord,
    ) {
        let key = run_singleton_key(self.flow_hash, self.run_id, RunTable::PermissionSet);
        transaction.put_message(ColumnFamily::Flow, key, record);
    }

    pub(crate) fn request_dedup(
        &self,
        transaction: &FlowTransaction<'_>,
        kind: RequestKind,
        request_id: &str,
    ) -> Result<Option<RequestDedupRecord>, ApplyError> {
        let key = request_dedup_key(self.flow_hash, self.run_id, kind, request_id);
        transaction.get_message(ColumnFamily::Flow, &key, "request dedup")
    }

    /// Records a request ID and evicts the oldest entries beyond `capacity`.
    pub(crate) fn put_request_dedup(
        &self,
        transaction: &mut FlowTransaction<'_>,
        kind: RequestKind,
        request_id: &str,
        record: &RequestDedupRecord,
        capacity: u32,
    ) -> Result<(), ApplyError> {
        let key = request_dedup_key(self.flow_hash, self.run_id, kind, request_id);
        transaction.put_message(ColumnFamily::Flow, key, record);
        let mut order_value = vec![kind as u8];
        order_value.extend_from_slice(request_id.as_bytes());
        let order_key = request_dedup_order_key(self.flow_hash, self.run_id, record.recorded_seq);
        transaction.put(ColumnFamily::Flow, order_key, order_value);
        let prefix = run_table_prefix(self.flow_hash, self.run_id, RunTable::RequestDedupOrder);
        let (start, end) = prefix_range(&prefix);
        let entries = transaction.scan(ColumnFamily::Flow, &start, &end, WHOLE_TABLE)?;
        let excess = entries
            .len()
            .saturating_sub(usize::try_from(capacity).unwrap_or(usize::MAX));
        for (order_key, order_value) in entries.into_iter().take(excess) {
            transaction.delete(ColumnFamily::Flow, order_key);
            let Some((kind_byte, request_id_bytes)) = order_value.split_first() else {
                continue;
            };
            let mut evicted_key =
                run_table_prefix(self.flow_hash, self.run_id, RunTable::RequestDedup);
            evicted_key.push(*kind_byte);
            evicted_key.extend_from_slice(request_id_bytes);
            if *kind_byte == RequestKind::InvokeRpc as u8 {
                let previous: Option<RequestDedupRecord> =
                    transaction.get_message(ColumnFamily::Flow, &evicted_key, "request dedup")?;
                if let Some(output) = previous
                    .as_ref()
                    .and_then(|previous| previous.reply.as_ref())
                    .and_then(|reply| reply.reply.as_ref())
                    .and_then(|reply| match reply {
                        dexzd_protocol::dexzd::v1::command_reply::Reply::Rpc(rpc) => {
                            rpc.output.as_ref()
                        }
                        _ => None,
                    })
                {
                    let request_id = std::str::from_utf8(request_id_bytes).map_err(|error| {
                        ApplyError::InvariantViolation {
                            detail: format!("RPC dedup request ID is invalid UTF-8: {error}"),
                        }
                    })?;
                    let holder = ValueHolder::RpcReply {
                        request_id: request_id.to_owned(),
                    };
                    release_stored_value(transaction, *self, output, &holder, record.recorded_seq)?;
                }
            }
            transaction.delete(ColumnFamily::Flow, evicted_key);
        }
        Ok(())
    }

    #[expect(
        dead_code,
        reason = "Used by time travel and projections (tasks A4 and A7)."
    )]
    pub(crate) fn put_search_document(
        &self,
        transaction: &mut FlowTransaction<'_>,
        record: &SearchDocumentRecord,
    ) {
        let key = run_singleton_key(self.flow_hash, self.run_id, RunTable::SearchDocument);
        transaction.put_message(ColumnFamily::Flow, key, record);
    }

    #[expect(
        dead_code,
        reason = "Used by time travel and projections (tasks A4 and A7)."
    )]
    pub(crate) fn search_document(
        &self,
        transaction: &FlowTransaction<'_>,
    ) -> Result<Option<SearchDocumentRecord>, ApplyError> {
        let key = run_singleton_key(self.flow_hash, self.run_id, RunTable::SearchDocument);
        transaction.get_message(ColumnFamily::Flow, &key, "search document")
    }

    pub(crate) fn put_history_record(
        &self,
        transaction: &mut FlowTransaction<'_>,
        record: &HistoryRecord,
    ) {
        let key = history_record_key(self.flow_hash, self.run_id, record.seq);
        let row_size_bytes = u64::try_from(key.len() + record.encoded_len())
            .expect("a history record fits in a u64 byte count");
        transaction.put_message(ColumnFamily::History, key, record);
        transaction.put_message(
            ColumnFamily::Flow,
            history_row_metadata_key(
                self.flow_hash,
                self.run_id,
                record.seq,
                RunTable::HistoryRecord,
                0,
            ),
            &HistoryRowMetadata {
                row_size_bytes,
                is_semantic: record.is_semantic,
                is_external_input: record.is_external_input,
                attempt: record.attempt,
            },
        );
    }

    pub(crate) fn history_record(
        &self,
        transaction: &FlowTransaction<'_>,
        seq: u64,
    ) -> Result<Option<HistoryRecord>, ApplyError> {
        let key = history_record_key(self.flow_hash, self.run_id, seq);
        transaction.get_message(ColumnFamily::History, &key, "history record")
    }

    fn scan_named<RowMessage: prost::Message + Default>(
        &self,
        transaction: &FlowTransaction<'_>,
        column_family: ColumnFamily,
        prefix: &[u8],
        table: &'static str,
    ) -> Result<Vec<(String, RowMessage)>, ApplyError> {
        let (start, end) = prefix_range(prefix);
        transaction
            .scan_messages(column_family, &start, &end, WHOLE_TABLE, table)?
            .into_iter()
            .map(|(key, row)| {
                let name = String::from_utf8(key[prefix.len()..].to_vec()).map_err(|error| {
                    ApplyError::CorruptState {
                        table,
                        key: key.iter().map(|byte| format!("{byte:02x}")).collect(),
                        detail: error.to_string(),
                    }
                })?;
                Ok((name, row))
            })
            .collect()
    }
}
