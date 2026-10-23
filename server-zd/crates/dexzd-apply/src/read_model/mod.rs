// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Pure reads of committed Flow state for the server API and search backend.

mod attributes;
mod blobs;
mod flow;
mod history;
mod predicates;
mod search;

pub use attributes::{AttributesRead, HardDeleteAttribute, get_attributes};
pub use blobs::load_blobs;
pub use flow::{get_channel_messages, get_flow_state, get_flow_summary, resolve_run_id};
pub use history::get_history_events;
pub use predicates::{
    is_flow_closed, is_history_event_available, is_step_execution_finished, match_attribute,
};
pub use search::{get_search_document, list_search_documents};

use dexzd_keyspace::{FlowHash, PartitionId, RunId};
use dexzd_protocol::dexzd::v1::RunRecord;

use crate::state_access::{FlowTransaction, RunRows};
use crate::{ApplyContext, ApplyError, FlowStateReader};

/// Error from reading a Flow or interpreting a read request.
#[derive(Debug, thiserror::Error)]
pub enum ReadModelError {
    /// The requested run has no retained state.
    #[error("Flow run {run_id} is not found")]
    RunNotFound {
        /// Requested run.
        run_id: RunId,
    },
    /// A caller supplied an invalid read request.
    #[error("invalid read request: {detail}")]
    InvalidRequest {
        /// Rejected input.
        detail: String,
    },
    /// A stored Attribute cannot be compared by WaitForAttribute.
    #[error("Attribute {key:?} cannot be compared by WaitForAttribute")]
    UnsupportedAttributeValue {
        /// Attribute key.
        key: String,
    },
    /// Stored state could not be read or decoded.
    #[error(transparent)]
    State(#[from] ApplyError),
}

struct RunView<'reader> {
    transaction: FlowTransaction<'reader>,
    rows: RunRows,
    record: RunRecord,
}

impl<'reader> RunView<'reader> {
    fn open(
        reader: &'reader dyn FlowStateReader,
        flow_hash: FlowHash,
        run_id: RunId,
    ) -> Result<Self, ReadModelError> {
        let transaction = FlowTransaction::new(
            reader,
            ApplyContext {
                partition: PartitionId(0),
                flow_hash,
            },
        );
        let rows = RunRows::new(flow_hash, run_id);
        let Some(record) = rows.run_record(&transaction)? else {
            return Err(ReadModelError::RunNotFound { run_id });
        };
        if record.is_purging {
            return Err(ReadModelError::RunNotFound { run_id });
        }
        Ok(Self {
            transaction,
            rows,
            record,
        })
    }
}
