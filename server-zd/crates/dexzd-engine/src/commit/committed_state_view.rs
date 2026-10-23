// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;

use dexzd_apply::{FlowStateReader, StateReadError, StateRow};
use dexzd_keyspace::ColumnFamily;
use dexzd_storage::{LocalReadView, ScanDirection, StorageError};

use crate::commit::EngineLocalStore;

/// Read view of committed state in the local store; implements [`FlowStateReader`].
///
/// Every read is a synchronous local store call, so use the view from a blocking-pool thread.
#[derive(Clone)]
pub struct CommittedStateView {
    store: Arc<EngineLocalStore>,
}

impl CommittedStateView {
    pub(crate) fn new(store: Arc<EngineLocalStore>) -> Self {
        Self { store }
    }
}

impl FlowStateReader for CommittedStateView {
    fn get(
        &self,
        column_family: ColumnFamily,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StateReadError> {
        read_row(self.store.as_ref(), column_family, key)
    }

    fn scan(
        &self,
        column_family: ColumnFamily,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<StateRow>, StateReadError> {
        scan_rows(self.store.as_ref(), column_family, start, end, limit)
    }
}

pub(crate) fn read_row(
    view: &dyn LocalReadView,
    column_family: ColumnFamily,
    key: &[u8],
) -> Result<Option<Vec<u8>>, StateReadError> {
    view.get(column_family, key)
        .map_err(|error| state_read_error(column_family, &error))
}

pub(crate) fn scan_rows(
    view: &dyn LocalReadView,
    column_family: ColumnFamily,
    start: &[u8],
    end: &[u8],
    limit: usize,
) -> Result<Vec<StateRow>, StateReadError> {
    if start >= end || limit == 0 {
        return Ok(Vec::new());
    }
    view.scan(
        column_family,
        Some(start),
        Some(end),
        ScanDirection::Forward,
        limit,
    )
    .map_err(|error| state_read_error(column_family, &error))
}

fn state_read_error(column_family: ColumnFamily, error: &StorageError) -> StateReadError {
    StateReadError {
        column_family,
        detail: error.to_string(),
    }
}
