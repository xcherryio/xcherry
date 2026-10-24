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

use dexzd_storage::LocalStore;

use crate::commit::EngineLocalStore;

type StoreReadView<'store> = Box<dyn LocalReadView + Send + Sync + 'store>;

self_cell::self_cell!(
    /// A local store consistent view that keeps its store alive.
    struct OwnedStoreReadView {
        owner: Arc<EngineLocalStore>,
        #[covariant]
        dependent: StoreReadView,
    }
);

/// Committed state at one point in time: later writes stay invisible.
///
/// Reads block, so use it on a blocking-pool thread.
pub struct CommittedStateView {
    view: OwnedStoreReadView,
}

impl CommittedStateView {
    /// Opens a consistent view of the local store.
    pub(crate) fn open(store: Arc<EngineLocalStore>) -> Result<Self, StorageError> {
        let view = OwnedStoreReadView::try_new(store, |store| store.consistent_view())?;
        Ok(Self { view })
    }

    fn read_view(&self) -> &dyn LocalReadView {
        self.view.borrow_dependent().as_ref()
    }
}

impl LocalReadView for CommittedStateView {
    fn get(
        &self,
        column_family: ColumnFamily,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StorageError> {
        self.read_view().get(column_family, key)
    }

    fn scan(
        &self,
        column_family: ColumnFamily,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        direction: ScanDirection,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        self.read_view()
            .scan(column_family, start, end, direction, limit)
    }
}

impl FlowStateReader for CommittedStateView {
    fn get(
        &self,
        column_family: ColumnFamily,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StateReadError> {
        read_row(self.read_view(), column_family, key)
    }

    fn scan(
        &self,
        column_family: ColumnFamily,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<StateRow>, StateReadError> {
        scan_rows(self.read_view(), column_family, start, end, limit)
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
