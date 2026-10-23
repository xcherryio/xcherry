// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Python bindings for the shared Dex blob cache.

use dex_blob_cache::{BlobCache, BlobCacheConfig};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyModule};

#[pyclass]
struct NativeBlobCache {
    cache: BlobCache,
}

#[pymethods]
impl NativeBlobCache {
    #[new]
    fn new(directory: String, max_bytes: i64, frequency_counters: i64) -> PyResult<Self> {
        let config = BlobCacheConfig::new(directory, max_bytes, frequency_counters)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let cache =
            BlobCache::open(config).map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        Ok(Self { cache })
    }

    fn get<'py>(&self, py: Python<'py>, blob_id: &str) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let payload = py
            .allow_threads(|| self.cache.get(blob_id))
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        Ok(payload.map(|value| PyBytes::new(py, &value)))
    }

    fn put(&self, py: Python<'_>, blob_id: &str, payload: &[u8]) -> PyResult<bool> {
        py.allow_threads(|| self.cache.put(blob_id, payload))
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))
    }

    fn delete(&self, py: Python<'_>, blob_id: &str) -> PyResult<()> {
        py.allow_threads(|| self.cache.delete(blob_id))
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))
    }

    fn delete_all(&self, py: Python<'_>) -> PyResult<()> {
        py.allow_threads(|| self.cache.delete_all())
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        py.allow_threads(|| self.cache.close())
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))
    }
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<NativeBlobCache>()
}
