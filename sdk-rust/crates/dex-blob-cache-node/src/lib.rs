// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Node-API bindings for the shared Dex blob cache.

use dex_blob_cache::{BlobCache, BlobCacheConfig};
use napi::bindgen_prelude::*;
use napi_derive::napi;

#[napi]
pub struct NativeBlobCache {
    cache: BlobCache,
}

#[napi]
impl NativeBlobCache {
    #[napi(constructor)]
    pub fn new(directory: String, max_bytes: i64, frequency_counters: i64) -> Result<Self> {
        let config = BlobCacheConfig::new(directory, max_bytes, frequency_counters)
            .map_err(|error| Error::from_reason(error.to_string()))?;
        let cache =
            BlobCache::open(config).map_err(|error| Error::from_reason(error.to_string()))?;
        Ok(Self { cache })
    }

    #[napi]
    pub fn get(&self, blob_id: String) -> Result<Option<Buffer>> {
        match self
            .cache
            .get(&blob_id)
            .map_err(|error| Error::from_reason(error.to_string()))?
        {
            Some(payload) => Ok(Some(Buffer::from(payload))),
            None => Ok(None),
        }
    }

    #[napi]
    pub fn put(&self, blob_id: String, payload: Buffer) -> Result<bool> {
        self.cache
            .put(&blob_id, payload.as_ref())
            .map_err(|error| Error::from_reason(error.to_string()))
    }

    #[napi]
    pub fn delete(&self, blob_id: String) -> Result<()> {
        self.cache
            .delete(&blob_id)
            .map_err(|error| Error::from_reason(error.to_string()))
    }

    #[napi]
    pub fn delete_all(&self) -> Result<()> {
        self.cache
            .delete_all()
            .map_err(|error| Error::from_reason(error.to_string()))
    }

    #[napi]
    pub fn close(&self) -> Result<()> {
        self.cache
            .close()
            .map_err(|error| Error::from_reason(error.to_string()))
    }
}
