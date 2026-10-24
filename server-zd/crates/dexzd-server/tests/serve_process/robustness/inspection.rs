// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Reads a node's objects through the S3 API to check log, snapshot, and lease state.

use dexzd_object_store::{ObjectStore, S3Credentials, S3ObjectStore, S3ObjectStoreConfig};
use dexzd_protocol::dexzd::v1::{LogBatchHeader, NodeLease};
use prost::Message;

use crate::harness;
use crate::{NODE_ID, PRIMARY_BUCKET, PRIMARY_PREFIX};

const LOG_BATCH_MAGIC: &[u8; 8] = b"DEXZDLOG";
/// Magic, a u16 format version, and a u32 header length precede the header.
const LOG_BATCH_PREFIX_LENGTH: usize = 14;

/// Reads the node's objects through the S3 API, as another process would.
pub(crate) struct ObjectStoreInspector {
    store: S3ObjectStore,
}

impl ObjectStoreInspector {
    pub(crate) async fn connect(primary_endpoint: &str) -> Self {
        let mut config = S3ObjectStoreConfig::new("us-east-1", PRIMARY_BUCKET);
        config.endpoint = Some(primary_endpoint.to_owned());
        config.key_prefix = PRIMARY_PREFIX.to_owned();
        config.force_path_style = true;
        config.credentials = S3Credentials::Static {
            access_key_id: harness::ACCESS_KEY_ID.to_owned(),
            secret_access_key: harness::SECRET_ACCESS_KEY.to_owned(),
            session_token: None,
        };
        Self {
            store: S3ObjectStore::new(config)
                .await
                .expect("the inspection store is configured"),
        }
    }

    /// Returns every key under `prefix`, relative to the node's key prefix, in ascending order.
    pub(crate) async fn keys_under(&self, prefix: &str) -> Vec<String> {
        let mut keys = Vec::new();
        let mut start_after = None;
        loop {
            let listing = self
                .store
                .list(prefix, start_after.as_deref(), 1_000)
                .await
                .expect("the inspection store lists objects");
            keys.extend(listing.objects.into_iter().map(|object| object.key));
            match listing.next_start_after {
                Some(next) => start_after = Some(next),
                None => return keys,
            }
        }
    }

    pub(crate) async fn has_object(&self, key: &str) -> bool {
        self.store
            .head(key)
            .await
            .expect("the inspection store reads object metadata")
            .is_some()
    }

    /// Returns the keys of every retained snapshot manifest.
    pub(crate) async fn snapshot_manifest_keys(&self) -> Vec<String> {
        self.keys_under(&format!("snapshot/{NODE_ID}/"))
            .await
            .into_iter()
            .filter(|key| key.contains("/manifest/"))
            .collect()
    }

    /// Decodes the header of every log batch object, in key order.
    pub(crate) async fn log_batch_headers(&self) -> Vec<LogBatchHeader> {
        let mut headers = Vec::new();
        for key in self.keys_under(&format!("log/{NODE_ID}/")).await {
            let Some(content) = self
                .store
                .get(&key)
                .await
                .expect("the inspection store reads a log batch")
            else {
                // Log cleanup may delete a listed batch before this read.
                continue;
            };
            headers.push(decode_log_batch_header(&key, &content.bytes));
        }
        headers
    }

    pub(crate) async fn node_lease(&self) -> NodeLease {
        let content = self
            .store
            .get(&format!("lease/{NODE_ID}"))
            .await
            .expect("the inspection store reads the lease")
            .expect("the node lease exists");
        NodeLease::decode(content.bytes).expect("the lease object is a NodeLease")
    }
}

/// Returns the key of the first log batch the node ever wrote.
pub(crate) fn first_log_batch_key() -> String {
    format!("log/{NODE_ID}/{:010}/{:020}", 1, 1)
}

fn decode_log_batch_header(key: &str, body: &[u8]) -> LogBatchHeader {
    assert!(
        body.len() >= LOG_BATCH_PREFIX_LENGTH && &body[..LOG_BATCH_MAGIC.len()] == LOG_BATCH_MAGIC,
        "{key} is not a log batch"
    );
    let header_length = u32::from_le_bytes(
        body[10..LOG_BATCH_PREFIX_LENGTH]
            .try_into()
            .expect("the header length is four bytes"),
    ) as usize;
    let header_end = LOG_BATCH_PREFIX_LENGTH + header_length;
    assert!(body.len() >= header_end, "{key} has a truncated header");
    LogBatchHeader::decode(&body[LOG_BATCH_PREFIX_LENGTH..header_end])
        .unwrap_or_else(|error| panic!("{key} has an undecodable header: {error}"))
}
