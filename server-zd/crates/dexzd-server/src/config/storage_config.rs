// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::fmt;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Chooses how S3 credentials are supplied without putting secrets in YAML.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum CredentialSource {
    /// Default; AWS SDK resolves credentials from its configured provider chain.
    #[default]
    DefaultChain,
    /// Read an access key ID and secret from documented DEX_ZD_* environment variables.
    Environment,
    /// Send unsigned requests, suitable only for an embedded local S3 mock.
    Anonymous,
}

/// Static S3 credentials read from the environment after YAML loading.
#[derive(Clone, PartialEq, Eq)]
pub struct ObjectStoreCredentials {
    /// Access key ID from the selected environment variable.
    pub access_key_id: String,
    /// Secret access key from the selected environment variable.
    pub secret_access_key: String,
    /// Optional session token from the selected environment variable.
    pub session_token: Option<String>,
}

impl fmt::Debug for ObjectStoreCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ObjectStoreCredentials([redacted])")
    }
}

/// Names the durable object destination used by command logs and snapshots.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct ObjectStoreConfig {
    /// Defaults to empty; use AWS endpoint resolution, or set an HTTP(S) base URL for an S3-compatible store.
    /// Dev mode sets 127.0.0.1:0 until startup replaces it with the bound embedded S3 address.
    pub endpoint: String,
    /// Defaults to us-east-1; region used for bucket lookup and request signing.
    pub region: String,
    /// Defaults to empty and must be set before serving; bucket containing cluster manifests, logs, and snapshots.
    pub bucket: String,
    /// Defaults to empty; prepended to every durable object key and must not begin with a slash.
    pub prefix: String,
    /// Defaults to false; use path-style bucket addressing when an S3-compatible endpoint requires it.
    pub force_path_style: bool,
    /// Defaults to defaultChain; environment reads DEX_ZD_OBJECT_STORE_ACCESS_KEY_ID, DEX_ZD_OBJECT_STORE_SECRET_ACCESS_KEY, and optional DEX_ZD_OBJECT_STORE_SESSION_TOKEN.
    pub credentials_source: CredentialSource,
    /// Defaults to none; when present, hard-delete values use this separate destination instead of the primary bucket and prefix.
    pub hard_delete_store: Option<ObjectStoreLocationConfig>,
    /// Defaults to none; runtime credentials resolved from environment when credentialsSource is environment, never serialized.
    #[serde(skip)]
    pub credentials: Option<ObjectStoreCredentials>,
}

impl Default for ObjectStoreConfig {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            region: "us-east-1".to_owned(),
            bucket: String::new(),
            prefix: String::new(),
            force_path_style: false,
            credentials_source: CredentialSource::DefaultChain,
            hard_delete_store: None,
            credentials: None,
        }
    }
}

/// Selects an optional, independent S3 destination for hard-delete values.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct ObjectStoreLocationConfig {
    /// Defaults to empty; uses AWS endpoint resolution for this destination when omitted.
    pub endpoint: String,
    /// Defaults to us-east-1; region for this hard-delete destination's requests.
    pub region: String,
    /// Defaults to empty and must be set when this destination is configured.
    pub bucket: String,
    /// Defaults to empty; prefix for hard-delete objects, with no leading slash.
    pub prefix: String,
    /// Defaults to false; use path-style bucket addressing for this destination.
    pub force_path_style: bool,
    /// Defaults to defaultChain; environment reads DEX_ZD_HARD_DELETE_STORE_ACCESS_KEY_ID, DEX_ZD_HARD_DELETE_STORE_SECRET_ACCESS_KEY, and optional DEX_ZD_HARD_DELETE_STORE_SESSION_TOKEN.
    pub credentials_source: CredentialSource,
    /// Defaults to none; runtime credentials resolved from environment when credentialsSource is environment, never serialized.
    #[serde(skip)]
    pub credentials: Option<ObjectStoreCredentials>,
}

impl Default for ObjectStoreLocationConfig {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            region: "us-east-1".to_owned(),
            bucket: String::new(),
            prefix: String::new(),
            force_path_style: false,
            credentials_source: CredentialSource::DefaultChain,
            credentials: None,
        }
    }
}

/// Configures the rebuildable local RocksDB cache.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct LocalStorageConfig {
    /// Defaults to empty and must be set before serving; directory for RocksDB data that can be rebuilt from snapshots and logs.
    pub directory: PathBuf,
    /// Defaults to 256 MiB; memory reserved for the RocksDB block cache.
    pub block_cache_bytes: u64,
    /// Defaults to 256 MiB; total size of the RocksDB write-ahead log files kept in the directory.
    /// Beyond it, RocksDB flushes the column families whose memtables keep the oldest log file, so
    /// the file can be deleted. Lower values bound disk use more tightly at the cost of more, smaller
    /// flushes. Must be at least 64 MiB, the memtable size of one column family. Without this cap,
    /// a column family with few writes keeps every log file until they reach four times the memtable
    /// budget of all column families, about 3 GiB.
    pub max_wal_bytes: u64,
    /// Defaults to 64 MiB; once startup replays at least this many bytes of log records, the node
    /// compacts every RocksDB column family on a background thread while it serves. Any value is
    /// valid; 0 compacts after every startup. Compaction flushes the memtables, which releases the
    /// write-ahead log, and drops overwritten and deleted entries. Expected disk use: about the
    /// live data size once it finishes; while replay or compaction runs, up to the live data plus
    /// maxWalBytes plus level-0 files that RocksDB has not compacted yet (at most four 64 MiB files
    /// per column family). Graceful shutdown stops a running compaction.
    pub compact_after_replay_bytes: u64,
}

impl Default for LocalStorageConfig {
    fn default() -> Self {
        Self {
            directory: PathBuf::new(),
            block_cache_bytes: 256 * 1024 * 1024,
            max_wal_bytes: 256 * 1024 * 1024,
            compact_after_replay_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Selects fsync behavior for the embedded S3 emulator.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum DevDurabilityMode {
    /// Flush each acknowledged write to disk; required for crash-recovery tests.
    Strict,
    /// Default; skip fsync for faster local development, so a crash can lose acknowledged writes.
    #[default]
    Relaxed,
}

/// Holds paths and durability for the single-node development command.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct DevConfig {
    /// Defaults to empty until --data-dir is supplied; parent directory for embedded S3 and local storage.
    pub data_directory: PathBuf,
    /// Defaults to empty until --data-dir is supplied; embedded S3 files are placed under <data-dir>/s3.
    pub s3_data_directory: PathBuf,
    /// Defaults to relaxed; strict flushes acknowledged embedded S3 writes before replying.
    pub durability_mode: DevDurabilityMode,
    /// Defaults to none; optional directory that also receives dex-server-zd.log.
    pub log_directory: Option<PathBuf>,
}

impl Default for DevConfig {
    fn default() -> Self {
        Self {
            data_directory: PathBuf::new(),
            s3_data_directory: PathBuf::new(),
            durability_mode: DevDurabilityMode::Relaxed,
            log_directory: None,
        }
    }
}
