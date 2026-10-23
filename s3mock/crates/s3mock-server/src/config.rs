// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Command-line and environment configuration of the s3mock server.

use std::net::SocketAddr;
use std::path::PathBuf;

use s3mock::Durability;

/// Configuration of one s3mock server process; every flag also reads an `S3MOCK_*` environment variable.
#[derive(Clone, Debug, clap::Parser)]
#[command(
    name = "s3mock",
    about = "S3-compatible object store on local disk for Dex development"
)]
pub struct ServerConfig {
    /// TCP address the HTTP endpoint binds to. Default `127.0.0.1:9000`. Clients use path-style URLs
    /// against this address, such as `http://127.0.0.1:9000/<bucket>/<key>`; port 0 picks a free port.
    #[arg(long, env = "S3MOCK_LISTEN_ADDRESS", default_value = "127.0.0.1:9000")]
    pub listen_address: SocketAddr,
    /// Directory that holds the metadata database and the data files. Default `s3mock-data` in the
    /// working directory; created when missing. Only one process may use a directory at a time.
    #[arg(long, env = "S3MOCK_DATA_DIRECTORY", default_value = "s3mock-data")]
    pub data_directory: PathBuf,
    /// Write durability. Default `strict`: every acknowledged write is flushed with fsync and survives a
    /// crash. `relaxed` skips fsync for speed; a crash may lose acknowledged writes, so never use it for
    /// crash-recovery tests.
    #[arg(long, env = "S3MOCK_DURABILITY", value_enum, default_value_t = DurabilityFlag::Strict)]
    pub durability: DurabilityFlag,
    /// Default 0; deterministic seed for probabilistic fault selection. Rule replacement restarts its sequence.
    #[arg(long, env = "S3MOCK_FAULT_SEED", default_value_t = 0)]
    pub fault_seed: u64,
    /// Default 1; slash-separated key components grouped together in prefix counters. Zero groups all keys.
    #[arg(long, env = "S3MOCK_COUNTER_PREFIX_DEPTH", default_value_t = 1)]
    pub counter_prefix_depth: usize,
    /// Default 1024; maximum recent requests retained in memory. Zero disables the request ring buffer.
    #[arg(long, env = "S3MOCK_REQUEST_LOG_CAPACITY", default_value_t = 1024)]
    pub request_log_capacity: usize,
    /// Default md5; opaque gives every stored version a stable ETag unrelated to its content hash.
    #[arg(long, env = "S3MOCK_ETAG_MODE", value_enum, default_value_t = EtagModeFlag::Md5)]
    pub etag_mode: EtagModeFlag,
    /// Default off; strict validates SigV4 headers, payloads, signed chunks, and signed trailers before dispatch.
    #[arg(long, env = "S3MOCK_SIGNATURE_VERIFICATION", value_enum, default_value_t = SignatureVerification::Off)]
    pub signature_verification: SignatureVerification,
    /// Default s3mock; accepted access key identifier in strict mode. Admin endpoints do not require authentication.
    #[arg(long, env = "S3MOCK_ACCESS_KEY_ID", default_value = "s3mock")]
    pub access_key_id: String,
    /// Default s3mock; HMAC secret used in strict mode. Use test credentials only; never production credentials.
    #[arg(long, env = "S3MOCK_SECRET_ACCESS_KEY", default_value = "s3mock")]
    pub secret_access_key: String,
}

/// Command-line spelling of [`Durability`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum DurabilityFlag {
    Strict,
    Relaxed,
}

impl From<DurabilityFlag> for Durability {
    fn from(flag: DurabilityFlag) -> Durability {
        match flag {
            DurabilityFlag::Strict => Durability::Strict,
            DurabilityFlag::Relaxed => Durability::Relaxed,
        }
    }
}

/// Signature verification policy for S3 requests.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum SignatureVerification {
    Off,
    Strict,
}

/// Command-line ETag representation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum EtagModeFlag {
    Md5,
    Opaque,
}

impl From<EtagModeFlag> for s3mock::EtagMode {
    fn from(mode: EtagModeFlag) -> Self {
        match mode {
            EtagModeFlag::Md5 => Self::Md5,
            EtagModeFlag::Opaque => Self::Opaque,
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_address: "127.0.0.1:9000"
                .parse()
                .expect("the default listener is valid"),
            data_directory: PathBuf::from("s3mock-data"),
            durability: DurabilityFlag::Strict,
            fault_seed: 0,
            counter_prefix_depth: 1,
            request_log_capacity: 1024,
            etag_mode: EtagModeFlag::Md5,
            signature_verification: SignatureVerification::Off,
            access_key_id: "s3mock".into(),
            secret_access_key: "s3mock".into(),
        }
    }
}
