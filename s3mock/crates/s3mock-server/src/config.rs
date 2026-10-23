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
