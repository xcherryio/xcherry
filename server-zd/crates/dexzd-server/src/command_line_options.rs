// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

/// Command line of `dex-server-zd`.
#[derive(Debug, Parser)]
#[command(
    name = "dex-server-zd",
    version,
    about = "Dex Server with object storage as its only dependency"
)]
pub struct CommandLineOptions {
    /// The command to run.
    #[command(subcommand)]
    pub command: ServerCommand,
}

/// Commands of `dex-server-zd`.
#[derive(Debug, Subcommand)]
pub enum ServerCommand {
    /// Runs a single-node development Server with an embedded S3 emulator.
    Dev(DevCommandOptions),
}

/// Options of `dex-server-zd dev`. `dexcli dev -engine zd` depends on these exact flags.
#[derive(Debug, Args)]
pub struct DevCommandOptions {
    /// Directory for all local state: the embedded S3 emulator data and the local state store.
    #[arg(long)]
    pub data_dir: PathBuf,
    /// Plaintext gRPC address that serves FlowService and WorkerTaskService.
    #[arg(long, default_value = "127.0.0.1:8801")]
    pub listen: SocketAddr,
    /// Optional directory that also receives `dex-server-zd.log`.
    #[arg(long)]
    pub log_dir: Option<PathBuf>,
    /// Optional YAML file with configuration overrides.
    #[arg(long)]
    pub config: Option<PathBuf>,
}
