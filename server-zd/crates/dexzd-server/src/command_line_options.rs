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
    /// Runs a Server node from a YAML configuration file against external object storage.
    Serve(ServeCommandOptions),
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

/// Options of `dex-server-zd serve`.
#[derive(Debug, Args)]
pub struct ServeCommandOptions {
    /// YAML configuration file; `config/dex-server-zd.example.yaml` lists every key and default.
    #[arg(long)]
    pub config: PathBuf,
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{CommandLineOptions, ServerCommand};

    #[test]
    fn dev_command_accepts_the_flags_dexcli_passes() {
        let options = CommandLineOptions::try_parse_from([
            "dex-server-zd",
            "dev",
            "--data-dir",
            "/tmp/zd",
            "--listen",
            "127.0.0.1:9901",
            "--log-dir",
            "/tmp/zd-logs",
            "--config",
            "/tmp/zd.yaml",
        ])
        .unwrap();
        let ServerCommand::Dev(dev) = options.command else {
            panic!("dev parses as the dev command");
        };
        assert_eq!(dev.data_dir.to_str(), Some("/tmp/zd"));
        assert_eq!(dev.listen.to_string(), "127.0.0.1:9901");
        assert_eq!(dev.log_dir.unwrap().to_str(), Some("/tmp/zd-logs"));
        assert_eq!(dev.config.unwrap().to_str(), Some("/tmp/zd.yaml"));
    }

    #[test]
    fn dev_command_defaults_the_listen_address() {
        let options =
            CommandLineOptions::try_parse_from(["dex-server-zd", "dev", "--data-dir", "/tmp/zd"])
                .unwrap();
        let ServerCommand::Dev(dev) = options.command else {
            panic!("dev parses as the dev command");
        };
        assert_eq!(dev.listen.to_string(), "127.0.0.1:8801");
        assert!(dev.log_dir.is_none());
    }

    #[test]
    fn dev_command_rejects_missing_data_dir_and_bad_listen_address() {
        assert!(CommandLineOptions::try_parse_from(["dex-server-zd", "dev"]).is_err());
        assert!(
            CommandLineOptions::try_parse_from([
                "dex-server-zd",
                "dev",
                "--data-dir",
                "/tmp/zd",
                "--listen",
                "localhost",
            ])
            .is_err()
        );
    }

    #[test]
    fn serve_command_requires_a_config_file() {
        let options = CommandLineOptions::try_parse_from([
            "dex-server-zd",
            "serve",
            "--config",
            "/etc/dex/dex-server-zd.yaml",
        ])
        .unwrap();
        let ServerCommand::Serve(serve) = options.command else {
            panic!("serve parses as the serve command");
        };
        assert_eq!(serve.config.to_str(), Some("/etc/dex/dex-server-zd.yaml"));
        assert!(CommandLineOptions::try_parse_from(["dex-server-zd", "serve"]).is_err());
        assert!(
            CommandLineOptions::try_parse_from([
                "dex-server-zd",
                "serve",
                "--config",
                "/etc/dex/dex-server-zd.yaml",
                "--listen",
                "127.0.0.1:8801",
            ])
            .is_err()
        );
    }
}
