// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! The dex-server-zd executable: typed configuration, startup, shutdown, and the dev mode with an embedded S3 emulator.

mod command_line_options;
pub mod config;

pub use command_line_options::{CommandLineOptions, DevCommandOptions, ServerCommand};

use clap::Parser;

/// Errors that stop `dex-server-zd` before or while it serves requests.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    /// Configuration could not be loaded or validated.
    #[error(transparent)]
    Config(#[from] config::ConfigError),
    /// The requested command is not available in this build yet.
    #[error("the {command} command is not available in this build yet")]
    CommandNotAvailable {
        /// The command line subcommand name.
        command: &'static str,
    },
}

/// Parses the process command line and runs the selected command until it exits.
///
/// Returns `Ok(())` after a graceful shutdown, or the error that stopped the command.
pub fn run_from_command_line() -> Result<(), ServerError> {
    let options = CommandLineOptions::parse();
    match options.command {
        ServerCommand::Dev(options) => {
            let _configuration = config::Config::load_dev(&options)?;
            Err(ServerError::CommandNotAvailable { command: "dev" })
        }
    }
}
