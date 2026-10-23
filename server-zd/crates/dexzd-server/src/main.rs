// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! The dex-server-zd executable: typed configuration, startup, shutdown, and the dev mode with an embedded S3 emulator.

use std::process::ExitCode;

fn main() -> ExitCode {
    match dexzd_server::run_from_command_line() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("dex-server-zd: {error}");
            ExitCode::FAILURE
        }
    }
}
