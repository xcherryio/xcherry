// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::fs::{self, OpenOptions};
use std::io::IsTerminal;
use std::path::Path;
use std::sync::Arc;

use tokio::signal::unix::{Signal, SignalKind, signal};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

use crate::ServerError;

/// Log file name inside `--log-dir`; `dexcli dev` reads this name.
const LOG_FILE_NAME: &str = "dex-server-zd.log";

/// Logs to standard error and, when set, `<log_directory>/dex-server-zd.log`.
///
/// `RUST_LOG` filters records; the default level is info.
pub(crate) fn install(log_directory: Option<&Path>) -> Result<(), ServerError> {
    let filter = || EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let stderr_layer = tracing_subscriber::fmt::layer()
        .with_ansi(std::io::stderr().is_terminal())
        .with_writer(std::io::stderr)
        .with_filter(filter());
    let file_layer = match log_directory {
        Some(directory) => {
            fs::create_dir_all(directory).map_err(|error| ServerError::Logging {
                detail: format!("creating {}: {error}", directory.display()),
            })?;
            let path = directory.join(LOG_FILE_NAME);
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .map_err(|error| ServerError::Logging {
                    detail: format!("opening {}: {error}", path.display()),
                })?;
            Some(
                tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .with_writer(Arc::new(file))
                    .with_filter(filter()),
            )
        }
        None => None,
    };
    tracing_subscriber::registry()
        .with(stderr_layer)
        .with(file_layer)
        .try_init()
        .map_err(|error| ServerError::Logging {
            detail: error.to_string(),
        })
}

/// SIGINT and SIGTERM listeners; registering them before startup closes the window where a signal kills the process.
pub(crate) struct TerminationSignals {
    interrupt: Option<Signal>,
    terminate: Option<Signal>,
}

impl TerminationSignals {
    /// Replaces the default SIGINT and SIGTERM actions; call inside the async runtime.
    pub(crate) fn register() -> Self {
        let interrupt = match signal(SignalKind::interrupt()) {
            Ok(interrupt) => Some(interrupt),
            Err(error) => {
                tracing::error!(%error, "cannot listen for SIGINT");
                None
            }
        };
        let terminate = match signal(SignalKind::terminate()) {
            Ok(terminate) => Some(terminate),
            Err(error) => {
                tracing::warn!(%error, "cannot listen for SIGTERM; only SIGINT stops the server");
                None
            }
        };
        Self {
            interrupt,
            terminate,
        }
    }

    /// Resolves on the first SIGINT or SIGTERM.
    pub(crate) async fn received(&mut self) {
        let Self {
            interrupt,
            terminate,
        } = self;
        tokio::select! {
            () = next_signal(interrupt.as_mut()) => {}
            () = next_signal(terminate.as_mut()) => {}
        }
        tracing::info!("termination signal received");
    }
}

/// Waits for the next delivery; a listener that could not be registered never resolves.
async fn next_signal(listener: Option<&mut Signal>) {
    match listener {
        Some(listener) => {
            // None means the signal driver stopped, which only happens during runtime shutdown.
            if listener.recv().await.is_none() {
                tracing::debug!("signal listener closed");
            }
        }
        None => std::future::pending().await,
    }
}
