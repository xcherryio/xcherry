// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::fs::{self, OpenOptions};
use std::path::Path;
use std::sync::Arc;

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

use crate::ServerError;

/// Log file name inside `--log-dir`; `dexcli dev` reads this name.
const LOG_FILE_NAME: &str = "dex-server-zd.log";

/// Installs the global subscriber: standard error, plus `<log_directory>/dex-server-zd.log`
/// when a log directory is set. The `RUST_LOG` filter applies; the default level is info.
pub(crate) fn install(log_directory: Option<&Path>) -> Result<(), ServerError> {
    let filter = || EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let stderr_layer = tracing_subscriber::fmt::layer()
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

/// Resolves on the first SIGINT or SIGTERM.
pub(crate) async fn wait_for_termination_signal() {
    let mut terminate =
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(terminate) => terminate,
            Err(error) => {
                tracing::warn!(%error, "cannot listen for SIGTERM; only SIGINT stops the server");
                if let Err(error) = tokio::signal::ctrl_c().await {
                    tracing::error!(%error, "cannot listen for SIGINT");
                    std::future::pending::<()>().await;
                }
                return;
            }
        };
    tokio::select! {
        interrupted = tokio::signal::ctrl_c() => {
            if let Err(error) = interrupted {
                tracing::error!(%error, "cannot listen for SIGINT");
                terminate.recv().await;
            }
        }
        _ = terminate.recv() => {}
    }
    tracing::info!("termination signal received");
}
