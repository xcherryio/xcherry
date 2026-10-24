// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! The dex-server-zd executable: typed configuration, startup, shutdown, the serve mode for external object storage, and the dev mode with an embedded S3 emulator.

mod command_line_options;
mod component_configs;
pub mod config;
mod embedded_s3;
mod metrics_server;
mod object_stores;
mod runtime_hooks;
mod server_logging;
mod server_node;
mod worker_rpc_invoker;

pub use command_line_options::{
    CommandLineOptions, DevCommandOptions, ServeCommandOptions, ServerCommand,
};
pub use server_node::RunningServer;

use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use clap::Parser;
use dexzd_engine::{EngineStartError, NodeStopReason};
use tokio::sync::watch;

use crate::embedded_s3::EmbeddedS3;
use crate::server_logging::TerminationSignals;

/// Why the server could not start or stopped abnormally. Every variant exits with a failure status.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    /// Configuration could not be loaded or validated.
    #[error(transparent)]
    Config(#[from] config::ConfigError),
    /// A configuration value does not fit the component that uses it.
    #[error("configuration value {field} is out of range for this platform")]
    ConfigValueOutOfRange {
        /// CamelCase YAML field path.
        field: &'static str,
    },
    /// The configured object store credentials cannot be used.
    #[error("{field}: {detail}")]
    UnsupportedCredentials {
        /// CamelCase YAML field path.
        field: &'static str,
        /// Why the credentials cannot be used.
        detail: String,
    },
    /// The S3 client for a bucket could not be created.
    #[error("connecting to object storage bucket {bucket}: {source}")]
    ObjectStore {
        /// Bucket name.
        bucket: String,
        /// Client error.
        #[source]
        source: dexzd_object_store::ObjectStoreError,
    },
    /// The object store does not support the conditional writes the engine relies on.
    #[error("object storage bucket {bucket} failed the startup check: {source}")]
    ObjectStoreSelfCheck {
        /// Bucket name.
        bucket: String,
        /// Failed check.
        #[source]
        source: dexzd_object_store::StartupSelfCheckError,
    },
    /// The embedded S3 emulator of dev mode could not start.
    #[error("starting the embedded S3 emulator in {}: {detail}", directory.display())]
    EmbeddedS3 {
        /// Emulator data directory.
        directory: PathBuf,
        /// What failed.
        detail: String,
    },
    /// The Stream Store configuration is invalid.
    #[error("starting the Stream Store: {source}")]
    StreamStore {
        /// Store error.
        #[source]
        source: dexzd_stream_store::StreamStoreError,
    },
    /// The SearchFlows backend could not be created.
    #[error("starting the search backend: {source}")]
    SearchBackend {
        /// Search error.
        #[source]
        source: dexzd_search::SearchError,
    },
    /// The engine could not recover or start serving.
    #[error("starting the engine: {source}")]
    EngineStart {
        /// Engine error.
        #[source]
        source: EngineStartError,
    },
    /// The gRPC listener could not be bound.
    #[error("listening on {address}: {source}")]
    Listen {
        /// Configured address.
        address: SocketAddr,
        /// Socket error.
        #[source]
        source: std::io::Error,
    },
    /// The gRPC server failed.
    #[error("serving gRPC: {source}")]
    Serve {
        /// Transport error.
        #[source]
        source: tonic::transport::Error,
    },
    /// The node stopped serving, for example after losing its lease; the process must restart.
    #[error("the node stopped: {reason}")]
    NodeStopped {
        /// Why the node stopped.
        reason: NodeStopReason,
    },
    /// Graceful shutdown abandoned steps that did not finish within their budgets, or a step failed.
    ///
    /// The node lease may still be held until it expires when the engine step is listed.
    #[error("shutdown did not finish: {steps}")]
    ShutdownIncomplete {
        /// Comma-separated descriptions of the abandoned or failed steps, such as
        /// `engine exceeded its 5500 ms budget` or `engine failed: releasing node ... lease failed`.
        steps: String,
    },
    /// A server task ended unexpectedly.
    #[error("server task failed: {detail}")]
    ServingTaskFailed {
        /// What failed.
        detail: String,
    },
    /// Logging could not be set up.
    #[error("setting up logging: {detail}")]
    Logging {
        /// What failed.
        detail: String,
    },
    /// The async runtime could not start.
    #[error("starting the async runtime: {source}")]
    AsyncRuntime {
        /// Runtime error.
        #[source]
        source: std::io::Error,
    },
}

/// Parses the command line, runs the selected command until SIGINT or SIGTERM, and returns how it ended.
pub fn run_from_command_line() -> Result<(), ServerError> {
    let options = CommandLineOptions::parse();
    match options.command {
        ServerCommand::Dev(options) => {
            let configuration = config::Config::load_dev(&options)?;
            let log_directory = configuration.dev.log_directory.clone();
            serve_until_terminated(
                log_directory.as_deref(),
                move |mut cancellation| async move {
                    start_dev_server_with_config(configuration, &mut cancellation).await
                },
            )
        }
        ServerCommand::Serve(options) => {
            let configuration = config::Config::load_file(&options.config)?;
            serve_until_terminated(None, move |mut cancellation| async move {
                server_node::start_server(&configuration, None, &mut cancellation).await
            })
        }
    }
}

/// Installs logging, starts the server, and stops it gracefully on the first SIGINT or SIGTERM.
fn serve_until_terminated<StartFuture>(
    log_directory: Option<&Path>,
    start: impl FnOnce(watch::Receiver<bool>) -> StartFuture,
) -> Result<(), ServerError>
where
    StartFuture: Future<Output = Result<Option<RunningServer>, ServerError>>,
{
    server_logging::install(log_directory)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|source| ServerError::AsyncRuntime { source })?;
    runtime.block_on(async {
        let mut termination = TerminationSignals::register();
        let (cancellation_sender, cancellation) = watch::channel(false);
        let mut start = std::pin::pin!(start(cancellation));
        let server = tokio::select! {
            started = &mut start => match started? {
                Some(server) => server,
                None => return Ok(()),
            },
            () = termination.received() => {
                cancellation_sender.send_replace(true);
                match start.await? {
                    Some(server) => {
                        // Dropping a leased startup delays release until its cleanup task runs.
                        tracing::info!("termination signal during startup; stopping once startup finishes");
                        return server.shutdown().await;
                    }
                    None => {
                        tracing::info!("termination signal while waiting for the node lease; startup cancelled");
                        return Ok(());
                    }
                }
            }
        };
        let shutdown = server.shutdown_token();
        let signals = tokio::spawn(async move {
            termination.received().await;
            shutdown.cancel();
        });
        let result = server.wait().await;
        signals.abort();
        result
    })
}

/// Starts the single-node dev server with an embedded S3 emulator, serving on `--listen`.
///
/// The caller installs logging.
pub async fn start_dev_server(options: &DevCommandOptions) -> Result<RunningServer, ServerError> {
    let (_cancellation_sender, mut cancellation) = watch::channel(false);
    let started =
        start_dev_server_with_config(config::Config::load_dev(options)?, &mut cancellation).await?;
    Ok(started.expect("dev startup without cancellation must return a server"))
}

async fn start_dev_server_with_config(
    mut configuration: config::Config,
    cancellation: &mut watch::Receiver<bool>,
) -> Result<Option<RunningServer>, ServerError> {
    let embedded_s3 =
        EmbeddedS3::start(&configuration.dev, &configuration.object_store.bucket).await?;
    configuration.object_store.endpoint = embedded_s3.endpoint();
    server_node::start_server(&configuration, Some(embedded_s3), cancellation).await
}
