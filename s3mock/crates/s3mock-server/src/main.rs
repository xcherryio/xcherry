// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! S3-compatible HTTP server that serves an s3mock store over path-style URLs.

use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use s3mock::{Store, StoreConfig};
use s3mock_server::ServerConfig;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let config = ServerConfig::parse();
    let store_config = StoreConfig {
        root_directory: config.data_directory.clone(),
        durability: config.durability.into(),
        etag_mode: config.etag_mode.into(),
    };
    let store = Arc::new(tokio::task::spawn_blocking(move || Store::open(&store_config)).await??);
    let listener = tokio::net::TcpListener::bind(config.listen_address).await?;
    let server = s3mock_server::start_with_config(Arc::clone(&store), listener, config.clone())?;
    tracing::info!(
        address = %server.local_address(),
        data_directory = %config.data_directory.display(),
        durability = ?config.durability,
        signature_verification = ?config.signature_verification,
        "s3mock is serving"
    );
    let lifecycle_scanner = if config.lifecycle_scan_interval_seconds > 0 {
        let scanner = tokio::spawn(scan_lifecycle(
            Arc::clone(&store),
            Duration::from_secs(config.lifecycle_scan_interval_seconds),
        ));
        Some(scanner)
    } else {
        None
    };
    let orphan_collection = tokio::task::spawn_blocking(move || store.collect_orphan_data_files());
    tokio::signal::ctrl_c().await?;
    tracing::info!("shutting down");
    server.shutdown().await;
    if let Some(scanner) = lifecycle_scanner {
        scanner.abort();
    }
    match orphan_collection.await? {
        Ok(collection) => tracing::info!(
            examined_files = collection.examined_files,
            removed_files = collection.removed_files,
            "orphan data file collection finished"
        ),
        Err(error) => tracing::warn!(%error, "orphan data file collection failed"),
    }
    Ok(())
}

/// Applies lifecycle rules every `interval` until the task is aborted at shutdown.
async fn scan_lifecycle(store: Arc<Store>, interval: Duration) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let store = Arc::clone(&store);
        match tokio::task::spawn_blocking(move || store.run_lifecycle()).await {
            Ok(Ok(run)) => tracing::debug!(
                expired_current_versions = run.expired_current_versions,
                removed_delete_markers = run.removed_delete_markers,
                expired_noncurrent_versions = run.expired_noncurrent_versions,
                aborted_uploads = run.aborted_uploads,
                "lifecycle pass finished"
            ),
            Ok(Err(error)) => tracing::warn!(%error, "lifecycle pass failed"),
            Err(error) => tracing::error!(%error, "lifecycle pass panicked"),
        }
    }
}
