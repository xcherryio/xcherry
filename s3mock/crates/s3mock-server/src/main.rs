// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! S3-compatible HTTP server that serves an s3mock store over path-style URLs.

use std::sync::Arc;

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
    };
    let store = Arc::new(tokio::task::spawn_blocking(move || Store::open(&store_config)).await??);
    let listener = tokio::net::TcpListener::bind(config.listen_address).await?;
    let server = s3mock_server::start(Arc::clone(&store), listener)?;
    tracing::info!(
        address = %server.local_address(),
        data_directory = %config.data_directory.display(),
        durability = ?config.durability,
        "s3mock is serving"
    );
    let orphan_collection = tokio::task::spawn_blocking(move || store.collect_orphan_data_files());
    tokio::signal::ctrl_c().await?;
    tracing::info!("shutting down");
    server.shutdown().await;
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
