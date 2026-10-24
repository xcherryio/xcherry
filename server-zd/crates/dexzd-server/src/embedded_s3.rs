// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::net::SocketAddr;
use std::sync::Arc;

use s3mock::{Durability, EtagMode, Store, StoreConfig, StoreError};
use s3mock_server::{DurabilityFlag, ServerConfig, ServerHandle};
use tokio::net::TcpListener;

use crate::ServerError;
use crate::config::{DevConfig, DevDurabilityMode};

/// The in-process S3 emulator of `dex-server-zd dev`.
///
/// The engine uses the production S3 client over HTTP to reach it.
pub(crate) struct EmbeddedS3 {
    server: ServerHandle,
}

impl EmbeddedS3 {
    /// Opens the emulator's data under `dev.s3DataDirectory`, creates `bucket`, and serves it on
    /// a free loopback port.
    pub(crate) async fn start(config: &DevConfig, bucket: &str) -> Result<Self, ServerError> {
        let durability = match config.durability_mode {
            DevDurabilityMode::Strict => Durability::Strict,
            DevDurabilityMode::Relaxed => Durability::Relaxed,
        };
        let store_config = StoreConfig {
            root_directory: config.s3_data_directory.clone(),
            durability,
            etag_mode: EtagMode::Md5,
        };
        let bucket = bucket.to_owned();
        let directory = config.s3_data_directory.clone();
        let store = tokio::task::spawn_blocking(move || open_store(&store_config, &bucket))
            .await
            .map_err(|source| ServerError::EmbeddedS3 {
                directory: directory.clone(),
                detail: source.to_string(),
            })?
            .map_err(|source| ServerError::EmbeddedS3 {
                directory: directory.clone(),
                detail: source.to_string(),
            })?;
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .map_err(|source| ServerError::EmbeddedS3 {
                directory: directory.clone(),
                detail: source.to_string(),
            })?;
        let server_config = ServerConfig {
            data_directory: directory.clone(),
            durability: match durability {
                Durability::Strict => DurabilityFlag::Strict,
                Durability::Relaxed => DurabilityFlag::Relaxed,
            },
            ..ServerConfig::default()
        };
        let server = s3mock_server::start_with_config(Arc::new(store), listener, server_config)
            .map_err(|source| ServerError::EmbeddedS3 {
                directory: directory.clone(),
                detail: source.to_string(),
            })?;
        tracing::info!(address = %server.local_address(), directory = %directory.display(), "embedded S3 emulator serving");
        Ok(Self { server })
    }

    /// Returns the HTTP endpoint of the emulator.
    pub(crate) fn endpoint(&self) -> String {
        format!("http://{}", self.server.local_address())
    }

    /// Stops accepting requests and waits for in-flight requests.
    pub(crate) async fn shutdown(self) {
        self.server.shutdown().await;
    }
}

fn open_store(config: &StoreConfig, bucket: &str) -> Result<Store, StoreError> {
    let store = Store::open(config)?;
    match store.create_bucket(bucket, false) {
        Ok(_) | Err(StoreError::BucketAlreadyOwnedByYou(_)) => Ok(store),
        Err(error) => Err(error),
    }
}
