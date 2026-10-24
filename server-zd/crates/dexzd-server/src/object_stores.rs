// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;

use dexzd_metrics::NodeMetrics;
use dexzd_object_store::{
    InstrumentedObjectStore, ObjectStore, S3Credentials, S3ObjectStore, S3ObjectStoreConfig,
    startup_self_check,
};

use crate::ServerError;
use crate::config::{
    CredentialSource, ObjectStoreConfig, ObjectStoreCredentials, ObjectStoreLocationConfig,
};

/// The durable stores: logs, metadata, and snapshots, plus the hard-delete value destination.
pub(crate) struct ObjectStores {
    pub(crate) primary: Arc<dyn ObjectStore>,
    pub(crate) hard_delete: Arc<dyn ObjectStore>,
}

impl ObjectStores {
    /// Connects to the configured stores and verifies that each supports conditional writes.
    pub(crate) async fn connect(
        config: &ObjectStoreConfig,
        multipart_threshold_bytes: u64,
        metrics: &NodeMetrics,
    ) -> Result<Self, ServerError> {
        let primary_config = S3ObjectStoreConfig {
            endpoint: non_empty(&config.endpoint),
            key_prefix: config.prefix.clone(),
            force_path_style: config.force_path_style,
            credentials: s3_credentials(
                "objectStore.credentialsSource",
                config.credentials_source,
                config.credentials.as_ref(),
            )?,
            ..S3ObjectStoreConfig::new(config.region.clone(), config.bucket.clone())
        };
        let primary = connect_store(primary_config, multipart_threshold_bytes, metrics).await?;
        let hard_delete = match &config.hard_delete_store {
            Some(location) => {
                connect_store(
                    location_config(location)?,
                    multipart_threshold_bytes,
                    metrics,
                )
                .await?
            }
            None => primary.clone(),
        };
        Ok(Self {
            primary,
            hard_delete,
        })
    }
}

fn location_config(
    location: &ObjectStoreLocationConfig,
) -> Result<S3ObjectStoreConfig, ServerError> {
    Ok(S3ObjectStoreConfig {
        endpoint: non_empty(&location.endpoint),
        key_prefix: location.prefix.clone(),
        force_path_style: location.force_path_style,
        credentials: s3_credentials(
            "objectStore.hardDeleteStore.credentialsSource",
            location.credentials_source,
            location.credentials.as_ref(),
        )?,
        ..S3ObjectStoreConfig::new(location.region.clone(), location.bucket.clone())
    })
}

async fn connect_store(
    mut config: S3ObjectStoreConfig,
    multipart_threshold_bytes: u64,
    metrics: &NodeMetrics,
) -> Result<Arc<dyn ObjectStore>, ServerError> {
    config.multipart_threshold = usize::try_from(multipart_threshold_bytes).map_err(|_| {
        ServerError::ConfigValueOutOfRange {
            field: "commit.multipartThresholdBytes",
        }
    })?;
    let bucket = config.bucket.clone();
    let store = S3ObjectStore::new(config)
        .await
        .map_err(|source| ServerError::ObjectStore {
            bucket: bucket.clone(),
            source,
        })?;
    let instrumented = InstrumentedObjectStore::new(Arc::new(store), metrics.clone());
    startup_self_check(&instrumented)
        .await
        .map_err(|source| ServerError::ObjectStoreSelfCheck { bucket, source })?;
    Ok(Arc::new(instrumented))
}

fn s3_credentials(
    field: &'static str,
    source: CredentialSource,
    credentials: Option<&ObjectStoreCredentials>,
) -> Result<S3Credentials, ServerError> {
    match (source, credentials) {
        (CredentialSource::DefaultChain, _) => Ok(S3Credentials::DefaultChain),
        (CredentialSource::Environment, Some(credentials)) => Ok(S3Credentials::Static {
            access_key_id: credentials.access_key_id.clone(),
            secret_access_key: credentials.secret_access_key.clone(),
            session_token: credentials.session_token.clone(),
        }),
        (CredentialSource::Environment, None) => Err(ServerError::UnsupportedCredentials {
            field,
            detail: "environment credentials were not resolved".to_owned(),
        }),
        (CredentialSource::Anonymous, _) => Ok(S3Credentials::Anonymous),
    }
}

fn non_empty(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_owned())
}
