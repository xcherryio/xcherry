// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;

use dexzd_clock::WallClock;
use dexzd_cluster_metadata::{
    AttributeIndexDeclaration, MetadataError, read_attribute_index_registry,
    register_attribute_indexes,
};
use dexzd_keyspace::TenantId;
use dexzd_object_store::ObjectStore;
use dexzd_protocol::dex::{IndexType, SyncAttributeIndexRequest};
use dexzd_protocol::dexzd::v1::AttributeIndexRegistry;
use dexzd_visibility_query::FieldRegistry;
use tokio::sync::Mutex;

use crate::FlowOperationError;

/// Registers Attribute index declarations in the cluster registry and serves them to search.
///
/// The registry object is cached after the first read and replaced after each registration.
pub struct AttributeIndexes {
    object_store: Arc<dyn ObjectStore>,
    clock: Arc<dyn WallClock>,
    cached_registry: Mutex<Option<AttributeIndexRegistry>>,
}

impl AttributeIndexes {
    /// Creates a registry client that reads and writes `object_store`.
    pub fn new(object_store: Arc<dyn ObjectStore>, clock: Arc<dyn WallClock>) -> Self {
        Self {
            object_store,
            clock,
            cached_registry: Mutex::new(None),
        }
    }

    /// Validates and registers the declarations of `request` for `tenant_id`.
    ///
    /// Returns `InvalidArgument` for an empty name or unknown type, `FailedPrecondition` when a
    /// name is registered with another type, and `Unavailable` when object storage fails.
    pub async fn sync_attribute_indexes(
        &self,
        tenant_id: TenantId,
        request: &SyncAttributeIndexRequest,
    ) -> Result<(), FlowOperationError> {
        let mut declarations = Vec::with_capacity(request.attribute_indexes.len());
        for (index_key, index_type) in &request.attribute_indexes {
            if index_key.trim().is_empty() {
                return Err(FlowOperationError::invalid_argument(
                    "attribute index name must not be empty",
                ));
            }
            let is_known_type = *index_type > IndexType::Unspecified as i32
                && *index_type <= IndexType::Datetime as i32;
            if !is_known_type {
                return Err(FlowOperationError::invalid_argument(format!(
                    "attribute index {index_key:?} has invalid type {index_type}"
                )));
            }
            declarations.push(AttributeIndexDeclaration {
                index_key: index_key.clone(),
                index_type: *index_type,
            });
        }
        let mut cached_registry = self.cached_registry.lock().await;
        let registry = register_attribute_indexes(
            self.object_store.as_ref(),
            self.clock.as_ref(),
            tenant_id.0,
            &declarations,
        )
        .await
        .map_err(registry_error)?;
        *cached_registry = Some(registry);
        Ok(())
    }

    /// Returns the registered application fields of `tenant_id` for query type checking.
    ///
    /// Returns `Unavailable` when the registry cannot be read.
    pub async fn field_registry(
        &self,
        tenant_id: TenantId,
    ) -> Result<FieldRegistry, FlowOperationError> {
        let mut cached_registry = self.cached_registry.lock().await;
        if cached_registry.is_none() {
            let registry = read_attribute_index_registry(self.object_store.as_ref())
                .await
                .map_err(registry_error)?;
            *cached_registry = Some(registry);
        }
        let mut fields = FieldRegistry::new();
        for registration in cached_registry
            .iter()
            .flat_map(|registry| &registry.registrations)
            .filter(|registration| registration.tenant_id == tenant_id.0)
        {
            let index_type = IndexType::try_from(registration.index_type).map_err(|_| {
                FlowOperationError::Internal {
                    detail: format!(
                        "attribute index {:?} has stored type {}",
                        registration.index_key, registration.index_type
                    ),
                }
            })?;
            fields.insert(registration.index_key.clone(), index_type);
        }
        Ok(fields)
    }
}

fn registry_error(error: MetadataError) -> FlowOperationError {
    match error {
        MetadataError::AttributeIndexConflict { .. } => {
            FlowOperationError::failed_precondition(error.to_string())
        }
        MetadataError::Store(_) => FlowOperationError::Unavailable {
            detail: error.to_string(),
        },
        other => FlowOperationError::Internal {
            detail: other.to_string(),
        },
    }
}
