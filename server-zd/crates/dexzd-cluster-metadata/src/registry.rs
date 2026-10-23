// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use bytes::Bytes;
use dexzd_clock::WallClock;
use dexzd_object_store::{ObjectStore, ObjectStoreErrorKind, WriteCondition, WriteOutcome};
use dexzd_protocol::dexzd::v1::{AttributeIndexRegistration, AttributeIndexRegistry};
use prost::Message;

use crate::MetadataError;

const REGISTRY_KEY: &str = "registry/attribute-indexes";

/// One attribute index declaration supplied by an application.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttributeIndexDeclaration {
    /// Index key within a tenant.
    pub index_key: String,
    /// Numeric value of the public Dex IndexType enum.
    pub index_type: i32,
}

/// Reads the current attribute index registry.
pub async fn read_registry(
    store: &dyn ObjectStore,
) -> Result<AttributeIndexRegistry, MetadataError> {
    match store.get(REGISTRY_KEY).await? {
        Some(content) => {
            AttributeIndexRegistry::decode(content.bytes).map_err(|source| MetadataError::Decode {
                key: REGISTRY_KEY,
                source,
            })
        }
        None => Ok(AttributeIndexRegistry::default()),
    }
}

/// Registers new keys and rejects a type change for an existing tenant key.
pub async fn register(
    store: &dyn ObjectStore,
    clock: &dyn WallClock,
    tenant_id: u64,
    declarations: &[AttributeIndexDeclaration],
) -> Result<AttributeIndexRegistry, MetadataError> {
    loop {
        let current_object = store.get(REGISTRY_KEY).await?;
        let current = match &current_object {
            Some(content) => {
                AttributeIndexRegistry::decode(content.bytes.clone()).map_err(|source| {
                    MetadataError::Decode {
                        key: REGISTRY_KEY,
                        source,
                    }
                })?
            }
            None => AttributeIndexRegistry::default(),
        };
        let mut proposed = current.clone();
        for declaration in declarations {
            if let Some(existing) = proposed.registrations.iter().find(|existing| {
                existing.tenant_id == tenant_id && existing.index_key == declaration.index_key
            }) {
                if existing.index_type != declaration.index_type {
                    return Err(MetadataError::AttributeIndexConflict {
                        tenant_id,
                        index_key: declaration.index_key.clone(),
                        existing_type: existing.index_type,
                        requested_type: declaration.index_type,
                    });
                }
                continue;
            }
            proposed.registrations.push(AttributeIndexRegistration {
                tenant_id,
                index_key: declaration.index_key.clone(),
                index_type: declaration.index_type,
                registered_at_unix_micros: clock.now_unix_micros(),
            });
        }
        if proposed == current {
            return Ok(current);
        }
        proposed.version = current
            .version
            .checked_add(1)
            .ok_or(MetadataError::Overflow {
                field: "attribute index registry version",
            })?;
        let condition = current_object.map_or(WriteCondition::CreateOnly, |content| {
            WriteCondition::ReplaceIfEtagMatches { etag: content.etag }
        });
        let result = store
            .put(
                REGISTRY_KEY,
                Bytes::from(proposed.encode_to_vec()),
                condition,
            )
            .await;
        match result {
            Ok(WriteOutcome::Written { .. }) => return Ok(proposed),
            Ok(WriteOutcome::PreconditionFailed) => {
                if read_registry(store).await? == proposed {
                    return Ok(proposed);
                }
            }
            Err(error) if error.kind == ObjectStoreErrorKind::OutcomeUnknown => {
                if read_registry(store).await? == proposed {
                    return Ok(proposed);
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
}
