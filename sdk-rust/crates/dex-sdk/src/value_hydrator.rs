// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::HashMap;
use std::str;

use dex_blob_cache::BlobCache;
use dex_protocol::dex::flow_service_client::FlowServiceClient;
use dex_protocol::dex::{EncodedObject, LoadBlobRequestEntry, LoadBlobsRequest, Value, value};
use prost::Message;

use crate::credential::CredentialChannel;

use crate::sdk_error::FlowTargetRequirement;
use crate::{SdkError, SdkResult};

#[derive(Clone)]
pub(crate) struct ValueHydrator {
    service: FlowServiceClient<CredentialChannel>,
    cache: std::sync::Arc<BlobCache>,
}

impl ValueHydrator {
    pub(crate) fn new(
        service: FlowServiceClient<CredentialChannel>,
        cache: std::sync::Arc<BlobCache>,
    ) -> Self {
        Self { service, cache }
    }

    pub(crate) async fn hydrate(&self, flow_id: &str, value: Value) -> SdkResult<Value> {
        let mut values = self.hydrate_all(flow_id, vec![value]).await?;
        Ok(values.remove(0))
    }

    pub(crate) async fn hydrate_all(
        &self,
        flow_id: &str,
        values: Vec<Value>,
    ) -> SdkResult<Vec<Value>> {
        self.hydrate_for_flows(
            values
                .into_iter()
                .map(|value| (flow_id.to_string(), value))
                .collect(),
        )
        .await
    }

    pub(crate) async fn hydrate_for_flows(
        &self,
        flow_values: Vec<(String, Value)>,
    ) -> SdkResult<Vec<Value>> {
        let mut hydrated = flow_values
            .iter()
            .map(|(_, value)| value.clone())
            .collect::<Vec<_>>();
        let mut pending: HashMap<BlobKey, Vec<usize>> = HashMap::new();
        for (index, (flow_id, value)) in flow_values.iter().enumerate() {
            if flow_id.is_empty() {
                return Err(mapping_error("Flow ID is required"));
            }
            if let Some(key) = BlobKey::from_value(flow_id, value)? {
                pending.entry(key).or_default().push(index);
            } else {
                validate_concrete(value)?;
            }
        }

        let mut resolved = HashMap::new();
        let mut misses = Vec::new();
        for key in pending.keys() {
            match self.read_cache(key) {
                Ok(Some(value)) => {
                    resolved.insert(key.clone(), value);
                }
                Ok(None) | Err(_) => misses.push(key.clone()),
            }
        }
        if !misses.is_empty() {
            let request = LoadBlobsRequest {
                entries: misses
                    .iter()
                    .map(|key| LoadBlobRequestEntry {
                        flow_id: key.flow_id.clone(),
                        blob_value: Some(key.request_value()),
                    })
                    .collect(),
            };
            let response = self
                .service
                .clone()
                .load_blobs(request)
                .await
                .map_err(|status| {
                    SdkError::from_status(status, "load_blobs", None, FlowTargetRequirement::None)
                })?
                .into_inner();
            for key in misses {
                let value = response.values.get(&key.id).cloned().ok_or_else(|| {
                    SdkError::ValueMapping {
                        message: format!("LoadBlobs omitted blob {}", key.id),
                    }
                })?;
                key.validate_hydrated(&value)?;
                let _ = self.write_cache(&key, &value);
                resolved.insert(key, value);
            }
        }
        for (key, indexes) in pending {
            let value = resolved
                .remove(&key)
                .ok_or_else(|| SdkError::ValueMapping {
                    message: format!("blob {} was not hydrated", key.id),
                })?;
            for index in indexes {
                hydrated[index] = value.clone();
            }
        }
        Ok(hydrated)
    }

    fn read_cache(&self, key: &BlobKey) -> SdkResult<Option<Value>> {
        let payload = self.cache.get(&key.cache_key()).map_err(cache_error)?;
        let Some(payload) = payload else {
            return Ok(None);
        };
        let value = if key.object {
            Value {
                kind: Some(value::Kind::ObjValue(
                    EncodedObject::decode(payload.as_slice()).map_err(mapping_error)?,
                )),
            }
        } else {
            Value {
                kind: Some(value::Kind::StringValue(
                    str::from_utf8(&payload).map_err(mapping_error)?.to_string(),
                )),
            }
        };
        validate_concrete(&value)?;
        Ok(Some(value))
    }

    fn write_cache(&self, key: &BlobKey, value: &Value) -> SdkResult<()> {
        let payload = match value.kind.as_ref() {
            Some(value::Kind::ObjValue(object)) if key.object => object.encode_to_vec(),
            Some(value::Kind::StringValue(text)) if !key.object => text.as_bytes().to_vec(),
            _ => return Err(mapping_error("hydrated blob has the wrong kind")),
        };
        self.cache
            .put(&key.cache_key(), &payload)
            .map_err(cache_error)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct BlobKey {
    flow_id: String,
    id: String,
    object: bool,
}

impl BlobKey {
    fn from_value(flow_id: &str, value: &Value) -> SdkResult<Option<Self>> {
        let key = match value.kind.as_ref() {
            Some(value::Kind::InternalBlobIdForStringValue(id)) => Self {
                flow_id: flow_id.to_string(),
                id: require_blob_id(id)?,
                object: false,
            },
            Some(value::Kind::InternalBlobIdForObjValue(id)) => Self {
                flow_id: flow_id.to_string(),
                id: require_blob_id(id)?,
                object: true,
            },
            Some(_) => return Ok(None),
            None => return Err(mapping_error("Value has no concrete kind")),
        };
        Ok(Some(key))
    }

    fn request_value(&self) -> Value {
        Value {
            kind: Some(if self.object {
                value::Kind::InternalBlobIdForObjValue(self.id.clone())
            } else {
                value::Kind::InternalBlobIdForStringValue(self.id.clone())
            }),
        }
    }

    fn cache_key(&self) -> String {
        format!("{}:{}{}", self.flow_id.len(), self.flow_id, self.id)
    }

    fn validate_hydrated(&self, value: &Value) -> SdkResult<()> {
        match value.kind.as_ref() {
            Some(value::Kind::ObjValue(_)) if self.object => validate_concrete(value),
            Some(value::Kind::StringValue(_)) if !self.object => validate_concrete(value),
            _ => Err(mapping_error("blob hydrated to the wrong Value kind")),
        }
    }
}

fn validate_concrete(value: &Value) -> SdkResult<()> {
    match value.kind.as_ref() {
        Some(value::Kind::StringValue(_))
        | Some(value::Kind::IntValue(_))
        | Some(value::Kind::BoolValue(_)) => Ok(()),
        Some(value::Kind::DoubleValue(number)) if number.is_finite() => Ok(()),
        Some(value::Kind::DoubleValue(_)) => {
            Err(mapping_error("non-finite numbers are unsupported"))
        }
        Some(value::Kind::ObjValue(object))
            if object.encoding == "json" || object.encoding == "raw" =>
        {
            Ok(())
        }
        Some(value::Kind::ObjValue(object)) => Err(mapping_error(format!(
            "unsupported object encoding {}",
            object.encoding
        ))),
        Some(value::Kind::InternalBlobIdForStringValue(_))
        | Some(value::Kind::InternalBlobIdForObjValue(_)) => {
            Err(mapping_error("blob-backed Value was not hydrated"))
        }
        Some(value::Kind::NullValue(_)) => Ok(()),
        None => Err(mapping_error("Value has no concrete kind")),
    }
}

fn require_blob_id(id: &str) -> SdkResult<String> {
    if id.is_empty() {
        Err(mapping_error("blob ID is required"))
    } else {
        Ok(id.to_string())
    }
}

fn cache_error(error: impl std::fmt::Display) -> SdkError {
    mapping_error(format!("blob cache: {error}"))
}

fn mapping_error(error: impl std::fmt::Display) -> SdkError {
    SdkError::ValueMapping {
        message: error.to_string(),
    }
}
