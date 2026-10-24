// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Hard-delete Attribute objects: uploads before a command is proposed, reads at dispatch,
//! deletion after a committed overwrite, and cleanup of uploads whose command never committed.

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use dexzd_apply::FlowStateReader;
use dexzd_clock::WallClock;
use dexzd_keyspace::{
    ColumnFamily, FlowHash, PartitionHasher, PartitionId, RunId, RunTable, TenantId,
    run_table_prefix,
};
use dexzd_object_store::{ObjectStore, WriteCondition, WriteOutcome};
use dexzd_protocol::dex::{AttributeWrite, Value, value};
use dexzd_protocol::dexzd::v1::{
    HardDeleteObjectRecord, HardDeleteObjectReference, PendingHardDeleteUpload,
};
use dexzd_storage::{LocalStore, WriteBatch};
use futures::future::BoxFuture;
use prost::Message;

use crate::effects::EffectError;
use crate::effects::state_reads::{RunLocation, decode_row, scan_all};
use crate::engine_id_source::EngineIdSource;
use crate::runtime::{
    FlowAddress, HardDeleteReadError, HardDeleteUploadError, HardDeleteUploadOutcome,
    HardDeleteUploads as RuntimeHardDeleteUploads, HardDeleteValueReader, HardDeleteValueUploader,
};

/// Key prefix of every hard-delete object, relative to the hard-delete object store.
pub const HARD_DELETE_OBJECT_KEY_PREFIX: &str = "hard-delete/";

/// Largest number of keys in one object storage batch delete.
const DELETE_BATCH_KEYS: usize = 1000;

/// The run that owns hard-delete objects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HardDeleteObjectOwner {
    /// Tenant of the run's Flow.
    pub tenant_id: TenantId,
    /// Keyed hash of the run's Flow.
    pub flow_hash: FlowHash,
    /// The run.
    pub run_id: RunId,
}

impl HardDeleteObjectOwner {
    /// Returns `hard-delete/<tenant:016x>/<flow_hash:016x>/<run_id>/`, the prefix of every object
    /// of the run.
    pub fn object_key_prefix(&self) -> String {
        format!(
            "{HARD_DELETE_OBJECT_KEY_PREFIX}{:016x}/{:016x}/{}/",
            self.tenant_id.0, self.flow_hash.0, self.run_id
        )
    }

    /// Returns the object key of `object_id`.
    pub fn object_key(&self, object_id: &str) -> String {
        format!("{}{object_id}", self.object_key_prefix())
    }

    /// Parses an object key into its owner and object ID; `None` when it is not a hard-delete
    /// object key.
    pub fn parse_object_key(key: &str) -> Option<(Self, String)> {
        let rest = key.strip_prefix(HARD_DELETE_OBJECT_KEY_PREFIX)?;
        let mut parts = rest.splitn(4, '/');
        let tenant_id = u64::from_str_radix(parts.next()?, 16).ok()?;
        let flow_hash = u64::from_str_radix(parts.next()?, 16).ok()?;
        let run_id = parts.next()?.parse().ok()?;
        let object_id = parts.next().filter(|object_id| !object_id.is_empty())?;
        Some((
            Self {
                tenant_id: TenantId(tenant_id),
                flow_hash: FlowHash(flow_hash),
                run_id,
            },
            object_id.to_owned(),
        ))
    }
}

/// Encodes a hard-delete Attribute value as its object body: the protobuf `dex.Value`.
pub fn encode_hard_delete_value(value: &Value) -> Bytes {
    Bytes::from(value.encode_to_vec())
}

/// Decodes an object body written by [`encode_hard_delete_value`].
pub fn decode_hard_delete_value(object_key: &str, body: &[u8]) -> Result<Value, EffectError> {
    Value::decode(body).map_err(|error| EffectError::CorruptState {
        table: "hard-delete object",
        key: object_key.to_owned(),
        detail: error.to_string(),
    })
}

/// Records hard-delete uploads whose command may not have committed, so that a restart can
/// delete the orphans.
pub trait HardDeleteUploadLedger: Send + Sync + 'static {
    /// Durably records uploads before they start.
    fn record_pending_uploads(
        &self,
        uploads: &[PendingHardDeleteUpload],
    ) -> Result<(), EffectError>;
    /// Removes records once their command committed or their objects were deleted.
    fn clear_pending_uploads(&self, object_keys: &[String]) -> Result<(), EffectError>;
    /// Returns every recorded upload.
    fn pending_uploads(&self) -> Result<Vec<PendingHardDeleteUpload>, EffectError>;
}

/// [`HardDeleteUploadLedger`] stored in the local store's node metadata row.
pub struct NodeMetaUploadLedger {
    store: Arc<dyn LocalStore + Send + Sync>,
    node_meta_lock: Arc<Mutex<()>>,
}

impl NodeMetaUploadLedger {
    /// Creates a ledger over `store`, which must already hold a node metadata row.
    ///
    /// `node_meta_lock` must be held by every other read-modify-write of the node metadata
    /// row, such as the snapshot service's, or one writer loses the other's update.
    pub fn new(store: Arc<dyn LocalStore + Send + Sync>, node_meta_lock: Arc<Mutex<()>>) -> Self {
        Self {
            store,
            node_meta_lock,
        }
    }

    fn update(
        &self,
        change: impl FnOnce(&mut Vec<PendingHardDeleteUpload>),
    ) -> Result<(), EffectError> {
        let _guard = self.node_meta_lock.lock().expect("node meta lock poisoned");
        let mut meta = self
            .store
            .node_meta()?
            .ok_or_else(|| EffectError::CorruptState {
                table: "NodeMeta",
                key: "node".to_owned(),
                detail: "node metadata row is missing".to_owned(),
            })?;
        change(&mut meta.pending_hard_delete_uploads);
        let mut batch = WriteBatch::new();
        batch.put_node_meta(&meta);
        Ok(self.store.write(batch)?)
    }
}

impl HardDeleteUploadLedger for NodeMetaUploadLedger {
    fn record_pending_uploads(
        &self,
        uploads: &[PendingHardDeleteUpload],
    ) -> Result<(), EffectError> {
        self.update(|pending| pending.extend_from_slice(uploads))
    }

    fn clear_pending_uploads(&self, object_keys: &[String]) -> Result<(), EffectError> {
        self.update(|pending| pending.retain(|upload| !object_keys.contains(&upload.object_key)))
    }

    fn pending_uploads(&self) -> Result<Vec<PendingHardDeleteUpload>, EffectError> {
        Ok(self
            .store
            .node_meta()?
            .map(|meta| meta.pending_hard_delete_uploads)
            .unwrap_or_default())
    }
}

/// Objects uploaded for one command; finish them with [`HardDeleteObjects::finish_uploads`].
#[derive(Debug, Default, Clone, PartialEq)]
#[must_use = "uploads must be finished after the command commits or is rejected"]
pub struct HardDeleteUploads {
    /// References to put into the command, one per uploaded write.
    pub references: Vec<HardDeleteObjectReference>,
    object_keys: Vec<String>,
}

impl HardDeleteUploads {
    /// Returns true when the command had no hard-delete value to upload.
    pub fn is_empty(&self) -> bool {
        self.object_keys.is_empty()
    }
}

/// Hard-delete object storage operations shared by the effect runner and the client API path.
#[derive(Clone)]
pub struct HardDeleteObjects {
    store: Arc<dyn ObjectStore>,
    ledger: Arc<dyn HardDeleteUploadLedger>,
    clock: Arc<dyn WallClock>,
    hasher: PartitionHasher,
    id_source: Arc<dyn EngineIdSource>,
}

impl HardDeleteObjects {
    /// Creates the operations over the hard-delete object store, which may be another bucket
    /// or prefix than the log.
    pub fn new(
        store: Arc<dyn ObjectStore>,
        ledger: Arc<dyn HardDeleteUploadLedger>,
        clock: Arc<dyn WallClock>,
        hasher: PartitionHasher,
        id_source: Arc<dyn EngineIdSource>,
    ) -> Self {
        Self {
            store,
            ledger,
            clock,
            hasher,
            id_source,
        }
    }

    /// Returns the owner of `flow`'s run `run_id`.
    pub fn owner_of(&self, flow: &FlowAddress, run_id: RunId) -> HardDeleteObjectOwner {
        HardDeleteObjectOwner {
            tenant_id: flow.tenant_id,
            flow_hash: self.hasher.flow_hash(flow.tenant_id, &flow.flow_id),
            run_id,
        }
    }

    /// Uploads every non-null hard-delete Attribute value in `writes` as a new object and
    /// replaces it with [`hard_delete_placeholder_value`] so the value never enters the log.
    ///
    /// Returns the references to put into the command. The uploads are recorded in the ledger
    /// before they start; call [`Self::finish_uploads`] once the command's outcome is known.
    /// On error `writes` is unchanged, so the caller may retry.
    pub async fn upload_attribute_writes(
        &self,
        owner: HardDeleteObjectOwner,
        writes: &mut [AttributeWrite],
    ) -> Result<HardDeleteUploads, EffectError> {
        let mut planned = Vec::new();
        for (write_index, write) in writes.iter().enumerate() {
            let Some(is_string_value) = hard_delete_value_kind(write) else {
                continue;
            };
            let object_id = self
                .id_source
                .hard_delete_object_id()
                .hyphenated()
                .to_string();
            planned.push((write_index, object_id, is_string_value));
        }
        if planned.is_empty() {
            return Ok(HardDeleteUploads::default());
        }
        let uploaded_at_unix_micros = self.clock.now_unix_micros();
        let records: Vec<PendingHardDeleteUpload> = planned
            .iter()
            .map(|(_, object_id, _)| PendingHardDeleteUpload {
                object_key: owner.object_key(object_id),
                uploaded_at_unix_micros,
            })
            .collect();
        self.ledger.record_pending_uploads(&records)?;
        let mut uploads = HardDeleteUploads {
            references: Vec::with_capacity(planned.len()),
            object_keys: records
                .into_iter()
                .map(|record| record.object_key)
                .collect(),
        };
        for ((write_index, object_id, is_string_value), object_key) in
            planned.into_iter().zip(uploads.object_keys.clone())
        {
            let value = writes[write_index].value.clone().unwrap_or_default();
            let body = encode_hard_delete_value(&value);
            let size_bytes = body.len() as u64;
            let failure = match self
                .store
                .put(&object_key, body, WriteCondition::CreateOnly)
                .await
            {
                Ok(WriteOutcome::Written { .. }) => None,
                Ok(WriteOutcome::PreconditionFailed) => Some(EffectError::InvariantViolation {
                    detail: format!("hard-delete object {object_key} already exists"),
                }),
                Err(error) => Some(EffectError::ObjectStore(error)),
            };
            if let Some(failure) = failure {
                self.abandon_uploads(&uploads.object_keys).await;
                return Err(failure);
            }
            uploads.references.push(HardDeleteObjectReference {
                write_index: u32::try_from(write_index).map_err(|_| {
                    EffectError::InvariantViolation {
                        detail: "attribute write index exceeds u32".to_owned(),
                    }
                })?,
                object_id,
                size_bytes,
                is_string_value,
            });
        }
        for reference in &uploads.references {
            writes[reference.write_index as usize].value = Some(hard_delete_placeholder_value());
        }
        Ok(uploads)
    }

    /// Finishes uploads once their command's outcome is known: deletes the objects when the
    /// command was rejected or never committed, then clears the ledger records.
    pub async fn finish_uploads(
        &self,
        uploads: HardDeleteUploads,
        is_committed: bool,
    ) -> Result<(), EffectError> {
        if uploads.is_empty() {
            return Ok(());
        }
        if !is_committed {
            self.delete_keys(&uploads.object_keys).await?;
        }
        self.ledger.clear_pending_uploads(&uploads.object_keys)
    }

    /// Reads the current content of a hard-delete object, or `None` when it was deleted.
    pub async fn read_value(
        &self,
        owner: HardDeleteObjectOwner,
        object_id: &str,
    ) -> Result<Option<Value>, EffectError> {
        let key = owner.object_key(object_id);
        let Some(content) = self.store.get(&key).await? else {
            return Ok(None);
        };
        decode_hard_delete_value(&key, &content.bytes).map(Some)
    }

    /// Deletes objects of one run. Deleting a missing object succeeds.
    pub(crate) async fn delete_objects(
        &self,
        owner: HardDeleteObjectOwner,
        object_ids: &[String],
    ) -> Result<(), EffectError> {
        let keys: Vec<String> = object_ids
            .iter()
            .map(|object_id| owner.object_key(object_id))
            .collect();
        self.delete_keys(&keys).await
    }

    /// Deletes every object under the run's prefix, including orphans of lost local state.
    pub(crate) async fn delete_run_prefix(
        &self,
        owner: HardDeleteObjectOwner,
    ) -> Result<(), EffectError> {
        let prefix = owner.object_key_prefix();
        loop {
            // Deleted keys disappear from the listing, so every page starts from the prefix.
            let listing = self
                .store
                .list(&prefix, None, DELETE_BATCH_KEYS as u32)
                .await?;
            if listing.objects.is_empty() {
                return Ok(());
            }
            let keys: Vec<String> = listing
                .objects
                .into_iter()
                .map(|object| object.key)
                .collect();
            self.store.delete_many(&keys).await?;
        }
    }

    /// Returns the uploads recorded in the ledger.
    pub(crate) fn pending_uploads(&self) -> Result<Vec<PendingHardDeleteUpload>, EffectError> {
        self.ledger.pending_uploads()
    }

    /// Deletes `uploads` (recorded before this node started) of runs in `partition` that
    /// committed state does not reference.
    pub(crate) async fn recover_pending_uploads(
        &self,
        partition: PartitionId,
        uploads: &[PendingHardDeleteUpload],
        reader: Box<dyn FlowStateReader + Send>,
    ) -> Result<(), EffectError> {
        let mut finished_keys = Vec::new();
        let mut orphan_keys = Vec::new();
        for upload in uploads.iter().cloned() {
            let Some((owner, object_id)) =
                HardDeleteObjectOwner::parse_object_key(&upload.object_key)
            else {
                tracing::warn!(object_key = %upload.object_key, "ignoring malformed pending hard-delete upload");
                finished_keys.push(upload.object_key);
                continue;
            };
            if self.hasher.partition_of(owner.flow_hash) != partition {
                continue;
            }
            let run = RunLocation {
                flow_hash: owner.flow_hash,
                run_id: owner.run_id,
            };
            if !is_object_referenced(reader.as_ref(), run, &object_id)? {
                orphan_keys.push(upload.object_key.clone());
            }
            finished_keys.push(upload.object_key);
        }
        drop(reader);
        if finished_keys.is_empty() {
            return Ok(());
        }
        self.delete_keys(&orphan_keys).await?;
        for object_key in &orphan_keys {
            tracing::info!(%object_key, "deleted orphan hard-delete upload");
        }
        self.ledger.clear_pending_uploads(&finished_keys)
    }

    async fn delete_keys(&self, keys: &[String]) -> Result<(), EffectError> {
        for chunk in keys.chunks(DELETE_BATCH_KEYS) {
            self.store.delete_many(chunk).await?;
        }
        Ok(())
    }

    async fn abandon_uploads(&self, object_keys: &[String]) {
        // The ledger keeps the records when this fails, so recovery deletes the objects later.
        match self.delete_keys(object_keys).await {
            Ok(()) => {
                if let Err(error) = self.ledger.clear_pending_uploads(object_keys) {
                    tracing::warn!(%error, "clearing abandoned hard-delete uploads failed");
                }
            }
            Err(error) => tracing::warn!(%error, "deleting abandoned hard-delete uploads failed"),
        }
    }
}

impl HardDeleteValueReader for HardDeleteObjects {
    fn read_hard_delete_value<'a>(
        &'a self,
        flow: &'a FlowAddress,
        run_id: RunId,
        object: &'a HardDeleteObjectReference,
    ) -> BoxFuture<'a, Result<Option<Value>, HardDeleteReadError>> {
        Box::pin(async move {
            self.read_value(self.owner_of(flow, run_id), &object.object_id)
                .await
                .map_err(|error| HardDeleteReadError {
                    object_id: object.object_id.clone(),
                    detail: error.to_string(),
                })
        })
    }
}

impl HardDeleteValueUploader for HardDeleteObjects {
    fn upload_hard_delete_values<'a>(
        &'a self,
        flow: &'a FlowAddress,
        run_id: RunId,
        writes: &'a mut [AttributeWrite],
    ) -> BoxFuture<'a, Result<RuntimeHardDeleteUploads, HardDeleteUploadError>> {
        Box::pin(async move {
            let uploads = self
                .upload_attribute_writes(self.owner_of(flow, run_id), writes)
                .await
                .map_err(|error| HardDeleteUploadError {
                    detail: error.to_string(),
                })?;
            Ok(RuntimeHardDeleteUploads {
                references: uploads.references,
                object_keys: uploads.object_keys,
            })
        })
    }

    fn finish_hard_delete_uploads(
        &self,
        uploads: RuntimeHardDeleteUploads,
        outcome: HardDeleteUploadOutcome,
    ) -> BoxFuture<'_, Result<(), HardDeleteUploadError>> {
        Box::pin(async move {
            let is_committed = match outcome {
                HardDeleteUploadOutcome::Committed => true,
                HardDeleteUploadOutcome::NotCommitted => false,
                // Recovery resolves these from committed state and the pending-upload records.
                HardDeleteUploadOutcome::Unknown => return Ok(()),
            };
            let uploads = HardDeleteUploads {
                references: uploads.references,
                object_keys: uploads.object_keys,
            };
            self.finish_uploads(uploads, is_committed)
                .await
                .map_err(|error| HardDeleteUploadError {
                    detail: error.to_string(),
                })
        })
    }
}

/// The value that replaces an uploaded hard-delete value in a proposed command: a `Value`
/// without a kind. Apply reads the value from the command's [`HardDeleteObjectReference`].
pub fn hard_delete_placeholder_value() -> Value {
    Value { kind: None }
}

/// Returns `Some(is_string_value)` when the write stores a hard-delete string or object.
fn hard_delete_value_kind(write: &AttributeWrite) -> Option<bool> {
    if !write.is_hard_delete_on_null {
        return None;
    }
    match write.value.as_ref()?.kind.as_ref()? {
        value::Kind::StringValue(_) => Some(true),
        value::Kind::ObjValue(_) => Some(false),
        _ => None,
    }
}

/// Returns true when the run's hard-delete object table refers to `object_id`.
fn is_object_referenced(
    reader: &dyn FlowStateReader,
    run: RunLocation,
    object_id: &str,
) -> Result<bool, EffectError> {
    let prefix = run_table_prefix(run.flow_hash, run.run_id, RunTable::HardDeleteObject);
    for (key, value) in scan_all(reader, ColumnFamily::Flow, &prefix)? {
        let record: HardDeleteObjectRecord = decode_row("HardDeleteObject", &key, &value)?;
        if record
            .object
            .is_some_and(|object| object.object_id == object_id)
        {
            return Ok(true);
        }
    }
    Ok(false)
}
