// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Hard-delete Attribute values: uploaded before a client command is proposed, finished once
//! the command's outcome is known.

use dexzd_keyspace::RunId;
use dexzd_protocol::dex::{AttributeWrite, value};
use dexzd_protocol::dexzd::v1::{FlowCommand, HardDeleteObjectReference, command_reply};

use crate::runtime::registry::ProposalScope;
use crate::runtime::{
    CommittedCommand, FlowAddress, FlowRuntimeRegistry, HardDeleteUploadOutcome, HardDeleteUploads,
    RuntimeError,
};

impl FlowRuntimeRegistry {
    /// Uploads the hard-delete values in `writes` as objects of `flow`'s run `run_id`.
    ///
    /// Returns `None` when no write has a hard-delete value. Returns `Unavailable` when the upload
    /// fails, and `Internal` when a hard-delete value got no object; nothing is left to finish
    /// then.
    pub(crate) async fn upload_hard_delete_values(
        &self,
        flow: &FlowAddress,
        run_id: RunId,
        writes: &mut [AttributeWrite],
    ) -> Result<Option<HardDeleteUploads>, RuntimeError> {
        if !has_hard_delete_values(writes) {
            return Ok(None);
        }
        self.check_running()?;
        let uploads = self
            .inner
            .dependencies
            .hard_delete_uploader
            .upload_hard_delete_values(flow, run_id, writes)
            .await
            .map_err(|error| RuntimeError::Unavailable {
                detail: format!(
                    "uploading hard-delete Attribute values of Flow {} failed: {error}",
                    flow.flow_id
                ),
            })?;
        // Apply halts the partition on a hard-delete value without an object.
        let missing_key = writes.iter().enumerate().find_map(|(write_index, write)| {
            let has_reference = uploads
                .references
                .iter()
                .any(|reference| usize::try_from(reference.write_index).ok() == Some(write_index));
            (has_hard_delete_value(write) && !has_reference).then(|| write.key.clone())
        });
        if let Some(attribute_key) = missing_key {
            self.finish_hard_delete_uploads(flow, uploads, HardDeleteUploadOutcome::NotCommitted)
                .await;
            return Err(RuntimeError::Internal {
                detail: format!(
                    "hard-delete Attribute {attribute_key:?} of Flow {} has no uploaded object",
                    flow.flow_id
                ),
            });
        }
        Ok(Some(uploads))
    }

    /// Proposes a client command that carries `uploads`, waits for its commit, and finishes the
    /// uploads by the outcome.
    ///
    /// Returns the committed reply, or the proposal or commit error; an error reply becomes a
    /// [`RuntimeError`].
    pub(crate) async fn propose_client_command_with_uploads(
        &self,
        flow: &FlowAddress,
        uploads: Option<HardDeleteUploads>,
        build: impl FnMut(&mut ProposalScope<'_>) -> Result<FlowCommand, RuntimeError>,
    ) -> Result<command_reply::Reply, RuntimeError> {
        let Some(uploads) = uploads else {
            return self.propose_client_command(flow, build).await;
        };
        let committed = self.propose_and_wait_for_commit(flow, build).await;
        let outcome = match &committed {
            // A rejected command writes nothing, so nothing references the objects.
            Ok(committed) if committed.outcome.writes.is_empty() => {
                HardDeleteUploadOutcome::NotCommitted
            }
            Ok(_) => HardDeleteUploadOutcome::Committed,
            // Every failure after submit halts the registry first.
            Err(_) if !self.halted().is_cancelled() => HardDeleteUploadOutcome::NotCommitted,
            Err(_) => HardDeleteUploadOutcome::Unknown,
        };
        self.finish_hard_delete_uploads(flow, uploads, outcome)
            .await;
        committed.and_then(committed_reply)
    }

    async fn finish_hard_delete_uploads(
        &self,
        flow: &FlowAddress,
        uploads: HardDeleteUploads,
        outcome: HardDeleteUploadOutcome,
    ) {
        if let Err(error) = self
            .inner
            .dependencies
            .hard_delete_uploader
            .finish_hard_delete_uploads(uploads, outcome)
            .await
        {
            // The command's result stands; pending-upload recovery retries the cleanup.
            tracing::warn!(
                flow_id = %flow.flow_id,
                ?outcome,
                %error,
                "finishing hard-delete uploads failed"
            );
        }
    }
}

/// Returns the committed reply; an error reply becomes a [`RuntimeError`].
pub(crate) fn committed_reply(
    committed: CommittedCommand,
) -> Result<command_reply::Reply, RuntimeError> {
    match committed.outcome.reply.reply {
        Some(command_reply::Reply::Error(error)) => Err(RuntimeError::from_reply(&error)),
        Some(reply) => Ok(reply),
        None => Err(RuntimeError::Internal {
            detail: "apply returned an empty reply".to_owned(),
        }),
    }
}

/// Returns the references to put into a command, empty when nothing was uploaded.
pub(crate) fn hard_delete_references(
    uploads: Option<&HardDeleteUploads>,
) -> Vec<HardDeleteObjectReference> {
    uploads.map_or_else(Vec::new, |uploads| uploads.references.clone())
}

pub(crate) fn has_hard_delete_values(writes: &[AttributeWrite]) -> bool {
    writes.iter().any(has_hard_delete_value)
}

/// Mirrors apply: a hard-delete write with a non-null value needs an uploaded object.
fn has_hard_delete_value(write: &AttributeWrite) -> bool {
    write.is_hard_delete_on_null
        && write
            .value
            .as_ref()
            .and_then(|value| value.kind.as_ref())
            .is_some_and(|kind| !matches!(kind, value::Kind::NullValue(_)))
}
