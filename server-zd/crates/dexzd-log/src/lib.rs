// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Command log batch format and the conditional-write commit protocol that makes server-zd writes durable in object storage.

mod batch_codec;
mod batch_read_ahead;
mod commit_pipeline;
mod stream_recovery;

pub use batch_codec::{
    BatchContent, BatchRecord, DecodedBatch, LogError, StreamIdentity, decode_batch, encode_batch,
    read_partition_records,
};
pub use batch_read_ahead::{LogReadAheadConfig, OrderedBatchReader};
pub use commit_pipeline::{
    CommitPipeline, CommitPipelineConfig, CommittedBatch, ProposalFailure, ProposalTicket,
};
pub use stream_recovery::{
    RecoveredStream, StreamRecovery, StreamRecoveryInputs, StreamRecoveryResult,
    StreamRecoveryStep, recover_stream, recover_stream_from,
};
