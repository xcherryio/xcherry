// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Local-disk object store with S3 versioning, conditional writes, and listing semantics for s3mock.

mod checksum;
mod data_file;
mod error;
mod key_codec;
mod record;
mod store;

pub use checksum::{ChecksumAlgorithm, ChecksumType, ObjectChecksum, decode_base64, md5_base64};
pub use data_file::{ObjectWriter, StagedObject, UploadChecksumExpectations};
pub use error::StoreError;
pub use record::{
    BucketRecord, ContentHeaders, DefaultRetention, ObjectLockConfiguration, PartRecord,
    RetentionMode, UploadRecord, VersionRecord, VersioningState,
};
pub use store::{
    ByteRangeRequest, CompleteMultipartUploadRequest, CompletedPart, CreateMultipartUploadRequest,
    DeleteObjectOutcome, DeleteObjectRequest, Durability, ListMultipartUploadsRequest,
    ListObjectVersionsPage, ListObjectVersionsRequest, ListObjectsPage, ListObjectsRequest,
    ListPartsRequest, ListedObject, ListedVersion, MAX_KEYS_LIMIT, MAX_PART_NUMBER,
    MultipartUploadsPage, ObjectBody, ObjectReadRequest, ObjectSelection, OrphanCollection,
    PartsPage, PutObjectOutcome, PutObjectRequest, ReadConditions, ResolvedRange, Store,
    StoreConfig, WriteCondition,
};
