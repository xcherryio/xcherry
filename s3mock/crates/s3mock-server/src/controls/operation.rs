// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Closed operation names used by fault selection and observations.

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub(crate) enum S3OperationName {
    ListBuckets,
    GetObjectLockConfiguration,
    PutObjectLockConfiguration,
    DeleteObjects,
    ListMultipartUploads,
    CreateMultipartUpload,
    UploadPart,
    CompleteMultipartUpload,
    AbortMultipartUpload,
    ListParts,
    CreateBucket,
    HeadBucket,
    DeleteBucket,
    GetBucketLocation,
    GetBucketVersioning,
    PutBucketVersioning,
    ListObjectsV2,
    ListObjectVersions,
    PutObject,
    GetObject,
    HeadObject,
    DeleteObject,
    Unsupported,
    GetBucketLifecycleConfiguration,
    PutBucketLifecycleConfiguration,
    DeleteBucketLifecycle,
}

impl std::fmt::Display for S3OperationName {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}
