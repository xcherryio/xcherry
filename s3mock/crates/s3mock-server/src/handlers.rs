// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! One handler per S3 operation.

mod bucket;
mod listing;
mod object;

pub(crate) use bucket::{
    create_bucket, delete_bucket, get_bucket_location, get_bucket_versioning, head_bucket,
    list_buckets, put_bucket_versioning,
};
pub(crate) use listing::{list_object_versions, list_objects_v2};
pub(crate) use object::{delete_object, get_object, head_object, put_object};

/// Owner reported in listings; s3mock has a single implicit account.
const OWNER_ID: &str = "s3mock";
