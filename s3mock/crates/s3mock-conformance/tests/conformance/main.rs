// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! S3 conformance tests; each test runs against the target selected by `S3MOCK_CONFORMANCE_TARGET`.

mod buckets;
mod conditions;
mod delete_objects;
mod durability;
mod lifecycle;
mod listing;
mod multipart;
mod multipart_validation;
mod object_lock;
mod objects;
mod support;
mod versioning;
