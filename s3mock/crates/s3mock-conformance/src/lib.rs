// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! S3 conformance tests that run through aws-sdk-s3 against s3mock or real S3.
//!
//! By default each test starts an in-process s3mock server on a free port. Setting
//! `S3MOCK_CONFORMANCE_TARGET=aws` runs the same tests against real S3 with the default AWS
//! credential chain, so s3mock behavior can be compared with S3.
