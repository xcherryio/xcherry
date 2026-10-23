// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Object Lock bucket configuration without retention enforcement.

use aws_sdk_s3::types::{
    BucketVersioningStatus, DefaultRetention, ObjectLockConfiguration, ObjectLockEnabled,
    ObjectLockRetentionMode, ObjectLockRule,
};

use crate::support::{ConformanceTarget, error_code_and_status};

#[tokio::test]
async fn creating_an_object_lock_bucket_enables_versioning_and_object_lock() {
    let mut target = ConformanceTarget::connect().await;
    // Object Lock creation also enables versioning: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock-configure.html
    let bucket = target.create_bucket_with_object_lock(true).await;
    let versioning = target
        .client
        .get_bucket_versioning()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();
    assert_eq!(versioning.status(), Some(&BucketVersioningStatus::Enabled));
    let configuration = target
        .client
        .get_object_lock_configuration()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();
    assert_eq!(
        configuration
            .object_lock_configuration()
            .unwrap()
            .object_lock_enabled(),
        Some(&ObjectLockEnabled::Enabled)
    );
    target.cleanup().await;
}

#[tokio::test]
async fn default_governance_retention_configuration_round_trips() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = target.create_bucket_with_object_lock(true).await;
    // Default retention requires mode and one duration: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLockConfiguration.html
    let expected = ObjectLockConfiguration::builder()
        .object_lock_enabled(ObjectLockEnabled::Enabled)
        .rule(
            ObjectLockRule::builder()
                .default_retention(
                    DefaultRetention::builder()
                        .mode(ObjectLockRetentionMode::Governance)
                        .days(1)
                        .build(),
                )
                .build(),
        )
        .build();
    target
        .client
        .put_object_lock_configuration()
        .bucket(&bucket)
        .object_lock_configuration(expected.clone())
        .send()
        .await
        .unwrap();
    let retrieved = target
        .client
        .get_object_lock_configuration()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();
    assert_eq!(retrieved.object_lock_configuration(), Some(&expected));
    target.cleanup().await;
}

#[tokio::test]
async fn normal_buckets_have_no_object_lock_configuration() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = target.create_bucket().await;
    // Missing configuration is a 404: https://docs.aws.amazon.com/AmazonS3/latest/developerguide/ErrorResponses.html#ErrorCodeList
    let error = target
        .client
        .get_object_lock_configuration()
        .bucket(&bucket)
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error_code_and_status(&error),
        (
            Some("ObjectLockConfigurationNotFoundError".to_owned()),
            Some(404)
        )
    );
    target.cleanup().await;
}
