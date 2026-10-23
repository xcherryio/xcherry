// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Bucket lifecycle configuration round-trips; expiration timing is tested against the store directly.

use aws_sdk_s3::types::{
    AbortIncompleteMultipartUpload, BucketLifecycleConfiguration, ExpirationStatus,
    LifecycleExpiration, LifecycleRule, LifecycleRuleFilter, NoncurrentVersionExpiration,
};

use crate::support::{ConformanceTarget, error_code_and_status};

#[tokio::test]
async fn lifecycle_configuration_round_trips_and_deletes() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = target.create_bucket().await;
    let missing = target
        .client
        .get_bucket_lifecycle_configuration()
        .bucket(&bucket)
        .send()
        .await
        .unwrap_err();
    // https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLifecycleConfiguration.html
    assert_eq!(
        error_code_and_status(&missing),
        (Some("NoSuchLifecycleConfiguration".to_owned()), Some(404))
    );
    let rule = LifecycleRule::builder()
        .id("expire-logs")
        .status(ExpirationStatus::Enabled)
        .filter(LifecycleRuleFilter::builder().prefix("logs/").build())
        .expiration(LifecycleExpiration::builder().days(3).build())
        .noncurrent_version_expiration(
            NoncurrentVersionExpiration::builder()
                .noncurrent_days(5)
                .build(),
        )
        .abort_incomplete_multipart_upload(
            AbortIncompleteMultipartUpload::builder()
                .days_after_initiation(2)
                .build(),
        )
        .build()
        .unwrap();
    target
        .client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket)
        .lifecycle_configuration(
            BucketLifecycleConfiguration::builder()
                .rules(rule)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let stored = target
        .client
        .get_bucket_lifecycle_configuration()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();
    let rules = stored.rules();
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].id(), Some("expire-logs"));
    assert_eq!(rules[0].status(), &ExpirationStatus::Enabled);
    assert_eq!(
        rules[0].filter().and_then(|filter| filter.prefix()),
        Some("logs/")
    );
    assert_eq!(
        rules[0]
            .expiration()
            .and_then(|expiration| expiration.days()),
        Some(3)
    );
    assert_eq!(
        rules[0]
            .noncurrent_version_expiration()
            .and_then(|expiration| expiration.noncurrent_days()),
        Some(5)
    );
    assert_eq!(
        rules[0]
            .abort_incomplete_multipart_upload()
            .and_then(|abort| abort.days_after_initiation()),
        Some(2)
    );
    target
        .client
        .delete_bucket_lifecycle()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();
    let deleted = target
        .client
        .get_bucket_lifecycle_configuration()
        .bucket(&bucket)
        .send()
        .await
        .unwrap_err();
    assert_eq!(error_code_and_status(&deleted).1, Some(404));
    target.cleanup().await;
}

fn rule_with_expiration(expiration: LifecycleExpiration) -> LifecycleRule {
    LifecycleRule::builder()
        .status(ExpirationStatus::Enabled)
        .filter(LifecycleRuleFilter::builder().prefix("").build())
        .expiration(expiration)
        .build()
        .expect("the rule has a status")
}

// Days must be positive and cannot be combined with ExpiredObjectDeleteMarker; an explicit false
// marker action is a valid no-op: https://docs.aws.amazon.com/AmazonS3/latest/API/API_LifecycleExpiration.html
#[tokio::test]
async fn invalid_expiration_actions_are_rejected_and_a_false_marker_action_is_accepted() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = target.create_bucket().await;
    let put = |rule: LifecycleRule| {
        target
            .client
            .put_bucket_lifecycle_configuration()
            .bucket(&bucket)
            .lifecycle_configuration(
                BucketLifecycleConfiguration::builder()
                    .rules(rule)
                    .build()
                    .unwrap(),
            )
            .send()
    };
    let zero_days = put(rule_with_expiration(
        LifecycleExpiration::builder().days(0).build(),
    ))
    .await
    .unwrap_err();
    assert_eq!(error_code_and_status(&zero_days).1, Some(400));
    let combined = put(rule_with_expiration(
        LifecycleExpiration::builder()
            .days(1)
            .expired_object_delete_marker(true)
            .build(),
    ))
    .await
    .unwrap_err();
    assert_eq!(error_code_and_status(&combined).1, Some(400));
    put(rule_with_expiration(
        LifecycleExpiration::builder()
            .expired_object_delete_marker(false)
            .build(),
    ))
    .await
    .unwrap();
    let stored = target
        .client
        .get_bucket_lifecycle_configuration()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();
    assert_eq!(
        stored.rules()[0]
            .expiration()
            .and_then(|expiration| expiration.expired_object_delete_marker()),
        Some(false)
    );
    target.cleanup().await;
}
