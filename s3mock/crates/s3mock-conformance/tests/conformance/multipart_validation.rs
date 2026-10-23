// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! CompleteMultipartUpload integrity headers and checksummed part numbering.

use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    ChecksumAlgorithm, ChecksumType, CompletedMultipartUpload, CompletedPart, Delete,
    ObjectIdentifier,
};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;

use crate::support::{ConformanceTarget, error_code_and_status};

/// Creates a CRC32 FULL_OBJECT upload with one part `hello` and returns the upload ID and that part.
async fn full_object_crc32_upload(
    client: &Client,
    bucket: &str,
    key: &str,
) -> (String, CompletedPart) {
    let upload_id = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .checksum_algorithm(ChecksumAlgorithm::Crc32)
        .checksum_type(ChecksumType::FullObject)
        .send()
        .await
        .expect("creating the upload succeeds")
        .upload_id()
        .expect("creation returns an upload ID")
        .to_owned();
    let uploaded = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .part_number(1)
        .checksum_algorithm(ChecksumAlgorithm::Crc32)
        .body(ByteStream::from_static(b"hello"))
        .send()
        .await
        .expect("uploading the part succeeds");
    let part = CompletedPart::builder()
        .part_number(1)
        .e_tag(uploaded.e_tag().expect("parts have ETags"))
        .set_checksum_crc32(uploaded.checksum_crc32().map(str::to_owned))
        .build();
    (upload_id, part)
}

fn crc32_base64(body: &[u8]) -> String {
    let mut checksum = u32::MAX;
    for byte in body {
        checksum ^= u32::from(*byte);
        for _ in 0..8 {
            checksum = (checksum >> 1) ^ (0xedb8_8320 & 0_u32.wrapping_sub(checksum & 1));
        }
    }
    BASE64.encode((!checksum).to_be_bytes())
}

// The x-amz-checksum-* header on CompleteMultipartUpload is the whole object's checksum:
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
#[tokio::test]
async fn completion_checks_the_full_object_checksum_type_and_size_headers() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = target.create_bucket().await;
    let client = target.client.clone();
    let (upload_id, part) = full_object_crc32_upload(&client, &bucket, "k").await;
    let complete = || {
        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key("k")
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(part.clone())
                    .build(),
            )
    };
    let wrong_checksum = complete()
        .checksum_crc32(crc32_base64(b"other"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(error_code_and_status(&wrong_checksum).1, Some(400));
    let wrong_type = complete()
        .checksum_type(ChecksumType::Composite)
        .send()
        .await
        .unwrap_err();
    assert_eq!(error_code_and_status(&wrong_type).1, Some(400));
    let wrong_size = complete().mpu_object_size(999).send().await.unwrap_err();
    assert_eq!(error_code_and_status(&wrong_size).1, Some(400));
    let completed = complete()
        .checksum_crc32(crc32_base64(b"hello"))
        .checksum_type(ChecksumType::FullObject)
        .mpu_object_size(5)
        .send()
        .await
        .unwrap();
    assert_eq!(
        completed.checksum_crc32(),
        Some(crc32_base64(b"hello").as_str())
    );
    target.cleanup().await;
}

// Checksummed uploads must list consecutive parts starting at 1:
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompletedPart.html
#[tokio::test]
async fn checksummed_completion_requires_parts_to_start_at_one() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = target.create_bucket().await;
    let client = target.client.clone();
    let upload_id = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key("k")
        .checksum_algorithm(ChecksumAlgorithm::Sha256)
        .send()
        .await
        .unwrap()
        .upload_id()
        .unwrap()
        .to_owned();
    let uploaded = client
        .upload_part()
        .bucket(&bucket)
        .key("k")
        .upload_id(&upload_id)
        .part_number(2)
        .checksum_algorithm(ChecksumAlgorithm::Sha256)
        .body(ByteStream::from_static(b"second"))
        .send()
        .await
        .unwrap();
    let part = CompletedPart::builder()
        .part_number(2)
        .e_tag(uploaded.e_tag().unwrap())
        .set_checksum_sha256(uploaded.checksum_sha256().map(str::to_owned))
        .build();
    let error = client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key("k")
        .upload_id(&upload_id)
        .multipart_upload(CompletedMultipartUpload::builder().parts(part).build())
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error_code_and_status(&error),
        (Some("InvalidPartOrder".to_owned()), Some(400))
    );
    client
        .abort_multipart_upload()
        .bucket(&bucket)
        .key("k")
        .upload_id(&upload_id)
        .send()
        .await
        .unwrap();
    target.cleanup().await;
}

#[tokio::test]
async fn delete_objects_on_a_missing_bucket_is_no_such_bucket() {
    let target = ConformanceTarget::connect().await;
    let bucket = format!(
        "s3mock-conformance-missing-{}",
        ulid::Ulid::new().to_string().to_ascii_lowercase()
    );
    let error = target
        .client
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key("k").build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error_code_and_status(&error),
        (Some("NoSuchBucket".to_owned()), Some(404))
    );
    target.cleanup().await;
}

#[tokio::test]
async fn a_full_object_checksum_with_a_part_count_suffix_is_rejected() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = target.create_bucket().await;
    let client = target.client.clone();
    let (upload_id, part) = full_object_crc32_upload(&client, &bucket, "k").await;
    let error = client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key("k")
        .upload_id(&upload_id)
        .checksum_crc32(format!("{}-999", crc32_base64(b"hello")))
        .multipart_upload(CompletedMultipartUpload::builder().parts(part).build())
        .send()
        .await
        .unwrap_err();
    assert_eq!(error_code_and_status(&error).1, Some(400));
    target.cleanup().await;
}
