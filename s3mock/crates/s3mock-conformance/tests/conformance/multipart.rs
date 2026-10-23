// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Multipart upload lifecycle, assembly, checksums, pagination, and validation.

use aws_sdk_s3::Client;
use aws_sdk_s3::config::RequestChecksumCalculation;
use aws_sdk_s3::types::{
    BucketVersioningStatus, ChecksumAlgorithm, ChecksumMode, ChecksumType,
    CompletedMultipartUpload, CompletedPart, ServerSideEncryption, VersioningConfiguration,
};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use md5::{Digest, Md5};

use crate::objects::put_text;
use crate::support::{ConformanceTarget, error_code_and_status, read_body};

const MINIMUM_PART_BYTES: usize = 5 * 1024 * 1024;

#[tokio::test]
async fn three_parts_become_visible_only_after_completion_and_ranges_cross_boundaries() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = target.create_bucket().await;
    let client = multipart_client(&target);
    let upload_id = initiate(&client, &bucket, "assembled").await;
    let bodies = [
        vec![b'a'; MINIMUM_PART_BYTES],
        vec![b'b'; MINIMUM_PART_BYTES],
        b"last".to_vec(),
    ];
    let mut parts = Vec::new();
    for (index, body) in bodies.iter().enumerate() {
        parts.push(
            upload_part(
                &client,
                &bucket,
                "assembled",
                &upload_id,
                i32::try_from(index + 1).unwrap(),
                body.clone(),
            )
            .await,
        );
    }
    // In-progress parts are not objects: https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
    let missing = client
        .get_object()
        .bucket(&bucket)
        .key("assembled")
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error_code_and_status(&missing),
        (Some("NoSuchKey".to_owned()), Some(404))
    );
    assert!(
        client
            .list_objects_v2()
            .bucket(&bucket)
            .send()
            .await
            .unwrap()
            .contents()
            .is_empty()
    );
    let complete = client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key("assembled")
        .upload_id(&upload_id)
        .multipart_upload(completion(parts))
        .send()
        .await
        .unwrap();
    // Multipart ETags hash binary part digests: https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html
    let digests: Vec<u8> = bodies
        .iter()
        .flat_map(|body| Md5::digest(body).to_vec())
        .collect();
    let expected_etag = format!("\"{:x}-3\"", Md5::digest(&digests));
    assert_eq!(complete.e_tag(), Some(expected_etag.as_str()));
    let get = client
        .get_object()
        .bucket(&bucket)
        .key("assembled")
        .send()
        .await
        .unwrap();
    assert_eq!(get.e_tag(), Some(expected_etag.as_str()));
    assert_eq!(read_body(get).await, bodies.concat());
    // GET ranges address assembled bytes: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html#API_GetObject_RequestSyntax
    let range = format!(
        "bytes={}-{}",
        MINIMUM_PART_BYTES - 2,
        MINIMUM_PART_BYTES + 2
    );
    let get = client
        .get_object()
        .bucket(&bucket)
        .key("assembled")
        .range(range)
        .send()
        .await
        .unwrap();
    assert_eq!(get.content_length(), Some(5));
    assert_eq!(
        get.content_range(),
        Some(
            format!(
                "bytes {}-{}/{}",
                MINIMUM_PART_BYTES - 2,
                MINIMUM_PART_BYTES + 2,
                MINIMUM_PART_BYTES * 2 + 4
            )
            .as_str()
        )
    );
    assert_eq!(read_body(get).await, b"aabbb");
    target.cleanup().await;
}

#[tokio::test]
async fn uploading_the_same_part_number_replaces_its_body_and_etag() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = target.create_bucket().await;
    let client = multipart_client(&target);
    let upload_id = initiate(&client, &bucket, "replacement").await;
    let original = upload_part(
        &client,
        &bucket,
        "replacement",
        &upload_id,
        1,
        b"old".to_vec(),
    )
    .await;
    // Part numbers identify replaceable parts: https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html
    let replacement = upload_part(
        &client,
        &bucket,
        "replacement",
        &upload_id,
        1,
        b"new body".to_vec(),
    )
    .await;
    assert_ne!(original.e_tag(), replacement.e_tag());
    let listed = client
        .list_parts()
        .bucket(&bucket)
        .key("replacement")
        .upload_id(&upload_id)
        .send()
        .await
        .unwrap();
    assert_eq!(listed.parts().len(), 1);
    assert_eq!(listed.parts()[0].e_tag(), replacement.e_tag());
    assert_eq!(listed.parts()[0].size(), Some(8));
    client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key("replacement")
        .upload_id(&upload_id)
        .multipart_upload(completion(vec![replacement]))
        .send()
        .await
        .unwrap();
    let get = client
        .get_object()
        .bucket(&bucket)
        .key("replacement")
        .send()
        .await
        .unwrap();
    assert_eq!(read_body(get).await, b"new body");
    target.cleanup().await;
}

#[tokio::test]
async fn list_parts_paginates_in_part_number_order() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = target.create_bucket().await;
    let client = multipart_client(&target);
    let upload_id = initiate(&client, &bucket, "parts").await;
    let mut uploaded = Vec::new();
    for number in [3, 1, 2] {
        uploaded.push(
            upload_part(
                &client,
                &bucket,
                "parts",
                &upload_id,
                number,
                vec![number as u8; 7],
            )
            .await,
        );
    }
    // Continue after NextPartNumberMarker: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html
    let first = client
        .list_parts()
        .bucket(&bucket)
        .key("parts")
        .upload_id(&upload_id)
        .max_parts(2)
        .send()
        .await
        .unwrap();
    assert_eq!(first.is_truncated(), Some(true));
    assert_eq!(first.max_parts(), Some(2));
    assert_eq!(
        first
            .parts()
            .iter()
            .map(|part| part.part_number().unwrap())
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(first.next_part_number_marker(), Some("2"));
    for part in first.parts() {
        let expected = uploaded
            .iter()
            .find(|uploaded| uploaded.part_number() == part.part_number())
            .unwrap();
        assert_eq!(part.e_tag(), expected.e_tag());
        assert_eq!(part.size(), Some(7));
    }
    let second = client
        .list_parts()
        .bucket(&bucket)
        .key("parts")
        .upload_id(&upload_id)
        .max_parts(2)
        .part_number_marker(first.next_part_number_marker().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(second.is_truncated(), Some(false));
    assert_eq!(second.parts().len(), 1);
    assert_eq!(second.parts()[0].part_number(), Some(3));
    abort(&client, &bucket, "parts", &upload_id).await;
    target.cleanup().await;
}

#[tokio::test]
async fn list_multipart_uploads_filters_prefix_and_continues_with_both_markers() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = target.create_bucket().await;
    let client = multipart_client(&target);
    let mut uploads = Vec::new();
    for key in ["group/a", "group/a", "group/b", "other"] {
        uploads.push((key, initiate(&client, &bucket, key).await));
    }
    // Both markers preserve same-key uploads: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html
    let mut found = Vec::new();
    let mut key_marker = None;
    let mut upload_id_marker = None;
    for page_number in 0..3 {
        let page = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .prefix("group/")
            .max_uploads(1)
            .set_key_marker(key_marker)
            .set_upload_id_marker(upload_id_marker)
            .send()
            .await
            .unwrap();
        assert_eq!(page.max_uploads(), Some(1));
        assert_eq!(page.prefix(), Some("group/"));
        assert_eq!(page.uploads().len(), 1);
        let upload = &page.uploads()[0];
        let key = upload.key().unwrap();
        assert_eq!(
            key,
            if page_number < 2 {
                "group/a"
            } else {
                "group/b"
            }
        );
        found.push((key.to_owned(), upload.upload_id().unwrap().to_owned()));
        assert_eq!(page.is_truncated(), Some(page_number < 2));
        if page_number < 2 {
            key_marker = Some(page.next_key_marker().unwrap().to_owned());
            upload_id_marker = Some(page.next_upload_id_marker().unwrap().to_owned());
        } else {
            key_marker = None;
            upload_id_marker = None;
        }
    }
    let mut expected: Vec<_> = uploads
        .iter()
        .take(3)
        .map(|(key, upload_id)| (key.to_string(), upload_id.clone()))
        .collect();
    expected.sort();
    found.sort();
    assert_eq!(found, expected);
    // A key marker alone skips all uploads of that key: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html
    let after_key = client
        .list_multipart_uploads()
        .bucket(&bucket)
        .prefix("group/")
        .key_marker("group/a")
        .send()
        .await
        .unwrap();
    assert_eq!(after_key.uploads().len(), 1);
    assert_eq!(after_key.uploads()[0].key(), Some("group/b"));
    for (key, upload_id) in uploads {
        abort(&client, &bucket, key, &upload_id).await;
    }
    assert!(
        client
            .list_multipart_uploads()
            .bucket(&bucket)
            .send()
            .await
            .unwrap()
            .uploads()
            .is_empty()
    );
    target.cleanup().await;
}

#[tokio::test]
async fn completion_rejects_a_small_nonfinal_part() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = target.create_bucket().await;
    let client = multipart_client(&target);
    let upload_id = initiate(&client, &bucket, "small").await;
    let first = upload_part(&client, &bucket, "small", &upload_id, 1, b"small".to_vec()).await;
    let last = upload_part(&client, &bucket, "small", &upload_id, 2, b"last".to_vec()).await;
    // Only the last part may be under 5 MiB: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
    let error = client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key("small")
        .upload_id(&upload_id)
        .multipart_upload(completion(vec![first, last]))
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error_code_and_status(&error),
        (Some("EntityTooSmall".to_owned()), Some(400))
    );
    abort(&client, &bucket, "small", &upload_id).await;
    target.cleanup().await;
}

#[tokio::test]
async fn completion_rejects_parts_out_of_order() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = target.create_bucket().await;
    let client = multipart_client(&target);
    let upload_id = initiate(&client, &bucket, "order").await;
    let first = upload_part(
        &client,
        &bucket,
        "order",
        &upload_id,
        1,
        vec![1; MINIMUM_PART_BYTES],
    )
    .await;
    let second = upload_part(
        &client,
        &bucket,
        "order",
        &upload_id,
        2,
        vec![2; MINIMUM_PART_BYTES],
    )
    .await;
    // Completion requires ascending part numbers: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
    let error = client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key("order")
        .upload_id(&upload_id)
        .multipart_upload(completion(vec![second, first]))
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error_code_and_status(&error),
        (Some("InvalidPartOrder".to_owned()), Some(400))
    );
    abort(&client, &bucket, "order", &upload_id).await;
    target.cleanup().await;
}

#[tokio::test]
async fn completion_rejects_a_wrong_etag() {
    assert_invalid_part(false).await;
}

#[tokio::test]
async fn completion_rejects_a_missing_part() {
    assert_invalid_part(true).await;
}

#[tokio::test]
async fn aborted_and_unknown_upload_ids_return_no_such_upload() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = target.create_bucket().await;
    let client = multipart_client(&target);
    let upload_id = initiate(&client, &bucket, "aborted").await;
    let part = upload_part(
        &client,
        &bucket,
        "aborted",
        &upload_id,
        1,
        b"discarded".to_vec(),
    )
    .await;
    abort(&client, &bucket, "aborted", &upload_id).await;
    // Aborted or nonexistent IDs are NoSuchUpload: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
    for missing_id in [upload_id.as_str(), "nonexistent-upload-id"] {
        let error = client
            .list_parts()
            .bucket(&bucket)
            .key("aborted")
            .upload_id(missing_id)
            .send()
            .await
            .unwrap_err();
        assert_eq!(
            error_code_and_status(&error),
            (Some("NoSuchUpload".to_owned()), Some(404))
        );
        let error = client
            .upload_part()
            .bucket(&bucket)
            .key("aborted")
            .upload_id(missing_id)
            .part_number(1)
            .body(b"new".to_vec().into())
            .send()
            .await
            .unwrap_err();
        assert_eq!(
            error_code_and_status(&error),
            (Some("NoSuchUpload".to_owned()), Some(404))
        );
        let error = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key("aborted")
            .upload_id(missing_id)
            .multipart_upload(completion(vec![part.clone()]))
            .send()
            .await
            .unwrap_err();
        assert_eq!(
            error_code_and_status(&error),
            (Some("NoSuchUpload".to_owned()), Some(404))
        );
    }
    // Aborting an aborted upload again succeeds; an ID that never existed is NoSuchUpload (verified against AWS S3).
    abort(&client, &bucket, "aborted", &upload_id).await;
    let error = client
        .abort_multipart_upload()
        .bucket(&bucket)
        .key("aborted")
        .upload_id("nonexistent-upload-id")
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error_code_and_status(&error),
        (Some("NoSuchUpload".to_owned()), Some(404))
    );
    assert!(
        client
            .list_multipart_uploads()
            .bucket(&bucket)
            .send()
            .await
            .unwrap()
            .uploads()
            .is_empty()
    );
    let missing = client
        .get_object()
        .bucket(&bucket)
        .key("aborted")
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error_code_and_status(&missing),
        (Some("NoSuchKey".to_owned()), Some(404))
    );
    target.cleanup().await;
}

#[tokio::test]
async fn conditional_completion_cannot_overwrite_an_existing_key() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = target.create_bucket().await;
    let client = multipart_client(&target);
    let upload_id = initiate(&client, &bucket, "conditional").await;
    let part = upload_part(
        &client,
        &bucket,
        "conditional",
        &upload_id,
        1,
        b"multipart".to_vec(),
    )
    .await;
    put_text(&target, &bucket, "conditional", "existing").await;
    // Existing keys fail If-None-Match completion: https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html
    let error = client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key("conditional")
        .upload_id(&upload_id)
        .if_none_match("*")
        .multipart_upload(completion(vec![part]))
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error_code_and_status(&error),
        (Some("PreconditionFailed".to_owned()), Some(412))
    );
    let get = client
        .get_object()
        .bucket(&bucket)
        .key("conditional")
        .send()
        .await
        .unwrap();
    assert_eq!(read_body(get).await, b"existing");
    // The guide specifies retry lifecycle for 409, but not 412: https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html
    let uploads = client
        .list_multipart_uploads()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();
    for upload in uploads.uploads() {
        abort(
            &client,
            &bucket,
            upload.key().unwrap(),
            upload.upload_id().unwrap(),
        )
        .await;
    }
    target.cleanup().await;
}

#[tokio::test]
async fn crc32_parts_echo_checksums_and_get_returns_a_composite_checksum() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = target.create_bucket().await;
    let client = multipart_client(&target);
    // CRC32 supports explicit composite checksums: https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html
    let created = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key("checksummed")
        .checksum_algorithm(ChecksumAlgorithm::Crc32)
        .checksum_type(ChecksumType::Composite)
        .send()
        .await
        .unwrap();
    let upload_id = created.upload_id().unwrap();
    let bodies = [vec![b'x'; MINIMUM_PART_BYTES], b"tail".to_vec()];
    let mut parts = Vec::new();
    let mut part_checksums = Vec::new();
    for (index, body) in bodies.iter().enumerate() {
        let checksum = crc32(body).to_be_bytes();
        let encoded = BASE64.encode(checksum);
        let number = i32::try_from(index + 1).unwrap();
        let uploaded = client
            .upload_part()
            .bucket(&bucket)
            .key("checksummed")
            .upload_id(upload_id)
            .part_number(number)
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .checksum_crc32(&encoded)
            .body(body.clone().into())
            .send()
            .await
            .unwrap();
        // UploadPart returns the validated checksum: https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html
        assert_eq!(uploaded.checksum_crc32(), Some(encoded.as_str()));
        part_checksums.extend_from_slice(&checksum);
        parts.push(
            CompletedPart::builder()
                .part_number(number)
                .e_tag(uploaded.e_tag().unwrap())
                .checksum_crc32(encoded)
                .build(),
        );
    }
    client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key("checksummed")
        .upload_id(upload_id)
        .checksum_type(ChecksumType::Composite)
        .multipart_upload(completion(parts))
        .send()
        .await
        .unwrap();
    // Composite CRC32 aggregates binary part checksums: https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html
    let expected = format!("{}-2", BASE64.encode(crc32(&part_checksums).to_be_bytes()));
    let get = client
        .get_object()
        .bucket(&bucket)
        .key("checksummed")
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .unwrap();
    assert_eq!(get.checksum_type(), Some(&ChecksumType::Composite));
    assert_eq!(get.checksum_crc32(), Some(expected.as_str()));
    assert_eq!(read_body(get).await, bodies.concat());
    target.cleanup().await;
}

#[tokio::test]
async fn completion_in_a_versioned_bucket_returns_an_addressable_version_id() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = target.create_bucket().await;
    target
        .client
        .put_bucket_versioning()
        .bucket(&bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let client = multipart_client(&target);
    let upload_id = initiate(&client, &bucket, "versioned").await;
    let part = upload_part(
        &client,
        &bucket,
        "versioned",
        &upload_id,
        1,
        b"version body".to_vec(),
    )
    .await;
    // Completion returns the created version: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html#API_CompleteMultipartUpload_ResponseSyntax
    let completed = client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key("versioned")
        .upload_id(&upload_id)
        .multipart_upload(completion(vec![part]))
        .send()
        .await
        .unwrap();
    let version_id = completed
        .version_id()
        .expect("versioned completion returns a version ID");
    assert!(!version_id.is_empty());
    assert_ne!(version_id, "null");
    let get = client
        .get_object()
        .bucket(&bucket)
        .key("versioned")
        .version_id(version_id)
        .send()
        .await
        .unwrap();
    assert_eq!(get.version_id(), Some(version_id));
    assert_eq!(read_body(get).await, b"version body");
    target.cleanup().await;
}

fn multipart_client(target: &ConformanceTarget) -> Client {
    Client::from_conf(
        target
            .client
            .config()
            .to_builder()
            .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
            .build(),
    )
}

async fn initiate(client: &Client, bucket: &str, key: &str) -> String {
    client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .server_side_encryption(ServerSideEncryption::Aes256)
        .send()
        .await
        .expect("the multipart request succeeds")
        .upload_id()
        .expect("initiation returns an upload ID")
        .to_owned()
}

async fn upload_part(
    client: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    number: i32,
    body: Vec<u8>,
) -> CompletedPart {
    let uploaded = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(number)
        .body(body.into())
        .send()
        .await
        .expect("the multipart request succeeds");
    CompletedPart::builder()
        .part_number(number)
        .e_tag(uploaded.e_tag().expect("uploaded parts have ETags"))
        .build()
}

fn completion(parts: Vec<CompletedPart>) -> CompletedMultipartUpload {
    CompletedMultipartUpload::builder()
        .set_parts(Some(parts))
        .build()
}

async fn abort(client: &Client, bucket: &str, key: &str, upload_id: &str) {
    client
        .abort_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .send()
        .await
        .expect("the multipart request succeeds");
}

async fn assert_invalid_part(is_part_missing: bool) {
    let mut target = ConformanceTarget::connect().await;
    let bucket = target.create_bucket().await;
    let client = multipart_client(&target);
    let upload_id = initiate(&client, &bucket, "invalid").await;
    let uploaded = upload_part(&client, &bucket, "invalid", &upload_id, 1, b"body".to_vec()).await;
    let invalid = CompletedPart::builder()
        .part_number(if is_part_missing { 2 } else { 1 })
        .e_tag(if is_part_missing {
            uploaded.e_tag().expect("uploaded parts have ETags")
        } else {
            "\"00000000000000000000000000000000\""
        })
        .build();
    // Missing parts and mismatched ETags are InvalidPart: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
    let error = client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key("invalid")
        .upload_id(&upload_id)
        .multipart_upload(completion(vec![invalid]))
        .send()
        .await
        .expect_err("completion rejects an invalid part");
    assert_eq!(
        error_code_and_status(&error),
        (Some("InvalidPart".to_owned()), Some(400))
    );
    abort(&client, &bucket, "invalid", &upload_id).await;
    target.cleanup().await;
}

fn crc32(body: &[u8]) -> u32 {
    let mut checksum = u32::MAX;
    for byte in body {
        checksum ^= u32::from(*byte);
        for _ in 0..8 {
            checksum = (checksum >> 1) ^ (0xedb8_8320 & 0_u32.wrapping_sub(checksum & 1));
        }
    }
    !checksum
}
