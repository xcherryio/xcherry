// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Object writes and reads: bodies, metadata, checksums, ranges, and key encoding.

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{ChecksumAlgorithm, ChecksumMode};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use md5::{Digest, Md5};

use crate::support::{ConformanceTarget, error_code_and_status, read_body};

/// Larger than one aws-chunked chunk, so streamed uploads span several chunks.
const STREAMED_OBJECT_BYTES: usize = 300 * 1024;

#[tokio::test]
async fn in_memory_put_round_trips_body_metadata_and_etag() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = target.create_bucket().await;
    let body = b"hello s3mock".to_vec();
    let put = target
        .client
        .put_object()
        .bucket(&bucket)
        .key("greeting.txt")
        .body(ByteStream::from(body.clone()))
        .content_type("text/plain")
        .cache_control("no-cache")
        .metadata("color", "blue")
        .send()
        .await
        .unwrap();
    let expected_etag = format!("\"{}\"", hex_md5(&body));
    assert_eq!(put.e_tag(), Some(expected_etag.as_str()));
    assert!(
        put.checksum_crc32().is_some(),
        "the SDK sends CRC32 and S3 echoes it"
    );

    let get = target
        .client
        .get_object()
        .bucket(&bucket)
        .key("greeting.txt")
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .unwrap();
    assert_eq!(get.e_tag(), Some(expected_etag.as_str()));
    assert_eq!(get.content_type(), Some("text/plain"));
    assert_eq!(get.cache_control(), Some("no-cache"));
    assert_eq!(
        get.content_length(),
        Some(i64::try_from(body.len()).unwrap())
    );
    assert_eq!(
        get.metadata()
            .and_then(|metadata| metadata.get("color"))
            .map(String::as_str),
        Some("blue")
    );
    assert_eq!(get.checksum_crc32(), put.checksum_crc32());
    assert_eq!(read_body(get).await, body);

    let head = target
        .client
        .head_object()
        .bucket(&bucket)
        .key("greeting.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(
        head.content_length(),
        Some(i64::try_from(body.len()).unwrap())
    );
    target.cleanup().await;
}

#[tokio::test]
async fn streamed_file_put_uses_aws_chunked_with_trailing_checksum() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = target.create_bucket().await;
    let content: Vec<u8> = (0..STREAMED_OBJECT_BYTES)
        .map(|index| (index % 251) as u8)
        .collect();
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), &content).unwrap();
    let put = target
        .client
        .put_object()
        .bucket(&bucket)
        .key("streamed.bin")
        .body(ByteStream::from_path(file.path()).await.unwrap())
        .checksum_algorithm(ChecksumAlgorithm::Crc32C)
        .send()
        .await
        .unwrap();
    assert!(put.checksum_crc32_c().is_some());
    let get = target
        .client
        .get_object()
        .bucket(&bucket)
        .key("streamed.bin")
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .unwrap();
    assert_eq!(get.checksum_crc32_c(), put.checksum_crc32_c());
    assert_eq!(read_body(get).await, content);
    target.cleanup().await;
}

#[tokio::test]
async fn mismatched_digests_reject_the_upload() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = target.create_bucket().await;
    let wrong_md5 = BASE64.encode(Md5::digest(b"something else"));
    let bad_md5 = target
        .client
        .put_object()
        .bucket(&bucket)
        .key("k")
        .body(ByteStream::from_static(b"payload"))
        .content_md5(wrong_md5)
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error_code_and_status(&bad_md5),
        (Some("BadDigest".to_owned()), Some(400))
    );
    let bad_checksum = target
        .client
        .put_object()
        .bucket(&bucket)
        .key("k")
        .body(ByteStream::from_static(b"payload"))
        .checksum_crc32("AAAAAA==")
        .send()
        .await
        .unwrap_err();
    assert_eq!(error_code_and_status(&bad_checksum).1, Some(400));
    let missing = target
        .client
        .head_object()
        .bucket(&bucket)
        .key("k")
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error_code_and_status(&missing).1,
        Some(404),
        "rejected uploads leave no object"
    );
    target.cleanup().await;
}

#[tokio::test]
async fn ranges_return_partial_content() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = target.create_bucket().await;
    put_text(&target, &bucket, "digits", "0123456789").await;
    let read_range = |range: &'static str| {
        target
            .client
            .get_object()
            .bucket(&bucket)
            .key("digits")
            .range(range)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
    };
    let middle = read_range("bytes=2-5").await.unwrap();
    assert_eq!(middle.content_range(), Some("bytes 2-5/10"));
    assert_eq!(read_body(middle).await, b"2345");
    let suffix = read_range("bytes=-3").await.unwrap();
    assert_eq!(read_body(suffix).await, b"789");
    let open_ended = read_range("bytes=7-").await.unwrap();
    assert_eq!(read_body(open_ended).await, b"789");
    let clamped = read_range("bytes=8-100").await.unwrap();
    assert_eq!(clamped.content_range(), Some("bytes 8-9/10"));
    let unsatisfiable = read_range("bytes=10-20").await.unwrap_err();
    assert_eq!(
        error_code_and_status(&unsatisfiable),
        (Some("InvalidRange".to_owned()), Some(416))
    );
    target.cleanup().await;
}

#[tokio::test]
async fn keys_with_reserved_characters_round_trip() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = target.create_bucket().await;
    let keys = [
        "a b",
        "a+b",
        "a%2Fb",
        "dir//double",
        "unicode/\u{e9}\u{4e2d}",
        "query?x=1&y=2",
        "hash#tag",
        "~tilde",
    ];
    for key in keys {
        put_text(&target, &bucket, key, key).await;
    }
    let listed = target
        .client
        .list_objects_v2()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();
    let mut expected: Vec<&str> = keys.to_vec();
    expected.sort_unstable();
    let listed_keys: Vec<&str> = listed
        .contents()
        .iter()
        .filter_map(|object| object.key())
        .collect();
    assert_eq!(listed_keys, expected);
    for key in keys {
        let get = target
            .client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(read_body(get).await, key.as_bytes());
    }
    target.cleanup().await;
}

#[tokio::test]
async fn deleting_a_missing_key_succeeds() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = target.create_bucket().await;
    target
        .client
        .delete_object()
        .bucket(&bucket)
        .key("never-written")
        .send()
        .await
        .unwrap();
    put_text(&target, &bucket, "k", "v").await;
    target
        .client
        .delete_object()
        .bucket(&bucket)
        .key("k")
        .send()
        .await
        .unwrap();
    let missing = target
        .client
        .get_object()
        .bucket(&bucket)
        .key("k")
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error_code_and_status(&missing),
        (Some("NoSuchKey".to_owned()), Some(404))
    );
    target.cleanup().await;
}

pub(crate) async fn put_text(
    target: &ConformanceTarget,
    bucket: &str,
    key: &str,
    text: &str,
) -> String {
    target
        .client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(text.as_bytes().to_vec()))
        .send()
        .await
        .expect("the test object upload succeeds")
        .e_tag()
        .expect("PutObject returns an ETag")
        .to_owned()
}

fn hex_md5(data: &[u8]) -> String {
    Md5::digest(data)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
