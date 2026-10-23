// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! SigV4 validation through SDK uploads and deliberately malformed signed HTTP requests.

use std::collections::BTreeMap;

use aws_sdk_s3::primitives::ByteStream;
use hmac::{Hmac, Mac};
use serde_json::json;
use sha2::{Digest, Sha256};

use super::support::{RunningServer, client};

#[tokio::test]
async fn strict_accepts_sdk_memory_file_and_encoded_keys_and_rejects_wrong_credentials() {
    let server = RunningServer::start(&["--signature-verification", "strict"]).await;
    for key in ["memory", "space +percent%/目录", "a//b/../c"] {
        server.put(key, "signed object").await;
        assert_eq!(server.get(key).await, b"signed object");
    }
    let path = server.directory.path().join("stream-body");
    let bytes = vec![42_u8; 200_000];
    tokio::fs::write(&path, &bytes)
        .await
        .expect("the controls integration fixture succeeds");
    let config = server
        .client
        .config()
        .to_builder()
        .request_checksum_calculation(aws_sdk_s3::config::RequestChecksumCalculation::WhenSupported)
        .build();
    let streamed = aws_sdk_s3::Client::from_conf(config);
    streamed
        .put_object()
        .bucket("testing")
        .key("streamed")
        .body(
            ByteStream::from_path(&path)
                .await
                .expect("the controls integration fixture succeeds"),
        )
        .send()
        .await
        .expect("the controls integration fixture succeeds");
    assert_eq!(server.get("streamed").await, bytes);
    server
        .client
        .list_objects_v2()
        .bucket("testing")
        .prefix("space +percent%")
        .max_keys(1)
        .send()
        .await
        .expect("the controls integration fixture succeeds");
    let wrong_secret = client(&server.server, "s3mock", "wrong");
    let error = wrong_secret
        .put_object()
        .bucket("testing")
        .key("rejected")
        .body("bad".as_bytes().to_vec().into())
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error
            .raw_response()
            .expect("the controls integration fixture succeeds")
            .status()
            .as_u16(),
        403
    );
    use aws_sdk_s3::error::ProvideErrorMetadata;
    assert_eq!(
        error
            .as_service_error()
            .expect("the controls integration fixture succeeds")
            .code(),
        Some("SignatureDoesNotMatch")
    );
    assert_eq!(
        server
            .client
            .head_object()
            .bucket("testing")
            .key("rejected")
            .send()
            .await
            .unwrap_err()
            .raw_response()
            .expect("the controls integration fixture succeeds")
            .status()
            .as_u16(),
        404
    );
    let wrong_key = client(&server.server, "wrong", "s3mock");
    let error = wrong_key.list_buckets().send().await.unwrap_err();
    assert_eq!(
        error
            .as_service_error()
            .expect("the controls integration fixture succeeds")
            .code(),
        Some("AccessDenied")
    );
    assert_eq!(server.admin("GET", "counters", json!(null)).await.0, 200);
    server.server.shutdown().await;
}

#[tokio::test]
async fn strict_rejects_unsigned_and_presigned_requests_and_changed_plain_payload() {
    let server = RunningServer::start(&["--signature-verification", "strict"]).await;
    assert_status(&server.raw("GET", "/", "", b"").await, 403);
    assert_status(
        &server
            .raw("GET", "/?X-Amz-Algorithm=AWS4-HMAC-SHA256", "", b"")
            .await,
        501,
    );
    let headers = signed_headers(
        &server,
        "PUT",
        "/testing/plain",
        &hash(b"original"),
        BTreeMap::new(),
    )
    .0;
    assert_status(
        &server
            .raw("PUT", "/testing/plain", &headers, b"modified")
            .await,
        403,
    );
    let headers = signed_headers(
        &server,
        "PUT",
        "/testing/plain",
        &hash(b"original"),
        BTreeMap::new(),
    )
    .0;
    assert_status(
        &server
            .raw("PUT", "/testing/plain", &headers, b"original")
            .await,
        200,
    );
    assert_eq!(server.get("plain").await, b"original");
    let headers = signed_headers(
        &server,
        "PUT",
        "/testing/encoded%2Fslash",
        &hash(b"encoded"),
        BTreeMap::new(),
    )
    .0;
    assert_status(
        &server
            .raw("PUT", "/testing/encoded%2Fslash", &headers, b"encoded")
            .await,
        200,
    );
    assert_eq!(server.get("encoded/slash").await, b"encoded");
    server.server.shutdown().await;
}

#[tokio::test]
async fn signed_chunks_and_trailers_are_verified_before_commit() {
    let server = RunningServer::start(&["--signature-verification", "strict"]).await;
    for with_trailer in [false, true] {
        let path = if with_trailer {
            "/testing/chunk-trailer"
        } else {
            "/testing/chunk"
        };
        let (headers, body) = signed_stream(&server, path, with_trailer);
        assert_status(&server.raw("PUT", path, &headers, &body).await, 200);
        assert_eq!(
            server
                .get(
                    path.strip_prefix("/testing/")
                        .expect("the controls integration fixture succeeds")
                )
                .await,
            b"hello"
        );
        let mut corrupted = body.clone();
        let start = corrupted
            .windows(5)
            .position(|window| window == b"hello")
            .expect("the controls integration fixture succeeds");
        corrupted[start] ^= 1;
        assert_status(&server.raw("PUT", path, &headers, &corrupted).await, 403);
        let mut corrupted = body.clone();
        let signature = corrupted
            .windows(16)
            .position(|window| window == b"chunk-signature=")
            .expect("the controls integration fixture succeeds")
            + 16;
        corrupted[signature] = if corrupted[signature] == b'0' {
            b'1'
        } else {
            b'0'
        };
        assert_status(&server.raw("PUT", path, &headers, &corrupted).await, 403);
        if with_trailer {
            let mut corrupted = body.clone();
            let trailer = corrupted
                .windows(b"x-amz-trailer-signature:".len())
                .position(|window| window == b"x-amz-trailer-signature:")
                .expect("the controls integration fixture succeeds")
                + b"x-amz-trailer-signature:".len();
            corrupted[trailer] = if corrupted[trailer] == b'0' {
                b'1'
            } else {
                b'0'
            };
            assert_status(&server.raw("PUT", path, &headers, &corrupted).await, 403);
            let truncated = body
                .windows(b"x-amz-trailer-signature:".len())
                .position(|window| window == b"x-amz-trailer-signature:")
                .expect("the controls integration fixture succeeds");
            let mut missing = body[..truncated].to_vec();
            missing.extend_from_slice(b"\r\n");
            assert_status(&server.raw("PUT", path, &headers, &missing).await, 403);
        }
    }
    assert_eq!(server.get("chunk").await, b"hello");
    assert_eq!(server.get("chunk-trailer").await, b"hello");
    server.server.shutdown().await;
}

fn assert_status(response: &[u8], expected: u16) {
    let text = String::from_utf8_lossy(response);
    assert_eq!(
        text[9..12]
            .parse::<u16>()
            .expect("the controls integration fixture succeeds"),
        expected,
        "{text}"
    );
}

const TIMESTAMP: &str = "20261023T000000Z";
const SCOPE: &str = "20261023/us-east-1/s3/aws4_request";

fn signed_headers(
    server: &RunningServer,
    method: &str,
    path: &str,
    payload_hash: &str,
    mut headers: BTreeMap<String, String>,
) -> (String, String) {
    headers.insert("host".into(), server.server.local_address().to_string());
    headers.insert("x-amz-date".into(), TIMESTAMP.into());
    headers.insert("x-amz-content-sha256".into(), payload_hash.into());
    let signed_headers = headers.keys().cloned().collect::<Vec<_>>().join(";");
    let canonical_headers = headers
        .iter()
        .map(|(name, value)| format!("{name}:{value}\n"))
        .collect::<String>();
    let canonical =
        format!("{method}\n{path}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}");
    let signature = sign(&format!(
        "AWS4-HMAC-SHA256\n{TIMESTAMP}\n{SCOPE}\n{}",
        hash(canonical.as_bytes())
    ));
    let mut output = headers
        .iter()
        .filter(|(name, _)| name.as_str() != "host")
        .map(|(name, value)| format!("{name}: {value}\r\n"))
        .collect::<String>();
    output.push_str(&format!("authorization: AWS4-HMAC-SHA256 Credential=s3mock/{SCOPE}, SignedHeaders={signed_headers}, Signature={signature}\r\n"));
    (output, signature)
}

fn signed_stream(server: &RunningServer, path: &str, with_trailer: bool) -> (String, Vec<u8>) {
    let marker = if with_trailer {
        "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
    } else {
        "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
    };
    let mut extra = BTreeMap::from([
        ("content-encoding".into(), "aws-chunked".into()),
        ("x-amz-decoded-content-length".into(), "5".into()),
    ]);
    if with_trailer {
        extra.insert("x-amz-trailer".into(), "x-amz-checksum-crc32".into());
    }
    let (headers, mut previous) = signed_headers(server, "PUT", path, marker, extra);
    let mut body = Vec::new();
    for bytes in [b"hello".as_slice(), b"".as_slice()] {
        let signature = sign(&format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{TIMESTAMP}\n{SCOPE}\n{previous}\n{}\n{}",
            hash(b""),
            hash(bytes)
        ));
        body.extend_from_slice(
            format!("{:x};chunk-signature={signature}\r\n", bytes.len()).as_bytes(),
        );
        if !bytes.is_empty() {
            body.extend_from_slice(bytes);
            body.extend_from_slice(b"\r\n");
        }
        previous = signature;
    }
    if with_trailer {
        let canonical = "x-amz-checksum-crc32:NhCmhg==\n";
        let signature = sign(&format!(
            "AWS4-HMAC-SHA256-TRAILER\n{TIMESTAMP}\n{SCOPE}\n{previous}\n{}",
            hash(canonical.as_bytes())
        ));
        body.extend_from_slice(
            format!("x-amz-checksum-crc32:NhCmhg==\r\nx-amz-trailer-signature:{signature}\r\n")
                .as_bytes(),
        );
    }
    body.extend_from_slice(b"\r\n");
    (headers, body)
}

fn hash(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn sign(message: &str) -> String {
    let key = hmac(b"AWS4s3mock", b"20261023");
    let key = hmac(&key, b"us-east-1");
    let key = hmac(&key, b"s3");
    let key = hmac(&key, b"aws4_request");
    hex::encode(hmac(&key, message.as_bytes()))
}

fn hmac(key: &[u8], message: &[u8]) -> Vec<u8> {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(key).expect("the controls integration fixture succeeds");
    mac.update(message);
    mac.finalize().into_bytes().to_vec()
}
