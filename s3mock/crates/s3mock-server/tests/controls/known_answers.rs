// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Fixed signatures from AWS documentation and the independent review's Python signer.

use super::support::RunningServer;

#[tokio::test]
async fn aws_streaming_known_answer_and_minio_trailer_framing() {
    let server = RunningServer::start(&[
        "--signature-verification",
        "strict",
        "--access-key-id",
        "AKIAIOSFODNN7EXAMPLE",
        "--secret-access-key",
        "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    ])
    .await;
    let client = super::support::client(
        &server.server,
        "AKIAIOSFODNN7EXAMPLE",
        "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    );
    client
        .create_bucket()
        .bucket("examplebucket")
        .send()
        .await
        .expect("fixture bucket creation succeeds");
    let fixtures: serde_json::Value = serde_json::from_str(include_str!("fixtures/requests.json"))
        .expect("fixed requests are valid JSON");
    let headers = fixtures["aws_headers"].as_str().expect("AWS headers exist");
    let mut body = Vec::new();
    body.extend_from_slice(b"10000;chunk-signature=ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648\r\n");
    body.extend(std::iter::repeat_n(b'a', 65536));
    body.extend_from_slice(b"\r\n");
    body.extend_from_slice(
        b"400;chunk-signature=0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497\r\n",
    );
    body.extend(std::iter::repeat_n(b'a', 1024));
    body.extend_from_slice(b"\r\n");
    body.extend_from_slice(
        b"0;chunk-signature=b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9\r\n",
    );

    for ending in [b"\r\n".as_slice()] {
        let mut encoded = body.clone();
        encoded.extend_from_slice(ending);
        let response = server
            .raw_with_host(
                "PUT",
                "/examplebucket/chunkObject.txt",
                headers,
                &encoded,
                "s3.amazonaws.com",
            )
            .await;
        assert!(
            response.starts_with(b"HTTP/1.1 200"),
            "{}",
            String::from_utf8_lossy(&response)
        );
    }
    let response = server
        .raw_with_host(
            "PUT",
            "/examplebucket/minio",
            fixtures["minio_headers"]
                .as_str()
                .expect("minio headers exist"),
            fixtures["minio_body"]
                .as_str()
                .expect("minio body exists")
                .as_bytes(),
            "s3.amazonaws.com",
        )
        .await;
    assert!(
        response.starts_with(b"HTTP/1.1 200"),
        "{}",
        String::from_utf8_lossy(&response)
    );

    let mut malformed = body.clone();
    malformed.extend_from_slice(b"xx");
    let response = server
        .raw_with_host(
            "PUT",
            "/examplebucket/chunkObject.txt",
            headers,
            &malformed,
            "s3.amazonaws.com",
        )
        .await;
    assert!(response.starts_with(b"HTTP/1.1 400"));
    assert!(String::from_utf8_lossy(&response).contains("IncompleteBody"));
    for (key, expected) in [
        ("chunkObject.txt", vec![b'a'; 66560]),
        ("minio", b"hello".to_vec()),
    ] {
        let output = client
            .get_object()
            .bucket("examplebucket")
            .key(key)
            .send()
            .await
            .expect("verified upload exists");
        assert_eq!(
            output
                .body
                .collect()
                .await
                .expect("body is readable")
                .into_bytes()
                .as_ref(),
            expected
        );
    }
    server.server.shutdown().await;
}
