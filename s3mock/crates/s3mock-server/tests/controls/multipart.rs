// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Strict signatures, observations, and faults on multipart and bucket configuration requests.

use aws_sdk_s3::types::{
    CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier, ObjectLockConfiguration,
    ObjectLockEnabled,
};
use serde_json::json;

use super::support::RunningServer;

#[tokio::test]
async fn multipart_operations_use_strict_signatures_opaque_etags_and_testing_controls() {
    let server = RunningServer::start(&[
        "--signature-verification",
        "strict",
        "--etag-mode",
        "opaque",
    ])
    .await;
    let created = server
        .client
        .create_multipart_upload()
        .bucket("testing")
        .key("multipart/object")
        .send()
        .await
        .expect("multipart initiation succeeds");
    let upload_id = created.upload_id().expect("the upload has an ID");
    let part = server
        .client
        .upload_part()
        .bucket("testing")
        .key("multipart/object")
        .upload_id(upload_id)
        .part_number(1)
        .body(b"part body".to_vec().into())
        .send()
        .await
        .expect("a signed part uploads");
    let parts = server
        .client
        .list_parts()
        .bucket("testing")
        .key("multipart/object")
        .upload_id(upload_id)
        .send()
        .await
        .expect("parts are listed");
    assert_eq!(parts.parts().len(), 1);
    assert_eq!(
        server
            .client
            .list_multipart_uploads()
            .bucket("testing")
            .send()
            .await
            .expect("uploads are listed")
            .uploads()
            .len(),
        1
    );
    server
        .rules(json!([{"operation":"UploadPart","action":{"type":"error","status":503}}]))
        .await;
    let failed = server
        .client
        .upload_part()
        .bucket("testing")
        .key("multipart/object")
        .upload_id(upload_id)
        .part_number(1)
        .body(b"replacement".to_vec().into())
        .send()
        .await
        .expect_err("the injected part upload fails");
    assert_eq!(
        failed
            .raw_response()
            .expect("the failure has a response")
            .status()
            .as_u16(),
        503
    );
    server
        .rules(json!([{"operation":"UploadPart","action":{"type":"commit_then_drop"}}]))
        .await;
    let dropped_part = server
        .client
        .upload_part()
        .bucket("testing")
        .key("multipart/object")
        .upload_id(upload_id)
        .part_number(1)
        .body(b"part body".to_vec().into())
        .send()
        .await
        .expect_err("the committed part loses its response");
    assert!(dropped_part.as_service_error().is_none());
    let recovered_parts = server
        .client
        .list_parts()
        .bucket("testing")
        .key("multipart/object")
        .upload_id(upload_id)
        .send()
        .await
        .expect("the committed part remains visible");
    assert_eq!(recovered_parts.parts().len(), 1);
    assert_eq!(recovered_parts.parts()[0].e_tag(), part.e_tag());
    server.rules(json!([{"operation":"CompleteMultipartUpload","has_conditional_header":true,"action":{"type":"commit_then_drop"}}])).await;
    let completed = server
        .client
        .complete_multipart_upload()
        .bucket("testing")
        .key("multipart/object")
        .upload_id(upload_id)
        .if_none_match("*")
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .part_number(1)
                        .e_tag(part.e_tag().expect("the part has an ETag"))
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect_err("a committed completion loses its response");
    assert!(completed.as_service_error().is_none());
    server.admin("DELETE", "faults", json!(null)).await;
    let head = server
        .client
        .head_object()
        .bucket("testing")
        .key("multipart/object")
        .send()
        .await
        .expect("the completion committed");
    assert!(
        head.e_tag()
            .expect("the object has an ETag")
            .starts_with("\"opaque-")
    );
    assert_eq!(server.get("multipart/object").await, b"part body");
    let read = server
        .client
        .get_object()
        .bucket("testing")
        .key("multipart/object")
        .if_match(head.e_tag().expect("the object has an ETag"))
        .send()
        .await
        .expect("opaque If-Match succeeds");
    assert_eq!(
        read.body
            .collect()
            .await
            .expect("the body streams")
            .into_bytes()
            .as_ref(),
        b"part body"
    );
    let abandoned = server
        .client
        .create_multipart_upload()
        .bucket("testing")
        .key("abort")
        .send()
        .await
        .expect("another upload initiates");
    server
        .client
        .abort_multipart_upload()
        .bucket("testing")
        .key("abort")
        .upload_id(abandoned.upload_id().expect("the upload has an ID"))
        .send()
        .await
        .expect("abort succeeds");
    let log = server.requests(13).await;
    let (_, counters) = server.admin("GET", "counters", json!(null)).await;
    for operation in [
        "CreateMultipartUpload",
        "UploadPart",
        "ListParts",
        "ListMultipartUploads",
        "CompleteMultipartUpload",
        "AbortMultipartUpload",
    ] {
        assert!(
            counters["counters"]["by_operation"][operation]["requests"]
                .as_u64()
                .expect("the operation has a counter")
                > 0
        );
        assert!(
            log["requests"]
                .as_array()
                .expect("the log is an array")
                .iter()
                .any(|request| request["operation"] == operation)
        );
    }
    server.server.shutdown().await;
}

#[tokio::test]
async fn batch_delete_and_object_lock_requests_use_strict_signatures_and_observations() {
    let server = RunningServer::start(&["--signature-verification", "strict"]).await;
    server
        .client
        .create_bucket()
        .bucket("locked")
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .expect("the lock bucket is created");
    let configuration = ObjectLockConfiguration::builder()
        .object_lock_enabled(ObjectLockEnabled::Enabled)
        .build();
    server
        .client
        .put_object_lock_configuration()
        .bucket("locked")
        .object_lock_configuration(configuration.clone())
        .send()
        .await
        .expect("signed lock configuration is stored");
    let retrieved = server
        .client
        .get_object_lock_configuration()
        .bucket("locked")
        .send()
        .await
        .expect("lock configuration is read");
    assert_eq!(retrieved.object_lock_configuration(), Some(&configuration));
    server.put("delete-target", "body").await;
    let deletion = Delete::builder()
        .objects(
            ObjectIdentifier::builder()
                .key("delete-target")
                .build()
                .expect("the identifier is valid"),
        )
        .build()
        .expect("the delete request is valid");
    server
        .rules(json!([{"operation":"DeleteObjects","action":{"type":"fail_before_commit"}}]))
        .await;
    assert_eq!(
        server
            .client
            .delete_objects()
            .bucket("testing")
            .delete(deletion.clone())
            .send()
            .await
            .expect_err("the injected delete fails")
            .raw_response()
            .expect("the error has a response")
            .status()
            .as_u16(),
        500
    );
    assert_eq!(server.get("delete-target").await, b"body");
    server.admin("DELETE", "faults", json!(null)).await;
    let result = server
        .client
        .delete_objects()
        .bucket("testing")
        .delete(deletion)
        .send()
        .await
        .expect("the signed batch delete succeeds");
    assert!(result.errors().is_empty());
    assert_eq!(
        server
            .client
            .head_object()
            .bucket("testing")
            .key("delete-target")
            .send()
            .await
            .expect_err("the batch deleted the object")
            .raw_response()
            .expect("the error has a response")
            .status()
            .as_u16(),
        404
    );
    let log = server.requests(8).await;
    for operation in [
        "PutObjectLockConfiguration",
        "GetObjectLockConfiguration",
        "DeleteObjects",
    ] {
        assert!(
            log["requests"]
                .as_array()
                .expect("the log is an array")
                .iter()
                .any(|request| request["operation"] == operation)
        );
    }
    let (_, counters) = server.admin("GET", "counters", json!(null)).await;
    assert_eq!(
        counters["counters"]["by_operation"]["PutObjectLockConfiguration"]["requests"],
        1
    );
    assert_eq!(
        counters["counters"]["by_operation"]["GetObjectLockConfiguration"]["requests"],
        1
    );
    assert_eq!(
        counters["counters"]["by_operation"]["DeleteObjects"]["requests"],
        2
    );
    server.server.shutdown().await;
}
