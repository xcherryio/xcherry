// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! s3mock-specific behavior: committed data survives a server and store restart.

use crate::objects::put_text;
use crate::support::{ConformanceTarget, read_body};

#[tokio::test]
async fn committed_objects_survive_a_restart() {
    let mut target = ConformanceTarget::connect().await;
    if !target.is_s3mock() {
        target.cleanup().await;
        return;
    }
    let bucket = target.create_bucket().await;
    put_text(&target, &bucket, "durable", "survives").await;
    target.restart_s3mock().await;
    let get = target
        .client
        .get_object()
        .bucket(&bucket)
        .key("durable")
        .send()
        .await
        .unwrap();
    assert_eq!(read_body(get).await, b"survives");
    target.cleanup().await;
}
