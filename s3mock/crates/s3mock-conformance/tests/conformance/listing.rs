// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! ListObjectsV2: prefixes, delimiters, pagination, and start-after.

use crate::objects::put_text;
use crate::support::ConformanceTarget;

const KEYS: [&str; 7] = [
    "a.txt",
    "b/1.txt",
    "b/2.txt",
    "b/c/3.txt",
    "c.txt",
    "d/4.txt",
    "e.txt",
];

async fn bucket_with_keys(target: &mut ConformanceTarget) -> String {
    let bucket = target.create_bucket().await;
    for key in KEYS {
        put_text(target, &bucket, key, key).await;
    }
    bucket
}

#[tokio::test]
async fn delimiter_folds_common_prefixes() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = bucket_with_keys(&mut target).await;
    let root = target
        .client
        .list_objects_v2()
        .bucket(&bucket)
        .delimiter("/")
        .send()
        .await
        .unwrap();
    let keys: Vec<&str> = root
        .contents()
        .iter()
        .filter_map(|object| object.key())
        .collect();
    let prefixes: Vec<&str> = root
        .common_prefixes()
        .iter()
        .filter_map(|prefix| prefix.prefix())
        .collect();
    assert_eq!(keys, ["a.txt", "c.txt", "e.txt"]);
    assert_eq!(prefixes, ["b/", "d/"]);
    assert_eq!(root.key_count(), Some(5));

    let nested = target
        .client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("b/")
        .delimiter("/")
        .send()
        .await
        .unwrap();
    let keys: Vec<&str> = nested
        .contents()
        .iter()
        .filter_map(|object| object.key())
        .collect();
    let prefixes: Vec<&str> = nested
        .common_prefixes()
        .iter()
        .filter_map(|prefix| prefix.prefix())
        .collect();
    assert_eq!(keys, ["b/1.txt", "b/2.txt"]);
    assert_eq!(prefixes, ["b/c/"]);
    target.cleanup().await;
}

#[tokio::test]
async fn pagination_visits_every_entry_once() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = bucket_with_keys(&mut target).await;
    let mut seen = Vec::new();
    let mut continuation_token = None;
    loop {
        let page = target
            .client
            .list_objects_v2()
            .bucket(&bucket)
            .delimiter("/")
            .max_keys(2)
            .set_continuation_token(continuation_token.take())
            .send()
            .await
            .unwrap();
        assert!(page.key_count().unwrap_or_default() <= 2);
        seen.extend(
            page.contents()
                .iter()
                .filter_map(|object| object.key())
                .map(str::to_owned),
        );
        seen.extend(
            page.common_prefixes()
                .iter()
                .filter_map(|prefix| prefix.prefix())
                .map(str::to_owned),
        );
        if page.is_truncated() != Some(true) {
            break;
        }
        continuation_token = page.next_continuation_token().map(str::to_owned);
        assert!(
            continuation_token.is_some(),
            "truncated pages carry a continuation token"
        );
    }
    seen.sort_unstable();
    assert_eq!(seen, ["a.txt", "b/", "c.txt", "d/", "e.txt"]);
    target.cleanup().await;
}

#[tokio::test]
async fn start_after_skips_earlier_keys() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = bucket_with_keys(&mut target).await;
    let page = target
        .client
        .list_objects_v2()
        .bucket(&bucket)
        .start_after("b/2.txt")
        .send()
        .await
        .unwrap();
    let keys: Vec<&str> = page
        .contents()
        .iter()
        .filter_map(|object| object.key())
        .collect();
    assert_eq!(keys, ["b/c/3.txt", "c.txt", "d/4.txt", "e.txt"]);
    target.cleanup().await;
}

#[tokio::test]
async fn start_after_inside_a_folded_directory_skips_that_prefix() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = bucket_with_keys(&mut target).await;
    let page = target
        .client
        .list_objects_v2()
        .bucket(&bucket)
        .delimiter("/")
        .start_after("b/1.txt")
        .send()
        .await
        .unwrap();
    let keys: Vec<&str> = page
        .contents()
        .iter()
        .filter_map(|object| object.key())
        .collect();
    let prefixes: Vec<&str> = page
        .common_prefixes()
        .iter()
        .filter_map(|prefix| prefix.prefix())
        .collect();
    assert_eq!(keys, ["c.txt", "e.txt"]);
    assert_eq!(
        prefixes,
        ["d/"],
        "S3 returns only common prefixes after StartAfter"
    );
    target.cleanup().await;
}

#[tokio::test]
async fn version_pagination_advances_past_a_common_prefix_boundary() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = bucket_with_keys(&mut target).await;
    let mut seen = Vec::new();
    let (mut key_marker, mut version_id_marker) = (None, None);
    for _ in 0..KEYS.len() + 1 {
        let page = target
            .client
            .list_object_versions()
            .bucket(&bucket)
            .delimiter("/")
            .max_keys(1)
            .set_key_marker(key_marker.take())
            .set_version_id_marker(version_id_marker.take())
            .send()
            .await
            .unwrap();
        seen.extend(
            page.versions()
                .iter()
                .filter_map(|version| version.key())
                .map(str::to_owned),
        );
        seen.extend(
            page.common_prefixes()
                .iter()
                .filter_map(|prefix| prefix.prefix())
                .map(str::to_owned),
        );
        if page.is_truncated() != Some(true) {
            seen.sort_unstable();
            assert_eq!(seen, ["a.txt", "b/", "c.txt", "d/", "e.txt"]);
            target.cleanup().await;
            return;
        }
        key_marker = page.next_key_marker().map(str::to_owned);
        version_id_marker = page.next_version_id_marker().map(str::to_owned);
    }
    panic!("version pagination did not terminate: {seen:?}");
}
