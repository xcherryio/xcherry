// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use dexzd_object_store::{InMemoryObjectStore, ObjectStore, WriteCondition, WriteOutcome};
use xxhash_rust::xxh3::Xxh3;

/// Object storage contents, one file per object key.
pub(crate) const OBJECTS_DIRECTORY: &str = "objects";
/// Canonical dump of the committed state the recorded workload produced.
pub(crate) const STATE_DUMP_FILE: &str = "state.txt";
/// Describes the version; not covered by the pinned digest.
pub(crate) const README_FILE: &str = "README.md";
/// Leases name a random holder process and expire; restoring needs none.
pub(crate) const EXCLUDED_KEY_PREFIXES: &[&str] = &["lease/"];
const LISTING_PAGE_SIZE: u32 = 1000;

/// One `testdata/v<N>` directory.
#[derive(Debug, Clone)]
pub(crate) struct TestdataVersion {
    pub(crate) name: String,
    pub(crate) directory: PathBuf,
}

pub(crate) fn testdata_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata")
}

/// Returns every `v<N>` directory under `testdata/`, oldest first.
pub(crate) fn testdata_versions() -> Vec<TestdataVersion> {
    let root = testdata_root();
    let mut versions = BTreeMap::new();
    for entry in fs::read_dir(&root).expect("read the testdata directory") {
        let entry = entry.expect("read a testdata entry");
        let name = entry
            .file_name()
            .into_string()
            .expect("testdata entry names are UTF-8");
        let Some(number) = name.strip_prefix('v') else {
            continue;
        };
        let number: u32 = number
            .parse()
            .unwrap_or_else(|_| panic!("testdata directory {name} is not named v<number>"));
        assert!(
            entry.file_type().expect("read entry type").is_dir(),
            "testdata entry {name} is not a directory"
        );
        versions.insert(
            number,
            TestdataVersion {
                name,
                directory: entry.path(),
            },
        );
    }
    versions.into_values().collect()
}

/// Loads `<version>/objects` into a new in-memory object store.
pub(crate) async fn load_objects(version_directory: &Path) -> InMemoryObjectStore {
    let store = InMemoryObjectStore::new();
    for (key, body) in read_files(&version_directory.join(OBJECTS_DIRECTORY)) {
        let outcome = store
            .put(&key, Bytes::from(body), WriteCondition::CreateOnly)
            .await
            .expect("load a recorded object");
        assert!(
            matches!(outcome, WriteOutcome::Written { .. }),
            "recorded object {key} loads once"
        );
    }
    store
}

/// Writes every object except leases to `<output>/objects`.
pub(crate) async fn write_objects(store: &InMemoryObjectStore, output_directory: &Path) {
    let objects_directory = output_directory.join(OBJECTS_DIRECTORY);
    for (key, body) in recorded_objects(store).await {
        let path = objects_directory.join(&key);
        fs::create_dir_all(path.parent().expect("object paths have a parent"))
            .expect("create an object directory");
        fs::write(&path, body).expect("write a recorded object");
    }
}

/// Returns every object except leases, keyed by object key.
pub(crate) async fn recorded_objects(store: &InMemoryObjectStore) -> BTreeMap<String, Vec<u8>> {
    let mut objects = BTreeMap::new();
    let mut start_after = None;
    loop {
        let page = store
            .list("", start_after.as_deref(), LISTING_PAGE_SIZE)
            .await
            .expect("list recorded objects");
        for object in &page.objects {
            if EXCLUDED_KEY_PREFIXES
                .iter()
                .any(|prefix| object.key.starts_with(prefix))
            {
                continue;
            }
            let content = store
                .get(&object.key)
                .await
                .expect("read a recorded object")
                .expect("a listed object exists");
            objects.insert(object.key.clone(), content.bytes.to_vec());
        }
        match page.next_start_after {
            Some(next) => start_after = Some(next),
            None => return objects,
        }
    }
}

/// Returns the xxh3-128 digest of a version's data files in path order, excluding the README.
pub(crate) fn version_digest(version_directory: &Path) -> String {
    let mut hasher = Xxh3::new();
    for (relative_path, body) in read_files(version_directory) {
        if relative_path == README_FILE {
            continue;
        }
        hasher.update(&(relative_path.len() as u64).to_be_bytes());
        hasher.update(relative_path.as_bytes());
        hasher.update(&(body.len() as u64).to_be_bytes());
        hasher.update(&body);
    }
    format!("{:032x}", hasher.digest128())
}

/// Returns every file below `directory` keyed by its `/`-separated relative path.
pub(crate) fn read_files(directory: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut files = BTreeMap::new();
    collect_files(directory, "", &mut files);
    files
}

fn collect_files(directory: &Path, prefix: &str, files: &mut BTreeMap<String, Vec<u8>>) {
    for entry in fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("read {}: {error}", directory.display()))
    {
        let entry = entry.expect("read a directory entry");
        let name = entry
            .file_name()
            .into_string()
            .expect("testdata file names are UTF-8");
        let relative_path = format!("{prefix}{name}");
        if entry.file_type().expect("read entry type").is_dir() {
            collect_files(&entry.path(), &format!("{relative_path}/"), files);
        } else {
            let body = fs::read(entry.path()).expect("read a testdata file");
            files.insert(relative_path, body);
        }
    }
}
