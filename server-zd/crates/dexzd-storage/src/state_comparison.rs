// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_keyspace::ColumnFamily;
use dexzd_protocol::dexzd::v1::{NodeMeta, PartitionMeta};
use prost::Message;

use crate::{LocalReadView, ScanDirection, StorageError};

/// Store metadata fields whose lifecycle changes during valid snapshot recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CanonicalizedStoreField {
    /// Snapshot restore creates a new store lineage.
    NodeLineageId,
    /// Snapshot restore restarts the local snapshot sequence.
    NodeLastSnapshotNumber,
    /// Log recovery rewrites the applied position in its current epoch.
    PartitionApplied,
    /// Log recovery acquires a new writer epoch.
    PartitionAppliedEpoch,
}

/// The first key or value that differs between two store views.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateDifference {
    /// Column family containing the differing key.
    pub column_family: ColumnFamily,
    /// Differing key, present on at least one side.
    pub key: Vec<u8>,
    /// Value in the continuously running store, if present.
    pub expected: Option<Vec<u8>>,
    /// Value in the recovered store, if present.
    pub actual: Option<Vec<u8>>,
}

impl std::fmt::Display for StateDifference {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{:?}/{} differs: ",
            self.column_family,
            hex(&self.key)
        )?;
        match (&self.expected, &self.actual) {
            (None, Some(actual)) => {
                write!(formatter, "expected absent, actual {} bytes", actual.len())
            }
            (Some(expected), None) => write!(
                formatter,
                "expected {} bytes, actual absent",
                expected.len()
            ),
            (Some(expected), Some(actual)) => {
                if expected == actual {
                    write!(formatter, "metadata encoding is noncanonical")
                } else if let Some(offset) = expected
                    .iter()
                    .zip(actual)
                    .position(|(first, second)| first != second)
                {
                    write!(
                        formatter,
                        "byte {offset} is {:02x} versus {:02x}; lengths {} versus {}",
                        expected[offset],
                        actual[offset],
                        expected.len(),
                        actual.len()
                    )
                } else {
                    write!(
                        formatter,
                        "lengths {} versus {}",
                        expected.len(),
                        actual.len()
                    )
                }
            }
            (None, None) => write!(formatter, "both values absent"),
        }
    }
}

/// A read failure or a key-by-key state mismatch.
#[derive(Debug, thiserror::Error)]
pub enum StateComparisonError {
    /// One of the views could not be read.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// Both views were read, but one key differs.
    #[error("{0}")]
    Difference(StateDifference),
}

/// Compares all column families, returning the first difference; listed metadata fields are canonicalized.
/// Tests and simulations provide the continuously running twin at the same log point.
pub fn compare_store_views(
    expected: &dyn LocalReadView,
    actual: &dyn LocalReadView,
    canonicalized_fields: &[CanonicalizedStoreField],
) -> Result<(), StateComparisonError> {
    for column_family in ColumnFamily::ALL {
        let expected_rows = expected.scan(
            column_family,
            None,
            None,
            ScanDirection::Forward,
            usize::MAX,
        )?;
        let actual_rows = actual.scan(
            column_family,
            None,
            None,
            ScanDirection::Forward,
            usize::MAX,
        )?;
        let mut expected_index = 0;
        let mut actual_index = 0;
        while expected_index < expected_rows.len() || actual_index < actual_rows.len() {
            let expected_row = expected_rows.get(expected_index);
            let actual_row = actual_rows.get(actual_index);
            let key = match (expected_row, actual_row) {
                (Some((expected_key, _)), Some((actual_key, _))) => {
                    expected_key.min(actual_key).clone()
                }
                (Some((expected_key, _)), None) => expected_key.clone(),
                (None, Some((actual_key, _))) => actual_key.clone(),
                (None, None) => break,
            };
            let expected_value =
                expected_row.and_then(|(row_key, value)| (row_key == &key).then_some(value));
            let actual_value =
                actual_row.and_then(|(row_key, value)| (row_key == &key).then_some(value));
            if expected_value.is_some() {
                expected_index += 1;
            }
            if actual_value.is_some() {
                actual_index += 1;
            }
            let expected_original = expected_value.cloned();
            let actual_original = actual_value.cloned();
            let (expected_value, is_expected_lossless) =
                canonicalize_value(column_family, &key, expected_value, canonicalized_fields)?;
            let (actual_value, is_actual_lossless) =
                canonicalize_value(column_family, &key, actual_value, canonicalized_fields)?;
            if !is_expected_lossless || !is_actual_lossless {
                return Err(StateComparisonError::Difference(StateDifference {
                    column_family,
                    key,
                    expected: expected_original,
                    actual: actual_original,
                }));
            }
            if expected_value != actual_value {
                return Err(StateComparisonError::Difference(StateDifference {
                    column_family,
                    key,
                    expected: expected_value,
                    actual: actual_value,
                }));
            }
        }
    }
    Ok(())
}

fn canonicalize_value(
    column_family: ColumnFamily,
    key: &[u8],
    value: Option<&Vec<u8>>,
    canonicalized_fields: &[CanonicalizedStoreField],
) -> Result<(Option<Vec<u8>>, bool), StorageError> {
    let Some(value) = value else {
        return Ok((None, true));
    };
    if canonicalized_fields.is_empty() {
        return Ok((Some(value.clone()), true));
    }
    match column_family {
        ColumnFamily::NodeMeta if key == b"node" => {
            let mut metadata = NodeMeta::decode(value.as_slice())?;
            let is_lossless = metadata.encode_to_vec() == *value;
            for field in canonicalized_fields {
                match field {
                    CanonicalizedStoreField::NodeLineageId => metadata.lineage_id.clear(),
                    CanonicalizedStoreField::NodeLastSnapshotNumber => {
                        metadata.last_snapshot_number = 0;
                    }
                    _ => {}
                }
            }
            Ok((Some(metadata.encode_to_vec()), is_lossless))
        }
        ColumnFamily::PartitionMeta => {
            let mut metadata = PartitionMeta::decode(value.as_slice())?;
            let is_lossless = metadata.encode_to_vec() == *value;
            for field in canonicalized_fields {
                match field {
                    CanonicalizedStoreField::PartitionApplied => metadata.applied = None,
                    CanonicalizedStoreField::PartitionAppliedEpoch => metadata.applied_epoch = 0,
                    _ => {}
                }
            }
            Ok((Some(metadata.encode_to_vec()), is_lossless))
        }
        _ => Ok((Some(value.clone()), true)),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use dexzd_keyspace::ColumnFamily;
    use dexzd_protocol::dexzd::v1::{
        LogPosition, NodeMeta, PartitionMeta, PendingHardDeleteUpload,
    };
    use prost::Message;

    use crate::{InMemoryLocalStore, LocalStore, WriteBatch};

    use super::{CanonicalizedStoreField, StateComparisonError, compare_store_views};

    #[test]
    fn reports_a_different_key_in_every_column_family() {
        for column_family in ColumnFamily::ALL {
            let expected = InMemoryLocalStore::new();
            let actual = InMemoryLocalStore::new();
            let mut batch = WriteBatch::new();
            batch.put(column_family, b"different-key".to_vec(), b"value".to_vec());
            actual.write(batch).expect("write differing key");
            assert!(matches!(
                compare_store_views(&expected, &actual, &[]),
                Err(StateComparisonError::Difference(difference))
                    if difference.column_family == column_family && difference.key == b"different-key"
            ));
        }
    }

    #[test]
    fn compares_every_column_family_and_preserves_unlisted_node_fields() {
        let expected = InMemoryLocalStore::new();
        let actual = InMemoryLocalStore::new();
        let baseline = NodeMeta {
            lineage_id: vec![1],
            last_snapshot_number: 2,
            pending_hard_delete_uploads: Vec::new(),
        };
        expected
            .put_node_meta(&baseline)
            .expect("expected metadata");
        actual
            .put_node_meta(&NodeMeta {
                lineage_id: vec![3],
                last_snapshot_number: 0,
                ..baseline.clone()
            })
            .expect("restored metadata");
        let canonicalized = [
            CanonicalizedStoreField::NodeLineageId,
            CanonicalizedStoreField::NodeLastSnapshotNumber,
        ];
        compare_store_views(&expected, &actual, &canonicalized)
            .expect("selected node fields are canonicalized");
        assert!(
            matches!(compare_store_views(&expected, &actual, &[]), Err(StateComparisonError::Difference(difference)) if difference.column_family == ColumnFamily::NodeMeta)
        );

        let mut batch = WriteBatch::new();
        batch.put(
            ColumnFamily::History,
            b"history-key".to_vec(),
            b"different".to_vec(),
        );
        actual.write(batch).expect("write history");
        assert!(
            matches!(compare_store_views(&expected, &actual, &canonicalized), Err(StateComparisonError::Difference(difference)) if difference.column_family == ColumnFamily::History && difference.key == b"history-key")
        );

        let mut batch = WriteBatch::new();
        batch.delete(ColumnFamily::History, b"history-key".to_vec());
        actual.write(batch).expect("remove history");
        let mut changed = baseline;
        changed.lineage_id = vec![3];
        changed.last_snapshot_number = 0;
        changed
            .pending_hard_delete_uploads
            .push(PendingHardDeleteUpload::default());
        actual
            .put_node_meta(&changed)
            .expect("change noncanonical metadata");
        assert!(
            matches!(compare_store_views(&expected, &actual, &canonicalized), Err(StateComparisonError::Difference(difference)) if difference.column_family == ColumnFamily::NodeMeta && difference.key == b"node")
        );
    }

    #[test]
    fn canonicalizes_only_selected_partition_fields() {
        let expected = InMemoryLocalStore::new();
        let actual = InMemoryLocalStore::new();
        let node_metadata = NodeMeta {
            lineage_id: vec![1],
            ..NodeMeta::default()
        };
        expected
            .put_node_meta(&node_metadata)
            .expect("expected node metadata");
        actual
            .put_node_meta(&node_metadata)
            .expect("actual node metadata");
        let baseline = PartitionMeta {
            partition: 2,
            applied: Some(LogPosition {
                node_id: "first".to_owned(),
                incarnation: 1,
                seq: 2,
            }),
            last_command_timestamp_micros: 42,
            last_behavior_version: 3,
            applied_epoch: 4,
        };
        let mut recovered = baseline.clone();
        recovered.applied = Some(LogPosition {
            node_id: "second".to_owned(),
            incarnation: 1,
            seq: 2,
        });
        recovered.applied_epoch = 5;
        let key = 2_u16.to_be_bytes().to_vec();
        let mut batch = WriteBatch::new();
        batch.put(
            ColumnFamily::PartitionMeta,
            key.clone(),
            baseline.encode_to_vec(),
        );
        expected
            .write(batch)
            .expect("write expected partition metadata");
        let mut batch = WriteBatch::new();
        batch.put(
            ColumnFamily::PartitionMeta,
            key.clone(),
            recovered.encode_to_vec(),
        );
        actual
            .write(batch)
            .expect("write recovered partition metadata");
        let canonicalized = [
            CanonicalizedStoreField::PartitionApplied,
            CanonicalizedStoreField::PartitionAppliedEpoch,
        ];
        assert!(matches!(
            compare_store_views(&expected, &actual, &[]),
            Err(StateComparisonError::Difference(difference))
                if difference.column_family == ColumnFamily::PartitionMeta && difference.key == key
        ));
        compare_store_views(&expected, &actual, &canonicalized)
            .expect("selected partition fields are canonicalized");

        recovered.last_command_timestamp_micros = 43;
        let mut batch = WriteBatch::new();
        batch.put(
            ColumnFamily::PartitionMeta,
            key.clone(),
            recovered.encode_to_vec(),
        );
        actual.write(batch).expect("write changed timestamp");
        assert!(matches!(
            compare_store_views(&expected, &actual, &canonicalized),
            Err(StateComparisonError::Difference(difference))
                if difference.column_family == ColumnFamily::PartitionMeta && difference.key == key
        ));
    }

    #[test]
    fn reports_unknown_partition_metadata_field_with_canonicalization() {
        let expected = InMemoryLocalStore::new();
        let actual = InMemoryLocalStore::new();
        let metadata = PartitionMeta {
            partition: 2,
            ..PartitionMeta::default()
        };
        let key = 2_u16.to_be_bytes().to_vec();
        let mut batch = WriteBatch::new();
        batch.put(
            ColumnFamily::PartitionMeta,
            key.clone(),
            metadata.encode_to_vec(),
        );
        expected.write(batch).expect("write expected metadata");
        let mut bytes = metadata.encode_to_vec();
        bytes.extend_from_slice(&[0x80, 0x06, 0x01]);
        let mut batch = WriteBatch::new();
        batch.put(ColumnFamily::PartitionMeta, key.clone(), bytes);
        actual
            .write(batch)
            .expect("write metadata with unknown field");
        assert!(matches!(
            compare_store_views(
                &expected,
                &actual,
                &[CanonicalizedStoreField::PartitionApplied],
            ),
            Err(StateComparisonError::Difference(difference))
                if difference.column_family == ColumnFamily::PartitionMeta && difference.key == key
        ));
    }

    #[test]
    fn reports_noncanonical_node_metadata_encoding() {
        let expected = InMemoryLocalStore::new();
        let actual = InMemoryLocalStore::new();
        let metadata = NodeMeta {
            lineage_id: vec![1],
            ..NodeMeta::default()
        };
        expected
            .put_node_meta(&metadata)
            .expect("expected metadata");
        let mut bytes = metadata.encode_to_vec();
        bytes.extend_from_slice(&[0x10, 0x80, 0x00]);
        let mut batch = WriteBatch::new();
        batch.put(ColumnFamily::NodeMeta, b"node".to_vec(), bytes);
        actual.write(batch).expect("write noncanonical metadata");
        assert!(matches!(
            compare_store_views(
                &expected,
                &actual,
                &[CanonicalizedStoreField::NodeLineageId],
            ),
            Err(StateComparisonError::Difference(difference))
                if difference.column_family == ColumnFamily::NodeMeta && difference.key == b"node"
        ));
    }
}
