// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use crate::{Attribute, AttributeIndex, AttributeMap, Channel, ChannelMap, Stream};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
/// Declares the Attributes, Channels, and Streams a Flow owns.
///
/// Return a schema from [`crate::Flow::persistence`]. Definitions must have unique names,
/// and all values accessed by Step or RPC code must appear in the schema.
///
/// # Examples
///
/// ```
/// use dex_sdk::{Attribute, Channel, PersistenceSchema};
///
/// let status = Attribute::<String>::new("status");
/// let commands = Channel::<String>::new("commands");
/// let schema = PersistenceSchema::new().attribute(&status).channel(&commands);
/// ```
pub struct PersistenceSchema {
    definitions: Vec<PersistenceDefinition>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PersistenceDefinition {
    pub(crate) name: String,
    pub(crate) kind: PersistenceKind,
    pub(crate) index: Option<AttributeIndex>,
    pub(crate) sync_to_attribute_store: bool,
    pub(crate) is_hard_delete_on_null: bool,
    pub(crate) numeric_or_boolean_type_name: Option<&'static str>,
    pub(crate) stream_identity: Option<usize>,
    pub(crate) stream_capacity_bytes: Option<i64>,
}

impl PersistenceDefinition {
    pub(crate) fn new(name: &str, kind: PersistenceKind) -> Self {
        Self {
            name: name.to_string(),
            kind,
            index: None,
            sync_to_attribute_store: false,
            is_hard_delete_on_null: false,
            numeric_or_boolean_type_name: None,
            stream_identity: None,
            stream_capacity_bytes: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PersistenceKind {
    Attribute,
    AttributeMap,
    Channel,
    ChannelMap,
    Stream,
}

impl PersistenceSchema {
    /// Creates an empty schema.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds one single-value Attribute definition.
    pub fn attribute<T>(mut self, attribute: &Attribute<T>) -> Self {
        self.definitions.push(PersistenceDefinition {
            index: attribute.index().cloned(),
            sync_to_attribute_store: attribute.is_sync_to_attribute_store(),
            is_hard_delete_on_null: attribute.is_hard_delete_on_null(),
            numeric_or_boolean_type_name: attribute.numeric_or_boolean_type_name(),
            ..PersistenceDefinition::new(attribute.name(), PersistenceKind::Attribute)
        });
        self
    }

    /// Adds one keyed Attribute-map definition.
    pub fn attribute_map<T>(mut self, attribute: &AttributeMap<T>) -> Self {
        self.definitions.push(PersistenceDefinition {
            index: attribute.index().cloned(),
            sync_to_attribute_store: attribute.is_sync_to_attribute_store(),
            is_hard_delete_on_null: attribute.is_hard_delete_on_null(),
            numeric_or_boolean_type_name: attribute.numeric_or_boolean_type_name(),
            ..PersistenceDefinition::new(attribute.name(), PersistenceKind::AttributeMap)
        });
        self
    }

    /// Adds one Channel definition.
    pub fn channel<T>(mut self, channel: &Channel<T>) -> Self {
        self.definitions.push(PersistenceDefinition::new(
            channel.name(),
            PersistenceKind::Channel,
        ));
        self
    }

    /// Adds one keyed Channel-map definition.
    pub fn channel_map<T>(mut self, channel: &ChannelMap<T>) -> Self {
        self.definitions.push(PersistenceDefinition::new(
            channel.name(),
            PersistenceKind::ChannelMap,
        ));
        self
    }

    /// Adds one best-effort Stream definition.
    pub fn stream<T>(mut self, stream: &Stream<T>) -> Self {
        self.definitions.push(PersistenceDefinition {
            stream_identity: Some(stream.identity()),
            stream_capacity_bytes: Some(stream.stream_capacity_bytes()),
            ..PersistenceDefinition::new(stream.name(), PersistenceKind::Stream)
        });
        self
    }

    pub(crate) fn definitions(&self) -> &[PersistenceDefinition] {
        &self.definitions
    }
}

#[cfg(test)]
mod tests {
    use super::PersistenceSchema;
    use crate::{Attribute, AttributeMap, Stream};

    #[test]
    fn registration_metadata_retains_sync_configuration() {
        let attribute = Attribute::<String>::new("plain");
        let attribute_map = AttributeMap::<String>::new("map").sync_to_attribute_store();
        let schema = PersistenceSchema::new()
            .attribute(&attribute)
            .attribute_map(&attribute_map);

        assert!(!schema.definitions[0].sync_to_attribute_store);
        assert!(schema.definitions[1].sync_to_attribute_store);
    }

    #[test]
    fn registration_metadata_retains_hard_delete_configuration() {
        let plain = Attribute::<String>::new("plain");
        let secret = Attribute::<String>::new("secret").hard_delete_on_null();
        let counter = Attribute::<Option<u64>>::new("counter").hard_delete_on_null();
        let secrets = AttributeMap::<Vec<u8>>::new("secrets").hard_delete_on_null();
        let schema = PersistenceSchema::new()
            .attribute(&plain)
            .attribute(&secret)
            .attribute(&counter)
            .attribute_map(&secrets.clone());

        assert!(!schema.definitions[0].is_hard_delete_on_null);
        assert!(schema.definitions[1].is_hard_delete_on_null);
        assert_eq!(schema.definitions[1].numeric_or_boolean_type_name, None);
        assert!(
            schema.definitions[2]
                .numeric_or_boolean_type_name
                .is_some_and(|type_name| type_name.contains("u64"))
        );
        assert!(schema.definitions[3].is_hard_delete_on_null);
        assert_eq!(schema.definitions[3].numeric_or_boolean_type_name, None);
    }

    #[test]
    fn registration_metadata_retains_stream_identity_and_capacity() {
        let stream = Stream::<String>::new("thinking", 1_048_576);
        let schema = PersistenceSchema::new().stream(&stream);

        assert_eq!(
            schema.definitions[0].stream_identity,
            Some(stream.identity())
        );
        assert_eq!(schema.definitions[0].stream_capacity_bytes, Some(1_048_576));
    }
}
