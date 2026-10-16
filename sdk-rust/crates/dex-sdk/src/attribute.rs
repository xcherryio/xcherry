// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::any::TypeId;
use std::marker::PhantomData;

use dex_protocol::dex::{AttributeSyncConfig, AttributeWrite, IndexConfig, value};

use crate::{Context, HandlerError, HandlerResult, Value};

/// Selects one AttributeMap instance to load for a handler invocation.
///
/// Create instance loads with [`AttributeMap::load`], then attach them with
/// [`Rpc::load_attribute_map_instance`](crate::Rpc::load_attribute_map_instance).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttributeMapLoad {
    pub(crate) name: String,
    pub(crate) instance: Option<String>,
}

/// Defines one durable, typed value stored with a Flow.
///
/// Declare Attributes on the Flow, add them to [`crate::PersistenceSchema`], and reuse the same
/// definition from Step and RPC code. Values use Serde serialization. `get` distinguishes a missing
/// value with `None`; `get_required` converts absence into [`HandlerError`].
///
/// # Examples
///
/// ```
/// use dex_sdk::{Attribute, AttributeIndex};
///
/// let status = Attribute::<String>::new("status")
///     .indexed(AttributeIndex::keyword());
/// ```
pub struct Attribute<T> {
    name: String,
    index: Option<AttributeIndex>,
    sync_to_attribute_store: bool,
    is_hard_delete_on_null: bool,
    numeric_or_boolean_type_name: Option<&'static str>,
    marker: PhantomData<fn() -> T>,
}

impl<T> Attribute<T> {
    /// Defines an unindexed Attribute with a stable `name`.
    ///
    /// Slash is prohibited because it is a reserved character.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            index: None,
            sync_to_attribute_store: false,
            is_hard_delete_on_null: false,
            numeric_or_boolean_type_name: None,
            marker: PhantomData,
        }
    }

    /// Enables Flow-search indexing with the selected index type.
    pub fn indexed(mut self, index: AttributeIndex) -> Self {
        self.index = Some(index);
        self
    }

    /// Enables asynchronous latest-state projection to the Flow's selected Attribute Store.
    ///
    /// The Flow must select a server-configured store with
    /// [`FlowConfig::attribute_store_names`](crate::FlowConfig::attribute_store_names). Deletions
    /// project SQL `NULL`, and projection failures never roll back the Flow Attribute write.
    #[must_use]
    pub fn sync_to_attribute_store(mut self) -> Self {
        self.sync_to_attribute_store = true;
        self
    }

    /// Stores this Attribute's values only in the Dex blob store and permanently deletes the stored
    /// value when the Attribute is deleted or set to null.
    ///
    /// Use this option for short-lived secrets, such as API keys or access tokens. Dex never writes
    /// the value into Flow history, regardless of its size. Flow history keeps only a reference to
    /// the stored value. [`Self::delete`], [`Context::delete_attribute`], and a write whose value
    /// encodes to null permanently delete the stored value before the write commits. A deleted
    /// value reads as an absent Attribute, so [`Self::get`] returns `Ok(None)`.
    ///
    /// Step handlers, RPC handlers, and Attribute reads receive the value inline. The SDK never
    /// stores the value in the local [`BlobCache`](crate::BlobCache).
    ///
    /// # Supported values
    ///
    /// Only values that encode to a string or an object are supported. Strings, Serde structs,
    /// maps, sequences, and bytes are supported. Numeric and Boolean values are rejected.
    /// [`Registry::register`](crate::Registry::register) returns
    /// [`SdkError::FlowDefinition`](crate::SdkError::FlowDefinition) when `T` is a Rust integer,
    /// floating-point, or `bool` type, or an `Option` of one. Every write also checks its encoded
    /// value. A value of another type that encodes to a number or a Boolean, such as a newtype
    /// around `u64`, fails when it is written. Deletion and null values are always allowed.
    ///
    /// To store a numeric or Boolean secret, wrap it in a struct so that it encodes as an object:
    ///
    /// ```
    /// use dex_sdk::Attribute;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize, Serialize)]
    /// struct OneTimeCode {
    ///     code: u32,
    /// }
    ///
    /// let one_time_code = Attribute::<OneTimeCode>::new("one-time-code").hard_delete_on_null();
    /// ```
    ///
    /// # Restrictions
    ///
    /// - This option cannot be combined with [`Self::indexed`] or
    ///   [`Self::sync_to_attribute_store`]. Registration returns
    ///   [`SdkError::FlowDefinition`](crate::SdkError::FlowDefinition) for either combination.
    /// - [`Client::wait_for_attribute_match`](crate::Client::wait_for_attribute_match) rejects a
    ///   hard-delete Attribute with [`SdkError::InvalidArgument`](crate::SdkError::InvalidArgument)
    ///   before it contacts Dex.
    /// - Action and Action permission conditions cannot read a hard-delete Attribute. Dex
    ///   matches those conditions against stored Attribute values, which hard-delete Attributes
    ///   never keep in Flow history. Use an ordinary Attribute as the condition source instead.
    /// - Every write must use a definition whose hard-delete setting matches the definition that
    ///   the Flow registered. A mismatched write is rejected.
    /// - The Dex Server must have an enabled blob store. The Server rejects writes when its blob
    ///   store is disabled.
    /// - The Dex Server must support protocol version 3 or later. A Worker learns the protocol
    ///   version when it starts. A Client requests it once, before its first hard-delete write,
    ///   and reuses the result. Writes fail with an error when the Server is older.
    ///
    /// # Limitations
    ///
    /// - Each Flow keeps one stored copy per Attribute key. Every write overwrites it in place.
    /// - A Step or RPC attempt writes or deletes the stored value before Dex commits the attempt's
    ///   result. A retried attempt, other Steps, and Attribute reads can therefore observe a write
    ///   or deletion from an attempt that later failed.
    /// - When concurrent Steps or RPCs write the same key, the final stored value may not be the
    ///   value of the last committed write.
    /// - A Flow start can fail because a Flow with the same Flow ID is already running. That
    ///   failed start still overwrites the running Flow's stored value. A null initial value
    ///   deletes it. This applies to
    ///   [`Client::start_flow_with_options`](crate::Client::start_flow_with_options) and to SubFlow
    ///   starts.
    /// - Executions that reuse a Flow ID share the stored value.
    /// - Resetting a Flow does not restore a deleted value. A deleted value reads as absent.
    /// - Stored values remain until they are deleted, or until Dex removes the Flow through
    ///   retention and blob cleanup.
    /// - Blob store backups and bucket versioning can retain copies of stored values.
    ///
    /// # Examples
    ///
    /// ```
    /// use dex_sdk::{Attribute, Context, HandlerResult, PersistenceSchema, StartFlowOptions};
    /// use std::sync::LazyLock;
    ///
    /// static API_KEY: LazyLock<Attribute<String>> =
    ///     LazyLock::new(|| Attribute::new("api-key").hard_delete_on_null());
    ///
    /// // Register the definition in the Flow's persistence schema.
    /// let schema = PersistenceSchema::new().attribute(&API_KEY);
    ///
    /// // Supply the first secret when the Flow starts.
    /// let options = StartFlowOptions::new().initial_attribute(&API_KEY, "key-1".to_owned());
    ///
    /// // A Step reads the secret inline and rotates it.
    /// fn rotate_api_key(context: &mut Context) -> HandlerResult<()> {
    ///     let current = API_KEY.get_required(context)?;
    ///     API_KEY.set(context, format!("{current}-rotated"))
    /// }
    ///
    /// // A later Step permanently deletes the stored secret.
    /// fn revoke_api_key(context: &mut Context) -> HandlerResult<()> {
    ///     API_KEY.delete(context)
    /// }
    /// # let _ = (schema, options, rotate_api_key, revoke_api_key);
    /// ```
    #[must_use]
    pub fn hard_delete_on_null(mut self) -> Self
    where
        T: 'static,
    {
        self.is_hard_delete_on_null = true;
        self.numeric_or_boolean_type_name = numeric_or_boolean_type_name::<T>();
        self
    }

    /// Reads the current value from a Step or RPC invocation.
    ///
    /// Returns `Ok(None)` when no value is stored.
    ///
    /// # Errors
    ///
    /// Returns [`HandlerError`] when the payload cannot be decoded.
    pub fn get(&self, context: &Context) -> HandlerResult<Option<T>>
    where
        T: Value,
    {
        context.get_attribute(self)
    }

    /// Reads the current value and treats absence as an error.
    ///
    /// # Errors
    ///
    /// Returns [`HandlerError`] when the Attribute is missing or cannot be decoded.
    pub fn get_required(&self, context: &Context) -> HandlerResult<T>
    where
        T: Value,
    {
        self.get(context)?.ok_or_else(|| {
            HandlerError::new(
                "dex_sdk::HandlerError",
                format!("attribute {} is missing", self.name),
            )
        })
    }

    /// Stages `value` for durable persistence in the current invocation.
    ///
    /// # Errors
    ///
    /// Returns [`HandlerError`] when the value cannot be encoded or indexed.
    pub fn set(&self, context: &mut Context, value: T) -> HandlerResult<()>
    where
        T: Value,
    {
        context.set_attribute(self, value)
    }

    /// Stages deletion of the current value.
    ///
    /// Deleting a missing Attribute is valid.
    pub fn delete(&self, context: &mut Context) -> HandlerResult<()> {
        context.delete_attribute(self)
    }

    /// Creates a lock request for Step options or an RPC definition.
    pub fn lock(&self) -> AttributeLock {
        AttributeLock {
            attribute: self.name.clone(),
            instance: None,
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn index(&self) -> Option<&AttributeIndex> {
        self.index.as_ref()
    }

    pub(crate) fn sync_config(&self) -> Option<AttributeSyncConfig> {
        self.sync_to_attribute_store
            .then_some(AttributeSyncConfig { enabled: true })
    }

    pub(crate) fn is_sync_to_attribute_store(&self) -> bool {
        self.sync_to_attribute_store
    }

    pub(crate) fn is_hard_delete_on_null(&self) -> bool {
        self.is_hard_delete_on_null
    }

    pub(crate) fn numeric_or_boolean_type_name(&self) -> Option<&'static str> {
        self.numeric_or_boolean_type_name
    }

    pub(crate) fn write_settings(&self) -> AttributeWriteSettings {
        AttributeWriteSettings {
            index_config: self.index().map(|index| index.proto_config(false)),
            sync_config: self.sync_config(),
            is_hard_delete_on_null: self.is_hard_delete_on_null,
        }
    }
}

impl<T> Clone for Attribute<T> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            index: self.index.clone(),
            sync_to_attribute_store: self.sync_to_attribute_store,
            is_hard_delete_on_null: self.is_hard_delete_on_null,
            numeric_or_boolean_type_name: self.numeric_or_boolean_type_name,
            marker: PhantomData,
        }
    }
}

/// Defines keyed durable values sharing one Attribute name.
///
/// Each `instance` identifies an independent stored value and lock. Add the map definition once to
/// [`crate::PersistenceSchema`], then pass an instance on each access.
/// Slash is prohibited in instance keys because it is a reserved character.
pub struct AttributeMap<T> {
    name: String,
    index: Option<AttributeIndex>,
    sync_to_attribute_store: bool,
    is_hard_delete_on_null: bool,
    numeric_or_boolean_type_name: Option<&'static str>,
    marker: PhantomData<fn() -> T>,
}

impl<T> AttributeMap<T> {
    /// Defines an unindexed Attribute map with a stable `name`.
    ///
    /// Slash is prohibited because it is a reserved character.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            index: None,
            sync_to_attribute_store: false,
            is_hard_delete_on_null: false,
            numeric_or_boolean_type_name: None,
            marker: PhantomData,
        }
    }

    /// Enables search indexing for every instance using the selected index type.
    pub fn indexed(mut self, index: AttributeIndex) -> Self {
        self.index = Some(index);
        self
    }

    /// Enables asynchronous latest-state projection for every AttributeMap instance.
    ///
    /// Each instance maps by its physical Attribute name. The Flow must select a server-configured
    /// stores with [`FlowConfig::attribute_store_names`](crate::FlowConfig::attribute_store_names).
    /// Deletions project SQL `NULL`, and projection failures never roll back Flow Attribute writes.
    #[must_use]
    pub fn sync_to_attribute_store(mut self) -> Self {
        self.sync_to_attribute_store = true;
        self
    }

    /// Stores every instance's values only in the Dex blob store and permanently deletes an
    /// instance's stored value when that instance is deleted or set to null.
    ///
    /// Use this option for keyed short-lived secrets, such as one API key per tenant. Each
    /// instance has its own stored value. Dex never writes instance values into Flow history.
    /// [`Self::delete`] and a write whose value encodes to null permanently delete the instance's
    /// stored value. The instance then reads as absent. Handlers and Attribute reads receive
    /// values inline. The SDK never stores them in the local [`BlobCache`](crate::BlobCache).
    ///
    /// Only string and object values, including bytes, are supported. Numeric and Boolean values
    /// are rejected. This option cannot be combined with [`Self::indexed`] or
    /// [`Self::sync_to_attribute_store`].
    /// [`Client::wait_for_attribute_map_instance_match`](crate::Client::wait_for_attribute_map_instance_match)
    /// rejects it before it contacts Dex. Action and Action permission conditions cannot read
    /// it. Writes require a Server with an enabled blob store and
    /// protocol version 3 or later. Each instance key keeps one stored copy, and later writes
    /// overwrite it in place.
    ///
    /// Supported values, restrictions, Server requirements, and limitations are otherwise the same
    /// as for [`Attribute::hard_delete_on_null`]. Read that method before storing secrets.
    ///
    /// # Examples
    ///
    /// ```
    /// use dex_sdk::{AttributeMap, Context, HandlerResult, PersistenceSchema, StartFlowOptions};
    /// use std::sync::LazyLock;
    ///
    /// static TENANT_API_KEYS: LazyLock<AttributeMap<String>> =
    ///     LazyLock::new(|| AttributeMap::new("tenant-api-keys").hard_delete_on_null());
    ///
    /// let schema = PersistenceSchema::new().attribute_map(&TENANT_API_KEYS);
    /// let options = StartFlowOptions::new().initial_attribute_map(
    ///     &TENANT_API_KEYS,
    ///     "tenant-1",
    ///     "key-1".to_owned(),
    /// );
    ///
    /// // The Step or RPC must load the instance before reading it.
    /// fn revoke_tenant_api_key(context: &mut Context, tenant: &str) -> HandlerResult<()> {
    ///     if TENANT_API_KEYS.get(context, tenant)?.is_some() {
    ///         TENANT_API_KEYS.delete(context, tenant)?;
    ///     }
    ///     Ok(())
    /// }
    /// # let _ = (schema, options, revoke_tenant_api_key);
    /// ```
    #[must_use]
    pub fn hard_delete_on_null(mut self) -> Self
    where
        T: 'static,
    {
        self.is_hard_delete_on_null = true;
        self.numeric_or_boolean_type_name = numeric_or_boolean_type_name::<T>();
        self
    }

    /// Selects one slash-free map `instance` for loading into a handler Context.
    ///
    /// The instance remains readable for the handler even when it is absent; absence is returned as
    /// `None`. Load the map definition when the RPC must enumerate every instance.
    pub fn load(&self, instance: impl Into<String>) -> AttributeMapLoad {
        AttributeMapLoad {
            name: self.name.clone(),
            instance: Some(instance.into()),
        }
    }

    /// Reads one instance, returning `Ok(None)` when it is absent.
    ///
    /// # Errors
    ///
    /// Returns [`HandlerError`] when the payload cannot be decoded.
    pub fn get(&self, context: &Context, instance: &str) -> HandlerResult<Option<T>>
    where
        T: Value,
    {
        context.get_attribute_map(self, instance)
    }

    /// Reads one instance and treats absence as an error.
    ///
    /// # Errors
    ///
    /// Returns [`HandlerError`] when the instance is missing or cannot be decoded.
    pub fn get_required(&self, context: &Context, instance: &str) -> HandlerResult<T>
    where
        T: Value,
    {
        self.get(context, instance)?.ok_or_else(|| {
            HandlerError::new(
                "dex_sdk::HandlerError",
                format!("attribute {}[{instance}] is missing", self.name),
            )
        })
    }

    /// Stages one instance value for durable persistence.
    ///
    /// # Errors
    ///
    /// Returns [`HandlerError`] when the value cannot be encoded or indexed.
    pub fn set(&self, context: &mut Context, instance: &str, value: T) -> HandlerResult<()>
    where
        T: Value,
    {
        context.set_attribute_map(self, instance, value)
    }

    /// Stages deletion of one instance; missing instances are valid.
    pub fn delete(&self, context: &mut Context, instance: &str) -> HandlerResult<()> {
        context.delete_attribute_map(self, instance)
    }

    /// Returns the effective number of instances, including buffered writes.
    pub fn map_size(&self, context: &Context) -> HandlerResult<usize> {
        Ok(self.all_instance_keys(context)?.len())
    }

    /// Returns decoded effective instance keys in ascending string order.
    pub fn all_instance_keys(&self, context: &Context) -> HandlerResult<Vec<String>> {
        context.attribute_map_keys(self)
    }

    /// Creates a lock request scoped to one map instance.
    pub fn lock(&self, instance: impl Into<String>) -> AttributeLock {
        let instance = instance.into();
        crate::registry::assert_map_instance(&instance);
        AttributeLock {
            attribute: self.name.clone(),
            instance: Some(instance),
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn index(&self) -> Option<&AttributeIndex> {
        self.index.as_ref()
    }

    pub(crate) fn sync_config(&self) -> Option<AttributeSyncConfig> {
        self.sync_to_attribute_store
            .then_some(AttributeSyncConfig { enabled: true })
    }

    pub(crate) fn is_sync_to_attribute_store(&self) -> bool {
        self.sync_to_attribute_store
    }

    pub(crate) fn is_hard_delete_on_null(&self) -> bool {
        self.is_hard_delete_on_null
    }

    pub(crate) fn numeric_or_boolean_type_name(&self) -> Option<&'static str> {
        self.numeric_or_boolean_type_name
    }

    pub(crate) fn write_settings(&self) -> AttributeWriteSettings {
        AttributeWriteSettings {
            index_config: self.index().map(|index| index.proto_config(true)),
            sync_config: self.sync_config(),
            is_hard_delete_on_null: self.is_hard_delete_on_null,
        }
    }
}

impl<T> Clone for AttributeMap<T> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            index: self.index.clone(),
            sync_to_attribute_store: self.sync_to_attribute_store,
            is_hard_delete_on_null: self.is_hard_delete_on_null,
            numeric_or_boolean_type_name: self.numeric_or_boolean_type_name,
            marker: PhantomData,
        }
    }
}

/// Carries the definition-derived fields copied onto every write of one Attribute definition.
#[derive(Clone, Debug, Default)]
pub(crate) struct AttributeWriteSettings {
    pub(crate) index_config: Option<IndexConfig>,
    pub(crate) sync_config: Option<AttributeSyncConfig>,
    pub(crate) is_hard_delete_on_null: bool,
}

impl AttributeWriteSettings {
    pub(crate) fn attribute_write(
        self,
        key: String,
        value: dex_protocol::dex::Value,
    ) -> AttributeWrite {
        AttributeWrite {
            key,
            value: Some(value),
            index_config: self.index_config,
            sync_config: self.sync_config,
            is_hard_delete_on_null: self.is_hard_delete_on_null,
        }
    }
}

/// Returns `Ok(())` for a valid hard-delete write and for every write without the flag.
///
/// Returns an error when a hard-delete write also enables indexing or Attribute Store
/// synchronization, or when its value encodes to an integer, floating-point, or Boolean value.
pub(crate) fn validate_hard_delete_attribute_write(write: &AttributeWrite) -> Result<(), String> {
    if !write.is_hard_delete_on_null {
        return Ok(());
    }
    let key = &write.key;
    if write
        .index_config
        .as_ref()
        .is_some_and(|config| config.enable)
    {
        return Err(format!(
            "hard-delete Attribute {key} cannot be combined with an index"
        ));
    }
    if write
        .sync_config
        .as_ref()
        .is_some_and(|config| config.enabled)
    {
        return Err(format!(
            "hard-delete Attribute {key} cannot be combined with sync_to_attribute_store"
        ));
    }
    match write.value.as_ref().and_then(|value| value.kind.as_ref()) {
        Some(value::Kind::IntValue(_))
        | Some(value::Kind::DoubleValue(_))
        | Some(value::Kind::BoolValue(_)) => Err(format!(
            "hard-delete Attribute {key} supports only string and object values; numeric and \
             Boolean values are rejected"
        )),
        _ => Ok(()),
    }
}

/// Returns `Ok(())` when a write's hard-delete setting matches the registered definition of
/// `attribute_name`, and an error naming the Attribute otherwise.
pub(crate) fn require_registered_hard_delete_setting(
    attribute_name: &str,
    is_registered_hard_delete_on_null: bool,
    is_write_hard_delete_on_null: bool,
) -> Result<(), String> {
    if is_registered_hard_delete_on_null == is_write_hard_delete_on_null {
        return Ok(());
    }
    Err(format!(
        "Attribute {attribute_name} hard_delete_on_null setting does not match its registered \
         definition"
    ))
}

/// Returns the type name when `T` is a Rust numeric or Boolean type, or an `Option` of one.
///
/// Returns `None` for every other type, including newtypes that still encode to numbers.
fn numeric_or_boolean_type_name<T: 'static>() -> Option<&'static str> {
    macro_rules! scalar_type_ids {
        ($($scalar:ty),* $(,)?) => {
            [$(TypeId::of::<$scalar>(), TypeId::of::<Option<$scalar>>()),*]
        };
    }
    let scalar_type_ids = scalar_type_ids!(
        i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize, f32, f64, bool,
    );
    scalar_type_ids
        .contains(&TypeId::of::<T>())
        .then(std::any::type_name::<T>)
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Describes how Dex projects an Attribute value into its Flow-search index.
///
/// Without [`Self::with_key`], a single Attribute uses its name as the physical key. Attribute maps
/// derive per-instance physical keys unless an explicit key is set.
pub struct AttributeIndex {
    kind: AttributeIndexKind,
    key: Option<String>,
}

impl AttributeIndex {
    fn new(kind: AttributeIndexKind) -> Self {
        Self { kind, key: None }
    }

    /// Indexes one exact UTF-8 string for equality and aggregation queries.
    pub fn keyword() -> Self {
        Self::new(AttributeIndexKind::Keyword)
    }

    /// Indexes analyzed UTF-8 text for full-text queries.
    pub fn full_text() -> Self {
        Self::new(AttributeIndexKind::FullText)
    }

    /// Indexes an array of exact UTF-8 strings.
    pub fn keyword_array() -> Self {
        Self::new(AttributeIndexKind::KeywordArray)
    }

    /// Indexes a signed 64-bit integer.
    pub fn int() -> Self {
        Self::new(AttributeIndexKind::Int)
    }

    /// Indexes a finite 64-bit floating-point number.
    pub fn double() -> Self {
        Self::new(AttributeIndexKind::Double)
    }

    /// Indexes a Boolean value.
    pub fn bool() -> Self {
        Self::new(AttributeIndexKind::Bool)
    }

    /// Indexes an RFC 3339 date-time value.
    pub fn date_time() -> Self {
        Self::new(AttributeIndexKind::DateTime)
    }

    /// Uses an explicit physical search-index key instead of the default Attribute-derived key.
    pub fn with_key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AttributeIndexKind {
    Keyword,
    FullText,
    KeywordArray,
    Int,
    Double,
    Bool,
    DateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Identifies an Attribute or Attribute-map instance lock.
///
/// Create locks with [`Attribute::lock`] or [`AttributeMap::lock`], then attach them to
/// [`crate::StepOptions`] or [`crate::RpcDefinition`](crate::Rpc).
pub struct AttributeLock {
    attribute: String,
    instance: Option<String>,
}

impl AttributeIndex {
    pub(crate) fn proto_config(&self, dynamic: bool) -> IndexConfig {
        IndexConfig {
            enable: true,
            r#type: self.kind.proto_value(),
            index_key: self
                .key
                .clone()
                .or_else(|| dynamic.then(String::new))
                .unwrap_or_default(),
        }
    }

    pub(crate) fn key(&self) -> Option<&str> {
        self.key.as_deref()
    }

    pub(crate) fn proto_value(&self) -> i32 {
        self.kind.proto_value()
    }
}

impl AttributeIndexKind {
    pub(crate) fn proto_value(self) -> i32 {
        use dex_protocol::dex::IndexType;

        match self {
            Self::Keyword => IndexType::Keyword.into(),
            Self::FullText => IndexType::Text.into(),
            Self::KeywordArray => IndexType::KeywordArray.into(),
            Self::Int => IndexType::Int.into(),
            Self::Double => IndexType::Double.into(),
            Self::Bool => IndexType::Bool.into(),
            Self::DateTime => IndexType::Datetime.into(),
        }
    }
}

impl AttributeLock {
    pub(crate) fn attribute_name(&self) -> &str {
        &self.attribute
    }

    pub(crate) fn instance(&self) -> Option<&str> {
        self.instance.as_deref()
    }

    pub(crate) fn physical_name(&self) -> String {
        match self.instance.as_deref() {
            Some(instance) => crate::registry::physical_name(&self.attribute, instance),
            None => self.attribute.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Attribute, AttributeMap};

    #[test]
    fn sync_configuration_is_opt_in_and_survives_clone() {
        let plain = Attribute::<String>::new("plain");
        let synced = Attribute::<String>::new("synced").sync_to_attribute_store();
        let synced_map = AttributeMap::<String>::new("synced_map").sync_to_attribute_store();

        assert!(plain.sync_config().is_none());
        assert_eq!(
            synced.sync_config().map(|config| config.enabled),
            Some(true)
        );
        assert_eq!(
            synced_map
                .clone()
                .sync_config()
                .map(|config| config.enabled),
            Some(true)
        );
    }
}
