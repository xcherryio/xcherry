// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::any::TypeId;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::persistence::{PersistenceDefinition, PersistenceKind};
use crate::rpc::RegisteredRpc;
use crate::step::RegisteredStep;
use crate::step_options::ErasedStepOptions;
use crate::{Context, Flow, HandlerResult, SdkError, SdkResult, StepDecision, Stream, Wait};
use dex_protocol::dex::Value as ProtoValue;

#[derive(Clone, Default)]
/// Validates and stores all Flow definitions used by a Client or Worker.
///
/// Registration checks unique Flow, Step, RPC, and persistence names; starting-Step constraints;
/// Attribute index consistency; RPC locks; and handler options. Cloning is cheap because it shares
/// the registered data.
///
/// # Examples
///
/// ```
/// use dex_sdk::{Flow, Registry};
///
/// struct OrderFlow;
/// impl Flow for OrderFlow { type StartInput = String; }
///
/// let registry = Registry::new().register(OrderFlow)?;
/// # Ok::<(), dex_sdk::SdkError>(())
/// ```
pub struct Registry {
    inner: Arc<RegistryInner>,
}

#[derive(Clone, Default)]
struct RegistryInner {
    flows: HashMap<&'static str, RegisteredFlow>,
    attribute_indexes: HashMap<String, i32>,
    stream_flows: HashMap<usize, &'static str>,
}

#[derive(Clone)]
pub(crate) struct RegisteredFlow {
    pub(crate) name: &'static str,
    pub(crate) type_id: TypeId,
    pub(crate) steps: HashMap<&'static str, RegisteredStep>,
    pub(crate) start_step: Option<RegisteredStep>,
    pub(crate) rpcs: HashMap<&'static str, RegisteredRpc>,
    pub(crate) persistence: HashMap<String, PersistenceDefinition>,
    pub(crate) handler: Arc<dyn ErasedFlow>,
}

impl Registry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Validates and adds one owned Flow definition.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::FlowDefinition`] for invalid or conflicting names, schemas, indexes,
    /// Steps, RPCs, locks, or options.
    pub fn register<SomeFlow: Flow>(mut self, flow: SomeFlow) -> SdkResult<Self> {
        let flow = Arc::new(flow);
        let name = require_static_name(flow.flow_type(), "Flow type")?;
        let registered = assemble_flow(name, Arc::clone(&flow))?;
        let inner = Arc::make_mut(&mut self.inner);
        let mut additions = Vec::new();
        let mut stream_identities = Vec::new();
        for definition in registered.persistence.values() {
            if definition.kind == PersistenceKind::Stream {
                let identity = definition.stream_identity.ok_or_else(|| {
                    definition_error(format!(
                        "Flow {name} Stream {} has no definition identity",
                        definition.name
                    ))
                })?;
                if inner.stream_flows.contains_key(&identity) {
                    return Err(definition_error(format!(
                        "Stream {} is registered by multiple Flows",
                        definition.name
                    )));
                }
                stream_identities.push(identity);
            }
            let Some(index) = &definition.index else {
                continue;
            };
            if definition.kind == PersistenceKind::AttributeMap
                && index.key().is_none_or(str::is_empty)
            {
                return Err(definition_error(format!(
                    "Flow {name} indexed AttributeMap {} requires an index key",
                    definition.name
                )));
            }
            let key = index.key().unwrap_or(&definition.name).to_string();
            let index_type = index.proto_value();
            if inner
                .attribute_indexes
                .get(&key)
                .is_some_and(|existing| *existing != index_type)
            {
                return Err(definition_error(format!(
                    "Attribute index {key} has conflicting types"
                )));
            }
            additions.push((key, index_type));
        }
        if inner.flows.insert(name, registered).is_some() {
            return Err(definition_error(format!("duplicate Flow {name}")));
        }
        inner.attribute_indexes.extend(additions);
        for identity in stream_identities {
            inner.stream_flows.insert(identity, name);
        }
        Ok(self)
    }

    pub(crate) fn flow(&self, name: &str) -> SdkResult<&RegisteredFlow> {
        self.inner
            .flows
            .get(name)
            .ok_or_else(|| SdkError::FlowDefinition {
                message: format!("Flow is not registered: {name}"),
            })
    }

    pub(crate) fn flow_types(&self) -> Vec<String> {
        let mut flow_types = self
            .inner
            .flows
            .keys()
            .map(|name| (*name).to_string())
            .collect::<Vec<_>>();
        flow_types.sort();
        flow_types
    }

    pub(crate) fn rpc_with_flow(&self, name: &str) -> SdkResult<(&RegisteredFlow, &RegisteredRpc)> {
        let mut matched = None;
        for flow in self.inner.flows.values() {
            let Some(rpc) = flow.rpcs.get(name) else {
                continue;
            };
            if matched.is_some() {
                return Err(SdkError::FlowDefinition {
                    message: format!("RPC name is ambiguous across registered Flows: {name}"),
                });
            }
            matched = Some((flow, rpc));
        }
        matched.ok_or_else(|| SdkError::FlowDefinition {
            message: format!("RPC is not registered: {name}"),
        })
    }

    pub(crate) fn flow_for_stream<T>(&self, stream: &Stream<T>) -> SdkResult<&RegisteredFlow> {
        let flow_name = self
            .inner
            .stream_flows
            .get(&stream.identity())
            .ok_or_else(|| SdkError::FlowDefinition {
                message: format!("Stream is not registered: {}", stream.name()),
            })?;
        self.flow(flow_name)
    }

    pub(crate) fn attribute_indexes(&self) -> &HashMap<String, i32> {
        &self.inner.attribute_indexes
    }
}

fn assemble_flow<SomeFlow: Flow>(
    name: &'static str,
    flow: Arc<SomeFlow>,
) -> SdkResult<RegisteredFlow> {
    let timeout_handler = flow.timeout_handler();
    let mut steps = HashMap::new();
    let mut start_step = None;
    for step in flow.steps().into_definitions() {
        require_static_name(step.name, "Step type")?;
        if step.starting {
            if start_step.is_some() {
                return Err(definition_error(format!(
                    "Flow {name} must not have multiple start Steps"
                )));
            }
            start_step = Some(step.clone());
        }
        if steps.insert(step.name, step).is_some() {
            return Err(definition_error(format!(
                "Flow {name} has a duplicate Step"
            )));
        }
    }

    let persistence = assemble_persistence(name, flow.persistence().definitions())?;
    for step in steps.values() {
        let options = flow
            .steps()
            .find(step.name)
            .ok_or_else(|| definition_error(format!("Flow {name} Step disappeared")))?
            .options();
        validate_step_state_loads(name, step.name, &options, &persistence)?;
    }
    let mut rpcs = HashMap::new();
    for rpc in flow.rpcs().bind(Arc::clone(&flow)) {
        require_static_name(rpc.name, "RPC name")?;
        validate_rpc(name, &rpc, &persistence)?;
        if rpcs.insert(rpc.name, rpc).is_some() {
            return Err(definition_error(format!("Flow {name} has a duplicate RPC")));
        }
    }
    Ok(RegisteredFlow {
        name,
        type_id: TypeId::of::<SomeFlow>(),
        steps,
        start_step,
        rpcs,
        persistence,
        handler: Arc::new(TypedFlow {
            flow,
            timeout_handler,
        }),
    })
}

pub(crate) trait ErasedFlow: Send + Sync {
    fn has_timeout_handler(&self) -> bool;
    fn handle_timeout(&self, context: &mut Context) -> HandlerResult<StepDecision>;
    fn wait_for(
        &self,
        step_type: &str,
        context: &mut Context,
        input: &ProtoValue,
    ) -> HandlerResult<Wait>;
    fn execute(
        &self,
        step_type: &str,
        context: &mut Context,
        input: &ProtoValue,
    ) -> HandlerResult<StepDecision>;
    fn step_options(&self, step_type: &str) -> HandlerResult<ErasedStepOptions>;
}

struct TypedFlow<SomeFlow> {
    flow: Arc<SomeFlow>,
    timeout_handler: Option<crate::FlowTimeoutHandler<SomeFlow>>,
}

impl<SomeFlow: Flow> ErasedFlow for TypedFlow<SomeFlow> {
    fn has_timeout_handler(&self) -> bool {
        self.timeout_handler.is_some()
    }

    fn handle_timeout(&self, context: &mut Context) -> HandlerResult<StepDecision> {
        let handler = self.timeout_handler.ok_or_else(|| {
            crate::HandlerError::new("dex_sdk::HandlerError", "Flow has no timeout handler")
        })?;
        handler(self.flow.as_ref(), context)
    }
    fn wait_for(
        &self,
        step_type: &str,
        context: &mut Context,
        input: &ProtoValue,
    ) -> HandlerResult<Wait> {
        self.flow
            .steps()
            .find(step_type)
            .ok_or_else(|| {
                crate::HandlerError::new(
                    "dex_sdk::HandlerError",
                    format!("Step is not registered: {step_type}"),
                )
            })?
            .wait_for(context, input)
    }

    fn execute(
        &self,
        step_type: &str,
        context: &mut Context,
        input: &ProtoValue,
    ) -> HandlerResult<StepDecision> {
        self.flow
            .steps()
            .find(step_type)
            .ok_or_else(|| {
                crate::HandlerError::new(
                    "dex_sdk::HandlerError",
                    format!("Step is not registered: {step_type}"),
                )
            })?
            .execute(context, input)
    }

    fn step_options(&self, step_type: &str) -> HandlerResult<ErasedStepOptions> {
        Ok(self
            .flow
            .steps()
            .find(step_type)
            .ok_or_else(|| {
                crate::HandlerError::new(
                    "dex_sdk::HandlerError",
                    format!("Step is not registered: {step_type}"),
                )
            })?
            .options())
    }
}

fn assemble_persistence(
    flow_name: &str,
    definitions: &[PersistenceDefinition],
) -> SdkResult<HashMap<String, PersistenceDefinition>> {
    let mut persistence = HashMap::new();
    for definition in definitions {
        if definition.name.is_empty() {
            return Err(definition_error(format!(
                "Flow {flow_name} has an empty persistence definition name"
            )));
        }
        if matches!(
            definition.kind,
            PersistenceKind::Attribute
                | PersistenceKind::AttributeMap
                | PersistenceKind::Channel
                | PersistenceKind::ChannelMap
        ) && definition.name.contains('/')
        {
            return Err(definition_error(format!(
                "Flow {flow_name} persistence definition {} must not contain /",
                definition.name
            )));
        }
        if persistence
            .insert(definition.name.clone(), definition.clone())
            .is_some()
        {
            return Err(definition_error(format!(
                "Flow {flow_name} has duplicate persistence definition {}",
                definition.name
            )));
        }
    }
    Ok(persistence)
}

fn validate_rpc(
    flow_name: &str,
    rpc: &RegisteredRpc,
    persistence: &HashMap<String, PersistenceDefinition>,
) -> SdkResult<()> {
    if rpc.timeout.is_some_and(|timeout| timeout.is_zero()) {
        return Err(definition_error(format!(
            "Flow {flow_name} RPC {} timeout must be positive",
            rpc.name
        )));
    }
    let mut locks = HashSet::new();
    for lock in &rpc.locks {
        let physical_name = lock.physical_name();
        let logical_name = physical_name.split('/').next().unwrap_or(&physical_name);
        let definition = persistence.get(logical_name).ok_or_else(|| {
            definition_error(format!(
                "Flow {flow_name} RPC {} locks an unregistered Attribute",
                rpc.name
            ))
        })?;
        if !matches!(
            definition.kind,
            PersistenceKind::Attribute | PersistenceKind::AttributeMap
        ) {
            return Err(definition_error(format!(
                "Flow {flow_name} RPC {} locks a non-Attribute",
                rpc.name
            )));
        }
        if !locks.insert(physical_name) {
            return Err(definition_error(format!(
                "Flow {flow_name} RPC {} has a duplicate lock",
                rpc.name
            )));
        }
    }
    validate_map_loads(
        flow_name,
        &format!("RPC {}", rpc.name),
        &rpc.load_attribute_maps,
        PersistenceKind::AttributeMap,
        persistence,
        |load| (&load.name, load.instance.as_deref()),
    )?;
    validate_definition_loads(
        flow_name,
        &format!("RPC {}", rpc.name),
        &rpc.load_channels,
        PersistenceKind::Channel,
        persistence,
        |load| &load.name,
    )?;
    validate_map_loads(
        flow_name,
        &format!("RPC {}", rpc.name),
        &rpc.load_channel_maps,
        PersistenceKind::ChannelMap,
        persistence,
        |load| (&load.name, load.instance.as_deref()),
    )?;
    Ok(())
}

fn validate_step_state_loads(
    flow_name: &str,
    step_name: &str,
    options: &ErasedStepOptions,
    persistence: &HashMap<String, PersistenceDefinition>,
) -> SdkResult<()> {
    validate_step_method_state_loads(
        flow_name,
        &format!("Step {step_name} WaitFor"),
        &options.wait_for_load_attribute_maps,
        &options.wait_for_load_channels,
        &options.wait_for_load_channel_maps,
        persistence,
    )?;
    validate_step_method_state_loads(
        flow_name,
        &format!("Step {step_name} Execute"),
        &options.execute_load_attribute_maps,
        &options.execute_load_channels,
        &options.execute_load_channel_maps,
        persistence,
    )?;
    Ok(())
}

fn validate_step_method_state_loads(
    flow_name: &str,
    source: &str,
    attribute_maps: &[crate::AttributeMapLoad],
    channels: &[crate::channel::ChannelLoad],
    channel_maps: &[crate::ChannelMapLoad],
    persistence: &HashMap<String, PersistenceDefinition>,
) -> SdkResult<()> {
    validate_map_loads(
        flow_name,
        source,
        attribute_maps,
        PersistenceKind::AttributeMap,
        persistence,
        |load| (&load.name, load.instance.as_deref()),
    )?;
    validate_definition_loads(
        flow_name,
        source,
        channels,
        PersistenceKind::Channel,
        persistence,
        |load| &load.name,
    )?;
    validate_map_loads(
        flow_name,
        source,
        channel_maps,
        PersistenceKind::ChannelMap,
        persistence,
        |load| (&load.name, load.instance.as_deref()),
    )?;
    Ok(())
}

fn validate_map_loads<Load, GetLoad>(
    flow_name: &str,
    source: &str,
    loads: &[Load],
    kind: PersistenceKind,
    persistence: &HashMap<String, PersistenceDefinition>,
    get_load: GetLoad,
) -> SdkResult<()>
where
    GetLoad: for<'a> Fn(&'a Load) -> (&'a String, Option<&'a str>),
{
    let mut physical_names = HashSet::new();
    for load in loads {
        let (name, instance) = get_load(load);
        validate_load_definition(flow_name, source, name, kind, persistence)?;
        if instance.is_some_and(str::is_empty) {
            return Err(definition_error(format!(
                "Flow {flow_name} {source} loads an empty map instance"
            )));
        }
        if instance.is_some_and(|value| value.contains('/')) {
            return Err(definition_error(format!(
                "Flow {flow_name} {source} map instance must not contain /"
            )));
        }
        let physical_name = match instance {
            Some(instance) => physical_name(name, instance),
            None => format!("{name}/"),
        };
        if !physical_names.insert(physical_name) {
            return Err(definition_error(format!(
                "Flow {flow_name} {source} has a duplicate state load"
            )));
        }
    }
    Ok(())
}

fn validate_definition_loads<Load, GetLoad>(
    flow_name: &str,
    source: &str,
    loads: &[Load],
    kind: PersistenceKind,
    persistence: &HashMap<String, PersistenceDefinition>,
    get_load: GetLoad,
) -> SdkResult<()>
where
    GetLoad: for<'a> Fn(&'a Load) -> &'a String,
{
    let mut names = HashSet::new();
    for load in loads {
        let name = get_load(load);
        validate_load_definition(flow_name, source, name, kind, persistence)?;
        if !names.insert(name) {
            return Err(definition_error(format!(
                "Flow {flow_name} {source} has a duplicate state load"
            )));
        }
    }
    Ok(())
}

fn validate_load_definition(
    flow_name: &str,
    source: &str,
    name: &str,
    kind: PersistenceKind,
    persistence: &HashMap<String, PersistenceDefinition>,
) -> SdkResult<()> {
    let definition = persistence.get(name).ok_or_else(|| {
        definition_error(format!(
            "Flow {flow_name} {source} loads unregistered state: {name}"
        ))
    })?;
    if definition.kind != kind {
        return Err(definition_error(format!(
            "Flow {flow_name} {source} loads the wrong state kind: {name}"
        )));
    }
    Ok(())
}

fn require_static_name(name: &'static str, kind: &str) -> SdkResult<&'static str> {
    if name.is_empty() {
        Err(definition_error(format!("{kind} is required")))
    } else {
        Ok(name)
    }
}

fn definition_error(message: impl Into<String>) -> SdkError {
    SdkError::FlowDefinition {
        message: message.into(),
    }
}

pub(crate) fn physical_name(name: &str, instance: &str) -> String {
    assert_map_instance(instance);
    let mut encoded = String::new();
    for byte in instance.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    format!("{name}/{encoded}")
}

pub(crate) fn decode_instance(physical_name: &str, prefix: &str) -> Result<String, String> {
    let encoded = physical_name
        .strip_prefix(prefix)
        .ok_or_else(|| format!("map key {physical_name} does not start with {prefix}"))?;
    let bytes = encoded.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return Err(format!("invalid percent encoding in {physical_name}"));
            }
            let high = hex_value(bytes[index + 1])?;
            let low = hex_value(bytes[index + 2])?;
            decoded.push(high * 16 + low);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    let instance = String::from_utf8(decoded).map_err(|error| error.to_string())?;
    validate_map_instance(&instance)?;
    Ok(instance)
}

pub(crate) fn assert_map_instance(instance: &str) {
    if let Err(error) = validate_map_instance(instance) {
        panic!("{error}");
    }
}

pub(crate) fn validate_map_instance(instance: &str) -> Result<(), String> {
    if instance.is_empty() {
        return Err("map instance is required".to_string());
    }
    if instance.contains('/') {
        return Err("map instances must not contain '/'".to_string());
    }
    Ok(())
}

fn hex_value(byte: u8) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(format!(
            "invalid percent-encoding digit {}",
            char::from(byte)
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::{PersistenceDefinition, PersistenceKind, assemble_persistence, physical_name};

    #[test]
    fn persistence_definition_names_reserve_slash() {
        for kind in [
            PersistenceKind::Attribute,
            PersistenceKind::AttributeMap,
            PersistenceKind::Channel,
            PersistenceKind::ChannelMap,
        ] {
            let result = assemble_persistence(
                "TestFlow",
                &[PersistenceDefinition {
                    name: "orders/by-id".to_string(),
                    kind,
                    index: None,
                    sync_to_attribute_store: false,
                    stream_identity: None,
                    stream_capacity_bytes: None,
                }],
            );
            assert!(result.is_err());
        }

        assert!(std::panic::catch_unwind(|| physical_name("messages", "orders/by-id")).is_err());
    }
}
