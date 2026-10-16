// Portions of this file are derived from indeedeng/iwf-java-sdk.
// Those portions are licensed under the Apache License, Version 2.0.
// See LICENSES/Apache-2.0.txt and LEGACY_NOTICES.md.
//
// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications are licensed under the Sustainable Use License 1.0.
// Third-Party Materials remain under the Apache License, Version 2.0.
// See LICENSE and LEGACY_NOTICES.md.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use dex_protocol::dex::flow_service_client::FlowServiceClient;
use dex_protocol::dex::{
    AttributeWrite, GetAttributesRequest, SetAttributesRequest, Value as ProtoValue, value,
};
use dex_sdk::{AttributeMatch, Registry, SdkError, StartFlowOptions, WaitForAttributeOptions};
use tokio::runtime::Runtime;
use tonic::transport::Channel as TransportChannel;

use crate::hard_delete_attribute_workflow::{
    ABSENT, API_KEY, DELETION_OBSERVATION, HardDeleteAttributeFlow, INITIAL_API_KEY,
    INITIAL_TENANT_TOKEN, PLAIN_NOTE, ROTATION_OBSERVATION, RPC_API_KEY, STEP_API_KEY, TENANT,
    TENANT_TOKENS, TenantToken,
};
use crate::support::{DexDevTestEnvironment, flow_id};

const PLAIN_NOTE_VALUE: &str = "rust-plain-note";
const CLIENT_API_KEY: &str = "rust-client-api-key";
const CLIENT_API_KEY_AFTER_DELETE: &str = "rust-client-api-key-after-delete";
const API_KEY_NAME: &str = "api-key";
const TENANT_TOKEN_KEY: &str = "tenant-tokens/tenant-a";

#[test]
#[ignore = "requires dexcli dev"]
fn hard_delete_attributes_store_values_inline_and_delete_on_null() {
    let environment =
        DexDevTestEnvironment::start(Registry::new().register(HardDeleteAttributeFlow::new()));
    let client = &environment.client;
    let flow = HardDeleteAttributeFlow::new();
    let flow_id = flow_id("rust-hard-delete-attribute");
    let attribute_service = AttributeService::connect();

    client
        .start_flow_with_options(
            &flow,
            &flow_id,
            (),
            StartFlowOptions::new()
                .initial_attribute(&API_KEY, INITIAL_API_KEY.to_string())
                .initial_attribute_map(
                    &TENANT_TOKENS,
                    TENANT,
                    TenantToken {
                        token: INITIAL_TENANT_TOKEN.to_string(),
                    },
                )
                .initial_attribute(&PLAIN_NOTE, PLAIN_NOTE_VALUE.to_string()),
        )
        .expect("start hard-delete Flow");

    let rotation_observation = client
        .wait_for_attribute_match(
            &flow_id,
            &ROTATION_OBSERVATION,
            AttributeMatch::equal_to(format!(
                "{INITIAL_API_KEY}|{INITIAL_TENANT_TOKEN}|{PLAIN_NOTE_VALUE}"
            )),
            WaitForAttributeOptions::new(),
        )
        .expect("Step read initial hard-delete values inline");
    assert_eq!(
        format!("{INITIAL_API_KEY}|{INITIAL_TENANT_TOKEN}|{PLAIN_NOTE_VALUE}"),
        rotation_observation
    );

    attribute_service.await_string(&flow_id, API_KEY_NAME, STEP_API_KEY);
    let tenant_token = attribute_service
        .get_attributes(&flow_id, &[TENANT_TOKEN_KEY])
        .remove(TENANT_TOKEN_KEY)
        .expect("initial tenant token is present");
    let Some(value::Kind::ObjValue(tenant_token)) = tenant_token.kind else {
        panic!("tenant token must be an inline object, got {tenant_token:?}");
    };
    let tenant_token: TenantToken =
        serde_json::from_slice(&tenant_token.payload).expect("decode tenant token");
    assert_eq!(INITIAL_TENANT_TOKEN, tenant_token.token);

    assert_eq!(
        Some(STEP_API_KEY.to_string()),
        client
            .invoke_rpc_without_input(&flow_id, HardDeleteAttributeFlow::GET_API_KEY)
            .expect("RPC reads hard-delete value inline")
    );
    client
        .invoke_rpc(
            &flow_id,
            HardDeleteAttributeFlow::ROTATE_API_KEY,
            RPC_API_KEY.to_string(),
        )
        .expect("RPC writes hard-delete value");
    attribute_service.await_string(&flow_id, API_KEY_NAME, RPC_API_KEY);

    attribute_service.set_attribute(&flow_id, hard_delete_string_write(CLIENT_API_KEY));
    attribute_service.await_string(&flow_id, API_KEY_NAME, CLIENT_API_KEY);

    let wait_error = client
        .wait_for_attribute_match(
            &flow_id,
            &API_KEY,
            AttributeMatch::equal_to(CLIENT_API_KEY.to_string()),
            WaitForAttributeOptions::new(),
        )
        .expect_err("hard-delete Attributes cannot be awaited");
    assert!(matches!(wait_error, SdkError::InvalidArgument { .. }));

    client
        .invoke_rpc_without_input(&flow_id, HardDeleteAttributeFlow::APPROVE_DELETION)
        .expect("approve deletion");
    let deletion_observation = client
        .wait_for_attribute_match(
            &flow_id,
            &DELETION_OBSERVATION,
            AttributeMatch::equal_to(CLIENT_API_KEY.to_string()),
            WaitForAttributeOptions::new(),
        )
        .expect("Step read the Client-written hard-delete value");
    assert_eq!(CLIENT_API_KEY, deletion_observation);
    attribute_service.await_absent(&flow_id, API_KEY_NAME);
    attribute_service.await_absent(&flow_id, TENANT_TOKEN_KEY);
    assert_eq!(
        None,
        client
            .invoke_rpc_without_input(&flow_id, HardDeleteAttributeFlow::GET_API_KEY)
            .expect("RPC reads deleted hard-delete value as absent")
    );

    attribute_service.set_attribute(
        &flow_id,
        hard_delete_string_write(CLIENT_API_KEY_AFTER_DELETE),
    );
    attribute_service.await_string(&flow_id, API_KEY_NAME, CLIENT_API_KEY_AFTER_DELETE);
    attribute_service.set_attribute(
        &flow_id,
        AttributeWrite {
            key: API_KEY_NAME.to_string(),
            value: Some(ProtoValue {
                kind: Some(value::Kind::NullValue(
                    prost_types::NullValue::NullValue.into(),
                )),
            }),
            index_config: None,
            sync_config: None,
            is_hard_delete_on_null: true,
        },
    );
    attribute_service.await_absent(&flow_id, API_KEY_NAME);
    let plain_note = attribute_service
        .get_attributes(&flow_id, &["plain-note"])
        .remove("plain-note")
        .expect("plain Attribute remains");
    assert_eq!(
        Some(value::Kind::StringValue(PLAIN_NOTE_VALUE.to_string())),
        plain_note.kind
    );

    client
        .invoke_rpc_without_input(&flow_id, HardDeleteAttributeFlow::APPROVE_COMPLETION)
        .expect("approve completion");
    let output: String = client
        .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
        .and_then(|result| result.single_output())
        .expect("hard-delete Flow completes");
    assert_eq!(format!("{ABSENT}|{ABSENT}"), output);
}

fn hard_delete_string_write(api_key: &str) -> AttributeWrite {
    AttributeWrite {
        key: API_KEY_NAME.to_string(),
        value: Some(ProtoValue {
            kind: Some(value::Kind::StringValue(api_key.to_string())),
        }),
        index_config: None,
        sync_config: None,
        is_hard_delete_on_null: true,
    }
}

/// Calls GetAttributes and SetAttributes directly because the Rust Client exposes neither API.
struct AttributeService {
    runtime: Runtime,
    client: FlowServiceClient<TransportChannel>,
}

impl AttributeService {
    fn connect() -> Self {
        let server_address =
            std::env::var("DEX_SERVER_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8801".to_string());
        let runtime = Runtime::new().expect("create FlowService runtime");
        let client = runtime
            .block_on(FlowServiceClient::connect(format!(
                "http://{server_address}"
            )))
            .expect("connect FlowService");
        Self { runtime, client }
    }

    fn get_attributes(&self, flow_id: &str, keys: &[&str]) -> HashMap<String, ProtoValue> {
        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(client.get_attributes(GetAttributesRequest {
                flow_id: flow_id.to_string(),
                run_id: String::new(),
                keys: keys.iter().map(|key| (*key).to_string()).collect(),
                all_keys: false,
            }))
            .expect("GetAttributes")
            .into_inner();
        response
            .attributes
            .into_iter()
            .map(|attribute| {
                let value = attribute.value.expect("GetAttributes returns a Value");
                assert!(
                    !matches!(
                        value.kind,
                        Some(value::Kind::InternalBlobIdForStringValue(_))
                            | Some(value::Kind::InternalBlobIdForObjValue(_))
                    ),
                    "Attribute {} must be returned inline, got {value:?}",
                    attribute.key
                );
                (attribute.key, value)
            })
            .collect()
    }

    fn set_attribute(&self, flow_id: &str, write: AttributeWrite) {
        let mut client = self.client.clone();
        self.runtime
            .block_on(client.set_attributes(SetAttributesRequest {
                flow_id: flow_id.to_string(),
                run_id: String::new(),
                attributes: vec![write],
                request_id: uuid::Uuid::new_v4().to_string(),
                action_permission_mappings: None,
            }))
            .expect("SetAttributes");
    }

    fn await_string(&self, flow_id: &str, key: &str, expected: &str) {
        let expected_kind = Some(value::Kind::StringValue(expected.to_string()));
        let mut last_value = None;
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            last_value = self.get_attributes(flow_id, &[key]).remove(key);
            if last_value.as_ref().map(|value| &value.kind) == Some(&expected_kind) {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!("Attribute {key} never became {expected}; last value {last_value:?}");
    }

    fn await_absent(&self, flow_id: &str, key: &str) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if !self.get_attributes(flow_id, &[key]).contains_key(key) {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!("Attribute {key} was never deleted");
    }
}
