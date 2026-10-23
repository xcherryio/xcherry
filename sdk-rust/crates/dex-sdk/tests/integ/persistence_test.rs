// Portions of this file are derived from indeedeng/iwf-java-sdk.
// Those portions are licensed under the Apache License, Version 2.0.
// See LICENSES/Apache-2.0.txt and LEGACY_NOTICES.md.
//
// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications are licensed under the Sustainable Use License 1.0.
// Third-Party Materials remain under the Apache License, Version 2.0.
// See LICENSE and LEGACY_NOTICES.md.

use std::time::Duration;

use dex_sdk::{
    Attribute, AttributeMatch, Client, Registry, SdkError, SdkResult, StartFlowOptions,
    WaitForAttributeOptions,
};

use crate::persistence_set_attributes_workflow::{
    self as set_attributes, PersistenceSetAttributesWorkflow,
};
use crate::persistence_workflow::{self as persistence, PersistenceModel, PersistenceWorkflow};
use crate::support::{DexDevTestEnvironment, flow_id};

#[test]
#[ignore = "requires dexcli dev"]
fn test_persistence_reads() {
    let environment =
        DexDevTestEnvironment::start(Registry::new().register(PersistenceWorkflow::new()));
    let workflow = PersistenceWorkflow::new();
    let flow_id = flow_id("persistence");
    let options = StartFlowOptions::new()
        .initial_attribute(&persistence::INITIAL, "initial".to_string())
        .initial_attribute_map(&workflow.data_map, "one", "initial".to_string());
    environment
        .client
        .start_flow_with_options(&workflow, &flow_id, "input".to_string(), options)
        .expect("start persistence Flow");
    assert_eq!(
        "input",
        environment
            .client
            .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
            .and_then(|result| result.single_output::<String>())
            .expect("complete persistence Flow")
    );
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_set_indexed_attributes() {
    let environment = DexDevTestEnvironment::start(
        Registry::new().register(PersistenceSetAttributesWorkflow::new()),
    );
    let workflow = PersistenceSetAttributesWorkflow::new();
    let flow_id = flow_id("set-indexed-attributes");
    environment
        .client
        .start_flow(&workflow, &flow_id, "start".to_string())
        .expect("start set-indexed-attributes Flow");
    environment
        .client
        .invoke_rpc_without_input::<()>(&flow_id, PersistenceSetAttributesWorkflow::SET_INDEXED)
        .expect("set indexed Attributes through RPC");
    environment
        .client
        .invoke_rpc_without_input::<()>(&flow_id, PersistenceSetAttributesWorkflow::COMPLETE)
        .expect("complete through RPC");
    assert_eq!(
        "test-result",
        environment
            .client
            .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
            .and_then(|result| result.single_output::<String>())
            .expect("complete set-indexed-attributes Flow")
    );
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_set_data_attributes() {
    let environment = DexDevTestEnvironment::start(
        Registry::new().register(PersistenceSetAttributesWorkflow::new()),
    );
    let workflow = PersistenceSetAttributesWorkflow::new();
    let flow_id = flow_id("set-data-attributes");
    environment
        .client
        .start_flow(&workflow, &flow_id, "start".to_string())
        .expect("start set-data-attributes Flow");
    let timeout_result = environment.client.wait_for_attribute_match(
        &flow_id,
        &set_attributes::DATA,
        AttributeMatch::equal_to("never".to_string()),
        wait_options(&flow_id, "never", 1),
    );
    assert!(
        matches!(&timeout_result, Err(SdkError::RequestTimeout { .. })),
        "unexpected timeout result: {timeout_result:?}"
    );
    std::thread::scope(|scope| {
        let waiting = scope.spawn(|| {
            environment.client.wait_for_attribute_match(
                &flow_id,
                &set_attributes::DATA,
                AttributeMatch::equal_to("query-start".to_string()),
                wait_options(&flow_id, "data", 30),
            )
        });
        environment
            .client
            .invoke_rpc(
                &flow_id,
                PersistenceSetAttributesWorkflow::SET_DATA,
                "query-start".to_string(),
            )
            .expect("set data Attribute");
        let matched = waiting
            .join()
            .expect("join Attribute wait")
            .expect("wait for data Attribute");
        assert_eq!("query-start", matched);
    });
    std::thread::scope(|scope| {
        let waiting = scope.spawn(|| {
            environment.client.wait_for_attribute_map_instance_match(
                &flow_id,
                &set_attributes::DATA_MAP,
                "special % key",
                AttributeMatch::equal_to("mapped-value".to_string()),
                wait_options(&flow_id, "map", 30),
            )
        });
        environment
            .client
            .invoke_rpc(
                &flow_id,
                PersistenceSetAttributesWorkflow::SET_MAP_SPECIAL,
                "mapped-value".to_string(),
            )
            .expect("set special AttributeMap entry");
        let matched = waiting
            .join()
            .expect("join AttributeMap wait")
            .expect("wait for AttributeMap entry");
        assert_eq!("mapped-value", matched);
    });
    environment
        .client
        .invoke_rpc(&flow_id, PersistenceSetAttributesWorkflow::SET_INTEGER, 3)
        .expect("set revision Attribute");
    assert_eq!(
        3,
        environment
            .client
            .wait_for_attribute_match(
                &flow_id,
                &set_attributes::INTEGER,
                AttributeMatch::greater_than(0),
                wait_options(&flow_id, "integer", 30),
            )
            .expect("wait for revision Attribute")
    );
    assert!(matches!(
        environment.client.wait_for_attribute_match(
            &flow_id,
            &set_attributes::MODEL,
            AttributeMatch::equal_to(PersistenceModel { value: 8 }),
            wait_options(&flow_id, "model", 30),
        ),
        Err(SdkError::InvalidArgument { .. })
    ));
    assert!(matches!(
        environment.client.wait_for_attribute_match(
            &flow_id,
            &Attribute::<Vec<u8>>::new("bytes"),
            AttributeMatch::equal_to(vec![1]),
            wait_options(&flow_id, "bytes", 30),
        ),
        Err(SdkError::InvalidArgument { .. })
    ));
    assert!(matches!(
        environment.client.wait_for_attribute_match(
            &flow_id,
            &Attribute::<()>::new("null"),
            AttributeMatch::equal_to(()),
            wait_options(&flow_id, "null", 30),
        ),
        Err(SdkError::InvalidArgument { .. })
    ));
    environment
        .client
        .invoke_rpc(
            &flow_id,
            PersistenceSetAttributesWorkflow::SET_MAP_ONE,
            "mapped-value".to_string(),
        )
        .expect("set AttributeMap entry");
    environment
        .client
        .invoke_rpc(
            &flow_id,
            PersistenceSetAttributesWorkflow::SET_MODEL,
            PersistenceModel { value: 7 },
        )
        .expect("set model Attribute");
    environment
        .client
        .invoke_rpc_without_input::<()>(&flow_id, PersistenceSetAttributesWorkflow::COMPLETE)
        .expect("complete through RPC");
    assert_eq!(
        "test-result",
        environment
            .client
            .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
            .and_then(|result| result.single_output::<String>())
            .expect("complete set-data-attributes Flow")
    );
}

#[expect(
    dead_code,
    reason = "Integration fixtures implement schema variants exercised by selected test targets."
)]
fn compile_persistence_reads(client: &Client) -> SdkResult<()> {
    let workflow = PersistenceWorkflow::new();
    let options = StartFlowOptions::new()
        .initial_attribute(&persistence::INITIAL, "initial".to_string())
        .initial_attribute_map(&workflow.data_map, "one", "initial".to_string());
    client.start_flow_with_options(&workflow, "persistence", "input".to_string(), options)?;
    let _: String = client.wait_for_flow("persistence")?.single_output()?;
    Ok(())
}

#[expect(
    dead_code,
    reason = "Integration fixtures implement schema variants exercised by selected test targets."
)]
fn compile_persistence_writes(client: &Client) -> SdkResult<()> {
    let workflow = PersistenceSetAttributesWorkflow::new();
    client.start_flow(&workflow, "set-attributes", "input".to_string())?;
    client.invoke_rpc(
        "set-attributes",
        PersistenceSetAttributesWorkflow::SET_DATA,
        "value".to_string(),
    )?;
    client.invoke_rpc(
        "set-attributes",
        PersistenceSetAttributesWorkflow::SET_MAP_ONE,
        "value".to_string(),
    )?;
    client.invoke_rpc_without_input::<()>(
        "set-attributes",
        PersistenceSetAttributesWorkflow::SET_INDEXED,
    )?;
    let _: String = client.wait_for_attribute_match(
        "set-attributes",
        &set_attributes::DATA,
        AttributeMatch::equal_to("value".to_string()),
        wait_options("set-attributes", "data", 30),
    )?;
    let _: String = client.wait_for_attribute_map_instance_match(
        "set-attributes",
        &set_attributes::DATA_MAP,
        "one",
        AttributeMatch::equal_to("value".to_string()),
        wait_options("set-attributes", "map", 30),
    )?;
    client.invoke_rpc_without_input::<()>(
        "set-attributes",
        PersistenceSetAttributesWorkflow::COMPLETE,
    )?;
    let _: String = client.wait_for_flow("set-attributes")?.single_output()?;
    Ok(())
}

fn wait_options(flow_id: &str, suffix: &str, seconds: u64) -> WaitForAttributeOptions {
    WaitForAttributeOptions::new()
        .request_id(format!("{flow_id}-wait-{suffix}"))
        .request_timeout(Duration::from_secs(seconds))
}
