// Portions of this file are derived from indeedeng/iwf-java-sdk.
// Those portions are licensed under the Apache License, Version 2.0.
// See LICENSES/Apache-2.0.txt and LEGACY_NOTICES.md.
//
// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications are licensed under the Sustainable Use License 1.0.
// Third-Party Materials remain under the Apache License, Version 2.0.
// See LICENSE and LEGACY_NOTICES.md.

use std::time::{Duration, Instant};

use dex_sdk::{
    FlowConfig, FlowStatus, IdReusePolicy, Registry, SdkError, SearchFlowsOptions, StartFlowOptions,
};
use serde_json::Value as JsonValue;

use crate::search_flows_workflow::SearchFlowsWorkflow;
use crate::support::{DexDevTestEnvironment, flow_id, skip_if_server_zd};

#[test]
#[ignore = "requires dexcli dev"]
fn test_search_flows_finds_indexed_flow() {
    if skip_if_server_zd("Continue-As-New") {
        return;
    }
    let environment =
        DexDevTestEnvironment::start(Registry::new().register(SearchFlowsWorkflow::new()));
    let workflow = SearchFlowsWorkflow::new();
    let keyword_value = flow_id("sf");
    let flow_id = flow_id("search-flows");
    environment
        .client
        .start_flow_with_options(
            &workflow,
            &flow_id,
            keyword_value.clone(),
            StartFlowOptions::new()
                .id_reuse_policy(IdReusePolicy::Disallow)
                .config_override(FlowConfig::new().continue_as_new_threshold(1)),
        )
        .expect("start indexed Flow");
    assert_eq!(
        keyword_value,
        environment
            .client
            .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
            .and_then(|result| result.single_output::<String>())
            .expect("complete indexed Flow")
    );
    let query = format!("{} = '{}'", SearchFlowsWorkflow::KEYWORD_KEY, keyword_value);
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last_error = None;
    let entry = loop {
        match environment.client.search_flows_page(&query, 100, "") {
            Ok(page) => {
                if let Some(entry) = page
                    .flows
                    .into_iter()
                    .find(|entry| entry.flow_id == flow_id)
                {
                    break entry;
                }
            }
            Err(error) => last_error = Some(error.to_string()),
        }
        assert!(
            Instant::now() < deadline,
            "Flow {flow_id} not found via SearchFlows: {last_error:?}"
        );
        std::thread::yield_now();
    };
    let chain_query = format!("WorkflowId = '{flow_id}'");
    let deadline = Instant::now() + Duration::from_secs(30);
    let all_runs = loop {
        let page = environment
            .client
            .search_flows_with_options(
                &chain_query,
                100,
                "",
                SearchFlowsOptions::new().include_continued_as_new(true),
            )
            .expect("include earlier runs");
        if page
            .flows
            .iter()
            .any(|run| run.status == FlowStatus::ContinuedAsNew)
        {
            break page.flows;
        }
        assert!(
            Instant::now() < deadline,
            "expected a Continue-as-New chain"
        );
        std::thread::sleep(Duration::from_millis(100));
    };
    assert!(all_runs.len() > 1);
    let current = environment
        .client
        .search_flows_page(&chain_query, 100, "")
        .expect("search default current runs");
    assert_eq!(1, current.flows.len());
    assert_eq!(FlowStatus::Completed, current.flows[0].status);
    let mut token = String::new();
    let mut run_ids = std::collections::HashSet::new();
    loop {
        let page = environment
            .client
            .search_flows_with_options(
                &chain_query,
                1,
                &token,
                SearchFlowsOptions::new().include_continued_as_new(true),
            )
            .expect("page through earlier runs");
        for run in page.flows {
            assert!(run_ids.insert(run.run_id));
        }
        token = page.next_page_token;
        if token.is_empty() {
            break;
        }
    }
    assert_eq!(all_runs.len(), run_ids.len());
    assert_eq!(flow_id, entry.flow_id);
    assert!(!entry.run_id.is_empty());
    assert_eq!(FlowStatus::Completed, entry.status);
    assert!(entry.started_at.is_some());
    assert_eq!(
        Some(&JsonValue::String(keyword_value)),
        entry
            .indexed_attributes
            .get(SearchFlowsWorkflow::KEYWORD_KEY)
    );
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_search_flows_rejects_negative_page_size() {
    let environment =
        DexDevTestEnvironment::start(Registry::new().register(SearchFlowsWorkflow::new()));
    assert!(matches!(
        environment
            .client
            .search_flows("CustomKeywordField = 'x'", -1)
            .expect_err("negative search page size must fail"),
        SdkError::InvalidArgument { .. }
    ));
}
