// Copyright (c) 2022-2026 Super Durable, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dex_examples_rust::create_example_registry;
use dex_examples_rust::patterns::polling::{FakeJobService, JOB_PROGRESS, JobState, PollingFlow};
use dex_examples_rust::patterns::recovery::FailureRecoveryFlow;
use dex_examples_rust::primitives::channel::flow::{
    ChannelFlow, DELETE_QUEUED_MESSAGE, ENQUEUE_CHANNEL_MESSAGE, GET_PRIORITIZED_MESSAGES,
    GET_QUEUED_MESSAGES, MOVE_QUEUED_MESSAGE_TO_PRIORITIZED_MESSAGES, PUBLISH_APPROVAL_MESSAGE,
    QueuedMessageReference,
};
use dex_examples_rust::primitives::stream::flow::{PROGRESS, StreamFlow};
use dex_examples_rust::production::pull_workers::{
    GET_GREETING, GreetingFlow, start_pull_greeting, stop_pull_greeting,
};
use dex_examples_rust::products::deal_dsl::{
    ConditionMessage, DEAL_CURRENT_STATE, DealDSLFlow, SEND_DEAL_CONDITION_MESSAGE,
    example_deal_start,
};
use dex_examples_rust::products::engagement::{
    ACCEPT_ENGAGEMENT, DESCRIBE_ENGAGEMENT, EngagementFlow, EngagementRequest, EngagementStatus,
};
use dex_examples_rust::products::job_post::{
    GET_JOB_POST, Init, JobPost, JobPostingFlow, UPDATE_JOB_POST, UpdateIndeedPosting,
    UpdateLinkedInPosting,
};
use dex_examples_rust::products::microservices::{
    DATA, OrchestrationFlow, SIGNAL_ORCHESTRATION_READY, SWAP_ORCHESTRATION_DATA,
};
use dex_examples_rust::products::money_transfer::{MoneyTransferFlow, TransferRequest};
use dex_examples_rust::products::order_processing::{
    APPROVE_ORDER, Charge, OrderProcessingFlow, OrderRequest, Ship,
};
use dex_examples_rust::products::signup::{
    ACCOMPLISH_ONBOARDING_TASK_1, ACCOMPLISH_ONBOARDING_TASK_2, ONBOARDING_STATUS,
    UserOnboardingFlow, VERIFY_ONBOARDING, WAITING_FOR_TASK_1, WAITING_FOR_TASK_2,
    WAITING_FOR_VERIFICATION,
};
use dex_examples_rust::products::subscription::{
    CANCEL_SUBSCRIPTION, DESCRIBE_SUBSCRIPTION, SubscriptionFlow, SubscriptionRequest,
    SubscriptionState, UPDATE_SUBSCRIPTION_CHARGE,
};
use dex_sdk::{
    AttributeMatch, BlobCache, BlobCacheConfig, Client, ClientOptions, FlowStatus, SdkError,
    SdkResult, StepExecutionId, StopFlowOptions, TimerId, WaitForAttributeOptions,
    WaitForStepCompletionOptions, Worker, WorkerOptions,
};
use tempfile::TempDir;

struct DexEnvironment {
    client: Client,
    polling_jobs: Arc<FakeJobService>,
    worker: Arc<Worker>,
    worker_thread: Option<JoinHandle<SdkResult<()>>>,
    cache: Arc<BlobCache>,
    _cache_directory: TempDir,
}

impl DexEnvironment {
    fn start() -> Self {
        let polling_jobs = Arc::new(FakeJobService::default());
        let registry =
            create_example_registry(Arc::clone(&polling_jobs)).expect("register Rust examples");
        let server_address =
            std::env::var("DEX_SERVER_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8801".to_string());
        let worker_port = available_worker_port();
        let worker_address = format!("127.0.0.1:{worker_port}");
        let cache_directory = tempfile::tempdir().expect("create Rust examples cache directory");
        let cache = Arc::new(
            BlobCache::open(
                BlobCacheConfig::new(cache_directory.path(), 64 * 1024 * 1024, 10_000)
                    .expect("valid Rust examples cache config"),
            )
            .expect("open Rust examples cache"),
        );
        let worker = Arc::new(
            Worker::try_new(
                registry.clone(),
                Arc::clone(&cache),
                WorkerOptions::new()
                    .server_address(&server_address)
                    .bind_address(&worker_address),
            )
            .expect("create Rust examples Worker"),
        );
        let thread_worker = Arc::clone(&worker);
        let worker_thread = thread::Builder::new()
            .name("dex-rust-examples-worker".to_string())
            .spawn(move || thread_worker.start())
            .expect("start Rust examples Worker thread");
        await_worker(worker_port, &worker_thread);
        let client = Client::try_new(
            registry,
            Arc::clone(&cache),
            ClientOptions::new()
                .server_address(server_address)
                .worker_target(worker.worker_target().clone()),
        )
        .expect("create Rust examples Client");
        client.health_check().expect("Dex health check");
        Self {
            client,
            polling_jobs,
            worker,
            worker_thread: Some(worker_thread),
            cache,
            _cache_directory: cache_directory,
        }
    }

    fn await_engagement_status(&self, flow_id: &str, expected: &str) -> EngagementStatus {
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            let status = self
                .client
                .invoke_rpc_without_input(flow_id, DESCRIBE_ENGAGEMENT)
                .expect("describe Rust Engagement Flow");
            if status.status == expected {
                return status;
            }
            thread::yield_now();
        }
        panic!("Rust Engagement Flow did not reach {expected}");
    }

    fn await_subscription_charge(&self, flow_id: &str, expected: i64) -> SubscriptionState {
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            let state = self
                .client
                .invoke_rpc_without_input(flow_id, DESCRIBE_SUBSCRIPTION)
                .expect("describe Rust Subscription Flow");
            if state.charge_cents == expected {
                return state;
            }
            thread::yield_now();
        }
        panic!("Rust Subscription Flow charge did not reach {expected}");
    }
}

impl Drop for DexEnvironment {
    fn drop(&mut self) {
        self.worker.stop();
        if let Some(worker_thread) = self.worker_thread.take() {
            worker_thread
                .join()
                .expect("join Rust examples Worker")
                .expect("stop Rust examples Worker");
        }
        self.cache.close().expect("close Rust examples cache");
    }
}

#[test]
#[ignore = "requires dexcli dev"]
fn pull_worker_serves_greeting_flow() {
    let cache_directory = tempfile::tempdir().expect("create Rust pull greeting cache directory");
    let cache = Arc::new(
        BlobCache::open(
            BlobCacheConfig::new(cache_directory.path(), 64 * 1024 * 1024, 10_000)
                .expect("valid Rust pull greeting cache config"),
        )
        .expect("open Rust pull greeting cache"),
    );
    let server_address =
        std::env::var("DEX_SERVER_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8801".to_string());
    let (worker, client, worker_thread) = start_pull_greeting(Arc::clone(&cache), &server_address)
        .expect("start Rust pull greeting Worker");
    let flow_id = unique_flow_id("pull-greeting");
    client
        .start_flow(&GreetingFlow::default(), &flow_id, "Dex".to_string())
        .expect("start Rust pull Greeting Flow");
    let output: String = client
        .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(45))
        .expect("complete Rust pull Greeting Flow")
        .single_output()
        .expect("decode Rust pull Greeting output");
    assert_eq!(output, "Hello, Dex!");
    assert_eq!(
        client
            .invoke_rpc_without_input(&flow_id, GET_GREETING)
            .expect("read Rust pull Greeting RPC"),
        output
    );
    stop_pull_greeting(worker, client, worker_thread)
        .expect("join Rust pull greeting Worker")
        .expect("stop Rust pull greeting Worker");
    cache.close().expect("close Rust pull greeting cache");
}

#[test]
#[ignore = "requires dexcli dev"]
fn channel_message_can_be_moved_by_id() {
    let environment = DexEnvironment::start();
    let flow = ChannelFlow::default();
    let flow_id = unique_flow_id("channel-message");
    environment
        .client
        .start_flow(&flow, &flow_id, 30)
        .expect("start Rust Channel Flow");
    environment
        .client
        .invoke_rpc(&flow_id, ENQUEUE_CHANNEL_MESSAGE, "delete me".to_string())
        .expect("publish first queued message");
    environment
        .client
        .invoke_rpc(&flow_id, ENQUEUE_CHANNEL_MESSAGE, "move me".to_string())
        .expect("publish second queued message");

    let pending = environment
        .client
        .invoke_rpc_without_input(&flow_id, GET_QUEUED_MESSAGES)
        .expect("list queued messages");
    assert_eq!(
        pending
            .iter()
            .map(|message| message.value.as_str())
            .collect::<Vec<_>>(),
        ["delete me", "move me"]
    );
    environment
        .client
        .invoke_rpc(
            &flow_id,
            DELETE_QUEUED_MESSAGE,
            QueuedMessageReference {
                message_id: pending[0].message_id.clone(),
            },
        )
        .expect("delete first queued message");

    let queued_message = QueuedMessageReference {
        message_id: pending[1].message_id.clone(),
    };
    environment
        .client
        .invoke_rpc(
            &flow_id,
            MOVE_QUEUED_MESSAGE_TO_PRIORITIZED_MESSAGES,
            queued_message.clone(),
        )
        .expect("move queued message");
    let prioritized_messages = environment
        .client
        .invoke_rpc_without_input(&flow_id, GET_PRIORITIZED_MESSAGES)
        .expect("list prioritized messages");
    assert_eq!(prioritized_messages[0].value, "move me");

    assert!(matches!(
        environment.client.invoke_rpc(
            &flow_id,
            MOVE_QUEUED_MESSAGE_TO_PRIORITIZED_MESSAGES,
            queued_message,
        ),
        Err(SdkError::ChannelMessageNotFound { .. })
    ));
    assert_eq!(
        environment
            .client
            .invoke_rpc_without_input(&flow_id, GET_PRIORITIZED_MESSAGES)
            .expect("list prioritized messages after failed retry")
            .len(),
        1
    );

    environment
        .client
        .invoke_rpc_without_input(&flow_id, PUBLISH_APPROVAL_MESSAGE)
        .expect("approve Channel Flow");
}

#[test]
#[ignore = "requires dexcli dev"]
fn money_transfer_completes_with_released_sdk() {
    let environment = DexEnvironment::start();
    let flow = MoneyTransferFlow::default();
    let flow_id = unique_flow_id("money-transfer");
    let input = TransferRequest {
        from_account: "released-sdk-source".to_string(),
        to_account: "released-sdk-destination".to_string(),
        amount_cents: 4_200,
        notes: "examples/rust integration".to_string(),
    };
    environment
        .client
        .start_flow(&flow, &flow_id, input)
        .expect("start Rust Money Transfer Flow");
    let output: TransferRequest = environment
        .client
        .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
        .expect("complete Rust Money Transfer Flow")
        .single_output()
        .expect("decode Rust Money Transfer output");
    assert_eq!(output.from_account, "released-sdk-source");
    assert_eq!(output.to_account, "released-sdk-destination");
    assert_eq!(output.amount_cents, 4_200);
    assert_eq!(
        environment
            .client
            .describe_flow(&flow_id)
            .expect("describe completed Rust Money Transfer Flow")
            .status,
        FlowStatus::Completed
    );
}

#[test]
#[ignore = "requires dexcli dev"]
fn deal_dsl_completes_an_item_purchase() {
    let environment = DexEnvironment::start();
    let flow = DealDSLFlow::default();
    let flow_id = unique_flow_id("deal-dsl");
    environment
        .client
        .start_flow(&flow, &flow_id, example_deal_start("buyer-1"))
        .expect("start Rust Deal DSL Flow");
    environment
        .client
        .wait_for_attribute_match(
            &flow_id,
            &DEAL_CURRENT_STATE,
            AttributeMatch::equal_to("negotiating".to_string()),
            attribute_wait_options(30),
        )
        .expect("wait for Rust Deal DSL negotiation");
    environment
        .client
        .invoke_rpc(
            &flow_id,
            SEND_DEAL_CONDITION_MESSAGE,
            ConditionMessage {
                condition_name: "buyer-decision".to_string(),
                values: BTreeMap::from([("accepted".to_string(), "true".to_string())]),
            },
        )
        .expect("accept Rust Deal DSL item purchase");
    let output: BTreeMap<String, String> = environment
        .client
        .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
        .expect("complete Rust Deal DSL Flow")
        .single_output()
        .expect("decode Rust Deal DSL output");
    assert_eq!(
        output.get("lastAction"),
        Some(&"deliverItemToBuyer".to_string())
    );
    assert_eq!(
        output.get("itemDeliveryStatus"),
        Some(&"delivered".to_string())
    );
}

#[test]
#[ignore = "requires dexcli dev"]
fn job_posting_update_reaches_both_job_boards() {
    let environment = DexEnvironment::start();
    let flow = JobPostingFlow::default();
    let flow_id = unique_flow_id("job-posting");
    let initial = JobPost {
        title: "Software Engineer".to_string(),
        description: "Build reliable systems".to_string(),
        ..JobPost::default()
    };
    environment
        .client
        .start_flow(&flow, &flow_id, initial.clone())
        .expect("start Rust Job Posting Flow");
    environment
        .client
        .wait_for_step_completion(&flow_id, StepExecutionId::of(&Init), step_wait_options(30))
        .expect("wait for Rust Job Posting Create Step");
    assert_eq!(
        environment
            .client
            .invoke_rpc_without_input(&flow_id, GET_JOB_POST)
            .expect("read initial Rust Job Posting"),
        initial
    );

    let updated = JobPost {
        title: "Senior Software Engineer".to_string(),
        description: "Build durable systems".to_string(),
        notes: "expanded scope".to_string(),
        deleted: false,
    };
    assert_eq!(
        environment
            .client
            .invoke_rpc(&flow_id, UPDATE_JOB_POST, updated)
            .expect("update Rust Job Posting"),
        1
    );
    let newest = JobPost {
        title: "Principal Software Engineer".to_string(),
        description: "Lead durable systems".to_string(),
        notes: "final scope".to_string(),
        deleted: false,
    };
    assert_eq!(
        environment
            .client
            .invoke_rpc(&flow_id, UPDATE_JOB_POST, newest.clone())
            .expect("update Rust Job Posting again"),
        2
    );
    environment
        .client
        .wait_for_step_completion(
            &flow_id,
            StepExecutionId::of(&UpdateLinkedInPosting::default()).execution_number(2),
            step_wait_options(30),
        )
        .expect("wait for Rust LinkedIn posting update");
    environment
        .client
        .wait_for_step_completion(
            &flow_id,
            StepExecutionId::of(&UpdateIndeedPosting::default()).execution_number(2),
            step_wait_options(30),
        )
        .expect("wait for Rust Indeed posting update");
    assert_eq!(
        environment
            .client
            .invoke_rpc_without_input(&flow_id, GET_JOB_POST)
            .expect("read updated Rust Job Posting"),
        newest
    );
    environment
        .client
        .stop_flow(&flow_id, StopFlowOptions::cancel())
        .expect("stop Rust Job Posting Flow");
}

#[test]
#[ignore = "requires dexcli dev"]
fn stream_resumes_after_step_and_client_writes() {
    let environment = DexEnvironment::start();
    let flow = StreamFlow::default();
    let flow_id = unique_flow_id("stream");
    environment
        .client
        .start_flow(&flow, &flow_id, "invoice".to_string())
        .expect("start Rust Stream Flow");

    let step_message = environment
        .client
        .read_stream_with_timeout(&flow_id, &PROGRESS, "", Duration::from_secs(20))
        .expect("read Rust Step Stream message");
    assert_eq!(
        step_message.value,
        "Rendering preview for invoicePreview ready for invoice"
    );
    assert!(!step_message.resume_token.is_empty());
    assert!(step_message.source.starts_with('#'));

    environment
        .client
        .write_stream(
            &flow_id,
            &PROGRESS,
            "browser/complete",
            "Preview displayed".to_string(),
        )
        .expect("write Rust Client Stream message");
    let client_message = environment
        .client
        .read_stream_with_timeout(
            &flow_id,
            &PROGRESS,
            &step_message.resume_token,
            Duration::from_secs(20),
        )
        .expect("resume Rust Stream");
    assert_eq!(client_message.value, "Preview displayed");
    assert_eq!(client_message.source, "browser/complete");

    let newest_page = environment
        .client
        .list_stream_messages(&flow_id, &PROGRESS, 1, "")
        .expect("list newest Rust Stream message");
    assert_eq!(newest_page.messages.len(), 1);
    assert_eq!(newest_page.messages[0].value, "Preview displayed");
    assert!(!newest_page.next_page_token.is_empty());

    let older_page = environment
        .client
        .list_stream_messages(&flow_id, &PROGRESS, 1, &newest_page.next_page_token)
        .expect("list older Rust Stream message");
    assert_eq!(older_page.messages.len(), 1);
    assert_eq!(
        older_page.messages[0].value,
        "Rendering preview for invoicePreview ready for invoice"
    );
}

#[test]
#[ignore = "requires dexcli dev"]
fn order_processing_happy_path() {
    let environment = DexEnvironment::start();
    let flow_id = unique_flow_id("order-processing");
    start_order_processing(&environment, &flow_id, false);
    environment
        .client
        .wait_for_step_completion(
            &flow_id,
            StepExecutionId::of(&Charge::default()),
            step_wait_options(30),
        )
        .expect("wait for Rust Order Processing ChargeStep");
    let approved: String = environment
        .client
        .invoke_rpc(&flow_id, APPROVE_ORDER, String::new())
        .expect("approve Rust Order Processing Flow");
    assert_eq!(approved, "ok");
    let output: String = environment
        .client
        .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(45))
        .expect("complete Rust Order Processing Flow")
        .single_output()
        .expect("decode Rust Order Processing output");
    assert_eq!(output, format!("shipped:{flow_id}"));
}

#[test]
#[ignore = "requires dexcli dev"]
fn order_processing_reminder_then_ship() {
    let environment = DexEnvironment::start();
    let flow_id = unique_flow_id("order-processing-reminder");
    start_order_processing(&environment, &flow_id, false);
    environment
        .client
        .wait_for_step_completion(
            &flow_id,
            StepExecutionId::of(&Charge::default()),
            step_wait_options(30),
        )
        .expect("wait for Rust Order Processing ChargeStep");
    skip_seller_reminder(&environment, &flow_id);
    environment
        .client
        .wait_for_step_completion(
            &flow_id,
            StepExecutionId::of(&Ship::default()),
            step_wait_options(30),
        )
        .expect("wait for Rust Order Processing reminder ShipStep");
    let approved: String = environment
        .client
        .invoke_rpc(&flow_id, APPROVE_ORDER, String::new())
        .expect("approve Rust Order Processing Flow after reminder");
    assert_eq!(approved, "ok");
    let output: String = environment
        .client
        .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(45))
        .expect("complete Rust Order Processing Flow after reminder")
        .single_output()
        .expect("decode Rust Order Processing output after reminder");
    assert_eq!(output, format!("shipped:{flow_id}"));
}

#[test]
#[ignore = "requires dexcli dev"]
fn order_processing_ship_failure_refunds() {
    let environment = DexEnvironment::start();
    let flow_id = unique_flow_id("order-processing-refund");
    start_order_processing(&environment, &flow_id, true);
    environment
        .client
        .wait_for_step_completion(
            &flow_id,
            StepExecutionId::of(&Charge::default()),
            step_wait_options(30),
        )
        .expect("wait for Rust Order Processing ChargeStep");
    let approved: String = environment
        .client
        .invoke_rpc(&flow_id, APPROVE_ORDER, String::new())
        .expect("approve Rust Order Processing Flow for refund");
    assert_eq!(approved, "ok");
    let output: String = environment
        .client
        .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(45))
        .expect("complete Rust Order Processing refund Flow")
        .single_output()
        .expect("decode Rust Order Processing refund output");
    assert_eq!(output, format!("refunded:{flow_id}"));
}

fn start_order_processing(
    environment: &DexEnvironment,
    flow_id: &str,
    test_fail_at_shipping: bool,
) {
    environment
        .client
        .start_flow(
            &OrderProcessingFlow::default(),
            flow_id,
            OrderRequest {
                order_id: flow_id.to_string(),
                email: "buyer@example.com".to_string(),
                customer_id: "customer-1".to_string(),
                amount: 42,
                test_fail_at_shipping,
            },
        )
        .expect("start Rust Order Processing Flow");
}

fn skip_seller_reminder(environment: &DexEnvironment, flow_id: &str) {
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut last_error = None;
    while Instant::now() < deadline {
        match environment.client.skip_timer(
            flow_id,
            StepExecutionId::of(&Ship::default()),
            TimerId::by_condition_index(0),
        ) {
            Ok(()) => return,
            Err(error) => last_error = Some(error),
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("skip timer did not succeed: {last_error:?}");
}

#[test]
#[ignore = "requires dexcli dev"]
fn engagement_invokes_rpcs_and_completes() {
    let environment = DexEnvironment::start();
    let flow = EngagementFlow::default();
    let flow_id = unique_flow_id("engagement");
    let run_id = environment
        .client
        .start_flow(
            &flow,
            &flow_id,
            EngagementRequest {
                employer_id: "released-sdk-employer".to_string(),
                candidate_id: "released-sdk-candidate".to_string(),
            },
        )
        .expect("start Rust Engagement Flow");
    assert!(!run_id.is_empty());

    let pending = environment.await_engagement_status(&flow_id, "pending");
    assert!(pending.notes.is_empty());
    environment
        .client
        .invoke_rpc(
            &flow_id,
            ACCEPT_ENGAGEMENT,
            "accepted in integration test".to_string(),
        )
        .expect("accept Rust Engagement Flow");
    environment
        .client
        .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
        .expect("complete Rust Engagement Flow");
    assert_eq!(
        environment
            .client
            .describe_flow(&flow_id)
            .expect("describe completed Rust Engagement Flow")
            .status,
        FlowStatus::Completed
    );
}

#[test]
#[ignore = "requires dexcli dev"]
fn user_onboarding_verifies_and_completes_both_tasks() {
    let environment = DexEnvironment::start();
    let flow = UserOnboardingFlow::default();
    let flow_id = unique_flow_id("user-onboarding");
    let run_id = environment
        .client
        .start_flow(&flow, &flow_id, format!("{flow_id}@example.com"))
        .expect("start Rust User Onboarding Flow");
    assert!(!run_id.is_empty());
    environment
        .client
        .wait_for_attribute_match(
            &flow_id,
            &ONBOARDING_STATUS,
            AttributeMatch::equal_to(WAITING_FOR_VERIFICATION.to_string()),
            attribute_wait_options(20),
        )
        .expect("wait for email verification");

    let verified: String = environment
        .client
        .invoke_rpc_without_input(&flow_id, VERIFY_ONBOARDING)
        .expect("verify onboarding email");
    assert_eq!(verified, "verified");
    environment
        .client
        .wait_for_attribute_match(
            &flow_id,
            &ONBOARDING_STATUS,
            AttributeMatch::equal_to(WAITING_FOR_TASK_1.to_string()),
            attribute_wait_options(20),
        )
        .expect("wait for onboarding task 1");

    let task_1: String = environment
        .client
        .invoke_rpc_without_input(&flow_id, ACCOMPLISH_ONBOARDING_TASK_1)
        .expect("accomplish onboarding task 1");
    assert_eq!(task_1, "task 1 accomplished");
    environment
        .client
        .wait_for_attribute_match(
            &flow_id,
            &ONBOARDING_STATUS,
            AttributeMatch::equal_to(WAITING_FOR_TASK_2.to_string()),
            attribute_wait_options(20),
        )
        .expect("wait for onboarding task 2");

    let task_2: String = environment
        .client
        .invoke_rpc_without_input(&flow_id, ACCOMPLISH_ONBOARDING_TASK_2)
        .expect("accomplish onboarding task 2");
    assert_eq!(task_2, "task 2 accomplished");
    let output: String = environment
        .client
        .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
        .and_then(|result| result.single_output())
        .expect("complete Rust User Onboarding Flow");
    assert_eq!(output, "onboarding completed");
}

#[test]
#[ignore = "requires dexcli dev"]
fn microservice_swaps_data_and_completes_when_ready() {
    let environment = DexEnvironment::start();
    let flow = OrchestrationFlow::default();
    let flow_id = unique_flow_id("microservice");
    let run_id = environment
        .client
        .start_flow(&flow, &flow_id, "initial-data".to_string())
        .expect("start Rust Microservice Flow");
    assert!(!run_id.is_empty());

    environment
        .client
        .wait_for_attribute_match(
            &flow_id,
            &DATA,
            AttributeMatch::equal_to("initial-data".to_string()),
            attribute_wait_options(20),
        )
        .expect("wait for initial Rust Microservice data");
    let previous = environment
        .client
        .invoke_rpc(
            &flow_id,
            SWAP_ORCHESTRATION_DATA,
            "updated-data".to_string(),
        )
        .expect("swap Rust Microservice data");
    assert_eq!(previous, "initial-data");
    environment
        .client
        .wait_for_attribute_match(
            &flow_id,
            &DATA,
            AttributeMatch::equal_to("updated-data".to_string()),
            attribute_wait_options(20),
        )
        .expect("wait for updated Rust Microservice data");
    environment
        .client
        .invoke_rpc_without_input::<()>(&flow_id, SIGNAL_ORCHESTRATION_READY)
        .expect("release Rust Microservice Flow");
    let output: String = environment
        .client
        .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
        .and_then(|result| result.single_output())
        .expect("complete Rust Microservice Flow");
    assert_eq!(output, "updated-data");
}

#[test]
#[ignore = "requires dexcli dev"]
fn subscription_updates_charge_and_cancels() {
    let environment = DexEnvironment::start();
    let flow = SubscriptionFlow::default();
    let flow_id = unique_flow_id("subscription");
    let run_id = environment
        .client
        .start_flow(
            &flow,
            &flow_id,
            SubscriptionRequest {
                customer_id: "released-sdk-customer".to_string(),
                charge_cents: 100,
                billing_periods: 2,
            },
        )
        .expect("start Rust Subscription Flow");
    assert!(!run_id.is_empty());

    let initial = environment.await_subscription_charge(&flow_id, 100);
    assert_eq!(initial.periods_charged, 0);
    assert!(!initial.cancelled);
    environment
        .client
        .invoke_rpc(&flow_id, UPDATE_SUBSCRIPTION_CHARGE, 250)
        .expect("update Rust Subscription charge");
    let updated = environment.await_subscription_charge(&flow_id, 250);
    assert!(!updated.cancelled);
    environment
        .client
        .invoke_rpc_without_input::<()>(&flow_id, CANCEL_SUBSCRIPTION)
        .expect("cancel Rust Subscription Flow");

    let output: SubscriptionState = environment
        .client
        .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
        .expect("complete Rust Subscription Flow")
        .single_output()
        .expect("decode Rust Subscription output");
    assert_eq!(output.charge_cents, 250);
    assert_eq!(output.periods_charged, 0);
    assert!(output.cancelled);
}

#[test]
#[ignore = "requires dexcli dev"]
fn failure_recovery_retries_and_compensates() {
    let environment = DexEnvironment::start();
    let flow = FailureRecoveryFlow::default();
    let flow_id = unique_flow_id("failure-recovery");
    let run_id = environment
        .client
        .start_flow(&flow, &flow_id, "released-sdk-reservation".to_string())
        .expect("start Rust Failure Recovery Flow");
    assert!(!run_id.is_empty());

    let output: String = environment
        .client
        .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
        .expect("complete Rust Failure Recovery Flow")
        .single_output()
        .expect("decode Rust Failure Recovery output");
    assert_eq!(output, "compensated");
    assert_eq!(
        environment
            .client
            .describe_flow(&flow_id)
            .expect("describe completed Rust Failure Recovery Flow")
            .status,
        FlowStatus::Completed
    );
}

#[test]
#[ignore = "requires dexcli dev"]
fn polling_streams_job_progress_and_completes_with_job_result() {
    let environment = DexEnvironment::start();
    let flow = PollingFlow::new(Arc::clone(&environment.polling_jobs));
    let flow_id = unique_flow_id("pattern-polling");
    environment
        .client
        .start_flow(&flow, &flow_id, ())
        .expect("start Rust Polling Flow");

    let queued = environment
        .client
        .read_stream_with_timeout(&flow_id, &JOB_PROGRESS, "", Duration::from_secs(20))
        .expect("read QUEUED JobProgress message");
    assert_eq!(queued.value.state, JobState::Queued);
    let running = environment
        .client
        .read_stream_with_timeout(
            &flow_id,
            &JOB_PROGRESS,
            &queued.resume_token,
            Duration::from_secs(20),
        )
        .expect("read RUNNING JobProgress message");
    assert_eq!(running.value.state, JobState::Running);

    environment
        .polling_jobs
        .complete_job(&flow_id)
        .expect("complete the fake job");
    let result = environment
        .client
        .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
        .expect("complete Rust Polling Flow");
    assert_eq!(result.status(), FlowStatus::Completed);
    let output: String = result.single_output().expect("decode Rust Polling output");
    assert_eq!(output, format!("artifact for {flow_id}"));
}

fn attribute_wait_options(seconds: u64) -> WaitForAttributeOptions {
    WaitForAttributeOptions::new().request_timeout(Duration::from_secs(seconds))
}

fn step_wait_options(seconds: u64) -> WaitForStepCompletionOptions {
    WaitForStepCompletionOptions::new().request_timeout(Duration::from_secs(seconds))
}

fn unique_flow_id(prefix: &str) -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("current time after Unix epoch")
        .as_nanos();
    format!("rust-examples-{prefix}-{}-{timestamp}", std::process::id())
}

fn available_worker_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind an available Worker port")
        .local_addr()
        .expect("read available Worker port")
        .port()
}

fn await_worker(worker_port: u16, worker_thread: &JoinHandle<SdkResult<()>>) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        assert!(
            !worker_thread.is_finished(),
            "Rust examples Worker exited before becoming ready"
        );
        if TcpStream::connect(("127.0.0.1", worker_port)).is_ok() {
            return;
        }
        thread::yield_now();
    }
    panic!("Rust examples Worker did not become ready");
}
