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

use std::sync::{Arc, LazyLock};
use std::thread;
use std::time::Duration;

use dex_sdk::{
    Attribute, Channel, Client, Context, Flow, FlowStatus, HandlerError, HandlerResult,
    IdReusePolicy, PersistenceSchema, Rpc, RpcList, RpcResult, SdkError, StartFlowOptions, Step,
    StepDecision, StepList, StepMovement, StepOptions, StopFlowOptions, SubFlow, Wait,
};
use serde::{Deserialize, Serialize};

const DEFAULT_CONCURRENCY: usize = 10;
const MAX_BUFFERED_REQUESTS: usize = 100;

static REQUEST_CHANNEL: LazyLock<Channel<String>> =
    LazyLock::new(|| Channel::new("RequestChannel"));
static STOPPED: LazyLock<Attribute<bool>> = LazyLock::new(|| Attribute::new("Stopped"));
static CURR_SUB_FLOW_NUM: LazyLock<Attribute<usize>> =
    LazyLock::new(|| Attribute::new("CurrSubFlowNum"));
static SUB_FLOW_COMPLETED_CH: LazyLock<Channel<bool>> =
    LazyLock::new(|| Channel::new("SubFlowCompletedCh"));
static ALL_DONE_CH: LazyLock<Channel<bool>> = LazyLock::new(|| Channel::new("AllDoneCh"));

#[derive(Clone, Deserialize, Serialize)]
pub struct ParentInput {
    pub requests: Vec<String>,
    pub concurrency: usize,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct SubmitRequestInput {
    pub request: String,
    pub parent_ids: Vec<String>,
}

#[derive(Default)]
pub struct ExampleSubFlow {
    do_work: DoWork,
}

impl Flow for ExampleSubFlow {
    type StartInput = String;

    fn steps(&self) -> StepList<'_, Self::StartInput> {
        StepList::start(&self.do_work)
    }
}

#[derive(Default)]
struct DoWork;

impl Step for DoWork {
    type Input = String;

    fn execute(&self, _context: &mut Context, request: Self::Input) -> HandlerResult<StepDecision> {
        thread::sleep(Duration::from_millis(50 + (request.len() as u64 % 10) * 50));
        Ok(StepDecision::graceful_complete(request))
    }
}

#[derive(Default)]
pub struct BasicParentFlow {
    sub_flows: SubFlowsStep,
}

impl Flow for BasicParentFlow {
    type StartInput = Vec<String>;

    fn steps(&self) -> StepList<'_, Self::StartInput> {
        StepList::start(&self.sub_flows)
    }
}

#[derive(Default)]
struct SubFlowsStep;

impl Step for SubFlowsStep {
    type Input = Vec<String>;

    fn wait_for(&self, _context: &mut Context, requests: Self::Input) -> HandlerResult<Wait> {
        let child = ExampleSubFlow::default();
        let mut conditions = Vec::with_capacity(requests.len());
        for request in requests {
            conditions.push(SubFlow::run(&child, request).map_err(HandlerError::from_error)?);
        }
        Ok(Wait::all_of(conditions))
    }

    fn execute(
        &self,
        _context: &mut Context,
        _requests: Self::Input,
    ) -> HandlerResult<StepDecision> {
        Ok(StepDecision::graceful_complete(()))
    }
}

#[derive(Default)]
pub struct WaitForHalfParentFlow {
    init: WaitForHalfInit,
    sub_flow: SubFlowStep,
    wait_sub_flows: WaitSubFlows,
}

impl WaitForHalfParentFlow {
    pub fn new(client: Arc<Client>) -> Self {
        Self {
            sub_flow: SubFlowStep {
                client: Some(client),
            },
            ..Self::default()
        }
    }
}

impl Flow for WaitForHalfParentFlow {
    type StartInput = Vec<String>;

    fn steps(&self) -> StepList<'_, Self::StartInput> {
        StepList::start(&self.init)
            .and(&self.sub_flow)
            .and(&self.wait_sub_flows)
    }

    fn persistence(&self) -> PersistenceSchema {
        PersistenceSchema::new()
            .channel(&SUB_FLOW_COMPLETED_CH)
            .channel(&ALL_DONE_CH)
    }
}

#[derive(Default)]
struct WaitForHalfInit;

impl Step for WaitForHalfInit {
    type Input = Vec<String>;

    fn step_type(&self) -> &'static str {
        "InitStep"
    }

    fn execute(
        &self,
        _context: &mut Context,
        requests: Self::Input,
    ) -> HandlerResult<StepDecision> {
        if requests.is_empty() {
            return Ok(StepDecision::graceful_complete(()));
        }
        let mut movements = Vec::with_capacity(requests.len() + 1);
        movements.push(StepMovement::to(&WaitSubFlows, requests.len()));
        movements.extend(
            requests
                .into_iter()
                .map(|request| StepMovement::to(&SubFlowStep::default(), request)),
        );
        Ok(StepDecision::go_to_many(movements))
    }
}

#[derive(Default)]
struct SubFlowStep {
    client: Option<Arc<Client>>,
}

impl Step for SubFlowStep {
    type Input = String;

    fn wait_for(&self, _context: &mut Context, request: Self::Input) -> HandlerResult<Wait> {
        let sub_flow =
            SubFlow::run(&ExampleSubFlow::default(), request).map_err(HandlerError::from_error)?;
        Ok(Wait::any_of([sub_flow, ALL_DONE_CH.for_one()]))
    }

    fn execute(&self, context: &mut Context, _request: Self::Input) -> HandlerResult<StepDecision> {
        if SubFlow::condition_result(context)?.status() != FlowStatus::Running {
            SUB_FLOW_COMPLETED_CH.publish(context, true)?;
            return Ok(StepDecision::graceful_complete(()));
        }
        let client = self.client.as_ref().ok_or_else(|| {
            HandlerError::new("ParallelSubFlows", "Dex client is not initialized")
        })?;
        client
            .stop_flow(
                &SubFlow::flow_id(context)?,
                StopFlowOptions::cancel().reason("enough SubFlows completed"),
            )
            .map_err(HandlerError::from_error)?;
        Ok(StepDecision::graceful_complete(()))
    }
}

#[derive(Default)]
struct WaitSubFlows;

impl Step for WaitSubFlows {
    type Input = usize;

    fn wait_for(&self, _context: &mut Context, total: Self::Input) -> HandlerResult<Wait> {
        Ok(Wait::until(SUB_FLOW_COMPLETED_CH.for_n(total.div_ceil(2))))
    }

    fn execute(&self, context: &mut Context, total: Self::Input) -> HandlerResult<StepDecision> {
        for _ in 0..total - total.div_ceil(2) {
            ALL_DONE_CH.publish(context, true)?;
        }
        Ok(StepDecision::graceful_complete(()))
    }
}

pub(super) const SEND_LONG_LIVE_REQUEST: Rpc<String, bool> = Rpc::new("SendLongLiveRequest");
pub(super) const STOP_LONG_LIVE_FLOW: Rpc<(), ()> = Rpc::new("StopLongLiveFlow");

#[derive(Default)]
pub struct AdvancedLongLiveParentFlow {
    init: LongLiveInit,
    handle_request: LongLiveHandleRequest,
    handle_sub_flow: LongLiveHandleSubFlow,
}

impl AdvancedLongLiveParentFlow {
    fn send_request(
        &self,
        context: &mut Context,
        request: String,
    ) -> HandlerResult<RpcResult<bool>> {
        if REQUEST_CHANNEL.size(context)? >= MAX_BUFFERED_REQUESTS {
            return Ok(RpcResult::new(false));
        }
        REQUEST_CHANNEL.publish(context, request)?;
        Ok(RpcResult::new(true))
    }

    fn stop(&self, context: &mut Context) -> HandlerResult<RpcResult<()>> {
        STOPPED.set(context, true)?;
        Ok(RpcResult::new(()))
    }
}

impl Flow for AdvancedLongLiveParentFlow {
    type StartInput = ParentInput;

    fn steps(&self) -> StepList<'_, Self::StartInput> {
        StepList::start(&self.init)
            .and(&self.handle_request)
            .and(&self.handle_sub_flow)
    }

    fn persistence(&self) -> PersistenceSchema {
        PersistenceSchema::new()
            .attribute(&STOPPED)
            .channel(&REQUEST_CHANNEL)
    }

    fn rpcs(&self) -> RpcList<Self> {
        RpcList::new()
            .function(SEND_LONG_LIVE_REQUEST, Self::send_request)
            .function_without_input(STOP_LONG_LIVE_FLOW, Self::stop)
    }
}

#[derive(Default)]
struct LongLiveInit;

impl Step for LongLiveInit {
    type Input = ParentInput;

    fn step_type(&self) -> &'static str {
        "InitStep"
    }

    fn execute(&self, context: &mut Context, input: Self::Input) -> HandlerResult<StepDecision> {
        for request in input.requests {
            REQUEST_CHANNEL.publish(context, request)?;
        }
        STOPPED.set(context, false)?;
        let concurrency = if input.concurrency == 0 {
            DEFAULT_CONCURRENCY
        } else {
            input.concurrency
        };
        Ok(StepDecision::go_to_many(
            (0..concurrency).map(|_| StepMovement::to(&LongLiveHandleRequest, ())),
        ))
    }
}

#[derive(Default)]
struct LongLiveHandleRequest;

impl Step for LongLiveHandleRequest {
    type Input = ();

    fn step_type(&self) -> &'static str {
        "HandleRequestStep"
    }

    fn wait_for(&self, _context: &mut Context, _input: ()) -> HandlerResult<Wait> {
        Ok(Wait::until(REQUEST_CHANNEL.for_one()))
    }

    fn execute(&self, context: &mut Context, _input: ()) -> HandlerResult<StepDecision> {
        let request = REQUEST_CHANNEL
            .condition_results(context)?
            .into_iter()
            .next()
            .unwrap_or_default();
        Ok(StepDecision::go_to(&LongLiveHandleSubFlow, request))
    }
}

#[derive(Default)]
struct LongLiveHandleSubFlow;

impl Step for LongLiveHandleSubFlow {
    type Input = String;

    fn step_type(&self) -> &'static str {
        "HandleSubFlowStep"
    }

    fn wait_for(&self, _context: &mut Context, request: String) -> HandlerResult<Wait> {
        let child = ExampleSubFlow::default();
        Ok(Wait::until(
            SubFlow::run(&child, request).map_err(HandlerError::from_error)?,
        ))
    }

    fn execute(&self, context: &mut Context, _request: String) -> HandlerResult<StepDecision> {
        if STOPPED.get(context)?.unwrap_or(false) {
            return Ok(StepDecision::graceful_complete(()));
        }
        Ok(StepDecision::go_to(&LongLiveHandleRequest, ()))
    }
}

pub(super) const SEND_SHORT_LIVE_REQUEST: Rpc<String, bool> = Rpc::new("SendShortLiveRequest");

#[derive(Default)]
pub struct AdvancedShortLiveParentFlow {
    init: ShortLiveInit,
    handle_request: ShortLiveHandleRequest,
    handle_sub_flow: ShortLiveHandleSubFlow,
}

impl AdvancedShortLiveParentFlow {
    fn send_request(
        &self,
        context: &mut Context,
        request: String,
    ) -> HandlerResult<RpcResult<bool>> {
        if REQUEST_CHANNEL.size(context)? >= MAX_BUFFERED_REQUESTS {
            return Ok(RpcResult::new(false));
        }
        REQUEST_CHANNEL.publish(context, request)?;
        Ok(RpcResult::new(true))
    }
}

impl Flow for AdvancedShortLiveParentFlow {
    type StartInput = ParentInput;

    fn steps(&self) -> StepList<'_, Self::StartInput> {
        StepList::start(&self.init)
            .and(&self.handle_request)
            .and(&self.handle_sub_flow)
    }

    fn persistence(&self) -> PersistenceSchema {
        PersistenceSchema::new()
            .attribute(&CURR_SUB_FLOW_NUM)
            .channel(&REQUEST_CHANNEL)
    }

    fn rpcs(&self) -> RpcList<Self> {
        RpcList::new().function(SEND_SHORT_LIVE_REQUEST, Self::send_request)
    }
}

#[derive(Default)]
struct ShortLiveInit;

impl Step for ShortLiveInit {
    type Input = ParentInput;

    fn step_type(&self) -> &'static str {
        "InitStep"
    }

    fn execute(&self, context: &mut Context, input: Self::Input) -> HandlerResult<StepDecision> {
        for request in input.requests {
            REQUEST_CHANNEL.publish(context, request)?;
        }
        CURR_SUB_FLOW_NUM.set(context, 0)?;
        let concurrency = if input.concurrency == 0 {
            DEFAULT_CONCURRENCY
        } else {
            input.concurrency
        };
        Ok(StepDecision::go_to_many(
            (0..concurrency).map(|_| StepMovement::to(&ShortLiveHandleRequest, ())),
        ))
    }
}

#[derive(Default)]
struct ShortLiveHandleRequest;

impl Step for ShortLiveHandleRequest {
    type Input = ();

    fn step_type(&self) -> &'static str {
        "HandleRequestStep"
    }

    fn options(&self) -> StepOptions<Self::Input> {
        StepOptions::new().execute_lock(CURR_SUB_FLOW_NUM.lock())
    }

    fn wait_for(&self, _context: &mut Context, _input: ()) -> HandlerResult<Wait> {
        Ok(Wait::until(REQUEST_CHANNEL.for_one()))
    }

    fn execute(&self, context: &mut Context, _input: ()) -> HandlerResult<StepDecision> {
        let request = REQUEST_CHANNEL
            .condition_results(context)?
            .into_iter()
            .next()
            .unwrap_or_default();
        let current = CURR_SUB_FLOW_NUM.get(context)?.unwrap_or(0);
        CURR_SUB_FLOW_NUM.set(context, current + 1)?;
        Ok(StepDecision::go_to(&ShortLiveHandleSubFlow, request))
    }
}

#[derive(Default)]
struct ShortLiveHandleSubFlow;

impl Step for ShortLiveHandleSubFlow {
    type Input = String;

    fn step_type(&self) -> &'static str {
        "HandleSubFlowStep"
    }

    fn options(&self) -> StepOptions<Self::Input> {
        StepOptions::new().execute_lock(CURR_SUB_FLOW_NUM.lock())
    }

    fn wait_for(&self, _context: &mut Context, request: String) -> HandlerResult<Wait> {
        let child = ExampleSubFlow::default();
        Ok(Wait::until(
            SubFlow::run(&child, request).map_err(HandlerError::from_error)?,
        ))
    }

    fn execute(&self, context: &mut Context, _request: String) -> HandlerResult<StepDecision> {
        let current = CURR_SUB_FLOW_NUM.get(context)?.unwrap_or(1) - 1;
        CURR_SUB_FLOW_NUM.set(context, current)?;
        if current == 0 {
            return Ok(StepDecision::force_complete_if_channels_empty(
                (),
                StepMovement::to(&ShortLiveHandleRequest, ()),
                [REQUEST_CHANNEL.when_empty()],
            ));
        }
        Ok(StepDecision::go_to(&ShortLiveHandleRequest, ()))
    }
}

#[derive(Default)]
pub struct SubmitRequestFlow {
    submit: SubmitStep,
}

impl SubmitRequestFlow {
    pub fn new(client: Arc<Client>) -> Self {
        Self {
            submit: SubmitStep {
                client: Some(client),
            },
        }
    }
}

impl Flow for SubmitRequestFlow {
    type StartInput = SubmitRequestInput;

    fn steps(&self) -> StepList<'_, Self::StartInput> {
        StepList::start(&self.submit)
    }
}

#[derive(Default)]
struct SubmitStep {
    client: Option<Arc<Client>>,
}

impl Step for SubmitStep {
    type Input = SubmitRequestInput;

    fn execute(&self, _context: &mut Context, input: Self::Input) -> HandlerResult<StepDecision> {
        if input.parent_ids.is_empty() {
            return Err(HandlerError::new(
                "ParallelSubFlows",
                "at least one parent Flow ID is required",
            ));
        }
        let parent_id = &input.parent_ids[partition(&input.request, input.parent_ids.len())];
        let client = self.client.as_ref().ok_or_else(|| {
            HandlerError::new("ParallelSubFlows", "Dex client is not initialized")
        })?;
        let accepted = enqueue_request(client, parent_id, input.request)?;
        if !accepted {
            return Err(HandlerError::new(
                "ParallelSubFlows",
                format!("parent {parent_id} rejected the request"),
            ));
        }
        Ok(StepDecision::graceful_complete(parent_id.clone()))
    }
}

fn enqueue_request(client: &Client, parent_id: &str, request: String) -> HandlerResult<bool> {
    match client.invoke_rpc(parent_id, SEND_SHORT_LIVE_REQUEST, request.clone()) {
        Ok(accepted) => Ok(accepted),
        Err(SdkError::FlowNotFound { .. } | SdkError::FlowNotActiveOrNotFound { .. }) => {
            let parent = AdvancedShortLiveParentFlow::default();
            let input = ParentInput {
                requests: vec![request.clone()],
                concurrency: DEFAULT_CONCURRENCY,
            };
            let options = StartFlowOptions::new().id_reuse_policy(IdReusePolicy::AllowIfNotRunning);
            client
                .start_flow_with_options(&parent, parent_id, input, options)
                .map_err(HandlerError::from_error)?;
            Ok(true)
        }
        Err(error) => Err(HandlerError::from_error(error)),
    }
}

fn partition(request: &str, partitions: usize) -> usize {
    let mut hash = 2_166_136_261_u32;
    for byte in request.bytes() {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(16_777_619);
    }
    hash as usize % partitions
}
