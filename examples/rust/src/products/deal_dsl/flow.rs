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

use std::{collections::BTreeMap, sync::LazyLock};

use dex_sdk::{
    Attribute, ChannelMap, Context, Flow, HandlerError, HandlerResult, PersistenceSchema, Rpc,
    RpcList, Step, StepDecision, StepList, Wait,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct DealCondition {
    pub name: String,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct DealCase {
    pub equals: String,
    pub go_to_state: String,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct DealTransition {
    pub else_state: String,
    pub wait_for: Option<DealCondition>,
    pub key: String,
    pub cases: Vec<DealCase>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct DealState {
    pub name: String,
    pub pre_condition: Option<DealCondition>,
    pub actions: Vec<String>,
    pub transition: Option<DealTransition>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct DealDefinition {
    pub process_id: String,
    pub item_id: String,
    pub item_name: String,
    pub initial_state: String,
    pub initial_state_data: BTreeMap<String, String>,
    pub states: Vec<DealState>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct DealStart {
    pub definition: DealDefinition,
    pub buyer_id: String,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ConditionMessage {
    pub condition_name: String,
    pub values: BTreeMap<String, String>,
}

pub const SEND_DEAL_CONDITION_MESSAGE: Rpc<ConditionMessage, ()> =
    Rpc::new("SendDealConditionMessage");

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct StateStepInput {
    pub state_name: String,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct ActionStepInput {
    pub state_name: String,
    pub action_index: usize,
}

#[derive(Default)]
pub struct DealDSLFlow {
    initialize: InitializeDeal,
    wait_for_condition: WaitForDealCondition,
    execute_action: ExecuteDealAction,
    evaluate_transition: EvaluateDealTransition,
}

impl DealDSLFlow {
    fn send_condition_message(
        &self,
        context: &mut Context,
        message: ConditionMessage,
    ) -> HandlerResult<()> {
        DEAL_CONDITION_MESSAGES.publish(context, &message.condition_name, message.values)
    }
}

impl Flow for DealDSLFlow {
    type StartInput = DealStart;

    fn steps(&self) -> StepList<'_, Self::StartInput> {
        StepList::start(&self.initialize)
            .and(&self.wait_for_condition)
            .and(&self.execute_action)
            .and(&self.evaluate_transition)
    }

    fn persistence(&self) -> PersistenceSchema {
        PersistenceSchema::new()
            .attribute(&DEAL_DEFINITION)
            .attribute(&DEAL_STATE_DATA)
            .attribute(&DEAL_PROCESS_ID)
            .attribute(&DEAL_ITEM_ID)
            .attribute(&DEAL_BUYER_ID)
            .attribute(&DEAL_CURRENT_STATE)
            .attribute(&DEAL_PENDING_CONDITION)
            .channel_map(&DEAL_CONDITION_MESSAGES)
    }

    fn rpcs(&self) -> RpcList<Self> {
        RpcList::new().procedure(SEND_DEAL_CONDITION_MESSAGE, Self::send_condition_message)
    }
}

#[derive(Default)]
pub(super) struct InitializeDeal;

impl Step for InitializeDeal {
    type Input = DealStart;

    fn execute(&self, context: &mut Context, input: Self::Input) -> HandlerResult<StepDecision> {
        deal_state(&input.definition, &input.definition.initial_state)?;
        DEAL_DEFINITION.set(context, input.definition.clone())?;
        DEAL_PROCESS_ID.set(context, input.definition.process_id.clone())?;
        DEAL_ITEM_ID.set(context, input.definition.item_id.clone())?;
        DEAL_BUYER_ID.set(context, input.buyer_id)?;
        DEAL_STATE_DATA.set(context, input.definition.initial_state_data)?;
        Ok(StepDecision::go_to(
            &WaitForDealCondition,
            StateStepInput {
                state_name: input.definition.initial_state,
            },
        ))
    }
}

#[derive(Default)]
pub(super) struct WaitForDealCondition;

impl Step for WaitForDealCondition {
    type Input = StateStepInput;

    fn wait_for(&self, context: &mut Context, input: Self::Input) -> HandlerResult<Wait> {
        let definition = DEAL_DEFINITION.get_required(context)?;
        let state = deal_state(&definition, &input.state_name)?;
        let Some(condition) = state.pre_condition else {
            return Ok(Wait::skip_immediately());
        };
        DEAL_PENDING_CONDITION.set(context, condition.name.clone())?;
        Ok(Wait::until(
            DEAL_CONDITION_MESSAGES.for_one(&condition.name),
        ))
    }

    fn execute(&self, context: &mut Context, input: Self::Input) -> HandlerResult<StepDecision> {
        let definition = DEAL_DEFINITION.get_required(context)?;
        let state = deal_state(&definition, &input.state_name)?;
        if let Some(condition) = state.pre_condition {
            merge_condition(context, &condition.name)?;
            DEAL_PENDING_CONDITION.delete(context)?;
        }
        DEAL_CURRENT_STATE.set(context, state.name.clone())?;
        if state.actions.is_empty() {
            return Ok(StepDecision::go_to(&EvaluateDealTransition, input));
        }
        Ok(StepDecision::go_to(
            &ExecuteDealAction,
            ActionStepInput {
                state_name: state.name,
                action_index: 0,
            },
        ))
    }
}

#[derive(Default)]
pub(super) struct ExecuteDealAction;

impl Step for ExecuteDealAction {
    type Input = ActionStepInput;

    fn execute(&self, context: &mut Context, input: Self::Input) -> HandlerResult<StepDecision> {
        let definition = DEAL_DEFINITION.get_required(context)?;
        let state = deal_state(&definition, &input.state_name)?;
        let action = state.actions.get(input.action_index).ok_or_else(|| {
            HandlerError::new(
                "InvalidDealDSL",
                format!("invalid action index {}", input.action_index),
            )
        })?;
        run_action(context, action)?;
        let next_index = input.action_index + 1;
        if next_index < state.actions.len() {
            return Ok(StepDecision::go_to(
                &ExecuteDealAction,
                ActionStepInput {
                    state_name: state.name,
                    action_index: next_index,
                },
            ));
        }
        Ok(StepDecision::go_to(
            &EvaluateDealTransition,
            StateStepInput {
                state_name: state.name,
            },
        ))
    }
}

#[derive(Default)]
pub(super) struct EvaluateDealTransition;

impl Step for EvaluateDealTransition {
    type Input = StateStepInput;

    fn wait_for(&self, context: &mut Context, input: Self::Input) -> HandlerResult<Wait> {
        let definition = DEAL_DEFINITION.get_required(context)?;
        let state = deal_state(&definition, &input.state_name)?;
        let Some(condition) = state.transition.and_then(|transition| transition.wait_for) else {
            return Ok(Wait::skip_immediately());
        };
        DEAL_PENDING_CONDITION.set(context, condition.name.clone())?;
        Ok(Wait::until(
            DEAL_CONDITION_MESSAGES.for_one(&condition.name),
        ))
    }

    fn execute(&self, context: &mut Context, input: Self::Input) -> HandlerResult<StepDecision> {
        let definition = DEAL_DEFINITION.get_required(context)?;
        let state = deal_state(&definition, &input.state_name)?;
        let Some(transition) = state.transition else {
            return Ok(StepDecision::graceful_complete(
                DEAL_STATE_DATA.get_required(context)?,
            ));
        };
        if let Some(condition) = transition.wait_for {
            merge_condition(context, &condition.name)?;
            DEAL_PENDING_CONDITION.delete(context)?;
        }
        let state_data = DEAL_STATE_DATA.get_required(context)?;
        let value = state_data.get(&transition.key);
        let next_state = transition
            .cases
            .iter()
            .find(|deal_case| value == Some(&deal_case.equals))
            .map_or(transition.else_state, |deal_case| {
                deal_case.go_to_state.clone()
            });
        Ok(StepDecision::go_to(
            &WaitForDealCondition,
            StateStepInput {
                state_name: next_state,
            },
        ))
    }
}

fn deal_state(definition: &DealDefinition, name: &str) -> HandlerResult<DealState> {
    definition
        .states
        .iter()
        .find(|state| state.name == name)
        .cloned()
        .ok_or_else(|| {
            HandlerError::new(
                "InvalidDealDSL",
                format!("deal state {name} is not defined"),
            )
        })
}

fn merge_condition(context: &mut Context, condition_name: &str) -> HandlerResult<()> {
    let mut messages = DEAL_CONDITION_MESSAGES.condition_results(context, condition_name)?;
    if messages.len() != 1 {
        return Err(HandlerError::new(
            "InvalidDealCondition",
            format!("condition {condition_name} requires one message"),
        ));
    }
    let mut state_data = DEAL_STATE_DATA.get_required(context)?;
    state_data.extend(messages.pop().expect("message length checked"));
    DEAL_STATE_DATA.set(context, state_data)
}

fn run_action(context: &mut Context, action_name: &str) -> HandlerResult<()> {
    let mut state_data = DEAL_STATE_DATA.get_required(context)?;
    match action_name {
        "chargeBuyer" => {}
        "deliverItemToBuyer" => {
            state_data.insert("itemDeliveryStatus".to_string(), "delivered".to_string());
        }
        _ => {
            return Err(HandlerError::new(
                "UnknownDealAction",
                format!("deal action {action_name} is not registered"),
            ));
        }
    }
    state_data.insert("lastAction".to_string(), action_name.to_string());
    DEAL_STATE_DATA.set(context, state_data)
}

pub fn example_deal_start(buyer_id: &str) -> DealStart {
    DealStart {
        buyer_id: buyer_id.to_string(),
        definition: DealDefinition {
            process_id: "item-deal-v1".to_string(),
            item_id: "item-42".to_string(),
            item_name: "Any sellable item".to_string(),
            initial_state: "negotiating".to_string(),
            initial_state_data: BTreeMap::from([("accepted".to_string(), "false".to_string())]),
            states: vec![
                DealState {
                    name: "negotiating".to_string(),
                    transition: Some(DealTransition {
                        wait_for: Some(DealCondition {
                            name: "buyer-decision".to_string(),
                        }),
                        key: "accepted".to_string(),
                        cases: vec![DealCase {
                            equals: "true".to_string(),
                            go_to_state: "fulfill".to_string(),
                        }],
                        else_state: "declined".to_string(),
                    }),
                    ..DealState::default()
                },
                DealState {
                    name: "fulfill".to_string(),
                    actions: vec!["chargeBuyer".to_string(), "deliverItemToBuyer".to_string()],
                    ..DealState::default()
                },
                DealState {
                    name: "declined".to_string(),
                    ..DealState::default()
                },
            ],
        },
    }
}

static DEAL_DEFINITION: LazyLock<Attribute<DealDefinition>> =
    LazyLock::new(|| Attribute::new("DealDefinition"));
static DEAL_STATE_DATA: LazyLock<Attribute<BTreeMap<String, String>>> =
    LazyLock::new(|| Attribute::new("DealStateData"));
static DEAL_PROCESS_ID: LazyLock<Attribute<String>> =
    LazyLock::new(|| Attribute::new("DealProcessID"));
static DEAL_ITEM_ID: LazyLock<Attribute<String>> = LazyLock::new(|| Attribute::new("DealItemID"));
static DEAL_BUYER_ID: LazyLock<Attribute<String>> = LazyLock::new(|| Attribute::new("DealBuyerID"));
pub static DEAL_CURRENT_STATE: LazyLock<Attribute<String>> =
    LazyLock::new(|| Attribute::new("DealCurrentState"));
static DEAL_PENDING_CONDITION: LazyLock<Attribute<String>> =
    LazyLock::new(|| Attribute::new("DealPendingCondition"));
pub(super) static DEAL_CONDITION_MESSAGES: LazyLock<ChannelMap<BTreeMap<String, String>>> =
    LazyLock::new(|| ChannelMap::new("DealConditionMessages"));
