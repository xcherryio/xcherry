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

use std::sync::LazyLock;

use dex_sdk::{
    Attribute, AttributeIndex, AttributeMap, Context, Flow, FlowConfig, HandlerResult,
    PersistenceSchema, Rpc, RpcList, RpcResult, Step, StepDecision, StepList, StepOptions, Wait,
};

const UPDATE_STATUS: Rpc<String, String> = Rpc::new("UpdateAttributeStatus");

static STATUS: LazyLock<Attribute<String>> = LazyLock::new(|| {
    Attribute::new("primitive-attribute-status")
        .indexed(AttributeIndex::keyword().with_key("keyword1"))
});
static EMAIL: LazyLock<Attribute<String>> =
    LazyLock::new(|| Attribute::new("primitive-attribute-email").sync_to_attribute_store());
static MODEL_API_KEY: LazyLock<Attribute<String>> =
    LazyLock::new(|| Attribute::new("primitive-attribute-model-api-key").hard_delete_on_null());

pub fn attribute_store_config() -> FlowConfig {
    FlowConfig::new().attribute_store_names(vec!["profiles".to_owned()])
}

pub struct AttributeFlow {
    progress: AttributeMap<String>,
    start: AttributeStep,
}

impl Default for AttributeFlow {
    fn default() -> Self {
        let progress = AttributeMap::new("primitive-attribute-progress")
            .indexed(AttributeIndex::keyword().with_key("keyword2"));
        Self {
            start: AttributeStep {
                progress: progress.clone(),
            },
            progress,
        }
    }
}

impl Flow for AttributeFlow {
    type StartInput = String;

    fn steps(&self) -> StepList<'_, Self::StartInput> {
        StepList::start(&self.start)
    }

    fn persistence(&self) -> PersistenceSchema {
        PersistenceSchema::new()
            .attribute(&STATUS)
            .attribute(&EMAIL)
            .attribute(&MODEL_API_KEY)
            .attribute_map(&self.progress)
    }

    fn rpcs(&self) -> RpcList<Self> {
        RpcList::new().function(
            UPDATE_STATUS
                .lock(STATUS.lock())
                .lock(self.progress.lock("payment")),
            Self::update_status,
        )
    }
}

impl AttributeFlow {
    fn update_status(
        &self,
        context: &mut Context,
        input: String,
    ) -> HandlerResult<RpcResult<String>> {
        STATUS.set(context, input.clone())?;
        self.progress.set(context, "payment", input.clone())?;
        Ok(RpcResult::new(input))
    }
}

struct AttributeStep {
    progress: AttributeMap<String>,
}

impl Step for AttributeStep {
    type Input = String;

    fn options(&self) -> StepOptions<Self::Input> {
        StepOptions::new()
            .wait_for_lock(STATUS.lock())
            .wait_for_lock(self.progress.lock("payment"))
            .execute_lock(STATUS.lock())
            .execute_lock(self.progress.lock("payment"))
    }

    fn wait_for(&self, context: &mut Context, _input: Self::Input) -> HandlerResult<Wait> {
        STATUS.set(context, "processing".to_owned())?;
        self.progress
            .set(context, "payment", "authorized".to_owned())?;
        Ok(Wait::skip_immediately())
    }

    fn execute(&self, context: &mut Context, input: Self::Input) -> HandlerResult<StepDecision> {
        STATUS.set(context, "completed".to_owned())?;
        // The Flow no longer needs the short-lived key, so delete it permanently.
        MODEL_API_KEY.delete(context)?;
        Ok(StepDecision::graceful_complete(input))
    }
}
