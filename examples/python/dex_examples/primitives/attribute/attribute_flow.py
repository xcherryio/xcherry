# Copyright (c) 2022-2026 Super Durable, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Minimal Attribute Flow: declare, write, and read a persisted Attribute."""

from __future__ import annotations

from dex import (
    Attribute,
    AttributeIndex,
    AttributeMap,
    Context,
    Flow,
    FlowConfig,
    IndexType,
    PersistenceSchema,
    RPCResult,
    Step,
    StepDecision,
    StepList,
    StepOptions,
    Wait,
    graceful_complete,
    rpc,
)


class AttributeStep(Step[str]):
    def __init__(self, flow: "AttributeFlow") -> None:
        self.flow = flow

    def wait_for(self, context: Context, input: str) -> Wait:
        self.flow.status.set(context, "processing")
        self.flow.progress.set(context, "payment", "authorized")
        return Wait.skip_immediately()

    def get_step_options(self) -> StepOptions:
        return StepOptions(
            wait_for_lock_attributes=(
                self.flow.status.lock(),
                self.flow.progress.lock("payment"),
            ),
            execute_lock_attributes=(
                self.flow.status.lock(),
                self.flow.progress.lock("payment"),
            ),
        )

    def execute(self, context: Context, input: str) -> StepDecision:
        self.flow.status.set(context, "completed")
        # The Flow no longer needs the short-lived key, so delete it permanently.
        self.flow.model_api_key.delete(context)
        return graceful_complete(input)


class AttributeFlow(Flow[str]):
    status = Attribute(
        "primitive-attribute-status",
        str,
        index=AttributeIndex(IndexType.KEYWORD, "keyword1"),
    )
    email = Attribute("primitive-attribute-email", str, sync_to_attribute_store=True)
    model_api_key = Attribute(
        "primitive-attribute-model-api-key",
        str,
        hard_delete_on_null=True,
    )
    progress = AttributeMap(
        "primitive-attribute-progress",
        str,
        index=AttributeIndex(IndexType.KEYWORD, "keyword2"),
    )
    attribute_store_config = FlowConfig(attribute_store_names=["profiles"])

    def __init__(self) -> None:
        self.start = AttributeStep(self)

    def get_steps(self) -> StepList[str]:
        return StepList.start_step(self.start)

    def get_persistence_schema(self) -> PersistenceSchema:
        return PersistenceSchema.of(
            self.status, self.progress, self.email, self.model_api_key
        )

    @rpc(lock_attributes=(status.lock(), progress.lock("payment")))
    def update_status(self, context: Context, input: str) -> RPCResult[str]:
        self.status.set(context, input)
        self.progress.set(context, "payment", input)
        return RPCResult(input)
