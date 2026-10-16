/*
 * Copyright (c) 2022-2026 Super Durable, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {
  Attribute,
  AttributeMap,
  IndexType,
  StepList,
  Wait,
  goTo,
  gracefulComplete,
  rpc,
  stringCodec,
  type Context,
  type Flow,
  type FlowConfig,
  type PersistenceSchema,
  type RPCResult,
  type Step,
  type StepDecision,
} from "@superdurable/dex";

const status = new Attribute("primitive-attribute-status", stringCodec, {
  type: IndexType.KEYWORD,
  indexKey: "keyword1",
});
const email = new Attribute("primitive-attribute-email", stringCodec).syncToAttributeStore();
const modelApiKey = new Attribute(
  "primitive-attribute-model-api-key",
  stringCodec,
).hardDeleteOnNull();
const progress = new AttributeMap("primitive-attribute-progress", stringCodec, {
  type: IndexType.KEYWORD,
  indexKey: "keyword2",
});

class AttributeStep implements Step<string> {
  public readonly inputCodec = stringCodec;

  public constructor(private readonly flow: AttributeFlow) {}

  public getStepType(): string {
    return "AttributeStep";
  }

  public getStepOptions() {
    return {
      waitForLockAttributes: [this.flow.status.lock(), this.flow.progress.lock("payment")],
      executeLockAttributes: [this.flow.status.lock(), this.flow.progress.lock("payment")],
    };
  }

  public waitFor(context: Context, input: string): Wait {
    this.flow.status.set(context, "processing");
    this.flow.progress.set(context, "payment", "authorized");
    return Wait.skipImmediately();
  }

  public execute(context: Context, input: string): StepDecision {
    this.flow.status.set(context, "completed");
    // The Flow no longer needs the short-lived key, so delete it permanently.
    this.flow.modelApiKey.delete(context);
    return gracefulComplete(input);
  }
}

export class AttributeFlow implements Flow<string> {
  public readonly status = status;
  public readonly email = email;
  public readonly modelApiKey = modelApiKey;
  public readonly progress = progress;
  public readonly attributeStoreConfig: FlowConfig = { attributeStoreNames: ["profiles"] };
  private readonly start = new AttributeStep(this);

  public getFlowType(): string {
    return "AttributeFlow";
  }

  public getSteps() {
    return StepList.startStep(this.start);
  }

  public getPersistenceSchema(): PersistenceSchema {
    return { attributes: [this.status, this.progress, this.email, this.modelApiKey] };
  }

  @rpc({
    inputCodec: stringCodec,
    outputCodec: stringCodec,
    lockAttributes: [status.lock(), progress.lock("payment")],
  })
  public updateStatus(context: Context, input: string): RPCResult<string> {
    this.status.set(context, input);
    this.progress.set(context, "payment", input);
    return { output: input };
  }
}

export const attributeFlow = new AttributeFlow();
