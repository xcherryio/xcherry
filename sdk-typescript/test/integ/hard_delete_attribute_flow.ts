// Portions of this file are derived from indeedeng/iwf-java-sdk.
// Those portions are licensed under the Apache License, Version 2.0.
// See LICENSES/Apache-2.0.txt and LEGACY_NOTICES.md.
//
// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications are licensed under the Sustainable Use License 1.0.
// Third-Party Materials remain under the Apache License, Version 2.0.
// See LICENSE and LEGACY_NOTICES.md.

import {
  Attribute,
  AttributeMap,
  Channel,
  StepList,
  Wait,
  goTo,
  gracefulComplete,
  jsonCodec,
  rpc,
  stringCodec,
  voidCodec,
  type Context,
  type Flow,
  type PersistenceSchema,
  type Step,
  type StepDecision,
  type StepOptions,
} from "../../src/index.js";

export type VendorToken = { readonly token: string };

export interface HardDeleteSecrets {
  readonly apiKey: string | undefined;
  readonly vendorToken: string | undefined;
  readonly vendorTokenInstances: readonly string[];
  readonly phase: string | undefined;
}

export interface HardDeleteSecretsInput {
  readonly apiKey: string;
  readonly vendorToken: string;
}

export const vendorInstance = "github";
export const initialApiKey = "initial-api-key-5c21";
export const initialVendorToken = "initial-vendor-token-7d43";
export const rotatedApiKey = "rotated-api-key-9e18";
export const rotatedVendorToken = "rotated-vendor-token-2b64";

const apiKey = new Attribute("api-key", stringCodec).hardDeleteOnNull();
const vendorTokens = new AttributeMap("vendor-tokens", jsonCodec<VendorToken>()).hardDeleteOnNull();
const phase = new Attribute("phase", stringCodec);
const deletionRequests = new Channel("deletion-requests", voidCodec);
const completionRequests = new Channel("completion-requests", voidCodec);

class RotateSecretsStep implements Step<void> {
  public getStepType(): string {
    return "RotateSecretsStep";
  }

  public getStepOptions(): StepOptions {
    return { executeLoadAttributeMapInstances: [vendorTokens.load(vendorInstance)] };
  }

  public execute(context: Context): StepDecision {
    const isInitialApiKey = apiKey.get(context) === initialApiKey;
    const isInitialVendorToken = vendorTokens.get(context, vendorInstance)?.token === initialVendorToken;
    if (!isInitialApiKey || !isInitialVendorToken) {
      phase.set(context, "initial-values-mismatch");
      return gracefulComplete("initial-values-mismatch");
    }
    apiKey.set(context, rotatedApiKey);
    vendorTokens.set(context, vendorInstance, { token: rotatedVendorToken });
    phase.set(context, "rotated");
    return goTo(DeleteSecretsStep, undefined);
  }
}

class DeleteSecretsStep implements Step<void> {
  public getStepType(): string {
    return "DeleteSecretsStep";
  }

  public getStepOptions(): StepOptions {
    return { executeLoadAttributeMapInstances: [vendorTokens.load(vendorInstance)] };
  }

  public waitFor(_context: Context): Wait {
    return Wait.until(deletionRequests.forOne());
  }

  public execute(context: Context): StepDecision {
    const hasValues = apiKey.get(context) !== undefined
      && vendorTokens.get(context, vendorInstance) !== undefined;
    apiKey.delete(context);
    vendorTokens.delete(context, vendorInstance);
    phase.set(context, hasValues ? "deleted" : "values-missing-before-delete");
    return goTo(AwaitCompletionStep, undefined);
  }
}

class AwaitCompletionStep implements Step<void> {
  public getStepType(): string {
    return "AwaitCompletionStep";
  }

  public waitFor(_context: Context): Wait {
    return Wait.until(completionRequests.forOne());
  }

  public execute(context: Context): StepDecision {
    return gracefulComplete(phase.get(context) ?? "");
  }
}

export class HardDeleteAttributeFlow implements Flow<void> {
  public readonly apiKey = apiKey;
  public readonly vendorTokens = vendorTokens;
  public readonly phase = phase;
  private readonly rotate = new RotateSecretsStep();
  private readonly delete = new DeleteSecretsStep();
  private readonly awaitCompletion = new AwaitCompletionStep();

  public getFlowType(): string {
    return "HardDeleteAttributeFlow";
  }

  public getSteps(): StepList<void> {
    return StepList.startStep<void>(this.rotate).otherSteps(this.delete, this.awaitCompletion);
  }

  public getPersistenceSchema(): PersistenceSchema {
    return {
      attributes: [apiKey, vendorTokens, phase],
      channels: [deletionRequests, completionRequests],
    };
  }

  @rpc({ outputCodec: jsonCodec<HardDeleteSecrets>(), loadAttributeMaps: [vendorTokens] })
  public getSecrets(context: Context) {
    return {
      output: {
        apiKey: apiKey.get(context),
        vendorToken: vendorTokens.getAllInstanceKeys(context).includes(vendorInstance)
          ? vendorTokens.get(context, vendorInstance).token
          : undefined,
        vendorTokenInstances: vendorTokens.getAllInstanceKeys(context),
        phase: phase.get(context),
      },
    };
  }

  @rpc({ inputCodec: jsonCodec<HardDeleteSecretsInput>() })
  public setSecrets(context: Context, input: HardDeleteSecretsInput): void {
    apiKey.set(context, input.apiKey);
    vendorTokens.set(context, vendorInstance, { token: input.vendorToken });
  }

  @rpc()
  public deleteSecrets(context: Context): void {
    apiKey.delete(context);
    vendorTokens.delete(context, vendorInstance);
  }

  @rpc()
  public requestDeletion(context: Context): void {
    deletionRequests.publish(context, undefined);
  }

  @rpc()
  public requestCompletion(context: Context): void {
    completionRequests.publish(context, undefined);
  }
}
