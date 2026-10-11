// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import type { Context } from "./context.js";
import type { Flow } from "./flow.js";
import type { FlowResult } from "./flow-result.js";
import { InvocationContext } from "./invocation-context.js";
import type {
  FlowConfig,
  FlowTimeoutHandlerOptions,
  FlowTimeoutPolicy,
  InitialAttribute,
} from "./options.js";
import type { RetryPolicy } from "./step.js";
import type { Condition } from "./wait.js";

/** Controls how a generated SubFlow Flow ID resolves an existing execution. */
export const SubFlowReusePolicy = Object.freeze({
  /** Attaches to a running execution or returns its existing terminal result. */
  ATTACH: "attach",
  /** Restarts abnormal executions, attaches while running, and returns completed results. */
  RESTART_IF_PREVIOUS_EXITS_ABNORMALLY: "restartIfPreviousExitsAbnormally",
  /** Replaces any different existing execution, including a running one. */
  ALWAYS_RESTART: "alwaysRestart",
} as const);

/** Represents a value from {@link SubFlowReusePolicy}. */
export type SubFlowReusePolicy =
  (typeof SubFlowReusePolicy)[keyof typeof SubFlowReusePolicy];

/** Configures one durable SubFlow condition. All durations use milliseconds. */
export interface SubFlowOptions {
  /** Maximum SubFlow lifetime; normal Flow defaults apply when omitted. */
  readonly timeoutMs?: number;
  /** Action taken when a positive timeout expires; defaults from the target Flow's hook. */
  readonly timeoutPolicy?: FlowTimeoutPolicy;
  /** Execution settings for the target Flow's timeout handler. */
  readonly timeoutHandlerOptions?: FlowTimeoutHandlerOptions;
  /** Delay before the SubFlow starting Step, in milliseconds. */
  readonly startDelayMs?: number;
  /** Whole-Flow retry behavior after abnormal completion. */
  readonly retryPolicy?: RetryPolicy;
  /** Initial Attributes owned by the target SubFlow. */
  readonly attributes?: readonly InitialAttribute<any>[];
  /** Fields applied over the inherited parent Flow configuration. */
  readonly configOverride?: FlowConfig;
  /** Existing-execution policy; defaults to abnormal restart. */
  readonly reusePolicy?: SubFlowReusePolicy;
  /** Stable condition ID required by `Wait.anyCombinationOf`. */
  readonly conditionId?: string;
}

/** Creates durable SubFlow conditions and reads their Execute results. */
export const SubFlow = Object.freeze({
  /**
   * Creates a condition that starts or reuses a registered Flow and awaits completion.
   *
   * The server starts the SubFlow after `waitFor` returns and retries the start within its
   * configured retry budget. When the start still fails, the waiting Step's `waitForFailure`
   * option decides the outcome. The default `"failFlow"` fails the parent Flow. `"proceed"` runs
   * `execute`, and the SubFlow result reports `FlowErrorType.SUB_FLOW_START_FAILED`.
   * @typeParam Input - Target starting Step input type.
   * @param flow - Exact target Flow instance registered by the Worker.
   * @param input - Input accepted by its starting Step.
   * @param options - Optional timing, retry, Attribute, configuration, reuse, and ID settings.
   * @returns A durable SubFlow condition accepted by Wait factories.
   */
  run<Input>(flow: Flow<Input>, input: Input, options: SubFlowOptions = {}): Condition {
    return Object.freeze({
      kind: "subFlow" as const,
      subFlow: flow,
      subFlowInput: input,
      subFlowOptions: options,
      ...(options.conditionId === undefined ? {} : { conditionId: options.conditionId }),
    });
  },
  /**
   * Returns one stable-indexed SubFlow result during Step execute.
   *
   * A SubFlow that the server could not start returns a failed result with
   * `FlowErrorType.SUB_FLOW_START_FAILED` and the start error as its error message. `execute` sees
   * this result only when the Step uses `waitForFailure: "proceed"`. Check the error type to tell a
   * start failure apart from a SubFlow that started and then failed.
   *
   * ```ts
   * const charge = SubFlow.getConditionResults(context);
   * if (charge.errorType === FlowErrorType.SUB_FLOW_START_FAILED) {
   *   return forceFail(`SubFlow did not start: ${charge.errorMessage}`);
   * }
   * ```
   * @param context - Current Dex Execute context.
   * @param index - Zero-based SubFlow order; defaults to zero.
   * @returns A terminal result or an AnyOf loser's running snapshot.
   */
  getConditionResults(context: Context, index = 0): FlowResult {
    return invocation(context).subFlowResult(index);
  },
  /**
   * Returns one generated SubFlow Flow ID during Step execute.
   * @param context - Current Dex Execute context.
   * @param index - Zero-based SubFlow order; defaults to zero.
   * @returns An addressable Flow ID suitable for `Client.stopFlow`.
   */
  getFlowId(context: Context, index = 0): string {
    return invocation(context).subFlowId(index);
  },
});

function invocation(context: Context): InvocationContext {
  if (!(context instanceof InvocationContext)) {
    throw new TypeError("SubFlow access requires a Dex invocation Context");
  }
  return context;
}
