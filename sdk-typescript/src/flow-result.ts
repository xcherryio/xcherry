// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import type { Codec } from "./codec.js";
import {
  FlowErrorType as ProtoFlowErrorType,
  FlowStatus as ProtoFlowStatus,
  type FlowResult as ProtoFlowResult,
  type StepCompletionOutput,
  type Value,
} from "./gen/dex.js";
import { FlowErrorType, type FlowErrorType as FlowErrorTypeValue } from "./errors.js";
import type { FlowStatus } from "./options.js";
import { codecOrJson, decodeValue } from "./value-mapper.js";

/** Contains one output-bearing Step completion returned by {@link Client.waitForFlow}. */
export interface StepCompletion {
  /** Registered Step type that produced this output. */
  readonly stepType: string;
  /** Exact server Step execution identity that produced this output. */
  readonly stepExecutionId: string;
  /**
   * Decodes this already hydrated Step output.
   * @typeParam Output - Expected application output type.
   * @param codec - Codec for the expected output type. Omit this for JSON objects.
   *   Scalar wire kinds still need an explicit codec.
   * @returns The decoded Step output.
   */
  decode<Output>(codec?: Codec<Output>): Output;
}

/** Describes an observed Flow status and its output-bearing completions. */
export interface FlowResult {
  /** Observed lifecycle state. A running SubFlow value is a durable snapshot. */
  readonly status: FlowStatus;
  /** Dex failure category when available. */
  readonly errorType: FlowErrorTypeValue | undefined;
  /** Server failure detail when available. */
  readonly errorMessage: string | undefined;
  /** Whether the observed run is terminal. */
  readonly isTerminal: boolean;
  /**
   * Immutable completions in server collection order. Parallel Step order is not deterministic.
   */
  readonly completions: readonly StepCompletion[];
  /**
   * Decodes the output when exactly one completion exists.
   * @typeParam Output - Expected application output type.
   * @param codec - Codec for the expected output type. Omit this for JSON objects.
   *   Scalar wire kinds still need an explicit codec.
   * @returns The only decoded Step output.
   * @throws {@link TypeError} when nonterminal or when zero or multiple outputs exist.
   */
  singleOutput<Output>(codec?: Codec<Output>): Output;
}

/** @internal */
export function createStepCompletions(
  records: readonly StepCompletionOutput[],
  values: readonly Value[],
): readonly StepCompletion[] {
  return Object.freeze(records.map((record, index) => {
    const value = values[index];
    if (value === undefined) {
      throw new TypeError(`Step completion ${index} has no output`);
    }
    return Object.freeze({
      stepType: record.completedStepType,
      stepExecutionId: record.completedStepExecutionId,
      decode<Output>(codec?: Codec<Output>): Output {
        return decodeValue(codecOrJson(codec), value);
      },
    });
  }));
}

/** @internal */
export function createFlowResult(
  status: FlowStatus,
  completions: readonly StepCompletion[],
  errorType?: FlowErrorTypeValue,
  errorMessage?: string,
): FlowResult {
  const isTerminal = status !== "running" && status !== "continuedAsNew";
  return Object.freeze({
    status,
    errorType,
    errorMessage,
    isTerminal,
    completions,
    singleOutput<Output>(codec?: Codec<Output>): Output {
      if (!isTerminal) {
        throw new TypeError("Flow result is not terminal");
      }
      if (completions.length !== 1) {
        throw new TypeError(`Expected exactly one Step output, found ${completions.length}`);
      }
      return completions[0]!.decode(codec);
    },
  });
}

/** @internal */
export function createFlowResultFromProto(
  result: ProtoFlowResult,
  values: readonly Value[],
): FlowResult {
  return createFlowResult(
    mapStatus(result.flowStatus),
    createStepCompletions(result.results, values),
    mapErrorType(result.errorType),
    result.errorMessage || undefined,
  );
}

function mapStatus(status: ProtoFlowStatus): FlowStatus {
  const values: Partial<Record<ProtoFlowStatus, FlowStatus>> = {
    [ProtoFlowStatus.FLOW_STATUS_RUNNING]: "running",
    [ProtoFlowStatus.FLOW_STATUS_COMPLETED]: "completed",
    [ProtoFlowStatus.FLOW_STATUS_FAILED]: "failed",
    [ProtoFlowStatus.FLOW_STATUS_SERVER_SIDE_TIMEOUT_INTERNAL_ONLY]:
      "serverSideTimeoutInternalOnly",
    [ProtoFlowStatus.FLOW_STATUS_TERMINATED]: "terminated",
    [ProtoFlowStatus.FLOW_STATUS_CANCELED]: "cancelled",
    [ProtoFlowStatus.FLOW_STATUS_CONTINUED_AS_NEW]: "continuedAsNew",
  };
  const mapped = values[status];
  if (mapped === undefined) throw new TypeError(`unsupported Flow status ${status}`);
  return mapped;
}

function mapErrorType(type: ProtoFlowErrorType): FlowErrorTypeValue | undefined {
  switch (type) {
    case ProtoFlowErrorType.FLOW_ERROR_TYPE_UNSPECIFIED: return undefined;
    case ProtoFlowErrorType.FLOW_ERROR_TYPE_STEP_DECISION_FAILING_FLOW:
      return FlowErrorType.STEP_DECISION_FAILED;
    case ProtoFlowErrorType.FLOW_ERROR_TYPE_CLIENT_API_FAILING_FLOW:
      return FlowErrorType.CLIENT_API_FAILED;
    case ProtoFlowErrorType.FLOW_ERROR_TYPE_WORKER_API_FAIL:
      return FlowErrorType.WORKER_API_FAILED;
    case ProtoFlowErrorType.FLOW_ERROR_TYPE_INVALID_USER_FLOW_CODE:
      return FlowErrorType.INVALID_USER_FLOW_CODE;
    case ProtoFlowErrorType.FLOW_ERROR_TYPE_INTERNAL: return FlowErrorType.INTERNAL;
    case ProtoFlowErrorType.FLOW_ERROR_TYPE_FLOW_TIMEOUT: return FlowErrorType.FLOW_TIMEOUT;
    case ProtoFlowErrorType.FLOW_ERROR_TYPE_SUB_FLOW_START_FAILED:
      return FlowErrorType.SUB_FLOW_START_FAILED;
    default: throw new TypeError(`unsupported Flow error type ${type}`);
  }
}
