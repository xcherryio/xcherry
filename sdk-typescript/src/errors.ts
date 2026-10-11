// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import type { status } from "@grpc/grpc-js";


/** Indicates that an API is intentionally unavailable in the current SDK phase. */
export class PhaseNotImplementedError extends Error {}

/** Provides stable Dex-specific classifications beyond gRPC status codes. */
export const ErrorSubStatus = Object.freeze({
  /** Dex returned no more specific classification. */
  UNCATEGORIZED: "uncategorized",
  /** A start request conflicted with an existing Flow ID. */
  FLOW_ALREADY_STARTED: "flowAlreadyStarted",
  /** No Flow exists for the requested ID. */
  FLOW_NOT_EXISTS: "flowNotExists",
  /** A nested application Worker invocation failed. */
  WORKER_API_ERROR: "workerApiError",
  /** A retryable long poll ended without observing its condition. */
  LONG_POLL_TIMEOUT: "longPollTimeout",
  /** A pending Channel message ID no longer exists. */
  CHANNEL_MESSAGE_NOT_FOUND: "channelMessageNotFound",
  /** A caller-defined request budget expired. */
  REQUEST_TIMEOUT: "requestTimeout",
} as const);

/** Represents a value from {@link ErrorSubStatus}. */
export type ErrorSubStatus = (typeof ErrorSubStatus)[keyof typeof ErrorSubStatus];

/** Provides terminal Flow failure categories returned by Dex. */
export const FlowErrorType = Object.freeze({
  /** A Step decision could not be applied. */
  STEP_DECISION_FAILED: "stepDecisionFailed",
  /** A Client-originated operation failed the Flow. */
  CLIENT_API_FAILED: "clientApiFailed",
  /** Worker dispatch or application handler execution failed. */
  WORKER_API_FAILED: "workerApiFailed",
  /** Application Flow code returned an invalid definition or result. */
  INVALID_USER_FLOW_CODE: "invalidUserFlowCode",
  /** Dex encountered an internal invariant or infrastructure failure. */
  INTERNAL: "internal",
  /** A Dex soft Flow timeout expired under the fail policy. */
  FLOW_TIMEOUT: "flowTimeout",
  /**
   * The server could not start a SubFlow within its configured retry budget.
   *
   * Only SubFlow results report it. The SubFlow never ran, so the result has no outputs and its
   * error message holds the last start error.
   */
  SUB_FLOW_START_FAILED: "subFlowStartFailed",
} as const);

/** Represents a value from {@link FlowErrorType}. */
export type FlowErrorType = (typeof FlowErrorType)[keyof typeof FlowErrorType];

/** Exposes structured metadata returned by a failed FlowService operation. */
export class DexServiceError extends Error {
  /**
   * Creates a structured service error.
   * @param code - Outer gRPC status code.
   * @param subStatus - Stable Dex-specific classification.
   * @param detail - Human-readable service detail.
   * @param operation - Client operation that failed.
   * @param flowId - Target Flow ID, or `undefined` for service-wide calls.
   * @param options - Standard Error construction options.
   */
  public constructor(
    public readonly code: status,
    public readonly subStatus: ErrorSubStatus,
    public readonly detail: string,
    public readonly operation: string,
    public readonly flowId: string | undefined,
    options?: ErrorOptions,
  ) {
    super(detail, options);
    this.name = new.target.name;
  }
}

/** Indicates that `startFlow` conflicts with an existing Flow ID. */
export class FlowAlreadyStartedError extends DexServiceError {}

/** Indicates that an operation targeted a Flow ID that does not exist. */
export class FlowNotFoundError extends DexServiceError {}

/**
 * Reports a missing Flow or an operation that cannot use a closed Flow.
 *
 * RPC calls use this error for missing targets, including query-only reads. A query-only RPC
 * without locks, transactionality, durable effects, or Server-forced Update routing can read a
 * retained terminal execution. At that confirmed read boundary, handle this error as not found
 * without a lifecycle probe. Preserve other service and Worker failures.
 *
 * Mutations and active-only RPCs also use this error for closed targets. It does not distinguish
 * missing from closed Flows or prove the requested action succeeded. Inherited service metadata
 * and the original transport cause remain available.
 *
 * @example
 * ```typescript
 * try {
 *   await client.invokeRPC(orders.updateOrder, flowId, update);
 * } catch (error) {
 *   if (error instanceof FlowNotActiveOrNotFoundError) {
 *     throw new OrderUnavailableError(flowId);
 *   }
 *   throw error;
 * }
 * ```
 */
export class FlowNotActiveOrNotFoundError extends DexServiceError {}

/** Exposes both outer FlowService and nested WorkerService failure details. */
export class WorkerInvocationError extends DexServiceError {
  /**
   * Creates an error from outer and nested Worker metadata.
   * @param code - Outer FlowService gRPC status.
   * @param subStatus - Dex-specific classification.
   * @param detail - Outer human-readable detail.
   * @param operation - Client operation that invoked the Worker.
   * @param flowId - Target Flow ID, when available.
   * @param workerCode - Nested Worker gRPC status, when available.
   * @param workerErrorType - Worker-reported application error type.
   * @param workerErrorDetail - Worker-reported human-readable detail.
   * @param options - Standard Error construction options.
   */
  public constructor(
    code: status,
    subStatus: ErrorSubStatus,
    detail: string,
    operation: string,
    flowId: string | undefined,
    public readonly workerCode: status | undefined,
    public readonly workerErrorType: string,
    public readonly workerErrorDetail: string,
    options?: ErrorOptions,
  ) {
    super(code, subStatus, detail, operation, flowId, options);
  }
}

/** Indicates that an RPC could not acquire all requested Attribute locks. */
export class RpcLockConflictError extends DexServiceError {}

/** Indicates that a retryable long poll ended before its condition was observed. */
export class LongPollTimeoutError extends DexServiceError {}

/** Indicates that a caller-defined request budget expired. */
export class RequestTimeoutError extends DexServiceError {}

/** Indicates that a requested Channel message is no longer pending. */
export class ChannelMessageNotFoundError extends DexServiceError {}

/** Indicates that an RPC did not load the requested AttributeMap entries. */
export class AttributeMapNotLoadedError extends Error {}

/** Indicates that an RPC did not load the requested pending Channel messages. */
export class ChannelMessagesNotLoadedError extends Error {}

/** Indicates that Registry construction found an invalid Flow definition. */
export class FlowDefinitionError extends Error {
  /**
   * Creates a Flow definition error.
   * @param message - Precise validation failure detail.
   * @param options - Standard Error construction options.
   */
  public constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = new.target.name;
  }
}

/** Identifies a Step or RPC result that violates the SDK contract. */
export class InvalidStepResultError extends FlowDefinitionError {
  /**
   * Creates an invalid-handler-result error.
   * @param flowType - Containing Flow type.
   * @param stepType - Step type, or `undefined` for an RPC.
   * @param method - Handler method that returned the invalid value.
   * @param detail - Precise contract violation.
   * @param options - Standard Error construction options.
   */
  public constructor(
    public readonly flowType: string,
    public readonly stepType: string | undefined,
    public readonly method: "waitFor" | "execute" | "rpc",
    detail: string,
    options?: ErrorOptions,
  ) {
    const target = stepType === undefined ? `RPC in Flow ${flowType}` : `Flow ${flowType} Step ${stepType}`;
    super(`${target} ${method} returned an invalid result: ${detail}`, options);
  }
}

/** Reports an application value encoding, decoding, or hydration failure. */
export class ValueMappingError extends Error {
  /**
   * Creates a value-mapping error.
   * @param operation - Mapping phase that failed.
   * @param detail - Incompatible type, wire kind, or malformed payload detail.
   * @param options - Standard Error construction options.
   */
  public constructor(
    public readonly operation: "encode" | "decode" | "hydrate",
    detail: string,
    options?: ErrorOptions,
  ) {
    super(`Cannot ${operation} Dex Value: ${detail}`, options);
    this.name = new.target.name;
  }
}

/**
 * Creates a consistent error for an API planned in a later implementation phase.
 * @param component - Human-readable unavailable component name.
 * @returns A new PhaseNotImplementedError.
 */
export function laterPhase(component: string): PhaseNotImplementedError {
  return new PhaseNotImplementedError(`${component} belongs to a later phase`);
}
