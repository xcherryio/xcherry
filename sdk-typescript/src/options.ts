// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import type { Credential } from "./credential.js";
import type { Attribute, AttributeLock, AttributeMap, AttributeMapLoad } from "./persistence.js";
import type { RetryPolicy, StepClass, StepDurability, StepOptions } from "./step.js";
import type { Channel, ChannelMap, ChannelMapLoad } from "./wait.js";
import { requireMapInstance, requireName } from "./validation.js";

/** Configures FlowService connectivity and default Flow routing. */
export interface ClientOptions {
  /** Plaintext Dex gRPC target; defaults to `localhost:8801`. */
  readonly serverAddress?: string;
  /** Bearer token provider for every FlowService call; omitted sends no authorization metadata. */
  readonly credential?: Credential;
  /** Worker endpoint advertised by `startFlow` unless a Flow overrides it; use `{ pull: true }` for pull Workers. */
  readonly workerTarget?: WorkerTarget | PullWorkerTarget;
}

/**
 * Configures one durable Step completion wait.
 * The server derives a stable Request ID from the Step execution when none is supplied.
 * `requestTimeoutMs` bounds the entire SDK call across transparent transport reattachments.
 * Temporal permits 10 in-flight Updates per Workflow Execution. `internalHandlerTimeoutMs`
 * reclaims accepted waits that outlive callers and could consume those slots. Active callers
 * transparently start another generation, which adds another Update to history.
 */
export interface WaitForStepCompletionOptions {
  /** Overrides the stable Request ID that the server derives from the Step execution. */
  readonly requestId?: string;
  /** Total SDK call budget. Omit or use zero to wait indefinitely. */
  readonly requestTimeoutMs?: number;
  /**
   * Sets the Temporal Update handler generation lifetime.
   * Use it only when abandoned waits can approach Temporal's in-flight Update limit. Omit or use
   * zero to disable rollover. Prefer a value longer than normal request timeouts and reconnect
   * gaps; each generation counts toward Temporal's 2,000-Update history limit.
   */
  readonly internalHandlerTimeoutMs?: number;
}

/**
 * Configures one durable Attribute match wait.
 * The server derives a stable Request ID from the Attribute condition when none is supplied.
 * `requestTimeoutMs` bounds the entire SDK call across transparent transport reattachments.
 * Temporal permits 10 in-flight Updates per Workflow Execution. `internalHandlerTimeoutMs`
 * reclaims accepted waits that outlive callers and could consume those slots. Active callers
 * transparently start another generation, which adds another Update to history.
 */
export interface WaitForAttributeOptions {
  /** Overrides the stable Request ID that the server derives from the Attribute condition. */
  readonly requestId?: string;
  /** Total SDK call budget. Omit or use zero to wait indefinitely. */
  readonly requestTimeoutMs?: number;
  /**
   * Sets the Temporal Update handler generation lifetime.
   * Use it only when abandoned waits can approach Temporal's in-flight Update limit. Omit or use
   * zero to disable rollover. Prefer a value longer than normal request timeouts and reconnect
   * gaps; each generation counts toward Temporal's 2,000-Update history limit.
   */
  readonly internalHandlerTimeoutMs?: number;
}

/** Controls which active Steps are included in Flow search indexing. */
export const ActiveStepSearchMode = Object.freeze({
  /** Uses the Dex server's current default policy. */
  DEFAULT: "default",
  /** Indexes every active Step. */
  ALL: "all",
} as const);

/** Represents a value from {@link ActiveStepSearchMode}. */
export type ActiveStepSearchMode =
  (typeof ActiveStepSearchMode)[keyof typeof ActiveStepSearchMode];

/** Controls whether `startFlow` may reuse a previously used Flow ID. */
export const IdReusePolicy = Object.freeze({
  /** Uses the Dex server default, which rejects every previously used Flow ID. */
  DEFAULT: "default",
  /** Reuses an ID only after an unsuccessful closed run. */
  ALLOW_IF_PREVIOUS_FAILED: "allowIfPreviousFailed",
  /** Reuses an ID after any closed run. */
  ALLOW_IF_NOT_RUNNING: "allowIfNotRunning",
  /** Terminates an active run before starting the replacement. */
  ALLOW_TERMINATE_IF_RUNNING: "allowTerminateIfRunning",
  /** Rejects every previously used Flow ID. */
  DISALLOW: "disallow",
} as const);

/** Represents a value from {@link IdReusePolicy}. */
export type IdReusePolicy = (typeof IdReusePolicy)[keyof typeof IdReusePolicy];

/** Controls how Dex responds when a positive soft Flow timeout expires. */
export const FlowTimeoutPolicy = Object.freeze({
  /** Invokes `Flow.handleTimeout` when present, and fails otherwise. */
  DEFAULT: "default",
  /** Fails with `FlowErrorType.FLOW_TIMEOUT` and permits Flow retries. */
  FAIL: "fail",
  /** Cancels without retrying the Flow. */
  CANCEL: "cancel",
  /** Starts one logical `Flow.handleTimeout` execution after the durable timer fires or is skipped. */
  HANDLER: "handler",
} as const);

/** Represents a value from {@link FlowTimeoutPolicy}. */
export type FlowTimeoutPolicy =
  (typeof FlowTimeoutPolicy)[keyof typeof FlowTimeoutPolicy];

/** Routes exhausted Flow timeout-handler retries to an input-free Step. */
export interface FlowTimeoutHandlerFailure {
  /** Registered fallback Step whose input codec is {@link voidCodec}. */
  readonly step: StepClass<void>;
  /** Options applied when the fallback Step is scheduled. */
  readonly options?: StepOptions;
}

/** Creates Flow timeout-handler failure routing settings. */
export const FlowTimeoutHandlerFailure = Object.freeze({
  /**
   * Routes exhausted timeout-handler retries to an input-free Step.
   * The Step receives `undefined` and reads the terminal handler failure from `Context.recoveryError`.
   * @param step - Registered Step class with {@link voidCodec} as its input codec.
   * @param options - Optional options for the fallback Step movement.
   * @returns The timeout-handler failure-routing settings.
   */
  proceedTo(step: StepClass<void>, options?: StepOptions): FlowTimeoutHandlerFailure {
    return { step, ...(options === undefined ? {} : { options }) };
  },
});

/** Configures execution and selective state loading for `Flow.handleTimeout`. */
export interface FlowTimeoutHandlerOptions {
  /** Per-attempt handler limit in milliseconds; uses the server default when omitted. */
  readonly methodTimeoutMs?: number;
  /** Heartbeat timeout in milliseconds; requires whole seconds and uses the server default when omitted. */
  readonly heartbeatTimeoutMs?: number;
  /** Retry policy for the timeout handler's logical execution. */
  readonly retry?: RetryPolicy;
  /** Routing after all timeout-handler attempts fail. */
  readonly failure?: FlowTimeoutHandlerFailure;
  /** Durability override for the handler response. */
  readonly durability?: StepDurability;
  /** Attribute locks held during each timeout-handler attempt. */
  readonly lockAttributes?: readonly AttributeLock[];
  /** AttributeMap definitions whose complete contents are loaded for the handler. */
  readonly loadAttributeMaps?: readonly AttributeMap<unknown>[];
  /** Exact AttributeMap instances loaded for the handler. */
  readonly loadAttributeMapInstances?: readonly AttributeMapLoad[];
  /** Channels whose pending messages are loaded for the handler. */
  readonly loadChannels?: readonly Channel<unknown>[];
  /** ChannelMap definitions whose complete pending contents are loaded for the handler. */
  readonly loadChannelMaps?: readonly ChannelMap<unknown>[];
  /** Exact ChannelMap instances whose pending messages are loaded for the handler. */
  readonly loadChannelMapInstances?: readonly ChannelMapLoad[];
}

/** Overrides mutable server behavior for one Flow execution. */
export interface FlowConfig {
  /** Optional active-Step visibility indexing policy. */
  readonly activeStepSearchMode?: ActiveStepSearchMode;
  /**
   * Selects server-configured Attribute Stores receiving opted-in Attribute writes.
   *
   * Omit this property to preserve the existing Flow setting. An explicit empty array is sent with
   * presence and disables later synchronization. Each enabled Attribute write is projected to every
   * selected store; projection failures never roll back Flow Attribute writes.
   */
  readonly attributeStoreNames?: readonly string[];
  /** Positive history-event threshold requesting continue-as-new. */
  readonly continueAsNewThreshold?: number;
  /** Positive history page-size budget in bytes for continue-as-new. */
  readonly continueAsNewPageSizeBytes?: number;
  /** Default durability for later Step handler results. */
  readonly stepDurability?: StepDurability;
  /** Worker endpoint used for later handler calls; use `{ pull: true }` for pull routing. */
  readonly workerTarget?: WorkerTarget | PullWorkerTarget;
}

/**
 * Describes one Attribute value written atomically when a Flow starts.
 * @typeParam T - Attribute value type.
 */
export interface InitialAttribute<T> {
  /** Registered singleton Attribute or AttributeMap definition. */
  readonly attribute: Attribute<T> | AttributeMap<T>;
  /** The map instance. Omit it for a singleton Attribute. Slash is prohibited because it is a reserved character. */
  readonly instance?: string;
  /** Typed initial value. */
  readonly value: T;
}

/** Creates type-safe initial Attribute writes for {@link StartFlowOptions}. */
export const InitialAttribute = Object.freeze({
  /**
   * Creates a singleton Attribute initialization.
   * @typeParam T - Attribute value type.
   * @param attribute - Registered singleton Attribute.
   * @param value - Typed initial value.
   * @returns The Attribute initialization value.
   */
  of<T>(attribute: Attribute<T>, value: T): InitialAttribute<T> {
    return { attribute, value };
  },
  /**
   * Creates an AttributeMap instance initialization.
   * @typeParam T - Attribute value type.
   * @param attribute - Registered AttributeMap.
   * @param instance - The map instance. Slash is prohibited because it is a reserved character.
   * @param value - Typed initial value.
   * @returns The AttributeMap initialization value.
   */
  mapValue<T>(
    attribute: AttributeMap<T>,
    instance: string,
    value: T,
  ): InitialAttribute<T> {
    requireMapInstance(instance);
    return { attribute, instance, value };
  },
});

/**
 * Configures creation of a new Flow execution. All durations use milliseconds.
 *
 * @example
 * ```ts
 * const status = new Attribute("status", stringCodec);
 * const options: StartFlowOptions = {
 *   timeoutMs: 30 * 60_000,
 *   idReusePolicy: IdReusePolicy.ALLOW_IF_NOT_RUNNING,
 *   attributes: [InitialAttribute.of(status, "queued")],
 *   configOverride: { activeStepSearchMode: ActiveStepSearchMode.ALL },
 * };
 * ```
 */
export interface StartFlowOptions {
  /** Dex durable soft timeout in milliseconds; omitted or zero disables it. */
  readonly timeoutMs?: number;
  /** Action taken when a positive timeout expires; defaults from the Flow's hook. */
  readonly timeoutPolicy?: FlowTimeoutPolicy;
  /** Execution settings for `Flow.handleTimeout`; valid only with a positive handler-policy timeout. */
  readonly timeoutHandlerOptions?: FlowTimeoutHandlerOptions;
  /** Delay before the starting Step becomes eligible, in milliseconds. */
  readonly startDelayMs?: number;
  /** Flow ID reuse policy; uses `DEFAULT` when omitted. */
  readonly idReusePolicy?: IdReusePolicy;
  /** Optional Flow-level retry policy. */
  readonly retryPolicy?: RetryPolicy;
  /** Initial Attribute writes applied atomically with Flow creation. */
  readonly attributes?: readonly InitialAttribute<any>[];
  /** Flow configuration applied over registered defaults. */
  readonly configOverride?: FlowConfig;
  /** Returns the existing run instead of raising an already-started error. */
  readonly ignoreAlreadyStarted?: boolean;
  /** Non-empty idempotency key; generates a UUID when omitted. */
  readonly requestId?: string;
}

/**
 * Describes the lifecycle state of one Flow run.
 * `serverSideTimeoutInternalOnly` is reserved for backend hard-timeout reporting.
 */
export type FlowStatus =
  | "running"
  | "completed"
  | "failed"
  | "cancelled"
  | "terminated"
  | "serverSideTimeoutInternalOnly"
  | "continuedAsNew";

/** Summarizes the current or latest run for one Flow ID. */
export interface FlowInfo {
  /** Stable application-assigned Flow ID. */
  readonly flowId: string;
  /** Server-assigned run ID. */
  readonly runId: string;
  /** Registered Flow type. */
  readonly flowType: string;
  /** Current lifecycle state. */
  readonly status: FlowStatus;
  /** UTC run start timestamp. */
  readonly startedAt: Date;
}

/** Represents one indexed Flow run returned by `searchFlows`. */
export interface SearchFlowEntry {
  /** Stable application-assigned Flow ID. */
  readonly flowId: string;
  /** Server-assigned run ID for this entry. */
  readonly runId: string;
  /** Registered Flow type. */
  readonly flowType: string;
  /** Indexed lifecycle state. */
  readonly status: FlowStatus;
  /** UTC start timestamp. */
  readonly startedAt: Date;
  /** UTC close timestamp, or `undefined` for open or unknown runs. */
  readonly closedAt: Date | undefined;
  /** Hydrated values keyed by physical search-index name. */
  readonly indexedAttributes: ReadonlyMap<string, unknown>;
}

/** Contains one server-ordered page of Flow search results. */
export interface SearchFlowsPage {
  /** Result entries in server-defined query order. */
  readonly flows: readonly SearchFlowEntry[];
  /** Opaque next-page token, or an empty string on the final page. */
  readonly nextPageToken: string;
}

/** Identifies one numbered execution of a registered Step type. */
export interface StepExecutionId {
  /** Registered Step type. */
  readonly stepType: string;
  /** Positive execution number; defaults to the first execution. */
  readonly number?: number;
}

/** Creates normalized {@link StepExecutionId} values. */
export const StepExecutionId = Object.freeze({
  /**
   * Creates a Step execution identifier.
   * @param stepType - Non-empty registered Step type.
   * @param number - Positive execution number; defaults to one.
   * @returns The normalized identifier.
   */
  of(stepType: string, number?: number): StepExecutionId {
    return { stepType, number: number ?? 1 };
  },
});

/** Selects one Timer condition within a Step execution. */
export interface TimerId {
  /** Stable condition ID, mutually exclusive with `conditionIndex`. */
  readonly conditionId?: string;
  /** Zero-based condition index, mutually exclusive with `conditionId`. */
  readonly conditionIndex?: number;
}

/** Creates Timer selectors by stable ID or Wait-tree position. */
export const TimerId = Object.freeze({
  /**
   * Selects a Timer by stable condition ID.
   * @param conditionId - Non-empty ID assigned by `Timer.byDuration`.
   * @returns An ID-based Timer selector.
   */
  byConditionId(conditionId: string): TimerId {
    requireName(conditionId);
    return { conditionId };
  },
  /**
   * Selects a Timer by zero-based Wait-tree position.
   * @param conditionIndex - Non-negative flattened Timer index.
   * @returns An index-based Timer selector.
   */
  byConditionIndex(conditionIndex: number): TimerId {
    return { conditionIndex };
  },
});

/** Selects the historical point from which time travel creates a new run. */
export const TimeTravelType = Object.freeze({
  /** Restarts from the beginning of the Flow. */
  BEGINNING: "beginning",
  /** Resumes at the first event at or after a timestamp. */
  HISTORY_EVENT_TIME: "historyEventTime",
  /** Resumes at the latest execution of a Step type. */
  STEP_TYPE: "stepType",
  /** Resumes at one exact Step execution ID. */
  STEP_EXECUTION_ID: "stepExecutionId",
} as const);

/** Represents a value from {@link TimeTravelType}. */
export type TimeTravelType = (typeof TimeTravelType)[keyof typeof TimeTravelType];

/** Selects the Step method used as a Step execution time travel boundary. */
export const TimeTravelStepMethod = Object.freeze({
  /** Reruns WaitFor and everything after it. */
  WAIT_FOR: "waitFor",
  /** Keeps the WaitFor result and reruns Execute and everything after it. */
  EXECUTE: "execute",
} as const);

/** Represents a value from {@link TimeTravelStepMethod}. */
export type TimeTravelStepMethod = (typeof TimeTravelStepMethod)[keyof typeof TimeTravelStepMethod];

/** Configures creation of a new run from existing Flow history. */
export interface TimeTravelOptions {
  /** Time travel point selector kind. */
  readonly type: TimeTravelType;
  /** Timestamp used with `HISTORY_EVENT_TIME`. */
  readonly historyEventTime?: Date;
  /** Registered Step type used with `STEP_TYPE`. */
  readonly stepType?: string;
  /** Exact execution ID used with `STEP_EXECUTION_ID`. */
  readonly stepExecutionId?: string;
  /** WaitFor or Execute boundary required with `STEP_EXECUTION_ID`. */
  readonly stepMethod?: TimeTravelStepMethod;
  /** Optional operator-readable time travel reason. */
  readonly reason?: string;
  /** Prevents reapplication of RPCs, Channel publications, and Attribute writes after the selected point. */
  readonly skipWritesReapply?: boolean;
}

/** Selects how an active Flow should close. */
export const StopType = Object.freeze({
  /** Requests cooperative cancellation. */
  CANCEL: "cancel",
  /** Forces immediate termination. */
  TERMINATE: "terminate",
  /** Closes the Flow as failed. */
  FAIL: "fail",
} as const);

/** Represents a value from {@link StopType}. */
export type StopType = (typeof StopType)[keyof typeof StopType];

/** Configures a `stopFlow` request. */
export interface StopFlowOptions {
  /** Stop behavior; defaults to cooperative cancellation. */
  readonly type?: StopType;
  /** Optional operator-readable reason recorded by Dex. */
  readonly reason?: string;
}

/**
 * Configures Worker startup and either a listening endpoint or outbound pull streams.
 *
 * Set `pull: {}` to execute tasks without a listener. A pull Worker cannot set
 * `bindAddress` or `workerTarget`. See {@link PullWorkerOptions} for capacity and shutdown.
 */
export interface WorkerOptions {
  /** Enables outbound polling when supplied; omitted to listen for WorkerService calls. */
  readonly pull?: PullWorkerOptions;
  /** Local plaintext WorkerService listener; defaults to `127.0.0.1:8803` outside pull mode. */
  readonly bindAddress?: string;
  /** Endpoint advertised to Dex; derived from `bindAddress` when omitted. */
  readonly workerTarget?: WorkerTarget;
  /** Dex FlowService target; defaults to `localhost:8801`. */
  readonly serverAddress?: string;
  /** Bearer token provider for startup, hydration, and pull streams; omitted sends no authorization metadata. */
  readonly credential?: Credential;
  /** Startup Indexed Attribute synchronization deadline in milliseconds. Defaults to 120000. */
  readonly attributeIndexSyncTimeoutMs?: number;
}

/**
 * Configures an outbound pull Worker.
 *
 * Pass this as `WorkerOptions.pull`, then advertise `worker.workerTarget` to a Client.
 * Each stream handles one task, while handlers on different streams can run concurrently.
 *
 * @example
 * ```ts
 * const worker = new Worker(registry, cache, { pull: { maxConcurrentTasks: 8 } });
 * await worker.start();
 * const client = new Client(registry, cache, { workerTarget: worker.workerTarget });
 * try { await client.startFlow(flow, flowId); } finally { await client.close(); await worker.close(); }
 * ```
 */
export interface PullWorkerOptions {
  /** Maximum simultaneous handlers; zero or omission uses the logical CPU count. Negative values are invalid. */
  readonly maxConcurrentTasks?: number;
  /** Waiting streams on the sticky queue and each Flow type queue; zero or omission uses one. Negative values are invalid. */
  readonly idleStreamsPerQueue?: number;
  /** Process-unique Worker ID; defaults to host identity plus eight random base32 characters. Use a new ID after every restart. Values contain 1-63 ASCII letters, digits, dots, underscores, or hyphens and cannot equal a registered Flow type. */
  readonly appWorkerID?: string;
  /** Milliseconds allowed for graceful shutdown; defaults to 30000. Requires a non-negative finite value. After expiry, Worker.close cancels active handlers and waits for them to return. */
  readonly stopDrainTimeoutMs?: number;
}

/**
 * Selects the listening endpoint used for Step and RPC work.
 *
 * Use this for a WorkerService listener. Use {@link PullWorkerTarget} for
 * outbound pull routing.
 *
 * @example
 * ```ts
 * const client = new Client(registry, cache, { workerTarget: { address: "worker.example:8803" } });
 * ```
 */
export interface WorkerTarget {
  /** Plaintext gRPC target; headless targets must use `host:port`. */
  readonly address: string;
  /** Whether Dex connects directly without service discovery. */
  readonly headless?: boolean;
}

/**
 * Routes Step and RPC work through Dex task queues to an outbound pull Worker.
 *
 * Supply this target as a Client default, StartFlow configuration override, or
 * SubFlow configuration override. The Worker must have `WorkerOptions.pull` enabled.
 * Pull cannot include a nonempty address or enable headless routing.
 *
 * @example
 * ```ts
 * const client = new Client(registry, cache, { workerTarget: { pull: true } });
 * await client.startFlow(flow, flowId, input);
 * ```
 */
export interface PullWorkerTarget {
  /** Enables routing through Dex task queues. */
  readonly pull: true;
  /** Omit this field or use an empty string; pull Workers do not listen. */
  readonly address?: "";
  /** Omit this field or use false; headless routing requires a listener. */
  readonly headless?: false;
}

/** Selects whether a visibility search includes earlier Continue-as-New runs. */
export interface SearchFlowsOptions {
  /** Disable default exclusion while retaining every explicit query filter. Defaults to false. */
  readonly includeContinuedAsNew?: boolean;
}
