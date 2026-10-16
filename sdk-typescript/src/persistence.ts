// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import type { Channel, ChannelMap } from "./wait.js";
import {
  markHardDeleteOnNull,
  requireHardDeleteOnNullCompatible,
  requireNotHardDeleteOnNull,
} from "./attribute-hard-delete.js";
import { markAttributeStoreSynced } from "./attribute-store-sync.js";
import type { Codec } from "./codec.js";
import type { Context } from "./context.js";
import type { Stream } from "./stream.js";
import { requireMapInstance, requirePersistenceDefinitionName } from "./validation.js";

/** Selects one AttributeMap instance for a handler snapshot. */
export interface AttributeMapLoad {
  /** Exact AttributeMap definition registered with the Flow. */
  readonly attributeMap: AttributeMap<unknown>;
  /** One slash-free logical instance key. */
  readonly instance: string;
}

/** Selects how Dex indexes an Attribute for Flow search. */
export const IndexType = Object.freeze({
  /** Indexes an exact string value. */
  KEYWORD: "keyword",
  /** Indexes a string for tokenized full-text matching. */
  FULL_TEXT: "fullText",
  /** Indexes every string in an array as a keyword. */
  KEYWORD_ARRAY: "keywordArray",
  /** Indexes a signed 64-bit integer. */
  INT: "int",
  /** Indexes a finite floating-point number. */
  DOUBLE: "double",
  /** Indexes a Boolean value. */
  BOOL: "bool",
  /** Indexes a JavaScript Date value. */
  DATETIME: "datetime",
} as const);

/** Represents a value from {@link IndexType}. */
export type IndexType = (typeof IndexType)[keyof typeof IndexType];

/** Configures the search index for an Attribute or AttributeMap. */
export interface AttributeIndex {
  /** Value representation accepted by the search index. */
  readonly type: IndexType;
  /** Physical search key; indexed AttributeMaps require an explicit key. */
  readonly indexKey?: string;
}

/** Describes an Attribute lock acquired for a Step or RPC handler. */
export interface AttributeLock {
  /** Singleton Attribute or AttributeMap definition to lock. */
  readonly attribute: Attribute<unknown> | AttributeMap<unknown>;
  /** AttributeMap instance; omitted for a singleton Attribute. */
  readonly instance?: string;
}

/**
 * Defines a typed, durable singleton value owned by a Flow.
 *
 * @example
 * ```ts
 * const status = new Attribute("status", stringCodec, {
 *   type: IndexType.KEYWORD,
 * });
 * const persistence: PersistenceSchema = { attributes: [status] };
 * ```
 * @typeParam T - Value encoded by the Attribute's codec.
 */
export class Attribute<T> {
  /**
   * Creates an Attribute definition for a PersistenceSchema.
   * @param name - Non-empty logical name without `/`, unique within the Flow.
   * @param codec - Value codec used by handlers and Client calls.
   * @param index - Optional visibility search index.
   */
  public constructor(
    public readonly name: string,
    public readonly codec: Codec<T>,
    public readonly index?: AttributeIndex,
  ) {
    requirePersistenceDefinitionName(name);
  }

  /**
   * Reads the current value from handler decision state.
   * @param context - Current Step or RPC Context.
   * @returns The decoded Attribute value.
   */
  public get(context: Context): T {
    return context.getAttribute(this);
  }

  /**
   * Stages a value to persist with the current decision.
   * @param context - Current Step or RPC Context.
   * @param value - Typed value to persist.
   */
  public set(context: Context, value: T): void {
    context.setAttribute(this, value);
  }

  /**
   * Stages deletion in the current decision.
   * @param context - Current Step or RPC Context.
   */
  public delete(context: Context): void {
    context.deleteAttribute(this as Attribute<unknown>);
  }

  /**
   * Returns an immutable Attribute definition whose writes are projected to the Flow's Attribute Store.
   *
   * Projection is asynchronous and latest-state only. Deletion writes SQL `NULL`, projection failures do
   * not roll back Flow Attribute writes, and the Flow must select a configured Attribute Store name.
   * @returns A new synced definition; this definition remains unchanged.
   * @throws {TypeError} When this definition uses {@link Attribute.hardDeleteOnNull}.
   */
  public syncToAttributeStore(): Attribute<T> {
    requireNotHardDeleteOnNull(this, "syncToAttributeStore");
    return markAttributeStoreSynced(new Attribute(this.name, this.codec, this.index));
  }

  /**
   * Returns an immutable Attribute definition for short-lived secrets that Dex can permanently delete.
   *
   * Use this option for values such as API keys that must not remain in Flow history. Dex stores
   * each value of a hard-delete Attribute only in the Dex blob store, never in Flow history.
   * Deleting the Attribute or setting it to `null` or `undefined` permanently deletes the stored value.
   * Register the returned definition in the Flow's PersistenceSchema. Use that same definition for
   * handler reads and writes, InitialAttribute values, and SubFlow initial Attributes.
   *
   * Only string and object values are supported. Object values include JSON values and byte arrays.
   * Numeric and Boolean values are rejected. A built-in numeric or Boolean codec fails here. A custom
   * codec that encodes a number or Boolean fails when the value is written. Deletion is always allowed.
   * A hard-delete Attribute cannot have an index and cannot use {@link Attribute.syncToAttributeStore}.
   * {@link Client.waitForAttributeMatch} rejects a hard-delete Attribute before contacting Dex.
   *
   * Writes require a Dex Server with an enabled blob store and Server protocol version 3 or later.
   * Otherwise, the write fails. The Client checks the Server protocol version once before its first
   * hard-delete write. The Worker checks it at startup and fails any handler result that writes a
   * hard-delete Attribute to an older Server. Step and RPC handlers receive the value inline. The SDK
   * never stores it in the local blob cache.
   *
   * Limitations:
   * - Each Flow keeps one stored copy per Attribute key and overwrites it in place.
   * - A Step or RPC attempt writes or deletes the stored value before Dex commits its result.
   *   A retried attempt, other Steps and RPCs, and Client reads can therefore observe a write or
   *   deletion from an attempt that later failed.
   * - When concurrent Steps or RPCs write the same key, the final stored value may not be the value
   *   of the last committed write.
   * - A failed `startFlow` or SubFlow start for a Flow ID that is already running still overwrites
   *   the running Flow's stored value. A null initial value deletes it.
   * - Executions that reuse a Flow ID share the stored value.
   * - {@link Client.timeTravel} does not restore a deleted value. A deleted value reads as absent.
   * - Values remain until they are deleted, or until Dex removes the Flow through retention and
   *   blob cleanup.
   * - Blob store backups or bucket versioning can retain copies outside Dex's control.
   *
   * @example
   * ```ts
   * const apiKey = new Attribute("api-key", stringCodec).hardDeleteOnNull();
   *
   * class CallVendorStep implements Step<string> {
   *   public readonly inputCodec = stringCodec;
   *
   *   public getStepType(): string {
   *     return "CallVendorStep";
   *   }
   *
   *   public async execute(context: Context, vendorUrl: string): Promise<StepDecision> {
   *     const response = await fetch(vendorUrl, {
   *       headers: { authorization: `Bearer ${apiKey.get(context)}` },
   *     });
   *     apiKey.delete(context);
   *     return gracefulComplete(response.status);
   *   }
   * }
   *
   * class VendorCallFlow implements Flow<string> {
   *   private readonly callVendor = new CallVendorStep();
   *
   *   public getFlowType(): string {
   *     return "VendorCallFlow";
   *   }
   *
   *   public getSteps() {
   *     return StepList.startStep(this.callVendor);
   *   }
   *
   *   public getPersistenceSchema(): PersistenceSchema {
   *     return { attributes: [apiKey] };
   *   }
   * }
   *
   * const flow = new VendorCallFlow();
   * const client = new Client(new Registry([flow]), cache);
   * await client.startFlow(flow, "vendor-call-42", "https://vendor.example/report", {
   *   attributes: [InitialAttribute.of(apiKey, process.env.VENDOR_API_KEY ?? "")],
   * });
   * ```
   * @returns A new hard-delete definition; this definition remains unchanged.
   * @throws {TypeError} When this definition has an index, uses Attribute Store synchronization, or
   * uses a built-in numeric or Boolean codec.
   */
  public hardDeleteOnNull(): Attribute<T> {
    requireHardDeleteOnNullCompatible(this as Attribute<unknown>);
    return markHardDeleteOnNull(new Attribute(this.name, this.codec, this.index));
  }

  /**
   * Creates a lock request for this Attribute.
   * @returns A lock for this singleton Attribute.
   */
  public lock(): AttributeLock {
    return { attribute: this as Attribute<unknown> };
  }
}

/**
 * Defines a typed family of durable values keyed by instance.
 * Slash is prohibited in instance keys because it is a reserved character.
 * @typeParam T - Value encoded for every map instance.
 */
export class AttributeMap<T> {
  /**
   * Creates an AttributeMap definition for a PersistenceSchema.
   * @param name - Non-empty logical name without `/`, unique within the Flow.
   * @param codec - Value codec shared by every instance.
   * @param index - Optional shared visibility search index.
   */
  public constructor(
    public readonly name: string,
    public readonly codec: Codec<T>,
    public readonly index?: AttributeIndex,
  ) {
    requirePersistenceDefinitionName(name);
  }

  /**
   * Reads one map instance from handler decision state.
   * @param context - Current Step or RPC Context.
   * @param instance - The map instance. Slash is prohibited because it is a reserved character.
   * @returns The decoded instance value.
   */
  public get(context: Context, instance: string): T {
    return context.getAttribute(this, instance);
  }

  /**
   * Stages one map-instance write.
   * @param context - Current Step or RPC Context.
   * @param instance - The map instance. Slash is prohibited because it is a reserved character.
   * @param value - Typed value to persist.
   */
  public set(context: Context, instance: string, value: T): void {
    context.setAttribute(this, value, instance);
  }

  /**
   * Stages deletion of one map instance.
   * @param context - Current Step or RPC Context.
   * @param instance - The map instance. Slash is prohibited because it is a reserved character.
   */
  public delete(context: Context, instance: string): void {
    context.deleteAttribute(this as AttributeMap<unknown>, instance);
  }

  /**
   * Selects one logical instance for a handler snapshot.
   * @param instance - Non-empty logical map key. The SDK escapes it for the protocol.
   * @returns An exact instance load for RPC, Step, or timeout-handler options.
   */
  public load(instance: string): AttributeMapLoad {
    requireMapInstance(instance);
    return { attributeMap: this as AttributeMap<unknown>, instance };
  }

  /**
   * Returns the number of existing instances, including buffered writes.
   * @param context - Current Step or RPC Context.
   * @returns The number of keys visible after decision-local writes and deletions.
   */
  public getMapSize(context: Context): number {
    return this.getAllInstanceKeys(context).length;
  }

  /**
   * Returns decoded existing instance keys in ascending order.
   * @param context - Current Step or RPC Context.
   * @returns Keys visible after decision-local writes and deletions.
   */
  public getAllInstanceKeys(context: Context): readonly string[] {
    return context.attributeMapKeys(this as AttributeMap<unknown>);
  }

  /**
   * Returns an immutable AttributeMap definition whose writes are projected to the Flow's Attribute Store.
   *
   * Projection is asynchronous and latest-state only. Each map instance uses its physical Attribute name.
   * Deletion writes SQL `NULL`, projection failures do not roll back Flow Attribute writes, and the Flow
   * must select a configured Attribute Store name.
   * @returns A new synced definition; this definition remains unchanged.
   * @throws {TypeError} When this definition uses {@link AttributeMap.hardDeleteOnNull}.
   */
  public syncToAttributeStore(): AttributeMap<T> {
    requireNotHardDeleteOnNull(this, "syncToAttributeStore");
    return markAttributeStoreSynced(new AttributeMap(this.name, this.codec, this.index));
  }

  /**
   * Returns an immutable AttributeMap definition for short-lived secrets that Dex can permanently delete.
   *
   * Use this option for per-instance values such as API keys or access tokens that must not remain
   * in Flow history. Dex stores each instance value only in the Dex blob store, never in Flow history.
   * Deleting an instance or setting it to `null` or `undefined` permanently deletes its stored value.
   * Register the returned definition in the Flow's PersistenceSchema. Use that same definition for
   * handler reads and writes, InitialAttribute values, and SubFlow initial Attributes.
   *
   * Only string and object values are supported. Object values include JSON values and byte arrays.
   * Numeric and Boolean values are rejected. A built-in numeric or Boolean codec fails here. A custom
   * codec that encodes a number or Boolean fails when the value is written. Deletion is always allowed.
   * A hard-delete AttributeMap cannot have an index and cannot use
   * {@link AttributeMap.syncToAttributeStore}. {@link Client.waitForAttributeMatch} rejects a
   * hard-delete AttributeMap before contacting Dex.
   *
   * Writes require a Dex Server with an enabled blob store and Server protocol version 3 or later.
   * Otherwise, the write fails. The Client checks the Server protocol version once before its first
   * hard-delete write. The Worker checks it at startup and fails any handler result that writes a
   * hard-delete AttributeMap instance to an older Server. Step and RPC handlers receive instance
   * values inline. The SDK never stores them in the local blob cache.
   *
   * Limitations:
   * - Each Flow keeps one stored copy per map instance and overwrites it in place.
   * - A Step or RPC attempt writes or deletes the stored value before Dex commits its result.
   *   A retried attempt, other Steps and RPCs, and Client reads can therefore observe a write or
   *   deletion from an attempt that later failed.
   * - When concurrent Steps or RPCs write the same instance, the final stored value may not be the
   *   value of the last committed write.
   * - A failed `startFlow` or SubFlow start for a Flow ID that is already running still overwrites
   *   the running Flow's stored value. A null initial value deletes it.
   * - Executions that reuse a Flow ID share the stored values.
   * - {@link Client.timeTravel} does not restore a deleted value. A deleted instance reads as absent.
   * - Values remain until they are deleted, or until Dex removes the Flow through retention and
   *   blob cleanup.
   * - Blob store backups or bucket versioning can retain copies outside Dex's control.
   *
   * @example
   * ```ts
   * type VendorToken = { readonly token: string };
   * const vendorTokens = new AttributeMap("vendor-tokens", jsonCodec<VendorToken>())
   *   .hardDeleteOnNull();
   *
   * class RevokeTokenStep implements Step<string> {
   *   public readonly inputCodec = stringCodec;
   *
   *   public getStepType(): string {
   *     return "RevokeTokenStep";
   *   }
   *
   *   public getStepOptions(): StepOptions {
   *     return { executeLoadAttributeMapInstances: [vendorTokens.load("billing")] };
   *   }
   *
   *   public async execute(context: Context, revokeUrl: string): Promise<StepDecision> {
   *     const { token } = vendorTokens.get(context, "billing");
   *     await fetch(revokeUrl, { method: "POST", body: token });
   *     vendorTokens.delete(context, "billing");
   *     return gracefulComplete("revoked");
   *   }
   * }
   *
   * class TokenRevocationFlow implements Flow<string> {
   *   private readonly revokeToken = new RevokeTokenStep();
   *
   *   public getFlowType(): string {
   *     return "TokenRevocationFlow";
   *   }
   *
   *   public getSteps() {
   *     return StepList.startStep(this.revokeToken);
   *   }
   *
   *   public getPersistenceSchema(): PersistenceSchema {
   *     return { attributes: [vendorTokens] };
   *   }
   * }
   *
   * const flow = new TokenRevocationFlow();
   * const client = new Client(new Registry([flow]), cache);
   * await client.startFlow(flow, "revoke-42", "https://vendor.example/revoke", {
   *   attributes: [InitialAttribute.mapValue(vendorTokens, "billing", { token: "token-value" })],
   * });
   * ```
   * @returns A new hard-delete definition; this definition remains unchanged.
   * @throws {TypeError} When this definition has an index, uses Attribute Store synchronization, or
   * uses a built-in numeric or Boolean codec.
   */
  public hardDeleteOnNull(): AttributeMap<T> {
    requireHardDeleteOnNullCompatible(this as AttributeMap<unknown>);
    return markHardDeleteOnNull(new AttributeMap(this.name, this.codec, this.index));
  }

  /**
   * Creates a lock request scoped to one map instance.
   * @param instance - The map instance. Slash is prohibited because it is a reserved character.
   * @returns A lock for the requested instance.
   */
  public lock(instance: string): AttributeLock {
    requireMapInstance(instance);
    return { attribute: this as AttributeMap<unknown>, instance };
  }
}

/** Declares the Attributes, Channels, and Streams owned by a Flow type. */
export interface PersistenceSchema {
  /** Singleton and map Attribute definitions with unique names. */
  readonly attributes?: readonly (Attribute<unknown> | AttributeMap<unknown>)[];
  /** Singleton and map Channel definitions with unique names. */
  readonly channels?: readonly (Channel<unknown> | ChannelMap<unknown>)[];
  /** Best-effort Stream definitions with unique names. */
  readonly streams?: readonly Stream<unknown>[];
}
