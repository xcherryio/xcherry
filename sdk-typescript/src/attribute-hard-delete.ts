// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import { isAttributeStoreSynced } from "./attribute-store-sync.js";
import type { Codec } from "./codec.js";
import { ValueMappingError } from "./errors.js";
import type { AttributeWrite, Value } from "./gen/dex.js";

export const hardDeleteAttributeMinimumServerProtocolVersion: number = 3;

const hardDeleteDefinitions = new WeakSet<object>();

export function markHardDeleteOnNull<Definition extends object>(
  definition: Definition,
): Definition {
  hardDeleteDefinitions.add(definition);
  return definition;
}

export function isHardDeleteOnNullDefinition(definition: object): boolean {
  return hardDeleteDefinitions.has(definition);
}

export function requireHardDeleteOnNullCompatible(definition: {
  readonly name: string;
  readonly codec: Codec<unknown>;
  readonly index?: unknown;
}): void {
  if (definition.index !== undefined) {
    throw new TypeError(`hard-delete Attribute ${definition.name} cannot be indexed`);
  }
  if (isAttributeStoreSynced(definition)) {
    throw new TypeError(
      `hard-delete Attribute ${definition.name} cannot sync to an Attribute Store`,
    );
  }
  const wireKind = definition.codec.wireKind;
  if (wireKind === "bool" || wireKind === "int64" || wireKind === "double") {
    throw new TypeError(
      `hard-delete Attribute ${definition.name} supports only string and object values; `
        + `codec ${definition.codec.typeName} uses ${wireKind}`,
    );
  }
}

export function requireNotHardDeleteOnNull(definition: { readonly name: string }, operation: string): void {
  if (isHardDeleteOnNullDefinition(definition)) {
    throw new TypeError(`${operation} does not support hard-delete Attribute ${definition.name}`);
  }
}

/** Validates the encoded value because a codec's declared wire kind does not bind its output. */
export function requireHardDeleteOnNullValueKind(definition: object, key: string, value: Value): Value {
  if (!isHardDeleteOnNullDefinition(definition)) {
    return value;
  }
  const kind = value.kind?.$case;
  if (kind !== "stringValue" && kind !== "objValue" && kind !== "nullValue") {
    throw new ValueMappingError(
      "encode",
      `hard-delete Attribute ${key} accepts only string, object, or null values; got ${kind ?? "no kind"}`,
    );
  }
  return value;
}

export function requireHardDeleteAttributeWriteSupport(
  negotiatedServerProtocolVersion: number,
  writes: readonly AttributeWrite[],
): void {
  if (negotiatedServerProtocolVersion >= hardDeleteAttributeMinimumServerProtocolVersion) {
    return;
  }
  const write = writes.find((candidate) => candidate.isHardDeleteOnNull);
  if (write !== undefined) {
    throw new Error(
      `hard-delete Attribute ${write.key} requires Server protocol version `
        + `${hardDeleteAttributeMinimumServerProtocolVersion} or later; `
        + `negotiated protocol version is ${negotiatedServerProtocolVersion}`,
    );
  }
}
