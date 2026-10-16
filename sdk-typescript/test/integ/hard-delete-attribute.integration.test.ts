// Portions of this file are derived from indeedeng/iwf-java-sdk.
// Those portions are licensed under the Apache License, Version 2.0.
// See LICENSES/Apache-2.0.txt and LEGACY_NOTICES.md.
//
// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications are licensed under the Sustainable Use License 1.0.
// Third-Party Materials remain under the Apache License, Version 2.0.
// See LICENSE and LEGACY_NOTICES.md.

import assert from "node:assert/strict";
import test from "node:test";

import { AttributeMatch, InitialAttribute, stringCodec } from "../../src/index.js";
import type { KV } from "../../src/gen/dex.js";
import { flowId, withEnvironment } from "./environment.js";
import { getFlowAttributes } from "./flow-service-client.js";
import {
  HardDeleteAttributeFlow,
  initialApiKey,
  initialVendorToken,
  rotatedApiKey,
  rotatedVendorToken,
  vendorInstance,
} from "./hard_delete_attribute_flow.js";

const apiKeyName = "api-key";
const vendorTokenName = `vendor-tokens/${vendorInstance}`;

test("hard-delete Attributes are written inline, overwritten, and permanently deleted", async () => {
  const flow = new HardDeleteAttributeFlow();
  await withEnvironment([flow], async ({ client }) => {
    const id = flowId("hard-delete-attributes");
    await client.startFlow(flow, id, undefined, {
      attributes: [
        InitialAttribute.of(flow.apiKey, initialApiKey),
        InitialAttribute.mapValue(flow.vendorTokens, vendorInstance, { token: initialVendorToken }),
      ],
    });
    assert.equal(
      await client.waitForAttributeMatch(id, flow.phase, AttributeMatch.equalTo("rotated"), {
        requestId: `${id}-rotated`,
        requestTimeoutMs: 30_000,
      }),
      "rotated",
    );

    assert.deepEqual(await client.invokeRPC(flow.getSecrets, id), {
      apiKey: rotatedApiKey,
      vendorToken: rotatedVendorToken,
      vendorTokenInstances: [vendorInstance],
      phase: "rotated",
    });
    await requireInlineSecrets(id, rotatedApiKey, rotatedVendorToken);

    await client.invokeRPC(flow.setSecrets, id, {
      apiKey: "client-api-key-3f90",
      vendorToken: "client-vendor-token-8a17",
    });
    assert.deepEqual(await client.invokeRPC(flow.getSecrets, id), {
      apiKey: "client-api-key-3f90",
      vendorToken: "client-vendor-token-8a17",
      vendorTokenInstances: [vendorInstance],
      phase: "rotated",
    });
    await requireInlineSecrets(id, "client-api-key-3f90", "client-vendor-token-8a17");

    await client.invokeRPC(flow.deleteSecrets, id);
    assert.deepEqual(await client.invokeRPC(flow.getSecrets, id), {
      vendorTokenInstances: [],
      phase: "rotated",
    });
    await requireSecretsAbsent(id);

    await client.invokeRPC(flow.setSecrets, id, {
      apiKey: "recreated-api-key-61cd",
      vendorToken: "recreated-vendor-token-04be",
    });
    await requireInlineSecrets(id, "recreated-api-key-61cd", "recreated-vendor-token-04be");

    await client.invokeRPC(flow.requestDeletion, id);
    assert.equal(
      await client.waitForAttributeMatch(id, flow.phase, AttributeMatch.equalTo("deleted"), {
        requestId: `${id}-deleted`,
        requestTimeoutMs: 30_000,
      }),
      "deleted",
    );
    await requireSecretsAbsent(id);
    assert.deepEqual(await client.invokeRPC(flow.getSecrets, id), {
      vendorTokenInstances: [],
      phase: "deleted",
    });

    await client.invokeRPC(flow.requestCompletion, id);
    assert.equal(
      await client.waitForFlow(id, 30_000).then((result) => result.singleOutput(stringCodec)),
      "deleted",
    );
  });
});

async function requireInlineSecrets(id: string, apiKey: string, vendorToken: string): Promise<void> {
  const attributes = attributesByKey(await getFlowAttributes(id, [apiKeyName, vendorTokenName]));
  const apiKeyValue = attributes.get(apiKeyName)?.value?.kind;
  assert.equal(apiKeyValue?.$case, "stringValue");
  assert.equal(apiKeyValue.value, apiKey);
  const vendorTokenValue = attributes.get(vendorTokenName)?.value?.kind;
  assert.equal(vendorTokenValue?.$case, "objValue");
  assert.equal(vendorTokenValue.value.encoding, "json");
  assert.deepEqual(JSON.parse(new TextDecoder().decode(vendorTokenValue.value.payload)), { token: vendorToken });
}

async function requireSecretsAbsent(id: string): Promise<void> {
  const attributes = attributesByKey(await getFlowAttributes(id, [apiKeyName, vendorTokenName]));
  assert.equal(attributes.has(apiKeyName), false);
  assert.equal(attributes.has(vendorTokenName), false);
}

function attributesByKey(attributes: readonly KV[]): Map<string, KV> {
  return new Map(attributes.map((attribute) => [attribute.key, attribute]));
}
