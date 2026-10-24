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

package io.superdurable.dex.integ.smoke;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class FlowSmokeCatalog {
    private FlowSmokeCatalog() {
    }

    static List<FlowSmokeEntry> entries(final FlowSmokeEnvironment environment) {
        return List.of(
                FlowSmokeEntry.get("products/engagement", "/products/engagement/start", Map.of()),
                FlowSmokeEntry.get(
                        "products/microservices",
                        "/products/microservices/start",
                        Map.of("workflowId", environment.newFlowId("microservices"))),
                FlowSmokeEntry.get(
                        "products/money-transfer",
                        "/products/money-transfer/start",
                        Map.of(
                                "amount", "100",
                                "fromAccount", "from-smoke",
                                "toAccount", "to-smoke",
                                "notes", "smoke"))
                        .withServerZDUnsupportedFeature("failure policies"),
                FlowSmokeEntry.get(
                        "products/order-processing",
                        "/products/order-processing/start",
                        Map.of())
                        .withServerZDUnsupportedFeature("failure policies"),
                FlowSmokeEntry.get("products/subscription", "/products/subscription/start", Map.of()),
                FlowSmokeEntry.get(
                        "products/signup",
                        "/products/signup/submit",
                        signupQuery(environment)),
                FlowSmokeEntry.get(
                        "products/job-post",
                        "/products/job-post/create",
                        Map.of(
                                "title", "Smoke Test Job",
                                "description", "Smoke test description"),
                        FlowSmokeFlags.noStartStep()),
                FlowSmokeEntry.get(
                        "patterns/polling",
                        "/patterns/polling/start",
                        Map.of("workflowId", environment.newFlowId("pattern-polling")))
                        .withServerZDUnsupportedFeature("failure policies"),
                FlowSmokeEntry.get(
                        "patterns/interruptible",
                        "/patterns/interruptible/start",
                        Map.of("workflowId", environment.newFlowId("interruptible"))),
                FlowSmokeEntry.get("patterns/reminders", "/patterns/reminders/start", Map.of()),
                entityStore(environment),
                FlowSmokeEntry.post(
                        "patterns/sequentially-chunked-attribute-map",
                        "/patterns/sequentially-chunked-attribute-map/start",
                        null,
                        FlowSmokeFlags.noStartStep()),
                FlowSmokeEntry.post(
                        "patterns/hash-partitioned-attribute-map",
                        "/patterns/hash-partitioned-attribute-map/start",
                        null,
                        FlowSmokeFlags.noStartStep()),
                FlowSmokeEntry.get(
                        "patterns/manual-recovery",
                        "/patterns/manual-recovery/start",
                        Map.of("workflowId", environment.newFlowId("manual-recovery")))
                        .withServerZDUnsupportedFeature("failure policies"),
                FlowSmokeEntry.get(
                        "patterns/inactiveness-tracker-timer",
                        "/patterns/inactiveness-tracker-timer/start",
                        Map.of("workflowId", environment.newFlowId("inactiveness-tracker-timer"))),
                FlowSmokeEntry.get(
                        "patterns/parallel/static",
                        "/patterns/parallel/start/static",
                        Map.of("workflowId", environment.newFlowId("parallel-static"))),
                FlowSmokeEntry.get(
                        "patterns/parallel/dynamic",
                        "/patterns/parallel/start/dynamic",
                        Map.of("workflowId", environment.newFlowId("parallel-dynamic"))),
                FlowSmokeEntry.get(
                        "patterns/parallel/await",
                        "/patterns/parallel/start/await",
                        Map.of("workflowId", environment.newFlowId("parallel-await"))),
                FlowSmokeEntry.get(
                        "patterns/parallel/first-win",
                        "/patterns/parallel/start/first-win",
                        Map.of("workflowId", environment.newFlowId("parallel-first-win"))),
                FlowSmokeEntry.get(
                        "patterns/recovery",
                        "/patterns/recovery/start",
                        Map.of(
                                "workflowId", environment.newFlowId("recovery"),
                                "itemName", "smoke-item",
                                "quantity", "2"),
                        FlowSmokeFlags.stepStartMayFail())
                        .withServerZDUnsupportedFeature("failure policies"),
                parallelSubFlows(environment, "basic"),
                parallelSubFlows(environment, "wait-for-half"),
                parallelSubFlows(environment, "long-lived-parent"),
                parallelSubFlows(environment, "short-lived-parent"),
                FlowSmokeEntry.get(
                        "patterns/drain-channels/internal",
                        "/patterns/drain-channels/internal/start",
                        Map.of("workflowId", environment.newFlowId("drain-internal"))),
                FlowSmokeEntry.get(
                        "patterns/drain-channels/external-publishing",
                        "/patterns/drain-channels/external-publishing/start-or-publish",
                        Map.of("workflowId", environment.newFlowId("drain-external"))),
                FlowSmokeEntry.get(
                        "patterns/wait-for-step-completion",
                        "/patterns/wait-for-step-completion/start",
                        Map.of("workflowId", environment.newFlowId("wait-for-state"))),
                FlowSmokeEntry.get(
                        "patterns/timeout",
                        "/patterns/timeout/start",
                        Map.of(
                                "workflowId", environment.newFlowId("timeout"),
                                "successfulWorkflow", "true")),
                FlowSmokeEntry.get(
                        "primitives/step",
                        "/primitives/step/start",
                        Map.of(
                                "workflowId", environment.newFlowId("primitive-step"),
                                "inputNum", "1")),
                FlowSmokeEntry.get(
                        "primitives/step/retry",
                        "/primitives/step/retry/start",
                        Map.of(
                                "workflowId", environment.newFlowId("primitive-step-retry"),
                                "readyAfterAttempt", "2")),
                FlowSmokeEntry.get(
                        "primitives/step/custom-retry",
                        "/primitives/step/custom-retry/start",
                        Map.of(
                                "workflowId", environment.newFlowId("primitive-step-custom-retry"),
                                "readyAfterAttempt", "1")),
                FlowSmokeEntry.get(
                        "primitives/step/durability",
                        "/primitives/step/durability/start",
                        Map.of(
                                "workflowId", environment.newFlowId("primitive-step-durability"),
                                "mode", "sync")),
                FlowSmokeEntry.get(
                        "primitives/step/heartbeat",
                        "/primitives/step/heartbeat/start",
                        Map.of(
                                "workflowId", environment.newFlowId("primitive-step-heartbeat"),
                                "batches", "0")),
                FlowSmokeEntry.get(
                        "primitives/step/options-override",
                        "/primitives/step/options-override/start",
                        Map.of(
                                "workflowId", environment.newFlowId("primitive-step-options-override"),
                                "input", "smoke"))
                        .withServerZDUnsupportedFeature("failure policies"),
                FlowSmokeEntry.get(
                        "primitives/step/step-decision",
                        "/primitives/step/step-decision/start",
                        Map.of(
                                "workflowId", environment.newFlowId("primitive-step-decision"),
                                "mode", "graceful")),
                FlowSmokeEntry.get(
                        "primitives/step/wait-types",
                        "/primitives/step/wait-types/start",
                        Map.of(
                                "workflowId", environment.newFlowId("primitive-step-wait-types"),
                                "mode", "any",
                                "timeoutSeconds", "1")),
                FlowSmokeEntry.get(
                        "primitives/attribute",
                        "/primitives/attribute/start",
                        Map.of(
                                "workflowId", environment.newFlowId("primitive-attribute"),
                                "message", "smoke")),
                FlowSmokeEntry.get(
                        "primitives/channel",
                        "/primitives/channel/start",
                        Map.of(
                                "workflowId", environment.newFlowId("primitive-channel"),
                                "inputNum", "1")),
                FlowSmokeEntry.get(
                        "primitives/stream",
                        "/primitives/stream/start",
                        Map.of(
                                "workflowId", environment.newFlowId("primitive-stream"),
                                "input", "smoke")),
                FlowSmokeEntry.get(
                        "primitives/timer",
                        "/primitives/timer/start",
                        Map.of(
                                "workflowId", environment.newFlowId("primitive-timer"),
                                "seconds", "1")),
                FlowSmokeEntry.get(
                        "primitives/rpc",
                        "/primitives/rpc/start",
                        Map.of("workflowId", environment.newFlowId("primitive-rpc"))),
                FlowSmokeEntry.get(
                        "primitives/subflow",
                        "/primitives/subflow/start",
                        Map.of(
                                "workflowId", environment.newFlowId("primitive-subflow"),
                                "inputNum", "1")),
                FlowSmokeEntry.get(
                        "primitives/client-apis",
                        "/primitives/client-apis/start",
                        Map.of(
                                "workflowId", environment.newFlowId("primitive-client-apis"),
                                "keyword", "smoke")));
    }

    private static FlowSmokeEntry parallelSubFlows(
            final FlowSmokeEnvironment environment,
            final String kind) {
        return FlowSmokeEntry.get(
                "patterns/parallel-subflows/" + kind,
                "/patterns/parallel-subflows/start/" + kind,
                Map.of("workflowId", environment.newFlowId("parallel-subflows-" + kind)));
    }

    private static Map<String, String> signupQuery(final FlowSmokeEnvironment environment) {
        final String username = environment.newFlowId("signup");
        return Map.of(
                "username", username,
                "email", username + "@example.com");
    }

    private static FlowSmokeEntry entityStore(final FlowSmokeEnvironment environment) {
        return FlowSmokeEntry.custom(
                "patterns/entity-store",
                FlowSmokeFlags.noStartStep(),
                env -> {
                    final String userId = env.newFlowId("entity-store");
                    final Map<String, Object> body = new LinkedHashMap<>();
                    body.put("userId", userId);
                    body.put("displayName", "Smoke Tester");
                    body.put("email", userId + "@example.com");
                    body.put("marketingOptIn", true);
                    body.put("credits", 120);
                    body.put("weight", 59.5);
                    body.put("lastLoggedInTime", "2026-08-11T15:30:00Z");
                    body.put("metadata", Map.of("source", "smoke", "tags", List.of("example")));
                    final FlowSmokeTriggerResult result =
                            env.triggerHttp(
                                    "POST", "/patterns/entity-store/profile", Map.of(), body);
                    final String flowId = result.flowId.isEmpty() ? userId : result.flowId;
                    return new FlowSmokeTriggerResult(flowId, result.runId);
                })
                .withServerZDUnsupportedFeature("Attribute Store sync");
    }
}
