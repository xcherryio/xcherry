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

import java.util.Map;

@FunctionalInterface
interface FlowSmokeTrigger {
    FlowSmokeTriggerResult trigger(FlowSmokeEnvironment environment) throws Exception;
}

final class FlowSmokeEntry {
    final String name;
    final String method;
    final String path;
    final Map<String, String> query;
    final Object body;
    final FlowSmokeFlags flags;
    final FlowSmokeTrigger trigger;
    // Names the feature that makes server-zd reject this sample; empty when supported.
    final String serverZDUnsupportedFeature;

    private FlowSmokeEntry(
            final String name,
            final String method,
            final String path,
            final Map<String, String> query,
            final Object body,
            final FlowSmokeFlags flags,
            final FlowSmokeTrigger trigger) {
        this(name, method, path, query, body, flags, trigger, "");
    }

    private FlowSmokeEntry(
            final String name,
            final String method,
            final String path,
            final Map<String, String> query,
            final Object body,
            final FlowSmokeFlags flags,
            final FlowSmokeTrigger trigger,
            final String serverZDUnsupportedFeature) {
        this.name = name;
        this.method = method;
        this.path = path;
        this.query = query;
        this.body = body;
        this.flags = flags;
        this.trigger = trigger;
        this.serverZDUnsupportedFeature = serverZDUnsupportedFeature;
    }

    FlowSmokeEntry withServerZDUnsupportedFeature(final String feature) {
        return new FlowSmokeEntry(name, method, path, query, body, flags, trigger, feature);
    }

    static FlowSmokeEntry get(
            final String name,
            final String path,
            final Map<String, String> query) {
        return get(name, path, query, FlowSmokeFlags.none());
    }

    static FlowSmokeEntry get(
            final String name,
            final String path,
            final Map<String, String> query,
            final FlowSmokeFlags flags) {
        return new FlowSmokeEntry(
                name,
                "GET",
                path,
                query,
                null,
                flags,
                environment -> environment.triggerHttp("GET", path, query, null));
    }

    static FlowSmokeEntry post(
            final String name,
            final String path,
            final Object body) {
        return post(name, path, body, FlowSmokeFlags.none());
    }

    static FlowSmokeEntry post(
            final String name,
            final String path,
            final Object body,
            final FlowSmokeFlags flags) {
        return new FlowSmokeEntry(
                name,
                "POST",
                path,
                Map.of(),
                body,
                flags,
                environment -> environment.triggerHttp("POST", path, Map.of(), body));
    }

    static FlowSmokeEntry custom(
            final String name,
            final FlowSmokeFlags flags,
            final FlowSmokeTrigger trigger) {
        return new FlowSmokeEntry(name, "", "", Map.of(), null, flags, trigger);
    }

    @Override
    public String toString() {
        return name;
    }
}

final class FlowSmokeTriggerResult {
    final String flowId;
    final String runId;

    FlowSmokeTriggerResult(final String flowId, final String runId) {
        this.flowId = flowId;
        this.runId = runId;
    }
}
