/*
 * Copyright (c) 2026 Super Durable, Inc.
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

package io.superdurable.dex.primitives.attribute;

import io.superdurable.dex.Attribute;
import io.superdurable.dex.AttributeIndex;
import io.superdurable.dex.AttributeLock;
import io.superdurable.dex.AttributeMap;
import io.superdurable.dex.Context;
import io.superdurable.dex.Flow;
import io.superdurable.dex.FlowConfig;
import io.superdurable.dex.PersistenceSchema;
import io.superdurable.dex.RPC;
import io.superdurable.dex.RPCAttributeMapLock;
import io.superdurable.dex.RPCResult;
import io.superdurable.dex.Step;
import io.superdurable.dex.StepDecision;
import io.superdurable.dex.StepList;
import io.superdurable.dex.StepOptions;
import io.superdurable.dex.Wait;
import org.springframework.stereotype.Component;

@Component
public class AttributeFlow implements Flow<String> {
    private final Attribute<String> status = Attribute.define(
            "primitive-attribute-status",
            String.class,
            new AttributeIndex(AttributeIndex.Type.KEYWORD, "keyword1"));
    private final Attribute<String> email = Attribute.define(
            "primitive-attribute-email",
            String.class).syncToAttributeStore();
    private final Attribute<String> modelApiKey = Attribute.define(
            "primitive-attribute-model-api-key",
            String.class).hardDeleteOnNull();
    private final AttributeMap<String> progress =
            AttributeMap.define(
                    "primitive-attribute-progress",
                    String.class,
                    new AttributeIndex(AttributeIndex.Type.KEYWORD, "keyword2"));
    private final FlowConfig attributeStoreConfig = FlowConfig.newBuilder()
            .attributeStoreNames("profiles")
            .build();
    private final AttributeStep start = new AttributeStep();

    @Override
    public StepList<String> getSteps() {
        return StepList.startStep(start);
    }

    @Override
    public PersistenceSchema getPersistenceSchema() {
        return PersistenceSchema.of(status, progress, email, modelApiKey);
    }

    final class AttributeStep implements Step<String> {
        @Override
        public Class<String> getInputType() {
            return String.class;
        }

        @Override
        public StepOptions getStepOptions() {
            return StepOptions.newBuilder()
                    .addWaitForLock(AttributeLock.of(status))
                    .addWaitForLock(AttributeLock.of(progress, "payment"))
                    .addExecuteLock(AttributeLock.of(status))
                    .addExecuteLock(AttributeLock.of(progress, "payment"))
                    .build();
        }

        @Override
        public Wait waitFor(final Context context, final String input) {
            status.set(context, "processing");
            progress.set(context, "payment", "authorized");
            return Wait.skipImmediately();
        }

        @Override
        public StepDecision execute(final Context context, final String input) {
            status.set(context, "completed");
            // The Flow no longer needs the short-lived key, so delete it permanently.
            modelApiKey.delete(context);
            return StepDecision.gracefulComplete(input);
        }
    }

    @RPC(
            lockAttributes = {"primitive-attribute-status"},
            lockAttributeMaps = {
                @RPCAttributeMapLock(
                        attribute = "primitive-attribute-progress",
                        instance = "payment")
            })
    public RPCResult<String> updateStatus(final Context context, final String input) {
        status.set(context, input);
        progress.set(context, "payment", input);
        return RPCResult.of(input);
    }
}
