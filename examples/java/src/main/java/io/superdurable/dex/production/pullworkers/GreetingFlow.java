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

package io.superdurable.dex.production.pullworkers;

import io.superdurable.dex.Attribute;
import io.superdurable.dex.Context;
import io.superdurable.dex.Flow;
import io.superdurable.dex.PersistenceSchema;
import io.superdurable.dex.RPC;
import io.superdurable.dex.RPCResult;
import io.superdurable.dex.Step;
import io.superdurable.dex.StepDecision;
import io.superdurable.dex.StepList;

public class GreetingFlow implements Flow<String> {
    private final Attribute<String> greetingMessage =
            Attribute.define("pull-greeting-message", String.class);
    private final GreetStep greetStep = new GreetStep();

    @Override
    public StepList<String> getSteps() {
        return StepList.startStep(greetStep);
    }

    @Override
    public PersistenceSchema getPersistenceSchema() {
        return PersistenceSchema.of(greetingMessage);
    }

    @RPC
    public RPCResult<String> getGreeting(final Context context) {
        return RPCResult.of(greetingMessage.get(context));
    }

    final class GreetStep implements Step<String> {
        @Override
        public Class<String> getInputType() {
            return String.class;
        }

        @Override
        public StepDecision execute(final Context context, final String name) {
            final String message = "Hello, " + name + "!";
            greetingMessage.set(context, message);
            return StepDecision.gracefulComplete(message);
        }
    }
}
