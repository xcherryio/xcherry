/*
 * Portions of this file are derived from indeedeng/iwf-java-sdk.
 * Those portions are licensed under the Apache License, Version 2.0.
 * See LICENSES/Apache-2.0.txt and LEGACY_NOTICES.md.
 *
 * Modifications Copyright (c) 2026 Super Durable, Inc.
 *
 * Modifications are licensed under the Sustainable Use License 1.0.
 * Third-Party Materials remain under the Apache License, Version 2.0.
 * See LICENSE and LEGACY_NOTICES.md.
 */

package io.superdurable.dex.integ;

import io.superdurable.dex.Context;
import io.superdurable.dex.Flow;
import io.superdurable.dex.PersistenceSchema;
import io.superdurable.dex.RetryPolicy;
import io.superdurable.dex.Step;
import io.superdurable.dex.StepDecision;
import io.superdurable.dex.StepDurability;
import io.superdurable.dex.StepList;
import io.superdurable.dex.StepOptions;
import io.superdurable.dex.Stream;

import java.time.Duration;

final class HeartbeatTestWorkflow implements Flow<Void> {
    enum Scenario {
        RESTORE_VALUE,
        CLEAR_VALUE,
        LOCAL_IGNORES_VALUE
    }

    final Stream<String> progress = Stream.define("heartbeat-progress", String.class, 1L << 20);
    private final HeartbeatStep start = new HeartbeatStep();
    private final Scenario scenario;
    private final boolean isPullMode;

    HeartbeatTestWorkflow(final Scenario scenario, final boolean isPullMode) {
        this.scenario = scenario;
        this.isPullMode = isPullMode;
    }

    @Override
    public StepList<Void> getSteps() {
        return StepList.startStep(start);
    }

    @Override
    public PersistenceSchema getPersistenceSchema() {
        return PersistenceSchema.of(progress);
    }

    final class HeartbeatStep implements Step<Void> {
        @Override
        public Class<Void> getInputType() {
            return Void.class;
        }

        @Override
        public StepDecision execute(final Context context, final Void input) {
            if (scenario == Scenario.LOCAL_IGNORES_VALUE) {
                if (context.getAttempt() <= 3) {
                    context.recordHeartbeat("local-checkpoint");
                    progress.write(context, "local-attempt-" + context.getAttempt());
                    throw new IllegalStateException("retry local activity");
                }
                if (isPullMode) {
                    if (!context.hasLastHeartbeatValue()
                            || !"local-checkpoint".equals(context.getLastHeartbeatValue(String.class))) {
                        throw new IllegalStateException("regular retry heartbeat was not restored");
                    }
                } else if (context.hasLastHeartbeatValue()) {
                    throw new IllegalStateException("local heartbeat reached fallback");
                }
                return StepDecision.gracefulComplete("local-ignored");
            }
            if (context.getAttempt() == 1) {
                context.recordHeartbeat("checkpoint");
                if (scenario == Scenario.CLEAR_VALUE) {
                    context.recordHeartbeat(null);
                }
                progress.write(context, "attempt-one");
                throw new IllegalStateException("retry after heartbeat");
            }
            if (scenario == Scenario.RESTORE_VALUE) {
                if (!context.hasLastHeartbeatValue()
                        || !"checkpoint".equals(
                                context.getLastHeartbeatValue(String.class))) {
                    throw new IllegalStateException("heartbeat value was not restored");
                }
                return StepDecision.gracefulComplete("restored");
            }
            if (context.hasLastHeartbeatValue()) {
                throw new IllegalStateException("cleared heartbeat value was restored");
            }
            return StepDecision.gracefulComplete("cleared");
        }

        @Override
        public StepOptions getStepOptions() {
            final StepOptions.Builder options = StepOptions.newBuilder()
                    .heartbeatTimeout(Duration.ofSeconds(10))
                    .executeRetry(RetryPolicy.newBuilder()
                            .initialInterval(Duration.ofSeconds(1))
                            .maximumAttempts(scenario == Scenario.LOCAL_IGNORES_VALUE ? 4 : 2)
                            .totalDuration(Duration.ofSeconds(30))
                            .build());
            if (scenario == Scenario.LOCAL_IGNORES_VALUE) {
                options.executeDurability(StepDurability.ASYNC);
            }
            return options.build();
        }
    }
}
