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

import io.superdurable.dex.Attribute;
import io.superdurable.dex.AttributeMap;
import io.superdurable.dex.Channel;
import io.superdurable.dex.Context;
import io.superdurable.dex.Flow;
import io.superdurable.dex.PersistenceSchema;
import io.superdurable.dex.RPC;
import io.superdurable.dex.RPCResult;
import io.superdurable.dex.Step;
import io.superdurable.dex.StepDecision;
import io.superdurable.dex.StepList;
import io.superdurable.dex.StepOptions;
import io.superdurable.dex.Wait;

import java.util.List;

class HardDeleteAttributeWorkflow implements Flow<String> {
    static final String TENANT_CREDENTIALS_NAME = "hard-delete-tenant-credentials";

    final Attribute<String> apiKey =
            Attribute.define("hard-delete-api-key", String.class).hardDeleteOnNull();
    final AttributeMap<TenantCredential> tenantCredentials = AttributeMap
            .define(TENANT_CREDENTIALS_NAME, TenantCredential.class)
            .hardDeleteOnNull();
    final Attribute<String> stepObservation =
            Attribute.define("hard-delete-step-observation", String.class);
    final Channel<Void> deletionRequests = Channel.define("hard-delete-deletion-requests", Void.class);
    final Channel<Void> finishRequests = Channel.define("hard-delete-finish-requests", Void.class);
    private final HardDeleteAttributeRotateStep rotate = new HardDeleteAttributeRotateStep();
    private final HardDeleteAttributeDeleteStep delete = new HardDeleteAttributeDeleteStep();
    private final HardDeleteAttributeFinishStep finish = new HardDeleteAttributeFinishStep();

    @Override
    public StepList<String> getSteps() {
        return StepList.startStep(rotate).otherSteps(delete, finish);
    }

    @Override
    public PersistenceSchema getPersistenceSchema() {
        return PersistenceSchema.of(
                apiKey,
                tenantCredentials,
                stepObservation,
                deletionRequests,
                finishRequests);
    }

    @RPC(loadAttributeMaps = {TENANT_CREDENTIALS_NAME})
    public RPCResult<HardDeleteAttributeSnapshot> getHardDeleteAttributes(final Context context) {
        final HardDeleteAttributeSnapshot snapshot = new HardDeleteAttributeSnapshot();
        snapshot.apiKey = apiKey.get(context);
        snapshot.tenantA = tenantCredentials.get(context, "tenant-a");
        snapshot.tenantB = tenantCredentials.get(context, "tenant-b");
        snapshot.tenantKeys = tenantCredentials.getAllInstanceKeys(context);
        snapshot.stepObservation = stepObservation.get(context);
        return RPCResult.of(snapshot);
    }

    @RPC
    public void updateHardDeleteAttributes(final Context context, final String value) {
        apiKey.set(context, value);
        tenantCredentials.set(context, "tenant-b", new TenantCredential(value + "-tenant-b"));
    }

    @RPC
    public void clearApiKeyWithNull(final Context context) {
        apiKey.set(context, null);
    }

    @RPC
    public void deleteTenantB(final Context context) {
        tenantCredentials.delete(context, "tenant-b");
    }

    @RPC
    public void requestDeletion(final Context context) {
        deletionRequests.publish(context, null);
    }

    @RPC
    public void requestFinish(final Context context) {
        finishRequests.publish(context, null);
    }

    static final class TenantCredential {
        public String token;

        TenantCredential() {
        }

        TenantCredential(final String token) {
            this.token = token;
        }
    }

    static final class HardDeleteAttributeSnapshot {
        public String apiKey;
        public TenantCredential tenantA;
        public TenantCredential tenantB;
        public List<String> tenantKeys;
        public String stepObservation;
    }

    final class HardDeleteAttributeRotateStep implements Step<String> {
        @Override
        public Class<String> getInputType() {
            return String.class;
        }

        @Override
        public StepOptions getStepOptions() {
            return StepOptions.newBuilder()
                    .addExecuteLoadAttributeMap(tenantCredentials)
                    .build();
        }

        @Override
        public StepDecision execute(final Context context, final String input) {
            final TenantCredential tenantA = tenantCredentials.get(context, "tenant-a");
            stepObservation.set(
                    context,
                    "start:" + apiKey.get(context) + "|" + (tenantA == null ? null : tenantA.token));
            apiKey.set(context, input + "-rotated-key");
            return StepDecision.goTo(HardDeleteAttributeDeleteStep.class, null);
        }
    }

    final class HardDeleteAttributeDeleteStep implements Step<Void> {
        @Override
        public Class<Void> getInputType() {
            return Void.class;
        }

        @Override
        public StepOptions getStepOptions() {
            return StepOptions.newBuilder()
                    .addExecuteLoadAttributeMap(tenantCredentials)
                    .build();
        }

        @Override
        public Wait waitFor(final Context context, final Void input) {
            return Wait.until(deletionRequests.forOne());
        }

        @Override
        public StepDecision execute(final Context context, final Void input) {
            final TenantCredential tenantB = tenantCredentials.get(context, "tenant-b");
            final String observed = "delete:" + apiKey.get(context) + "|"
                    + (tenantB == null ? null : tenantB.token);
            apiKey.delete(context);
            tenantCredentials.delete(context, "tenant-a");
            tenantCredentials.set(context, "tenant-b", null);
            stepObservation.set(context, observed + "|after:" + apiKey.get(context) + "|"
                    + tenantCredentials.getAllInstanceKeys(context));
            return StepDecision.goTo(HardDeleteAttributeFinishStep.class, null);
        }
    }

    final class HardDeleteAttributeFinishStep implements Step<Void> {
        @Override
        public Class<Void> getInputType() {
            return Void.class;
        }

        @Override
        public Wait waitFor(final Context context, final Void input) {
            return Wait.until(finishRequests.forOne());
        }

        @Override
        public StepDecision execute(final Context context, final Void input) {
            return StepDecision.gracefulComplete(stepObservation.get(context));
        }
    }
}
