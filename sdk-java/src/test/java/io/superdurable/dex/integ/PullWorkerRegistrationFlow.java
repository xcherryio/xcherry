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

import io.superdurable.dex.Flow;

final class PullWorkerRegistrationFlow implements Flow<Void> {
    private final String flowType;

    PullWorkerRegistrationFlow(final String flowType) {
        this.flowType = flowType;
    }

    @Override
    public String getFlowType() {
        return flowType;
    }
}
