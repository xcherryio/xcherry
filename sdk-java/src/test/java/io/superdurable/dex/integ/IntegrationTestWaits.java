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

import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.superdurable.dex.Client;
import io.superdurable.dex.StepExecutionId;
import io.superdurable.dex.TimerId;
import io.superdurable.dex.exceptions.DexServiceException;
import io.superdurable.gen.FlowServiceGrpc;
import io.superdurable.gen.ServerEngine;
import org.junit.jupiter.api.Assumptions;

import java.time.Duration;

final class IntegrationTestWaits {
    private static final ServerEngine SERVER_ENGINE = fetchServerEngine();

    private IntegrationTestWaits() {
    }

    static void skipIfServerZD(final String feature) {
        Assumptions.assumeFalse(
                SERVER_ENGINE == ServerEngine.SERVER_ENGINE_ZD,
                "server-zd does not support " + feature);
    }

    private static ServerEngine fetchServerEngine() {
        final String serverAddress = System.getProperty(
                "dex.test.serverAddress", "127.0.0.1:8801");
        final ManagedChannel channel = ManagedChannelBuilder.forTarget(serverAddress)
                .usePlaintext()
                .build();
        try {
            return FlowServiceGrpc.newBlockingStub(channel)
                    .getServerInfo(Empty.getDefaultInstance())
                    .getServerEngine();
        } finally {
            channel.shutdownNow();
        }
    }

    static void skipTimerWhenRegistered(
            final Client client,
            final String flowId,
            final StepExecutionId stepExecutionId,
            final TimerId timerId) {
        final long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        DexServiceException lastFailure = null;
        while (System.nanoTime() < deadline) {
            try {
                client.skipTimer(flowId, stepExecutionId, timerId);
                return;
            } catch (DexServiceException failure) {
                if (!failure.getDetail().contains(
                        "timer condition does not exist or is not pending")) {
                    throw failure;
                }
                lastFailure = failure;
                Thread.yield();
            }
        }
        throw new AssertionError("timer condition was not registered", lastFailure);
    }
}
