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

import io.grpc.Status;
import io.superdurable.dex.BlobCache;
import io.superdurable.dex.BlobCacheConfig;
import io.superdurable.dex.Flow;
import io.superdurable.dex.PullRegistrationError;
import io.superdurable.dex.PullWorkerOptions;
import io.superdurable.dex.Registry;
import io.superdurable.dex.Worker;
import io.superdurable.dex.WorkerOptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("dex-dev")
final class PullWorkerRegistrationTest {
    @TempDir
    Path cacheDirectory;

    @Test
    void registersEveryQueueAndPreservesReadinessAfterStop() throws Exception {
        verifyRegistration(false);
    }

    @Test
    void invalidArgumentRegistrationWakesWaitersAndStopsStart() throws Exception {
        verifyRegistration(true);
    }

    private void verifyRegistration(final boolean shouldReject) throws Exception {
        final String flowType = shouldReject ? String.join("", Collections.nCopies(257, "x"))
                : "JavaRegistrationFlow-" + UUID.randomUUID();
        final Registry registry = new Registry(Collections.<Flow<?>>singletonList(
                new PullWorkerRegistrationFlow(flowType)));
        try (BlobCache cache = BlobCache.open(new BlobCacheConfig(cacheDirectory.toString(), 64L << 20));
                Worker worker = new Worker(registry, cache,
                        WorkerOptions.newBuilder()
                                .serverAddress(System.getProperty("dex.test.serverAddress", "127.0.0.1:8801"))
                                .pull(PullWorkerOptions.newBuilder()
                                        .appWorkerID("java-registration-" + UUID.randomUUID())
                                        .stopDrainTimeout(Duration.ofSeconds(1))
                                        .stopOnPermanentStreamRejection(true).build()).build())) {
            final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
            final Thread starting = new Thread(() -> {
                try {
                    worker.start();
                } catch (Throwable rejected) {
                    failure.set(rejected);
                }
            });
            starting.start();
            try {
                if (shouldReject) {
                    final PullRegistrationError rejection = assertThrows(PullRegistrationError.class,
                            () -> worker.waitPullRegistered(Duration.ofSeconds(10)));
                    assertEquals(Status.Code.INVALID_ARGUMENT, Status.fromThrowable(rejection.getCause()).getCode());
                    starting.join(5000L);
                    assertFalse(starting.isAlive());
                    assertSame(rejection, failure.get());
                    assertFalse(worker.getPullRegistrationState().isReady());
                } else {
                    worker.waitPullRegistered(Duration.ofSeconds(10));
                    assertTrue(worker.getPullRegistrationState().isReady());
                    assertEquals(2, worker.getPullRegistrationState().getQueues().size());
                    assertTrue(worker.getPullRegistrationState().getQueues().stream()
                            .allMatch(queue -> queue.hasRegisteredStream()));
                }
            } finally {
                worker.stop();
                starting.join(5000L);
                assertFalse(starting.isAlive());
            }
            if (!shouldReject) {
                assertEquals(null, failure.get());
                worker.waitPullRegistered(Duration.ZERO);
                assertTrue(worker.getPullRegistrationState().isReady());
            }
        }
    }
}
