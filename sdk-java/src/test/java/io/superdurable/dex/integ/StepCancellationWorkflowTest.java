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
import io.superdurable.dex.Context;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class StepCancellationWorkflowTest {
    private final StepCancellationWorkflow workflow = new StepCancellationWorkflow(
            StepCancellationWorkflow.Scenario.HEARTBEAT_WAIT_FOR);
    private final CountDownLatch heartbeatCanceled = new CountDownLatch(1);
    private final AtomicReference<Throwable> handlerFailure = new AtomicReference<Throwable>();
    private final Context context = (Context) Proxy.newProxyInstance(
            Context.class.getClassLoader(), new Class<?>[]{Context.class}, this::invokeContextMethod);

    @Test
    void waitsForInterruptAfterHeartbeatReportsContextCancellation() throws Exception {
        final Thread handler = new Thread(this::runHandler, "dex-cancellation-order-test");
        handler.start();
        try {
            assertTrue(heartbeatCanceled.await(5, TimeUnit.SECONDS));
            awaitHandlerWaitingOrReturned(handler);
            assertFalse(workflow.awaitCancellation(Duration.ZERO),
                    "Context cancellation must not finish observation before the interrupt arrives");
            assertFalse(workflow.wasHandlerInterrupted());
            handler.interrupt();
            assertTrue(workflow.awaitCancellation(Duration.ofSeconds(5)));
            assertTrue(workflow.wasHandlerInterrupted());
            assertTrue(workflow.didContextReportCancellation());
        } finally {
            handler.interrupt();
            handler.join(5_000L);
        }
        assertFalse(handler.isAlive());
        assertNull(handlerFailure.get());
    }

    private Object invokeContextMethod(final Object proxy, final Method method, final Object[] arguments) {
        if ("recordHeartbeat".equals(method.getName())) {
            heartbeatCanceled.countDown();
            throw Status.CANCELLED.asRuntimeException();
        }
        if ("isCancellationRequested".equals(method.getName())) {
            return true;
        }
        throw new UnsupportedOperationException(method.getName());
    }

    private void runHandler() {
        try {
            workflow.new StepCancellationBlockingWaitForStep().waitFor(context, null);
        } catch (Throwable failure) {
            handlerFailure.set(failure);
        }
    }

    private static void awaitHandlerWaitingOrReturned(final Thread handler) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            final Thread.State state = handler.getState();
            if (state == Thread.State.TIMED_WAITING || state == Thread.State.TERMINATED) {
                return;
            }
            Thread.yield();
        }
        throw new AssertionError("Handler did not wait for interruption or return");
    }
}
