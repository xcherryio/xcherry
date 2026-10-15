/*
 * Copyright (c) 2026 Super Durable, Inc.
 *
 * Licensed under the Sustainable Use License 1.0.
 * You may not use this file except in compliance with the License.
 * See the LICENSE file in the repository root.
 *
 * SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0
 */

package io.superdurable.dex;

import com.google.protobuf.Any;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import io.superdurable.dex.exceptions.RetryAfterException;
import io.superdurable.gen.InvokeExecuteMethodOutput;
import io.superdurable.gen.InvokeExecuteMethodRequest;
import io.superdurable.gen.InvokeExecuteMethodResponse;
import io.superdurable.gen.InvokeWaitForMethodOutput;
import io.superdurable.gen.InvokeWaitForMethodRequest;
import io.superdurable.gen.InvokeWaitForMethodResponse;
import io.superdurable.gen.InvokeWorkerRPCRequest;
import io.superdurable.gen.InvokeWorkerRPCResponse;
import io.superdurable.gen.StepMethodHeartbeat;
import io.superdurable.gen.StepStreamWrite;
import io.superdurable.gen.WorkerErrorResponse;
import io.superdurable.gen.WorkerServiceGrpc;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

final class JavaWorkerService extends WorkerServiceGrpc.WorkerServiceImplBase {
    private static final Logger LOGGER = Logger.getLogger(JavaWorkerService.class.getName());
    static final int MAX_WORKER_ERROR_DETAIL_BYTES = 1024;
    static final int MAX_WORKER_ERROR_TYPE_BYTES = 256;
    static final int MAX_WORKER_STACK_TRACE_BYTES = 4 * 1024;
    static final byte[] ERROR_DETAIL_TRUNCATION_MARKER =
            "\n... error detail truncated by Dex Java SDK ..."
                    .getBytes(StandardCharsets.UTF_8);
    private static final byte[] ERROR_TYPE_TRUNCATION_MARKER =
            "\n... error type truncated by Dex Java SDK ..."
                    .getBytes(StandardCharsets.UTF_8);
    private static final byte[] STACK_TRACE_TRUNCATION_MARKER =
            "\n... stack trace truncated by Dex Java SDK ..."
                    .getBytes(StandardCharsets.UTF_8);
    private static final Executor DIRECT_EXECUTOR = Runnable::run;

    private final WorkerDispatcher dispatcher;
    private final ExecutorService handlers;
    private final GrpcErrorStatusMapping grpcErrorStatusMapping;

    JavaWorkerService(
            final WorkerDispatcher dispatcher,
            final ExecutorService handlers,
            final GrpcErrorStatusMapping grpcErrorStatusMapping) {
        this.dispatcher = dispatcher;
        this.handlers = handlers;
        this.grpcErrorStatusMapping = grpcErrorStatusMapping;
    }

    @Override
    public void invokeWaitForMethod(
            final InvokeWaitForMethodRequest request,
            final StreamObserver<InvokeWaitForMethodOutput> observer) {
        final WaitForOutputEmitter emitter = new WaitForOutputEmitter(observer, Context.current());
        submit(observer, () -> emitter.emitResult(dispatcher.invokeWaitFor(request, emitter)));
    }

    @Override
    public void invokeExecuteMethod(
            final InvokeExecuteMethodRequest request,
            final StreamObserver<InvokeExecuteMethodOutput> observer) {
        final ExecuteOutputEmitter emitter = new ExecuteOutputEmitter(observer, Context.current());
        submit(observer, () -> emitter.emitResult(dispatcher.invokeExecute(request, emitter)));
    }

    @Override
    public void invokeWorkerRPC(
            final InvokeWorkerRPCRequest request,
            final StreamObserver<InvokeWorkerRPCResponse> observer) {
        submit(observer, () -> emit(observer, dispatcher.invokeRpc(request)));
    }

    private void submit(
            final StreamObserver<?> observer,
            final Invocation invocation) {
        final Context requestContext = Context.current();
        final HandlerCancellation cancellation = new HandlerCancellation(requestContext);
        final Runnable task = requestContext.wrap(() -> {
            try {
                invoke(observer, invocation);
            } finally {
                cancellation.complete();
            }
        });
        try {
            cancellation.attach(handlers.submit(task));
        } catch (RejectedExecutionException rejected) {
            cancellation.complete();
            observer.onError(Status.RESOURCE_EXHAUSTED
                    .withDescription("Java Worker handler capacity is exhausted")
                    .withCause(rejected)
                    .asRuntimeException());
        }
    }

    private void invoke(
            final StreamObserver<?> observer,
            final Invocation invocation) {
        try {
            invocation.invoke();
            observer.onCompleted();
        } catch (Throwable failure) {
            if (Context.current().isCancelled()) {
                observer.onError(Status.CANCELLED
                        .withDescription("Java Worker invocation canceled")
                        .withCause(failure)
                        .asRuntimeException());
                return;
            }
            LOGGER.log(Level.SEVERE, "Java Worker invocation failed", failure);
            observer.onError(mapFailure(failure));
        }
    }

    private Throwable mapFailure(final Throwable failure) {
        final RetryAfterException retryAfter = failure instanceof RetryAfterException
                ? (RetryAfterException) failure
                : null;
        final Throwable reportedFailure = retryAfter == null ? failure : retryAfter.getCause();
        final String message = truncateUtf8(reportedFailure.getMessage() == null
                ? reportedFailure.toString()
                : reportedFailure.getMessage(),
                MAX_WORKER_ERROR_DETAIL_BYTES,
                ERROR_DETAIL_TRUNCATION_MARKER);
        final WorkerErrorResponse.Builder details = WorkerErrorResponse.newBuilder()
                .setDetail(message)
                .setErrorType(truncateUtf8(
                        reportedFailure.getClass().getName(),
                        MAX_WORKER_ERROR_TYPE_BYTES,
                        ERROR_TYPE_TRUNCATION_MARKER))
                .setStackTrace(stackTrace(reportedFailure))
                .setGrpcStatus(grpcErrorStatusMapping.statusFor(reportedFailure).value());
        if (retryAfter != null) {
            details.setRetryAfterSeconds((int) retryAfter.getRetryAfter().getSeconds());
        }
        final com.google.rpc.Status status = com.google.rpc.Status.newBuilder()
                .setCode(grpcErrorStatusMapping.statusFor(reportedFailure).value())
                .setMessage(message)
                .addDetails(Any.pack(details.build()))
                .build();
        return StatusProto.toStatusRuntimeException(status);
    }

    private static String stackTrace(final Throwable failure) {
        final StringWriter buffer = new StringWriter();
        final PrintWriter writer = new PrintWriter(buffer);
        failure.printStackTrace(writer);
        writer.flush();
        return truncateUtf8(
                buffer.toString(),
                MAX_WORKER_STACK_TRACE_BYTES,
                STACK_TRACE_TRUNCATION_MARKER);
    }

    private static String truncateUtf8(
            final String value,
            final int maximumBytes,
            final byte[] truncationMarker) {
        final byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        if (encoded.length <= maximumBytes) {
            return value;
        }
        int prefixLength = maximumBytes - truncationMarker.length;
        while (prefixLength > 0 && (encoded[prefixLength] & 0xc0) == 0x80) {
            prefixLength--;
        }
        return new String(encoded, 0, prefixLength, StandardCharsets.UTF_8)
                + new String(truncationMarker, StandardCharsets.UTF_8);
    }

    private static <Response> void emit(
            final StreamObserver<Response> observer,
            final Response response) {
        try {
            requireActiveInvocation();
            observer.onNext(response);
        } catch (RuntimeException failure) {
            if (Context.current().isCancelled()) {
                Thread.currentThread().interrupt();
            }
            throw failure;
        }
    }

    private static <Response> void emit(
            final StreamObserver<Response> observer,
            final Response response,
            final Context requestContext) {
        final Context previous = requestContext.attach();
        try {
            emit(observer, response);
        } finally {
            requestContext.detach(previous);
        }
    }

    private static void requireActiveInvocation() {
        if (Context.current().isCancelled()) {
            Thread.currentThread().interrupt();
            throw Status.CANCELLED
                    .withDescription("Java Worker invocation canceled")
                    .asRuntimeException();
        }
    }

    private interface Invocation {
        void invoke() throws Throwable;
    }

    private static final class WaitForOutputEmitter implements StepOutputEmitter {
        private final StreamObserver<InvokeWaitForMethodOutput> observer;
        private final Context requestContext;

        private WaitForOutputEmitter(
                final StreamObserver<InvokeWaitForMethodOutput> observer,
                final Context requestContext) {
            this.observer = observer;
            this.requestContext = requestContext;
        }

        @Override
        public synchronized void emitHeartbeat(final StepMethodHeartbeat heartbeat) {
            emit(observer, InvokeWaitForMethodOutput.newBuilder()
                    .setHeartbeat(heartbeat)
                    .build(), requestContext);
        }

        @Override
        public synchronized void emitStreamWrite(final StepStreamWrite streamWrite) {
            emit(observer, InvokeWaitForMethodOutput.newBuilder()
                    .setStreamWrite(streamWrite)
                    .build(), requestContext);
        }

        private synchronized void emitResult(final InvokeWaitForMethodResponse result) {
            emit(observer, InvokeWaitForMethodOutput.newBuilder().setResult(result).build(),
                    requestContext);
        }
    }

    private static final class ExecuteOutputEmitter implements StepOutputEmitter {
        private final StreamObserver<InvokeExecuteMethodOutput> observer;
        private final Context requestContext;

        private ExecuteOutputEmitter(
                final StreamObserver<InvokeExecuteMethodOutput> observer,
                final Context requestContext) {
            this.observer = observer;
            this.requestContext = requestContext;
        }

        @Override
        public synchronized void emitHeartbeat(final StepMethodHeartbeat heartbeat) {
            emit(observer, InvokeExecuteMethodOutput.newBuilder()
                    .setHeartbeat(heartbeat)
                    .build(), requestContext);
        }

        @Override
        public synchronized void emitStreamWrite(final StepStreamWrite streamWrite) {
            emit(observer, InvokeExecuteMethodOutput.newBuilder()
                    .setStreamWrite(streamWrite)
                    .build(), requestContext);
        }

        private synchronized void emitResult(final InvokeExecuteMethodResponse result) {
            emit(observer, InvokeExecuteMethodOutput.newBuilder().setResult(result).build(),
                    requestContext);
        }
    }

    private static final class HandlerCancellation implements Context.CancellationListener {
        private final Context context;
        private Future<?> future;
        private boolean isCompleted;

        private HandlerCancellation(final Context context) {
            this.context = context;
            context.addListener(this, DIRECT_EXECUTOR);
        }

        synchronized void attach(final Future<?> value) {
            if (isCompleted) {
                value.cancel(true);
                return;
            }
            future = value;
            if (context.isCancelled()) {
                value.cancel(true);
            }
        }

        void complete() {
            synchronized (this) {
                isCompleted = true;
                future = null;
            }
            context.removeListener(this);
        }

        @Override
        public void cancelled(final Context ignored) {
            final Future<?> running;
            synchronized (this) {
                isCompleted = true;
                running = future;
                future = null;
            }
            context.removeListener(this);
            if (running != null) {
                running.cancel(true);
            }
        }
    }
}
