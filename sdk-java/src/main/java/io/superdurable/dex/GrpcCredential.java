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

import io.grpc.CallCredentials;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

final class GrpcCredential extends CallCredentials {
    private static final Context.Key<AtomicReference<String>> CALL_TOKEN =
            Context.key("dex-call-token");
    private static final Metadata.Key<String> AUTHORIZATION =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    private final Credential credential;

    GrpcCredential(final Credential credential) {
        this.credential = credential;
    }

    @Override
    public void applyRequestMetadata(
            final RequestInfo requestInfo,
            final Executor executor,
            final MetadataApplier applier) {
        final Context context = Context.current();
        try {
            executor.execute(() -> {
                if (context.isCancelled()) {
                    applier.fail(Status.CANCELLED);
                    return;
                }
                final Metadata headers;
                try {
                    final String token = credential.token();
                    if (token == null || token.isEmpty()) {
                        throw new IllegalStateException("credential token is empty");
                    }
                    validateToken(token);
                    final AtomicReference<String> callToken = CALL_TOKEN.get(context);
                    if (callToken != null && credential instanceof RejectedTokenCredential) {
                        callToken.set(token);
                    }
                    if (context.isCancelled()) {
                        applier.fail(Status.CANCELLED);
                        return;
                    }
                    headers = new Metadata();
                    headers.put(AUTHORIZATION, "Bearer " + token);
                } catch (InterruptedException interruption) {
                    Thread.currentThread().interrupt();
                    applier.fail(Status.UNAUTHENTICATED
                            .withDescription(interruption.getMessage()).withCause(interruption));
                    return;
                } catch (Exception failure) {
                    applier.fail(Status.UNAUTHENTICATED
                            .withDescription(failure.getMessage()).withCause(failure));
                    return;
                }
                applier.apply(headers);
            });
        } catch (RejectedExecutionException rejection) {
            applier.fail(Status.UNAVAILABLE
                    .withDescription(rejection.getMessage()).withCause(rejection));
        } catch (RuntimeException failure) {
            applier.fail(Status.UNAVAILABLE
                    .withDescription(failure.getMessage()).withCause(failure));
        }
    }

    static Context withTokenCapture(final AtomicReference<String> token) {
        return Context.current().withValue(CALL_TOKEN, token);
    }

    static void rejectToken(final Credential credential, final String token) {
        if (credential instanceof RejectedTokenCredential && token != null) {
            ((RejectedTokenCredential) credential).rejectToken(token);
        }
    }

    static <T> T callUnaryWithRetry(final Credential credential, final Supplier<T> rpc) {
        final boolean canRetry = credential instanceof RejectedTokenCredential;
        for (int attempt = 0; attempt < (canRetry ? 2 : 1); attempt++) {
            final AtomicReference<String> callToken = new AtomicReference<String>();
            final AtomicReference<T> result = new AtomicReference<T>();
            try {
                withTokenCapture(callToken).run(() -> result.set(rpc.get()));
                return result.get();
            } catch (StatusRuntimeException failure) {
                if (canRetry && failure.getStatus().getCode() == Status.Code.UNAUTHENTICATED) {
                    rejectToken(credential, callToken.get());
                    if (attempt == 0 && callToken.get() != null) {
                        continue;
                    }
                }
                throw failure;
            }
        }
        throw new IllegalStateException("unreachable credential retry state");
    }

    private static void validateToken(final String token) {
        for (int index = 0; index < token.length(); index++) {
            final char character = token.charAt(index);
            if (character <= 0x20 || character >= 0x7f) {
                throw new IllegalArgumentException("credential token contains invalid characters");
            }
        }
    }
}
