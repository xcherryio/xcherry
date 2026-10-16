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

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

final class GrpcCredential extends CallCredentials {
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

    private static void validateToken(final String token) {
        for (int index = 0; index < token.length(); index++) {
            final char character = token.charAt(index);
            if (character <= 0x20 || character >= 0x7f) {
                throw new IllegalArgumentException("credential token contains invalid characters");
            }
        }
    }
}
