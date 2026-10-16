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
                    if (context.isCancelled()) {
                        applier.fail(Status.CANCELLED);
                        return;
                    }
                    headers = new Metadata();
                    headers.put(AUTHORIZATION, "Bearer " + token);
                } catch (Exception failure) {
                    applier.fail(Status.UNAUTHENTICATED.withDescription("Dex credential unavailable"));
                    return;
                }
                applier.apply(headers);
            });
        } catch (RuntimeException failure) {
            applier.fail(Status.UNAUTHENTICATED.withDescription("Dex credential unavailable"));
        }
    }
}
