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

/**
 * Supplies a bearer token before each Client or Worker service call.
 *
 * <p>Implementations must be safe for concurrent calls. A blocked lookup can
 * occupy a gRPC executor thread after a deadline or cancellation ends the call. {@link
 * ApiKeyCredential} bounds its first and expired-token exchanges by a timeout. A failure prevents
 * the request from reaching Dex and becomes a Client {@link
 * io.superdurable.dex.exceptions.DexServiceException} with gRPC {@code UNAUTHENTICATED}, or an
 * {@link IllegalStateException} during Worker startup. Pull Worker streams obtain a token when each stream opens, so
 * reconnects pick up rotated tokens. Tokens travel over the same plaintext gRPC connection as
 * other requests; protect the network path to Dex.
 *
 * <pre>{@code
 * Credential credential = () -> currentToken.get();
 * ClientOptions options = ClientOptions.newBuilder().credential(credential).build();
 * }</pre>
 */
@FunctionalInterface
public interface Credential {
    /**
     * Returns the current printable ASCII token without the {@code Bearer } prefix.
     *
     * <p>This method can run concurrently for different calls. Throw when no usable token is
     * available; Dex is not contacted for that call. Spaces, control characters, and non-ASCII
     * characters fail the call as {@code UNAUTHENTICATED}. Avoid including the token in exception
     * messages, which may be shown to the caller.
     *
     * @return the nonempty bearer token
     * @throws Exception if the token cannot be supplied
     */
    String token() throws Exception;
}
