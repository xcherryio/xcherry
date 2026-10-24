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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;

/**
 * Exchanges a long-lived API key for short-lived Dex bearer tokens.
 *
 * <p>Use this credential when the application holds an API key and Dex accepts access tokens.
 * Use {@link FileCredential} when another process writes an access token to a local file. The
 * first call exchanges the key; concurrent callers share that exchange. After 80% of a token's
 * lifetime, one background exchange starts while callers continue using the valid token. A failed
 * refresh is retried no more often than every five seconds while that token remains valid. An
 * expired token makes callers wait for the shared exchange, up to the configured timeout. Dex
 * rejection discards only the rejected token, and unary Client calls retry once with a fresh one.
 *
 * <p>The token endpoint must use HTTPS, except that HTTP is allowed for localhost, 127.0.0.0/8,
 * and ::1. This class is thread-safe. The Client or Worker closes it when closed; use a separate
 * instance for each Client or Worker lifecycle. A supplied HTTP client is borrowed and is not
 * closed. Neither the API key nor an access token appears in this credential's description or
 * exchange errors.
 *
 * <pre>{@code
 * ApiKeyCredential credential = ApiKeyCredential.builder(
 *         URI.create("https://auth.example.com/token"), "dex-server-id", apiKey).build();
 * try (Client client = new Client(registry, blobCache,
 *         ClientOptions.newBuilder().serverAddress("dex.example.com:8801")
 *                 .credential(credential).build())) {
 *     client.searchFlows("", 10);
 * }
 * }</pre>
 */
public final class ApiKeyCredential implements RejectedTokenCredential, AutoCloseable {
    private static final long RETRY_NANOS = TimeUnit.SECONDS.toNanos(5);
    private static final ObjectMapper JSON = new ObjectMapper();

    private final URI tokenUrl;
    private final String serverId;
    private final String apiKey;
    private final Duration exchangeTimeout;
    private final HttpClient httpClient;
    private final LongSupplier clock;
    private Thread exchangeThread;
    private String cachedToken;
    private String lastAccessToken;
    private long refreshAtNanos;
    private long expiresAtNanos;
    private long nextRefreshAttemptNanos;
    private CompletableFuture<String> exchangeInFlight;
    private boolean isClosed;

    private ApiKeyCredential(final Builder builder) {
        this.tokenUrl = builder.tokenUrl;
        this.serverId = builder.serverId;
        this.apiKey = builder.apiKey;
        this.exchangeTimeout = builder.exchangeTimeout;
        this.httpClient = builder.httpClient == null
                ? HttpClient.newBuilder().connectTimeout(exchangeTimeout).build()
                : builder.httpClient;
        this.clock = builder.clock;
    }

    /**
     * Begins configuring an API key exchange without contacting the token endpoint.
     *
     * @param tokenUrl the absolute token endpoint URL, using HTTPS or loopback HTTP
     * @param serverId the exact Dex server ID used to scope the access token
     * @param apiKey the nonempty long-lived API key
     * @return a builder with a ten-second exchange timeout
     * @throws IllegalArgumentException if any input is absent or the URL violates the transport rule
     */
    public static Builder builder(final URI tokenUrl, final String serverId, final String apiKey) {
        validateUrl(tokenUrl);
        if (serverId == null || serverId.isEmpty() || apiKey == null || apiKey.isEmpty()) {
            throw new IllegalArgumentException("serverId and apiKey are required");
        }
        return new Builder(tokenUrl, serverId, apiKey);
    }

    /**
     * Returns a cached access token or waits for one exchange when no valid token exists.
     *
     * <p>The first call and expired-token calls wait no longer than the exchange timeout.
     * Concurrent callers share one exchange. Valid tokens return immediately while a refresh runs.
     *
     * @return a nonempty access token without the Bearer prefix
     * @throws ExchangeException if the endpoint, response, transport, or exchange deadline fails
     * @throws InterruptedException if the waiting caller is interrupted
     * @throws IllegalStateException if the credential has been closed
     */
    @Override
    public String token() throws ExchangeException, InterruptedException {
        final CompletableFuture<String> pending;
        synchronized (this) {
            if (isClosed) {
                throw new IllegalStateException("API key credential is closed");
            }
            final long now = clock.getAsLong();
            if (cachedToken != null && now < expiresAtNanos) {
                if (now >= refreshAtNanos && now >= nextRefreshAttemptNanos
                        && exchangeInFlight == null) {
                    startExchange();
                }
                return cachedToken;
            }
            if (exchangeInFlight == null) {
                startExchange();
            }
            pending = exchangeInFlight;
        }
        try {
            return pending.get(exchangeTimeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (TimeoutException timeout) {
            throw exchangeError(-1, "exchange timed out");
        } catch (ExecutionException failure) {
            final Throwable cause = failure.getCause();
            if (cause instanceof ExchangeException) {
                throw (ExchangeException) cause;
            }
            throw exchangeError(-1, "exchange failed");
        } catch (CancellationException cancelled) {
            throw exchangeError(-1, "exchange cancelled");
        }
    }

    /**
     * Discards a rejected access token only when it is still cached.
     *
     * <p>The SDK invokes this after Dex rejects an authenticated call. An older rejected token
     * leaves a newer cached token intact. This method is safe for concurrent calls.
     *
     * @param token the exact access token rejected by Dex, or null to do nothing
     */
    @Override
    public synchronized void rejectToken(final String token) {
        if (token != null && token.equals(cachedToken)) {
            cachedToken = null;
            nextRefreshAttemptNanos = 0L;
        }
    }

    /**
     * Cancels an exchange and stops its daemon thread.
     *
     * <p>The Client or Worker that receives this credential closes it with its own lifecycle.
     * Closing it earlier causes later token calls to fail. Closing is safe more than once.
     */
    @Override
    public synchronized void close() {
        if (!isClosed) {
            isClosed = true;
            if (exchangeInFlight != null) {
                exchangeInFlight.cancel(true);
            }
            if (exchangeThread != null) {
                exchangeThread.interrupt();
            }
        }
    }

    /**
     * Describes this credential without revealing either secret.
     *
     * @return a fixed redacted description
     */
    @Override
    public String toString() {
        return "ApiKeyCredential(<redacted>)";
    }

    synchronized boolean isExchangeInFlight() {
        return exchangeInFlight != null;
    }

    private synchronized void startExchange() {
        final CompletableFuture<String> pending = new CompletableFuture<String>();
        exchangeInFlight = pending;
        final Thread thread = new Thread(() -> {
            try {
                final long startedAt = clock.getAsLong();
                final ExchangeResult result = exchange();
                synchronized (ApiKeyCredential.this) {
                    if (!isClosed) {
                        cachedToken = result.token;
                        lastAccessToken = result.token;
                        final long lifetime = TimeUnit.SECONDS.toNanos(result.expiresIn);
                        refreshAtNanos = saturatingAdd(startedAt, lifetime - lifetime / 5);
                        expiresAtNanos = saturatingAdd(startedAt, lifetime);
                        nextRefreshAttemptNanos = 0L;
                    }
                    exchangeInFlight = null;
                    exchangeThread = null;
                }
                pending.complete(result.token);
            } catch (ExchangeException failure) {
                synchronized (ApiKeyCredential.this) {
                    nextRefreshAttemptNanos = saturatingAdd(clock.getAsLong(), RETRY_NANOS);
                    exchangeInFlight = null;
                    exchangeThread = null;
                }
                pending.completeExceptionally(failure);
            }
        }, "dex-api-key-exchange");
        thread.setDaemon(true);
        exchangeThread = thread;
        thread.start();
    }

    private ExchangeResult exchange() throws ExchangeException {
        final Map<String, String> payload = new LinkedHashMap<String, String>();
        payload.put("server_id", serverId);
        payload.put("api_key", apiKey);
        try {
            final HttpRequest request = HttpRequest.newBuilder(tokenUrl)
                    .timeout(exchangeTimeout)
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(payload)))
                    .build();
            final HttpResponse<String> response = httpClient.send(
                    request, HttpResponse.BodyHandlers.ofString());
            final JsonNode body;
            try {
                body = JSON.readTree(response.body());
            } catch (IOException malformed) {
                throw exchangeError(response.statusCode(), "malformed JSON response");
            }
            if (response.statusCode() != 200) {
                throw exchangeError(response.statusCode(), responseError(body));
            }
            if (body == null || !body.isObject()) {
                throw exchangeError(200, "invalid response object");
            }
            final JsonNode token = body.get("access_token");
            final JsonNode type = body.get("token_type");
            final JsonNode expiry = body.get("expires_in");
            if (token == null || !token.isTextual() || token.textValue().isEmpty()
                    || type == null || !type.isTextual()
                    || !"Bearer".equalsIgnoreCase(type.textValue())
                    || expiry == null || !expiry.isIntegralNumber()
                    || !expiry.canConvertToLong() || expiry.longValue() <= 0) {
                final String responseError = responseError(body);
                throw exchangeError(200, "invalid access_token, token_type, or expires_in"
                        + ("unexpected response".equals(responseError) ? "" : ": " + responseError));
            }
            return new ExchangeResult(token.textValue(), expiry.longValue());
        } catch (InterruptedException interruption) {
            Thread.currentThread().interrupt();
            throw exchangeError(-1, "exchange interrupted");
        } catch (IOException transport) {
            if (transport instanceof ExchangeException) {
                throw (ExchangeException) transport;
            }
            throw exchangeError(-1, "transport failure");
        } catch (RuntimeException failure) {
            throw exchangeError(-1, "exchange failed");
        }
    }

    private String responseError(final JsonNode body) {
        if (body == null || !body.isObject()) {
            return "unexpected response";
        }
        final String error = responseField(body, "error");
        final String description = responseField(body, "error_description");
        return error.isEmpty() && description.isEmpty()
                ? "unexpected response" : error + (description.isEmpty() ? "" : ": " + description);
    }

    private String responseField(final JsonNode body, final String name) {
        final JsonNode value = body.get(name);
        return value != null && value.isTextual()
                ? redact(value.textValue().substring(0, Math.min(200, value.textValue().length())))
                : "";
    }

    private ExchangeException exchangeError(final int status, final String detail) {
        return new ExchangeException(status, "API key exchange at "
                + redact(tokenUrl.toString()) + " failed (HTTP "
                + (status < 0 ? "status unavailable" : status) + "): " + redact(detail));
    }

    private String redact(final String value) {
        String safe = value.replace(apiKey, "<redacted>");
        synchronized (this) {
            if (cachedToken != null && !cachedToken.isEmpty()) {
                safe = safe.replace(cachedToken, "<redacted>");
            }
            if (lastAccessToken != null && !lastAccessToken.isEmpty()) {
                safe = safe.replace(lastAccessToken, "<redacted>");
            }
        }
        return safe;
    }

    private static long saturatingAdd(final long start, final long duration) {
        return duration > 0 && start > Long.MAX_VALUE - duration
                ? Long.MAX_VALUE : start + duration;
    }

    private static void validateUrl(final URI tokenUrl) {
        if (tokenUrl == null || !tokenUrl.isAbsolute() || tokenUrl.getHost() == null
                || tokenUrl.getRawUserInfo() != null || tokenUrl.getRawFragment() != null) {
            throw new IllegalArgumentException("tokenUrl must be an absolute HTTP URL without user info");
        }
        final String scheme = tokenUrl.getScheme();
        if ("https".equalsIgnoreCase(scheme)) {
            return;
        }
        if (!"http".equalsIgnoreCase(scheme) || !isLoopback(tokenUrl.getHost())) {
            throw new IllegalArgumentException("tokenUrl must use HTTPS except for loopback HTTP");
        }
    }

    private static boolean isLoopback(final String host) {
        final String address = host.startsWith("[") && host.endsWith("]")
                ? host.substring(1, host.length() - 1) : host;
        if ("localhost".equalsIgnoreCase(address) || "::1".equals(address)) {
            return true;
        }
        final String[] octets = address.split("\\.", -1);
        if (octets.length != 4 || !"127".equals(octets[0])) {
            return false;
        }
        for (String octet : octets) {
            if (octet.isEmpty() || octet.length() > 3) {
                return false;
            }
            for (int index = 0; index < octet.length(); index++) {
                if (!Character.isDigit(octet.charAt(index))) {
                    return false;
                }
            }
            if (Integer.parseInt(octet) > 255) {
                return false;
            }
        }
        return true;
    }

    private static final class ExchangeResult {
        private final String token;
        private final long expiresIn;

        private ExchangeResult(final String token, final long expiresIn) {
            this.token = token;
            this.expiresIn = expiresIn;
        }
    }

    /**
     * Reports a token endpoint, response, transport, or timeout failure.
     *
     * <p>The message identifies the token URL and HTTP status when available. It includes at most
     * 200 characters from each response error field and never includes a raw response, API key, or
     * access token. A status of -1 means the exchange did not receive an HTTP response.
     */
    public static final class ExchangeException extends IOException {
        private final int httpStatus;

        private ExchangeException(final int httpStatus, final String message) {
            super(message);
            this.httpStatus = httpStatus;
        }

        /**
         * Returns the token endpoint's HTTP status.
         *
         * @return the status, or -1 when no response arrived
         */
        public int getHttpStatus() {
            return httpStatus;
        }
    }

    /**
     * Configures the timeout and HTTP transport for an API key credential.
     *
     * <p>Obtain a builder from {@link ApiKeyCredential#builder(URI, String, String)}. The enclosing type shows a
     * complete Client example. Build does not contact the token endpoint. The default timeout is
     * ten seconds, and a supplied HTTP client remains the caller's property.
     */
    public static final class Builder {
        private final URI tokenUrl;
        private final String serverId;
        private final String apiKey;
        private Duration exchangeTimeout = Duration.ofSeconds(10);
        private HttpClient httpClient;
        private LongSupplier clock = System::nanoTime;

        private Builder(final URI tokenUrl, final String serverId, final String apiKey) {
            this.tokenUrl = tokenUrl;
            this.serverId = serverId;
            this.apiKey = apiKey;
        }

        /**
         * Sets the maximum duration for one exchange, including connection and response.
         *
         * @param value a positive duration; the default is ten seconds
         * @return this builder
         * @throws IllegalArgumentException if the duration is null, zero, negative, or too large
         */
        public Builder exchangeTimeout(final Duration value) {
            if (value == null || value.isZero() || value.isNegative()) {
                throw new IllegalArgumentException("exchangeTimeout must be positive");
            }
            try {
                value.toNanos();
            } catch (ArithmeticException overflow) {
                throw new IllegalArgumentException("exchangeTimeout is too large");
            }
            this.exchangeTimeout = value;
            return this;
        }

        /**
         * Uses a borrowed HTTP client for token exchanges and proxy configuration.
         *
         * @param value the nonnull HTTP client; the caller retains ownership
         * @return this builder
         * @throws IllegalArgumentException if the client is null
         */
        public Builder httpClient(final HttpClient value) {
            if (value == null) {
                throw new IllegalArgumentException("httpClient is required");
            }
            this.httpClient = value;
            return this;
        }

        Builder clock(final LongSupplier value) {
            this.clock = value;
            return this;
        }

        /**
         * Builds the thread-safe credential without contacting the token endpoint.
         *
         * @return a credential owned and closed by its Client or Worker
         */
        public ApiKeyCredential build() {
            return new ApiKeyCredential(this);
        }
    }
}
