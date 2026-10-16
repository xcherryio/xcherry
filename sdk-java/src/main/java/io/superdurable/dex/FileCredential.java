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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.function.LongSupplier;

/**
 * Reads a bearer token from a file and picks up rotations without a restart.
 *
 * <p>Construction reads the file immediately. Calls check its modification time and size at most
 * once per second and reread changed content. Surrounding whitespace is removed. An empty or
 * unreadable file fails calls until a later check succeeds; no stale token is sent. Concurrent
 * callers share a short lock. Prefer atomic replacement when rotating the file.
 *
 * <pre>{@code
 * Credential credential = new FileCredential("/var/run/secrets/dex/token");
 * ClientOptions clientOptions = ClientOptions.newBuilder().credential(credential).build();
 * WorkerOptions workerOptions = WorkerOptions.newBuilder().credential(credential).build();
 * }</pre>
 */
public final class FileCredential implements Credential {
    private static final long CHECK_INTERVAL_NANOS = 1_000_000_000L;
    private final Path path;
    private final LongSupplier clock;
    private String currentToken;
    private FileTime modifiedAt;
    private long size;
    private long checkedAt;
    private IOException loadError;

    /**
     * Reads the initial token from a file path.
     *
     * @param path the token file path
     * @throws IllegalArgumentException if path is null or invalid
     * @throws IOException if the file cannot be read or contains only whitespace
     */
    public FileCredential(final String path) throws IOException {
        this(path == null ? null : Paths.get(path));
    }

    /**
     * Reads the initial token from a file.
     *
     * @param path the nonnull token file path
     * @throws IllegalArgumentException if path is null
     * @throws IOException if the file cannot be read or contains only whitespace
     */
    public FileCredential(final Path path) throws IOException {
        this(path, System::nanoTime);
    }

    FileCredential(final Path path, final LongSupplier clock) throws IOException {
        if (path == null || clock == null) {
            throw new IllegalArgumentException("path and clock are required");
        }
        this.path = path;
        this.clock = clock;
        refresh();
        if (loadError != null) {
            throw loadError;
        }
    }

    /**
     * Returns the cached token after any due file check.
     *
     * <p>A changed file is reloaded on the next check, at most one second after the previous
     * check. This method is safe for concurrent callers.
     *
     * @return the current token without surrounding whitespace
     * @throws IOException if the file is unreadable or empty after trimming
     */
    @Override
    public synchronized String token() throws IOException {
        if (clock.getAsLong() - checkedAt >= CHECK_INTERVAL_NANOS) {
            refresh();
        }
        if (loadError != null) {
            throw loadError;
        }
        return currentToken;
    }

    /**
     * Describes this credential without revealing its token.
     *
     * @return a fixed redacted description
     */
    @Override
    public String toString() {
        return "FileCredential(<redacted>)";
    }

    private void refresh() {
        checkedAt = clock.getAsLong();
        try {
            final BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class);
            if (loadError == null && currentToken != null
                    && attributes.lastModifiedTime().equals(modifiedAt) && attributes.size() == size) {
                return;
            }
            final String token = new String(Files.readAllBytes(path), StandardCharsets.UTF_8).trim();
            if (token.isEmpty()) {
                throw new IOException("dex: credential file " + path + " is empty");
            }
            currentToken = token;
            modifiedAt = attributes.lastModifiedTime();
            size = attributes.size();
            loadError = null;
        } catch (IOException failure) {
            currentToken = null;
            loadError = failure;
        }
    }
}
