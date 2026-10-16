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

import io.superdurable.gen.ServerInfo;

final class ServerProtocolCompatibility {
    static final int MINIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION = 1;
    static final int MAXIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION = 3;
    static final int HARD_DELETE_ATTRIBUTE_PROTOCOL_VERSION = 3;

    private ServerProtocolCompatibility() {
    }

    static String sdkVersion() {
        final String implementationVersion = Worker.class.getPackage().getImplementationVersion();
        if (implementationVersion == null || implementationVersion.isEmpty()) {
            return "dev";
        }
        return implementationVersion;
    }

    static int negotiate(final ServerInfo serverInfo, final String sdkVersion) {
        if (serverInfo == null) {
            throw incompatible(sdkVersion, "unknown", 0, 0, "Server returned an empty response");
        }
        final int serverMinimum = serverInfo.getMinimumSupportedProtocolVersion();
        final int serverCurrent = serverInfo.getCurrentProtocolVersion();
        if (MINIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION == 0
                || MAXIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION == 0
                || MINIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION
                        > MAXIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION) {
            throw incompatible(
                    sdkVersion,
                    serverInfo.getServerVersion(),
                    serverMinimum,
                    serverCurrent,
                    "Java SDK protocol interval is invalid");
        }
        if (serverMinimum == 0 || serverCurrent == 0 || serverMinimum > serverCurrent) {
            throw incompatible(
                    sdkVersion,
                    serverInfo.getServerVersion(),
                    serverMinimum,
                    serverCurrent,
                    "Server protocol interval is invalid");
        }
        final int negotiated = Math.min(
                serverCurrent,
                MAXIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION);
        if (negotiated < serverMinimum
                || negotiated < MINIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION) {
            throw incompatible(
                    sdkVersion,
                    serverInfo.getServerVersion(),
                    serverMinimum,
                    serverCurrent,
                    "protocol intervals do not overlap");
        }
        return negotiated;
    }

    static IllegalStateException requestFailure(
            final String sdkVersion,
            final RuntimeException cause) {
        return new IllegalStateException(
                "Java SDK version \"" + sdkVersion + "\" protocol ["
                        + MINIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION + ","
                        + MAXIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION + "] is incompatible with "
                        + "Server version \"unknown\" protocol [unknown,unknown]: "
                        + "GetServerInfo failed",
                cause);
    }

    private static IllegalStateException incompatible(
            final String sdkVersion,
            final String serverVersion,
            final int serverMinimum,
            final int serverCurrent,
            final String reason) {
        final String displayedServerVersion = serverVersion.isEmpty() ? "unknown" : serverVersion;
        return new IllegalStateException(
                "Java SDK version \"" + sdkVersion + "\" protocol ["
                        + MINIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION + ","
                        + MAXIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION + "] is incompatible with "
                        + "Server version \"" + displayedServerVersion + "\" protocol ["
                        + serverMinimum + "," + serverCurrent + "]: " + reason);
    }
}
