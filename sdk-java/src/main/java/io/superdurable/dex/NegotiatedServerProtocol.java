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

import io.superdurable.gen.AttributeWrite;

import java.util.List;

final class NegotiatedServerProtocol {
    private static final int NOT_NEGOTIATED = 0;

    private volatile int negotiatedVersion = NOT_NEGOTIATED;

    void recordNegotiatedVersion(final int version) {
        if (version == NOT_NEGOTIATED) {
            throw new IllegalArgumentException("negotiated protocol version must be positive");
        }
        negotiatedVersion = version;
    }

    boolean isNegotiated() {
        return negotiatedVersion != NOT_NEGOTIATED;
    }

    int getNegotiatedVersion() {
        return negotiatedVersion;
    }

    void requireHardDeleteAttributeSupport(final List<AttributeWrite> writes) {
        for (AttributeWrite write : writes) {
            if (write.getIsHardDeleteOnNull()) {
                requireHardDeleteAttributeSupport(write.getKey());
            }
        }
    }

    void requireHardDeleteAttributeSupport(final String attributeKey) {
        final int version = negotiatedVersion;
        if (version >= ServerProtocolCompatibility.HARD_DELETE_ATTRIBUTE_PROTOCOL_VERSION) {
            return;
        }
        final String negotiated = version == NOT_NEGOTIATED
                ? "has not been negotiated"
                : "is " + version;
        throw new IllegalStateException(
                "hard-delete Attribute " + attributeKey + " requires Server protocol version "
                        + ServerProtocolCompatibility.HARD_DELETE_ATTRIBUTE_PROTOCOL_VERSION
                        + " or later, but the negotiated Server protocol version " + negotiated
                        + "; upgrade the Dex Server before writing this Attribute");
    }
}
