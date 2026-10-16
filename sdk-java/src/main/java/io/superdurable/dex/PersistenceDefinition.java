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
 * Represents a typed Attribute, Channel, or Stream definition in a {@link PersistenceSchema}.
 *
 * <p>This is the common, type-erased base accepted by schema factory methods. Applications create
 * concrete definitions with {@link Attribute#define}, {@link AttributeMap#define},
 * {@link Channel#define}, {@link ChannelMap#define}, or {@link Stream#define}; custom subclasses
 * are intentionally not supported.
 */
public abstract class PersistenceDefinition {
    PersistenceDefinition() {
    }

    abstract String getName();

    abstract Class<?> getValueType();

    boolean isSyncToAttributeStore() {
        return false;
    }

    boolean isHardDeleteOnNull() {
        return false;
    }
}
