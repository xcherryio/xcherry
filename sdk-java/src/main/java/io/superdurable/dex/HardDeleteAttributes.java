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
import io.superdurable.gen.Value;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

final class HardDeleteAttributes {
    private static final Set<Class<?>> UNSUPPORTED_VALUE_TYPES =
            Collections.unmodifiableSet(new HashSet<Class<?>>(Arrays.<Class<?>>asList(
                    Boolean.class, boolean.class,
                    Byte.class, byte.class,
                    Short.class, short.class,
                    Integer.class, int.class,
                    Long.class, long.class,
                    Float.class, float.class,
                    Double.class, double.class)));

    private HardDeleteAttributes() {
    }

    static void requireDefinitionSupport(
            final String name,
            final Class<?> valueType,
            final AttributeIndex index,
            final boolean isSyncToAttributeStore) {
        if (index != null) {
            throw new IllegalStateException(
                    "hard-delete Attribute " + name + " cannot have an index");
        }
        if (isSyncToAttributeStore) {
            throw new IllegalStateException(
                    "hard-delete Attribute " + name + " cannot use syncToAttributeStore");
        }
        if (UNSUPPORTED_VALUE_TYPES.contains(valueType)) {
            throw new IllegalStateException(
                    "hard-delete Attribute " + name + " supports only string and object values, not "
                            + valueType.getName());
        }
    }

    static void applyHardDeleteOnNull(
            final AttributeWrite.Builder write,
            final PersistenceDefinition definition) {
        if (!definition.isHardDeleteOnNull()) {
            return;
        }
        final Value.KindCase kind = write.getValue().getKindCase();
        if (kind == Value.KindCase.INT_VALUE
                || kind == Value.KindCase.DOUBLE_VALUE
                || kind == Value.KindCase.BOOL_VALUE) {
            throw new IllegalArgumentException(
                    "hard-delete Attribute " + write.getKey()
                            + " supports only string and object values, not a numeric or Boolean value");
        }
        write.setIsHardDeleteOnNull(true);
    }
}
