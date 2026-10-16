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

import java.util.Objects;

/**
 * Defines one durable, typed value stored with a Flow execution.
 *
 * <p>Declare Attributes once on the Flow, include them in its {@link PersistenceSchema}, and use
 * the same object from Step and RPC code. Values are serialized using the {@link Class} supplied at
 * definition time. A parameterized value type such as {@code List<String>} cannot be supplied
 * directly because {@code List.class} does not retain its element type. Wrap the value in a
 * concrete class whose field is a {@code List<String>}, or use {@code String[]} when an array fits
 * the data model.
 *
 * <pre>{@code
 * private final Attribute<Integer> counter =
 *         Attribute.define("counter", Integer.class);
 * private final Attribute<Tags> tags =
 *         Attribute.define("tags", Tags.class);
 * private final Attribute<String[]> tagArray =
 *         Attribute.define("tag-array", String[].class);
 *
 * static final class Tags {
 *     public List<String> values;
 * }
 *
 * public StepDecision execute(Context context, Command input) {
 *     counter.set(context, counter.get(context) + 1);
 *     return StepDecision.gracefulComplete();
 * }
 * }</pre>
 *
 * @param <T> the Java value type stored in the Attribute
 */
public final class Attribute<T> extends PersistenceDefinition {
    private final String name;
    private final Class<T> valueType;
    private final AttributeIndex index;
    private final boolean syncToAttributeStore;
    private final boolean hardDeleteOnNull;

    private Attribute(
            final String name,
            final Class<T> valueType,
            final AttributeIndex index,
            final boolean syncToAttributeStore,
            final boolean hardDeleteOnNull) {
        this.name = requirePersistenceDefinitionName(name);
        this.valueType = Objects.requireNonNull(valueType, "valueType");
        this.index = index;
        this.syncToAttributeStore = syncToAttributeStore;
        this.hardDeleteOnNull = hardDeleteOnNull;
    }

    /**
     * Defines an Attribute without a search index.
     *
     * @param name the stable Attribute name; must not be blank or contain {@code /}
     * @param valueType the concrete Java class used to serialize and deserialize values
     * @param <T> the Java value type
     * @return the typed Attribute definition
     * @throws IllegalArgumentException if {@code name} is {@code null}, blank, or contains {@code /}
     * @throws NullPointerException if {@code valueType} is {@code null}
     */
    public static <T> Attribute<T> define(final String name, final Class<T> valueType) {
        return new Attribute<T>(name, valueType, null, false, false);
    }

    /**
     * Defines an Attribute with a search index.
     *
     * @param name the stable Attribute name; must not be blank or contain {@code /}
     * @param valueType the concrete Java class used to serialize and deserialize values
     * @param index the search-index definition, or {@code null} for no index
     * @param <T> the Java value type
     * @return the typed Attribute definition
     * @throws IllegalArgumentException if {@code name} is {@code null}, blank, or contains {@code /}
     * @throws NullPointerException if {@code valueType} is {@code null}
     */
    public static <T> Attribute<T> define(
            final String name,
            final Class<T> valueType,
            final AttributeIndex index) {
        return new Attribute<T>(name, valueType, index, false, false);
    }

    /**
     * Returns a definition whose writes are projected through the Flow's Attribute Store.
     *
     * <p>The projection is asynchronous and latest-state only. A projection failure does not roll
     * back the Flow Attribute. Deleting this Attribute projects SQL {@code NULL} when the target
     * column permits it.
     *
     * @return a new immutable Attribute definition with Attribute Store synchronization enabled
     * @throws IllegalStateException if this definition uses {@link #hardDeleteOnNull()}
     */
    public Attribute<T> syncToAttributeStore() {
        if (hardDeleteOnNull) {
            throw new IllegalStateException(
                    "hard-delete Attribute " + name + " cannot use syncToAttributeStore");
        }
        return new Attribute<T>(name, valueType, index, true, false);
    }

    /**
     * Returns a definition whose value is stored only in the Dex blob store and is permanently
     * deleted when the Attribute is deleted or set to {@code null}.
     *
     * <p>Use this option for short-lived secrets, such as an API key or an access token, that a Flow
     * needs for a limited time. A regular Attribute value is recorded in Flow history. A hard-delete
     * Attribute value is never recorded in Flow history. Dex stores it only in the blob store and
     * records only a reference in Flow history. Calling {@link #delete(Context)} or
     * {@link #set(Context, Object)} with {@code null} permanently deletes the stored value. After
     * the deletion, the Attribute reads as absent.
     *
     * <p>Only string and object values are supported. Object values include JSON-mapped classes and
     * {@code byte[]}. Numeric and Boolean value types, such as {@link Integer}, {@link Long},
     * {@link Double}, and {@link Boolean}, are rejected when this option is applied. When the
     * declared value type is broader, such as {@link Object} or {@link Number}, a write whose
     * value maps to a number or a Boolean is rejected when it is written. Writing {@code null} is
     * always allowed.
     *
     * <p>This option cannot be combined with an {@link AttributeIndex} or with
     * {@link #syncToAttributeStore()}. {@link Client#waitForAttributeMatch} rejects a hard-delete
     * Attribute before it contacts the Server. Use an RPC that reads the Attribute when a caller
     * needs the value.
     *
     * <p>The Dex Server must have an enabled blob store and must support Server protocol version 3
     * or later. A Step, RPC, StartFlow, or SubFlow start that writes this Attribute fails with
     * {@link IllegalStateException} when the negotiated Server protocol is older. The Server rejects
     * the write when its blob store is disabled. Steps and RPC handlers receive the value inline.
     * The SDK never stores a hard-delete value in the local {@link BlobCache}.
     *
     * <p>Know these limitations before you store a secret:
     *
     * <ul>
     *   <li>Each Flow keeps one stored copy per Attribute key. Every write overwrites that copy in
     *       place.</li>
     *   <li>A Step or RPC attempt writes or deletes the stored value before Dex commits the attempt's
     *       result. A retried attempt, other Steps, and other readers can therefore observe a write
     *       or a deletion from an attempt that later failed.</li>
     *   <li>When concurrent Steps or RPCs write the same key, the final stored value may not be the
     *       value from the last committed write. When the order matters, use a single writer, or
     *       make every Step and RPC that writes the key hold an {@link AttributeLock} on it. A
     *       failed locked attempt can still overwrite the value.</li>
     *   <li>A {@link Client#startFlow} call or {@link SubFlow#run} start that fails because the
     *       Flow ID is already running still
     *       overwrites the running Flow's value. With a {@code null} initial value, it deletes the
     *       running Flow's value.</li>
     *   <li>Executions that reuse a Flow ID share the stored value.</li>
     *   <li>{@link Client#timeTravel} does not restore a deleted value. A deleted value reads as absent.</li>
     *   <li>A value remains in the blob store until it is deleted, or until Flow retention removes
     *       the Flow and blob cleanup runs.</li>
     *   <li>Blob store backups and bucket versioning can retain copies after deletion. Configure
     *       them separately when deleted secrets must not survive.</li>
     * </ul>
     *
     * <pre>{@code
     * final class ProviderSyncFlow implements Flow<String> {
     *     final Attribute<String> apiKey =
     *             Attribute.define("api-key", String.class).hardDeleteOnNull();
     *     private final SyncWithProvider sync = new SyncWithProvider(apiKey);
     *
     *     @Override
     *     public StepList<String> getSteps() {
     *         return StepList.startStep(sync);
     *     }
     *
     *     @Override
     *     public PersistenceSchema getPersistenceSchema() {
     *         return PersistenceSchema.of(apiKey);
     *     }
     * }
     *
     * final class SyncWithProvider implements Step<String> {
     *     private final Attribute<String> apiKey;
     *
     *     SyncWithProvider(final Attribute<String> apiKey) {
     *         this.apiKey = apiKey;
     *     }
     *
     *     @Override
     *     public StepDecision execute(final Context context, final String accountId) {
     *         final String key = apiKey.get(context);
     *         ProviderApi.sync(accountId, key);
     *         // Permanently delete the secret once it is no longer needed.
     *         apiKey.delete(context);
     *         return StepDecision.gracefulComplete();
     *     }
     * }
     *
     * // Start the Flow with the secret as an initial Attribute value.
     * client.startFlow(
     *         flow,
     *         "provider-sync-42",
     *         "account-42",
     *         StartFlowOptions.newBuilder()
     *                 .addAttribute(flow.apiKey, System.getenv("PROVIDER_API_KEY"))
     *                 .build());
     * }</pre>
     *
     * @return a new immutable Attribute definition with hard deletion on {@code null} enabled
     * @throws IllegalStateException if this definition has an index, uses
     *     {@link #syncToAttributeStore()}, or declares a numeric or Boolean value type
     */
    public Attribute<T> hardDeleteOnNull() {
        HardDeleteAttributes.requireDefinitionSupport(
                name, valueType, index, syncToAttributeStore);
        return new Attribute<T>(name, valueType, index, false, true);
    }

    String getName() {
        return name;
    }

    Class<T> getValueType() {
        return valueType;
    }

    AttributeIndex getIndex() {
        return index;
    }

    @Override
    boolean isSyncToAttributeStore() {
        return syncToAttributeStore;
    }

    @Override
    boolean isHardDeleteOnNull() {
        return hardDeleteOnNull;
    }

    /**
     * Reads this Attribute from the current invocation.
     *
     * @param context the Step or RPC invocation context
     * @return the current typed value, or {@code null} when no value is stored
     */
    public T get(final Context context) {
        return context.getAttribute(this);
    }

    /**
     * Writes this Attribute in the current invocation.
     *
     * @param context the Step or RPC invocation context
     * @param value the typed value to persist; {@code null} deletes the value
     * @throws IllegalArgumentException if this is a {@link #hardDeleteOnNull()} Attribute and the
     *     value maps to a number or a Boolean
     */
    public void set(final Context context, final T value) {
        context.setAttribute(this, value);
    }

    /**
     * Deletes this Attribute in the current invocation.
     *
     * @param context the Step or RPC invocation context
     */
    public void delete(final Context context) {
        context.deleteAttribute(this);
    }

    static String requireName(final String name) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("name is required");
        }
        return name;
    }

    static String requirePersistenceDefinitionName(final String name) {
        final String value = requireName(name);
        if (value.contains("/")) {
            throw new IllegalArgumentException("persistence definition names must not contain '/'");
        }
        return value;
    }

    static String requireMapInstance(final String instance) {
        final String value = requireName(instance);
        if (value.contains("/")) {
            throw new IllegalArgumentException("map instances must not contain '/'");
        }
        return value;
    }
}
