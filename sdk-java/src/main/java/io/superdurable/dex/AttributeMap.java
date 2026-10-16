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
 * Defines a durable map of named, typed Attribute instances.
 *
 * <p>An {@code AttributeMap} is useful when keys are dynamically determined at runtime. Register
 * the definition once in the Flow's {@link PersistenceSchema}, then supply an instance name for
 * each read, write, delete, or lock. Each key/value pair is stored as a separate blob, so updating
 * one entry does not rewrite a large Attribute containing the entire logical map. Each value is
 * serialized with the definition's concrete {@link Class}. Instance names must be nonblank and
 * must not contain {@code /}.
 *
 * <pre>{@code
 * private final AttributeMap<String> orderStatus =
 *         AttributeMap.define("order-status", String.class);
 *
 * orderStatus.set(context, orderId, "PAID");
 * String status = orderStatus.get(context, orderId);
 * }</pre>
 *
 * @param <T> the Java value type stored in every map instance
 */
public final class AttributeMap<T> extends PersistenceDefinition {
    private final String name;
    private final Class<T> valueType;
    private final AttributeIndex index;
    private final boolean syncToAttributeStore;
    private final boolean hardDeleteOnNull;

    private AttributeMap(
            final String name,
            final Class<T> valueType,
            final AttributeIndex index,
            final boolean syncToAttributeStore,
            final boolean hardDeleteOnNull) {
        this.name = Attribute.requirePersistenceDefinitionName(name);
        this.valueType = Objects.requireNonNull(valueType, "valueType");
        this.index = index;
        this.syncToAttributeStore = syncToAttributeStore;
        this.hardDeleteOnNull = hardDeleteOnNull;
    }

    /**
     * Defines an Attribute map without a search index.
     *
     * @param name the stable Attribute-map name; must not be blank or contain {@code /}
     * @param valueType the concrete Java class used for every instance value
     * @param <T> the Java value type
     * @return the typed Attribute-map definition
     * @throws IllegalArgumentException if {@code name} is {@code null}, blank, or contains {@code /}
     * @throws NullPointerException if {@code valueType} is {@code null}
     */
    public static <T> AttributeMap<T> define(final String name, final Class<T> valueType) {
        return new AttributeMap<T>(name, valueType, null, false, false);
    }

    /**
     * Defines an Attribute map with a search index.
     *
     * @param name the stable Attribute-map name; must not be blank or contain {@code /}
     * @param valueType the concrete Java class used for every instance value
     * @param index the search-index definition, or {@code null} for no index
     * @param <T> the Java value type
     * @return the typed Attribute-map definition
     * @throws IllegalArgumentException if {@code name} is {@code null}, blank, or contains {@code /}
     * @throws NullPointerException if {@code valueType} is {@code null}
     */
    public static <T> AttributeMap<T> define(
            final String name,
            final Class<T> valueType,
            final AttributeIndex index) {
        return new AttributeMap<T>(name, valueType, index, false, false);
    }

    /**
     * Returns a definition whose instance writes are projected through the Flow's Attribute Store.
     *
     * <p>The physical Attribute-map key must match a target column. Projection is asynchronous and
     * does not roll back the Flow Attribute when external storage fails. Deletion projects SQL
     * {@code NULL} when the target column permits it.
     *
     * @return a new immutable Attribute-map definition with Attribute Store synchronization enabled
     * @throws IllegalStateException if this definition uses {@link #hardDeleteOnNull()}
     */
    public AttributeMap<T> syncToAttributeStore() {
        if (hardDeleteOnNull) {
            throw new IllegalStateException(
                    "hard-delete Attribute " + name + " cannot use syncToAttributeStore");
        }
        return new AttributeMap<T>(name, valueType, index, true, false);
    }

    /**
     * Returns a definition whose instance values are stored only in the Dex blob store and are
     * permanently deleted when an instance is deleted or set to {@code null}.
     *
     * <p>Use this option for short-lived secrets that are keyed at runtime, such as one API key per
     * tenant or per connected account. Each instance value is never recorded in Flow history. Dex
     * stores it only in the blob store and records only a reference in Flow history. Calling
     * {@link #delete(Context, String)} or {@link #set(Context, String, Object)} with {@code null}
     * permanently deletes that instance's stored value. After the deletion, the instance reads as
     * absent. Other instances are unaffected.
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
     * Attribute map before it contacts the Server. Use an RPC that reads the instance when a caller
     * needs the value.
     *
     * <p>The Dex Server must have an enabled blob store and must support Server protocol version 3
     * or later. A Step, RPC, StartFlow, or SubFlow start that writes this Attribute map fails with
     * {@link IllegalStateException} when the negotiated Server protocol is older. The Server rejects
     * the write when its blob store is disabled. Steps and RPC handlers receive instance values
     * inline. The SDK never stores a hard-delete value in the local {@link BlobCache}.
     *
     * <p>Know these limitations before you store a secret:
     *
     * <ul>
     *   <li>Each Flow keeps one stored copy per instance key. Every write overwrites that copy in
     *       place.</li>
     *   <li>A Step or RPC attempt writes or deletes the stored value before Dex commits the attempt's
     *       result. A retried attempt, other Steps, and other readers can therefore observe a write
     *       or a deletion from an attempt that later failed.</li>
     *   <li>When concurrent Steps or RPCs write the same instance, the final stored value may not be
     *       the value from the last committed write. When the order matters, use a single writer,
     *       or make every Step and RPC that writes the instance hold an {@link AttributeLock} on
     *       it. A failed locked attempt can still overwrite the value.</li>
     *   <li>A {@link Client#startFlow} call or {@link SubFlow#run} start that fails because the
     *       Flow ID is already running still
     *       overwrites the running Flow's instance value. With a {@code null} initial value, it
     *       deletes the running Flow's instance value.</li>
     *   <li>Executions that reuse a Flow ID share the stored values.</li>
     *   <li>{@link Client#timeTravel} does not restore a deleted value. A deleted value reads as absent.</li>
     *   <li>A Flow retry or a restarted SubFlow starts from the original initial value. If an
     *       earlier run deleted that value, the new run reads it as absent.</li>
     *   <li>Removing {@code hardDeleteOnNull()} from a definition leaves the stored value in place
     *       until Dex removes the Flow. Delete the value with the hard-delete definition first.</li>
     *   <li>A value remains in the blob store until it is deleted, or until Flow retention removes
     *       the Flow and blob cleanup runs.</li>
     *   <li>Blob store backups and bucket versioning can retain copies after deletion. Configure
     *       them separately when deleted secrets must not survive.</li>
     * </ul>
     *
     * <pre>{@code
     * final class TenantSyncFlow implements Flow<String> {
     *     final AttributeMap<String> tenantApiKeys =
     *             AttributeMap.define("tenant-api-keys", String.class).hardDeleteOnNull();
     *     private final SyncTenant sync = new SyncTenant(tenantApiKeys);
     *
     *     @Override
     *     public StepList<String> getSteps() {
     *         return StepList.startStep(sync);
     *     }
     *
     *     @Override
     *     public PersistenceSchema getPersistenceSchema() {
     *         return PersistenceSchema.of(tenantApiKeys);
     *     }
     * }
     *
     * final class SyncTenant implements Step<String> {
     *     private final AttributeMap<String> tenantApiKeys;
     *
     *     SyncTenant(final AttributeMap<String> tenantApiKeys) {
     *         this.tenantApiKeys = tenantApiKeys;
     *     }
     *
     *     @Override
     *     public StepDecision execute(final Context context, final String tenantId) {
     *         final String key = tenantApiKeys.get(context, tenantId);
     *         ProviderApi.sync(tenantId, key);
     *         // Permanently delete this tenant's secret once it is no longer needed.
     *         tenantApiKeys.delete(context, tenantId);
     *         return StepDecision.gracefulComplete();
     *     }
     * }
     *
     * // Start the Flow with one tenant's secret as an initial instance value.
     * client.startFlow(
     *         flow,
     *         "tenant-sync-7",
     *         "tenant-7",
     *         StartFlowOptions.newBuilder()
     *                 .addAttribute(flow.tenantApiKeys, "tenant-7", System.getenv("TENANT_7_KEY"))
     *                 .build());
     * }</pre>
     *
     * @return a new immutable Attribute-map definition with hard deletion on {@code null} enabled
     * @throws IllegalStateException if this definition has an index, uses
     *     {@link #syncToAttributeStore()}, or declares a numeric or Boolean value type
     */
    public AttributeMap<T> hardDeleteOnNull() {
        HardDeleteAttributes.requireDefinitionSupport(
                name, valueType, index, syncToAttributeStore);
        return new AttributeMap<T>(name, valueType, index, false, true);
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
     * Reads one map instance from the current invocation.
     *
     * @param context the Step or RPC invocation context
     * @param instance the instance key within this map
     * @return the current typed value, or {@code null} when the instance is absent
     */
    public T get(final Context context, final String instance) {
        return context.getAttribute(this, instance);
    }

    /**
     * Writes one map instance in the current invocation.
     *
     * @param context the Step or RPC invocation context
     * @param instance the instance key within this map
     * @param value the typed value to persist; {@code null} deletes the instance
     * @throws IllegalArgumentException if this is a {@link #hardDeleteOnNull()} Attribute map and
     *     the value maps to a number or a Boolean
     */
    public void set(final Context context, final String instance, final T value) {
        context.setAttribute(this, instance, value);
    }

    /**
     * Deletes one map instance in the current invocation.
     *
     * @param context the Step or RPC invocation context
     * @param instance the instance key within this map
     */
    public void delete(final Context context, final String instance) {
        context.deleteAttribute(this, instance);
    }

    /**
     * Returns the number of existing instances, including writes buffered by this invocation.
     *
     * @param context the Step or RPC invocation context
     * @return the current number of instances
     */
    public int getMapSize(final Context context) {
        return getAllInstanceKeys(context).size();
    }

    /**
     * Returns existing instance keys in ascending order, including buffered writes.
     *
     * @param context the Step or RPC invocation context
     * @return an immutable sorted list of decoded instance keys
     */
    public java.util.List<String> getAllInstanceKeys(final Context context) {
        if (!(context instanceof InvocationContext)) {
            throw new IllegalArgumentException("Dex invocation Context is required");
        }
        return ((InvocationContext) context).attributeMapKeys(this);
    }
}
