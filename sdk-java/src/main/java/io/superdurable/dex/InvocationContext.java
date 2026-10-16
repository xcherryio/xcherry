/*
 * Legacy Materials in this file remain under their original licenses.
 * See LEGACY_NOTICES.md.
 */

/*
 * Modifications Copyright (c) 2026 Super Durable, Inc.
 *
 * Modifications after the Legacy Cutoff are licensed under the
 * Sustainable Use License 1.0.
 * Legacy Materials remain under their original licenses.
 * See LICENSE and LEGACY_NOTICES.md.
 */

package io.superdurable.dex;

import io.superdurable.dex.exceptions.AttributeMapNotLoadedException;
import io.superdurable.dex.exceptions.ChannelMessagesNotLoadedException;
import io.superdurable.gen.AttributeSyncConfig;
import io.superdurable.gen.AttributeWrite;
import io.superdurable.gen.ChannelInfo;
import io.superdurable.gen.ChannelMessageDeletion;
import io.superdurable.gen.ChannelResult;
import io.superdurable.gen.ChannelValues;
import io.superdurable.gen.ConditionResults;
import io.superdurable.gen.ConditionStatus;
import io.superdurable.gen.KV;
import io.superdurable.gen.FlowResult;
import io.superdurable.gen.StepMethodHeartbeat;
import io.superdurable.gen.StepStreamWrite;
import io.superdurable.gen.TimerResult;
import io.superdurable.gen.Value;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

final class InvocationContext implements Context {
    enum Method {
        WAIT_FOR,
        EXECUTE,
        RPC
    }

    private final Method method;
    private final Registry.RegisteredFlow flow;
    private final io.superdurable.gen.Context metadata;
    private final ValueMapper values;
    private final StepOutputEmitter stepOutputEmitter;
    private final Map<String, Value> attributes;
    private final Map<String, Value> locals;
    private final ConditionResults conditionResults;
    private final Map<String, ChannelInfo> channelInfos;
    private final Map<String, ChannelValues> loadedChannelMessages;
    private final Set<String> loadedAttributeMapInstances;
    private final Set<String> loadedChannelNames;
    private final Set<String> loadedChannelMapInstances;
    private final ScheduledExecutorService bufferedStreamScheduler;
    private final io.grpc.Context requestContext;
    private final io.grpc.Context.CancellationListener cancellationListener;
    private final Map<String, AttributeWrite> attributeWrites =
            new LinkedHashMap<String, AttributeWrite>();
    private final Map<String, KV> localWrites = new LinkedHashMap<String, KV>();
    private final List<KV> events = new ArrayList<KV>();
    private final Set<String> eventNames = new HashSet<String>();
    private final List<io.superdurable.gen.ChannelMessage> publications =
            new ArrayList<io.superdurable.gen.ChannelMessage>();
    private final List<ChannelMessageDeletion> channelDeletions =
            new ArrayList<ChannelMessageDeletion>();
    private final List<StepOutputFinalizer> stepOutputFinalizers =
            new ArrayList<StepOutputFinalizer>();
    private boolean stepOutputsFinalized;

    InvocationContext(
            final Method method,
            final Registry.RegisteredFlow flow,
            final io.superdurable.gen.Context metadata,
            final ValueMapper values,
            final StepOutputEmitter stepOutputEmitter,
            final List<KV> attributes,
            final List<KV> locals,
            final ConditionResults conditionResults,
            final Map<String, ChannelInfo> channelInfos) {
        this(
                method,
                flow,
                metadata,
                values,
                stepOutputEmitter,
                attributes,
                locals,
                conditionResults,
                channelInfos,
                null);
    }

    InvocationContext(
            final Method method,
            final Registry.RegisteredFlow flow,
            final io.superdurable.gen.Context metadata,
            final ValueMapper values,
            final StepOutputEmitter stepOutputEmitter,
            final List<KV> attributes,
            final List<KV> locals,
            final ConditionResults conditionResults,
            final Map<String, ChannelInfo> channelInfos,
            final ScheduledExecutorService bufferedStreamScheduler) {
        this(
                method,
                flow,
                metadata,
                values,
                stepOutputEmitter,
                attributes,
                locals,
                conditionResults,
                channelInfos,
                bufferedStreamScheduler,
                Collections.<String, ChannelValues>emptyMap(),
                Collections.<String>emptyList(),
                Collections.<String>emptyList(),
                Collections.<String>emptyList());
    }

    InvocationContext(
            final Method method,
            final Registry.RegisteredFlow flow,
            final io.superdurable.gen.Context metadata,
            final ValueMapper values,
            final StepOutputEmitter stepOutputEmitter,
            final List<KV> attributes,
            final List<KV> locals,
            final ConditionResults conditionResults,
            final Map<String, ChannelInfo> channelInfos,
            final ScheduledExecutorService bufferedStreamScheduler,
            final Map<String, ChannelValues> loadedChannelMessages,
            final List<String> loadedAttributeMapInstances,
            final List<String> loadedChannelNames,
            final List<String> loadedChannelMapInstances) {
        if (metadata == null) {
            throw new IllegalArgumentException("Worker request Context is required");
        }
        this.method = method;
        this.flow = flow;
        this.metadata = metadata;
        this.values = values;
        this.stepOutputEmitter = stepOutputEmitter;
        this.bufferedStreamScheduler = bufferedStreamScheduler;
        this.requestContext = io.grpc.Context.current();
        this.cancellationListener = ignored -> cancelStepOutputs();
        if (stepOutputEmitter != null) {
            requestContext.addListener(cancellationListener, Runnable::run);
        }
        this.attributes = mapValues("Attribute", attributes);
        this.locals = mapValues("step-execution local", locals);
        this.conditionResults = conditionResults;
        this.channelInfos = channelInfos == null
                ? new HashMap<String, ChannelInfo>()
                : new HashMap<String, ChannelInfo>(channelInfos);
        this.loadedChannelMessages = new HashMap<String, ChannelValues>(loadedChannelMessages);
        this.loadedAttributeMapInstances = new HashSet<String>(loadedAttributeMapInstances);
        this.loadedChannelNames = new HashSet<String>(loadedChannelNames);
        this.loadedChannelMapInstances = new HashSet<String>(loadedChannelMapInstances);
    }

    @Override
    public String getFlowId() {
        return metadata.getFlowId();
    }

    @Override
    public String getRunId() {
        return metadata.getRunId();
    }

    @Override
    public Instant getFlowStartedAt() {
        return Instant.ofEpochSecond(metadata.getFlowStartedTimestamp());
    }

    @Override
    public String getStepExecutionId() {
        return metadata.getStepExecutionId();
    }

    @Override
    public String getFromStepExecutionId() {
        return metadata.getFromStepExecutionId();
    }

    @Override
    public RecoveryErrorInfo getRecoveryError() {
        return metadata.hasRecoveryError()
                ? RecoveryErrorInfo.fromProto(metadata.getRecoveryError())
                : null;
    }

    @Override
    public Instant getFirstAttemptAt() {
        return Instant.ofEpochSecond(metadata.getFirstAttemptTimestamp());
    }

    @Override
    public int getAttempt() {
        return metadata.getAttempt();
    }

    @Override
    public boolean isCancellationRequested() {
        return io.grpc.Context.current().isCancelled()
                || Thread.currentThread().isInterrupted();
    }

    @Override
    public boolean hasTimerFired() {
        if (conditionResults == null) {
            return false;
        }
        for (TimerResult result : conditionResults.getTimerResultsList()) {
            if (result.getConditionStatus() == ConditionStatus.CONDITION_STATUS_COMPLETED) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean hasTimerFired(final int index) {
        return conditionResults != null
                && index >= 0
                && index < conditionResults.getTimerResultsCount()
                && conditionResults.getTimerResults(index).getConditionStatus()
                == ConditionStatus.CONDITION_STATUS_COMPLETED;
    }

    @Override
    public boolean waitForMethodFailed() {
        return conditionResults != null && conditionResults.getWaitForFailed();
    }

    @Override
    public void recordHeartbeat(final Object value) {
        requireStepOutput("Heartbeats");
        final StepMethodHeartbeat.Builder heartbeat = StepMethodHeartbeat.newBuilder();
        if (value != null) {
            heartbeat.setValue(values.encode(value));
        }
        stepOutputEmitter.emitHeartbeat(heartbeat.build());
    }

    @Override
    public boolean hasLastHeartbeatValue() {
        return metadata.hasLastHeartbeatValue();
    }

    @Override
    public <T> T getLastHeartbeatValue(final Class<T> valueType) {
        if (!metadata.hasLastHeartbeatValue()) {
            return null;
        }
        return values.decode(metadata.getLastHeartbeatValue(), valueType);
    }

    @Override
    public <T> void writeStream(final Stream<T> stream, final T value) {
        requireStepOutput("Stream writes");
        requireRegistered(stream);
        stepOutputEmitter.emitStreamWrite(StepStreamWrite.newBuilder()
                .setStreamName(stream.getStreamName())
                .setStreamCapacityBytes(stream.getStreamCapacityBytes())
                .setValue(values.encode(value))
                .build());
    }

    synchronized void prepareBufferedStream(final Stream<String> stream) {
        requireStepOutput("Buffered Streams");
        requireRegistered(stream);
        if (stream.getValueType() != String.class) {
            throw new IllegalArgumentException("Buffered Streams require Stream<String>");
        }
        if (stepOutputsFinalized) {
            throw new IllegalStateException("Buffered Stream invocation has finished");
        }
    }

    synchronized void registerStepOutputFinalizer(final StepOutputFinalizer finalizer) {
        if (stepOutputsFinalized) {
            throw new IllegalStateException("Buffered Stream invocation has finished");
        }
        stepOutputFinalizers.add(finalizer);
    }

    synchronized Throwable finalizeStepOutputs(final Throwable failure) {
        if (stepOutputsFinalized) {
            return failure;
        }
        stepOutputsFinalized = true;
        requestContext.removeListener(cancellationListener);
        Throwable combined = failure;
        for (StepOutputFinalizer finalizer : stepOutputFinalizers) {
            try {
                finalizer.finalizeStepOutput();
            } catch (Throwable finalizationFailure) {
                if (combined == null) {
                    combined = finalizationFailure;
                } else if (combined != finalizationFailure) {
                    combined.addSuppressed(finalizationFailure);
                }
            }
        }
        return combined;
    }

    private synchronized void cancelStepOutputs() {
        if (stepOutputsFinalized) {
            return;
        }
        stepOutputsFinalized = true;
        for (StepOutputFinalizer finalizer : stepOutputFinalizers) {
            finalizer.cancelStepOutput();
        }
    }

    ScheduledExecutorService getBufferedStreamScheduler() {
        if (bufferedStreamScheduler == null) {
            throw new IllegalStateException("Buffered Stream scheduler is unavailable");
        }
        return bufferedStreamScheduler;
    }

    private void requireStepOutput(final String operation) {
        if (method == Method.RPC) {
            throw new IllegalStateException(operation + " require a Step Context");
        }
        if (stepOutputEmitter == null) {
            throw new IllegalStateException("Step output emitter is required");
        }
    }

    io.superdurable.dex.FlowResult subFlowResult(final int index) {
        if (method != Method.EXECUTE || conditionResults == null) {
            throw new IllegalStateException(
                    "SubFlow condition results are only available during Step execute");
        }
        if (index < 0 || index >= conditionResults.getSubFlowResultsCount()) {
            throw new IllegalArgumentException("SubFlow condition index is out of range: " + index);
        }
        final FlowResult result = conditionResults.getSubFlowResults(index);
        final List<StepCompletion> completions = new ArrayList<StepCompletion>();
        for (io.superdurable.gen.StepCompletionOutput completion
                : result.getResultsList()) {
            completions.add(new StepCompletion(
                    completion,
                    (value, outputType) -> values.decode(value, outputType)));
        }
        return new io.superdurable.dex.FlowResult(
                Client.mapFlowStatus(result.getFlowStatus()),
                Client.mapNullableFlowErrorType(result.getErrorType()),
                result.getErrorMessage().isEmpty() ? null : result.getErrorMessage(),
                completions);
    }

    String subFlowId(final int index) {
        if (method != Method.EXECUTE || conditionResults == null) {
            throw new IllegalStateException(
                    "SubFlow IDs are only available during Step execute");
        }
        if (index < 0 || index >= conditionResults.getSubFlowResultsCount()) {
            throw new IllegalArgumentException("SubFlow condition index is out of range: " + index);
        }
        return "SubFlow:" + metadata.getFlowId() + "-" + metadata.getStepExecutionId() + "-" + index;
    }

    @Override
    public <T> void setStepExecutionLocal(
            final String key,
            final T value,
            final Class<T> valueType) {
        requireName(key, "step-execution local key");
        localWrites.put(key, KV.newBuilder().setKey(key).setValue(values.encode(value)).build());
    }

    @Override
    public <T> T getStepExecutionLocal(final String key, final Class<T> valueType) {
        requireName(key, "step-execution local key");
        final Value value = localWrites.containsKey(key)
                ? localWrites.get(key).getValue()
                : locals.get(key);
        return value == null ? defaultValue(valueType) : values.decode(value, valueType);
    }

    @Override
    public <T> void recordEvent(
            final String name,
            final T value,
            final Class<T> valueType) {
        requireName(name, "event name");
        if (!eventNames.add(name)) {
            throw new IllegalArgumentException("event was already recorded: " + name);
        }
        events.add(KV.newBuilder().setKey(name).setValue(values.encode(value)).build());
    }

    @Override
    public <T> T getAttribute(final Attribute<T> attribute) {
        return getAttributeValue(attribute, null, attribute.getValueType());
    }

    @Override
    public <T> T getAttribute(
            final AttributeMap<T> attribute,
            final String instance) {
        return getAttributeValue(attribute, instance, attribute.getValueType());
    }

    @Override
    public <T> void setAttribute(final Attribute<T> attribute, final T value) {
        setAttributeValue(attribute, null, value, attribute.getIndex());
    }

    @Override
    public <T> void setAttribute(
            final AttributeMap<T> attribute,
            final String instance,
            final T value) {
        setAttributeValue(attribute, instance, value, attribute.getIndex());
    }

    @Override
    public void deleteAttribute(final Attribute<?> attribute) {
        deleteAttributeValue(attribute, null, attribute.getIndex());
    }

    @Override
    public void deleteAttribute(
            final AttributeMap<?> attribute,
            final String instance) {
        deleteAttributeValue(attribute, instance, attribute.getIndex());
    }

    @Override
    public <T> void publish(final Channel<T> channel, final T value) {
        publishValue(channel, null, value);
    }

    @Override
    public <T> void publish(
            final ChannelMap<T> channel,
            final String instance,
            final T value) {
        publishValue(channel, instance, value);
    }

    @Override
    public void deleteChannelMessage(final Channel<?> channel, final String messageId) {
        deleteChannelMessageValue(channel, null, messageId);
    }

    @Override
    public void deleteChannelMessage(
            final ChannelMap<?> channel,
            final String instance,
            final String messageId) {
        deleteChannelMessageValue(channel, instance, messageId);
    }

    @Override
    public int channelSize(final Channel<?> channel) {
        return channelSizeValue(channel, null);
    }

    @Override
    public int channelSize(final ChannelMap<?> channel, final String instance) {
        return channelSizeValue(channel, instance);
    }

    @Override
    public <T> List<ChannelMessage<T>> pendingChannelMessages(final Channel<T> channel) {
        return pendingChannelMessagesValue(channel, null, channel.getValueType());
    }

    @Override
    public <T> List<ChannelMessage<T>> pendingChannelMessages(
            final ChannelMap<T> channel,
            final String instance) {
        return pendingChannelMessagesValue(channel, instance, channel.getValueType());
    }

    @Override
    public <T> List<T> channelResults(final Channel<T> channel) {
        return channelResultsValue(channel, null, channel.getValueType());
    }

    @Override
    public <T> List<T> channelResults(
            final ChannelMap<T> channel,
            final String instance) {
        return channelResultsValue(channel, instance, channel.getValueType());
    }

    List<AttributeWrite> getAttributeWrites() {
        return new ArrayList<AttributeWrite>(attributeWrites.values());
    }

    List<String> attributeMapKeys(final AttributeMap<?> attribute) {
        requireRegistered(attribute);
        if (!loadedAttributeMapInstances.contains(attribute.getName() + "/")) {
            throw new AttributeMapNotLoadedException(
                    "all AttributeMap instances were not loaded for this invocation: "
                            + attribute.getName());
        }
        final String prefix = attribute.getName() + "/";
        final Set<String> physicalKeys = new HashSet<String>();
        for (String key : attributes.keySet()) {
            if (key.startsWith(prefix)) {
                physicalKeys.add(key);
            }
        }
        for (Map.Entry<String, AttributeWrite> entry : attributeWrites.entrySet()) {
            if (!entry.getKey().startsWith(prefix)) {
                continue;
            }
            if (entry.getValue().getValue().getKindCase() == Value.KindCase.NULL_VALUE) {
                physicalKeys.remove(entry.getKey());
            } else {
                physicalKeys.add(entry.getKey());
            }
        }
        return sortedInstanceKeys(prefix, physicalKeys);
    }

    List<String> channelMapKeys(final ChannelMap<?> channel) {
        requireRegistered(channel);
        final String prefix = channel.getName() + "/";
        final Set<String> physicalKeys = new HashSet<String>();
        for (Map.Entry<String, ChannelInfo> entry : channelInfos.entrySet()) {
            if (entry.getValue().getSize() > 0 && entry.getKey().startsWith(prefix)) {
                physicalKeys.add(entry.getKey());
            }
        }
        return sortedInstanceKeys(prefix, physicalKeys);
    }

    private static List<String> sortedInstanceKeys(
            final String prefix,
            final Set<String> physicalKeys) {
        final List<String> keys = new ArrayList<String>(physicalKeys.size());
        for (String physical : physicalKeys) {
            try {
                keys.add(Attribute.requireMapInstance(
                        URLDecoder.decode(physical.substring(prefix.length()), "UTF-8")));
            } catch (UnsupportedEncodingException impossible) {
                throw new IllegalStateException(impossible);
            }
        }
        Collections.sort(keys);
        return Collections.unmodifiableList(keys);
    }

    List<KV> getLocalWrites() {
        return new ArrayList<KV>(localWrites.values());
    }

    List<KV> getEvents() {
        return Collections.unmodifiableList(events);
    }

    List<io.superdurable.gen.ChannelMessage> getPublications() {
        return Collections.unmodifiableList(publications);
    }

    List<ChannelMessageDeletion> getChannelDeletions() {
        return Collections.unmodifiableList(channelDeletions);
    }

    private <T> T getAttributeValue(
            final PersistenceDefinition definition,
            final String instance,
            final Class<T> valueType) {
        requireRegistered(definition);
        final String key = physicalName(definition, instance);
        if (definition instanceof AttributeMap
                && !isMapInstanceLoaded(
                        loadedAttributeMapInstances, definition.getName(), key)) {
            throw new AttributeMapNotLoadedException(
                    "AttributeMap instance was not loaded for this invocation: " + key);
        }
        final AttributeWrite write = attributeWrites.get(key);
        if (write != null) {
            if (write.getValue().getKindCase() == Value.KindCase.NULL_VALUE) {
                return defaultValue(valueType);
            }
            return values.decode(write.getValue(), valueType);
        }
        final Value value = attributes.get(key);
        return value == null ? defaultValue(valueType) : values.decode(value, valueType);
    }

    private void setAttributeValue(
            final PersistenceDefinition definition,
            final String instance,
            final Object value,
            final AttributeIndex index) {
        requireRegistered(definition);
        final String key = physicalName(definition, instance);
        final AttributeWrite.Builder write = AttributeWrite.newBuilder()
                .setKey(key)
                .setValue(values.encode(value));
        final io.superdurable.gen.IndexConfig indexConfig =
                values.indexConfig(index, definition instanceof AttributeMap);
        if (indexConfig != null) {
            write.setIndexConfig(indexConfig);
        }
        applyAttributeSync(write, definition);
        HardDeleteAttributes.applyHardDeleteOnNull(write, definition);
        attributeWrites.put(key, write.build());
    }

    private void deleteAttributeValue(
            final PersistenceDefinition definition,
            final String instance,
            final AttributeIndex index) {
        requireRegistered(definition);
        final String key = physicalName(definition, instance);
        final AttributeWrite.Builder write = AttributeWrite.newBuilder()
                .setKey(key)
                .setValue(values.deletion());
        final io.superdurable.gen.IndexConfig indexConfig =
                values.indexConfig(index, definition instanceof AttributeMap);
        if (indexConfig != null) {
            write.setIndexConfig(indexConfig);
        }
        applyAttributeSync(write, definition);
        HardDeleteAttributes.applyHardDeleteOnNull(write, definition);
        attributeWrites.put(key, write.build());
    }

    private static void applyAttributeSync(
            final AttributeWrite.Builder write,
            final PersistenceDefinition definition) {
        if (definition.isSyncToAttributeStore()) {
            write.setSyncConfig(AttributeSyncConfig.newBuilder().setEnabled(true));
        }
    }

    private void publishValue(
            final PersistenceDefinition definition,
            final String instance,
            final Object value) {
        requireRegistered(definition);
        final String name = physicalName(definition, instance);
        publications.add(io.superdurable.gen.ChannelMessage.newBuilder()
                .setChannelName(name)
                .setValue(values.encode(value))
                .build());
        final ChannelInfo existing = channelInfos.get(name);
        final int size = existing == null ? 0 : existing.getSize();
        channelInfos.put(name, ChannelInfo.newBuilder().setSize(size + 1).build());
    }

    private void deleteChannelMessageValue(
            final PersistenceDefinition definition,
            final String instance,
            final String messageId) {
        requireRegistered(definition);
        final String name = physicalName(definition, instance);
        channelDeletions.add(ChannelMessageDeletion.newBuilder()
                .setChannelName(name)
                .setMessageId(Attribute.requireName(messageId))
                .build());
        final ChannelInfo existing = channelInfos.get(name);
        if (existing != null && existing.getSize() > 0) {
            channelInfos.put(
                    name,
                    ChannelInfo.newBuilder().setSize(existing.getSize() - 1).build());
        }
    }

    private int channelSizeValue(
            final PersistenceDefinition definition,
            final String instance) {
        requireRegistered(definition);
        final ChannelInfo info = channelInfos.get(physicalName(definition, instance));
        return info == null ? 0 : info.getSize();
    }

    private <T> List<T> channelResultsValue(
            final PersistenceDefinition definition,
            final String instance,
            final Class<T> valueType) {
        requireRegistered(definition);
        if (conditionResults == null) {
            return Collections.emptyList();
        }
        final String name = physicalName(definition, instance);
        final List<T> decoded = new ArrayList<T>();
        for (ChannelResult result : conditionResults.getChannelResultsList()) {
            if (name.equals(result.getChannelName())
                    && result.getConditionStatus() == ConditionStatus.CONDITION_STATUS_COMPLETED) {
                for (Value value : result.getValuesList()) {
                    decoded.add(values.decode(value, valueType));
                }
            }
        }
        return Collections.unmodifiableList(decoded);
    }

    private <T> List<ChannelMessage<T>> pendingChannelMessagesValue(
            final PersistenceDefinition definition,
            final String instance,
            final Class<T> valueType) {
        requireRegistered(definition);
        final String name = physicalName(definition, instance);
        final boolean isLoaded;
        if (definition instanceof ChannelMap) {
            isLoaded = isMapInstanceLoaded(
                    loadedChannelMapInstances, definition.getName(), name);
        } else {
            isLoaded = loadedChannelNames.contains(name);
        }
        if (!isLoaded) {
            throw new ChannelMessagesNotLoadedException(
                    "Channel messages were not loaded for this invocation: " + name);
        }
        final ChannelValues loaded = loadedChannelMessages.get(name);
        if (loaded == null) {
            return Collections.emptyList();
        }
        final List<ChannelMessage<T>> decoded = new ArrayList<ChannelMessage<T>>();
        for (io.superdurable.gen.ChannelMessage message : loaded.getMessagesList()) {
            decoded.add(new ChannelMessage<T>(
                    message.getMessageId(), values.decode(message.getValue(), valueType)));
        }
        return Collections.unmodifiableList(decoded);
    }

    private static boolean isMapInstanceLoaded(
            final Set<String> instances,
            final String name,
            final String physicalName) {
        return instances.contains(name + "/") || instances.contains(physicalName);
    }

    private void requireRegistered(final PersistenceDefinition definition) {
        final PersistenceDefinition registered = flow.getPersistence().get(definition.getName());
        if (registered != definition) {
            throw new IllegalArgumentException(
                    "persistence definition does not belong to Flow: " + definition.getName());
        }
    }

    private static String physicalName(
            final PersistenceDefinition definition,
            final String instance) {
        if (definition instanceof AttributeMap || definition instanceof ChannelMap) {
            return Registry.physicalName(definition.getName(), instance);
        }
        if (instance != null) {
            throw new IllegalArgumentException("static definition cannot use an instance");
        }
        return definition.getName();
    }

    private static Map<String, Value> mapValues(final String kind, final List<KV> entries) {
        final Map<String, Value> mapped = new HashMap<String, Value>();
        if (entries == null) {
            return mapped;
        }
        for (KV entry : entries) {
            if (entry.getKey().isEmpty() || !entry.hasValue()
                    || mapped.put(entry.getKey(), entry.getValue()) != null) {
                throw new IllegalArgumentException("invalid or duplicate " + kind);
            }
        }
        return mapped;
    }

    @SuppressWarnings("unchecked")
    private static <T> T defaultValue(final Class<T> valueType) {
        final Object value;
        if (valueType == Boolean.class || valueType == Boolean.TYPE) {
            value = false;
        } else if (valueType == Byte.class || valueType == Byte.TYPE) {
            value = (byte) 0;
        } else if (valueType == Short.class || valueType == Short.TYPE) {
            value = (short) 0;
        } else if (valueType == Integer.class || valueType == Integer.TYPE) {
            value = 0;
        } else if (valueType == Long.class || valueType == Long.TYPE) {
            value = 0L;
        } else if (valueType == Float.class || valueType == Float.TYPE) {
            value = 0.0f;
        } else if (valueType == Double.class || valueType == Double.TYPE) {
            value = 0.0d;
        } else {
            value = null;
        }
        return (T) value;
    }

    private static void requireName(final String value, final String kind) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(kind + " is required");
        }
    }
}
