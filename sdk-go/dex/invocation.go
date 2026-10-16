// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package dex

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/superdurable/dex/sdk-go/gen/dexpb"
)

type invocationMethod uint8

const (
	invocationWaitFor invocationMethod = iota + 1
	invocationExecute
	invocationRPC
)

type invocationContext struct {
	context.Context
	method        invocationMethod
	flow          *registeredFlow
	outputEmitter *stepOutputEmitter
	active        bool

	flowID              string
	runID               string
	flowStartedAt       time.Time
	stepExecutionID     string
	fromStepExecutionID string
	recoveryError       *RecoveryErrorInfo
	firstAttemptAt      time.Time
	attempt             int32
	lastHeartbeatValue  *dexpb.Value

	attributes          map[string]*dexpb.Value
	attributeWrites     map[string]*dexpb.AttributeWrite
	attributeWriteOrder []string

	locals          map[string]*dexpb.Value
	localWrites     map[string]*dexpb.KV
	localWriteOrder []string

	events         map[string]struct{}
	recordedEvents []*dexpb.KV
	publications   []*dexpb.ChannelMessage
	deletions      []*dexpb.ChannelMessageDeletion

	conditionResults            *dexpb.ConditionResults
	channelSizes                map[string]int
	loadedChannelMessages       map[string]*dexpb.ChannelValues
	loadedAttributeMapInstances map[string]struct{}
	loadedChannelNames          map[string]struct{}
	loadedChannelMapInstances   map[string]struct{}
	outputFinalizers            []invocationOutputFinalizer
}

type invocationOutputFinalizer interface {
	finalize() error
}

func newInvocationContext(
	ctx context.Context,
	method invocationMethod,
	flow *registeredFlow,
	outputStream stepProgressStream,
	metadata *dexpb.Context,
	attributes []*dexpb.KV,
	locals []*dexpb.KV,
	conditionResults *dexpb.ConditionResults,
	channelInfos map[string]*dexpb.ChannelInfo,
	loadedChannelMessages map[string]*dexpb.ChannelValues,
	loadedAttributeMapInstances []string,
	loadedChannelNames []string,
	loadedChannelMapInstances []string,
) (*invocationContext, error) {
	attributeValues, err := buildInvocationValues("attribute", attributes)
	if err != nil {
		return nil, err
	}
	localValues, err := buildInvocationValues("step-execution local", locals)
	if err != nil {
		return nil, err
	}
	sizes, err := buildChannelSizes(channelInfos)
	if err != nil {
		return nil, err
	}
	if err := validateConditionResults(conditionResults); err != nil {
		return nil, err
	}
	invocationHandlerContext := ctx
	var outputEmitter *stepOutputEmitter
	if method == invocationRPC {
		if outputStream != nil {
			panic("dex: RPC invocation cannot have a Step output stream")
		}
	} else if outputStream != nil {
		invocationHandlerContext, outputEmitter = newStepOutputEmitter(ctx, outputStream)
	}
	invocation := &invocationContext{
		Context:                     invocationHandlerContext,
		method:                      method,
		flow:                        flow,
		outputEmitter:               outputEmitter,
		active:                      true,
		flowID:                      metadata.FlowId,
		runID:                       metadata.RunId,
		flowStartedAt:               time.Unix(metadata.FlowStartedTimestamp, 0),
		attributes:                  attributeValues,
		attributeWrites:             make(map[string]*dexpb.AttributeWrite),
		locals:                      localValues,
		localWrites:                 make(map[string]*dexpb.KV),
		events:                      make(map[string]struct{}),
		conditionResults:            conditionResults,
		channelSizes:                sizes,
		loadedChannelMessages:       loadedChannelMessages,
		loadedAttributeMapInstances: stringSet(loadedAttributeMapInstances),
		loadedChannelNames:          stringSet(loadedChannelNames),
		loadedChannelMapInstances:   stringSet(loadedChannelMapInstances),
	}
	if method != invocationRPC {
		invocation.stepExecutionID = metadata.StepExecutionId
		invocation.fromStepExecutionID = metadata.FromStepExecutionId
		if metadata.RecoveryError != nil {
			invocation.recoveryError = &RecoveryErrorInfo{
				Detail:    metadata.RecoveryError.Detail,
				ErrorType: metadata.RecoveryError.ErrorType,
			}
		}
		invocation.attempt = metadata.Attempt
		invocation.firstAttemptAt = time.Unix(metadata.FirstAttemptTimestamp, 0)
		invocation.lastHeartbeatValue = metadata.LastHeartbeatValue
	}
	return invocation, nil
}

func (invocation *invocationContext) writeStream(
	definition *streamDefinition,
	value any,
) error {
	if invocation.method == invocationRPC || invocation.outputEmitter == nil {
		return errInvalidInvocationContext
	}
	registered, err := invocation.flow.resolveStream(definition)
	if err != nil {
		return fmt.Errorf("dex: %w", err)
	}
	encoded, err := encodeValue(value)
	if err != nil {
		return err
	}
	return invocation.outputEmitter.sendStreamWrite(&dexpb.StepStreamWrite{
		StreamName:          registered.definition.name,
		StreamCapacityBytes: registered.definition.streamCapacityBytes,
		Value:               encoded,
	})
}

func (invocation *invocationContext) RecordHeartbeat(value any) error {
	if invocation.method == invocationRPC || invocation.outputEmitter == nil {
		return errInvalidInvocationContext
	}
	heartbeat := &dexpb.StepMethodHeartbeat{}
	if value != nil && !isNilValue(reflect.ValueOf(value)) {
		encoded, err := encodeValue(value)
		if err != nil {
			return err
		}
		heartbeat.Value = encoded
	}
	return invocation.outputEmitter.sendHeartbeat(heartbeat)
}

func (invocation *invocationContext) GetLastHeartbeatValue(
	valuePtr any,
) (bool, error) {
	if err := invocation.requireActive(invocationWaitFor, invocationExecute); err != nil {
		return false, err
	}
	if _, err := decodeTarget(valuePtr); err != nil {
		return false, err
	}
	if invocation.lastHeartbeatValue == nil {
		return false, nil
	}
	if err := decodeValue(invocation.lastHeartbeatValue, valuePtr); err != nil {
		return false, err
	}
	return true, nil
}

func buildInvocationValues(
	kind string,
	values []*dexpb.KV,
) (map[string]*dexpb.Value, error) {
	mapped := make(map[string]*dexpb.Value, len(values))
	for index, value := range values {
		if value == nil || value.Key == "" || value.Value == nil {
			return nil, fmt.Errorf("dex: invalid %s at index %d", kind, index)
		}
		if _, found := mapped[value.Key]; found {
			return nil, fmt.Errorf("dex: duplicate %s key %q", kind, value.Key)
		}
		mapped[value.Key] = value.Value
	}
	return mapped, nil
}

func buildChannelSizes(
	infos map[string]*dexpb.ChannelInfo,
) (map[string]int, error) {
	sizes := make(map[string]int, len(infos))
	for name, info := range infos {
		if name == "" || info == nil || info.Size < 0 {
			return nil, fmt.Errorf("dex: invalid channel info %q", name)
		}
		sizes[name] = int(info.Size)
	}
	return sizes, nil
}

func stringSet(values []string) map[string]struct{} {
	mapped := make(map[string]struct{}, len(values))
	for _, value := range values {
		mapped[value] = struct{}{}
	}
	return mapped
}

func validateConditionResults(results *dexpb.ConditionResults) error {
	if results == nil {
		return nil
	}
	conditionIDs := make(map[string]struct{},
		len(results.TimerResults)+len(results.ChannelResults))
	for index, result := range results.TimerResults {
		if result == nil || !validConditionStatus(result.ConditionStatus) {
			return fmt.Errorf("dex: invalid timer result at index %d", index)
		}
		if result.ConditionId != "" {
			if _, found := conditionIDs[result.ConditionId]; found {
				return fmt.Errorf("dex: duplicate condition result %q", result.ConditionId)
			}
			conditionIDs[result.ConditionId] = struct{}{}
		}
	}
	for index, result := range results.ChannelResults {
		if result == nil || result.ChannelName == "" ||
			!validConditionStatus(result.ConditionStatus) {
			return fmt.Errorf("dex: invalid channel result at index %d", index)
		}
		if result.ConditionId != "" {
			if _, found := conditionIDs[result.ConditionId]; found {
				return fmt.Errorf("dex: duplicate condition result %q", result.ConditionId)
			}
			conditionIDs[result.ConditionId] = struct{}{}
		}
		if result.ConditionStatus == dexpb.ConditionStatus_CONDITION_STATUS_WAITING &&
			len(result.Values) != 0 {
			return fmt.Errorf("dex: waiting channel result %q has values", result.ConditionId)
		}
		for valueIndex, value := range result.Values {
			if err := validateConcreteValue(value); err != nil {
				return fmt.Errorf(
					"dex: channel result %q value %d: %w",
					result.ConditionId,
					valueIndex,
					err,
				)
			}
		}
	}
	for index, result := range results.SubFlowResults {
		if result == nil {
			return fmt.Errorf("dex: invalid SubFlow result at index %d", index)
		}
		if _, err := mapFlowResult(result); err != nil {
			return fmt.Errorf("dex: invalid SubFlow result at index %d: %w", index, err)
		}
	}
	return nil
}

func validConditionStatus(status dexpb.ConditionStatus) bool {
	return status == dexpb.ConditionStatus_CONDITION_STATUS_WAITING ||
		status == dexpb.ConditionStatus_CONDITION_STATUS_COMPLETED
}

func (invocation *invocationContext) FlowID() string {
	return invocation.flowID
}

func (invocation *invocationContext) RunID() string {
	return invocation.runID
}

func (invocation *invocationContext) FlowStartedAt() time.Time {
	return invocation.flowStartedAt
}

func (invocation *invocationContext) StepExecutionID() string {
	return invocation.stepExecutionID
}

func (invocation *invocationContext) FromStepExecutionID() string {
	return invocation.fromStepExecutionID
}

func (invocation *invocationContext) RecoveryError() *RecoveryErrorInfo {
	return invocation.recoveryError
}

func (invocation *invocationContext) FirstAttemptAt() time.Time {
	return invocation.firstAttemptAt
}

func (invocation *invocationContext) Attempt() int32 {
	return invocation.attempt
}

func (invocation *invocationContext) HasTimerFired() bool {
	if !invocation.active || invocation.method != invocationExecute ||
		invocation.conditionResults == nil {
		return false
	}
	for _, result := range invocation.conditionResults.TimerResults {
		if result.ConditionStatus == dexpb.ConditionStatus_CONDITION_STATUS_COMPLETED {
			return true
		}
	}
	return false
}

func (invocation *invocationContext) HasTimerFiredByIndex(index int) bool {
	if !invocation.active || invocation.method != invocationExecute ||
		invocation.conditionResults == nil ||
		index < 0 || index >= len(invocation.conditionResults.TimerResults) {
		return false
	}
	return invocation.conditionResults.TimerResults[index].ConditionStatus ==
		dexpb.ConditionStatus_CONDITION_STATUS_COMPLETED
}

func (invocation *invocationContext) WaitForMethodFailed() bool {
	return invocation.active && invocation.method == invocationExecute &&
		invocation.conditionResults != nil &&
		invocation.conditionResults.WaitForFailed
}

func (invocation *invocationContext) subFlowResult(index int) (FlowResult, error) {
	if !invocation.active || invocation.method != invocationExecute ||
		invocation.conditionResults == nil ||
		index < 0 || index >= len(invocation.conditionResults.SubFlowResults) {
		return FlowResult{}, fmt.Errorf("dex: SubFlow result index %d is unavailable", index)
	}
	return mapFlowResult(invocation.conditionResults.SubFlowResults[index])
}

func (invocation *invocationContext) SetStepExecutionLocal(
	key string,
	value any,
) error {
	if err := invocation.requireActive(invocationWaitFor); err != nil {
		return err
	}
	if key == "" {
		return fmt.Errorf("dex: step-execution local key must not be empty")
	}
	encoded, err := encodeValue(value)
	if err != nil {
		return err
	}
	if _, found := invocation.localWrites[key]; !found {
		invocation.localWriteOrder = append(invocation.localWriteOrder, key)
	}
	invocation.localWrites[key] = &dexpb.KV{Key: key, Value: encoded}
	return nil
}

func (invocation *invocationContext) GetStepExecutionLocal(
	key string,
	valuePtr any,
) (bool, error) {
	if err := invocation.requireActive(invocationExecute); err != nil {
		return false, err
	}
	if key == "" {
		return false, fmt.Errorf("dex: step-execution local key must not be empty")
	}
	target := reflect.ValueOf(valuePtr)
	if valuePtr == nil || target.Kind() != reflect.Pointer || target.IsNil() {
		return false, fmt.Errorf("dex: decode target must be a non-nil pointer")
	}
	value, found := invocation.locals[key]
	if !found {
		return false, nil
	}
	if err := decodeValue(value, valuePtr); err != nil {
		return false, err
	}
	return true, nil
}

func (invocation *invocationContext) RecordEvent(name string, value any) error {
	if err := invocation.requireActive(
		invocationWaitFor,
		invocationExecute,
		invocationRPC,
	); err != nil {
		return err
	}
	if name == "" {
		return fmt.Errorf("dex: event name must not be empty")
	}
	if _, found := invocation.events[name]; found {
		return fmt.Errorf("dex: event %q was already recorded", name)
	}
	encoded, err := encodeValue(value)
	if err != nil {
		return err
	}
	invocation.events[name] = struct{}{}
	invocation.recordedEvents = append(invocation.recordedEvents, &dexpb.KV{
		Key:   name,
		Value: encoded,
	})
	return nil
}

func (invocation *invocationContext) getAttribute(
	name string,
	valuePtr any,
) (bool, error) {
	return invocation.getAttributeValue(name, "", false, valuePtr)
}

func (invocation *invocationContext) setAttribute(
	name string,
	value any,
	_ *AttributeIndex,
) error {
	return invocation.setAttributeValue(name, "", false, value)
}

func (invocation *invocationContext) deleteAttribute(
	name string,
	_ *AttributeIndex,
) error {
	return invocation.deleteAttributeValue(name, "", false)
}

func (invocation *invocationContext) getAttributeMap(
	name string,
	instance string,
	valuePtr any,
) (bool, error) {
	return invocation.getAttributeValue(name, instance, true, valuePtr)
}

func (invocation *invocationContext) setAttributeMap(
	name string,
	instance string,
	value any,
	_ *AttributeIndex,
) error {
	return invocation.setAttributeValue(name, instance, true, value)
}

func (invocation *invocationContext) deleteAttributeMap(
	name string,
	instance string,
	_ *AttributeIndex,
) error {
	return invocation.deleteAttributeValue(name, instance, true)
}

func (invocation *invocationContext) getAttributeValue(
	name string,
	instance string,
	isMap bool,
	valuePtr any,
) (bool, error) {
	if err := invocation.requireActive(
		invocationWaitFor,
		invocationExecute,
		invocationRPC,
	); err != nil {
		return false, err
	}
	_, physical, err := invocation.resolveAttribute(name, instance, isMap)
	if err != nil {
		return false, err
	}
	if isMap && !isMapInstanceLoaded(
		invocation.loadedAttributeMapInstances,
		name,
		physical,
	) {
		return false, &AttributeMapNotLoadedError{Name: physical}
	}
	if write, found := invocation.attributeWrites[physical]; found {
		if _, deleted := write.Value.Kind.(*dexpb.Value_NullValue); deleted {
			return false, nil
		}
		if err := decodeValue(write.Value, valuePtr); err != nil {
			return false, err
		}
		return true, nil
	}
	value, found := invocation.attributes[physical]
	if !found {
		return false, nil
	}
	if err := decodeValue(value, valuePtr); err != nil {
		return false, err
	}
	return true, nil
}

func (invocation *invocationContext) setAttributeValue(
	name string,
	instance string,
	isMap bool,
	value any,
) error {
	if err := invocation.requireActive(
		invocationWaitFor,
		invocationExecute,
		invocationRPC,
	); err != nil {
		return err
	}
	attribute, physical, err := invocation.resolveAttribute(name, instance, isMap)
	if err != nil {
		return err
	}
	encoded, indexConfig, err := encodeAttributeValue(value, attribute.index)
	if err != nil {
		return err
	}
	if attribute.isHardDeleteOnNull {
		if err := validateHardDeleteAttributeValue(physical, encoded); err != nil {
			return err
		}
	}
	invocation.bufferAttributeWrite(&dexpb.AttributeWrite{
		Key:                physical,
		Value:              encoded,
		IndexConfig:        indexConfig,
		SyncConfig:         mapAttributeSyncConfig(attribute.syncToAttributeStore),
		IsHardDeleteOnNull: attribute.isHardDeleteOnNull,
	})
	return nil
}

func (invocation *invocationContext) deleteAttributeValue(
	name string,
	instance string,
	isMap bool,
) error {
	if err := invocation.requireActive(
		invocationWaitFor,
		invocationExecute,
		invocationRPC,
	); err != nil {
		return err
	}
	attribute, physical, err := invocation.resolveAttribute(name, instance, isMap)
	if err != nil {
		return err
	}
	write, err := mapAttributeDelete(
		physical,
		attribute.index,
		attribute.syncToAttributeStore,
		attribute.isHardDeleteOnNull,
	)
	if err != nil {
		return err
	}
	invocation.bufferAttributeWrite(write)
	return nil
}

func (invocation *invocationContext) attributeMapKeys(name string) []string {
	if err := invocation.requireActive(
		invocationWaitFor,
		invocationExecute,
		invocationRPC,
	); err != nil {
		panic(err)
	}
	if _, loaded := invocation.loadedAttributeMapInstances[name+"/"]; !loaded {
		panic(&AttributeMapNotLoadedError{Name: name})
	}
	attribute, found := invocation.flow.attributes[name]
	if !found || !attribute.isMap {
		panic(fmt.Errorf("dex: attribute %q is not a declared AttributeMap", name))
	}
	physicalKeys := make(map[string]struct{})
	for physical := range invocation.attributes {
		if strings.HasPrefix(physical, name+"/") {
			physicalKeys[physical] = struct{}{}
		}
	}
	for physical, write := range invocation.attributeWrites {
		if !strings.HasPrefix(physical, name+"/") {
			continue
		}
		if _, deleted := write.Value.Kind.(*dexpb.Value_NullValue); deleted {
			delete(physicalKeys, physical)
		} else {
			physicalKeys[physical] = struct{}{}
		}
	}
	return sortedInstanceKeys(name, physicalKeys)
}

func (invocation *invocationContext) resolveAttribute(
	name string,
	instance string,
	isMap bool,
) (registeredAttribute, string, error) {
	attribute, found := invocation.flow.attributes[name]
	if !found {
		return registeredAttribute{}, "", fmt.Errorf("dex: attribute %q is not declared", name)
	}
	if attribute.isMap != isMap {
		return registeredAttribute{}, "", fmt.Errorf(
			"dex: attribute %q static/map kind does not match",
			name,
		)
	}
	physical, err := physicalName(name, instance, isMap)
	return attribute, physical, err
}

func (invocation *invocationContext) bufferAttributeWrite(
	write *dexpb.AttributeWrite,
) {
	if _, found := invocation.attributeWrites[write.Key]; !found {
		invocation.attributeWriteOrder = append(invocation.attributeWriteOrder, write.Key)
	}
	invocation.attributeWrites[write.Key] = write
}

func (invocation *invocationContext) publishChannel(name string, value any) error {
	return invocation.publishChannelValue(name, "", false, value)
}

func (invocation *invocationContext) publishChannelMap(
	name string,
	instance string,
	value any,
) error {
	return invocation.publishChannelValue(name, instance, true, value)
}

func (invocation *invocationContext) publishChannelValue(
	name string,
	instance string,
	isMap bool,
	value any,
) error {
	if err := invocation.requireActive(
		invocationWaitFor,
		invocationExecute,
		invocationRPC,
	); err != nil {
		return err
	}
	physical, err := invocation.resolveChannel(name, instance, isMap)
	if err != nil {
		return err
	}
	encoded, err := encodeValue(value)
	if err != nil {
		return err
	}
	invocation.publications = append(invocation.publications, &dexpb.ChannelMessage{
		ChannelName: physical,
		Value:       encoded,
	})
	invocation.channelSizes[physical]++
	return nil
}

func (invocation *invocationContext) deleteChannelMessage(
	name string,
	instance string,
	isMap bool,
	messageID string,
) error {
	if err := invocation.requireActive(
		invocationWaitFor,
		invocationExecute,
		invocationRPC,
	); err != nil {
		return err
	}
	if messageID == "" {
		return fmt.Errorf("dex: Channel message ID must not be empty")
	}
	physical, err := invocation.resolveChannel(name, instance, isMap)
	if err != nil {
		return err
	}
	invocation.deletions = append(invocation.deletions, &dexpb.ChannelMessageDeletion{
		ChannelName: physical,
		MessageId:   messageID,
	})
	if invocation.channelSizes[physical] > 0 {
		invocation.channelSizes[physical]--
	}
	return nil
}

func (invocation *invocationContext) channelSize(name string) int {
	return invocation.channelSizeValue(name, "", false)
}

func (invocation *invocationContext) channelMapSize(name string, instance string) int {
	return invocation.channelSizeValue(name, instance, true)
}

func (invocation *invocationContext) channelMapKeys(name string) []string {
	if err := invocation.requireActive(
		invocationWaitFor,
		invocationExecute,
		invocationRPC,
	); err != nil {
		panic(err)
	}
	channel, found := invocation.flow.channels[name]
	if !found || !channel.isMap {
		panic(fmt.Errorf("dex: channel %q is not a declared ChannelMap", name))
	}
	physicalKeys := make(map[string]struct{})
	for physical, size := range invocation.channelSizes {
		if size > 0 && strings.HasPrefix(physical, name+"/") {
			physicalKeys[physical] = struct{}{}
		}
	}
	return sortedInstanceKeys(name, physicalKeys)
}

func (invocation *invocationContext) pendingChannelMessages(
	name string,
	instance string,
	isMap bool,
) ([]*dexpb.ChannelMessage, error) {
	if err := invocation.requireActive(
		invocationWaitFor,
		invocationExecute,
		invocationRPC,
	); err != nil {
		return nil, err
	}
	loadedNames := invocation.loadedChannelNames
	if isMap {
		loadedNames = invocation.loadedChannelMapInstances
	}
	physical, err := invocation.resolveChannel(name, instance, isMap)
	if err != nil {
		return nil, err
	}
	isLoaded := false
	if isMap {
		isLoaded = isMapInstanceLoaded(loadedNames, name, physical)
	} else {
		_, isLoaded = loadedNames[name]
	}
	if !isLoaded {
		return nil, &ChannelMessagesNotLoadedError{Name: physical}
	}
	values := invocation.loadedChannelMessages[physical]
	if values == nil {
		return nil, nil
	}
	return values.Messages, nil
}

func isMapInstanceLoaded(instances map[string]struct{}, name string, physical string) bool {
	if _, loaded := instances[name+"/"]; loaded {
		return true
	}
	_, loaded := instances[physical]
	return loaded
}

func sortedInstanceKeys(
	name string,
	physicalKeys map[string]struct{},
) []string {
	keys := make([]string, 0, len(physicalKeys))
	prefix := name + "/"
	for physical := range physicalKeys {
		instance, err := url.PathUnescape(strings.TrimPrefix(physical, prefix))
		if err != nil {
			panic(fmt.Errorf("dex: invalid map instance key %q: %w", physical, err))
		}
		if err := validateMapInstance(instance); err != nil {
			panic(fmt.Errorf("dex: invalid map instance key %q: %w", physical, err))
		}
		keys = append(keys, instance)
	}
	sort.Strings(keys)
	return keys
}

func (invocation *invocationContext) channelSizeValue(
	name string,
	instance string,
	isMap bool,
) int {
	if err := invocation.requireActive(invocationWaitFor, invocationExecute, invocationRPC); err != nil {
		panic(err)
	}
	physical, err := invocation.resolveChannel(name, instance, isMap)
	if err != nil {
		panic(err)
	}
	return invocation.channelSizes[physical]
}

func (invocation *invocationContext) getChannelResults(
	name string,
	instance string,
	isMap bool,
	resultsPtr any,
) error {
	if err := invocation.requireActive(invocationExecute); err != nil {
		return err
	}
	physical, err := invocation.resolveChannel(name, instance, isMap)
	if err != nil {
		return err
	}
	target, err := channelResultsTarget(resultsPtr)
	if err != nil {
		return err
	}
	result := reflect.MakeSlice(target.Type(), 0, 0)
	if invocation.conditionResults != nil {
		for _, channelResult := range invocation.conditionResults.ChannelResults {
			if channelResult.ChannelName != physical ||
				channelResult.ConditionStatus != dexpb.ConditionStatus_CONDITION_STATUS_COMPLETED {
				continue
			}
			for _, value := range channelResult.Values {
				decoded, decodeErr := decodeReflectValue(value, target.Type().Elem())
				if decodeErr != nil {
					return decodeErr
				}
				result = reflect.Append(result, decoded)
			}
		}
	}
	target.Set(result)
	return nil
}

func (invocation *invocationContext) resolveChannel(
	name string,
	instance string,
	isMap bool,
) (string, error) {
	channel, found := invocation.flow.channels[name]
	if !found {
		return "", fmt.Errorf("dex: channel %q is not declared", name)
	}
	if channel.isMap != isMap {
		return "", fmt.Errorf("dex: channel %q static/map kind does not match", name)
	}
	return physicalName(name, instance, isMap)
}

func channelResultsTarget(resultsPtr any) (reflect.Value, error) {
	if resultsPtr == nil {
		return reflect.Value{}, fmt.Errorf("dex: channel results target must be a non-nil pointer")
	}
	target := reflect.ValueOf(resultsPtr)
	if target.Kind() != reflect.Pointer || target.IsNil() ||
		target.Elem().Kind() != reflect.Slice {
		return reflect.Value{}, fmt.Errorf("dex: channel results target must point to a slice")
	}
	return target.Elem(), nil
}

func decodeReflectValue(
	value *dexpb.Value,
	valueType reflect.Type,
) (reflect.Value, error) {
	target := reflect.New(valueType)
	if err := decodeValue(value, target.Interface()); err != nil {
		return reflect.Value{}, err
	}
	return target.Elem(), nil
}

func (invocation *invocationContext) requireActive(
	allowed ...invocationMethod,
) error {
	if !invocation.active {
		return errInvalidInvocationContext
	}
	for _, method := range allowed {
		if invocation.method == method {
			return nil
		}
	}
	return errInvalidInvocationContext
}

func (invocation *invocationContext) finish() error {
	var finalErr error
	for _, finalizer := range invocation.outputFinalizers {
		finalErr = errors.Join(finalErr, finalizer.finalize())
	}
	invocation.active = false
	if invocation.outputEmitter == nil {
		return finalErr
	}
	return errors.Join(finalErr, invocation.outputEmitter.close())
}

func (invocation *invocationContext) registerOutputFinalizer(
	finalizer invocationOutputFinalizer,
) error {
	if finalizer == nil {
		panic("dex: invocation output finalizer is nil")
	}
	if err := invocation.requireActive(invocationWaitFor, invocationExecute); err != nil ||
		invocation.outputEmitter == nil {
		return errInvalidInvocationContext
	}
	invocation.outputFinalizers = append(invocation.outputFinalizers, finalizer)
	return nil
}

func (invocation *invocationContext) mappedAttributeWrites() []*dexpb.AttributeWrite {
	writes := make([]*dexpb.AttributeWrite, 0, len(invocation.attributeWriteOrder))
	for _, key := range invocation.attributeWriteOrder {
		writes = append(writes, invocation.attributeWrites[key])
	}
	return writes
}

func (invocation *invocationContext) mappedAttributeWritesWithActionPermissionMappings() (
	[]*dexpb.AttributeWrite,
	*dexpb.ActionPermissionMappings,
) {
	writes := invocation.mappedAttributeWrites()
	return writes, invocation.flow.actionPermissionMappingsForWrites(writes)
}

func (invocation *invocationContext) mappedLocalWrites() []*dexpb.KV {
	writes := make([]*dexpb.KV, 0, len(invocation.localWriteOrder))
	for _, key := range invocation.localWriteOrder {
		writes = append(writes, invocation.localWrites[key])
	}
	return writes
}
