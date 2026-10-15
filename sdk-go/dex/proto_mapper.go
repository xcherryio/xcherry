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
	"fmt"
	"math"
	"net/url"
	"strings"
	"time"

	"github.com/superdurable/dex/sdk-go/dex/ptr"
	"github.com/superdurable/dex/sdk-go/gen/dexpb"
)

func mapInitialAttributes(
	attributes []InitialAttributeDef,
) ([]*dexpb.AttributeWrite, error) {
	mapped := make([]*dexpb.AttributeWrite, 0, len(attributes))
	seen := make(map[string]struct{}, len(attributes))
	for _, attribute := range attributes {
		concrete, ok := attribute.(initialAttribute)
		if !ok {
			return nil, fmt.Errorf("dex: invalid initial attribute %T", attribute)
		}
		key, err := physicalName(
			concrete.name,
			concrete.instance,
			concrete.isMap,
		)
		if err != nil {
			return nil, err
		}
		if _, found := seen[key]; found {
			return nil, fmt.Errorf("dex: duplicate initial attribute %q", key)
		}
		seen[key] = struct{}{}
		if err := validateConcreteValue(concrete.encoded); err != nil {
			return nil, fmt.Errorf("dex: initial attribute %q: %w", key, err)
		}
		mapped = append(mapped, &dexpb.AttributeWrite{
			Key:         key,
			Value:       concrete.encoded,
			IndexConfig: concrete.indexConfig,
			SyncConfig:  mapAttributeSyncConfig(concrete.syncToAttributeStore),
		})
	}
	return mapped, nil
}

func mapAttributeDelete(
	name string,
	index *AttributeIndex,
	syncToAttributeStore bool,
) (*dexpb.AttributeWrite, error) {
	if name == "" {
		return nil, fmt.Errorf("dex: attribute delete name must not be empty")
	}
	value, indexConfig, err := newDeleteValue(index)
	if err != nil {
		return nil, err
	}
	return &dexpb.AttributeWrite{
		Key:         name,
		Value:       value,
		IndexConfig: indexConfig,
		SyncConfig:  mapAttributeSyncConfig(syncToAttributeStore),
	}, nil
}

func mapAttributeSyncConfig(enabled bool) *dexpb.AttributeSyncConfig {
	if !enabled {
		return nil
	}
	return &dexpb.AttributeSyncConfig{Enabled: true}
}

func mapStartFlowOptions(
	options StartFlowOptions,
) (int32, dexpb.FlowTimeoutPolicy, *dexpb.FlowStartOptions, error) {
	timeout, err := optionalDurationSeconds32(options.Timeout)
	if err != nil {
		return 0, dexpb.FlowTimeoutPolicy_FLOW_TIMEOUT_POLICY_UNSPECIFIED, nil, fmt.Errorf("dex: flow timeout: %w", err)
	}
	timeoutPolicy, err := mapFlowTimeoutPolicy(options.TimeoutPolicy)
	if err != nil {
		return 0, dexpb.FlowTimeoutPolicy_FLOW_TIMEOUT_POLICY_UNSPECIFIED, nil, err
	}
	startDelay, err := optionalDurationSeconds32(options.StartDelay)
	if err != nil {
		return 0, dexpb.FlowTimeoutPolicy_FLOW_TIMEOUT_POLICY_UNSPECIFIED, nil, fmt.Errorf("dex: start delay: %w", err)
	}
	idReuse, err := mapIDReusePolicy(options.IDReusePolicy)
	if err != nil {
		return 0, dexpb.FlowTimeoutPolicy_FLOW_TIMEOUT_POLICY_UNSPECIFIED, nil, err
	}
	retry, err := mapFlowRetryPolicy(options.RetryPolicy)
	if err != nil {
		return 0, dexpb.FlowTimeoutPolicy_FLOW_TIMEOUT_POLICY_UNSPECIFIED, nil, err
	}
	attributes, err := mapInitialAttributes(options.Attributes)
	if err != nil {
		return 0, dexpb.FlowTimeoutPolicy_FLOW_TIMEOUT_POLICY_UNSPECIFIED, nil, err
	}
	config, err := mapFlowConfig(options.ConfigOverride)
	if err != nil {
		return 0, dexpb.FlowTimeoutPolicy_FLOW_TIMEOUT_POLICY_UNSPECIFIED, nil, err
	}
	timeoutHandlerOptions, err := mapFlowTimeoutHandlerOptions(options.TimeoutHandlerOptions)
	if err != nil {
		return 0, dexpb.FlowTimeoutPolicy_FLOW_TIMEOUT_POLICY_UNSPECIFIED, nil, err
	}
	return timeout, timeoutPolicy, &dexpb.FlowStartOptions{
		IdReusePolicy:         idReuse,
		FlowStartDelaySeconds: startDelay,
		RetryPolicy:           retry,
		Attributes:            attributes,
		FlowConfigOverride:    config,
		TimeoutHandlerOptions: timeoutHandlerOptions,
		FlowAlreadyStartedOptions: mapAlreadyStartedOptions(
			options.AlreadyStarted,
		),
	}, nil
}

func mapFlowTimeoutPolicy(policy FlowTimeoutPolicy) (dexpb.FlowTimeoutPolicy, error) {
	switch policy {
	case TimeoutDefault:
		return dexpb.FlowTimeoutPolicy_FLOW_TIMEOUT_POLICY_UNSPECIFIED, nil
	case TimeoutFail:
		return dexpb.FlowTimeoutPolicy_FLOW_TIMEOUT_POLICY_FAIL, nil
	case TimeoutCancel:
		return dexpb.FlowTimeoutPolicy_FLOW_TIMEOUT_POLICY_CANCEL, nil
	case TimeoutHandler:
		return dexpb.FlowTimeoutPolicy_FLOW_TIMEOUT_POLICY_HANDLER, nil
	default:
		return dexpb.FlowTimeoutPolicy_FLOW_TIMEOUT_POLICY_UNSPECIFIED,
			fmt.Errorf("dex: invalid Flow timeout policy %d", policy)
	}
}

func mapAlreadyStartedOptions(
	options *AlreadyStartedOptions,
) *dexpb.FlowAlreadyStartedOptions {
	if options == nil {
		return nil
	}
	return &dexpb.FlowAlreadyStartedOptions{
		IgnoreAlreadyStartedError: options.IgnoreError,
	}
}

func mapStepOptions(options *StepOptions) (*dexpb.StepOptions, error) {
	return mapStepOptionsRecursive(options, make(map[*StepOptions]bool))
}

func mapStepOptionsRecursive(
	options *StepOptions,
	active map[*StepOptions]bool,
) (*dexpb.StepOptions, error) {
	if options == nil {
		return nil, nil
	}
	if active[options] {
		return nil, fmt.Errorf("dex: step options contain a cycle")
	}
	active[options] = true
	defer delete(active, options)

	waitForMethodTimeout, err := durationSeconds32(options.WaitForMethodTimeout)
	if err != nil {
		return nil, fmt.Errorf("dex: WaitFor method timeout: %w", err)
	}
	executeMethodTimeout, err := durationSeconds32(options.ExecuteMethodTimeout)
	if err != nil {
		return nil, fmt.Errorf("dex: Execute method timeout: %w", err)
	}
	heartbeatTimeout, err := exactDurationSeconds32(options.HeartbeatTimeout)
	if err != nil {
		return nil, fmt.Errorf("dex: heartbeat timeout: %w", err)
	}
	waitForRetry, err := mapRetryPolicy(options.WaitForRetry)
	if err != nil {
		return nil, fmt.Errorf("dex: WaitFor retry: %w", err)
	}
	executeRetry, err := mapRetryPolicy(options.ExecuteRetry)
	if err != nil {
		return nil, fmt.Errorf("dex: Execute retry: %w", err)
	}
	waitForFailure, err := mapWaitForFailurePolicy(options.WaitForFailure)
	if err != nil {
		return nil, err
	}
	waitForDurability, err := mapStepDurability(options.WaitForDurability)
	if err != nil {
		return nil, err
	}
	executeDurability, err := mapStepDurability(options.ExecuteDurability)
	if err != nil {
		return nil, err
	}
	executeFailurePolicy, executeFailureStep, executeFailureOptions, err :=
		mapExecuteFailure(options.ExecuteFailure, active)
	if err != nil {
		return nil, err
	}
	waitForLocks, err := mapAttributeLocks(options.WaitForLockAttributes)
	if err != nil {
		return nil, err
	}
	executeLocks, err := mapAttributeLocks(options.ExecuteLockAttributes)
	if err != nil {
		return nil, err
	}
	waitForAttributeMaps, waitForChannels, waitForChannelMaps, err := validateStateLoads(
		nil,
		waitForStepStateLoads(options),
	)
	if err != nil {
		return nil, fmt.Errorf("dex: WaitFor state load: %w", err)
	}
	executeAttributeMaps, executeChannels, executeChannelMaps, err := validateStateLoads(
		nil,
		executeStepStateLoads(options),
	)
	if err != nil {
		return nil, fmt.Errorf("dex: Execute state load: %w", err)
	}
	return &dexpb.StepOptions{
		WaitForTimeoutSeconds:            waitForMethodTimeout,
		ExecuteTimeoutSeconds:            executeMethodTimeout,
		HeartbeatTimeoutSeconds:          heartbeatTimeout,
		WaitForRetryPolicy:               waitForRetry,
		ExecuteRetryPolicy:               executeRetry,
		WaitForFailurePolicy:             waitForFailure,
		ExecuteFailurePolicy:             executeFailurePolicy,
		ExecuteFailureProceedStepType:    executeFailureStep,
		ExecuteFailureProceedStepOptions: executeFailureOptions,
		WaitForDurabilityOverride:        waitForDurability,
		ExecuteDurabilityOverride:        executeDurability,
		WaitForLockAttributeKeys:         waitForLocks,
		ExecuteLockAttributeKeys:         executeLocks,
		WaitForLoadAttributeMapInstances: waitForAttributeMaps,
		WaitForLoadChannelNames:          waitForChannels,
		WaitForLoadChannelMapInstances:   waitForChannelMaps,
		ExecuteLoadAttributeMapInstances: executeAttributeMaps,
		ExecuteLoadChannelNames:          executeChannels,
		ExecuteLoadChannelMapInstances:   executeChannelMaps,
	}, nil
}

func waitForStepStateLoads(options *StepOptions) stateLoads {
	return stateLoads{
		attributeMaps:         options.WaitForLoadAttributeMaps,
		attributeMapInstances: options.WaitForLoadAttributeMapInstances,
		channels:              options.WaitForLoadChannels,
		channelMaps:           options.WaitForLoadChannelMaps,
		channelMapInstances:   options.WaitForLoadChannelMapInstances,
	}
}

func executeStepStateLoads(options *StepOptions) stateLoads {
	return stateLoads{
		attributeMaps:         options.ExecuteLoadAttributeMaps,
		attributeMapInstances: options.ExecuteLoadAttributeMapInstances,
		channels:              options.ExecuteLoadChannels,
		channelMaps:           options.ExecuteLoadChannelMaps,
		channelMapInstances:   options.ExecuteLoadChannelMapInstances,
	}
}

func timeoutHandlerStateLoads(options *FlowTimeoutHandlerOptions) stateLoads {
	return stateLoads{
		attributeMaps:         options.LoadAttributeMaps,
		attributeMapInstances: options.LoadAttributeMapInstances,
		channels:              options.LoadChannels,
		channelMaps:           options.LoadChannelMaps,
		channelMapInstances:   options.LoadChannelMapInstances,
	}
}

func mapFlowTimeoutHandlerOptions(
	options *FlowTimeoutHandlerOptions,
) (*dexpb.FlowTimeoutHandlerOptions, error) {
	if options == nil {
		return nil, nil
	}
	methodTimeout, err := durationSeconds32(options.MethodTimeout)
	if err != nil {
		return nil, fmt.Errorf("dex: timeout handler method timeout: %w", err)
	}
	heartbeatTimeout, err := exactDurationSeconds32(options.HeartbeatTimeout)
	if err != nil {
		return nil, fmt.Errorf("dex: timeout handler heartbeat timeout: %w", err)
	}
	retry, err := mapRetryPolicy(options.Retry)
	if err != nil {
		return nil, fmt.Errorf("dex: timeout handler retry: %w", err)
	}
	durability, err := mapStepDurability(options.Durability)
	if err != nil {
		return nil, err
	}
	locks, err := mapAttributeLocks(options.LockAttributes)
	if err != nil {
		return nil, err
	}
	attributeMaps, channels, channelMaps, err := validateStateLoads(
		nil,
		timeoutHandlerStateLoads(options),
	)
	if err != nil {
		return nil, fmt.Errorf("dex: timeout handler state load: %w", err)
	}
	failurePolicy, failureStep, failureOptions, err := mapFlowTimeoutHandlerFailure(options.Failure)
	if err != nil {
		return nil, err
	}
	return &dexpb.FlowTimeoutHandlerOptions{
		MethodTimeoutSeconds:      methodTimeout,
		HeartbeatTimeoutSeconds:   heartbeatTimeout,
		RetryPolicy:               retry,
		FailurePolicy:             failurePolicy,
		FailureProceedStepType:    failureStep,
		FailureProceedStepOptions: failureOptions,
		DurabilityOverride:        durability,
		LockAttributeKeys:         locks,
		LoadAttributeMapInstances: attributeMaps,
		LoadChannelNames:          channels,
		LoadChannelMapInstances:   channelMaps,
	}, nil
}

func mapFlowTimeoutHandlerFailure(
	failure *FlowTimeoutHandlerFailure,
) (dexpb.ExecuteMethodFailurePolicy, string, *dexpb.StepOptions, error) {
	if failure == nil {
		return dexpb.ExecuteMethodFailurePolicy_EXECUTE_METHOD_FAILURE_POLICY_UNSPECIFIED,
			"", nil, nil
	}
	if !validStepDef(failure.step) {
		return dexpb.ExecuteMethodFailurePolicy_EXECUTE_METHOD_FAILURE_POLICY_UNSPECIFIED,
			"", nil, fmt.Errorf("dex: timeout handler failure target is invalid")
	}
	options, err := mapStepOptions(mergeStepOptions(failure.step.stepOptions(), failure.options))
	if err != nil {
		return dexpb.ExecuteMethodFailurePolicy_EXECUTE_METHOD_FAILURE_POLICY_UNSPECIFIED,
			"", nil, err
	}
	if failure.step.skipWaitFor() {
		if options == nil {
			options = &dexpb.StepOptions{}
		}
		options.SkipWaitFor = true
	}
	return dexpb.ExecuteMethodFailurePolicy_EXECUTE_METHOD_FAILURE_POLICY_PROCEED_TO_CONFIGURED_STEP,
		failure.step.stepType(), options, nil
}

func mapExecuteFailure(
	failure *ExecuteFailure,
	active map[*StepOptions]bool,
) (
	dexpb.ExecuteMethodFailurePolicy,
	string,
	*dexpb.StepOptions,
	error,
) {
	if failure == nil {
		return dexpb.ExecuteMethodFailurePolicy_EXECUTE_METHOD_FAILURE_POLICY_UNSPECIFIED,
			"", nil, nil
	}
	if !validStepDef(failure.step) {
		return dexpb.ExecuteMethodFailurePolicy_EXECUTE_METHOD_FAILURE_POLICY_UNSPECIFIED,
			"", nil, fmt.Errorf("dex: Execute failure target is invalid")
	}
	options, err := mapStepOptionsRecursive(
		mergeStepOptions(failure.step.stepOptions(), failure.options),
		active,
	)
	if err != nil {
		return dexpb.ExecuteMethodFailurePolicy_EXECUTE_METHOD_FAILURE_POLICY_UNSPECIFIED,
			"", nil, err
	}
	if failure.step.skipWaitFor() {
		if options == nil {
			options = &dexpb.StepOptions{}
		}
		options.SkipWaitFor = true
	}
	return dexpb.ExecuteMethodFailurePolicy_EXECUTE_METHOD_FAILURE_POLICY_PROCEED_TO_CONFIGURED_STEP,
		failure.step.stepType(), options, nil
}

func mapRetryPolicy(policy *RetryPolicy) (*dexpb.RetryPolicy, error) {
	if policy == nil {
		return nil, nil
	}
	initial, err := durationSeconds32(policy.InitialInterval)
	if err != nil {
		return nil, err
	}
	maximum, err := durationSeconds32(policy.MaximumInterval)
	if err != nil {
		return nil, err
	}
	total, err := durationSeconds32(policy.TotalDuration)
	if err != nil {
		return nil, err
	}
	backoff, err := float32Value(policy.BackoffCoefficient)
	if err != nil {
		return nil, err
	}
	return &dexpb.RetryPolicy{
		InitialIntervalSeconds: initial,
		BackoffCoefficient:     backoff,
		MaximumIntervalSeconds: maximum,
		MaximumAttempts:        policy.MaximumAttempts,
		TotalDurationSeconds:   total,
	}, nil
}

func mapFlowRetryPolicy(
	policy *FlowRetryPolicy,
) (*dexpb.FlowRetryPolicy, error) {
	if policy == nil {
		return nil, nil
	}
	initial, err := durationSeconds32(policy.InitialInterval)
	if err != nil {
		return nil, err
	}
	maximum, err := durationSeconds32(policy.MaximumInterval)
	if err != nil {
		return nil, err
	}
	backoff, err := float32Value(policy.BackoffCoefficient)
	if err != nil {
		return nil, err
	}
	return &dexpb.FlowRetryPolicy{
		InitialIntervalSeconds: initial,
		BackoffCoefficient:     backoff,
		MaximumIntervalSeconds: maximum,
		MaximumAttempts:        policy.MaximumAttempts,
	}, nil
}

func mapFlowConfig(config *FlowConfig) (*dexpb.FlowConfig, error) {
	if config == nil {
		return nil, nil
	}
	if config.WorkerTarget != nil {
		if err := validateWorkerTarget(*config.WorkerTarget); err != nil {
			return nil, fmt.Errorf("dex: invalid Worker target: %w", err)
		}
	}
	searchMode, err := mapOptionalActiveStepSearchMode(config.ActiveStepSearchMode)
	if err != nil {
		return nil, err
	}
	durability, err := mapOptionalStepDurability(config.StepDurability)
	if err != nil {
		return nil, err
	}
	var attributeStoreNames *dexpb.AttributeStoreNames
	if config.AttributeStoreNames != nil {
		attributeStoreNames = &dexpb.AttributeStoreNames{Names: config.AttributeStoreNames}
	}
	return &dexpb.FlowConfig{
		ActiveStepSearchMode:         searchMode,
		ContinueAsNewThreshold:       config.ContinueAsNewThreshold,
		ContinueAsNewPageSizeInBytes: config.ContinueAsNewPageSizeBytes,
		StepDurability:               durability,
		WorkerTarget:                 mapWorkerTarget(config.WorkerTarget),
		AttributeStoreNames:          attributeStoreNames,
	}, nil
}

func mapWorkerTarget(target *WorkerTarget) *dexpb.WorkerTarget {
	if target == nil {
		return nil
	}
	return &dexpb.WorkerTarget{
		Address:           target.Address,
		IsHeadlessAddress: target.Headless,
		IsPullMode:        target.Pull,
	}
}

func mapWait(wait *Wait) (*dexpb.WaitingCondition, error) {
	return mapWaitWithRegistry(nil, nil, wait)
}

func mapWaitWithRegistry(
	registry *Registry,
	flow *registeredFlow,
	wait *Wait,
) (*dexpb.WaitingCondition, error) {
	if wait == nil {
		return nil, fmt.Errorf("dex: wait must not be nil")
	}
	switch wait.kind {
	case skipWaitImmediately:
		return nil, nil
	case waitAllOf:
		return mapFlatWait(
			registry,
			flow,
			dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ALL_COMPLETED,
			wait.conditions,
		)
	case waitAnyOf:
		return mapFlatWait(
			registry,
			flow,
			dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ANY_COMPLETED,
			wait.conditions,
		)
	case waitAnyComboOf:
		return mapCombinationWait(registry, flow, wait.combinations)
	default:
		return nil, fmt.Errorf("dex: invalid wait")
	}
}

func mapFlatWait(
	registry *Registry,
	flow *registeredFlow,
	waitType dexpb.WaitingConditionType,
	conditions []Condition,
) (*dexpb.WaitingCondition, error) {
	if len(conditions) == 0 {
		return nil, fmt.Errorf("dex: wait requires at least one condition")
	}
	mapper := newConditionMapper(registry, flow)
	for _, condition := range conditions {
		if _, err := mapper.add(condition, false); err != nil {
			return nil, err
		}
	}
	mapped := mapper.result(waitType)
	return mapped, nil
}

func mapCombinationWait(
	registry *Registry,
	flow *registeredFlow,
	combinations []ConditionCombination,
) (*dexpb.WaitingCondition, error) {
	if len(combinations) == 0 {
		return nil, fmt.Errorf("dex: AnyComboOf requires at least one combination")
	}
	mapper := newConditionMapper(registry, flow)
	mappedCombinations := make(
		[]*dexpb.ConditionCombination,
		0,
		len(combinations),
	)
	for _, combination := range combinations {
		if len(combination.conditions) == 0 {
			return nil, fmt.Errorf("dex: condition combination must not be empty")
		}
		ids := make([]string, 0, len(combination.conditions))
		for _, condition := range combination.conditions {
			id, err := mapper.add(condition, true)
			if err != nil {
				return nil, err
			}
			ids = append(ids, id)
		}
		mappedCombinations = append(mappedCombinations, &dexpb.ConditionCombination{
			ConditionIds: ids,
		})
	}
	mapped := mapper.result(
		dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ANY_COMBINATION_COMPLETED,
	)
	mapped.ConditionCombinations = mappedCombinations
	return mapped, nil
}

type conditionMapper struct {
	registry *Registry
	flow     *registeredFlow
	ids      map[*conditionImpl]string
	usedIDs  map[string]struct{}
	timers   []*dexpb.TimerCondition
	channels []*dexpb.ChannelCondition
	subFlows []*dexpb.SubFlowCondition
}

func newConditionMapper(registry *Registry, flow *registeredFlow) *conditionMapper {
	return &conditionMapper{
		registry: registry,
		flow:     flow,
		ids:      make(map[*conditionImpl]string),
		usedIDs:  make(map[string]struct{}),
	}
}

func (mapper *conditionMapper) add(
	condition Condition,
	idRequired bool,
) (string, error) {
	concrete, ok := condition.(*conditionImpl)
	if !ok || concrete == nil {
		return "", fmt.Errorf("dex: invalid condition %T", condition)
	}
	if id, found := mapper.ids[concrete]; found {
		return id, nil
	}
	if concrete.err != nil {
		return "", concrete.err
	}
	id, err := mapper.assignID(concrete.conditionID, concrete.idSet, idRequired)
	if err != nil {
		return "", err
	}
	switch concrete.kind {
	case conditionTimer:
		duration, durationErr := durationSeconds64(concrete.duration)
		if durationErr != nil {
			return "", durationErr
		}
		mapper.timers = append(mapper.timers, &dexpb.TimerCondition{
			ConditionId:     id,
			DurationSeconds: duration,
		})
	case conditionChannel:
		name, nameErr := physicalName(
			concrete.channelName,
			concrete.instance,
			concrete.isMap,
		)
		if nameErr != nil {
			return "", nameErr
		}
		atLeast, boundsErr := optionalInt32(concrete.atLeast)
		if boundsErr != nil {
			return "", boundsErr
		}
		atMost, boundsErr := optionalInt32(concrete.atMost)
		if boundsErr != nil {
			return "", boundsErr
		}
		mapper.channels = append(mapper.channels, &dexpb.ChannelCondition{
			ConditionId: id,
			ChannelName: name,
			AtLeast:     atLeast,
			AtMost:      atMost,
		})
	case conditionSubFlow:
		if mapper.registry == nil || mapper.flow == nil {
			return "", fmt.Errorf("dex: SubFlow requires a registered Worker Flow")
		}
		target, targetErr := flowRegistryTarget(concrete, mapper.registry)
		if targetErr != nil {
			return "", targetErr
		}
		input, inputErr := encodeValue(concrete.subFlowInput)
		if inputErr != nil {
			return "", inputErr
		}
		stepOptions, optionsErr := mapRegisteredStepOptions(target.startingStep, nil)
		if optionsErr != nil {
			return "", optionsErr
		}
		options, optionsErr := mapSubFlowOptions(target, concrete.subFlowOptions)
		if optionsErr != nil {
			return "", optionsErr
		}
		mapper.subFlows = append(mapper.subFlows, &dexpb.SubFlowCondition{
			ConditionId:   id,
			SubFlowType:   target.flowType,
			StartStepType: target.startingStep.stepType,
			StepInput:     input,
			StepOptions:   stepOptions,
			Options:       options,
			SubFlowIndex:  int32(len(mapper.subFlows)),
		})
	default:
		return "", fmt.Errorf("dex: unsupported condition kind %d", concrete.kind)
	}
	mapper.ids[concrete] = id
	return id, nil
}

func (mapper *conditionMapper) assignID(
	explicit string,
	explicitlySet bool,
	required bool,
) (string, error) {
	if explicitlySet && explicit == "" {
		return "", fmt.Errorf("dex: explicit condition ID must not be empty")
	}
	if required && explicit == "" {
		return "", fmt.Errorf("dex: AnyComboOf requires every condition to have an ID")
	}
	if explicit == "" {
		return "", nil
	}
	if _, found := mapper.usedIDs[explicit]; found {
		return "", fmt.Errorf("dex: duplicate condition ID %q", explicit)
	}
	mapper.usedIDs[explicit] = struct{}{}
	return explicit, nil
}

func (mapper *conditionMapper) result(
	waitType dexpb.WaitingConditionType,
) *dexpb.WaitingCondition {
	return &dexpb.WaitingCondition{
		WaitingConditionType: waitType,
		TimerConditions:      mapper.timers,
		ChannelConditions:    mapper.channels,
		SubFlowConditions:    mapper.subFlows,
	}
}

func mapSubFlowOptions(
	target *registeredFlow,
	options SubFlowOptions,
) (*dexpb.SubFlowOptions, error) {
	timeoutPolicy, err := resolveFlowTimeoutPolicy(
		target,
		options.Timeout,
		options.TimeoutPolicy,
	)
	if err != nil {
		return nil, err
	}
	if err := target.validateFlowTimeoutHandlerOptions(
		options.Timeout,
		timeoutPolicy,
		options.TimeoutHandlerOptions,
	); err != nil {
		return nil, err
	}
	timeout, err := optionalDurationSeconds32(options.Timeout)
	if err != nil {
		return nil, fmt.Errorf("dex: SubFlow timeout: %w", err)
	}
	startDelay, err := optionalDurationSeconds32(options.StartDelay)
	if err != nil {
		return nil, fmt.Errorf("dex: SubFlow start delay: %w", err)
	}
	retry, err := mapFlowRetryPolicy(options.RetryPolicy)
	if err != nil {
		return nil, err
	}
	attributes, err := mapInitialAttributes(options.Attributes)
	if err != nil {
		return nil, err
	}
	if err := validateSubFlowAttributes(target, options.Attributes); err != nil {
		return nil, err
	}
	attributes, err = target.appendInitialWorkQueuePermissions(attributes)
	if err != nil {
		return nil, err
	}
	config, err := mapFlowConfig(options.ConfigOverride)
	if err != nil {
		return nil, err
	}
	reusePolicy, err := mapSubFlowReusePolicy(options.ReusePolicy)
	if err != nil {
		return nil, err
	}
	mappedTimeoutPolicy, err := mapFlowTimeoutPolicy(timeoutPolicy)
	if err != nil {
		return nil, err
	}
	timeoutHandlerOptions, err := mapFlowTimeoutHandlerOptions(options.TimeoutHandlerOptions)
	if err != nil {
		return nil, err
	}
	return &dexpb.SubFlowOptions{
		ReusePolicy:           reusePolicy,
		FlowTimeoutSeconds:    timeout,
		FlowTimeoutPolicy:     mappedTimeoutPolicy,
		FlowStartDelaySeconds: startDelay,
		RetryPolicy:           retry,
		Attributes:            attributes,
		FlowConfigOverride:    config,
		TimeoutHandlerOptions: timeoutHandlerOptions,
	}, nil
}

func validateSubFlowAttributes(target *registeredFlow, attributes []InitialAttributeDef) error {
	for _, definition := range attributes {
		attribute, ok := definition.(initialAttribute)
		if !ok {
			return fmt.Errorf("dex: invalid SubFlow initial attribute %T", definition)
		}
		registered, found := target.attributes[attribute.name]
		if !found || registered.isMap != attribute.isMap {
			return fmt.Errorf(
				"dex: SubFlow attribute %q is not registered by %q",
				attribute.name,
				target.flowType,
			)
		}
	}
	return nil
}

func mapSubFlowReusePolicy(policy SubFlowReusePolicy) (dexpb.SubFlowReusePolicy, error) {
	switch policy {
	case RestartSubFlowIfPreviousExitedAbnormally:
		return dexpb.SubFlowReusePolicy_SUB_FLOW_REUSE_POLICY_RESTART_IF_PREVIOUS_EXITS_ABNORMALLY, nil
	case AttachSubFlow:
		return dexpb.SubFlowReusePolicy_SUB_FLOW_REUSE_POLICY_ATTACH, nil
	case AlwaysRestartSubFlow:
		return dexpb.SubFlowReusePolicy_SUB_FLOW_REUSE_POLICY_ALWAYS_RESTART, nil
	default:
		return dexpb.SubFlowReusePolicy_SUB_FLOW_REUSE_POLICY_UNSPECIFIED,
			fmt.Errorf("dex: invalid SubFlow reuse policy %d", policy)
	}
}

func mapStepDecision(decision *StepDecision) (*dexpb.StepDecision, error) {
	if decision == nil {
		return nil, fmt.Errorf("dex: step decision must not be nil")
	}
	switch decision.kind {
	case decisionNext:
		if len(decision.movements) == 0 {
			return nil, fmt.Errorf("dex: GoToMany requires at least one movement")
		}
		movements, err := mapStepMovements(decision.movements)
		if err != nil {
			return nil, err
		}
		return &dexpb.StepDecision{NextSteps: movements}, nil
	case decisionClose:
		if len(decision.movements) != 0 {
			return nil, fmt.Errorf("dex: close decision cannot have next steps")
		}
		closeDecision, err := mapCloseDecision(decision.close, false)
		if err != nil {
			return nil, err
		}
		return &dexpb.StepDecision{CloseDecision: closeDecision}, nil
	case decisionConditionalClose:
		if len(decision.movements) == 0 {
			return nil, fmt.Errorf("dex: conditional close requires fallback movements")
		}
		movements, err := mapStepMovements(decision.movements)
		if err != nil {
			return nil, err
		}
		closeDecision, err := mapCloseDecision(decision.close, true)
		if err != nil {
			return nil, err
		}
		return &dexpb.StepDecision{
			NextSteps:     movements,
			CloseDecision: closeDecision,
		}, nil
	default:
		return nil, fmt.Errorf("dex: Execute returned an empty decision")
	}
}

func mapStepMovements(
	movements []StepMovement,
) ([]*dexpb.StepMovement, error) {
	mapped := make([]*dexpb.StepMovement, 0, len(movements))
	for _, movement := range movements {
		value, err := mapStepMovement(movement)
		if err != nil {
			return nil, err
		}
		mapped = append(mapped, value)
	}
	return mapped, nil
}

func mapStepMovement(movement StepMovement) (*dexpb.StepMovement, error) {
	if !validStepDef(movement.step) {
		return nil, fmt.Errorf("dex: movement target is invalid")
	}
	input, err := encodeValue(movement.input)
	if err != nil {
		return nil, err
	}
	options, err := mapStepOptions(
		mergeStepOptions(movement.step.stepOptions(), movement.options),
	)
	if err != nil {
		return nil, err
	}
	return &dexpb.StepMovement{
		StepType:    movement.step.stepType(),
		StepInput:   input,
		StepOptions: options,
	}, nil
}

func mapCloseDecision(
	decision CloseDecision,
	conditional bool,
) (*dexpb.CloseDecision, error) {
	switch decision.kind {
	case closeGracefulComplete:
		return mapCloseWithInput(
			dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_GRACEFUL_COMPLETE,
			decision.output,
		)
	case closeForceComplete:
		return mapCloseWithInput(
			dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_FORCE_COMPLETE,
			decision.output,
		)
	case closeForceFail:
		return mapCloseWithInput(
			dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_FORCE_FAIL,
			decision.reason,
		)
	case closeDeadEnd:
		return &dexpb.CloseDecision{
			CloseDecisionType: dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_DEAD_END,
		}, nil
	case closeConditionalForceComplete:
		if !conditional {
			return nil, fmt.Errorf("dex: conditional close has invalid decision kind")
		}
		channels, err := mapUniqueChannels(decision.channels)
		if err != nil {
			return nil, err
		}
		closeDecision, err := mapCloseWithInput(
			dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_FORCE_COMPLETE_ON_CHANNELS_EMPTY,
			decision.output,
		)
		if err != nil {
			return nil, err
		}
		closeDecision.ConditionalChannelNames = channels
		return closeDecision, nil
	default:
		return nil, fmt.Errorf("dex: invalid close decision")
	}
}

func mapCloseWithInput(
	closeType dexpb.CloseDecisionType,
	input any,
) (*dexpb.CloseDecision, error) {
	value, err := encodeValue(input)
	if err != nil {
		return nil, err
	}
	return &dexpb.CloseDecision{
		CloseDecisionType: closeType,
		CloseInput:        value,
	}, nil
}

func mapUniqueChannels(channels []ChannelDef) ([]string, error) {
	if len(channels) == 0 {
		return nil, fmt.Errorf("dex: conditional close requires channels")
	}
	mapped := make([]string, 0, len(channels))
	seen := make(map[string]struct{}, len(channels))
	for _, channel := range channels {
		if channel == nil || channel.channelName() == "" {
			return nil, fmt.Errorf("dex: conditional close channel is invalid")
		}
		name := channel.channelName()
		if _, found := seen[name]; found {
			return nil, fmt.Errorf("dex: duplicate conditional close channel %q", name)
		}
		seen[name] = struct{}{}
		mapped = append(mapped, name)
	}
	return mapped, nil
}

func mapRPCResult[OUT any](result *RPCResult[OUT]) (*dexpb.InvokeWorkerRPCResponse, error) {
	if result == nil {
		return nil, fmt.Errorf("dex: RPC returned nil")
	}
	output, err := encodeValue(result.Output)
	if err != nil {
		return nil, err
	}
	response := &dexpb.InvokeWorkerRPCResponse{Output: output}
	if len(result.NextSteps) == 0 {
		return response, nil
	}
	nextSteps, err := mapStepMovements(result.NextSteps)
	if err != nil {
		return nil, err
	}
	response.StepDecision = &dexpb.StepDecision{NextSteps: nextSteps}
	return response, nil
}

func mapFlowResult(
	response *dexpb.FlowResult,
) (FlowResult, error) {
	if response == nil {
		return FlowResult{}, fmt.Errorf("dex: Flow result is nil")
	}
	status, err := mapFlowStatus(response.FlowStatus)
	if err != nil {
		return FlowResult{}, err
	}
	errorType, err := mapFlowErrorType(response.ErrorType)
	if err != nil {
		return FlowResult{}, err
	}
	completions := make([]StepCompletion, 0, len(response.Results))
	for _, completion := range response.Results {
		if completion == nil {
			return FlowResult{}, fmt.Errorf("dex: step completion is nil")
		}
		output, valueErr := newValue(completion.CompletedStepOutput)
		if valueErr != nil {
			return FlowResult{}, valueErr
		}
		completions = append(completions, StepCompletion{
			StepType:        completion.CompletedStepType,
			StepExecutionID: completion.CompletedStepExecutionId,
			Output:          output,
		})
	}
	return FlowResult{
		Status:       status,
		Completions:  completions,
		ErrorType:    errorType,
		ErrorMessage: response.ErrorMessage,
	}, nil
}

func mapSearchFlowsPage(
	response *dexpb.SearchFlowsResponse,
) (SearchFlowsPage, error) {
	if response == nil {
		return SearchFlowsPage{}, fmt.Errorf("dex: SearchFlows response is nil")
	}
	flows := make([]SearchFlowEntry, 0, len(response.FlowRuns))
	for _, flow := range response.FlowRuns {
		if flow == nil {
			return SearchFlowsPage{}, fmt.Errorf("dex: search flow entry is nil")
		}
		if flow.FlowId == "" || flow.RunId == "" || flow.FlowType == "" {
			return SearchFlowsPage{}, fmt.Errorf("dex: search flow entry is incomplete")
		}
		status, err := mapFlowStatus(flow.FlowStatus)
		if err != nil {
			return SearchFlowsPage{}, err
		}
		if flow.StartTime == nil {
			return SearchFlowsPage{}, fmt.Errorf("dex: search flow start time is missing")
		}
		if err := flow.StartTime.CheckValid(); err != nil {
			return SearchFlowsPage{}, fmt.Errorf("dex: invalid search flow start time: %w", err)
		}
		closedAt := time.Time{}
		if flow.CloseTime != nil {
			if err := flow.CloseTime.CheckValid(); err != nil {
				return SearchFlowsPage{}, fmt.Errorf("dex: invalid search flow close time: %w", err)
			}
			closedAt = flow.CloseTime.AsTime()
		}
		attributes, err := mapValues(flow.IndexedAttributes)
		if err != nil {
			return SearchFlowsPage{}, err
		}
		flows = append(flows, SearchFlowEntry{
			FlowID:            flow.FlowId,
			RunID:             flow.RunId,
			FlowType:          flow.FlowType,
			Status:            status,
			StartedAt:         flow.StartTime.AsTime(),
			ClosedAt:          closedAt,
			IndexedAttributes: attributes,
		})
	}
	return SearchFlowsPage{
		Flows:         flows,
		NextPageToken: response.NextPageToken,
	}, nil
}

func mapHealthInfo(info *dexpb.HealthInfo) (HealthInfo, error) {
	if info == nil {
		return HealthInfo{}, fmt.Errorf("dex: health info is nil")
	}
	return HealthInfo{
		Condition: info.Condition,
		Hostname:  info.Hostname,
		Duration:  info.Duration,
	}, nil
}

func mapStopOptions(options StopOptions) (dexpb.StopType, string, error) {
	var stopType dexpb.StopType
	switch options.Type {
	case 0, CancelFlow:
		stopType = dexpb.StopType_STOP_TYPE_CANCEL
	case TerminateFlow:
		stopType = dexpb.StopType_STOP_TYPE_TERMINATE
	case FailFlow:
		stopType = dexpb.StopType_STOP_TYPE_FAIL
	default:
		return dexpb.StopType_STOP_TYPE_UNSPECIFIED, "",
			fmt.Errorf("dex: unsupported stop type %d", options.Type)
	}
	return stopType, options.Reason, nil
}

func mapTimeTravelOptions(options TimeTravelOptions) (*dexpb.ResetFlowRequest, error) {
	if err := validateTimeTravelOptions(options); err != nil {
		return nil, err
	}
	resetType, err := mapTimeTravelType(options.Type)
	if err != nil {
		return nil, err
	}
	stepMethod, err := mapTimeTravelStepMethod(options.StepMethod)
	if err != nil {
		return nil, err
	}
	historyTime := ""
	if !options.HistoryEventTime.IsZero() {
		historyTime = options.HistoryEventTime.Format(dateTimeFormat)
	}
	return &dexpb.ResetFlowRequest{
		ResetType:         resetType,
		Reason:            options.Reason,
		HistoryEventTime:  historyTime,
		StepType:          options.StepType,
		StepExecutionId:   options.StepExecutionID,
		SkipWritesReapply: options.SkipWritesReapply,
		StepMethod:        stepMethod,
	}, nil
}

func validateTimeTravelOptions(options TimeTravelOptions) error {
	hasHistoryEventTime := !options.HistoryEventTime.IsZero()
	hasStepType := options.StepType != ""
	hasStepExecutionID := options.StepExecutionID != ""
	hasStepMethod := options.StepMethod != 0
	switch options.Type {
	case TimeTravelToBeginning:
		if hasHistoryEventTime || hasStepType || hasStepExecutionID || hasStepMethod {
			return fmt.Errorf("dex: time travel to beginning cannot include another selector")
		}
	case TimeTravelByHistoryEventTime:
		if !hasHistoryEventTime {
			return fmt.Errorf("dex: time travel history event time must not be zero")
		}
		if hasStepType || hasStepExecutionID || hasStepMethod {
			return fmt.Errorf("dex: time travel history event time cannot be combined with another selector")
		}
	case TimeTravelByStepType:
		if !hasStepType {
			return fmt.Errorf("dex: time travel Step type must not be empty")
		}
		if hasHistoryEventTime || hasStepExecutionID || hasStepMethod {
			return fmt.Errorf("dex: time travel Step type cannot be combined with another selector")
		}
	case TimeTravelByStepExecutionID:
		if !hasStepExecutionID {
			return fmt.Errorf("dex: time travel Step execution ID must not be empty")
		}
		if hasHistoryEventTime || hasStepType {
			return fmt.Errorf("dex: time travel Step execution ID cannot be combined with another selector")
		}
		if options.StepMethod != TimeTravelStepWaitFor && options.StepMethod != TimeTravelStepExecute {
			return fmt.Errorf("dex: time travel Step method must be WaitFor or Execute")
		}
	default:
		return fmt.Errorf("dex: unsupported time travel type %d", options.Type)
	}
	return nil
}

func mapWaitForFlowOptions(options WaitForFlowOptions) bool {
	return options.NeedsResults
}

func mapRPCOptions(
	options *RPCOptions,
) (int32, []string, error) {
	if options == nil {
		return 0, nil, nil
	}
	timeout, err := durationSeconds32(options.Timeout)
	if err != nil {
		return 0, nil, err
	}
	locks, err := mapAttributeLocks(options.LockAttributes)
	if err != nil {
		return 0, nil, err
	}
	return timeout, locks, nil
}

func mapSearchFlowsOptions(
	query string,
	pageSize int32,
	nextPageToken string,
) (*dexpb.SearchFlowsRequest, error) {
	if pageSize < 0 {
		return nil, fmt.Errorf("dex: search page size must not be negative")
	}
	return &dexpb.SearchFlowsRequest{
		Query:         query,
		PageSize:      pageSize,
		NextPageToken: nextPageToken,
	}, nil
}

func mapValues(values []*dexpb.KV) (map[string]Value, error) {
	mapped := make(map[string]Value, len(values))
	for _, keyValue := range values {
		if keyValue == nil || keyValue.Key == "" {
			return nil, fmt.Errorf("dex: invalid key/value result")
		}
		if _, found := mapped[keyValue.Key]; found {
			return nil, fmt.Errorf("dex: duplicate result key %q", keyValue.Key)
		}
		value, err := newValue(keyValue.Value)
		if err != nil {
			return nil, err
		}
		mapped[keyValue.Key] = value
	}
	return mapped, nil
}

func newValue(value *dexpb.Value) (Value, error) {
	if err := validateConcreteValue(value); err != nil {
		return Value{}, err
	}
	return Value{value: value}, nil
}

func validateConcreteValue(value *dexpb.Value) error {
	if value == nil || value.Kind == nil {
		return fmt.Errorf("dex: value has no concrete kind")
	}
	switch kind := value.Kind.(type) {
	case *dexpb.Value_StringValue,
		*dexpb.Value_IntValue,
		*dexpb.Value_BoolValue:
		return nil
	case *dexpb.Value_DoubleValue:
		if math.IsNaN(kind.DoubleValue) || math.IsInf(kind.DoubleValue, 0) {
			return fmt.Errorf("dex: non-finite double result is unsupported")
		}
		return nil
	case *dexpb.Value_ObjValue:
		if kind.ObjValue == nil {
			return fmt.Errorf("dex: object value is missing")
		}
		switch kind.ObjValue.Encoding {
		case jsonEncoding, rawBytesEncoding:
			return nil
		default:
			return fmt.Errorf(
				"dex: unsupported object encoding %q",
				kind.ObjValue.Encoding,
			)
		}
	case *dexpb.Value_InternalBlobIdForStringValue,
		*dexpb.Value_InternalBlobIdForObjValue:
		return fmt.Errorf("dex: blob-backed value is not hydrated")
	case *dexpb.Value_NullValue:
		return nil
	default:
		return fmt.Errorf("dex: unsupported value kind %T", kind)
	}
}

func mergeStepOptions(defaults *StepOptions, overrides *StepOptions) *StepOptions {
	if defaults == nil {
		return overrides
	}
	if overrides == nil {
		return defaults
	}
	merged := *defaults
	if overrides.WaitForMethodTimeout != 0 {
		merged.WaitForMethodTimeout = overrides.WaitForMethodTimeout
	}
	if overrides.ExecuteMethodTimeout != 0 {
		merged.ExecuteMethodTimeout = overrides.ExecuteMethodTimeout
	}
	if overrides.HeartbeatTimeout != 0 {
		merged.HeartbeatTimeout = overrides.HeartbeatTimeout
	}
	if overrides.WaitForRetry != nil {
		merged.WaitForRetry = overrides.WaitForRetry
	}
	if overrides.ExecuteRetry != nil {
		merged.ExecuteRetry = overrides.ExecuteRetry
	}
	if overrides.WaitForFailure != FailFlowOnWaitForFailure {
		merged.WaitForFailure = overrides.WaitForFailure
	}
	if overrides.ExecuteFailure != nil {
		merged.ExecuteFailure = overrides.ExecuteFailure
	}
	if overrides.WaitForDurability != StepDurabilityDefault {
		merged.WaitForDurability = overrides.WaitForDurability
	}
	if overrides.ExecuteDurability != StepDurabilityDefault {
		merged.ExecuteDurability = overrides.ExecuteDurability
	}
	if overrides.WaitForLockAttributes != nil {
		merged.WaitForLockAttributes = overrides.WaitForLockAttributes
	}
	if overrides.ExecuteLockAttributes != nil {
		merged.ExecuteLockAttributes = overrides.ExecuteLockAttributes
	}
	if overrides.WaitForLoadAttributeMaps != nil {
		merged.WaitForLoadAttributeMaps = overrides.WaitForLoadAttributeMaps
	}
	if overrides.WaitForLoadAttributeMapInstances != nil {
		merged.WaitForLoadAttributeMapInstances = overrides.WaitForLoadAttributeMapInstances
	}
	if overrides.WaitForLoadChannels != nil {
		merged.WaitForLoadChannels = overrides.WaitForLoadChannels
	}
	if overrides.WaitForLoadChannelMaps != nil {
		merged.WaitForLoadChannelMaps = overrides.WaitForLoadChannelMaps
	}
	if overrides.WaitForLoadChannelMapInstances != nil {
		merged.WaitForLoadChannelMapInstances = overrides.WaitForLoadChannelMapInstances
	}
	if overrides.ExecuteLoadAttributeMaps != nil {
		merged.ExecuteLoadAttributeMaps = overrides.ExecuteLoadAttributeMaps
	}
	if overrides.ExecuteLoadAttributeMapInstances != nil {
		merged.ExecuteLoadAttributeMapInstances = overrides.ExecuteLoadAttributeMapInstances
	}
	if overrides.ExecuteLoadChannels != nil {
		merged.ExecuteLoadChannels = overrides.ExecuteLoadChannels
	}
	if overrides.ExecuteLoadChannelMaps != nil {
		merged.ExecuteLoadChannelMaps = overrides.ExecuteLoadChannelMaps
	}
	if overrides.ExecuteLoadChannelMapInstances != nil {
		merged.ExecuteLoadChannelMapInstances = overrides.ExecuteLoadChannelMapInstances
	}
	return &merged
}

func mapAttributeLocks(locks []AttributeLock) ([]string, error) {
	if locks == nil {
		return nil, nil
	}
	mapped := make([]string, 0, len(locks))
	seen := make(map[string]struct{}, len(locks))
	for _, lock := range locks {
		concrete, ok := lock.(attributeLock)
		if !ok {
			return nil, fmt.Errorf("dex: invalid attribute lock %T", lock)
		}
		name, err := physicalName(concrete.name, concrete.instance, concrete.isMap)
		if err != nil {
			return nil, err
		}
		if _, found := seen[name]; found {
			return nil, fmt.Errorf("dex: duplicate attribute lock %q", name)
		}
		seen[name] = struct{}{}
		mapped = append(mapped, name)
	}
	return mapped, nil
}

func physicalName(name string, instance string, isMap bool) (string, error) {
	if name == "" {
		return "", fmt.Errorf("dex: definition name must not be empty")
	}
	if !isMap {
		return name, nil
	}
	if err := validateMapInstance(instance); err != nil {
		return "", err
	}
	return name + "/" + url.PathEscape(instance), nil
}

func validateMapInstance(instance string) error {
	if instance == "" {
		return fmt.Errorf("dex: map instance must not be empty")
	}
	if strings.Contains(instance, "/") {
		return fmt.Errorf("dex: map instance must not contain '/'")
	}
	return nil
}

func mapWaitForFailurePolicy(
	policy WaitForFailurePolicy,
) (dexpb.WaitForMethodFailurePolicy, error) {
	switch policy {
	case FailFlowOnWaitForFailure:
		return dexpb.WaitForMethodFailurePolicy_WAIT_FOR_METHOD_FAILURE_POLICY_FAIL_FLOW_ON_FAILURE,
			nil
	case ProceedOnWaitForFailure:
		return dexpb.WaitForMethodFailurePolicy_WAIT_FOR_METHOD_FAILURE_POLICY_PROCEED_ON_FAILURE,
			nil
	default:
		return dexpb.WaitForMethodFailurePolicy_WAIT_FOR_METHOD_FAILURE_POLICY_UNSPECIFIED,
			fmt.Errorf("dex: unsupported WaitFor failure policy %d", policy)
	}
}

func mapStepDurability(
	durability StepDurability,
) (dexpb.StepDurability, error) {
	switch durability {
	case StepDurabilityDefault:
		return dexpb.StepDurability_STEP_DURABILITY_UNSPECIFIED, nil
	case StepDurabilitySync:
		return dexpb.StepDurability_STEP_DURABILITY_SYNC, nil
	case StepDurabilityAsync:
		return dexpb.StepDurability_STEP_DURABILITY_ASYNC, nil
	default:
		return dexpb.StepDurability_STEP_DURABILITY_UNSPECIFIED,
			fmt.Errorf("dex: unsupported step durability %d", durability)
	}
}

func mapOptionalStepDurability(
	durability *StepDurability,
) (*dexpb.StepDurability, error) {
	if durability == nil {
		return nil, nil
	}
	mapped, err := mapStepDurability(*durability)
	if err != nil {
		return nil, err
	}
	return ptr.Any(mapped), nil
}

func mapOptionalActiveStepSearchMode(
	mode *ActiveStepSearchMode,
) (*dexpb.ActiveStepSearchMode, error) {
	if mode == nil {
		return nil, nil
	}
	var mapped dexpb.ActiveStepSearchMode
	switch *mode {
	case SearchActiveStepsDefault:
		mapped = dexpb.ActiveStepSearchMode_ACTIVE_STEP_SEARCH_MODE_UNSPECIFIED
	case SearchAllActiveSteps:
		mapped = dexpb.ActiveStepSearchMode_ACTIVE_STEP_SEARCH_MODE_ENABLED_FOR_ALL
	case SearchActiveStepsWithWaitFor:
		mapped = dexpb.ActiveStepSearchMode_ACTIVE_STEP_SEARCH_MODE_ENABLED_FOR_STEPS_WITH_WAIT_FOR
	case DisableActiveStepSearch:
		mapped = dexpb.ActiveStepSearchMode_ACTIVE_STEP_SEARCH_MODE_DISABLED
	default:
		return nil, fmt.Errorf("dex: unsupported active-step search mode %d", *mode)
	}
	return ptr.Any(mapped), nil
}

func mapIDReusePolicy(policy IDReusePolicy) (dexpb.IdReusePolicy, error) {
	switch policy {
	case IDReuseDefault:
		return dexpb.IdReusePolicy_ID_REUSE_POLICY_UNSPECIFIED, nil
	case IDReuseAllowIfPreviousFailed:
		return dexpb.IdReusePolicy_ID_REUSE_POLICY_ALLOW_IF_PREVIOUS_EXISTS_ABNORMALLY,
			nil
	case IDReuseAllowIfNotRunning:
		return dexpb.IdReusePolicy_ID_REUSE_POLICY_ALLOW_IF_NO_RUNNING, nil
	case IDReuseDisallow:
		return dexpb.IdReusePolicy_ID_REUSE_POLICY_DISALLOW_REUSE, nil
	case IDReuseTerminateIfRunning:
		return dexpb.IdReusePolicy_ID_REUSE_POLICY_ALLOW_TERMINATE_IF_RUNNING, nil
	default:
		return dexpb.IdReusePolicy_ID_REUSE_POLICY_UNSPECIFIED,
			fmt.Errorf("dex: unsupported ID reuse policy %d", policy)
	}
}

func mapTimeTravelType(timeTravelType TimeTravelType) (dexpb.FlowResetType, error) {
	switch timeTravelType {
	case TimeTravelToBeginning:
		return dexpb.FlowResetType_FLOW_RESET_TYPE_BEGINNING, nil
	case TimeTravelByHistoryEventTime:
		return dexpb.FlowResetType_FLOW_RESET_TYPE_HISTORY_EVENT_TIME, nil
	case TimeTravelByStepType:
		return dexpb.FlowResetType_FLOW_RESET_TYPE_STEP_TYPE, nil
	case TimeTravelByStepExecutionID:
		return dexpb.FlowResetType_FLOW_RESET_TYPE_STEP_EXECUTION_ID, nil
	default:
		return dexpb.FlowResetType_FLOW_RESET_TYPE_UNSPECIFIED,
			fmt.Errorf("dex: unsupported time travel type %d", timeTravelType)
	}
}

func mapTimeTravelStepMethod(stepMethod TimeTravelStepMethod) (dexpb.FlowResetStepMethod, error) {
	switch stepMethod {
	case 0:
		return dexpb.FlowResetStepMethod_FLOW_RESET_STEP_METHOD_UNSPECIFIED, nil
	case TimeTravelStepWaitFor:
		return dexpb.FlowResetStepMethod_FLOW_RESET_STEP_METHOD_WAIT_FOR, nil
	case TimeTravelStepExecute:
		return dexpb.FlowResetStepMethod_FLOW_RESET_STEP_METHOD_EXECUTE, nil
	default:
		return dexpb.FlowResetStepMethod_FLOW_RESET_STEP_METHOD_UNSPECIFIED,
			fmt.Errorf("dex: unsupported time travel Step method %d", stepMethod)
	}
}

func mapFlowStatus(status dexpb.FlowStatus) (FlowStatus, error) {
	switch status {
	case dexpb.FlowStatus_FLOW_STATUS_RUNNING:
		return FlowRunning, nil
	case dexpb.FlowStatus_FLOW_STATUS_COMPLETED:
		return FlowCompleted, nil
	case dexpb.FlowStatus_FLOW_STATUS_FAILED:
		return FlowFailed, nil
	case dexpb.FlowStatus_FLOW_STATUS_SERVER_SIDE_TIMEOUT_INTERNAL_ONLY:
		return FlowServerSideTimeoutInternalOnly, nil
	case dexpb.FlowStatus_FLOW_STATUS_TERMINATED:
		return FlowTerminated, nil
	case dexpb.FlowStatus_FLOW_STATUS_CANCELED:
		return FlowCanceled, nil
	case dexpb.FlowStatus_FLOW_STATUS_CONTINUED_AS_NEW:
		return FlowContinuedAsNew, nil
	default:
		return 0, fmt.Errorf("dex: unsupported flow status %d", status)
	}
}

func mapFlowErrorType(errorType dexpb.FlowErrorType) (FlowErrorType, error) {
	switch errorType {
	case dexpb.FlowErrorType_FLOW_ERROR_TYPE_UNSPECIFIED:
		return 0, nil
	case dexpb.FlowErrorType_FLOW_ERROR_TYPE_STEP_DECISION_FAILING_FLOW:
		return FlowErrorStepDecision, nil
	case dexpb.FlowErrorType_FLOW_ERROR_TYPE_CLIENT_API_FAILING_FLOW:
		return FlowErrorClientAPI, nil
	case dexpb.FlowErrorType_FLOW_ERROR_TYPE_WORKER_API_FAIL:
		return FlowErrorWorkerMethod, nil
	case dexpb.FlowErrorType_FLOW_ERROR_TYPE_INVALID_USER_FLOW_CODE:
		return FlowErrorInvalidUserCode, nil
	case dexpb.FlowErrorType_FLOW_ERROR_TYPE_INTERNAL:
		return FlowErrorInternal, nil
	case dexpb.FlowErrorType_FLOW_ERROR_TYPE_FLOW_TIMEOUT:
		return FlowErrorTimeout, nil
	default:
		return 0, fmt.Errorf("dex: unsupported flow error type %d", errorType)
	}
}

func optionalDurationSeconds32(duration *time.Duration) (int32, error) {
	if duration == nil {
		return 0, nil
	}
	return durationSeconds32(*duration)
}

func durationSeconds32(duration time.Duration) (int32, error) {
	seconds, err := durationSeconds64(duration)
	if err != nil {
		return 0, err
	}
	if seconds > math.MaxInt32 {
		return 0, fmt.Errorf("duration %s exceeds int32 seconds", duration)
	}
	return int32(seconds), nil
}

func exactDurationSeconds32(duration time.Duration) (int32, error) {
	if duration < 0 {
		return 0, fmt.Errorf("duration must not be negative")
	}
	if duration%time.Second != 0 {
		return 0, fmt.Errorf("duration must use whole seconds")
	}
	seconds := duration / time.Second
	if seconds > math.MaxInt32 {
		return 0, fmt.Errorf("duration %s exceeds int32 seconds", duration)
	}
	return int32(seconds), nil
}

func durationSeconds64(duration time.Duration) (int64, error) {
	if duration < 0 {
		return 0, fmt.Errorf("duration must not be negative")
	}
	if duration == 0 {
		return 0, nil
	}
	seconds := duration / time.Second
	if duration%time.Second != 0 {
		seconds++
	}
	return int64(seconds), nil
}

func optionalInt32(value *int) (*int32, error) {
	if value == nil {
		return nil, nil
	}
	if *value < math.MinInt32 || *value > math.MaxInt32 {
		return nil, fmt.Errorf("dex: count %d exceeds int32", *value)
	}
	return ptr.Any(int32(*value)), nil
}

func float32Value(value float64) (float32, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) ||
		value > math.MaxFloat32 || value < -math.MaxFloat32 {
		return 0, fmt.Errorf("dex: backoff coefficient %v exceeds float32", value)
	}
	return float32(value), nil
}
