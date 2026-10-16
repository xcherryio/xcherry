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
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/superdurable/dex/sdk-go/gen/dexpb"
)

// Registry stores immutable Flow definitions shared by Client and Worker.
//
// NewRegistry validates every definition before publishing the Registry. Build one
// Registry during application startup and reuse it for every Client and
// Worker that must exchange those Flow types. Registry is safe for concurrent reads
// after NewRegistry returns.
type Registry struct {
	flows            map[string]*registeredFlow
	streamFlows      map[*streamDefinition]*registeredFlow
	attributeIndexes map[string]dexpb.IndexType
}

type registeredFlow struct {
	flow           Flow
	timeoutHandler FlowTimeoutHandler
	flowType       string
	startingStep   *registeredStep
	steps          map[string]*registeredStep
	rpcs           map[string]*registeredRPC
	actions        []*registeredAction
	attributes     map[string]registeredAttribute
	channels       map[string]registeredChannel
	streams        map[string]registeredStream
}

type registeredStep struct {
	handler     StepDef
	stepType    string
	inputType   reflect.Type
	options     *StepOptions
	starting    bool
	skipWaitFor bool
}

type registeredAttribute struct {
	def                  AttributeDef
	name                 string
	index                *AttributeIndex
	isMap                bool
	valueType            reflect.Type
	syncToAttributeStore bool
	isHardDeleteOnNull   bool
}

type registeredChannel struct {
	def   ChannelDef
	name  string
	isMap bool
}

type registeredStream struct {
	def        StreamDef
	definition *streamDefinition
}

// NewRegistry validates and assembles Flow definitions atomically.
//
// flows may be empty, but every Flow must be non-nil and have a unique Flow type.
// The default Flow type is the Go type name without its package, so two Flows
// from different packages that share a Go type name conflict. NewRegistry also
// validates starting Steps, Step options, RPC signatures, persistence definitions,
// and cross-Flow Attribute index compatibility. It returns FlowDefinitionError for an invalid Flow and publishes no
// partially assembled Registry.
//
//	registry, err := dex.NewRegistry([]dex.Flow{OrderFlow{}, RefundFlow{}})
//	if err != nil {
//		return err
//	}
func NewRegistry(flows []Flow) (*Registry, error) {
	assembled := &Registry{
		flows:       make(map[string]*registeredFlow, len(flows)),
		streamFlows: make(map[*streamDefinition]*registeredFlow),
	}
	indexTypes := make(map[string]IndexType)
	for index, flow := range flows {
		if nilInterface(flow) {
			return nil, newFlowDefinitionError(
				"",
				"",
				fmt.Errorf("flow at index %d is nil", index),
			)
		}
		flowType := GetFinalFlowType(flow)
		registered, err := assembled.registerFlow(flow, indexTypes)
		if err != nil {
			return nil, newFlowDefinitionError(flowType, "", err)
		}
		if existing, found := assembled.flows[registered.flowType]; found {
			return nil, newFlowDefinitionError(
				registered.flowType,
				"",
				fmt.Errorf(
					"duplicate flow type %q is registered by %s and %s; rename one Go type or implement GetFlowType",
					registered.flowType,
					reflect.TypeOf(existing.flow),
					reflect.TypeOf(flow),
				),
			)
		}
		for _, stream := range registered.streams {
			if owner, found := assembled.streamFlows[stream.definition]; found {
				return nil, newFlowDefinitionError(
					registered.flowType,
					fmt.Sprintf("stream %q", stream.definition.name),
					fmt.Errorf("stream is already registered by flow %q", owner.flowType),
				)
			}
			assembled.streamFlows[stream.definition] = registered
		}
		assembled.flows[registered.flowType] = registered
	}
	assembled.attributeIndexes = make(map[string]dexpb.IndexType, len(indexTypes))
	for name, indexType := range indexTypes {
		mapped, err := mapIndexType(indexType)
		if err != nil {
			return nil, err
		}
		assembled.attributeIndexes[name] = mapped
	}
	return assembled, nil
}

func (registry *Registry) registerFlow(
	flow Flow,
	indexTypes map[string]IndexType,
) (*registeredFlow, error) {
	flowType := GetFinalFlowType(flow)
	if flowType == "" {
		return nil, missingFlowTypeError(flow)
	}
	registered := &registeredFlow{
		flow:           flow,
		timeoutHandler: flowTimeoutHandler(flow),
		flowType:       flowType,
		steps:          make(map[string]*registeredStep),
		rpcs:           make(map[string]*registeredRPC),
		attributes:     make(map[string]registeredAttribute),
		channels:       make(map[string]registeredChannel),
		streams:        make(map[string]registeredStream),
	}
	if err := registered.registerPersistence(
		flow.GetPersistenceSchema(),
		indexTypes,
	); err != nil {
		return nil, fmt.Errorf("dex: flow %q: %w", flowType, err)
	}
	if err := registered.registerSteps(flow.GetSteps()); err != nil {
		return nil, fmt.Errorf("dex: flow %q: %w", flowType, err)
	}
	if err := registered.registerRPCs(flow.GetRPCs()); err != nil {
		return nil, fmt.Errorf("dex: flow %q: %w", flowType, err)
	}
	if len(registered.actions) > 0 {
		indexTypes[WorkQueuePermissionsIndexKey] = IndexKeywordArray
	}
	return registered, nil
}

func flowTimeoutHandler(flow Flow) FlowTimeoutHandler {
	handler, _ := flow.(FlowTimeoutHandler)
	return handler
}

func (flow *registeredFlow) registerPersistence(
	schema PersistenceSchema,
	indexTypes map[string]IndexType,
) error {
	for index, definition := range schema.Attributes {
		if nilInterface(definition) {
			return fmt.Errorf("attribute at index %d is nil", index)
		}
		if err := flow.registerAttribute(definition, indexTypes); err != nil {
			return err
		}
	}
	for index, definition := range schema.Channels {
		if nilInterface(definition) {
			return fmt.Errorf("channel at index %d is nil", index)
		}
		if err := flow.registerChannel(definition); err != nil {
			return err
		}
	}
	for index, definition := range schema.Streams {
		if nilInterface(definition) {
			return fmt.Errorf("stream at index %d is nil", index)
		}
		if err := flow.registerStream(definition); err != nil {
			return err
		}
	}
	return nil
}

func (flow *registeredFlow) registerAttribute(
	definition AttributeDef,
	indexTypes map[string]IndexType,
) error {
	name := definition.attributeName()
	if err := validatePersistenceDefinitionName(name, "attribute"); err != nil {
		return err
	}
	if _, found := flow.attributes[name]; found {
		return fmt.Errorf("duplicate attribute %q", name)
	}
	if name == WorkQueuePermissionsIndexKey {
		return fmt.Errorf("attribute name %q is reserved by Dex", name)
	}
	index := definition.attributeIndex()
	isMap := definition.attributeIsMap()
	if definition.attributeIsHardDeleteOnNull() {
		if err := validateHardDeleteAttributeDefinition(definition); err != nil {
			return fmt.Errorf("attribute %q: %w", name, err)
		}
	}
	if index != nil {
		if _, err := mapIndexType(index.Type); err != nil {
			return fmt.Errorf("attribute %q: %w", name, err)
		}
		indexKey := effectiveIndexKey(name, index, isMap)
		if indexKey == "" {
			return fmt.Errorf("indexed attribute map %q requires an index key", name)
		}
		if indexKey == WorkQueuePermissionsIndexKey {
			return fmt.Errorf("index key %q is reserved by Dex", indexKey)
		}
		if existing, found := indexTypes[indexKey]; found &&
			existing != index.Type {
			return fmt.Errorf(
				"index key %q has conflicting types %d and %d",
				indexKey,
				existing,
				index.Type,
			)
		}
		indexTypes[indexKey] = index.Type
	}
	flow.attributes[name] = registeredAttribute{
		def:                  definition,
		name:                 name,
		index:                index,
		isMap:                isMap,
		valueType:            definition.attributeValueType(),
		syncToAttributeStore: definition.attributeSyncToAttributeStore(),
		isHardDeleteOnNull:   definition.attributeIsHardDeleteOnNull(),
	}
	return nil
}

func validateHardDeleteAttributeDefinition(definition AttributeDef) error {
	if definition.attributeIndex() != nil {
		return fmt.Errorf("HardDeleteOnNull cannot be combined with Indexed")
	}
	if definition.attributeSyncToAttributeStore() {
		return fmt.Errorf("HardDeleteOnNull cannot be combined with SyncToAttributeStore")
	}
	valueType := definition.attributeValueType()
	for valueType.Kind() == reflect.Pointer {
		valueType = valueType.Elem()
	}
	switch valueType.Kind() {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64:
		return fmt.Errorf("HardDeleteOnNull supports only string and object values, not %s", valueType)
	}
	return nil
}

func (flow *registeredFlow) registerChannel(
	definition ChannelDef,
) error {
	name := definition.channelName()
	if err := validatePersistenceDefinitionName(name, "channel"); err != nil {
		return err
	}
	if _, found := flow.channels[name]; found {
		return fmt.Errorf("duplicate channel %q", name)
	}
	flow.channels[name] = registeredChannel{
		def:   definition,
		name:  name,
		isMap: definition.channelIsMap(),
	}
	return nil
}

func validatePersistenceDefinitionName(name string, kind string) error {
	if name == "" {
		return fmt.Errorf("%s name must not be empty", kind)
	}
	if strings.Contains(name, "/") {
		return fmt.Errorf("%s name must not contain /", kind)
	}
	return nil
}

func (flow *registeredFlow) registerStream(definition StreamDef) error {
	stream := definition.streamDefinition()
	if err := validateStreamDefinition(stream); err != nil {
		return err
	}
	if _, found := flow.streams[stream.name]; found {
		return fmt.Errorf("duplicate stream %q", stream.name)
	}
	flow.streams[stream.name] = registeredStream{
		def:        definition,
		definition: stream,
	}
	return nil
}

func (flow *registeredFlow) registerSteps(definitions []StepDef) error {
	for index, definition := range definitions {
		if err := flow.registerStep(definition, index); err != nil {
			return err
		}
	}
	// Validate in GetSteps order so the first illegal step is reported stably.
	for _, definition := range definitions {
		step := flow.steps[definition.stepType()]
		if err := flow.validateStepOptions(step.options, step.inputType); err != nil {
			return fmt.Errorf("step %q options: %w", step.stepType, err)
		}
	}
	return nil
}

func (flow *registeredFlow) registerRPCs(definitions []RPCDef) error {
	for index, definition := range definitions {
		if err := flow.registerRPC(definition, index); err != nil {
			return err
		}
	}
	return nil
}

func (flow *registeredFlow) registerRPC(definition RPCDef, index int) error {
	if definition == nil || nilInterface(definition.rpcHandler()) {
		return fmt.Errorf("RPC at index %d is nil", index)
	}
	handler := definition.rpcHandler()
	durableName, err := rpcMethodName(handler)
	if err != nil {
		return fmt.Errorf("RPC at index %d: %w", index, err)
	}
	identity, err := rpcMethodIdentity(handler)
	if err != nil {
		return fmt.Errorf("RPC %q: %w", durableName, err)
	}
	flowMethod, found := reflect.TypeOf(flow.flow).MethodByName(durableName)
	if !found || runtime.FuncForPC(flowMethod.Func.Pointer()).Name() != identity {
		return fmt.Errorf(
			"RPC %q must be a direct bound method on Flow %q",
			durableName,
			flow.flowType,
		)
	}
	if _, found := flow.rpcs[durableName]; found {
		return fmt.Errorf("duplicate RPC %q", durableName)
	}
	if err := flow.validateRPCOptions(definition.rpcOptions()); err != nil {
		return fmt.Errorf("RPC %q options: %w", durableName, err)
	}
	var action *registeredAction
	if options := definition.rpcOptions(); options != nil && options.Action != nil {
		action, err = flow.compileAction(options.Action)
		if err != nil {
			return fmt.Errorf("RPC %q Action: %w", durableName, err)
		}
		flow.actions = append(flow.actions, action)
	}
	flow.rpcs[durableName] = &registeredRPC{
		handler:     definition,
		durableName: durableName,
		identity:    identity,
		input:       definition.rpcInputType(),
		output:      definition.rpcOutputType(),
		options:     definition.rpcOptions(),
		action:      action,
	}
	return nil
}

func (flow *registeredFlow) validateRPCOptions(options *RPCOptions) error {
	if options == nil {
		return nil
	}
	if err := flow.validateAttributeLocks(options.LockAttributes); err != nil {
		return fmt.Errorf("locks: %w", err)
	}
	if _, _, _, err := validateStateLoads(flow, rpcStateLoads(options)); err != nil {
		return fmt.Errorf("state load: %w", err)
	}
	if _, _, err := mapRPCOptions(options); err != nil {
		return err
	}
	return nil
}

func (flow *registeredFlow) registerStep(
	definition StepDef,
	index int,
) error {
	if definition == nil || nilInterface(definition.stepValue()) {
		return fmt.Errorf("step at index %d is nil", index)
	}
	stepType := definition.stepType()
	if stepType == "" {
		return fmt.Errorf(
			"step at index %d has no default Step type because %s is generic or unnamed; implement GetStepType",
			index,
			reflect.TypeOf(definition.stepValue()),
		)
	}
	if existing, found := flow.steps[stepType]; found {
		return fmt.Errorf(
			"duplicate step type %q is registered by %s and %s; rename one Go type or implement GetStepType",
			stepType,
			reflect.TypeOf(existing.handler.stepValue()),
			reflect.TypeOf(definition.stepValue()),
		)
	}
	registered := &registeredStep{
		handler:     definition,
		stepType:    stepType,
		inputType:   definition.stepInputType(),
		options:     definition.stepOptions(),
		starting:    definition.isStarting(),
		skipWaitFor: definition.skipWaitFor(),
	}
	if definition.isStarting() {
		if flow.startingStep != nil {
			return fmt.Errorf(
				"multiple starting steps %q and %q",
				flow.startingStep.stepType,
				stepType,
			)
		}
		flow.startingStep = registered
	}
	flow.steps[stepType] = registered
	return nil
}

func (flow *registeredFlow) validateStepOptions(
	options *StepOptions,
	inputType reflect.Type,
) error {
	if err := flow.doValidateStepOptions(
		options,
		inputType,
		make(map[*StepOptions]bool),
	); err != nil {
		return err
	}
	if _, err := mapStepOptions(options); err != nil {
		return err
	}
	return nil
}

func (flow *registeredFlow) doValidateStepOptions(
	options *StepOptions,
	inputType reflect.Type,
	active map[*StepOptions]bool,
) error {
	if options == nil {
		return nil
	}
	if active[options] {
		return fmt.Errorf("step options contain a cycle")
	}
	active[options] = true
	defer delete(active, options)

	if err := flow.validateAttributeLocks(
		options.WaitForLockAttributes,
	); err != nil {
		return fmt.Errorf("WaitFor locks: %w", err)
	}
	if err := flow.validateAttributeLocks(
		options.ExecuteLockAttributes,
	); err != nil {
		return fmt.Errorf("Execute locks: %w", err)
	}
	if _, _, _, err := validateStateLoads(flow, waitForStepStateLoads(options)); err != nil {
		return fmt.Errorf("WaitFor state load: %w", err)
	}
	if _, _, _, err := validateStateLoads(flow, executeStepStateLoads(options)); err != nil {
		return fmt.Errorf("Execute state load: %w", err)
	}
	if options.ExecuteFailure == nil {
		return nil
	}
	target, err := flow.resolveStepReference(options.ExecuteFailure.step)
	if err != nil {
		return fmt.Errorf("Execute failure target: %w", err)
	}
	if target.inputType != inputType {
		return fmt.Errorf(
			"Execute failure target %q input %s does not match %s",
			target.stepType,
			target.inputType,
			inputType,
		)
	}
	return flow.doValidateStepOptions(
		options.ExecuteFailure.options,
		target.inputType,
		active,
	)
}

func (flow *registeredFlow) validateFlowTimeoutHandlerOptions(
	timeout *time.Duration,
	policy FlowTimeoutPolicy,
	options *FlowTimeoutHandlerOptions,
) error {
	if options == nil {
		return nil
	}
	if timeout == nil || *timeout <= 0 || policy != TimeoutHandler {
		return fmt.Errorf("timeout handler options require a positive timeout with Handler policy")
	}
	if err := flow.validateAttributeLocks(options.LockAttributes); err != nil {
		return fmt.Errorf("timeout handler locks: %w", err)
	}
	if _, _, _, err := validateStateLoads(flow, timeoutHandlerStateLoads(options)); err != nil {
		return fmt.Errorf("timeout handler state load: %w", err)
	}
	if options.Failure != nil {
		target, err := flow.resolveStepReference(options.Failure.step)
		if err != nil {
			return fmt.Errorf("timeout handler failure target: %w", err)
		}
		if target.inputType != reflect.TypeFor[None]() {
			return fmt.Errorf(
				"timeout handler failure target %q input %s is not None",
				target.stepType,
				target.inputType,
			)
		}
		if err := flow.doValidateStepOptions(
			options.Failure.options,
			target.inputType,
			make(map[*StepOptions]bool),
		); err != nil {
			return err
		}
	}
	if _, err := mapFlowTimeoutHandlerOptions(options); err != nil {
		return err
	}
	return nil
}

func (flow *registeredFlow) validateAttributeLocks(
	locks []AttributeLock,
) error {
	seen := make(map[string]struct{}, len(locks))
	for _, lock := range locks {
		concrete, ok := lock.(attributeLock)
		if !ok {
			return fmt.Errorf("invalid attribute lock %T", lock)
		}
		attribute, found := flow.attributes[concrete.name]
		if !found {
			return fmt.Errorf("attribute %q is not declared", concrete.name)
		}
		if attribute.isMap != concrete.isMap {
			return fmt.Errorf(
				"attribute %q static/map kind does not match its lock",
				concrete.name,
			)
		}
		name, err := physicalName(
			concrete.name,
			concrete.instance,
			concrete.isMap,
		)
		if err != nil {
			return err
		}
		if _, found := seen[name]; found {
			return fmt.Errorf("duplicate attribute lock %q", name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

func (flow *registeredFlow) resolveMovement(
	movement StepMovement,
) (*registeredStep, error) {
	target, err := flow.resolveStepReference(movement.step)
	if err != nil {
		return nil, err
	}
	if !assignableValue(movement.input, target.inputType) {
		return nil, fmt.Errorf(
			"movement input %T is not assignable to step %q input %s",
			movement.input,
			target.stepType,
			target.inputType,
		)
	}
	if err := flow.validateStepOptions(
		movement.options,
		target.inputType,
	); err != nil {
		return nil, fmt.Errorf(
			"movement to step %q options: %w",
			target.stepType,
			err,
		)
	}
	return target, nil
}

func (flow *registeredFlow) resolveStepReference(
	reference StepDef,
) (*registeredStep, error) {
	if reference == nil || nilInterface(reference.stepValue()) {
		return nil, fmt.Errorf("step reference is nil")
	}
	stepType := reference.stepType()
	if stepType == "" {
		return nil, fmt.Errorf("step reference type must not be empty")
	}
	target, found := flow.steps[stepType]
	if !found {
		return nil, fmt.Errorf("step %q is not registered", stepType)
	}
	if reference.stepInputType() != target.inputType {
		return nil, fmt.Errorf(
			"step %q reference input %s does not match registered input %s",
			stepType,
			reference.stepInputType(),
			target.inputType,
		)
	}
	return target, nil
}

func (flow *registeredFlow) resolveStepSelector(
	selector StepSelector,
) (*registeredStep, error) {
	if selector == nil || nilInterface(selector) {
		return nil, fmt.Errorf("step cancellation selector is nil")
	}
	stepType := selector.GetStepType()
	if stepType == "" {
		stepType = defaultTypeNameFromReflect(selector)
	}
	target, found := flow.steps[stepType]
	if !found {
		return nil, fmt.Errorf("step %q is not registered", stepType)
	}
	if reflect.TypeOf(target.handler.stepValue()) != reflect.TypeOf(selector) {
		return nil, fmt.Errorf("step %q selector does not match its registered type", stepType)
	}
	return target, nil
}

func (flow *registeredFlow) resolveChannels(
	definitions []ChannelDef,
) ([]registeredChannel, error) {
	if len(definitions) == 0 {
		return nil, fmt.Errorf("channel references must not be empty")
	}
	resolved := make([]registeredChannel, 0, len(definitions))
	seen := make(map[string]struct{}, len(definitions))
	for _, definition := range definitions {
		if nilInterface(definition) {
			return nil, fmt.Errorf("channel reference is nil")
		}
		name := definition.channelName()
		channel, found := flow.channels[name]
		if !found {
			return nil, fmt.Errorf("channel %q is not declared", name)
		}
		if channel.isMap != definition.channelIsMap() {
			return nil, fmt.Errorf(
				"channel %q static/map kind does not match its definition",
				name,
			)
		}
		if _, found := seen[name]; found {
			return nil, fmt.Errorf(
				"duplicate channel reference %q",
				name,
			)
		}
		seen[name] = struct{}{}
		resolved = append(resolved, channel)
	}
	return resolved, nil
}

func (registry *Registry) lookupFlow(
	flowType string,
) (*registeredFlow, bool) {
	flow, found := registry.flows[flowType]
	return flow, found
}

func (registry *Registry) resolveFlow(reference Flow) (*registeredFlow, error) {
	if nilInterface(reference) {
		return nil, fmt.Errorf("dex: flow is nil")
	}
	flowType := GetFinalFlowType(reference)
	if flowType == "" {
		return nil, missingFlowTypeError(reference)
	}
	registered, found := registry.lookupFlow(flowType)
	if !found {
		return nil, newFlowDefinitionError(
			flowType,
			"",
			fmt.Errorf("flow is not registered"),
		)
	}
	if reflect.TypeOf(reference) != reflect.TypeOf(registered.flow) {
		return nil, newFlowDefinitionError(
			flowType,
			"",
			fmt.Errorf(
				"type %s does not match registered type %s",
				reflect.TypeOf(reference),
				reflect.TypeOf(registered.flow),
			),
		)
	}
	return registered, nil
}

// GetFinalFlowType returns the Flow type that NewRegistry registers for a Flow.
//
// A non-empty GetFlowType override wins. Otherwise the Flow type is the Go type
// name without its package or pointer, so OrderFlow{} and &OrderFlow{} from
// package orders both return "OrderFlow". Use it in tests and tools that need
// the registered name, such as a SearchFlows query on FlowType.
//
//	query := "FlowType = '" + dex.GetFinalFlowType(&orders.OrderFlow{}) + "'"
//
// A generic or unnamed Go type has no default Flow type. Such a Flow must
// implement GetFlowType, and NewRegistry rejects it otherwise.
//
// flow is the Flow value to name. It is not retained.
//
// Returns the override when it is non-empty, otherwise the Go type name without
// its package or pointer. Returns an empty string when there is no override and
// the Go type is generic or unnamed.
func GetFinalFlowType(flow Flow) string {
	if flowType := flow.GetFlowType(); flowType != "" {
		return flowType
	}
	return defaultTypeNameFromReflect(flow)
}

// GetFinalStepType returns the Step type that NewRegistry registers for a Step.
//
// A non-empty GetStepType override wins. Otherwise the Step type is the Go type
// name without its package or pointer, so shipOrder{} and &shipOrder{} both
// return "shipOrder". Use it in tests and tools that need the registered name,
// such as a StepExecutionID passed to SkipTimer or WaitForStepCompletion.
//
//	stepType := dex.GetFinalStepType[ShipInput](shipOrder{})
//	executionID := dex.StepExecutionID{StepType: stepType}
//
// A generic or unnamed Go type has no default Step type. Such a Step must
// implement GetStepType, and NewRegistry rejects it otherwise.
//
// IN is the Step input type. step is the Step value to name. It is not retained.
//
// Returns the override when it is non-empty, otherwise the Go type name without
// its package or pointer. Returns an empty string when there is no override and
// the Go type is generic or unnamed.
func GetFinalStepType[IN any](step Step[IN]) string {
	if stepType := step.GetStepType(); stepType != "" {
		return stepType
	}
	return defaultTypeNameFromReflect(step)
}

// Generic instantiations have no default name; reflection renders their type arguments with import paths.
func defaultTypeNameFromReflect(value any) string {
	valueType := reflect.TypeOf(value)
	for valueType.Kind() == reflect.Pointer {
		valueType = valueType.Elem()
	}
	if strings.Contains(valueType.Name(), "[") {
		return ""
	}
	return valueType.Name()
}

func missingFlowTypeError(flow Flow) error {
	return fmt.Errorf(
		"dex: flow %s has no default Flow type because it is generic or unnamed; implement GetFlowType",
		reflect.TypeOf(flow),
	)
}

func (registry *Registry) resolveRPC(reference any) (*registeredFlow, *registeredRPC, error) {
	rpcName, err := rpcMethodName(reference)
	if err != nil {
		return nil, nil, err
	}
	referenceType := reflect.TypeOf(reference)
	result := reflect.Zero(referenceType.Out(0).Elem()).Interface().(rpcResult)
	inputType := referenceType.In(1)
	outputType := result.rpcOutputType()
	identity, err := rpcMethodIdentity(reference)
	if err != nil {
		return nil, nil, err
	}
	for _, flow := range registry.flows {
		registered, found := flow.lookupRPC(rpcName)
		if found && registered.identity == identity &&
			registered.input == inputType && registered.output == outputType {
			return flow, registered, nil
		}
	}
	return nil, nil, newFlowDefinitionError(
		"",
		fmt.Sprintf("rpc %q", rpcName),
		fmt.Errorf(
			"input %s and output %s are not registered",
			inputType,
			outputType,
		),
	)
}

func (registry *Registry) resolveAttribute(
	reference AttributeDef,
	expectMap bool,
) (registeredAttribute, error) {
	if nilInterface(reference) {
		return registeredAttribute{}, fmt.Errorf("dex: attribute definition is nil")
	}
	name := reference.attributeName()
	if name == "" {
		return registeredAttribute{}, fmt.Errorf("dex: attribute name must not be empty")
	}
	if reference.attributeIsMap() != expectMap {
		return registeredAttribute{}, fmt.Errorf(
			"dex: attribute %q has the wrong static/map kind",
			name,
		)
	}
	for _, flow := range registry.flows {
		registered, found := flow.attributes[name]
		if found && registered.isMap == expectMap &&
			reflect.DeepEqual(registered.index, reference.attributeIndex()) &&
			registered.syncToAttributeStore == reference.attributeSyncToAttributeStore() &&
			registered.isHardDeleteOnNull == reference.attributeIsHardDeleteOnNull() {
			return registered, nil
		}
	}
	return registeredAttribute{}, newFlowDefinitionError(
		"",
		fmt.Sprintf("attribute %q", name),
		fmt.Errorf("attribute is not registered"),
	)
}

func (registry *Registry) resolveChannel(
	reference ChannelDef,
	expectMap bool,
) (registeredChannel, error) {
	if nilInterface(reference) {
		return registeredChannel{}, fmt.Errorf("dex: channel definition is nil")
	}
	name := reference.channelName()
	if name == "" {
		return registeredChannel{}, fmt.Errorf("dex: channel name must not be empty")
	}
	if reference.channelIsMap() != expectMap {
		return registeredChannel{}, fmt.Errorf(
			"dex: channel %q has the wrong static/map kind",
			name,
		)
	}
	for _, flow := range registry.flows {
		registered, found := flow.channels[name]
		if found && registered.isMap == expectMap {
			return registered, nil
		}
	}
	return registeredChannel{}, newFlowDefinitionError(
		"",
		fmt.Sprintf("channel %q", name),
		fmt.Errorf("channel is not registered"),
	)
}

func (registry *Registry) resolveStream(
	reference StreamDef,
) (*registeredFlow, registeredStream, error) {
	if nilInterface(reference) {
		return nil, registeredStream{}, fmt.Errorf("dex: stream definition is nil")
	}
	definition := reference.streamDefinition()
	if err := validateStreamDefinition(definition); err != nil {
		return nil, registeredStream{}, fmt.Errorf("dex: %w", err)
	}
	flow, found := registry.streamFlows[definition]
	if found {
		return flow, flow.streams[definition.name], nil
	}
	return nil, registeredStream{}, newFlowDefinitionError(
		"",
		fmt.Sprintf("stream %q", definition.name),
		fmt.Errorf("stream is not registered"),
	)
}

func (flow *registeredFlow) resolveStream(
	definition *streamDefinition,
) (registeredStream, error) {
	if err := validateStreamDefinition(definition); err != nil {
		return registeredStream{}, err
	}
	registered, found := flow.streams[definition.name]
	if !found || registered.definition != definition {
		return registeredStream{}, fmt.Errorf("stream %q is not declared", definition.name)
	}
	return registered, nil
}

func (flow *registeredFlow) lookupStep(
	stepType string,
) (*registeredStep, bool) {
	step, found := flow.steps[stepType]
	return step, found
}

func (flow *registeredFlow) lookupRPC(
	rpcName string,
) (*registeredRPC, bool) {
	rpc, found := flow.rpcs[rpcName]
	return rpc, found
}

func effectiveIndexKey(
	name string,
	index *AttributeIndex,
	isMap bool,
) string {
	if index.IndexKey != "" {
		return index.IndexKey
	}
	if isMap {
		return ""
	}
	return name
}

func assignableValue(value any, targetType reflect.Type) bool {
	if value == nil {
		return isNilableType(targetType)
	}
	return reflect.TypeOf(value).AssignableTo(targetType)
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	return reflected.IsValid() && isNilValue(reflected)
}
