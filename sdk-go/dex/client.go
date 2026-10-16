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
	"math"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/superdurable/dex/blob-cache-go/blobcache"
	"github.com/superdurable/dex/sdk-go/gen/dexpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

var errClientClosed = errors.New("dex: Client is closed")

const serverCappedLongPollSeconds = int32(math.MaxInt32)

// ClientOptions configures the FlowService client.
type ClientOptions struct {
	// FlowServiceAddress is Dex's plaintext gRPC target. Default: "localhost:8801".
	FlowServiceAddress string
	// WorkerTarget is advertised by StartFlow unless overridden. Default: nil.
	WorkerTarget *WorkerTarget
	// Logger defaults to the shared BlobCache logger.
	Logger Logger
	// Credential is sent as a bearer credential with every FlowService call. Default: nil sends none.
	// A Dex server with authentication enabled requires it; see FileCredential.
	Credential Credential
	// GrpcMaxMessageBytes limits the gRPC messages this Client receives, in bytes, such as
	// InvokeRPC results, state reads, and each LoadBlobs response used to hydrate values.
	// Zero uses DefaultGrpcMaxMessageBytes (32 MiB) and keeps gRPC's unlimited send limit.
	// A positive value also limits the messages the Client sends. Negative values are invalid.
	// The Dex server applies its own api.grpcMaxMessageBytes, and the smaller limit wins.
	// Keep it above the largest application value plus headroom for the message envelope.
	GrpcMaxMessageBytes int
}

// Client calls FlowService with registered typed definitions.
//
// A Client is safe for concurrent use. Calls use their context for cancellation and
// return an error after Close. The Client owns its gRPC connection; callers should
// close it when the application shuts down.
//
//	client, err := dex.NewClient(registry, cache, dex.ClientOptions{})
//	if err != nil {
//		return err
//	}
//	defer client.Close()
//	runID, err := client.StartFlow(ctx, orderFlow, "order-42", orderInput,
//		dex.StartFlowOptions{})
type Client struct {
	registry     *Registry
	cache        *blobcache.Cache
	service      dexpb.FlowServiceClient
	hydrator     valueHydrator
	connection   *grpc.ClientConn
	workerTarget *WorkerTarget
	logger       Logger
	// serverProtocol is negotiated lazily because only hard-delete writes depend on it.
	serverProtocol negotiatedServerProtocol

	lifecycleMu sync.RWMutex
	closed      bool
}

// NewClient constructs a Client from shared dependencies.
//
// registry supplies the validated definitions and cache hydrates large values. The
// Client connects lazily to ClientOptions.FlowServiceAddress, which defaults to
// localhost:8801. It panics when registry or cache is nil. It returns an error when
// an address, worker target, or GrpcMaxMessageBytes is invalid, or the gRPC client
// cannot be created.
// The caller owns the returned Client and must call Close.
func NewClient(
	registry *Registry,
	cache *blobcache.Cache,
	options ClientOptions,
) (*Client, error) {
	if registry == nil {
		panic("dex.NewClient requires Registry")
	}
	if cache == nil {
		panic("dex.NewClient requires BlobCache")
	}
	flowServiceAddress, workerTarget, err := resolveClientOptions(options)
	if err != nil {
		return nil, err
	}
	messageLimits, err := resolveGrpcMessageLimits(options.GrpcMaxMessageBytes)
	if err != nil {
		return nil, err
	}
	connection, err := grpc.NewClient(
		flowServiceAddress,
		messageLimits.flowServiceDialOptions(options.Credential)...,
	)
	if err != nil {
		return nil, fmt.Errorf("dex: create FlowService client: %w", err)
	}
	return newClient(
		registry,
		cache,
		connection,
		workerTarget,
		resolveLogger(options.Logger, cache.Logger()),
	), nil
}

func newClient(
	registry *Registry,
	cache *blobcache.Cache,
	connection *grpc.ClientConn,
	workerTarget *WorkerTarget,
	logger Logger,
) *Client {
	logger = resolveLogger(logger, cache.Logger())
	service := dexpb.NewFlowServiceClient(connection)
	return &Client{
		registry:     registry,
		cache:        cache,
		service:      service,
		hydrator:     newValueHydrator(service, cache, logger, connection.Target()),
		connection:   connection,
		workerTarget: workerTarget,
		logger:       logger,
	}
}

func resolveClientOptions(options ClientOptions) (string, *WorkerTarget, error) {
	flowServiceAddress := strings.TrimSpace(options.FlowServiceAddress)
	if flowServiceAddress == "" {
		flowServiceAddress = defaultFlowServiceTarget
	}
	if err := validatePlaintextTarget(flowServiceAddress, false); err != nil {
		return "", nil, fmt.Errorf("dex: invalid FlowService address: %w", err)
	}
	if options.WorkerTarget == nil {
		return flowServiceAddress, nil, nil
	}
	workerTarget := *options.WorkerTarget
	workerTarget.Address = strings.TrimSpace(workerTarget.Address)
	if err := validateWorkerTarget(workerTarget); err != nil {
		return "", nil, fmt.Errorf("dex: invalid Client Worker target: %w", err)
	}
	return flowServiceAddress, &workerTarget, nil
}

// Close closes the owned FlowService connection.
//
// Close is idempotent. Calls begun after Close return an error; calls already in
// progress are controlled by their contexts and the gRPC connection shutdown.
func (client *Client) Close() error {
	client.lifecycleMu.Lock()
	defer client.lifecycleMu.Unlock()
	if client.closed {
		return nil
	}
	client.closed = true
	return client.connection.Close()
}

func (client *Client) validateCall(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("dex: context must not be nil")
	}
	client.lifecycleMu.RLock()
	closed := client.closed
	client.lifecycleMu.RUnlock()
	if closed {
		return errClientClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (client *Client) validateFlowCall(ctx context.Context, flowID string) error {
	if err := client.validateCall(ctx); err != nil {
		return err
	}
	if strings.TrimSpace(flowID) == "" {
		return fmt.Errorf("dex: flow ID must not be empty")
	}
	return nil
}

func (client *Client) hydrateValues(
	ctx context.Context,
	flowID string,
	valuePointers []**dexpb.Value,
) error {
	return client.hydrateFlowValues(ctx, valuePointersForFlow(flowID, valuePointers))
}

func (client *Client) hydrateFlowValues(ctx context.Context, targets []flowValuePointer) error {
	if err := client.hydrator.HydrateValuesInPlace(ctx, targets); err != nil {
		var failure *workerFailure
		if errors.As(err, &failure) {
			return translateRPCError(failure.cause, "LoadBlobs", "", flowTargetNone)
		}
		return translateRPCError(err, "LoadBlobs", "", flowTargetNone)
	}
	return nil
}

// FlowStatus describes the current or terminal state of a Flow run.
type FlowStatus uint8

const (
	// FlowRunning means the Flow can still execute Steps or receive messages and RPCs.
	FlowRunning FlowStatus = iota + 1
	// FlowCompleted means the Flow completed successfully.
	FlowCompleted
	// FlowFailed means an unrecovered Step, RPC, or user-code error ended the Flow.
	FlowFailed
	// FlowServerSideTimeoutInternalOnly is reserved for backend hard-timeout reporting.
	// Applications must not depend on this status.
	FlowServerSideTimeoutInternalOnly
	// FlowTerminated means an operator ended the Flow immediately.
	FlowTerminated
	// FlowCanceled means the Flow completed cooperative cancellation.
	FlowCanceled
	// FlowContinuedAsNew means history rolled into a successor run.
	FlowContinuedAsNew
)

// String returns the Flow status name.
func (status FlowStatus) String() string {
	switch status {
	case FlowRunning:
		return "running"
	case FlowCompleted:
		return "completed"
	case FlowFailed:
		return "failed"
	case FlowServerSideTimeoutInternalOnly:
		return "server-side timeout (internal only)"
	case FlowTerminated:
		return "terminated"
	case FlowCanceled:
		return "canceled"
	case FlowContinuedAsNew:
		return "continued as new"
	default:
		return fmt.Sprintf("unknown status %d", status)
	}
}

// FlowErrorType categorizes the failure recorded for a terminal Flow.
type FlowErrorType uint8

const (
	// FlowErrorStepDecision identifies an invalid or failing Step decision.
	FlowErrorStepDecision FlowErrorType = iota + 1
	// FlowErrorClientAPI identifies a client API operation that failed the Flow.
	FlowErrorClientAPI
	// FlowErrorWorkerMethod identifies a failed Worker Step or RPC handler.
	FlowErrorWorkerMethod
	// FlowErrorInvalidUserCode identifies an invalid Flow or Step definition.
	FlowErrorInvalidUserCode
	// FlowErrorInternal identifies an internal Dex failure.
	FlowErrorInternal
	// FlowErrorTimeout identifies expiration of a Dex soft Flow timeout.
	FlowErrorTimeout
)

// StepCompletion contains one hydrated output from a completed Step.
type StepCompletion struct {
	// StepType is the registered Step type.
	StepType string
	// StepExecutionID is the completed execution's server identity.
	StepExecutionID string
	// Output is an opaque hydrated value decoded with Value.Decode.
	Output Value
}

// FlowResult describes an observed Flow status and its output-bearing Step completions.
//
// Client.WaitForFlow returns terminal results. SubFlowResult can return a running
// snapshot when another branch of AnyOf wins; that snapshot is not a live backend query.
type FlowResult struct {
	// Status is the observed Flow status.
	Status FlowStatus
	// Completions preserves server collection order when NeedsResults was true.
	// Parallel completion order is not deterministic; select by StepType or StepExecutionID.
	Completions []StepCompletion
	// ErrorType is the Flow failure category when available.
	ErrorType FlowErrorType
	// ErrorMessage is the server completion detail when available.
	ErrorMessage string
}

// IsTerminal reports whether the result can no longer execute in its observed run.
func (result FlowResult) IsTerminal() bool {
	return result.Status != FlowRunning && result.Status != FlowContinuedAsNew
}

// DecodeSingleOutput decodes the output when exactly one completion exists.
//
// target follows Value.Decode semantics and must be a non-nil pointer. Running results and zero
// or multiple completions return an error without decoding a value.
func (result FlowResult) DecodeSingleOutput(target any) error {
	if !result.IsTerminal() {
		return fmt.Errorf("dex: Flow result is not terminal")
	}
	if len(result.Completions) != 1 {
		return fmt.Errorf(
			"dex: expected exactly one Step output, found %d",
			len(result.Completions),
		)
	}
	return result.Completions[0].Output.Decode(target)
}

// SearchFlowEntry contains one Flow row returned by SearchFlows.
type SearchFlowEntry struct {
	// FlowID is the application-assigned Flow ID.
	FlowID string
	// RunID is the server-assigned run ID.
	RunID string
	// FlowType is the registered Flow type.
	FlowType string
	// Status is the indexed Flow status.
	Status FlowStatus
	// StartedAt is the indexed start time.
	StartedAt time.Time
	// ClosedAt is zero while open or when the close time is unavailable.
	ClosedAt time.Time
	// IndexedAttributes contains values keyed by their physical index names.
	IndexedAttributes map[string]Value
}

// SearchFlowsPage contains one page of Flow search results.
type SearchFlowsPage struct {
	// Flows are returned in server-defined query order.
	Flows []SearchFlowEntry
	// NextPageToken is opaque and empty on the last page.
	NextPageToken string
}

// HealthInfo describes one Dex health-check condition.
type HealthInfo struct {
	// Condition is the service-reported health state.
	Condition string
	// Hostname identifies the responding Dex server.
	Hostname string
	// Duration is the response duration in milliseconds.
	Duration int32
}

// StartFlow starts a new Flow execution and returns its server-assigned run ID.
//
// flow must be registered with the Client's Registry, flowID must be non-empty,
// and input must match the Flow's starting Step input type. options controls the
// request ID, timeout, worker target, initial attributes, and Flow configuration;
// omitted values use the registered Flow defaults. The method returns after the
// server accepts the Flow, not after the Flow completes.
//
// StartFlow returns FlowAlreadyStartedError when flowID identifies an existing
// Flow. It also returns validation, serialization, context, transport, or server
// errors without transferring ownership of input.
func (client *Client) StartFlow(
	ctx context.Context,
	flow Flow,
	flowID string,
	input any,
	options StartFlowOptions,
) (runID string, err error) {
	if err := client.validateFlowCall(ctx, flowID); err != nil {
		return "", err
	}
	registered, err := client.registry.resolveFlow(flow)
	if err != nil {
		return "", err
	}
	startStepType, stepInput, stepOptions, err := mapStartingStep(registered, input)
	if err != nil {
		return "", err
	}
	resolvedOptions, err := client.resolveStartFlowOptions(registered, options)
	if err != nil {
		return "", err
	}
	timeout, timeoutPolicy, flowOptions, err := mapStartFlowOptions(resolvedOptions)
	if err != nil {
		return "", err
	}
	flowOptions.Attributes, err = registered.appendInitialWorkQueuePermissions(
		flowOptions.Attributes,
	)
	if err != nil {
		return "", err
	}
	if err := client.requireHardDeleteAttributeSupport(ctx, flowOptions.Attributes); err != nil {
		return "", err
	}
	requestID, err := resolveStartRequestID(options.RequestID)
	if err != nil {
		return "", err
	}
	response, err := client.service.StartFlow(ctx, &dexpb.StartFlowRequest{
		FlowId:             flowID,
		FlowType:           registered.flowType,
		FlowTimeoutSeconds: timeout,
		FlowTimeoutPolicy:  timeoutPolicy,
		StartStepType:      startStepType,
		StepInput:          stepInput,
		StepOptions:        stepOptions,
		FlowStartOptions:   flowOptions,
		RequestId:          requestID,
	})
	if err != nil {
		return "", translateRPCError(err, "StartFlow", flowID, flowTargetNone)
	}
	if response == nil || response.RunId == "" {
		return "", fmt.Errorf("dex: StartFlow response has no run ID")
	}
	return response.RunId, nil
}

// requireHardDeleteAttributeSupport negotiates the Server protocol before the first hard-delete write.
func (client *Client) requireHardDeleteAttributeSupport(
	ctx context.Context,
	writes []*dexpb.AttributeWrite,
) error {
	if !slices.ContainsFunc(writes, (*dexpb.AttributeWrite).GetIsHardDeleteOnNull) {
		return nil
	}
	if client.serverProtocol.version.Load() == 0 {
		sdkVersion := currentGoSDKVersion()
		serverInfo, err := client.service.GetServerInfo(ctx, &emptypb.Empty{})
		if err != nil {
			return serverProtocolRequestError(sdkVersion, err)
		}
		negotiatedProtocolVersion, err := negotiateServerProtocol(serverInfo, sdkVersion)
		if err != nil {
			return err
		}
		client.serverProtocol.version.Store(negotiatedProtocolVersion)
	}
	return client.serverProtocol.requireHardDeleteAttributeSupport(writes)
}

func mapStartingStep(
	flow *registeredFlow,
	input any,
) (string, *dexpb.Value, *dexpb.StepOptions, error) {
	if flow.startingStep == nil {
		if !nilInterface(input) {
			return "", nil, nil, fmt.Errorf(
				"dex: flow %q has no starting step and requires nil input",
				flow.flowType,
			)
		}
		return "", nil, nil, nil
	}
	if !assignableValue(input, flow.startingStep.inputType) {
		return "", nil, nil, fmt.Errorf(
			"dex: starting input %T is not assignable to step %q input %s",
			input,
			flow.startingStep.stepType,
			flow.startingStep.inputType,
		)
	}
	encoded, err := encodeValue(input)
	if err != nil {
		return "", nil, nil, err
	}
	options, err := mapRegisteredStepOptions(flow.startingStep, nil)
	if err != nil {
		return "", nil, nil, err
	}
	return flow.startingStep.stepType, encoded, options, nil
}

func (client *Client) resolveStartFlowOptions(
	flow *registeredFlow,
	options StartFlowOptions,
) (StartFlowOptions, error) {
	resolved := options
	timeoutPolicy, err := resolveFlowTimeoutPolicy(
		flow,
		options.Timeout,
		options.TimeoutPolicy,
	)
	if err != nil {
		return StartFlowOptions{}, err
	}
	resolved.TimeoutPolicy = timeoutPolicy
	if err := flow.validateFlowTimeoutHandlerOptions(
		options.Timeout,
		timeoutPolicy,
		options.TimeoutHandlerOptions,
	); err != nil {
		return StartFlowOptions{}, err
	}
	attributes, err := validateInitialAttributes(flow, options.Attributes)
	if err != nil {
		return StartFlowOptions{}, err
	}
	resolved.Attributes = attributes
	config, err := client.resolveStartFlowConfig(options.ConfigOverride)
	if err != nil {
		return StartFlowOptions{}, err
	}
	resolved.ConfigOverride = config
	return resolved, nil
}

func resolveFlowTimeoutPolicy(
	flow *registeredFlow,
	timeout *time.Duration,
	policy FlowTimeoutPolicy,
) (FlowTimeoutPolicy, error) {
	hasPositiveTimeout := timeout != nil && *timeout > 0
	if !hasPositiveTimeout {
		if policy != TimeoutDefault {
			return TimeoutDefault,
				fmt.Errorf("dex: flow timeout policy requires a positive timeout")
		}
		return TimeoutDefault, nil
	}
	if policy == TimeoutDefault {
		if flow.timeoutHandler != nil {
			policy = TimeoutHandler
		} else {
			policy = TimeoutFail
		}
	}
	if policy == TimeoutHandler && flow.timeoutHandler == nil {
		return TimeoutDefault,
			fmt.Errorf("dex: flow %q does not implement FlowTimeoutHandler", flow.flowType)
	}
	return policy, nil
}

func validateInitialAttributes(
	flow *registeredFlow,
	definitions []InitialAttributeDef,
) ([]InitialAttributeDef, error) {
	resolved := make([]InitialAttributeDef, 0, len(definitions))
	seen := make(map[string]struct{}, len(definitions))
	for _, definition := range definitions {
		concrete, ok := definition.(initialAttribute)
		if !ok {
			return nil, fmt.Errorf("dex: invalid initial attribute %T", definition)
		}
		registered, found := flow.attributes[concrete.name]
		if !found {
			return nil, fmt.Errorf("dex: attribute %q is not declared", concrete.name)
		}
		if registered.isMap != concrete.isMap {
			return nil, fmt.Errorf(
				"dex: attribute %q static/map kind does not match its definition",
				concrete.name,
			)
		}
		if !reflect.DeepEqual(registered.index, concrete.index) {
			return nil, fmt.Errorf(
				"dex: attribute %q index does not match its registered definition",
				concrete.name,
			)
		}
		if registered.syncToAttributeStore != concrete.syncToAttributeStore {
			return nil, fmt.Errorf(
				"dex: attribute %q sync setting does not match its registered definition",
				concrete.name,
			)
		}
		if registered.isHardDeleteOnNull != concrete.isHardDeleteOnNull {
			return nil, fmt.Errorf(
				"dex: attribute %q hard-delete setting does not match its registered definition",
				concrete.name,
			)
		}
		physical, err := physicalName(concrete.name, concrete.instance, concrete.isMap)
		if err != nil {
			return nil, err
		}
		if _, found := seen[physical]; found {
			return nil, fmt.Errorf("dex: duplicate initial attribute %q", physical)
		}
		seen[physical] = struct{}{}
		encoded, indexConfig, err := encodeAttributeValue(concrete.value, registered.index)
		if err != nil {
			return nil, err
		}
		if registered.isHardDeleteOnNull {
			if err := validateHardDeleteAttributeValue(physical, encoded); err != nil {
				return nil, err
			}
		}
		concrete.index = registered.index
		concrete.encoded = encoded
		concrete.indexConfig = indexConfig
		concrete.syncToAttributeStore = registered.syncToAttributeStore
		resolved = append(resolved, concrete)
	}
	return resolved, nil
}

func (client *Client) resolveStartFlowConfig(config *FlowConfig) (*FlowConfig, error) {
	if config == nil && client.workerTarget == nil {
		return nil, nil
	}
	resolved := FlowConfig{}
	if config != nil {
		resolved = *config
	}
	if resolved.WorkerTarget == nil && client.workerTarget != nil {
		target := *client.workerTarget
		resolved.WorkerTarget = &target
	} else if resolved.WorkerTarget != nil {
		target := *resolved.WorkerTarget
		if err := validateWorkerTarget(target); err != nil {
			return nil, fmt.Errorf("dex: invalid StartFlow Worker target: %w", err)
		}
		resolved.WorkerTarget = &target
	}
	return &resolved, nil
}

func resolveStartRequestID(override *string) (string, error) {
	if override == nil {
		return newRequestID()
	}
	if *override == "" {
		return "", fmt.Errorf("dex: StartFlow request ID must not be empty")
	}
	return *override, nil
}

// StopFlow requests cancellation or termination of the active Flow identified by flowID.
//
// options selects the stop mode and optional reason. The method returns after the
// server accepts the request; it does not wait for the Flow to close. It returns
// FlowNotActiveOrNotFoundError when no active execution exists, plus validation, context,
// transport, or server errors.
func (client *Client) StopFlow(
	ctx context.Context,
	flowID string,
	options StopOptions,
) error {
	if err := client.validateFlowCall(ctx, flowID); err != nil {
		return err
	}
	stopType, reason, err := mapStopOptions(options)
	if err != nil {
		return err
	}
	_, err = client.service.StopFlow(ctx, &dexpb.StopFlowRequest{
		FlowId:   flowID,
		StopType: stopType,
		Reason:   reason,
	})
	return translateRPCError(err, "StopFlow", flowID, flowTargetActive)
}

// WaitForFlow blocks until a Flow closes or ctx ends.
//
// options controls whether Step completion results are included. The server may end one long poll
// at its configured cap and return LongPollTimeoutError; callers may repeat the call. Use
// context.WithTimeout or context.WithDeadline for a shorter Go-side wait.
func (client *Client) WaitForFlow(
	ctx context.Context,
	flowID string,
	options WaitForFlowOptions,
) (FlowResult, error) {
	if err := client.validateFlowCall(ctx, flowID); err != nil {
		return FlowResult{}, err
	}
	needsResults := mapWaitForFlowOptions(options)
	response, err := client.service.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{
		FlowId:          flowID,
		NeedsResults:    needsResults,
		WaitTimeSeconds: serverCappedLongPollSeconds,
	})
	if err != nil {
		return FlowResult{}, translateWaitRPCError(
			ctx,
			err,
			"WaitForFlow",
			flowID,
			flowTargetExisting,
		)
	}
	if err := client.hydrateValues(ctx, flowID, waitForFlowValuePointers(response)); err != nil {
		return FlowResult{}, err
	}
	result, err := mapFlowResult(response)
	if err != nil {
		return FlowResult{}, err
	}
	return result, nil
}

func waitForFlowValuePointers(response *dexpb.FlowResult) []**dexpb.Value {
	if response == nil {
		return nil
	}
	pointers := make([]**dexpb.Value, 0, len(response.Results))
	for _, completion := range response.Results {
		if completion != nil {
			pointers = append(pointers, &completion.CompletedStepOutput)
		}
	}
	return pointers
}

// SearchFlows returns one server-ordered page of Flow executions matching query.
//
// query uses the Dex visibility query language. pageSize must satisfy the server's
// accepted range, and nextPageToken must be empty for the first page or copied
// unchanged from a previous SearchFlowsPage. Indexed attributes are hydrated before
// return. An empty NextPageToken marks the final page. Validation, context,
// hydration, transport, and server failures are returned as errors.
func (client *Client) SearchFlows(
	ctx context.Context,
	query string,
	pageSize int32,
	nextPageToken string,
) (SearchFlowsPage, error) {
	return client.SearchFlowsWithOptions(ctx, query, pageSize, nextPageToken, SearchFlowsOptions{})
}

// SearchFlowsWithOptions searches Flow runs, optionally including earlier Continue-as-New runs.
func (client *Client) SearchFlowsWithOptions(
	ctx context.Context,
	query string,
	pageSize int32,
	nextPageToken string,
	options SearchFlowsOptions,
) (SearchFlowsPage, error) {
	if err := client.validateCall(ctx); err != nil {
		return SearchFlowsPage{}, err
	}
	request, err := mapSearchFlowsOptions(query, pageSize, nextPageToken)
	if err != nil {
		return SearchFlowsPage{}, err
	}
	request.IncludeContinuedAsNew = options.IncludeContinuedAsNew
	response, err := client.service.SearchFlows(ctx, request)
	if err != nil {
		return SearchFlowsPage{}, translateRPCError(err, "SearchFlows", "", flowTargetNone)
	}
	if err := client.hydrateFlowValues(ctx, searchFlowValuePointers(response)); err != nil {
		return SearchFlowsPage{}, err
	}
	return mapSearchFlowsPage(response)
}

func searchFlowValuePointers(response *dexpb.SearchFlowsResponse) []flowValuePointer {
	if response == nil {
		return nil
	}
	var pointers []flowValuePointer
	for _, flow := range response.FlowRuns {
		if flow == nil {
			continue
		}
		for _, attribute := range flow.IndexedAttributes {
			if attribute != nil {
				pointers = append(pointers, flowValuePointer{
					flowID:       flow.GetFlowId(),
					valuePointer: &attribute.Value,
				})
			}
		}
	}
	return pointers
}

// TimeTravel creates a new run from a selected point in an existing Flow's history.
//
// The new run keeps the Flow ID. Writes after the selected point are reapplied unless
// SkipWritesReapply is true. The returned run ID identifies the new execution.
//
// Example:
//
//	newRunID, err := client.TimeTravel(ctx, "order-123", dex.TimeTravelOptions{
//		Type:            dex.TimeTravelByStepExecutionID,
//		StepExecutionID: "ChargeOrder-2",
//		StepMethod:      dex.TimeTravelStepExecute,
//		Reason:          "retry after operator review",
//	})
//
// TimeTravel returns validation errors before sending the request. It returns context,
// transport, Flow-not-found, or server errors after the request is sent.
func (client *Client) TimeTravel(
	ctx context.Context,
	flowID string,
	options TimeTravelOptions,
) (newRunID string, err error) {
	if err := client.validateFlowCall(ctx, flowID); err != nil {
		return "", err
	}
	request, err := mapTimeTravelOptions(options)
	if err != nil {
		return "", err
	}
	request.FlowId = flowID
	response, err := client.service.ResetFlow(ctx, request)
	if err != nil {
		return "", translateRPCError(err, "TimeTravel", flowID, flowTargetExisting)
	}
	if response == nil || response.RunId == "" {
		return "", fmt.Errorf("dex: TimeTravel response has no run ID")
	}
	return response.RunId, nil
}

// SkipTimer makes one waiting Timer condition immediately ready.
//
// stepExecution identifies the Step type and execution number; a nil execution
// number means the first execution. timer must specify exactly one condition ID or
// zero-based condition index. The call affects only an active Flow and returns after
// the server accepts the skip. Invalid identifiers, inactive Flows, context,
// transport, and server failures are returned as errors.
func (client *Client) SkipTimer(
	ctx context.Context,
	flowID string,
	stepExecution StepExecutionID,
	timer TimerID,
) error {
	if err := client.validateFlowCall(ctx, flowID); err != nil {
		return err
	}
	executionNumber, err := effectiveExecutionNumber(stepExecution)
	if err != nil {
		return err
	}
	conditionID, conditionIndex, err := resolveTimerID(timer)
	if err != nil {
		return err
	}
	_, err = client.service.SkipTimer(ctx, &dexpb.SkipTimerRequest{
		FlowId:              flowID,
		StepExecutionId:     stepExecution.StepType + "-" + strconv.FormatInt(int64(executionNumber), 10),
		TimerConditionId:    conditionID,
		TimerConditionIndex: conditionIndex,
	})
	return translateRPCError(err, "SkipTimer", flowID, flowTargetActive)
}

func effectiveExecutionNumber(stepExecution StepExecutionID) (int32, error) {
	if stepExecution.StepType == "" {
		return 0, fmt.Errorf("dex: step type must not be empty")
	}
	if stepExecution.ExecutionNumber == nil {
		return 1, nil
	}
	if *stepExecution.ExecutionNumber <= 0 {
		return 0, fmt.Errorf("dex: step execution number must be positive")
	}
	return *stepExecution.ExecutionNumber, nil
}

func resolveTimerID(timer TimerID) (string, *int32, error) {
	hasID := timer.ConditionID != ""
	hasIndex := timer.Index != nil
	if hasID == hasIndex {
		return "", nil, fmt.Errorf("dex: timer requires exactly one condition ID or index")
	}
	if hasIndex && *timer.Index < 0 {
		return "", nil, fmt.Errorf("dex: timer condition index must not be negative")
	}
	return timer.ConditionID, timer.Index, nil
}

// UpdateFlowConfig replaces the mutable configuration of an active Flow.
//
// config is validated and serialized before the request. The update applies to
// later Flow decisions; work already dispatched is not recalled. The method
// returns validation, inactive-Flow, context, transport, or server errors.
func (client *Client) UpdateFlowConfig(
	ctx context.Context,
	flowID string,
	config FlowConfig,
) error {
	if err := client.validateFlowCall(ctx, flowID); err != nil {
		return err
	}
	mapped, err := mapFlowConfig(&config)
	if err != nil {
		return err
	}
	_, err = client.service.UpdateFlowConfig(ctx, &dexpb.UpdateFlowConfigRequest{
		FlowId:     flowID,
		FlowConfig: mapped,
	})
	return translateRPCError(err, "UpdateFlowConfig", flowID, flowTargetActive)
}

// WaitForStepCompletion blocks until a Step execution completes, the caller-visible request expires,
// or ctx ends.
//
// stepExecution identifies the Step type and execution number; nil means execution
// one. A nil error means the requested execution completed, but this method does not return its output.
// When options.RequestID is empty, the server derives a stable RequestID from the Step execution.
// RequestTimeout bounds the complete call across transparent transport reattachments. A positive
// value returns RequestTimeoutError. InternalHandlerTimeout reclaims accepted waits that outlive
// callers and could consume Temporal's in-flight Update limit. Active callers transparently roll
// to a new generation, which adds another Update to history.
// Invalid identifiers, inactive Flows, context, transport, and server errors are also returned.
func (client *Client) WaitForStepCompletion(
	ctx context.Context,
	flowID string,
	stepExecution StepExecutionID,
	options WaitForStepCompletionOptions,
) error {
	if err := client.validateFlowCall(ctx, flowID); err != nil {
		return err
	}
	executionNumber, err := effectiveExecutionNumber(stepExecution)
	if err != nil {
		return err
	}
	requestBudget, err := newClientRequestBudget(options.RequestTimeout)
	if err != nil {
		return err
	}
	internalHandlerTimeoutSeconds, err := exactDurationSeconds32(options.InternalHandlerTimeout)
	if err != nil {
		return err
	}
	for {
		remainingRequestTimeoutSeconds, err := requestBudget.remainingSeconds()
		if err != nil {
			return newRequestTimeoutError("WaitForStepCompletion", flowID)
		}
		requestCtx, cancelRequest, err := requestBudget.context(ctx)
		if err != nil {
			return newRequestTimeoutError("WaitForStepCompletion", flowID)
		}
		_, err = client.service.WaitForStepCompletion(
			requestCtx,
			&dexpb.WaitForStepCompletionRequest{
				FlowId:                        flowID,
				StepType:                      stepExecution.StepType,
				StepExecutionNumber:           strconv.FormatInt(int64(executionNumber), 10),
				RequestTimeoutSeconds:         remainingRequestTimeoutSeconds,
				InternalHandlerTimeoutSeconds: internalHandlerTimeoutSeconds,
				RequestId:                     options.RequestID,
			},
		)
		cancelRequest()
		if err == nil {
			return nil
		}
		translated := translateDurableWaitRPCError(
			ctx,
			requestBudget,
			err,
			"WaitForStepCompletion",
			flowID,
			flowTargetActive,
		)
		var longPollTimeout *LongPollTimeoutError
		if !errors.As(translated, &longPollTimeout) {
			return translated
		}
	}
}

// TriggerContinueAsNew asks an active Flow to roll its history into a new run.
//
// The request returns after the server accepts it and does not wait for the rollover.
// The Flow ID is preserved while the run ID changes. Empty identifiers, inactive
// Flows, context cancellation, transport failures, and server failures return errors.
func (client *Client) TriggerContinueAsNew(
	ctx context.Context,
	flowID string,
) error {
	if err := client.validateFlowCall(ctx, flowID); err != nil {
		return err
	}
	_, err := client.service.TriggerContinueAsNew(
		ctx,
		&dexpb.TriggerContinueAsNewRequest{FlowId: flowID},
	)
	return translateRPCError(err, "TriggerContinueAsNew", flowID, flowTargetActive)
}

// HealthCheck returns the responding Dex server's current health condition.
//
// The result includes the server hostname and reported duration in milliseconds.
// HealthCheck performs network I/O and honors ctx. It returns context, transport, or
// server errors when no valid response is available.
func (client *Client) HealthCheck(ctx context.Context) (HealthInfo, error) {
	if err := client.validateCall(ctx); err != nil {
		return HealthInfo{}, err
	}
	response, err := client.service.HealthCheck(ctx, &emptypb.Empty{})
	if err != nil {
		return HealthInfo{}, translateRPCError(err, "HealthCheck", "", flowTargetNone)
	}
	return mapHealthInfo(response)
}

// WriteStream appends one best-effort message with client-supplied source metadata.
//
// stream must be registered in exactly one Flow schema in this Client's Registry. flowID need not
// identify an existing or active Flow. source must be non-empty, may contain "#", and may be reused;
// every successful call appends a distinct message.
func (client *Client) WriteStream(
	ctx context.Context,
	flowID string,
	stream StreamDef,
	source string,
	value any,
) error {
	if err := client.validateFlowCall(ctx, flowID); err != nil {
		return err
	}
	if source == "" {
		return fmt.Errorf("dex: Stream source must not be empty")
	}
	flow, registered, err := client.registry.resolveStream(stream)
	if err != nil {
		return err
	}
	encoded, err := encodeValue(value)
	if err != nil {
		return err
	}
	_, err = client.service.WriteStream(ctx, &dexpb.WriteStreamRequest{
		FlowId:              flowID,
		FlowType:            flow.flowType,
		StreamName:          registered.definition.name,
		StreamCapacityBytes: registered.definition.streamCapacityBytes,
		Value:               encoded,
		Source:              source,
	})
	return translateRPCError(err, "WriteStream", flowID, flowTargetNone)
}

// ReadStream returns the next retained message after resumeToken and decodes it into valuePtr.
//
// An empty token starts at the current retained head. A token older than that head also returns the
// current head. The call blocks until a message arrives, the server's long-poll cap expires, or ctx
// is canceled. Use context.WithTimeout to impose a shorter Go-side deadline. Pass the returned
// StreamMessage.ResumeToken unchanged to resume after this message.
func (client *Client) ReadStream(
	ctx context.Context,
	flowID string,
	stream StreamDef,
	resumeToken string,
	valuePtr any,
) (StreamMessage, error) {
	if err := client.validateFlowCall(ctx, flowID); err != nil {
		return StreamMessage{}, err
	}
	flow, registered, err := client.registry.resolveStream(stream)
	if err != nil {
		return StreamMessage{}, err
	}
	response, err := client.service.ReadStream(ctx, &dexpb.ReadStreamRequest{
		FlowId:          flowID,
		FlowType:        flow.flowType,
		StreamName:      registered.definition.name,
		ResumeToken:     resumeToken,
		WaitTimeSeconds: serverCappedLongPollSeconds,
	})
	if err != nil {
		return StreamMessage{}, translateWaitRPCError(
			ctx,
			err,
			"ReadStream",
			flowID,
			flowTargetNone,
		)
	}
	if response == nil || response.Message == nil || response.Message.Value == nil ||
		response.Message.ResumeToken == "" || response.Message.CreatedTime == nil {
		return StreamMessage{}, fmt.Errorf("dex: ReadStream response is incomplete")
	}
	if err := response.Message.CreatedTime.CheckValid(); err != nil {
		return StreamMessage{}, fmt.Errorf("dex: ReadStream created time is invalid: %w", err)
	}
	if err := decodeValue(response.Message.Value, valuePtr); err != nil {
		return StreamMessage{}, err
	}
	return StreamMessage{
		ResumeToken: response.Message.ResumeToken,
		CreatedTime: response.Message.CreatedTime.AsTime(),
		Source:      response.Message.Source,
	}, nil
}

// ListStreamMessages decodes one newest-first page of retained messages into pagePtr.
//
// pagePtr must point to StreamMessagesPage[T] for the Stream's value type. pageSize must be
// positive and no greater than the server's configured limit. An empty beforePageToken starts at
// the retained tail. Otherwise pass StreamMessagesPage.NextPageToken unchanged to read older
// messages. The call does not wait for new messages. Stream trimming may remove messages between
// pages.
//
//	var page dex.StreamMessagesPage[string]
//	err := client.ListStreamMessages(ctx, "flow-1", Thinking, 100, "", &page)
func (client *Client) ListStreamMessages(
	ctx context.Context,
	flowID string,
	stream StreamDef,
	pageSize int32,
	beforePageToken string,
	pagePtr any,
) error {
	if err := client.validateFlowCall(ctx, flowID); err != nil {
		return err
	}
	if pageSize < 1 {
		return fmt.Errorf("dex: Stream page size must be positive")
	}
	flow, registered, err := client.registry.resolveStream(stream)
	if err != nil {
		return err
	}
	pageTarget, messageType, valueType, err := streamMessagesPageTarget(pagePtr)
	if err != nil {
		return err
	}
	response, err := client.service.ListStreamMessages(ctx, &dexpb.ListStreamMessagesRequest{
		FlowId:          flowID,
		FlowType:        flow.flowType,
		StreamName:      registered.definition.name,
		PageSize:        pageSize,
		BeforePageToken: beforePageToken,
	})
	if err != nil {
		return translateRPCError(err, "ListStreamMessages", flowID, flowTargetNone)
	}
	if response == nil {
		return fmt.Errorf("dex: ListStreamMessages response is nil")
	}
	valuePointers := make([]**dexpb.Value, 0, len(response.Messages))
	for _, message := range response.Messages {
		if message == nil || message.Value == nil || message.ResumeToken == "" ||
			message.CreatedTime == nil {
			return fmt.Errorf("dex: ListStreamMessages response contains an incomplete message")
		}
		valuePointers = append(valuePointers, &message.Value)
	}
	if err := client.hydrateValues(ctx, flowID, valuePointers); err != nil {
		return err
	}
	messagesTarget := pageTarget.FieldByName("Messages")
	messages := reflect.MakeSlice(messagesTarget.Type(), 0, len(response.Messages))
	for _, message := range response.Messages {
		if err := message.CreatedTime.CheckValid(); err != nil {
			return fmt.Errorf("dex: ListStreamMessages created time is invalid: %w", err)
		}
		value, decodeErr := decodeReflectValue(message.Value, valueType)
		if decodeErr != nil {
			return decodeErr
		}
		entry := reflect.New(messageType).Elem()
		entry.FieldByName("Value").Set(value)
		entry.FieldByName("ResumeToken").SetString(message.ResumeToken)
		entry.FieldByName("CreatedTime").Set(reflect.ValueOf(message.CreatedTime.AsTime()))
		entry.FieldByName("Source").SetString(message.Source)
		messages = reflect.Append(messages, entry)
	}
	messagesTarget.Set(messages)
	pageTarget.FieldByName("NextPageToken").SetString(response.NextPageToken)
	return nil
}

func streamMessagesPageTarget(
	pagePtr any,
) (reflect.Value, reflect.Type, reflect.Type, error) {
	if pagePtr == nil {
		return reflect.Value{}, nil, nil, fmt.Errorf("dex: Stream page target must be a non-nil pointer")
	}
	target := reflect.ValueOf(pagePtr)
	if target.Kind() != reflect.Pointer || target.IsNil() || target.Elem().Kind() != reflect.Struct {
		return reflect.Value{}, nil, nil, fmt.Errorf("dex: Stream page target must point to StreamMessagesPage[T]")
	}
	pageType := target.Elem().Type()
	if pageType.NumField() != 2 || pageType.Field(0).Name != "Messages" ||
		pageType.Field(0).Type.Kind() != reflect.Slice ||
		pageType.Field(1).Name != "NextPageToken" || pageType.Field(1).Type.Kind() != reflect.String {
		return reflect.Value{}, nil, nil, fmt.Errorf("dex: Stream page target must point to StreamMessagesPage[T]")
	}
	messageType := pageType.Field(0).Type.Elem()
	if messageType.Kind() != reflect.Struct || messageType.NumField() != 4 ||
		messageType.Field(0).Name != "Value" ||
		messageType.Field(1).Name != "ResumeToken" || messageType.Field(1).Type.Kind() != reflect.String ||
		messageType.Field(2).Name != "CreatedTime" || messageType.Field(2).Type != reflect.TypeOf(time.Time{}) ||
		messageType.Field(3).Name != "Source" || messageType.Field(3).Type.Kind() != reflect.String {
		return reflect.Value{}, nil, nil, fmt.Errorf("dex: Stream page target must point to StreamMessagesPage[T]")
	}
	return target.Elem(), messageType, messageType.Field(0).Type, nil
}

// WaitForAttributeMatch blocks until a singleton Attribute satisfies match.
//
// match must contain an operand matching the registered Attribute type. The
// matched current value is decoded into valuePtr before this method returns.
// valuePtr must be a non-nil pointer of the registered type. When options.RequestID is empty, the
// server derives one from the Attribute condition.
// RequestTimeout bounds the complete call across transparent transport reattachments. A positive
// value returns RequestTimeoutError. InternalHandlerTimeout reclaims accepted waits that outlive
// callers and could consume Temporal's in-flight Update limit. Active callers transparently roll
// to a new generation, which adds another Update to history.
// Use context.WithTimeout or context.WithDeadline to bound the caller-visible response.
func (client *Client) WaitForAttributeMatch(
	ctx context.Context,
	flowID string,
	attribute AttributeDef,
	match AttributeMatchDef,
	valuePtr any,
	options WaitForAttributeOptions,
) error {
	return client.waitForAttributeMatch(
		ctx,
		flowID,
		attribute,
		"",
		false,
		match,
		valuePtr,
		options,
	)
}

// WaitForAttributeMapInstanceMatch blocks until one AttributeMap instance satisfies match.
//
// instance identifies the map entry. Slash is prohibited because it is reserved.
// The matched current value is decoded into valuePtr. The match operand and
// valuePtr must use the registered AttributeMap value type. options has the same required RequestID,
// wait-budget, and error behavior as WaitForAttributeMatch.
func (client *Client) WaitForAttributeMapInstanceMatch(
	ctx context.Context,
	flowID string,
	attribute AttributeDef,
	instance string,
	match AttributeMatchDef,
	valuePtr any,
	options WaitForAttributeOptions,
) error {
	return client.waitForAttributeMatch(
		ctx,
		flowID,
		attribute,
		instance,
		true,
		match,
		valuePtr,
		options,
	)
}

func (client *Client) waitForAttributeMatch(
	ctx context.Context,
	flowID string,
	attribute AttributeDef,
	instance string,
	isMap bool,
	match AttributeMatchDef,
	valuePtr any,
	options WaitForAttributeOptions,
) error {
	if err := client.validateFlowCall(ctx, flowID); err != nil {
		return err
	}
	if match == nil {
		return fmt.Errorf("dex: AttributeMatch is required")
	}
	if _, err := decodeTarget(valuePtr); err != nil {
		return err
	}
	registered, err := client.registry.resolveAttribute(attribute, isMap)
	if err != nil {
		return err
	}
	if registered.isHardDeleteOnNull {
		return fmt.Errorf("dex: attribute %q uses HardDeleteOnNull and cannot be waited on", registered.name)
	}
	name, err := physicalName(registered.name, instance, isMap)
	if err != nil {
		return err
	}
	encoded, err := encodeValue(match.attributeMatchOperand())
	if err != nil {
		return err
	}
	if err := validateEncodedAttributeMatch(match.attributeMatchOperator(), encoded); err != nil {
		return err
	}
	requestBudget, err := newClientRequestBudget(options.RequestTimeout)
	if err != nil {
		return err
	}
	internalHandlerTimeoutSeconds, err := exactDurationSeconds32(options.InternalHandlerTimeout)
	if err != nil {
		return err
	}
	for {
		remainingRequestTimeoutSeconds, err := requestBudget.remainingSeconds()
		if err != nil {
			return newRequestTimeoutError("WaitForAttribute", flowID)
		}
		requestCtx, cancelRequest, err := requestBudget.context(ctx)
		if err != nil {
			return newRequestTimeoutError("WaitForAttribute", flowID)
		}
		response, waitErr := client.service.WaitForAttribute(requestCtx, &dexpb.WaitForAttributeRequest{
			FlowId: flowID,
			Match: &dexpb.AttributeMatch{
				Key:      name,
				Operator: match.attributeMatchOperator(),
				Operand:  encoded,
			},
			RequestTimeoutSeconds:         remainingRequestTimeoutSeconds,
			InternalHandlerTimeoutSeconds: internalHandlerTimeoutSeconds,
			RequestId:                     options.RequestID,
		})
		cancelRequest()
		if waitErr == nil {
			if response.GetMatchedValue() == nil {
				return fmt.Errorf("dex: WaitForAttribute response is incomplete")
			}
			return decodeValue(response.GetMatchedValue(), valuePtr)
		}
		translated := translateDurableWaitRPCError(
			ctx,
			requestBudget,
			waitErr,
			"WaitForAttribute",
			flowID,
			flowTargetActive,
		)
		var longPollTimeout *LongPollTimeoutError
		if !errors.As(translated, &longPollTimeout) {
			return translated
		}
	}
}

type clientRequestBudget struct {
	deadline time.Time
}

func newClientRequestBudget(requestTimeout time.Duration) (*clientRequestBudget, error) {
	if _, err := exactDurationSeconds32(requestTimeout); err != nil {
		return nil, err
	}
	requestBudget := &clientRequestBudget{}
	if requestTimeout > 0 {
		requestBudget.deadline = time.Now().Add(requestTimeout)
	}
	return requestBudget, nil
}

func (b *clientRequestBudget) hasExpired() bool {
	return !b.deadline.IsZero() && !time.Now().Before(b.deadline)
}

func (b *clientRequestBudget) remainingSeconds() (int32, error) {
	if b.deadline.IsZero() {
		return 0, nil
	}
	remaining := time.Until(b.deadline)
	if remaining <= 0 {
		return 0, context.DeadlineExceeded
	}
	seconds := (remaining + time.Second - 1) / time.Second
	return int32(seconds), nil
}

func (b *clientRequestBudget) context(parent context.Context) (context.Context, context.CancelFunc, error) {
	if b.deadline.IsZero() {
		requestCtx, cancelRequest := context.WithCancel(parent)
		return requestCtx, cancelRequest, nil
	}
	if !time.Now().Before(b.deadline) {
		return nil, nil, context.DeadlineExceeded
	}
	requestCtx, cancelRequest := context.WithDeadline(parent, b.deadline)
	return requestCtx, cancelRequest, nil
}

func (b *clientRequestBudget) hasDeadline() bool {
	return !b.deadline.IsZero()
}

func newRequestTimeoutError(operation string, flowID string) error {
	serviceError := &ServiceError{
		Op:        operation,
		FlowID:    flowID,
		Code:      codes.DeadlineExceeded,
		SubStatus: ErrorSubStatusRequestTimeout,
		Detail:    "request timed out",
	}
	return &RequestTimeoutError{ServiceError: serviceError}
}

func translateDurableWaitRPCError(
	ctx context.Context,
	requestBudget *clientRequestBudget,
	err error,
	op string,
	flowID string,
	target flowTargetRequirement,
) error {
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	if status.Code(err) == codes.Canceled && requestBudget.hasExpired() {
		// The server can reset the stream with CANCEL as the shared deadline passes.
		return newRequestTimeoutError(op, flowID)
	}
	if status.Code(err) == codes.DeadlineExceeded {
		// Transport deadlines can fire before the matching context timer.
		contextDeadline, hasContextDeadline := ctx.Deadline()
		if hasContextDeadline && (!requestBudget.hasDeadline() || !contextDeadline.After(requestBudget.deadline)) {
			return context.DeadlineExceeded
		}
		if requestBudget.hasDeadline() {
			return newRequestTimeoutError(op, flowID)
		}
	}
	return translateRPCError(err, op, flowID, target)
}

func translateWaitRPCError(
	ctx context.Context,
	err error,
	op string,
	flowID string,
	target flowTargetRequirement,
) error {
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	if status.Code(err) == codes.DeadlineExceeded {
		_, hasDeadline := ctx.Deadline()
		if hasDeadline {
			return context.DeadlineExceeded
		}
	}
	return translateRPCError(err, op, flowID, target)
}

func isPrimitiveValue(value *dexpb.Value) bool {
	switch value.GetKind().(type) {
	case *dexpb.Value_StringValue,
		*dexpb.Value_BoolValue,
		*dexpb.Value_IntValue,
		*dexpb.Value_DoubleValue:
		return true
	default:
		return false
	}
}

// InvokeRPC synchronously invokes a registered RPC on a Flow execution.
//
// rpc identifies an RPC definition belonging to the Flow type, input must match its
// input type, and outputPtr may be nil to discard the RPC output. Otherwise outputPtr
// must be a non-nil pointer to the RPC output type. The RPC's registered RPCOptions
// control timeout, locks, transactional execution, and selective state loading.
// A non-transactional RPC without Attribute locks starts from a backend query. If its
// handler returns no durable effects, a retained terminal execution can serve the query.
// Locks, transactional execution, returned effects, or server policy can require an
// active execution and cause FlowNotActiveOrNotFoundError for a terminal Flow. A missing
// execution also returns FlowNotActiveOrNotFoundError, including on the query-only path.
// InvokeRPC blocks until the handler returns, the timeout expires, or ctx is canceled,
// then decodes the result into outputPtr when one is provided.
// It may return validation, serialization, lock-conflict, worker, inactive-Flow,
// context, hydration, transport, or server errors. outputPtr is not owned by Dex.
//
//	var result Quote
//	err := client.InvokeRPC(ctx, "order-42", quoteRPC, request, &result)
func (client *Client) InvokeRPC(
	ctx context.Context,
	flowID string,
	rpc any,
	input any,
	outputPtr any,
) error {
	return client.doInvokeRPC(ctx, flowID, rpc, input, outputPtr, RPCInvokeOptions{})
}

// InvokeRPCWithOptions synchronously invokes a registered RPC with runtime-selected map instances.
//
// options adds exact AttributeMap locks, AttributeMap loads, and ChannelMap loads to the immutable
// RPCOptions registered with the handler. Duplicate selections are sent once. Invocation options
// cannot remove registered state requirements. A dynamic AttributeMap read-modify-write must lock
// and load the same instance.
//
// The input, output, blocking, and error behavior otherwise matches InvokeRPC.
//
//	options := dex.RPCInvokeOptions{
//		LockAttributeMapInstances: []dex.AttributeLock{
//			dex.LockAttributeMap(OrdersByTenant, tenantID),
//		},
//		LoadAttributeMapInstances: []dex.AttributeMapLoad{
//			OrdersByTenant.Load(tenantID),
//		},
//	}
//	err := client.InvokeRPCWithOptions(ctx, flowID, updateRPC, input, &output, options)
func (client *Client) InvokeRPCWithOptions(
	ctx context.Context,
	flowID string,
	rpc any,
	input any,
	outputPtr any,
	options RPCInvokeOptions,
) error {
	return client.doInvokeRPC(ctx, flowID, rpc, input, outputPtr, options)
}

func (client *Client) doInvokeRPC(
	ctx context.Context,
	flowID string,
	rpc any,
	input any,
	outputPtr any,
	options RPCInvokeOptions,
) error {
	if err := client.validateFlowCall(ctx, flowID); err != nil {
		return err
	}
	flow, registered, err := client.registry.resolveRPC(rpc)
	if err != nil {
		return err
	}
	if !assignableValue(input, registered.input) {
		return fmt.Errorf(
			"dex: RPC input %T is not assignable to %s",
			input,
			registered.input,
		)
	}
	if outputPtr == nil {
		outputPtr = reflect.New(registered.output).Interface()
	}
	outputTarget, err := decodeTarget(outputPtr)
	if err != nil {
		return err
	}
	if outputTarget.Type() != registered.output {
		return fmt.Errorf(
			"dex: RPC output target %s does not match %s",
			outputTarget.Type(),
			registered.output,
		)
	}
	attributeMapInstances, channelNames, channelMapInstances, err := validateRPCStateLoads(
		flow,
		registered.options,
	)
	if err != nil {
		return err
	}
	invokeAttributeMapInstances, _, invokeChannelMapInstances, err := validateStateLoads(
		flow,
		stateLoads{
			attributeMapInstances: options.LoadAttributeMapInstances,
			channelMapInstances:   options.LoadChannelMapInstances,
		},
	)
	if err != nil {
		return err
	}
	attributeMapInstances = mergeSortedUniqueNames(
		attributeMapInstances,
		invokeAttributeMapInstances,
	)
	channelMapInstances = mergeSortedUniqueNames(
		channelMapInstances,
		invokeChannelMapInstances,
	)
	timeout, locks, err := mapRPCOptions(registered.options)
	if err != nil {
		return err
	}
	invokeLocks, err := validateRPCInvokeAttributeMapLocks(
		flow,
		options.LockAttributeMapInstances,
	)
	if err != nil {
		return err
	}
	locks = mergeSortedUniqueNames(locks, invokeLocks)
	encoded, err := encodeValue(input)
	if err != nil {
		return err
	}
	requestID, err := newRequestID()
	if err != nil {
		return err
	}
	response, err := client.service.InvokeRPC(ctx, &dexpb.InvokeRPCRequest{
		FlowId:                    flowID,
		RpcName:                   registered.durableName,
		Input:                     encoded,
		TimeoutSeconds:            timeout,
		LockAttributeKeys:         locks,
		RequestId:                 requestID,
		IsTransactional:           registered.options != nil && registered.options.IsTransactional,
		LoadAttributeMapInstances: attributeMapInstances,
		LoadChannelNames:          channelNames,
		LoadChannelMapInstances:   channelMapInstances,
	})
	if err != nil {
		return translateRPCError(err, "InvokeRPC", flowID, flowTargetActive)
	}
	if response == nil {
		return fmt.Errorf("dex: InvokeRPC response is nil")
	}
	if err := client.hydrateValues(
		ctx,
		flowID,
		[]**dexpb.Value{&response.Output},
	); err != nil {
		return err
	}
	return decodeValue(response.Output, outputPtr)
}

func validateRPCInvokeAttributeMapLocks(
	flow *registeredFlow,
	locks []AttributeLock,
) ([]string, error) {
	mapped := make([]string, 0, len(locks))
	for _, lock := range locks {
		concrete, ok := lock.(attributeLock)
		if !ok {
			return nil, fmt.Errorf("dex: invalid AttributeMap lock %T", lock)
		}
		if !concrete.isMap {
			return nil, fmt.Errorf(
				"dex: invocation lock %q is not an AttributeMap instance",
				concrete.name,
			)
		}
		attribute, found := flow.attributes[concrete.name]
		if !found || !attribute.isMap {
			return nil, fmt.Errorf(
				"dex: AttributeMap %q is not registered with Flow %q",
				concrete.name,
				flow.flowType,
			)
		}
		name, err := physicalName(concrete.name, concrete.instance, true)
		if err != nil {
			return nil, err
		}
		mapped = append(mapped, name)
	}
	return mergeSortedUniqueNames(mapped), nil
}

func mergeSortedUniqueNames(groups ...[]string) []string {
	seen := make(map[string]struct{})
	for _, group := range groups {
		for _, name := range group {
			seen[name] = struct{}{}
		}
	}
	merged := make([]string, 0, len(seen))
	for name := range seen {
		merged = append(merged, name)
	}
	sort.Strings(merged)
	return merged
}

func validateRPCStateLoads(
	flow *registeredFlow,
	options *RPCOptions,
) ([]string, []string, []string, error) {
	return validateStateLoads(flow, rpcStateLoads(options))
}

type stateLoads struct {
	attributeMaps         []AttributeDef
	attributeMapInstances []AttributeMapLoad
	channels              []ChannelDef
	channelMaps           []ChannelDef
	channelMapInstances   []ChannelMapLoad
}

func validateStateLoads(
	flow *registeredFlow,
	loads stateLoads,
) ([]string, []string, []string, error) {
	attributeMaps := make([]string, 0, len(loads.attributeMaps)+len(loads.attributeMapInstances))
	for _, attributeMap := range loads.attributeMaps {
		if attributeMap == nil {
			return nil, nil, nil, fmt.Errorf("dex: AttributeMap load must not be nil")
		}
		name := attributeMap.attributeName()
		if !attributeMap.attributeIsMap() {
			return nil, nil, nil, fmt.Errorf("dex: AttributeMap load %q is not an AttributeMap", name)
		}
		attribute, found := registeredAttribute{}, false
		if flow != nil {
			attribute, found = flow.attributes[name]
		}
		if flow != nil && (!found || !attribute.isMap) {
			return nil, nil, nil, fmt.Errorf(
				"dex: AttributeMap %q is not registered with Flow %q",
				name,
				flow.flowType,
			)
		}
		attributeMaps = append(attributeMaps, name+"/")
	}
	for _, load := range loads.attributeMapInstances {
		attribute, found := registeredAttribute{}, false
		if flow != nil {
			attribute, found = flow.attributes[load.name]
		}
		if flow != nil && (!found || !attribute.isMap) {
			return nil, nil, nil, fmt.Errorf(
				"dex: AttributeMap %q is not registered with Flow %q",
				load.name,
				flow.flowType,
			)
		}
		instance, err := physicalName(load.name, load.instance, true)
		if err != nil {
			return nil, nil, nil, err
		}
		attributeMaps = append(attributeMaps, instance)
	}
	channels := make([]string, 0, len(loads.channels))
	for _, channelDefinition := range loads.channels {
		if channelDefinition == nil {
			return nil, nil, nil, fmt.Errorf("dex: Channel load must not be nil")
		}
		name := channelDefinition.channelName()
		if channelDefinition.channelIsMap() {
			return nil, nil, nil, fmt.Errorf("dex: Channel load %q is a ChannelMap", name)
		}
		channel, found := registeredChannel{}, false
		if flow != nil {
			channel, found = flow.channels[name]
		}
		if flow != nil && (!found || channel.isMap) {
			return nil, nil, nil, fmt.Errorf(
				"dex: Channel %q is not registered with Flow %q",
				name,
				flow.flowType,
			)
		}
		channels = append(channels, name)
	}
	channelMaps := make([]string, 0, len(loads.channelMaps)+len(loads.channelMapInstances))
	for _, channelMap := range loads.channelMaps {
		if channelMap == nil {
			return nil, nil, nil, fmt.Errorf("dex: ChannelMap load must not be nil")
		}
		name := channelMap.channelName()
		if !channelMap.channelIsMap() {
			return nil, nil, nil, fmt.Errorf("dex: ChannelMap load %q is not a ChannelMap", name)
		}
		channel, found := registeredChannel{}, false
		if flow != nil {
			channel, found = flow.channels[name]
		}
		if flow != nil && (!found || !channel.isMap) {
			return nil, nil, nil, fmt.Errorf(
				"dex: ChannelMap %q is not registered with Flow %q",
				name,
				flow.flowType,
			)
		}
		channelMaps = append(channelMaps, name+"/")
	}
	for _, load := range loads.channelMapInstances {
		channel, found := registeredChannel{}, false
		if flow != nil {
			channel, found = flow.channels[load.name]
		}
		if flow != nil && (!found || !channel.isMap) {
			return nil, nil, nil, fmt.Errorf(
				"dex: ChannelMap %q is not registered with Flow %q",
				load.name,
				flow.flowType,
			)
		}
		instance, err := physicalName(load.name, load.instance, true)
		if err != nil {
			return nil, nil, nil, err
		}
		channelMaps = append(channelMaps, instance)
	}
	if err := validateUniqueStateLoadNames("AttributeMap", attributeMaps); err != nil {
		return nil, nil, nil, err
	}
	if err := validateUniqueStateLoadNames("Channel", channels); err != nil {
		return nil, nil, nil, err
	}
	if err := validateUniqueStateLoadNames("ChannelMap", channelMaps); err != nil {
		return nil, nil, nil, err
	}
	sort.Strings(attributeMaps)
	sort.Strings(channels)
	sort.Strings(channelMaps)
	return attributeMaps, channels, channelMaps, nil
}

func validateUniqueStateLoadNames(kind string, names []string) error {
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if _, found := seen[name]; found {
			return fmt.Errorf("dex: duplicate %s load %q", kind, name)
		}
		seen[name] = struct{}{}
	}
	return nil
}
