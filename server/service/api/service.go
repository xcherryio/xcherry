// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package api

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service"
	uclient "github.com/superdurable/dex/service/client"
	"github.com/superdurable/dex/service/client/history"
	"github.com/superdurable/dex/service/common/attributestore"
	"github.com/superdurable/dex/service/common/blobstore"
	"github.com/superdurable/dex/service/common/channelmessage"
	serviceerrors "github.com/superdurable/dex/service/common/errors"
	"github.com/superdurable/dex/service/common/grpctarget"
	"github.com/superdurable/dex/service/common/index"
	"github.com/superdurable/dex/service/common/log"
	"github.com/superdurable/dex/service/common/log/tag"
	"github.com/superdurable/dex/service/common/ptr"
	"github.com/superdurable/dex/service/common/pulldispatch"
	"github.com/superdurable/dex/service/common/retry"
	"github.com/superdurable/dex/service/common/rpc"
	"github.com/superdurable/dex/service/common/streamstore"
	"github.com/superdurable/dex/service/common/utils"
	"github.com/superdurable/dex/service/common/workerclient"
	"github.com/superdurable/dex/service/indexsync"
	interpreterconfig "github.com/superdurable/dex/service/interpreter/config"
	"github.com/superdurable/dex/service/wake"
	"go.temporal.io/api/serviceerror"
	"go.uber.org/cadence/.gen/go/shared"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultHistoryPageSize                 = 100
	waitForStepCompletionUpdateIDNamespace = "wait-for-step-completion:"
	waitForAttributeUpdateIDNamespace      = "wait-for-attribute:"
)

var errWaitRequestDeadlineExceeded = errors.New("wait request deadline exceeded")

type serviceImpl struct {
	client             uclient.UnifiedClient
	store              blobstore.BlobStore
	taskQueue          string
	logger             log.Logger
	apiCfg             *config.ApiConfig
	blobStoreCfg       *config.BlobStoreConfig
	interpreterCfg     *config.Interpreter
	workerPool         *workerclient.WorkerClientPool
	stepInputPopulator *history.AsyncStepInputSnapshotPopulator
	attributeStore     *attributestore.Manager
	indexSynchronizer  *indexsync.Synchronizer
	streamStore        *streamstore.Store
	wakeNotifier       *wake.Notifier
	pullDispatcher     pulldispatch.Dispatcher
	matchingCfg        *config.MatchingConfig
}

func NewApiServiceWithPull(
	apiCfg *config.ApiConfig,
	blobStoreCfg *config.BlobStoreConfig,
	interpreterCfg *config.Interpreter,
	client uclient.UnifiedClient,
	taskQueue string,
	logger log.Logger,
	store blobstore.BlobStore,
	attributeStore *attributestore.Manager,
	streamStore *streamstore.Store,
	workerPool *workerclient.WorkerClientPool,
	wakeNotifier *wake.Notifier,
	pullDispatcher pulldispatch.Dispatcher,
	matchingCfg *config.MatchingConfig,
) (ApiService, error) {
	if apiCfg == nil || blobStoreCfg == nil || interpreterCfg == nil || wakeNotifier == nil || pullDispatcher == nil {
		panic("API service requires non-nil config sections")
	}
	if client == nil || logger == nil || workerPool == nil || attributeStore == nil || streamStore == nil || taskQueue == "" {
		panic("API service requires non-nil dependencies and a task queue")
	}
	if blobStoreCfg.EffectiveEnabled() && store == nil {
		panic("API service requires a blob store when blob storage is enabled")
	}
	return &serviceImpl{
		apiCfg:             apiCfg,
		blobStoreCfg:       blobStoreCfg,
		client:             client,
		store:              store,
		taskQueue:          taskQueue,
		logger:             logger,
		interpreterCfg:     interpreterCfg,
		workerPool:         workerPool,
		stepInputPopulator: history.NewAsyncStepInputSnapshotPopulator(blobStoreCfg, client, store),
		attributeStore:     attributeStore,
		indexSynchronizer:  indexsync.New(interpreterCfg, client, logger),
		streamStore:        streamStore,
		wakeNotifier:       wakeNotifier,
		pullDispatcher:     pullDispatcher,
		matchingCfg:        matchingCfg,
	}, nil
}

func (s *serviceImpl) Close() {
	s.wakeNotifier.Close()
	s.client.Close()
}

func (s *serviceImpl) GetServerInfo(
	context.Context,
	*emptypb.Empty,
) (*dexpb.ServerInfo, error) {
	return &dexpb.ServerInfo{
		ServerVersion:                   service.DexServerVersion,
		MinimumSupportedProtocolVersion: service.MinimumSupportedProtocolVersion,
		CurrentProtocolVersion:          service.CurrentProtocolVersion,
	}, nil
}

func (s *serviceImpl) StartFlow(
	ctx context.Context,
	req *dexpb.StartFlowRequest,
) (*dexpb.StartFlowResponse, error) {
	if req == nil || req.GetFlowId() == "" || req.GetFlowType() == "" {
		return nil, makeInvalidRequestError("flow ID and flow type are required")
	}
	if req.GetRequestId() == "" {
		return nil, makeInvalidRequestError("request ID is required")
	}
	timeoutPolicy, err := service.ResolveFlowTimeoutPolicy(
		req.GetFlowTimeoutSeconds(),
		req.GetFlowTimeoutPolicy(),
	)
	if err != nil {
		return nil, makeInvalidRequestError(err.Error())
	}
	if err := workerclient.RejectWorkerBlobIDs(req.GetStepInput()); err != nil {
		return nil, makeInvalidRequestError(err.Error())
	}

	startOptions := req.GetFlowStartOptions()
	if err := service.ValidateFlowTimeoutHandlerOptions(
		req.GetFlowTimeoutSeconds(),
		timeoutPolicy,
		startOptions.GetTimeoutHandlerOptions(),
		s.interpreterCfg.InterpreterActivityConfig.EffectiveMinimumStepHeartbeatTimeout(),
	); err != nil {
		return nil, makeInvalidRequestError(err.Error())
	}
	attributes := startOptions.GetAttributes()
	if err := validateAttributeWrites(attributes); err != nil {
		return nil, makeInvalidRequestError(err.Error())
	}
	if err := s.validateHardDeleteAttributeStorage(attributes); err != nil {
		return nil, err
	}

	searchAttributes := index.ConvertAttributeWritesToSearchAttributeUpsertMap(attributes)
	searchAttributes[service.SearchAttributeDexWorkflowType] = req.GetFlowType()
	if err := service.ValidateFlowID(req.GetFlowId()); err != nil {
		return nil, makeInvalidRequestError(err.Error())
	}
	if err := service.ValidateStepType(req.GetStartStepType()); err != nil {
		return nil, makeInvalidRequestError(err.Error())
	}
	if err := service.ValidateStepOptions(
		req.GetStepOptions(),
		s.interpreterCfg.InterpreterActivityConfig.EffectiveMinimumStepHeartbeatTimeout(),
	); err != nil {
		return nil, makeInvalidRequestError(err.Error())
	}
	if err := blobstore.OffloadLargeValue(
		ctx,
		req.GetStepInput(),
		req.GetFlowId(),
		req.GetRequestId(),
		s.blobStoreCfg.EffectiveThresholdInBytes(),
		s.store,
		s.blobStoreCfg.EffectiveEnabled(),
	); err != nil {
		return nil, s.handleError(err)
	}
	if err := blobstore.OffloadLargeAttributeWrites(
		ctx,
		attributes,
		req.GetFlowId(),
		req.GetRequestId(),
		s.blobStoreCfg.EffectiveThresholdInBytes(),
		s.store,
		s.blobStoreCfg.EffectiveEnabled(),
	); err != nil {
		return nil, s.handleError(err)
	}

	baseConfig := s.interpreterCfg.DefaultWorkflowConfig
	if s.interpreterCfg.DefaultWorkflowConfig == nil {
		baseConfig = config.DefaultWorkflowConfig
	}
	workflowConfig := proto.Clone(baseConfig).(*dexpb.FlowConfig)
	if startOptions.GetFlowConfigOverride() != nil {
		overrideWorkflowConfig(startOptions.GetFlowConfigOverride(), workflowConfig)
	}
	if err := interpreterconfig.ValidateFlowConfig(workflowConfig); err != nil {
		return nil, makeInvalidRequestError(err.Error())
	}
	if err := s.validateAttributeStoreNames(ctx, workflowConfig.GetAttributeStoreNames()); err != nil {
		return nil, err
	}
	workerTarget, err := grpctarget.NormalizeWorkerTarget(workflowConfig.GetWorkerTarget())
	if err != nil {
		return nil, makeInvalidRequestError(err.Error())
	}
	workflowConfig.WorkerTarget = workerTarget

	workflowOptions := uclient.StartWorkflowOptions{
		ID:               req.GetFlowId(),
		TaskQueue:        s.taskQueue,
		SearchAttributes: searchAttributes,
		Memo: map[string]interface{}{
			service.ReqId: &dexpb.EncodedObject{
				Payload: []byte(req.GetRequestId()),
			},
		},
		IdReusePolicy: ptr.Any(dexpb.IdReusePolicy_ID_REUSE_POLICY_DISALLOW_REUSE),
	}
	ignoreAlreadyStartedError := false
	if startOptions != nil {
		if startOptions.GetIdReusePolicy() != dexpb.IdReusePolicy_ID_REUSE_POLICY_UNSPECIFIED {
			if _, known := dexpb.IdReusePolicy_name[int32(startOptions.GetIdReusePolicy())]; !known {
				return nil, makeInvalidRequestError("unknown ID reuse policy")
			}
			workflowOptions.IdReusePolicy = ptr.Any(startOptions.GetIdReusePolicy())
		}
		workflowOptions.RetryPolicy = startOptions.GetRetryPolicy()
		if startOptions.GetFlowStartDelaySeconds() < 0 {
			return nil, makeInvalidRequestError("flow start delay must be non-negative")
		}
		if startOptions.GetFlowStartDelaySeconds() > 0 {
			workflowOptions.WorkflowStartDelay = ptr.Any(
				time.Duration(startOptions.GetFlowStartDelaySeconds()) * time.Second,
			)
		}
		if alreadyStartedOptions := startOptions.GetFlowAlreadyStartedOptions(); alreadyStartedOptions != nil {
			ignoreAlreadyStartedError = alreadyStartedOptions.GetIgnoreAlreadyStartedError()
		}
	}

	input := &dexpb.InterpreterWorkflowInput{
		FlowType:                     req.GetFlowType(),
		ConfiguredFlowTimeoutSeconds: req.GetFlowTimeoutSeconds(),
		FlowTimeoutPolicy:            timeoutPolicy,
		StartStepType:                req.GetStartStepType(),
		StepInput:                    req.GetStepInput(),
		StepOptions:                  req.GetStepOptions(),
		InitAttributes:               attributes,
		Config:                       workflowConfig,
		TimeoutHandlerOptions:        startOptions.GetTimeoutHandlerOptions(),
	}

	runId, err := s.client.StartInterpreterWorkflow(ctx, workflowOptions, input)
	if err != nil {
		shouldReturnError := true
		if s.client.IsWorkflowAlreadyStartedError(err) && ignoreAlreadyStartedError {
			alreadyRunningRunId, _ := s.client.GetRunIdFromWorkflowAlreadyStartedError(err)
			runId = alreadyRunningRunId
			response, descErr := s.client.DescribeWorkflowExecution(ctx, req.GetFlowId(), runId, nil)
			if descErr != nil {
				return nil, s.handleError(descErr)
			}
			requestMemo := response.Memos[service.ReqId]
			if requestMemo.GetObjValue() != nil &&
				string(requestMemo.GetObjValue().GetPayload()) == req.GetRequestId() {
				shouldReturnError = false
			}
		}
		if shouldReturnError {
			return nil, s.handleError(err)
		}
	} else {
		s.logger.Info("Started flow", tag.WorkflowID(req.GetFlowId()), tag.WorkflowRunID(runId))
	}
	return &dexpb.StartFlowResponse{RunId: runId}, nil
}

func overrideWorkflowConfig(configOverride *dexpb.FlowConfig, workflowConfig *dexpb.FlowConfig) {
	if configOverride.ActiveStepSearchMode != nil {
		workflowConfig.ActiveStepSearchMode = configOverride.ActiveStepSearchMode
	}
	if configOverride.ContinueAsNewThreshold != nil {
		workflowConfig.ContinueAsNewThreshold = configOverride.ContinueAsNewThreshold
	}
	if configOverride.ContinueAsNewPageSizeInBytes != nil {
		workflowConfig.ContinueAsNewPageSizeInBytes = configOverride.ContinueAsNewPageSizeInBytes
	}
	if configOverride.StepDurability != nil {
		workflowConfig.StepDurability = configOverride.StepDurability
	}
	if configOverride.WorkerTarget != nil {
		workflowConfig.WorkerTarget = configOverride.WorkerTarget
	}
	if configOverride.AttributeStoreNames != nil {
		workflowConfig.AttributeStoreNames = configOverride.AttributeStoreNames
	}
}

func (s *serviceImpl) WaitForStepCompletion(ctx context.Context, req *dexpb.WaitForStepCompletionRequest) (*dexpb.WaitForStepCompletionResponse, error) {
	if s.client.GetBackendType() == service.BackendTypeCadence {
		return nil, status.Errorf(codes.Unimplemented, "WaitForStepCompletion requires Temporal synchronous update")
	}
	if req == nil || req.GetFlowId() == "" ||
		req.GetRequestTimeoutSeconds() < 0 ||
		req.GetInternalHandlerTimeoutSeconds() < 0 {
		return nil, makeInvalidRequestError(
			"valid flow ID and non-negative request and internal handler timeouts are required",
		)
	}
	if req.GetStepType() == "" || req.GetStepExecutionNumber() == "" {
		return nil, makeInvalidRequestError("step type and step execution number are required")
	}
	stepExecutionNumber, err := strconv.ParseInt(
		req.GetStepExecutionNumber(),
		10,
		32,
	)
	if err != nil || stepExecutionNumber <= 0 {
		return nil, makeInvalidRequestError("step execution number must be a positive integer")
	}
	requestDeadline := waitRequestDeadline(req.GetRequestTimeoutSeconds())
	waitCtx, cancel := s.waitContext(ctx)
	defer cancel()
	baseUpdateID := waitForStepCompletionUpdateID(req)
	updateIDGeneration := 0
	var response dexpb.WaitForStepCompletionResponse
	backoff := 25 * time.Millisecond
	for {
		remainingSeconds, hasTimeRemaining := remainingWaitRequestSeconds(requestDeadline)
		if !hasTimeRemaining {
			return nil, serviceerrors.DeadlineExceededRequest(
				"step completion request timed out",
			).ToGRPCError()
		}
		req.RequestTimeoutSeconds = remainingSeconds
		updateCtx, cancelUpdate, isRequestBound := waitUpdateContext(waitCtx, requestDeadline)
		err := s.client.SynchronousUpdateWorkflow(
			updateCtx,
			&response,
			req.GetFlowId(),
			"",
			waitUpdateID(baseUpdateID, updateIDGeneration),
			service.WaitForStepCompletionUpdateType,
			req,
		)
		updateCause := context.Cause(updateCtx)
		cancelUpdate()
		if err == nil {
			return &response, nil
		}
		if s.isWaitRequestExpired(err, updateCause, isRequestBound, requestDeadline) {
			return nil, serviceerrors.DeadlineExceededRequest(
				"step completion request timed out",
			).ToGRPCError()
		}
		if waitCtx.Err() != nil {
			return nil, waitContextStatus(waitCtx.Err())
		}
		if isInternalHandlerTimeoutUpdateError(s.client, err) {
			updateIDGeneration++
			continue
		}
		if s.client.IsNotFoundError(err) {
			completed, queryErr := s.isStepExecutionCompleted(
				waitCtx,
				req.GetFlowId(),
				req.GetStepType(),
				int32(stepExecutionNumber),
			)
			if queryErr == nil && completed {
				return &response, nil
			}
			if queryErr != nil && !s.client.IsNotFoundError(queryErr) {
				s.logger.Warn(
					"failed to recover step completion after flow closed",
					tag.WorkflowID(req.GetFlowId()),
					tag.Error(queryErr),
				)
			}
			return nil, s.handleError(err)
		}
		if !s.isWaitForStepCompletionUpdateTransitionError(err) {
			return nil, s.handleError(err)
		}
		if err := waitForCANRetry(waitCtx, backoff); err != nil {
			return nil, waitContextStatus(err)
		}
		if backoff < time.Second {
			backoff *= 2
		}
	}
}

func waitForStepCompletionUpdateID(request *dexpb.WaitForStepCompletionRequest) string {
	if request.GetRequestId() != "" {
		return request.GetRequestId()
	}
	return waitForStepCompletionUpdateIDNamespace + request.GetStepType() + "-" + request.GetStepExecutionNumber()
}

func (s *serviceImpl) isWaitForStepCompletionUpdateTransitionError(err error) bool {
	if s.client.IsUnknownUpdateError(err, service.WaitForStepCompletionUpdateType) {
		return true
	}
	updateType, updateError := s.client.GetIfUpdateError(err, nil)
	return updateError &&
		updateType == dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_CONTINUE_AS_NEW_PREEMPTED
}

func (s *serviceImpl) isStepExecutionCompleted(
	ctx context.Context,
	flowID string,
	stepType string,
	stepExecutionNumber int32,
) (bool, error) {
	var completed bool
	if err := s.client.QueryWorkflow(
		ctx,
		&completed,
		flowID,
		"",
		service.IsStepExecutionCompletedQueryType,
		stepType,
		stepExecutionNumber,
	); err != nil {
		return false, err
	}
	return completed, nil
}

func (s *serviceImpl) WaitForAttribute(
	ctx context.Context,
	req *dexpb.WaitForAttributeRequest,
) (*dexpb.WaitForAttributeResponse, error) {
	if s.client.GetBackendType() == service.BackendTypeCadence {
		return nil, status.Errorf(codes.Unimplemented, "WaitForAttribute requires Temporal synchronous update")
	}
	if req == nil || req.GetFlowId() == "" ||
		req.GetRequestTimeoutSeconds() < 0 ||
		req.GetInternalHandlerTimeoutSeconds() < 0 {
		return nil, makeInvalidRequestError(
			"valid flow ID and non-negative request and internal handler timeouts are required",
		)
	}
	match := req.GetMatch()
	if match == nil || match.GetOperand() == nil {
		return nil, makeInvalidRequestError("attribute match is required")
	}
	if err := workerclient.RejectWorkerBlobIDs(match.GetOperand()); err != nil {
		return nil, makeInvalidRequestError(err.Error())
	}
	if err := validateAttributeMatch(match); err != nil {
		return nil, makeInvalidRequestError(err.Error())
	}
	requestDeadline := waitRequestDeadline(req.GetRequestTimeoutSeconds())
	waitCtx, cancel := s.waitContext(ctx)
	defer cancel()
	baseUpdateID := waitForAttributeUpdateID(req)
	updateIDGeneration := 0
	var response dexpb.WaitForAttributeResponse
	backoff := 25 * time.Millisecond
	for {
		remainingSeconds, hasTimeRemaining := remainingWaitRequestSeconds(requestDeadline)
		if !hasTimeRemaining {
			return nil, serviceerrors.DeadlineExceededRequest(
				"attribute request timed out",
			).ToGRPCError()
		}
		req.RequestTimeoutSeconds = remainingSeconds
		updateCtx, cancelUpdate, isRequestBound := waitUpdateContext(waitCtx, requestDeadline)
		err := s.client.SynchronousUpdateWorkflow(
			updateCtx,
			&response,
			req.GetFlowId(),
			"",
			waitUpdateID(baseUpdateID, updateIDGeneration),
			service.WaitForAttributeUpdateType,
			req,
		)
		updateCause := context.Cause(updateCtx)
		cancelUpdate()
		if err == nil {
			return &response, nil
		}
		if s.isWaitRequestExpired(err, updateCause, isRequestBound, requestDeadline) {
			return nil, serviceerrors.DeadlineExceededRequest(
				"attribute request timed out",
			).ToGRPCError()
		}
		if waitCtx.Err() != nil {
			return nil, waitContextStatus(waitCtx.Err())
		}
		if isInternalHandlerTimeoutUpdateError(s.client, err) {
			updateIDGeneration++
			continue
		}
		if updateType, ok := s.client.GetIfUpdateError(err, nil); !ok ||
			updateType != dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_CONTINUE_AS_NEW_PREEMPTED {
			return nil, s.handleError(err)
		}
		if err := waitForCANRetry(waitCtx, backoff); err != nil {
			return nil, waitContextStatus(err)
		}
		if backoff < time.Second {
			backoff *= 2
		}
	}
}

func waitForAttributeUpdateID(request *dexpb.WaitForAttributeRequest) string {
	if request.GetRequestId() != "" {
		return request.GetRequestId()
	}
	return waitForAttributeUpdateIDNamespace + attributeMatchCondition(request.GetMatch())
}

func attributeMatchCondition(match *dexpb.AttributeMatch) string {
	return url.QueryEscape(match.GetKey()) + attributeMatchOperatorSymbol(match.GetOperator()) + attributeMatchOperand(match.GetOperand())
}

func attributeMatchOperatorSymbol(operator dexpb.AttributeMatchOperator) string {
	switch operator {
	case dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_EQUAL:
		return "=="
	case dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_NOT_EQUAL:
		return "!="
	case dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_GREATER_THAN:
		return ">"
	case dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_GREATER_THAN_OR_EQUAL:
		return ">="
	case dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_LESS_THAN:
		return "<"
	case dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_LESS_THAN_OR_EQUAL:
		return "<="
	default:
		panic("validated Attribute match has an invalid operator")
	}
}

func attributeMatchOperand(operand *dexpb.Value) string {
	switch value := operand.GetKind().(type) {
	case *dexpb.Value_StringValue:
		return strconv.Quote(value.StringValue)
	case *dexpb.Value_IntValue:
		return strconv.FormatInt(value.IntValue, 10)
	case *dexpb.Value_DoubleValue:
		formatted := strconv.FormatFloat(value.DoubleValue, 'g', -1, 64)
		if !strings.ContainsAny(formatted, ".eE") {
			formatted += ".0"
		}
		return formatted
	case *dexpb.Value_BoolValue:
		return strconv.FormatBool(value.BoolValue)
	default:
		panic("validated Attribute match has an invalid operand")
	}
}

func waitUpdateID(baseUpdateID string, generation int) string {
	if generation == 0 {
		return baseUpdateID
	}
	return baseUpdateID + "-" + strconv.Itoa(generation)
}

func isInternalHandlerTimeoutUpdateError(client uclient.UnifiedClient, err error) bool {
	updateType, isUpdateError := client.GetIfUpdateError(err, nil)
	return isUpdateError && updateType == dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_DEADLINE_EXCEEDED
}

func validateAttributeMatch(match *dexpb.AttributeMatch) error {
	if match == nil || match.GetKey() == "" || match.GetOperand() == nil ||
		match.GetOperand().GetKind() == nil {
		return fmt.Errorf("attribute match key, operator, and scalar operand are required")
	}
	if !isAttributeMatchOperator(match.GetOperator()) {
		return fmt.Errorf("attribute match operator is invalid")
	}
	operand := match.GetOperand()
	switch operand.GetKind().(type) {
	case *dexpb.Value_StringValue, *dexpb.Value_BoolValue:
		if isAttributeOrderingOperator(match.GetOperator()) {
			return fmt.Errorf("attribute match ordering requires an integer or double operand")
		}
	case *dexpb.Value_IntValue:
	case *dexpb.Value_DoubleValue:
		if math.IsNaN(operand.GetDoubleValue()) || math.IsInf(operand.GetDoubleValue(), 0) {
			return fmt.Errorf("attribute match double operand must be finite")
		}
	default:
		return fmt.Errorf("attribute match supports only string, boolean, integer, or double operands")
	}
	return nil
}

func isAttributeMatchOperator(operator dexpb.AttributeMatchOperator) bool {
	return operator >= dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_EQUAL &&
		operator <= dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_LESS_THAN_OR_EQUAL
}

func isAttributeOrderingOperator(operator dexpb.AttributeMatchOperator) bool {
	return operator >= dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_GREATER_THAN
}

func (s *serviceImpl) PublishToChannel(
	ctx context.Context,
	req *dexpb.PublishToChannelRequest,
) (*emptypb.Empty, error) {
	if req == nil || req.GetFlowId() == "" {
		return nil, makeInvalidRequestError("flow ID is required")
	}
	for _, message := range req.GetMessages() {
		if message == nil || message.GetChannelName() == "" {
			return nil, makeInvalidRequestError("channel name is required")
		}
		if message.GetValue() == nil {
			message.Value = &dexpb.Value{}
		}
		if err := workerclient.RejectWorkerBlobIDs(message.GetValue()); err != nil {
			return nil, makeInvalidRequestError(err.Error())
		}
	}
	if len(req.GetMessages()) == 0 {
		return &emptypb.Empty{}, nil
	}
	if err := channelmessage.AssignIDs(req.GetMessages()); err != nil {
		return nil, s.handleError(err)
	}
	if err := blobstore.OffloadLargeChannelMessages(
		ctx,
		req.GetMessages(),
		req.GetFlowId(),
		uuid.NewString(),
		s.blobStoreCfg.EffectiveThresholdInBytes(),
		s.store,
		s.blobStoreCfg.EffectiveEnabled(),
	); err != nil {
		return nil, s.handleError(err)
	}
	if err := s.client.SignalWorkflow(
		ctx,
		req.GetFlowId(),
		req.GetRunId(),
		service.ExecuteRpcSignalChannelName,
		&dexpb.ExecuteRpcSignalRequest{PublishToChannel: req.GetMessages()},
	); err != nil {
		return nil, s.handleError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *serviceImpl) GetChannelMessages(
	ctx context.Context,
	req *dexpb.GetChannelMessagesRequest,
) (*dexpb.GetChannelMessagesResponse, error) {
	if req == nil || req.GetFlowId() == "" || req.GetChannelName() == "" {
		return nil, makeInvalidRequestError("flow ID and channel name are required")
	}
	var response dexpb.GetChannelMessagesResponse
	if err := s.client.QueryWorkflow(
		ctx,
		&response,
		req.GetFlowId(),
		req.GetRunId(),
		service.GetChannelMessagesWorkflowQueryType,
		req,
	); err != nil {
		return nil, s.handleError(err)
	}
	if !s.blobStoreCfg.EffectiveLazyLoading() {
		values := make([]*dexpb.Value, 0, len(response.GetMessages()))
		for _, message := range response.GetMessages() {
			values = append(values, message.GetValue())
		}
		if err := blobstore.HydrateFlowValues(ctx, req.GetFlowId(), values, s.store); err != nil {
			return nil, s.handleError(err)
		}
	}
	return &response, nil
}

func (s *serviceImpl) DeleteChannelMessage(
	ctx context.Context,
	req *dexpb.DeleteChannelMessageRequest,
) (*emptypb.Empty, error) {
	if req == nil || req.GetFlowId() == "" || req.GetChannelName() == "" || req.GetMessageId() == "" {
		return nil, makeInvalidRequestError("flow ID, channel name, and message ID are required")
	}
	if req.GetRequestId() == "" {
		return nil, makeInvalidRequestError("request ID is required")
	}
	runID := req.GetRunId()
	if runID == "" {
		description, err := s.client.DescribeWorkflowExecution(ctx, req.GetFlowId(), "", nil)
		if err != nil {
			return nil, s.handleError(err)
		}
		runID = description.RunId
	}
	if s.client.GetBackendType() == service.BackendTypeCadence {
		return s.deleteCadenceChannelMessage(ctx, req, runID)
	}
	retryBackoff := retry.NewInvokeRPCBackoff(s.apiCfg.InvokeRPCContinuedAsNewErrorRetryPolicy)
	for {
		response, err := s.deleteTemporalChannelMessage(ctx, req, runID)
		if err == nil {
			return response, nil
		}
		transitionError := s.isChannelMessageDeleteTransitionError(err)
		if req.GetRunId() != "" || (!s.client.IsNotFoundError(err) && !transitionError) {
			return nil, s.handleError(err)
		}
		description, describeErr := s.client.DescribeWorkflowExecution(ctx, req.GetFlowId(), "", nil)
		if describeErr != nil {
			return nil, s.handleError(err)
		}
		if description.RunId == runID && !transitionError {
			return nil, s.handleError(err)
		}
		shouldRetry, retryErr := retryBackoff.WaitForNextAttempt(ctx)
		if retryErr != nil {
			return nil, s.handleError(retryErr)
		}
		if !shouldRetry {
			return nil, s.handleError(err)
		}
		runID = description.RunId
	}
}

func (s *serviceImpl) deleteTemporalChannelMessage(
	ctx context.Context,
	req *dexpb.DeleteChannelMessageRequest,
	runID string,
) (*emptypb.Empty, error) {
	var response emptypb.Empty
	if err := s.client.SynchronousUpdateWorkflow(
		ctx,
		&response,
		req.GetFlowId(),
		runID,
		req.GetRequestId(),
		service.DeleteChannelMessageUpdateType,
		req,
	); err != nil {
		return nil, err
	}
	return &response, nil
}

func (s *serviceImpl) isChannelMessageDeleteTransitionError(err error) bool {
	if s.client.IsUnknownUpdateError(err, service.DeleteChannelMessageUpdateType) {
		return true
	}
	updateType, updateError := s.client.GetIfUpdateError(err, nil)
	return updateError &&
		updateType == dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_CONTINUE_AS_NEW_PREEMPTED
}

func (s *serviceImpl) deleteCadenceChannelMessage(
	ctx context.Context,
	req *dexpb.DeleteChannelMessageRequest,
	runID string,
) (*emptypb.Empty, error) {
	listRequest := &dexpb.GetChannelMessagesRequest{
		FlowId:      req.GetFlowId(),
		RunId:       runID,
		ChannelName: req.GetChannelName(),
	}
	response, err := s.GetChannelMessages(ctx, listRequest)
	if err != nil {
		return nil, err
	}
	if !containsChannelMessage(response.GetMessages(), req.GetMessageId()) {
		return nil, serviceerrors.ChannelMessageNotFound(
			fmt.Sprintf("Channel message %q was not found in channel %q", req.GetMessageId(), req.GetChannelName()),
		).ToGRPCError()
	}
	deletion := &dexpb.ChannelMessageDeletion{
		ChannelName: req.GetChannelName(),
		MessageId:   req.GetMessageId(),
	}
	if err := s.client.SignalWorkflow(
		ctx,
		req.GetFlowId(),
		runID,
		service.ExecuteRpcSignalChannelName,
		&dexpb.ExecuteRpcSignalRequest{
			DeleteFromChannel:         []*dexpb.ChannelMessageDeletion{deletion},
			IsDeleteChannelMessageApi: true,
		},
	); err != nil {
		return nil, s.handleError(err)
	}
	return &emptypb.Empty{}, nil
}

func containsChannelMessage(messages []*dexpb.ChannelMessage, messageID string) bool {
	for _, message := range messages {
		if message.GetMessageId() == messageID {
			return true
		}
	}
	return false
}

func (s *serviceImpl) WriteStream(
	ctx context.Context,
	req *dexpb.WriteStreamRequest,
) (*emptypb.Empty, error) {
	input, err := streamWriteInput(req)
	if err != nil {
		return nil, makeInvalidRequestError(err.Error())
	}
	if err := workerclient.RejectWorkerBlobIDs(req.GetValue()); err != nil {
		return nil, makeInvalidRequestError(err.Error())
	}
	if err := s.streamStore.Write(ctx, input); err != nil {
		return nil, streamStoreError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *serviceImpl) ReadStream(
	ctx context.Context,
	req *dexpb.ReadStreamRequest,
) (*dexpb.ReadStreamResponse, error) {
	if req == nil ||
		req.GetFlowId() == "" ||
		req.GetFlowType() == "" ||
		req.GetStreamName() == "" ||
		req.GetWaitTimeSeconds() < 0 {
		return nil, makeInvalidRequestError(
			"flow ID, Flow type, Stream name, and non-negative wait time are required",
		)
	}
	waitCtx, cancel := utils.TrimContextByTimeoutWithCappedDDL(
		ctx,
		&req.WaitTimeSeconds,
		s.apiCfg.EffectiveMaxWaitSeconds(),
	)
	defer cancel()
	message, err := s.streamStore.Read(
		waitCtx,
		req.GetFlowType(),
		req.GetFlowId(),
		req.GetStreamName(),
		req.GetResumeToken(),
	)
	if err != nil {
		return nil, streamStoreError(err)
	}
	resumeToken, err := streamstore.EncodeResumeToken(
		req.GetFlowType(),
		req.GetFlowId(),
		req.GetStreamName(),
		message.MessageID,
	)
	if err != nil {
		return nil, serviceerrors.Internal(err.Error()).ToGRPCError()
	}
	return &dexpb.ReadStreamResponse{
		Message: &dexpb.StreamMessage{
			Value:       message.Value,
			ResumeToken: resumeToken,
			CreatedTime: timestamppb.New(message.CreatedTime),
			Source:      message.Source,
		},
	}, nil
}

func (s *serviceImpl) ListStreamMessages(
	ctx context.Context,
	req *dexpb.ListStreamMessagesRequest,
) (*dexpb.ListStreamMessagesResponse, error) {
	if req == nil || req.GetFlowId() == "" || req.GetFlowType() == "" || req.GetStreamName() == "" {
		return nil, makeInvalidRequestError("flow ID, Flow type, and Stream name are required")
	}
	messages, hasMore, err := s.streamStore.List(
		ctx,
		req.GetFlowType(),
		req.GetFlowId(),
		req.GetStreamName(),
		req.GetPageSize(),
		req.GetBeforePageToken(),
	)
	if err != nil {
		return nil, streamStoreError(err)
	}
	response := &dexpb.ListStreamMessagesResponse{
		Messages: make([]*dexpb.StreamMessage, 0, len(messages)),
	}
	for _, message := range messages {
		resumeToken, tokenErr := streamstore.EncodeResumeToken(
			req.GetFlowType(),
			req.GetFlowId(),
			req.GetStreamName(),
			message.MessageID,
		)
		if tokenErr != nil {
			return nil, serviceerrors.Internal(tokenErr.Error()).ToGRPCError()
		}
		response.Messages = append(response.Messages, &dexpb.StreamMessage{
			Value:       message.Value,
			ResumeToken: resumeToken,
			CreatedTime: timestamppb.New(message.CreatedTime),
			Source:      message.Source,
		})
	}
	if hasMore {
		response.NextPageToken = response.Messages[len(response.Messages)-1].GetResumeToken()
	}
	return response, nil
}

func streamWriteInput(req *dexpb.WriteStreamRequest) (streamstore.WriteInput, error) {
	if req == nil || req.GetFlowId() == "" || req.GetFlowType() == "" || req.GetStreamName() == "" {
		return streamstore.WriteInput{}, fmt.Errorf("flow ID, Flow type, and Stream name are required")
	}
	if req.GetStreamCapacityBytes() <= 0 {
		return streamstore.WriteInput{}, fmt.Errorf("max estimated bytes must be positive")
	}
	if req.GetValue() == nil {
		return streamstore.WriteInput{}, fmt.Errorf("Stream Value is required")
	}
	if req.GetSource() == "" {
		return streamstore.WriteInput{}, fmt.Errorf("Stream source is required")
	}
	return streamstore.WriteInput{
		FlowID:              req.GetFlowId(),
		FlowType:            req.GetFlowType(),
		StreamName:          req.GetStreamName(),
		StreamCapacityBytes: req.GetStreamCapacityBytes(),
		Value:               req.GetValue(),
		Source:              req.GetSource(),
	}, nil
}

func streamStoreError(err error) error {
	switch {
	case errors.Is(err, streamstore.ErrDisabled):
		return serviceerrors.NewErrorAndStatus(
			codes.FailedPrecondition,
			dexpb.ErrorSubStatus_ERROR_SUB_STATUS_UNCATEGORIZED,
			err.Error(),
		).ToGRPCError()
	case errors.Is(err, streamstore.ErrMessageTooLarge), errors.Is(err, streamstore.ErrCapacityExceeded):
		return serviceerrors.NewErrorAndStatus(
			codes.ResourceExhausted,
			dexpb.ErrorSubStatus_ERROR_SUB_STATUS_UNCATEGORIZED,
			err.Error(),
		).ToGRPCError()
	case errors.Is(err, streamstore.ErrInvalidResumeToken), errors.Is(err, streamstore.ErrInvalidPageSize):
		return makeInvalidRequestError(err.Error())
	case errors.Is(err, streamstore.ErrWaitTimeout):
		return serviceerrors.DeadlineExceededLongPoll(err.Error()).ToGRPCError()
	case errors.Is(err, context.DeadlineExceeded):
		return serviceerrors.NewErrorAndStatus(
			codes.DeadlineExceeded,
			dexpb.ErrorSubStatus_ERROR_SUB_STATUS_UNCATEGORIZED,
			err.Error(),
		).ToGRPCError()
	case errors.Is(err, context.Canceled):
		return serviceerrors.NewErrorAndStatus(
			codes.Canceled,
			dexpb.ErrorSubStatus_ERROR_SUB_STATUS_UNCATEGORIZED,
			err.Error(),
		).ToGRPCError()
	case errors.Is(err, streamstore.ErrUnavailable):
		return serviceerrors.NewErrorAndStatus(
			codes.Unavailable,
			dexpb.ErrorSubStatus_ERROR_SUB_STATUS_UNCATEGORIZED,
			err.Error(),
		).ToGRPCError()
	default:
		return serviceerrors.Internal(err.Error()).ToGRPCError()
	}
}

func (s *serviceImpl) UpdateFlowConfig(
	ctx context.Context,
	req *dexpb.UpdateFlowConfigRequest,
) (*emptypb.Empty, error) {
	if req == nil || req.GetFlowId() == "" || req.GetFlowConfig() == nil {
		return nil, makeInvalidRequestError("flow ID and flow config are required")
	}
	if err := interpreterconfig.ValidateFlowConfig(req.GetFlowConfig()); err != nil {
		return nil, makeInvalidRequestError(err.Error())
	}
	if req.GetFlowConfig().AttributeStoreNames != nil {
		if err := s.validateAttributeStoreNames(ctx, req.GetFlowConfig().GetAttributeStoreNames()); err != nil {
			return nil, err
		}
	}
	if req.GetFlowConfig().GetWorkerTarget() != nil {
		workerTarget, err := grpctarget.NormalizeWorkerTarget(req.GetFlowConfig().GetWorkerTarget())
		if err != nil {
			return nil, makeInvalidRequestError(err.Error())
		}
		req.GetFlowConfig().WorkerTarget = workerTarget
	}
	if err := s.client.SignalWorkflow(
		ctx,
		req.GetFlowId(),
		req.GetRunId(),
		service.UpdateConfigSignalChannelName,
		req,
	); err != nil {
		return nil, s.handleError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *serviceImpl) TriggerContinueAsNew(
	ctx context.Context,
	req *dexpb.TriggerContinueAsNewRequest,
) (*emptypb.Empty, error) {
	if req == nil || req.GetFlowId() == "" {
		return nil, makeInvalidRequestError("flow ID is required")
	}
	if err := s.client.SignalWorkflow(
		ctx,
		req.GetFlowId(),
		req.GetRunId(),
		service.TriggerContinueAsNewSignalChannelName,
		&emptypb.Empty{},
	); err != nil {
		return nil, s.handleError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *serviceImpl) StopFlow(ctx context.Context, req *dexpb.StopFlowRequest) (*emptypb.Empty, error) {
	if req == nil || req.GetFlowId() == "" {
		return nil, makeInvalidRequestError("flow ID is required")
	}
	stopType := req.GetStopType()
	if stopType == dexpb.StopType_STOP_TYPE_UNSPECIFIED {
		stopType = dexpb.StopType_STOP_TYPE_CANCEL
	}
	var err error
	switch stopType {
	case dexpb.StopType_STOP_TYPE_CANCEL, dexpb.StopType_STOP_TYPE_FAIL:
		err = s.client.SignalWorkflow(
			ctx,
			req.GetFlowId(),
			req.GetRunId(),
			service.StopWorkflowSignalChannelName,
			&dexpb.StopFlowSignalRequest{StopType: stopType, Reason: req.GetReason()},
		)
	case dexpb.StopType_STOP_TYPE_TERMINATE:
		err = s.client.TerminateWorkflow(ctx, req.GetFlowId(), req.GetRunId(), req.GetReason())
	default:
		return nil, makeInvalidRequestError("stop type is required")
	}
	if err != nil {
		return nil, s.handleError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *serviceImpl) validateAttributeStoreNames(ctx context.Context, names *dexpb.AttributeStoreNames) error {
	if names == nil {
		return nil
	}
	seenNames := make(map[string]struct{}, len(names.GetNames()))
	for _, name := range names.GetNames() {
		if name == "" {
			return makeInvalidRequestError("Attribute Store name must not be empty")
		}
		if _, exists := seenNames[name]; exists {
			return makeInvalidRequestError(fmt.Sprintf("duplicate Attribute Store %q", name))
		}
		if !s.attributeStore.HasStore(name) {
			return makeInvalidRequestError(fmt.Sprintf("unknown Attribute Store %q", name))
		}
		seenNames[name] = struct{}{}
	}
	return nil
}

func (s *serviceImpl) GetAttributes(
	ctx context.Context,
	req *dexpb.GetAttributesRequest,
) (*dexpb.GetAttributesResponse, error) {
	if req == nil || req.GetFlowId() == "" {
		return nil, makeInvalidRequestError("flow ID is required")
	}
	var queryResponse dexpb.GetAttributesQueryResponse
	if err := s.client.QueryWorkflow(
		ctx,
		&queryResponse,
		req.GetFlowId(),
		req.GetRunId(),
		service.GetAttributesWorkflowQueryType,
		&dexpb.GetAttributesQueryRequest{
			Keys:    req.GetKeys(),
			AllKeys: req.GetAllKeys(),
		},
	); err != nil {
		return nil, s.handleError(err)
	}
	attributes, err := blobstore.HydrateHardDeleteKVs(ctx, req.GetFlowId(), queryResponse.GetAttributes(), s.store)
	if err != nil {
		return nil, s.handleError(err)
	}
	if !s.blobStoreCfg.EffectiveLazyLoading() {
		if err := blobstore.HydrateKVs(ctx, req.GetFlowId(), attributes, s.store); err != nil {
			return nil, s.handleError(err)
		}
	}
	return &dexpb.GetAttributesResponse{Attributes: attributes}, nil
}

func (s *serviceImpl) SetAttributes(
	ctx context.Context,
	req *dexpb.SetAttributesRequest,
) (*emptypb.Empty, error) {
	if req == nil || req.GetFlowId() == "" {
		return nil, makeInvalidRequestError("flow ID is required")
	}
	if req.GetRequestId() == "" {
		return nil, makeInvalidRequestError("request ID is required")
	}
	if err := validateAttributeWrites(req.GetAttributes()); err != nil {
		return nil, makeInvalidRequestError(err.Error())
	}
	if err := s.validateHardDeleteAttributeStorage(req.GetAttributes()); err != nil {
		return nil, err
	}
	if err := workerclient.ValidateRuntimeAttributeWrites(req.GetAttributes()); err != nil {
		return nil, makeInvalidRequestError(err.Error())
	}
	if err := workerclient.ValidateActionPermissionMappings(
		req.GetActionPermissionMappings(),
		s.client.GetBackendType(),
	); err != nil {
		return nil, makeInvalidRequestError(err.Error())
	}
	attributes := req.GetAttributes()
	if err := blobstore.OffloadLargeAttributeWrites(
		ctx,
		attributes,
		req.GetFlowId(),
		req.GetRequestId(),
		s.blobStoreCfg.EffectiveThresholdInBytes(),
		s.store,
		s.blobStoreCfg.EffectiveEnabled(),
	); err != nil {
		return nil, s.handleError(err)
	}
	if err := s.client.SignalWorkflow(
		ctx,
		req.GetFlowId(),
		req.GetRunId(),
		service.ExecuteRpcSignalChannelName,
		&dexpb.ExecuteRpcSignalRequest{
			IsSetAttributeApi:        true,
			UpsertAttributes:         attributes,
			ActionPermissionMappings: req.GetActionPermissionMappings(),
		},
	); err != nil {
		return nil, s.handleError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *serviceImpl) LoadBlobs(ctx context.Context, req *dexpb.LoadBlobsRequest) (*dexpb.LoadBlobsResponse, error) {
	if req == nil || len(req.GetEntries()) == 0 {
		return &dexpb.LoadBlobsResponse{Values: map[string]*dexpb.Value{}}, nil
	}
	values := make(map[string]*dexpb.Value, len(req.GetEntries()))
	for _, entry := range req.GetEntries() {
		if entry == nil || entry.GetFlowId() == "" {
			return nil, makeInvalidRequestError("Flow ID is required for every Blob")
		}
		blobId, hydrateValue, err := blobArmForLoad(entry.GetBlobValue())
		if err != nil {
			return nil, makeInvalidRequestError(err.Error())
		}
		// Hard-delete values are omitted like unavailable Blobs so they never reach a client cache.
		if blobstore.IsHardDeleteBlobID(blobId) {
			continue
		}
		if err := blobstore.HydrateValue(ctx, entry.GetFlowId(), hydrateValue, s.store); err != nil {
			if blobstore.IsObjectUnavailable(err) {
				continue
			}
			return nil, s.handleError(err)
		}
		values[blobId] = hydrateValue
	}
	return &dexpb.LoadBlobsResponse{Values: values}, nil
}

// validateHardDeleteAttributeStorage rejects hard-delete writes when no blob store can hold them.
func (s *serviceImpl) validateHardDeleteAttributeStorage(attributes []*dexpb.AttributeWrite) error {
	if s.blobStoreCfg.EffectiveEnabled() && s.store != nil {
		return nil
	}
	for _, attribute := range attributes {
		if attribute.GetIsHardDeleteOnNull() {
			return makeInvalidRequestError(blobstore.ErrHardDeleteRequiresBlobStore.Error())
		}
	}
	return nil
}

// blobArmForLoad requires a blob-id arm and returns a fresh Value for hydrate.
func blobArmForLoad(value *dexpb.Value) (blobId string, hydrateValue *dexpb.Value, err error) {
	if value == nil {
		return "", nil, fmt.Errorf("value is required")
	}
	switch kind := value.GetKind().(type) {
	case *dexpb.Value_InternalBlobIdForStringValue:
		if kind.InternalBlobIdForStringValue == "" {
			return "", nil, fmt.Errorf("blob ID is required")
		}
		return kind.InternalBlobIdForStringValue, &dexpb.Value{
			Kind: &dexpb.Value_InternalBlobIdForStringValue{
				InternalBlobIdForStringValue: kind.InternalBlobIdForStringValue,
			},
		}, nil
	case *dexpb.Value_InternalBlobIdForObjValue:
		if kind.InternalBlobIdForObjValue == "" {
			return "", nil, fmt.Errorf("blob ID is required")
		}
		return kind.InternalBlobIdForObjValue, &dexpb.Value{
			Kind: &dexpb.Value_InternalBlobIdForObjValue{
				InternalBlobIdForObjValue: kind.InternalBlobIdForObjValue,
			},
		}, nil
	default:
		return "", nil, fmt.Errorf("LoadBlobs accepts only blob-id arms, not payloads")
	}
}

func (s *serviceImpl) WaitForFlow(
	ctx context.Context,
	req *dexpb.WaitForFlowRequest,
) (*dexpb.FlowResult, error) {
	if req == nil || req.GetFlowId() == "" || req.GetWaitTimeSeconds() < 0 {
		return nil, makeInvalidRequestError("valid flow ID and non-negative wait time are required")
	}
	getCtx, cancel := utils.TrimContextByTimeoutWithCappedDDL(
		ctx,
		ptr.Any(req.GetWaitTimeSeconds()),
		s.apiCfg.EffectiveMaxWaitSeconds(),
	)
	defer cancel()
	var output dexpb.InterpreterWorkflowOutput
	flowStatus, getErr := s.client.GetWorkflowResult(
		getCtx,
		&output,
		req.GetFlowId(),
		req.GetRunId(),
	)
	response := &dexpb.FlowResult{
		FlowStatus: flowStatus,
	}
	if getErr == nil {
		if req.GetNeedsResults() {
			response.Results = output.GetStepCompletionOutputs()
		}
		return response, nil
	}
	if errors.Is(getCtx.Err(), context.DeadlineExceeded) || s.client.IsRequestTimeoutError(getErr) {
		return nil, serviceerrors.DeadlineExceededLongPoll(
			"flow is still running and waiting exceeded the timeout",
		).ToGRPCError()
	}
	if getCtx.Err() != nil {
		return nil, waitContextStatus(getCtx.Err())
	}

	var errorResponse dexpb.ServiceErrorResponse
	if errorType, ok := s.client.GetIfFlowError(getErr, &errorResponse); ok {
		response.FlowStatus = dexpb.FlowStatus_FLOW_STATUS_FAILED
		response.ErrorType = errorType
		response.ErrorMessage = serviceerrors.ServiceErrorResponseDetail(&errorResponse)
		return response, nil
	}

	switch flowStatus {
	case dexpb.FlowStatus_FLOW_STATUS_CANCELED,
		dexpb.FlowStatus_FLOW_STATUS_TERMINATED,
		dexpb.FlowStatus_FLOW_STATUS_SERVER_SIDE_TIMEOUT_INTERNAL_ONLY:
		return response, nil
	case dexpb.FlowStatus_FLOW_STATUS_FAILED:
		response.ErrorMessage = "unknown flow failure from interpreter implementation"
		return response, nil
	default:
		return nil, s.handleError(getErr)
	}
}

func (s *serviceImpl) SearchFlows(
	ctx context.Context,
	req *dexpb.SearchFlowsRequest,
) (*dexpb.SearchFlowsResponse, error) {
	if req == nil || req.GetPageSize() < 0 {
		return nil, makeInvalidRequestError("page size must be non-negative")
	}
	pageSize := int32(1000)
	if req.GetPageSize() > 0 {
		pageSize = req.GetPageSize()
	}
	query := req.GetQuery()
	const currentRuns = `ExecutionStatus != "ContinuedAsNew"`
	if !req.GetIncludeContinuedAsNew() {
		if strings.TrimSpace(query) == "" {
			query = currentRuns
		} else {
			query = fmt.Sprintf("(%s) AND (%s)", query, currentRuns)
		}
	}
	response, err := s.client.ListWorkflow(ctx, &uclient.ListWorkflowExecutionsRequest{
		PageSize:      pageSize,
		Query:         query,
		NextPageToken: []byte(req.GetNextPageToken()),
	})
	if err != nil {
		return nil, s.handleError(err)
	}
	return &dexpb.SearchFlowsResponse{
		FlowRuns:      response.Executions,
		NextPageToken: string(response.NextPageToken),
	}, nil
}

func (s *serviceImpl) SyncAttributeIndexes(
	ctx context.Context,
	req *dexpb.SyncAttributeIndexRequest,
) (*dexpb.SyncAttributeIndexResponse, error) {
	if req == nil {
		return nil, makeInvalidRequestError("request is required")
	}
	if err := s.indexSynchronizer.Sync(ctx, req.GetAttributeIndexes()); err != nil {
		return nil, err
	}
	return &dexpb.SyncAttributeIndexResponse{}, nil
}

func (s *serviceImpl) GetFlowSummary(
	ctx context.Context,
	req *dexpb.GetFlowSummaryRequest,
) (*dexpb.GetFlowSummaryResponse, error) {
	if req == nil || req.GetFlowId() == "" {
		return nil, makeInvalidRequestError("flow ID is required")
	}
	description, err := s.client.DescribeWorkflowExecution(
		ctx,
		req.GetFlowId(),
		req.GetRunId(),
		map[string]dexpb.IndexType{
			service.SearchAttributeDexWorkflowType: dexpb.IndexType_INDEX_TYPE_KEYWORD,
		},
	)
	if err != nil {
		return nil, s.handleError(err)
	}
	response := &dexpb.GetFlowSummaryResponse{
		FlowExecutionId: &dexpb.FlowExecutionID{
			FlowId: req.GetFlowId(),
			RunId:  description.RunId,
		},
		FirstRunId: description.FirstRunId,
		RequestId:  decodeStringMemo(description.Memos[service.ReqId]),
		FlowType:   description.IndexedAttributes[service.SearchAttributeDexWorkflowType].GetStringValue(),
		FlowStatus: description.Status,
		StartTime:  timestamppb.New(description.StartTime),
	}
	if description.CloseTime != nil {
		response.CloseTime = timestamppb.New(*description.CloseTime)
	}
	return response, nil
}

func decodeStringMemo(value *dexpb.Value) string {
	if value == nil {
		return ""
	}
	return string(value.GetObjValue().GetPayload())
}

func (s *serviceImpl) GetHistoryEvents(
	ctx context.Context,
	req *dexpb.GetHistoryEventsRequest,
) (*dexpb.GetHistoryEventsResponse, error) {
	if req == nil || req.GetFlowId() == "" || req.GetRunId() == "" {
		return nil, makeInvalidRequestError("flow ID and run ID are required")
	}
	if req.GetStartInternalEventId() < 0 || req.GetEstimatePageSize() < 0 {
		return nil, makeInvalidRequestError("event ID and page size must be non-negative")
	}
	pageSize := req.GetEstimatePageSize()
	if pageSize == 0 {
		pageSize = defaultHistoryPageSize
	}
	history, err := s.client.GetWorkflowHistory(ctx, &uclient.GetWorkflowHistoryRequest{
		WorkflowID:           req.GetFlowId(),
		RunID:                req.GetRunId(),
		StartInternalEventID: req.GetStartInternalEventId(),
		EstimatePageSize:     pageSize,
		NextPageToken:        req.GetNextPageToken(),
	})
	if err != nil {
		return nil, s.handleHistoryReadError(ctx, req.GetFlowId(), req.GetRunId(), err)
	}
	if err := s.stepInputPopulator.Populate(
		ctx,
		req.GetFlowId(),
		req.GetRunId(),
		history.Events,
		history.NextInternalEventID,
		history.NextPageToken,
	); err != nil {
		return nil, s.handleError(err)
	}
	return &dexpb.GetHistoryEventsResponse{
		Events:              history.Events,
		NextPageToken:       history.NextPageToken,
		NextInternalEventId: history.NextInternalEventID,
	}, nil
}

func (s *serviceImpl) WaitForHistoryEvent(
	ctx context.Context,
	req *dexpb.WaitForHistoryEventRequest,
) (*dexpb.WaitForHistoryEventResponse, error) {
	if req == nil || req.GetFlowId() == "" || req.GetRunId() == "" || req.GetNextInternalEventId() <= 0 {
		return nil, makeInvalidRequestError("flow ID, run ID, and positive next event ID are required")
	}
	history, err := s.client.WaitForWorkflowHistoryEvent(
		ctx,
		req.GetFlowId(),
		req.GetRunId(),
		req.GetNextInternalEventId(),
	)
	if err != nil {
		if s.client.IsRequestTimeoutError(err) {
			return nil, serviceerrors.DeadlineExceededLongPoll(
				"history long poll exceeded the deadline",
			).ToGRPCError()
		}
		return nil, s.handleError(err)
	}
	description, err := s.client.DescribeWorkflowExecution(
		ctx,
		req.GetFlowId(),
		req.GetRunId(),
		nil,
	)
	if err != nil {
		return nil, s.handleError(err)
	}
	return &dexpb.WaitForHistoryEventResponse{
		EventAvailable:           history.EventAvailable,
		AvailableInternalEventId: history.AvailableInternalEventID,
		FlowStatus:               description.Status,
	}, nil
}

func (s *serviceImpl) GetFlowState(
	ctx context.Context,
	req *dexpb.GetFlowStateRequest,
) (*dexpb.GetFlowStateResponse, error) {
	if req == nil || req.GetFlowId() == "" {
		return nil, makeInvalidRequestError("flow ID is required")
	}
	description, err := s.client.DescribeWorkflowExecution(ctx, req.GetFlowId(), req.GetRunId(), nil)
	if err != nil {
		return nil, s.handleError(err)
	}
	var dump dexpb.DebugDumpResponse
	if err := s.client.QueryWorkflow(
		ctx,
		&dump,
		req.GetFlowId(),
		description.RunId,
		service.DebugDumpQueryType,
	); err != nil {
		return nil, s.handleError(err)
	}
	snapshot := dump.GetSnapshot()
	activeStepExecutions := dump.GetActiveStepExecutions()
	attachPendingStepFailures(activeStepExecutions, description.PendingStepFailures)
	return &dexpb.GetFlowStateResponse{
		FlowConfig:             dump.GetConfig(),
		StickyAppWorkerId:      snapshot.GetDexTaskQueue(),
		Attributes:             snapshot.GetAttributes(),
		ActiveStepExecutions:   activeStepExecutions,
		QueuedSteps:            snapshot.GetStepsToStartFromBeginning(),
		PendingChannelMessages: snapshot.GetChannelReceived(),
		CompletedSteps:         snapshot.GetStepOutputs(),
	}, nil
}

func attachPendingStepFailures(
	activeStepExecutions []*dexpb.ActiveStepExecutionState,
	pendingStepFailures map[string]*dexpb.StepMethodFailure,
) {
	for _, activeStepExecution := range activeStepExecutions {
		failure := pendingStepFailures[activeStepExecution.GetStepExecutionId()]
		if failure != nil {
			activeStepExecution.LastFailureInfo = failure
		}
	}
}

func (s *serviceImpl) InvokeRPC(
	ctx context.Context,
	req *dexpb.InvokeRPCRequest,
) (*dexpb.InvokeRPCResponse, error) {
	if req == nil || req.GetFlowId() == "" || req.GetRpcName() == "" {
		return nil, makeInvalidRequestError("flow ID and RPC name are required")
	}
	if req.GetRequestId() == "" {
		return nil, makeInvalidRequestError("request ID is required")
	}
	if req.GetTimeoutSeconds() < 0 {
		return nil, makeInvalidRequestError("RPC timeout must be non-negative")
	}
	if _, err := rpc.ValidateAndSortSelections(
		req.GetLoadAttributeMapInstances(),
		req.GetLoadChannelNames(),
		req.GetLoadChannelMapInstances(),
	); err != nil {
		return nil, makeInvalidRequestError(err.Error())
	}
	if err := workerclient.RejectWorkerBlobIDs(req.GetInput()); err != nil {
		return nil, makeInvalidRequestError(err.Error())
	}
	backendType := s.client.GetBackendType()
	if len(req.GetLockAttributeKeys()) > 0 && backendType == service.BackendTypeCadence {
		return nil, status.Errorf(codes.Unimplemented, "locking RPC requires Temporal synchronous update")
	}
	isTransactional := s.shouldInvokeRPCTransactionally(req)

	// Pin each attempt to one run so its query, signal, or Update cannot cross Continue-as-New.
	runID := req.GetRunId()
	if runID == "" {
		description, err := s.client.DescribeWorkflowExecution(ctx, req.GetFlowId(), "", nil)
		if err != nil {
			return nil, s.handleError(err)
		}
		runID = description.RunId
	}
	if isTransactional {
		if err := s.offloadRPCValue(ctx, req, req.GetInput()); err != nil {
			return nil, s.handleError(err)
		}
	}

	retryBackoff := retry.NewInvokeRPCBackoff(
		s.apiCfg.InvokeRPCContinuedAsNewErrorRetryPolicy,
	)
	for {
		response, err := s.doInvokeRPC(ctx, req, runID, isTransactional)
		if err == nil {
			if !s.blobStoreCfg.EffectiveLazyLoading() {
				if err := blobstore.HydrateValue(ctx, req.GetFlowId(), response.GetOutput(), s.store); err != nil {
					return nil, s.handleError(err)
				}
			}
			return response, nil
		}
		updateTransitionError := isTransactional && s.isInvokeRPCUpdateTransitionError(err)
		if req.GetRunId() != "" ||
			(!s.client.IsNotFoundError(err) && !updateTransitionError) {
			return nil, s.handleInvokeRPCError(err)
		}
		description, describeErr := s.client.DescribeWorkflowExecution(
			ctx,
			req.GetFlowId(),
			"",
			nil,
		)
		if describeErr != nil {
			s.logger.Warn(
				"failed to resolve current run after RPC error",
				tag.WorkflowID(req.GetFlowId()),
				tag.Error(describeErr),
			)
			return nil, s.handleInvokeRPCError(err)
		}
		if description.RunId == runID && !updateTransitionError {
			return nil, s.handleInvokeRPCError(err)
		}
		shouldRetry, retryErr := retryBackoff.WaitForNextAttempt(ctx)
		if retryErr != nil {
			return nil, s.handleError(retryErr)
		}
		if !shouldRetry {
			return nil, s.handleInvokeRPCError(err)
		}
		runID = description.RunId
	}
}

func (s *serviceImpl) isInvokeRPCUpdateTransitionError(err error) bool {
	if s.client.IsUnknownUpdateError(err, service.InvokeRpcUpdateType) {
		return true
	}
	updateType, updateError := s.client.GetIfUpdateError(err, nil)
	return updateError &&
		updateType == dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_CONTINUE_AS_NEW_PREEMPTED
}

func (s *serviceImpl) doInvokeRPC(
	ctx context.Context,
	req *dexpb.InvokeRPCRequest,
	runID string,
	useSynchronousUpdate bool,
) (*dexpb.InvokeRPCResponse, error) {
	if useSynchronousUpdate {
		return s.doInvokeRpcUpdate(ctx, req, runID)
	}

	var preparation dexpb.PrepareRpcQueryResponse
	if err := s.client.QueryWorkflow(
		ctx,
		&preparation,
		req.GetFlowId(),
		runID,
		service.PrepareRpcQueryType,
		&dexpb.PrepareRpcQueryRequest{
			LoadAttributeMapInstances: req.GetLoadAttributeMapInstances(),
			LoadChannelNames:          req.GetLoadChannelNames(),
			LoadChannelMapInstances:   req.GetLoadChannelMapInstances(),
		},
	); err != nil {
		return nil, err
	}
	workerResponse, servedByAppWorkerID, err := rpc.InvokeWorkerRpc(
		ctx,
		s.workerPool,
		&preparation,
		req,
		s.apiCfg,
		&s.interpreterCfg.InterpreterActivityConfig,
		s.client.GetBackendType(),
		s.store,
		req.GetRequestId(),
		s.blobStoreCfg,
		false,
		s.wakeNotifier,
		s.pullDispatcher,
		s.matchingCfg,
		false,
	)
	if err != nil {
		return nil, err
	}
	decision := workerResponse.GetStepDecision()
	if len(workerResponse.GetUpsertAttributes()) > 0 ||
		len(workerResponse.GetRecordEvents()) > 0 ||
		len(workerResponse.GetDeleteFromChannel()) > 0 ||
		len(workerResponse.GetPublishToChannel()) > 0 ||
		len(decision.GetNextSteps()) > 0 ||
		len(decision.GetCancelStepTypes()) > 0 ||
		decision.GetCloseDecision() != nil {
		signalRequest := &dexpb.ExecuteRpcSignalRequest{
			UpsertAttributes:         workerResponse.GetUpsertAttributes(),
			StepDecision:             workerResponse.GetStepDecision(),
			RecordEvents:             workerResponse.GetRecordEvents(),
			DeleteFromChannel:        workerResponse.GetDeleteFromChannel(),
			PublishToChannel:         workerResponse.GetPublishToChannel(),
			ActionPermissionMappings: workerResponse.GetActionPermissionMappings(),
			ServedByAppWorkerId:      servedByAppWorkerID,
		}
		if err := s.client.SignalWorkflow(
			ctx,
			req.GetFlowId(),
			runID,
			service.ExecuteRpcSignalChannelName,
			signalRequest,
		); err != nil {
			return nil, err
		}
	}
	return &dexpb.InvokeRPCResponse{Output: workerResponse.GetOutput()}, nil
}

func (s *serviceImpl) shouldInvokeRPCTransactionally(req *dexpb.InvokeRPCRequest) bool {
	return s.client.GetBackendType() == service.BackendTypeTemporal &&
		(req.GetIsTransactional() || len(req.GetLockAttributeKeys()) > 0 ||
			s.apiCfg.UseTemporalSynchronousUpdateForAllRPCs)
}

func (s *serviceImpl) handleInvokeRPCError(err error) error {
	var dispatchError *pulldispatch.ServerError
	if errors.As(err, &dispatchError) {
		if dispatchStatus, ok := status.FromError(dispatchError.Err); ok {
			return serviceerrors.NewErrorAndStatus(
				dispatchStatus.Code(),
				dexpb.ErrorSubStatus_ERROR_SUB_STATUS_UNCATEGORIZED,
				dispatchError.Error(),
			).ToGRPCError()
		}
		return s.handleError(dispatchError.Err)
	}
	if mapped, ok := serviceerrors.WorkerAPIFailure(err); ok {
		return mapped.ToGRPCError()
	}
	return s.handleError(err)
}

func (s *serviceImpl) doInvokeRpcUpdate(
	ctx context.Context,
	req *dexpb.InvokeRPCRequest,
	runID string,
) (*dexpb.InvokeRPCResponse, error) {
	var result dexpb.InvokeRpcUpdateResult
	if err := s.client.SynchronousUpdateWorkflow(
		ctx,
		&result,
		req.GetFlowId(),
		runID,
		req.GetRequestId(),
		service.InvokeRpcUpdateType,
		req,
	); err != nil {
		return nil, err
	}
	if result.GetResponse() == nil {
		return nil, fmt.Errorf("InvokeRpc Update returned no response")
	}
	return result.GetResponse(), nil
}

func (s *serviceImpl) offloadRPCValue(ctx context.Context, req *dexpb.InvokeRPCRequest, value *dexpb.Value) error {
	return blobstore.OffloadLargeValue(
		ctx, value, req.GetFlowId(), req.GetRequestId(),
		s.blobStoreCfg.EffectiveThresholdInBytes(), s.store, s.blobStoreCfg.EffectiveEnabled(),
	)
}

func (s *serviceImpl) ResetFlow(
	ctx context.Context,
	req *dexpb.ResetFlowRequest,
) (*dexpb.ResetFlowResponse, error) {
	if err := validateResetFlowRequest(req); err != nil {
		return nil, err
	}
	runId, err := s.client.ResetWorkflow(ctx, req)
	if err != nil {
		return nil, s.handleError(err)
	}
	return &dexpb.ResetFlowResponse{RunId: runId}, nil
}

func validateResetFlowRequest(req *dexpb.ResetFlowRequest) error {
	if req == nil || req.GetFlowId() == "" {
		return makeInvalidRequestError("flow ID is required")
	}
	switch req.GetResetType() {
	case dexpb.FlowResetType_FLOW_RESET_TYPE_BEGINNING:
		if req.GetStepMethod() != dexpb.FlowResetStepMethod_FLOW_RESET_STEP_METHOD_UNSPECIFIED {
			return makeInvalidRequestError("step method is only valid for a Step execution ID")
		}
	case dexpb.FlowResetType_FLOW_RESET_TYPE_HISTORY_EVENT_TIME:
		if req.GetHistoryEventTime() == "" {
			return makeInvalidRequestError("history event time is required")
		}
		if req.GetStepMethod() != dexpb.FlowResetStepMethod_FLOW_RESET_STEP_METHOD_UNSPECIFIED {
			return makeInvalidRequestError("step method is only valid for a Step execution ID")
		}
	case dexpb.FlowResetType_FLOW_RESET_TYPE_STEP_TYPE:
		if req.GetStepType() == "" {
			return makeInvalidRequestError("step type is required")
		}
		if req.GetStepMethod() != dexpb.FlowResetStepMethod_FLOW_RESET_STEP_METHOD_UNSPECIFIED {
			return makeInvalidRequestError("step method is only valid for a Step execution ID")
		}
	case dexpb.FlowResetType_FLOW_RESET_TYPE_STEP_EXECUTION_ID:
		if req.GetStepExecutionId() == "" {
			return makeInvalidRequestError("step execution ID is required")
		}
		if req.GetStepMethod() != dexpb.FlowResetStepMethod_FLOW_RESET_STEP_METHOD_WAIT_FOR &&
			req.GetStepMethod() != dexpb.FlowResetStepMethod_FLOW_RESET_STEP_METHOD_EXECUTE {
			return makeInvalidRequestError("step method must be WaitFor or Execute")
		}
	default:
		return makeInvalidRequestError("time travel type is required")
	}
	return nil
}

func (s *serviceImpl) SkipTimer(
	ctx context.Context,
	req *dexpb.SkipTimerRequest,
) (*emptypb.Empty, error) {
	if req == nil || req.GetFlowId() == "" || req.GetStepExecutionId() == "" {
		return nil, makeInvalidRequestError("flow ID and step execution ID are required")
	}
	if req.GetTimerConditionId() == "" && req.TimerConditionIndex == nil {
		return nil, makeInvalidRequestError("timer condition ID or index is required")
	}
	var timerInfos dexpb.GetCurrentTimerInfosQueryResponse
	if err := s.client.QueryWorkflow(
		ctx,
		&timerInfos,
		req.GetFlowId(),
		req.GetRunId(),
		service.GetCurrentTimerInfosQueryType,
	); err != nil {
		return nil, s.handleError(err)
	}
	stepTimerInfos := timerInfos.GetStepExecutionCurrentTimerInfos()[req.GetStepExecutionId()]
	if _, valid := service.ValidateTimerSkipRequest(
		stepTimerInfos.GetTimers(),
		req.GetTimerConditionId(),
		int(req.GetTimerConditionIndex()),
	); !valid {
		return nil, makeInvalidRequestError("requested timer condition does not exist or is not pending")
	}
	if err := s.client.SignalWorkflow(
		ctx,
		req.GetFlowId(),
		req.GetRunId(),
		service.SkipTimerSignalChannelName,
		&dexpb.SkipTimerSignalRequest{
			StepExecutionId:     req.GetStepExecutionId(),
			TimerConditionId:    req.GetTimerConditionId(),
			TimerConditionIndex: req.GetTimerConditionIndex(),
		},
	); err != nil {
		return nil, s.handleError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *serviceImpl) DumpFlowForContinueAsNew(
	ctx context.Context,
	req *dexpb.ContinueAsNewDumpRequest,
) (*dexpb.ContinueAsNewDumpResponse, error) {
	if req == nil || req.GetFlowId() == "" || req.GetRunId() == "" {
		return nil, makeInvalidRequestError("flow ID and run ID are required")
	}
	var response dexpb.ContinueAsNewDumpResponse
	if err := s.client.QueryWorkflow(
		ctx,
		&response,
		req.GetFlowId(),
		req.GetRunId(),
		service.ContinueAsNewDumpByPageQueryType,
		req,
	); err != nil {
		return nil, s.handleError(err)
	}
	return &response, nil
}

func (s *serviceImpl) HealthCheck(ctx context.Context, _ *emptypb.Empty) (*dexpb.HealthInfo, error) {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "Hostname Not Available"
	}
	return &dexpb.HealthInfo{
		Condition: "OK",
		Hostname:  hostname,
		Duration:  0,
	}, nil
}

func (s *serviceImpl) waitContext(parent context.Context) (context.Context, context.CancelFunc) {
	deadline := time.Now().Add(time.Duration(s.apiCfg.EffectiveMaxWaitSeconds()) * time.Second)
	if parentDeadline, ok := parent.Deadline(); ok && parentDeadline.Before(deadline) {
		deadline = parentDeadline
	}
	ctx, cancel := context.WithDeadline(parent, deadline)
	return ctx, cancel
}

func waitRequestDeadline(requestedSeconds int32) time.Time {
	if requestedSeconds == 0 {
		return time.Time{}
	}
	return time.Now().Add(time.Duration(requestedSeconds) * time.Second)
}

// waitUpdateContext bounds one Update call by the earlier of the request and transport deadlines. The returned flag
// is true only when the request deadline is the bound, so an equal or earlier transport deadline keeps its own outcome.
func waitUpdateContext(
	parent context.Context,
	requestDeadline time.Time,
) (context.Context, context.CancelFunc, bool) {
	if requestDeadline.IsZero() {
		ctx, cancel := context.WithCancel(parent)
		return ctx, cancel, false
	}
	if parentDeadline, ok := parent.Deadline(); ok && !requestDeadline.Before(parentDeadline) {
		ctx, cancel := context.WithCancel(parent)
		return ctx, cancel, false
	}
	ctx, cancel := context.WithDeadlineCause(parent, requestDeadline, errWaitRequestDeadlineExceeded)
	return ctx, cancel, true
}

// isWaitRequestExpired reports whether a failed wait Update call ended because the caller's request deadline passed.
// The transport can report that deadline before this process's own timer for it has fired, which leaves the context
// cause unset, so a timeout error after the request deadline also counts.
func (s *serviceImpl) isWaitRequestExpired(
	err error,
	updateCause error,
	isRequestBound bool,
	requestDeadline time.Time,
) bool {
	if errors.Is(updateCause, errWaitRequestDeadlineExceeded) {
		return true
	}
	return isRequestBound &&
		!time.Now().Before(requestDeadline) &&
		(errors.Is(err, context.DeadlineExceeded) || s.client.IsRequestTimeoutError(err))
}

func remainingWaitRequestSeconds(deadline time.Time) (int32, bool) {
	if deadline.IsZero() {
		return 0, true
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return 0, false
	}
	seconds := (remaining + time.Second - 1) / time.Second
	return int32(seconds), true
}

func waitForCANRetry(
	ctx context.Context,
	backoff time.Duration,
) error {
	timer := time.NewTimer(backoff)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// handleHistoryReadError reports a missing run as NotFound, because some backends answer a history read for an
// unknown run with an internal error.
func (s *serviceImpl) handleHistoryReadError(ctx context.Context, flowID string, runID string, err error) error {
	if s.client.IsNotFoundError(err) {
		return s.handleError(err)
	}
	_, describeErr := s.client.DescribeWorkflowExecution(ctx, flowID, runID, nil)
	if s.client.IsNotFoundError(describeErr) {
		return s.handleError(describeErr)
	}
	return s.handleError(err)
}

func (s *serviceImpl) handleError(err error) error {
	if engineStatus := serviceerror.ToStatus(err); engineStatus.Code() == codes.ResourceExhausted {
		return engineStatus.Err()
	}
	var cadenceLimitExceededError *shared.LimitExceededError
	if errors.As(err, &cadenceLimitExceededError) {
		return status.Error(codes.ResourceExhausted, cadenceLimitExceededError.GetMessage())
	}
	var cadenceServiceBusyError *shared.ServiceBusyError
	if errors.As(err, &cadenceServiceBusyError) {
		return status.Error(codes.ResourceExhausted, cadenceServiceBusyError.GetMessage())
	}
	if errors.Is(err, uclient.ErrQueryRequestTimeout) {
		return serviceerrors.DeadlineExceededRequest(err.Error()).ToGRPCError()
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return waitContextStatus(err)
	}
	if s.client.IsNotFoundError(err) {
		return serviceerrors.NotFound(err.Error()).ToGRPCError()
	}
	if s.client.IsRequestTimeoutError(err) {
		return serviceerrors.DeadlineExceededLongPoll(err.Error()).ToGRPCError()
	}
	if s.client.IsWorkflowAlreadyStartedError(err) {
		return serviceerrors.AlreadyExists(err.Error()).ToGRPCError()
	}
	var details string
	if updateType, ok := s.client.GetIfUpdateError(err, &details); ok {
		switch updateType {
		case dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_INVALID_ARGUMENT:
			return serviceerrors.InvalidArgument(
				dexpb.ErrorSubStatus_ERROR_SUB_STATUS_UNCATEGORIZED,
				details,
			).ToGRPCError()
		case dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_FAILED_PRECONDITION:
			return serviceerrors.NewErrorAndStatus(
				codes.FailedPrecondition,
				dexpb.ErrorSubStatus_ERROR_SUB_STATUS_UNCATEGORIZED,
				details,
			).ToGRPCError()
		case dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_DEADLINE_EXCEEDED:
			return serviceerrors.Internal(details).ToGRPCError()
		case dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_RPC_ACQUIRE_LOCK_FAILURE:
			return serviceerrors.AbortedLockFailure(details).ToGRPCError()
		case dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_SERVER_INTERNAL:
			return serviceerrors.Internal(details).ToGRPCError()
		case dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_CHANNEL_MESSAGE_NOT_FOUND:
			return serviceerrors.ChannelMessageNotFound(details).ToGRPCError()
		default:
			return serviceerrors.Internal(details).ToGRPCError()
		}
	}

	var errorResponse dexpb.ServiceErrorResponse
	if flowErrorType, ok := s.client.GetIfFlowError(err, &errorResponse); ok {
		switch flowErrorType {
		case dexpb.FlowErrorType_FLOW_ERROR_TYPE_WORKER_API_FAIL,
			dexpb.FlowErrorType_FLOW_ERROR_TYPE_INTERNAL:
			if errorResponse.GetDetail() == "" &&
				errorResponse.GetSubStatus() == dexpb.ErrorSubStatus_ERROR_SUB_STATUS_UNSPECIFIED &&
				errorResponse.GetOriginalWorkerErrorDetail() == "" &&
				errorResponse.GetOriginalWorkerErrorType() == "" &&
				errorResponse.GetOriginalWorkerErrorStatus() == 0 &&
				errorResponse.GetOriginalWorkerErrorStackTrace() == "" {
				return serviceerrors.Internal(err.Error()).ToGRPCError()
			}
			grpcCode := codes.Internal
			if flowErrorType == dexpb.FlowErrorType_FLOW_ERROR_TYPE_WORKER_API_FAIL {
				grpcCode = codes.FailedPrecondition
			}
			return serviceerrors.NewErrorAndStatusWithWorkerError(
				grpcCode,
				errorResponse.GetSubStatus(),
				errorResponse.GetDetail(),
				errorResponse.GetOriginalWorkerErrorDetail(),
				errorResponse.GetOriginalWorkerErrorType(),
				errorResponse.GetOriginalWorkerErrorStatus(),
				errorResponse.GetOriginalWorkerErrorStackTrace(),
			).ToGRPCError()
		}
	}

	s.logger.Error("encountered unknown application error", tag.Error(err))
	return serviceerrors.Internal(err.Error()).ToGRPCError()
}

func waitContextStatus(err error) error {
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, err.Error())
	}
	return serviceerrors.DeadlineExceededLongPoll(err.Error()).ToGRPCError()
}

func makeInvalidRequestError(details string) error {
	return serviceerrors.InvalidArgument(
		dexpb.ErrorSubStatus_ERROR_SUB_STATUS_UNCATEGORIZED,
		details,
	).ToGRPCError()
}
