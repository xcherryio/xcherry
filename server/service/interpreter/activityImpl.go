// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package interpreter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"time"

	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service"
	uclient "github.com/superdurable/dex/service/client"
	"github.com/superdurable/dex/service/common/attributestore"
	"github.com/superdurable/dex/service/common/blobstore"
	"github.com/superdurable/dex/service/common/channelmessage"
	"github.com/superdurable/dex/service/common/event"
	"github.com/superdurable/dex/service/common/grpctarget"
	"github.com/superdurable/dex/service/common/index"
	"github.com/superdurable/dex/service/common/log"
	"github.com/superdurable/dex/service/common/ptr"
	"github.com/superdurable/dex/service/common/pulldispatch"
	"github.com/superdurable/dex/service/common/retry"
	"github.com/superdurable/dex/service/common/rpc"
	"github.com/superdurable/dex/service/common/streamstore"
	"github.com/superdurable/dex/service/common/utils"
	"github.com/superdurable/dex/service/common/workerclient"
	interpreterconfig "github.com/superdurable/dex/service/interpreter/config"
	"github.com/superdurable/dex/service/interpreter/interfaces"
	"github.com/superdurable/dex/service/wake"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type Activities struct {
	activityProvider interfaces.ActivityProvider
	workerPool       *workerclient.WorkerClientPool
	internalClient   *workerclient.InternalServiceClient
	unifiedClient    uclient.UnifiedClient
	blobStore        blobstore.BlobStore
	attributeStore   *attributestore.Manager
	eventHandler     event.HandleEventFunc
	cfg              *config.Config
	subFlowResolver  *SubFlowReuseResolver
	streamStore      *streamstore.Store
	metrics          client.MetricsHandler
	wakeNotifier     *wake.Notifier
	pullDispatcher   pulldispatch.Dispatcher
}

func NewActivities(
	activityProvider interfaces.ActivityProvider,
	workerPool *workerclient.WorkerClientPool,
	internalClient *workerclient.InternalServiceClient,
	unifiedClient uclient.UnifiedClient,
	blobStore blobstore.BlobStore,
	attributeStore *attributestore.Manager,
	eventHandler event.HandleEventFunc,
	cfg *config.Config,
	streamStore *streamstore.Store,
	metrics client.MetricsHandler,
	pullDispatcher pulldispatch.Dispatcher,
) *Activities {
	if activityProvider == nil || workerPool == nil || internalClient == nil ||
		unifiedClient == nil || attributeStore == nil || eventHandler == nil ||
		streamStore == nil || metrics == nil || pullDispatcher == nil {
		panic("Activities requires non-nil dependencies")
	}
	if cfg == nil {
		panic("Activities requires non-nil config sections")
	}
	return &Activities{
		activityProvider: activityProvider,
		workerPool:       workerPool,
		internalClient:   internalClient,
		unifiedClient:    unifiedClient,
		blobStore:        blobStore,
		attributeStore:   attributeStore,
		eventHandler:     eventHandler,
		cfg:              cfg,
		subFlowResolver:  NewSubFlowReuseResolver(unifiedClient),
		wakeNotifier:     NewWakeNotifier(cfg),
		streamStore:      streamStore,
		metrics:          metrics,
		pullDispatcher:   pullDispatcher,
	}
}

func (a *Activities) SyncAttributeBatch(
	ctx context.Context,
	input *dexpb.SyncAttributeBatchActivityInput,
) error {
	items, err := a.hydrateAttributeSyncItems(ctx, input.GetFlowId(), input.GetItems())
	if err != nil {
		return err
	}
	attributeStoreInput := utils.ShallowCopyMessage(input)
	attributeStoreInput.Items = items
	return a.attributeStore.WriteBatch(ctx, attributeStoreInput)
}

func (a *Activities) hydrateAttributeSyncItems(
	ctx context.Context,
	flowID string,
	items []*dexpb.AttributeSyncItem,
) ([]*dexpb.AttributeSyncItem, error) {
	var hydratedItems []*dexpb.AttributeSyncItem
	for index, item := range items {
		if item == nil {
			continue
		}
		value, err := blobstore.HydrateValueCopy(ctx, flowID, item.GetValue(), a.blobStore)
		if err != nil {
			return nil, fmt.Errorf("hydrate Attribute Store item: %w", err)
		}
		if value == item.GetValue() {
			continue
		}
		if hydratedItems == nil {
			hydratedItems = append([]*dexpb.AttributeSyncItem(nil), items...)
		}
		hydratedItem := utils.ShallowCopyMessage(item)
		hydratedItem.Value = value
		hydratedItems[index] = hydratedItem
	}
	if hydratedItems == nil {
		return items, nil
	}
	return hydratedItems, nil
}

// IWaitForM invokes a Worker's WaitFor method through the selected transport.
func (a *Activities) IWaitForM(
	ctx context.Context,
	input *dexpb.InvokeWaitForMethodActivityInput,
	localInput *dexpb.InternalLocalActivityInput,
) (*dexpb.InvokeWaitForMethodActivityOutput, error) {
	provider := a.activityProvider
	logger := provider.GetLogger(ctx)
	logger.Info("InvokeWaitForMethodActivity", "input", log.ToJsonAndTruncateForLogging(input))

	activityInfo := provider.GetActivityInfo(ctx)
	req := waitForMethodRequestForAttempt(input.GetRequest())
	activityAttempt := retry.NewStepActivityAttempt(
		req.GetContext(),
		activityInfo.ScheduledTime,
		activityInfo.Attempt,
	)
	activityAttempt.ApplyToWorkerContext(req.GetContext())
	routing := pulldispatch.Routing(input.GetWorkerTarget(), input.GetDexTaskQueue(), "")
	var localActivityFailure *dexpb.InternalLocalStepActivityFailure
	if activityInfo.IsLocalActivity {
		localActivityFailure = activityAttempt.LocalFailureDetails(
			req.GetContext(),
			localInput.GetMethodOptions(),
		)
		localActivityFailure.PullRouting = routing
	}
	if err := a.restoreHeartbeatValue(ctx, activityInfo, req.GetContext()); err != nil {
		return nil, newServerSideActivityError(ctx, provider, err, localActivityFailure)
	}

	lazyLoading := a.cfg.BlobStore.EffectiveLazyLoading()
	if !lazyLoading {
		if err := a.hydrateWorkerRequestValues(ctx, req); err != nil {
			a.logLocalActivityWarn(logger, activityInfo, "IWaitForM", req.GetContext().GetStepExecutionId(), req, err)
			return nil, newServerSideActivityError(ctx, provider, err, localActivityFailure)
		}
		loadedChannelMessages, err := blobstore.HydrateChannelValuesCopy(
			ctx, req.GetContext().GetFlowId(), req.GetLoadedChannelMessages(), a.blobStore,
		)
		if err != nil {
			return nil, newServerSideActivityError(ctx, provider, err, localActivityFailure)
		}
		req.LoadedChannelMessages = loadedChannelMessages
	}
	workerReq, err := a.waitForMethodRequestForWorker(ctx, req)
	if err != nil {
		return nil, newServerSideActivityError(ctx, provider, err, localActivityFailure)
	}
	heartbeat := a.dispatchHeartbeat(ctx, activityInfo, req.GetContext())
	var stream waitForMethodOutputStream
	var callCtx = ctx
	var pullStream *pullWaitForStream
	if input.GetWorkerTarget().GetIsPullMode() {
		pullStream, err = a.openPullWaitFor(ctx, input, workerReq, activityInfo.IsLocalActivity, heartbeat)
		stream = pullStream
	} else {
		dispatch, openErr := openWorkerStream(
			ctx, a, input.GetWorkerTarget(), activityInfo.WorkflowExecution.ID, heartbeat,
			func(client dexpb.WorkerServiceClient, callCtx context.Context) (
				grpc.ServerStreamingClient[dexpb.InvokeWaitForMethodOutput], error,
			) {
				return client.InvokeWaitForMethod(callCtx, workerReq)
			},
		)
		if openErr != nil && !dispatch.isOpened {
			return nil, newServerSideActivityError(ctx, provider, openErr, localActivityFailure)
		}
		defer dispatch.release()
		stream, callCtx, err = dispatch.stream, dispatch.callCtx, openErr
	}
	if err != nil {
		a.emitStepWaitForMethodEvent(req, activityInfo, event.EventTypeWaitForAttemptFail)
		a.logLocalActivityWarn(logger, activityInfo, "IWaitForM", req.GetContext().GetStepExecutionId(), req, err)
		if pullStream == nil && input.GetWorkerTarget().GetIsPullMode() {
			return nil, newServerSideActivityError(ctx, provider, err, localActivityFailure)
		}
		return nil, newWorkerSideActivityError(ctx, provider, a.unifiedClient.GetBackendType(), err, localActivityFailure, routing)
	}
	resp, err := a.receiveWaitForMethodResponse(ctx, stream, req, activityInfo, logger)
	if pullStream != nil {
		routing = pulldispatch.Routing(input.GetWorkerTarget(), input.GetDexTaskQueue(), pullStream.servedByAppWorkerID)
		if localActivityFailure != nil {
			localActivityFailure.PullRouting = routing
		}
	}
	printDebugMsg(logger, err, workerAddressForLogging(callCtx, input.GetWorkerTarget()))
	if err != nil {
		a.emitStepWaitForMethodEvent(req, activityInfo, event.EventTypeWaitForAttemptFail)
		a.logLocalActivityWarn(logger, activityInfo, "IWaitForM", req.GetContext().GetStepExecutionId(), req, err)
		var matchingError *matchingDispatchError
		if errors.As(err, &matchingError) {
			return nil, newServerSideActivityError(ctx, provider, err, localActivityFailure)
		}
		return nil, newWorkerSideActivityError(ctx, provider, a.unifiedClient.GetBackendType(), err, localActivityFailure, routing)
	}
	if err := validateWaitingCondition(
		resp.GetWaitingCondition(),
		a.cfg.Interpreter.InterpreterActivityConfig.EffectiveMinimumStepHeartbeatTimeout(),
	); err != nil {
		a.emitStepWaitForMethodEvent(req, activityInfo, event.EventTypeWaitForAttemptFail)
		return nil, newWorkerSideActivityError(ctx, provider, a.unifiedClient.GetBackendType(), err, localActivityFailure, routing)
	}
	if err := validateWorkerWaitForResponse(resp, a.unifiedClient.GetBackendType()); err != nil {
		return nil, newWorkerSideActivityError(ctx, provider, a.unifiedClient.GetBackendType(), err, localActivityFailure, routing)
	}
	if err := a.rejectHardDeleteWritesWithoutBlobStore(
		resp.GetUpsertAttributes(), resp.GetWaitingCondition().GetSubFlowConditions(),
	); err != nil {
		return nil, newWorkerSideActivityError(ctx, provider, a.unifiedClient.GetBackendType(), err, localActivityFailure, routing)
	}
	if err := a.offloadSubFlowStartInputs(
		ctx,
		req.GetContext().GetStepExecutionId(),
		resp.GetWaitingCondition().GetSubFlowConditions(),
	); err != nil {
		return nil, newServerSideActivityError(ctx, provider, err, localActivityFailure)
	}
	if err := a.persistStepEventInput(
		ctx,
		localInput.GetCurrentRunStartedTimestamp(),
		activityInfo,
		req.GetContext().GetStepExecutionId(),
		blobstore.StepEventInputMethodWaitFor,
		&dexpb.InternalAsyncStepInputSnapshot{
			MethodOptions: localInput.GetMethodOptions(),
			Request: &dexpb.InternalAsyncStepInputSnapshot_WaitForRequest{
				WaitForRequest: req,
			},
		},
	); err != nil {
		return nil, newServerSideActivityError(ctx, provider, err, localActivityFailure)
	}

	resp.LocalActivityMetadata = nil
	if activityInfo.IsLocalActivity {
		resp.LocalActivityMetadata = composeLocalActivityMetadata(req.GetContext())
	}
	if err := a.offloadWorkerAttributeWrites(
		ctx,
		resp.GetUpsertAttributes(),
		activityInfo.WorkflowExecution.ID,
		activityInvocationId(activityInfo),
	); err != nil {
		return nil, newServerSideActivityError(ctx, provider, err, localActivityFailure)
	}
	if err := a.offloadWorkerSideEffects(
		ctx,
		resp.GetUpsertStepExeLocals(),
		resp.GetRecordEvents(),
		resp.GetPublishToChannel(),
		activityInfo.WorkflowExecution.ID,
		activityInvocationId(activityInfo),
	); err != nil {
		return nil, newServerSideActivityError(ctx, provider, err, localActivityFailure)
	}

	a.emitStepWaitForMethodEvent(req, activityInfo, event.EventTypeWaitForAttemptSucc)
	output := &dexpb.InvokeWaitForMethodActivityOutput{Response: resp}
	if pullStream != nil {
		output.ServedByAppWorkerId = pullStream.servedByAppWorkerID
	}
	output.PullRouting = pulldispatch.Routing(input.GetWorkerTarget(), input.GetDexTaskQueue(), output.GetServedByAppWorkerId())
	return output, nil
}

func (a *Activities) offloadSubFlowStartInputs(
	ctx context.Context,
	stepExecutionID string,
	conditions []*dexpb.SubFlowCondition,
) error {
	for index, condition := range conditions {
		parentFlowID, subFlowID, requestID, err := a.subFlowStartIdentity(
			ctx, stepExecutionID, int32(index),
		)
		if err != nil {
			return err
		}
		if err := blobstore.TransferValueBlobOwnership(
			ctx,
			condition.GetStepInput(),
			parentFlowID,
			subFlowID,
			requestID,
			a.cfg.BlobStore.EffectiveThresholdInBytes(),
			a.blobStore,
			a.cfg.BlobStore.EffectiveEnabled(),
		); err != nil {
			return err
		}
		for _, attribute := range condition.GetOptions().GetAttributes() {
			if attribute == nil {
				continue
			}
			if attribute.GetIsHardDeleteOnNull() {
				if err := blobstore.OffloadLargeAttributeWrites(
					ctx,
					[]*dexpb.AttributeWrite{attribute},
					subFlowID,
					requestID,
					a.cfg.BlobStore.EffectiveThresholdInBytes(),
					a.blobStore,
					a.cfg.BlobStore.EffectiveEnabled(),
				); err != nil {
					return err
				}
				continue
			}
			if err := blobstore.TransferValueBlobOwnership(
				ctx,
				attribute.GetValue(),
				parentFlowID,
				subFlowID,
				requestID,
				a.cfg.BlobStore.EffectiveThresholdInBytes(),
				a.blobStore,
				a.cfg.BlobStore.EffectiveEnabled(),
			); err != nil {
				return err
			}
		}
	}
	return nil
}

// IExecuteM invokes a Worker's Execute method through the selected transport.
func (a *Activities) IExecuteM(
	ctx context.Context,
	input *dexpb.InvokeExecuteMethodActivityInput,
	localInput *dexpb.InternalLocalActivityInput,
) (*dexpb.InvokeExecuteMethodActivityOutput, error) {
	provider := a.activityProvider
	logger := provider.GetLogger(ctx)
	logger.Info("InvokeExecuteMethodActivity", "input", log.ToJsonAndTruncateForLogging(input))

	activityInfo := provider.GetActivityInfo(ctx)
	req := executeMethodRequestForAttempt(input.GetRequest())
	activityAttempt := retry.NewStepActivityAttempt(
		req.GetContext(),
		activityInfo.ScheduledTime,
		activityInfo.Attempt,
	)
	activityAttempt.ApplyToWorkerContext(req.GetContext())
	routing := pulldispatch.Routing(input.GetWorkerTarget(), input.GetDexTaskQueue(), "")
	var localActivityFailure *dexpb.InternalLocalStepActivityFailure
	if activityInfo.IsLocalActivity {
		localActivityFailure = activityAttempt.LocalFailureDetails(
			req.GetContext(),
			localInput.GetMethodOptions(),
		)
		localActivityFailure.PullRouting = routing
	}
	if err := a.restoreHeartbeatValue(ctx, activityInfo, req.GetContext()); err != nil {
		return nil, newServerSideActivityError(ctx, provider, err, localActivityFailure)
	}

	lazyLoading := a.cfg.BlobStore.EffectiveLazyLoading()
	originalStepInputBlob := stepInputBlobRef(req.GetStepInput())
	if !lazyLoading {
		if err := a.hydrateWorkerRequestValues(ctx, req); err != nil {
			a.logLocalActivityWarn(logger, activityInfo, "IExecuteM", req.GetContext().GetStepExecutionId(), req, err)
			return nil, newServerSideActivityError(ctx, provider, err, localActivityFailure)
		}
		stepExeLocals, err := blobstore.HydrateKVsCopy(ctx, req.GetContext().GetFlowId(), req.GetStepExeLocals(), a.blobStore)
		if err != nil {
			return nil, newServerSideActivityError(ctx, provider, err, localActivityFailure)
		}
		req.StepExeLocals = stepExeLocals
		conditionResults, err := blobstore.HydrateConditionResultsCopy(
			ctx, req.GetContext().GetFlowId(), req.GetConditionResults(), a.blobStore,
		)
		if err != nil {
			return nil, newServerSideActivityError(ctx, provider, err, localActivityFailure)
		}
		req.ConditionResults = conditionResults
		loadedChannelMessages, err := blobstore.HydrateChannelValuesCopy(
			ctx, req.GetContext().GetFlowId(), req.GetLoadedChannelMessages(), a.blobStore,
		)
		if err != nil {
			return nil, newServerSideActivityError(ctx, provider, err, localActivityFailure)
		}
		req.LoadedChannelMessages = loadedChannelMessages
	}
	workerReq, err := a.executeMethodRequestForWorker(ctx, req)
	if err != nil {
		return nil, newServerSideActivityError(ctx, provider, err, localActivityFailure)
	}
	heartbeat := a.dispatchHeartbeat(ctx, activityInfo, req.GetContext())
	var stream executeMethodOutputStream
	var callCtx = ctx
	var pullStream *pullExecuteStream
	if input.GetWorkerTarget().GetIsPullMode() {
		pullStream, err = a.openPullExecute(ctx, input, workerReq, activityInfo.IsLocalActivity, heartbeat)
		stream = pullStream
	} else {
		dispatch, openErr := openWorkerStream(
			ctx, a, input.GetWorkerTarget(), activityInfo.WorkflowExecution.ID, heartbeat,
			func(client dexpb.WorkerServiceClient, callCtx context.Context) (
				grpc.ServerStreamingClient[dexpb.InvokeExecuteMethodOutput], error,
			) {
				return client.InvokeExecuteMethod(callCtx, workerReq)
			},
		)
		if openErr != nil && !dispatch.isOpened {
			return nil, newServerSideActivityError(ctx, provider, openErr, localActivityFailure)
		}
		defer dispatch.release()
		stream, callCtx, err = dispatch.stream, dispatch.callCtx, openErr
	}
	if err != nil {
		a.emitStepExecuteMethodEvent(req, activityInfo, event.EventTypeExecuteAttemptFail)
		a.logLocalActivityWarn(logger, activityInfo, "IExecuteM", req.GetContext().GetStepExecutionId(), req, err)
		if pullStream == nil && input.GetWorkerTarget().GetIsPullMode() {
			return nil, newServerSideActivityError(ctx, provider, err, localActivityFailure)
		}
		return nil, newWorkerSideActivityError(ctx, provider, a.unifiedClient.GetBackendType(), err, localActivityFailure, routing)
	}
	resp, err := a.receiveExecuteMethodResponse(ctx, stream, req, activityInfo, logger)
	if pullStream != nil {
		routing = pulldispatch.Routing(input.GetWorkerTarget(), input.GetDexTaskQueue(), pullStream.servedByAppWorkerID)
		if localActivityFailure != nil {
			localActivityFailure.PullRouting = routing
		}
	}
	printDebugMsg(logger, err, workerAddressForLogging(callCtx, input.GetWorkerTarget()))
	if err != nil {
		a.emitStepExecuteMethodEvent(req, activityInfo, event.EventTypeExecuteAttemptFail)
		a.logLocalActivityWarn(logger, activityInfo, "IExecuteM", req.GetContext().GetStepExecutionId(), req, err)
		var matchingError *matchingDispatchError
		if errors.As(err, &matchingError) {
			return nil, newServerSideActivityError(ctx, provider, err, localActivityFailure)
		}
		return nil, newWorkerSideActivityError(ctx, provider, a.unifiedClient.GetBackendType(), err, localActivityFailure, routing)
	}
	if err := validateExecuteResponse(
		resp,
		a.cfg.Interpreter.InterpreterActivityConfig.EffectiveMinimumStepHeartbeatTimeout(),
		a.unifiedClient.GetBackendType(),
	); err != nil {
		return nil, newWorkerSideActivityError(ctx, provider, a.unifiedClient.GetBackendType(), err, localActivityFailure, routing)
	}
	if err := a.rejectHardDeleteWritesWithoutBlobStore(resp.GetUpsertAttributes(), nil); err != nil {
		return nil, newWorkerSideActivityError(ctx, provider, a.unifiedClient.GetBackendType(), err, localActivityFailure, routing)
	}
	if err := a.persistStepEventInput(
		ctx,
		localInput.GetCurrentRunStartedTimestamp(),
		activityInfo,
		req.GetContext().GetStepExecutionId(),
		blobstore.StepEventInputMethodExecute,
		&dexpb.InternalAsyncStepInputSnapshot{
			MethodOptions: localInput.GetMethodOptions(),
			Request: &dexpb.InternalAsyncStepInputSnapshot_ExecuteRequest{
				ExecuteRequest: req,
			},
		},
	); err != nil {
		return nil, newServerSideActivityError(ctx, provider, err, localActivityFailure)
	}

	service.SetFromStepExecutionIDForStepDecision(
		resp.GetStepDecision(),
		req.GetContext().GetStepExecutionId(),
	)
	resp.LocalActivityMetadata = nil
	if activityInfo.IsLocalActivity {
		resp.LocalActivityMetadata = composeLocalActivityMetadata(req.GetContext())
	}
	if err := a.reuseOrOffloadDecisionInputs(
		ctx,
		resp.GetStepDecision(),
		originalStepInputBlob,
		req.GetStepInput(),
		activityInfo.WorkflowExecution.ID,
		activityInvocationId(activityInfo),
	); err != nil {
		return nil, newServerSideActivityError(ctx, provider, err, localActivityFailure)
	}
	if err := a.offloadWorkerAttributeWrites(
		ctx,
		resp.GetUpsertAttributes(),
		activityInfo.WorkflowExecution.ID,
		activityInvocationId(activityInfo),
	); err != nil {
		return nil, newServerSideActivityError(ctx, provider, err, localActivityFailure)
	}
	if err := a.offloadWorkerSideEffects(
		ctx,
		resp.GetUpsertStepExeLocals(),
		resp.GetRecordEvents(),
		resp.GetPublishToChannel(),
		activityInfo.WorkflowExecution.ID,
		activityInvocationId(activityInfo),
	); err != nil {
		return nil, newServerSideActivityError(ctx, provider, err, localActivityFailure)
	}

	a.emitStepExecuteMethodEvent(req, activityInfo, event.EventTypeExecuteAttemptSucc)
	output := &dexpb.InvokeExecuteMethodActivityOutput{Response: resp}
	if pullStream != nil {
		output.ServedByAppWorkerId = pullStream.servedByAppWorkerID
	}
	output.PullRouting = pulldispatch.Routing(input.GetWorkerTarget(), input.GetDexTaskQueue(), output.GetServedByAppWorkerId())
	return output, nil
}

type waitForMethodOutputStream interface {
	Recv() (*dexpb.InvokeWaitForMethodOutput, error)
}

type executeMethodOutputStream interface {
	Recv() (*dexpb.InvokeExecuteMethodOutput, error)
}

func (a *Activities) receiveWaitForMethodResponse(
	ctx context.Context,
	stream waitForMethodOutputStream,
	request *dexpb.InvokeWaitForMethodRequest,
	activityInfo interfaces.ActivityInfo,
	logger interfaces.UnifiedLogger,
) (*dexpb.InvokeWaitForMethodResponse, error) {
	var result *dexpb.InvokeWaitForMethodResponse
	lastHeartbeatValue := request.GetContext().GetLastHeartbeatValue()
	for {
		output, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			if result == nil {
				return nil, fmt.Errorf("InvokeWaitForMethod stream ended without a result")
			}
			return result, nil
		}
		if err != nil {
			return nil, err
		}
		if result != nil {
			return nil, fmt.Errorf("InvokeWaitForMethod emitted output after its result")
		}
		switch frame := output.GetOutput().(type) {
		case *dexpb.InvokeWaitForMethodOutput_Heartbeat:
			a.recordStepHeartbeat(ctx, activityInfo, frame.Heartbeat.GetValue(), &lastHeartbeatValue)
		case *dexpb.InvokeWaitForMethodOutput_StreamWrite:
			a.recordImplicitStepHeartbeat(ctx, activityInfo, lastHeartbeatValue)
			a.writeStepStream(ctx, request.GetContext(), request.GetFlowType(), request.GetStepType(), "wait_for", frame.StreamWrite, logger)
		case *dexpb.InvokeWaitForMethodOutput_Result:
			if frame.Result == nil {
				return nil, fmt.Errorf("InvokeWaitForMethod emitted an empty result")
			}
			result = frame.Result
		default:
			return nil, fmt.Errorf("InvokeWaitForMethod emitted an empty output")
		}
	}
}

func (a *Activities) receiveExecuteMethodResponse(
	ctx context.Context,
	stream executeMethodOutputStream,
	request *dexpb.InvokeExecuteMethodRequest,
	activityInfo interfaces.ActivityInfo,
	logger interfaces.UnifiedLogger,
) (*dexpb.InvokeExecuteMethodResponse, error) {
	var result *dexpb.InvokeExecuteMethodResponse
	lastHeartbeatValue := request.GetContext().GetLastHeartbeatValue()
	for {
		output, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			if result == nil {
				return nil, fmt.Errorf("InvokeExecuteMethod stream ended without a result")
			}
			return result, nil
		}
		if err != nil {
			return nil, err
		}
		if result != nil {
			return nil, fmt.Errorf("InvokeExecuteMethod emitted output after its result")
		}
		switch frame := output.GetOutput().(type) {
		case *dexpb.InvokeExecuteMethodOutput_Heartbeat:
			a.recordStepHeartbeat(ctx, activityInfo, frame.Heartbeat.GetValue(), &lastHeartbeatValue)
		case *dexpb.InvokeExecuteMethodOutput_StreamWrite:
			a.recordImplicitStepHeartbeat(ctx, activityInfo, lastHeartbeatValue)
			a.writeStepStream(ctx, request.GetContext(), request.GetFlowType(), request.GetStepType(), "execute", frame.StreamWrite, logger)
		case *dexpb.InvokeExecuteMethodOutput_Result:
			if frame.Result == nil {
				return nil, fmt.Errorf("InvokeExecuteMethod emitted an empty result")
			}
			result = frame.Result
		default:
			return nil, fmt.Errorf("InvokeExecuteMethod emitted an empty output")
		}
	}
}

func (a *Activities) restoreHeartbeatValue(
	ctx context.Context,
	activityInfo interfaces.ActivityInfo,
	workerContext *dexpb.Context,
) error {
	if activityInfo.IsLocalActivity {
		return nil
	}
	value, err := a.activityProvider.GetHeartbeatValue(ctx)
	if err != nil {
		return fmt.Errorf("decode Step heartbeat value: %w", err)
	}
	workerContext.LastHeartbeatValue = value
	return nil
}

func (a *Activities) recordStepHeartbeat(
	ctx context.Context,
	activityInfo interfaces.ActivityInfo,
	value *dexpb.Value,
	lastHeartbeatValue **dexpb.Value,
) {
	if activityInfo.IsLocalActivity {
		return
	}
	*lastHeartbeatValue = value
	a.recordStepHeartbeatValue(ctx, activityInfo, value)
}

func (a *Activities) recordImplicitStepHeartbeat(
	ctx context.Context,
	activityInfo interfaces.ActivityInfo,
	lastHeartbeatValue *dexpb.Value,
) {
	a.recordStepHeartbeatValue(ctx, activityInfo, lastHeartbeatValue)
}

func (a *Activities) recordStepHeartbeatValue(
	ctx context.Context,
	activityInfo interfaces.ActivityInfo,
	value *dexpb.Value,
) {
	if activityInfo.IsLocalActivity {
		return
	}
	if value == nil {
		a.activityProvider.RecordHeartbeat(ctx)
		return
	}
	a.activityProvider.RecordHeartbeat(ctx, value)
}

func (a *Activities) writeStepStream(
	ctx context.Context,
	workerContext *dexpb.Context,
	flowType string,
	stepType string,
	stepMethod string,
	write *dexpb.StepStreamWrite,
	logger interfaces.UnifiedLogger,
) {
	input, err := stepStreamWriteInput(workerContext, flowType, write)
	if err == nil {
		err = a.streamStore.Write(ctx, input)
	}
	if err == nil {
		return
	}
	reason := stepStreamWriteFailureReason(err)
	a.metrics.WithTags(map[string]string{
		"flow_type":   flowType,
		"step_type":   stepType,
		"step_method": stepMethod,
		"reason":      reason,
	}).Counter("dex_step_stream_write_failure").Inc(1)
	logger.Warn(
		"Step Stream write failed",
		"flowType", flowType,
		"stepType", stepType,
		"stepMethod", stepMethod,
		"streamName", write.GetStreamName(),
		"source", input.Source,
		"reason", reason,
		"error", err,
	)
}

func stepStreamWriteInput(
	workerContext *dexpb.Context,
	flowType string,
	write *dexpb.StepStreamWrite,
) (streamstore.WriteInput, error) {
	if write == nil || write.GetStreamName() == "" {
		return streamstore.WriteInput{}, fmt.Errorf("Step Stream name is required")
	}
	if write.GetStreamCapacityBytes() <= 0 {
		return streamstore.WriteInput{}, fmt.Errorf("Step Stream capacity must be positive")
	}
	if write.GetValue() == nil {
		return streamstore.WriteInput{}, fmt.Errorf("Step Stream Value is required")
	}
	if workerContext.GetStepExecutionId() == "" {
		return streamstore.WriteInput{}, fmt.Errorf("Step Stream source requires a Step execution ID")
	}
	source := "#" + workerContext.GetStepExecutionId()
	return streamstore.WriteInput{
		FlowID:              workerContext.GetFlowId(),
		FlowType:            flowType,
		StreamName:          write.GetStreamName(),
		StreamCapacityBytes: write.GetStreamCapacityBytes(),
		Value:               write.GetValue(),
		Source:              source,
	}, nil
}

func stepStreamWriteFailureReason(err error) string {
	switch {
	case errors.Is(err, streamstore.ErrDisabled):
		return codes.FailedPrecondition.String()
	case errors.Is(err, streamstore.ErrMessageTooLarge), errors.Is(err, streamstore.ErrCapacityExceeded):
		return codes.ResourceExhausted.String()
	case errors.Is(err, streamstore.ErrUnavailable):
		return codes.Unavailable.String()
	case errors.Is(err, context.Canceled):
		return codes.Canceled.String()
	case errors.Is(err, context.DeadlineExceeded):
		return codes.DeadlineExceeded.String()
	default:
		return codes.Internal.String()
	}
}

func waitForMethodRequestForAttempt(
	request *dexpb.InvokeWaitForMethodRequest,
) *dexpb.InvokeWaitForMethodRequest {
	if request == nil {
		panic("InvokeWaitForMethod request required")
	}
	return &dexpb.InvokeWaitForMethodRequest{
		Context:                     workerContextForAttempt(request.GetContext()),
		FlowType:                    request.GetFlowType(),
		StepType:                    request.GetStepType(),
		StepInput:                   request.GetStepInput(),
		Attributes:                  request.GetAttributes(),
		ChannelInfos:                request.GetChannelInfos(),
		LoadedChannelMessages:       request.GetLoadedChannelMessages(),
		LoadedAttributeMapInstances: request.GetLoadedAttributeMapInstances(),
		LoadedChannelNames:          request.GetLoadedChannelNames(),
		LoadedChannelMapInstances:   request.GetLoadedChannelMapInstances(),
	}
}

func executeMethodRequestForAttempt(
	request *dexpb.InvokeExecuteMethodRequest,
) *dexpb.InvokeExecuteMethodRequest {
	if request == nil {
		panic("InvokeExecuteMethod request required")
	}
	return &dexpb.InvokeExecuteMethodRequest{
		Context:                     workerContextForAttempt(request.GetContext()),
		FlowType:                    request.GetFlowType(),
		StepType:                    request.GetStepType(),
		StepInput:                   request.GetStepInput(),
		Attributes:                  request.GetAttributes(),
		StepExeLocals:               request.GetStepExeLocals(),
		ConditionResults:            request.GetConditionResults(),
		ChannelInfos:                request.GetChannelInfos(),
		LoadedChannelMessages:       request.GetLoadedChannelMessages(),
		LoadedAttributeMapInstances: request.GetLoadedAttributeMapInstances(),
		LoadedChannelNames:          request.GetLoadedChannelNames(),
		LoadedChannelMapInstances:   request.GetLoadedChannelMapInstances(),
	}
}

// waitForMethodRequestForWorker returns req, or a copy whose hard-delete Attributes are hydrated.
// The copy keeps payloads out of req, which also feeds logs and the step-input snapshot.
func (a *Activities) waitForMethodRequestForWorker(
	ctx context.Context,
	req *dexpb.InvokeWaitForMethodRequest,
) (*dexpb.InvokeWaitForMethodRequest, error) {
	if !blobstore.HasHardDeleteKV(req.GetAttributes()) {
		return req, nil
	}
	attributes, err := blobstore.HydrateHardDeleteKVs(ctx, req.GetContext().GetFlowId(), req.GetAttributes(), a.blobStore)
	if err != nil {
		return nil, err
	}
	workerReq := utils.ShallowCopyMessage(req)
	workerReq.Attributes = attributes
	return workerReq, nil
}

// executeMethodRequestForWorker returns req, or a copy whose hard-delete Attributes are hydrated.
// The copy keeps payloads out of req, which also feeds logs and the step-input snapshot.
func (a *Activities) executeMethodRequestForWorker(
	ctx context.Context,
	req *dexpb.InvokeExecuteMethodRequest,
) (*dexpb.InvokeExecuteMethodRequest, error) {
	if !blobstore.HasHardDeleteKV(req.GetAttributes()) {
		return req, nil
	}
	attributes, err := blobstore.HydrateHardDeleteKVs(ctx, req.GetContext().GetFlowId(), req.GetAttributes(), a.blobStore)
	if err != nil {
		return nil, err
	}
	workerReq := utils.ShallowCopyMessage(req)
	workerReq.Attributes = attributes
	return workerReq, nil
}

func workerContextForAttempt(workerContext *dexpb.Context) *dexpb.Context {
	if workerContext == nil {
		return &dexpb.Context{}
	}
	return &dexpb.Context{
		FlowId:                workerContext.GetFlowId(),
		RunId:                 workerContext.GetRunId(),
		FlowStartedTimestamp:  workerContext.GetFlowStartedTimestamp(),
		StepExecutionId:       workerContext.GetStepExecutionId(),
		FirstAttemptTimestamp: workerContext.GetFirstAttemptTimestamp(),
		Attempt:               workerContext.GetAttempt(),
		FromStepExecutionId:   workerContext.GetFromStepExecutionId(),
		RecoveryError:         workerContext.GetRecoveryError(),
	}
}

func (a *Activities) persistStepEventInput(
	ctx context.Context,
	runStartedTimestamp int64,
	activityInfo interfaces.ActivityInfo,
	stepExecutionID string,
	method string,
	input *dexpb.InternalAsyncStepInputSnapshot,
) error {
	if !activityInfo.IsLocalActivity ||
		!a.cfg.BlobStore.EffectiveAsyncStepInputSnapshotsEnabled() ||
		!a.cfg.BlobStore.EffectiveEnabled() {
		return nil
	}
	if stepExecutionID == "" {
		return fmt.Errorf("step event input requires a step execution ID")
	}
	runStarted := time.Unix(runStartedTimestamp, 0)
	if runStartedTimestamp <= 0 {
		description, err := a.unifiedClient.DescribeWorkflowExecution(
			ctx,
			activityInfo.WorkflowExecution.ID,
			activityInfo.WorkflowExecution.RunID,
			nil,
		)
		if err != nil {
			return fmt.Errorf("describe current run for step event input: %w", err)
		}
		runStarted = description.StartTime
	}
	data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(input)
	if err != nil {
		return fmt.Errorf("marshal step event input: %w", err)
	}
	return a.blobStore.WriteStepEventInput(
		ctx,
		runStarted,
		activityInfo.WorkflowExecution.ID,
		activityInfo.WorkflowExecution.RunID,
		stepExecutionID,
		method,
		data,
	)
}

// DumpFlowForContinueAsNew pages ContinueAsNewDump via InternalService.
func (a *Activities) DumpFlowForContinueAsNew(
	ctx context.Context, input *dexpb.DumpFlowForContinueAsNewActivityInput,
) (*dexpb.DumpFlowForContinueAsNewActivityOutput, error) {
	provider := a.activityProvider
	logger := provider.GetLogger(ctx)
	logger.Info("DumpFlowForContinueAsNewActivity", "input", log.ToJsonAndTruncateForLogging(input))

	client, callCtx, err := a.internalClient.Client(ctx)
	if err != nil {
		return nil, newServerSideActivityError(ctx, provider, err, nil)
	}
	resp, err := client.DumpFlowForContinueAsNew(callCtx, input.GetRequest())
	if err != nil {
		return nil, newServerSideActivityError(ctx, provider, err, nil)
	}
	return &dexpb.DumpFlowForContinueAsNewActivityOutput{Response: resp}, nil
}

const maxWorkerRpcActivityAttempts = 3

// IWRPC wraps rpc.InvokeWorkerRpc for the activity worker.
func (a *Activities) IWRPC(
	ctx context.Context, input *dexpb.InvokeWorkerRPCActivityInput,
) (*dexpb.InvokeWorkerRPCActivityOutput, error) {
	provider := a.activityProvider
	logger := provider.GetLogger(ctx)
	logger.Info("InvokeWorkerRpcActivity", "input", log.ToJsonAndTruncateForLogging(input))
	activityInfo := provider.GetActivityInfo(ctx)

	resp, servedByAppWorkerID, err := rpc.InvokeWorkerRpc(
		ctx,
		a.workerPool,
		input.GetRpcPrep(),
		input.GetRequest(),
		&a.cfg.Api,
		&a.cfg.Interpreter.InterpreterActivityConfig,
		a.unifiedClient.GetBackendType(),
		a.blobStore,
		input.GetRequest().GetRequestId(),
		&a.cfg.BlobStore,
		true,
		a.wakeNotifier,
		a.pullDispatcher,
		a.cfg.Matching,
		activityInfo.IsLocalActivity,
	)
	if err != nil {
		var oversizedRequest *rpc.WorkerRequestTooLargeError
		if errors.As(err, &oversizedRequest) {
			// Transactional RPCs use Temporal; oversized requests cannot succeed on retry.
			return nil, provider.NewNonRetryableActivityError(
				dexpb.FlowErrorType_FLOW_ERROR_TYPE_WORKER_API_FAIL,
				&dexpb.InternalActivityError{
					WorkerGrpcStatus: int32(codes.ResourceExhausted), ServerDetail: err.Error(),
				},
			)
		}
		var dispatchError *pulldispatch.ServerError
		if errors.As(err, &dispatchError) {
			return nil, newServerSideActivityError(ctx, provider, err, nil)
		}
		return nil, newWorkerSideActivityError(ctx, provider, a.unifiedClient.GetBackendType(), err, nil)
	}
	out := &dexpb.InvokeWorkerRPCActivityOutput{
		Response:            resp,
		RequestId:           input.GetRequest().GetRequestId(),
		ServedByAppWorkerId: servedByAppWorkerID,
	}
	if activityInfo.IsLocalActivity {
		payloadSize, sized := jsonPayloadSize(logger, out)
		if threshold := a.cfg.Interpreter.InterpreterActivityConfig.LogLocalActivityThresholdBytes; sized && threshold > 0 && payloadSize >= threshold {
			logger.Warn("InvokeWorkerRpc local activity return",
				"workflowId", activityInfo.WorkflowExecution.ID,
				"payloadSize", payloadSize)
		}
	}
	return out, nil
}

func (a *Activities) StartSubFlow(
	ctx context.Context, input *dexpb.StartSubFlowActivityInput,
) (*dexpb.StartSubFlowActivityOutput, error) {
	condition := input.GetCondition()
	if condition == nil {
		return nil, fmt.Errorf("SubFlow start requires a condition")
	}
	parentFlowID, subFlowID, requestID, err := a.subFlowStartIdentity(
		ctx, input.GetParentStepExecutionId(), condition.GetSubFlowIndex(),
	)
	if err != nil {
		return nil, err
	}
	options := condition.GetOptions()
	timeoutPolicy, err := service.ResolveFlowTimeoutPolicy(
		options.GetFlowTimeoutSeconds(),
		options.GetFlowTimeoutPolicy(),
	)
	if err != nil {
		return nil, a.newSubFlowStartRejection(input, err)
	}
	if err := service.ValidateFlowTimeoutHandlerOptions(
		options.GetFlowTimeoutSeconds(),
		timeoutPolicy,
		options.GetTimeoutHandlerOptions(),
		a.cfg.Interpreter.InterpreterActivityConfig.EffectiveMinimumStepHeartbeatTimeout(),
	); err != nil {
		return nil, a.newSubFlowStartRejection(input, err)
	}
	if options.GetFlowStartDelaySeconds() < 0 {
		return nil, a.newSubFlowStartRejection(input, fmt.Errorf("SubFlow start delay must be non-negative"))
	}

	parentFlowConfig := input.GetParentFlowConfig()
	if parentFlowConfig == nil {
		return nil, fmt.Errorf("SubFlow start requires the parent FlowConfig")
	}
	flowConfig, err := buildSubFlowConfig(parentFlowConfig, options.GetFlowConfigOverride())
	if err != nil {
		return nil, a.newSubFlowStartRejection(input, err)
	}
	for _, storeName := range flowConfig.GetAttributeStoreNames().GetNames() {
		if !a.attributeStore.HasStore(storeName) {
			return nil, a.newSubFlowStartRejection(input, fmt.Errorf("Attribute Store %q is unavailable", storeName))
		}
	}

	workflowOptions := buildSubFlowStartOptions(
		condition, flowConfig, parentFlowID, subFlowID, requestID,
	)
	workflowInput := &dexpb.InterpreterWorkflowInput{
		FlowType:                     condition.GetSubFlowType(),
		ConfiguredFlowTimeoutSeconds: options.GetFlowTimeoutSeconds(),
		FlowTimeoutPolicy:            timeoutPolicy,
		StartStepType:                condition.GetStartStepType(),
		StepInput:                    condition.GetStepInput(),
		StepOptions:                  condition.GetStepOptions(),
		InitAttributes:               options.GetAttributes(),
		Config:                       flowConfig,
		TimeoutHandlerOptions:        options.GetTimeoutHandlerOptions(),
	}
	output, err := a.subFlowResolver.Resolve(
		ctx, condition, subFlowID, requestID, workflowOptions, workflowInput,
	)
	if err != nil {
		return nil, err
	}
	if err := blobstore.TransferFlowResultBlobOwnership(
		ctx,
		output.GetImmediateFlowResult(),
		subFlowID,
		parentFlowID,
		requestID,
		a.cfg.BlobStore.EffectiveThresholdInBytes(),
		a.blobStore,
		a.cfg.BlobStore.EffectiveEnabled(),
	); err != nil {
		return nil, fmt.Errorf("transfer immediate SubFlow result Blob ownership: %w", err)
	}
	return output, nil
}

func (a *Activities) newSubFlowStartRejection(input *dexpb.StartSubFlowActivityInput, err error) error {
	if !input.GetTypedRejectionEnabled() {
		return err
	}
	return a.activityProvider.NewSubFlowStartRejection(err.Error())
}

func (a *Activities) subFlowStartIdentity(
	ctx context.Context,
	stepExecutionID string,
	subFlowIndex int32,
) (string, string, string, error) {
	if stepExecutionID == "" {
		return "", "", "", fmt.Errorf("SubFlow start requires a Step execution ID")
	}
	activityInfo := a.activityProvider.GetActivityInfo(ctx)
	parentExecution := activityInfo.WorkflowExecution
	if parentExecution.ID == "" || parentExecution.RunID == "" {
		return "", "", "", fmt.Errorf("SubFlow start requires a parent Workflow execution")
	}
	requestID := parentExecution.RunID + stepExecutionID
	subFlowID := service.SubFlowID(parentExecution.ID, stepExecutionID, subFlowIndex)
	return parentExecution.ID, subFlowID, requestID, nil
}

func (a *Activities) ReportSubFlowCompletion(
	ctx context.Context, input *dexpb.ReportSubFlowCompletionActivityInput,
) (*dexpb.ReportSubFlowCompletionActivityOutput, error) {
	request := input.GetRequest()
	result := request.GetFlowResult()
	activityInfo := a.activityProvider.GetActivityInfo(ctx)
	parentFlowID := input.GetParentFlowId()
	if parentFlowID == "" {
		return nil, fmt.Errorf("SubFlow completion requires a parent Flow ID")
	}
	if result == nil || request.GetSubFlowId() != activityInfo.WorkflowExecution.ID {
		return nil, fmt.Errorf("SubFlow completion result does not match the reporting workflow")
	}
	destinationRequest := copySubFlowCompletionRequestForBlobOwnershipTransfer(request)
	if err := blobstore.TransferFlowResultBlobOwnership(
		ctx,
		destinationRequest.GetFlowResult(),
		activityInfo.WorkflowExecution.ID,
		parentFlowID,
		activityInvocationId(activityInfo),
		a.cfg.BlobStore.EffectiveThresholdInBytes(),
		a.blobStore,
		a.cfg.BlobStore.EffectiveEnabled(),
	); err != nil {
		return nil, fmt.Errorf("transfer SubFlow completion Blob ownership: %w", err)
	}
	err := a.unifiedClient.SignalWorkflow(
		ctx, parentFlowID, "", service.SubFlowCompletionSignalChannelName, destinationRequest,
	)
	if err == nil {
		return &dexpb.ReportSubFlowCompletionActivityOutput{
			Status: dexpb.SubFlowCompletionDeliveryStatus_SUB_FLOW_COMPLETION_DELIVERY_STATUS_DELIVERED,
		}, nil
	}
	if a.unifiedClient.IsNotFoundError(err) {
		return &dexpb.ReportSubFlowCompletionActivityOutput{
			Status: dexpb.SubFlowCompletionDeliveryStatus_SUB_FLOW_COMPLETION_DELIVERY_STATUS_PARENT_CLOSED_OR_NOT_FOUND,
		}, nil
	}
	return nil, fmt.Errorf("report SubFlow completion: %w", err)
}

func copySubFlowCompletionRequestForBlobOwnershipTransfer(
	request *dexpb.SubFlowCompletionSignalRequest,
) *dexpb.SubFlowCompletionSignalRequest {
	result := request.GetFlowResult()
	copiedResult := &dexpb.FlowResult{
		FlowStatus:   result.GetFlowStatus(),
		ErrorType:    result.GetErrorType(),
		ErrorMessage: result.GetErrorMessage(),
		Results:      make([]*dexpb.StepCompletionOutput, 0, len(result.GetResults())),
	}
	for _, completion := range result.GetResults() {
		if completion == nil {
			copiedResult.Results = append(copiedResult.Results, nil)
			continue
		}
		var copiedValue *dexpb.Value
		if completion.GetCompletedStepOutput() != nil {
			copiedValue = &dexpb.Value{Kind: completion.GetCompletedStepOutput().GetKind()}
		}
		copiedResult.Results = append(copiedResult.Results, &dexpb.StepCompletionOutput{
			CompletedStepType:        completion.GetCompletedStepType(),
			CompletedStepExecutionId: completion.GetCompletedStepExecutionId(),
			CompletedStepOutput:      copiedValue,
		})
	}
	return &dexpb.SubFlowCompletionSignalRequest{
		SubFlowId:  request.GetSubFlowId(),
		FlowResult: copiedResult,
	}
}

func buildSubFlowConfig(parent, override *dexpb.FlowConfig) (*dexpb.FlowConfig, error) {
	if parent == nil {
		parent = config.DefaultWorkflowConfig
	}
	flowConfig := &dexpb.FlowConfig{
		ActiveStepSearchMode:         parent.ActiveStepSearchMode,
		ContinueAsNewThreshold:       parent.ContinueAsNewThreshold,
		ContinueAsNewPageSizeInBytes: parent.ContinueAsNewPageSizeInBytes,
		StepDurability:               parent.StepDurability,
		WorkerTarget:                 parent.WorkerTarget,
		AttributeStoreNames:          parent.AttributeStoreNames,
	}
	if override != nil {
		if override.ActiveStepSearchMode != nil {
			flowConfig.ActiveStepSearchMode = override.ActiveStepSearchMode
		}
		if override.ContinueAsNewThreshold != nil {
			flowConfig.ContinueAsNewThreshold = override.ContinueAsNewThreshold
		}
		if override.ContinueAsNewPageSizeInBytes != nil {
			flowConfig.ContinueAsNewPageSizeInBytes = override.ContinueAsNewPageSizeInBytes
		}
		if override.StepDurability != nil {
			flowConfig.StepDurability = override.StepDurability
		}
		if override.WorkerTarget != nil {
			flowConfig.WorkerTarget = override.WorkerTarget
		}
		if override.AttributeStoreNames != nil {
			flowConfig.AttributeStoreNames = override.AttributeStoreNames
		}
	}
	if err := interpreterconfig.ValidateFlowConfig(flowConfig); err != nil {
		return nil, err
	}
	workerTarget, err := grpctarget.NormalizeWorkerTarget(flowConfig.GetWorkerTarget())
	if err != nil {
		return nil, err
	}
	flowConfig.WorkerTarget = workerTarget
	return flowConfig, nil
}

func buildSubFlowStartOptions(
	condition *dexpb.SubFlowCondition,
	flowConfig *dexpb.FlowConfig,
	parentFlowID string,
	subFlowID string,
	requestID string,
) uclient.StartWorkflowOptions {
	options := condition.GetOptions()
	workflowOptions := uclient.StartWorkflowOptions{
		ID:               subFlowID,
		TaskQueue:        service.TaskQueue,
		IdReusePolicy:    ptr.Any(dexpb.IdReusePolicy_ID_REUSE_POLICY_DISALLOW_REUSE),
		RetryPolicy:      options.GetRetryPolicy(),
		SearchAttributes: index.ConvertAttributeWritesToSearchAttributeUpsertMap(options.GetAttributes()),
		Memo: map[string]interface{}{
			service.ReqId: &dexpb.EncodedObject{Payload: []byte(requestID)},
		},
	}
	workflowOptions.SearchAttributes[service.SearchAttributeDexWorkflowType] = condition.GetSubFlowType()
	workflowOptions.SearchAttributes[service.SearchAttributeDexParentFlowID] = parentFlowID
	if options.GetFlowStartDelaySeconds() > 0 {
		workflowOptions.WorkflowStartDelay = ptr.Any(time.Duration(options.GetFlowStartDelaySeconds()) * time.Second)
	}
	return workflowOptions
}

// CleanupBlobsAfterAllRunsDeleted deletes blobs after the backend removes every run.
func (a *Activities) CleanupBlobsAfterAllRunsDeleted(
	ctx context.Context, input *dexpb.CleanupBlobStoreActivityInput,
) (*dexpb.CleanupBlobStoreActivityOutput, error) {
	logger := a.activityProvider.GetLogger(ctx)
	logger.Info("CleanupBlobsAfterAllRunsDeleted started")
	cleanup := &blobCleanup{activities: a, ctx: ctx, storeID: input.GetStoreId(), logger: logger}
	if err := cleanup.cleanDirectory(); err != nil {
		return nil, err
	}
	logger.Info("CleanupBlobsAfterAllRunsDeleted completed", "totalDeleted", cleanup.totalDeleted)
	return &dexpb.CleanupBlobStoreActivityOutput{TotalDeleted: cleanup.totalDeleted}, nil
}

// blobCleanup is one run of the blob cleanup activity.
type blobCleanup struct {
	activities   *Activities
	ctx          context.Context
	storeID      string
	logger       interfaces.UnifiedLogger
	totalDeleted int32
}

// cleanDirectory handles every workflow path of the namespace root.
func (c *blobCleanup) cleanDirectory() error {
	var continueToken *string
	for {
		listOutput, err := c.activities.blobStore.ListWorkflowPaths(c.ctx, blobstore.ListObjectPathsInput{
			StoreId:           c.storeID,
			ContinuationToken: continueToken,
		})
		if err != nil {
			return err
		}
		for _, workflowPath := range listOutput.WorkflowPaths {
			if err := c.cleanWorkflowPath(workflowPath); err != nil {
				return err
			}
			c.activities.activityProvider.RecordHeartbeat(c.ctx)
		}
		continueToken = listOutput.ContinuationToken
		if continueToken == nil {
			return nil
		}
	}
}

func (c *blobCleanup) cleanWorkflowPath(workflowPath string) error {
	parsedPath, err := blobstore.ParseWorkflowPath(workflowPath)
	if err != nil {
		c.logger.Info("CleanupBlobsAfterAllRunsDeleted skipped workflow path", "path", workflowPath, "error", err)
		return nil
	}
	isNotFound, err := c.isFlowNotFound(parsedPath)
	if err != nil || !isNotFound {
		return err
	}
	return c.delete(workflowPath)
}

func (c *blobCleanup) isFlowNotFound(parsedPath blobstore.WorkflowPath) (bool, error) {
	client := c.activities.unifiedClient
	_, err := client.DescribeWorkflowExecution(c.ctx, parsedPath.FlowID, parsedPath.RunID, nil)
	if client.IsNotFoundError(err) {
		return true, nil
	}
	if err != nil {
		c.logger.Error("CleanupBlobsAfterAllRunsDeleted failed to describe workflow", "flowID", parsedPath.FlowID, "error", err)
		return false, err
	}
	return false, nil
}

func (c *blobCleanup) delete(workflowPath string) error {
	if err := c.activities.blobStore.DeleteWorkflowObjects(c.ctx, c.storeID, workflowPath); err != nil {
		c.logger.Error("CleanupBlobsAfterAllRunsDeleted failed to delete workflow objects", "workflowPath", workflowPath, "error", err)
		return err
	}
	c.totalDeleted++
	c.logger.Info("CleanupBlobsAfterAllRunsDeleted deleted workflow objects", "workflowPath", workflowPath)
	return nil
}

func (a *Activities) hydrateWorkerRequestValues(ctx context.Context, req interface {
	GetContext() *dexpb.Context
	GetStepInput() *dexpb.Value
	GetAttributes() []*dexpb.KV
}) error {
	activityInfo := a.activityProvider.GetActivityInfo(ctx)
	flowID := activityInfo.WorkflowExecution.ID
	stepInput, err := blobstore.HydrateValueCopy(ctx, flowID, req.GetStepInput(), a.blobStore)
	if err != nil {
		return err
	}
	attributes, err := blobstore.HydrateKVsCopy(ctx, flowID, req.GetAttributes(), a.blobStore)
	if err != nil {
		return err
	}
	switch typedReq := req.(type) {
	case *dexpb.InvokeWaitForMethodRequest:
		typedReq.StepInput = stepInput
		typedReq.Attributes = attributes
	case *dexpb.InvokeExecuteMethodRequest:
		typedReq.StepInput = stepInput
		typedReq.Attributes = attributes
	default:
		panic("unsupported worker method request")
	}
	return nil
}

func (a *Activities) offloadWorkerAttributeWrites(
	ctx context.Context,
	writes []*dexpb.AttributeWrite,
	flowId string,
	invocationId string,
) error {
	return blobstore.OffloadLargeAttributeWrites(
		ctx,
		writes,
		flowId,
		invocationId,
		a.cfg.BlobStore.EffectiveThresholdInBytes(),
		a.blobStore,
		a.cfg.BlobStore.EffectiveEnabled() && a.blobStore != nil,
	)
}

// rejectHardDeleteWritesWithoutBlobStore fails the Worker result so its hard-delete values never reach history.
func (a *Activities) rejectHardDeleteWritesWithoutBlobStore(
	writes []*dexpb.AttributeWrite,
	subFlowConditions []*dexpb.SubFlowCondition,
) error {
	if a.cfg.BlobStore.EffectiveEnabled() && a.blobStore != nil {
		return nil
	}
	allWrites := slices.Clone(writes)
	for _, condition := range subFlowConditions {
		allWrites = append(allWrites, condition.GetOptions().GetAttributes()...)
	}
	if slices.ContainsFunc(allWrites, (*dexpb.AttributeWrite).GetIsHardDeleteOnNull) {
		return blobstore.ErrHardDeleteRequiresBlobStore
	}
	return nil
}

func (a *Activities) offloadWorkerSideEffects(
	ctx context.Context,
	stepLocals []*dexpb.KV,
	recordEvents []*dexpb.KV,
	channelMessages []*dexpb.ChannelMessage,
	flowId string,
	invocationId string,
) error {
	if err := channelmessage.AssignIDs(channelMessages); err != nil {
		return err
	}
	threshold := a.cfg.BlobStore.EffectiveThresholdInBytes()
	if err := blobstore.OffloadLargeKVs(
		ctx, stepLocals, flowId, invocationId, threshold, a.blobStore, a.cfg.BlobStore.EffectiveEnabled(),
	); err != nil {
		return err
	}
	if err := blobstore.OffloadLargeKVs(
		ctx, recordEvents, flowId, invocationId, threshold, a.blobStore, a.cfg.BlobStore.EffectiveEnabled(),
	); err != nil {
		return err
	}
	return blobstore.OffloadLargeChannelMessages(
		ctx, channelMessages, flowId, invocationId, threshold, a.blobStore, a.cfg.BlobStore.EffectiveEnabled(),
	)
}

func activityInvocationId(activityInfo interfaces.ActivityInfo) string {
	return activityInfo.WorkflowExecution.RunID + activityInfo.ActivityID
}

// reuseOrOffloadDecisionInputs accepts worker-echoed blob ids as-is, reuses the
// original blob id when the concrete payload matches, otherwise offloads.
func (a *Activities) reuseOrOffloadDecisionInputs(
	ctx context.Context,
	decision *dexpb.StepDecision,
	originalStepInputBlob stepInputBlob,
	hydratedStepInput *dexpb.Value,
	flowId string,
	invocationId string,
) error {
	if decision == nil || !a.cfg.BlobStore.EffectiveEnabled() || a.blobStore == nil {
		return nil
	}
	for _, step := range decision.GetNextSteps() {
		if err := a.reuseOrOffloadStepInput(
			ctx,
			step,
			originalStepInputBlob,
			hydratedStepInput,
			flowId,
			invocationId,
		); err != nil {
			return err
		}
	}
	closeDecision := decision.GetCloseDecision()
	if closeDecision.GetCloseDecisionType() ==
		dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_FORCE_FAIL {
		return nil
	}
	if closeInput := closeDecision.GetCloseInput(); closeInput != nil {
		if stepInputBlobRef(closeInput).id != "" {
			return nil
		}
		if shouldReuseStepInputBlob(originalStepInputBlob, hydratedStepInput, closeInput) {
			closeDecision.CloseInput = originalStepInputBlob.toValue()
			return nil
		}
		if err := a.offloadStepInput(
			ctx,
			closeInput,
			flowId,
			invocationId,
		); err != nil {
			return err
		}
	}
	return nil
}

func (a *Activities) reuseOrOffloadStepInput(
	ctx context.Context,
	step *dexpb.StepMovement,
	originalStepInputBlob stepInputBlob,
	hydratedStepInput *dexpb.Value,
	flowId string,
	invocationId string,
) error {
	if step == nil || step.GetStepInput() == nil ||
		!a.cfg.BlobStore.EffectiveEnabled() || a.blobStore == nil {
		return nil
	}
	if stepInputBlobRef(step.GetStepInput()).id != "" {
		return nil
	}
	if shouldReuseStepInputBlob(originalStepInputBlob, hydratedStepInput, step.GetStepInput()) {
		step.StepInput = originalStepInputBlob.toValue()
		return nil
	}
	return a.offloadStepInput(ctx, step.StepInput, flowId, invocationId)
}

func (a *Activities) offloadStepInput(
	ctx context.Context,
	stepInput *dexpb.Value,
	flowId string,
	invocationId string,
) error {
	return blobstore.OffloadLargeValue(
		ctx,
		stepInput,
		flowId,
		invocationId,
		a.cfg.BlobStore.EffectiveThresholdInBytes(),
		a.blobStore,
		true,
	)
}

type stepInputBlob struct {
	id    string
	isObj bool
}

func stepInputBlobRef(value *dexpb.Value) stepInputBlob {
	if value == nil {
		return stepInputBlob{}
	}
	if blobId := value.GetInternalBlobIdForObjValue(); blobId != "" {
		return stepInputBlob{id: blobId, isObj: true}
	}
	return stepInputBlob{id: value.GetInternalBlobIdForStringValue()}
}

func (blob stepInputBlob) toValue() *dexpb.Value {
	if blob.id == "" {
		return nil
	}
	if blob.isObj {
		return &dexpb.Value{
			Kind: &dexpb.Value_InternalBlobIdForObjValue{InternalBlobIdForObjValue: blob.id},
		}
	}
	return &dexpb.Value{
		Kind: &dexpb.Value_InternalBlobIdForStringValue{InternalBlobIdForStringValue: blob.id},
	}
}

func shouldReuseStepInputBlob(original stepInputBlob, hydrated, next *dexpb.Value) bool {
	if original.id == "" || next == nil {
		return false
	}
	return valuePayloadEqual(hydrated, next)
}

func valuePayloadEqual(left, right *dexpb.Value) bool {
	if left == nil || right == nil {
		return false
	}
	if leftString := left.GetStringValue(); leftString != "" || right.GetStringValue() != "" {
		return leftString == right.GetStringValue()
	}
	leftObj := left.GetObjValue()
	rightObj := right.GetObjValue()
	if leftObj == nil || rightObj == nil {
		return false
	}
	return leftObj.GetEncoding() == rightObj.GetEncoding() &&
		bytes.Equal(leftObj.GetPayload(), rightObj.GetPayload())
}

func validateWorkerWaitForResponse(
	resp *dexpb.InvokeWaitForMethodResponse,
	backendType service.BackendType,
) error {
	if resp == nil {
		return fmt.Errorf("nil InvokeWaitForMethodResponse")
	}
	if err := workerclient.RejectWorkerAttributeWriteBlobIDs(resp.GetUpsertAttributes()); err != nil {
		return err
	}
	if err := workerclient.ValidateHardDeleteAttributeWrites(resp.GetUpsertAttributes()); err != nil {
		return err
	}
	if err := workerclient.ValidateRuntimeAttributeWrites(resp.GetUpsertAttributes()); err != nil {
		return err
	}
	if err := workerclient.ValidateActionPermissionMappings(
		resp.GetActionPermissionMappings(),
		backendType,
	); err != nil {
		return err
	}
	if err := workerclient.RejectWorkerKVBlobIDs(resp.GetUpsertStepExeLocals()); err != nil {
		return err
	}
	if err := workerclient.RejectWorkerKVBlobIDs(resp.GetRecordEvents()); err != nil {
		return err
	}
	if err := validateWorkerChannelMessages(resp.GetPublishToChannel()); err != nil {
		return err
	}
	return validateWorkerChannelDeletions(resp.GetDeleteFromChannel())
}

func validateExecuteResponse(
	resp *dexpb.InvokeExecuteMethodResponse,
	minimumHeartbeatTimeout time.Duration,
	backendType service.BackendType,
) error {
	if err := validateStepDecision(resp.GetStepDecision(), minimumHeartbeatTimeout); err != nil {
		return err
	}
	return validateWorkerExecuteResponse(resp, backendType)
}

func validateWorkerExecuteResponse(
	resp *dexpb.InvokeExecuteMethodResponse,
	backendType service.BackendType,
) error {
	if resp == nil {
		return fmt.Errorf("nil InvokeExecuteMethodResponse")
	}
	if err := workerclient.RejectWorkerAttributeWriteBlobIDs(resp.GetUpsertAttributes()); err != nil {
		return err
	}
	if err := workerclient.ValidateHardDeleteAttributeWrites(resp.GetUpsertAttributes()); err != nil {
		return err
	}
	if err := workerclient.ValidateRuntimeAttributeWrites(resp.GetUpsertAttributes()); err != nil {
		return err
	}
	if err := workerclient.ValidateActionPermissionMappings(
		resp.GetActionPermissionMappings(),
		backendType,
	); err != nil {
		return err
	}
	if err := workerclient.RejectWorkerKVBlobIDs(resp.GetUpsertStepExeLocals()); err != nil {
		return err
	}
	if err := workerclient.RejectWorkerKVBlobIDs(resp.GetRecordEvents()); err != nil {
		return err
	}
	if err := validateWorkerChannelMessages(resp.GetPublishToChannel()); err != nil {
		return err
	}
	return validateWorkerChannelDeletions(resp.GetDeleteFromChannel())
}

func validateWorkerChannelMessages(messages []*dexpb.ChannelMessage) error {
	for index, message := range messages {
		if message == nil || message.GetChannelName() == "" {
			return fmt.Errorf("Channel publication at index %d is invalid", index)
		}
		if message.GetValue() == nil {
			message.Value = &dexpb.Value{}
		}
		if err := workerclient.RejectWorkerBlobIDs(message.GetValue()); err != nil {
			return err
		}
	}
	return nil
}

func validateWorkerChannelDeletions(deletions []*dexpb.ChannelMessageDeletion) error {
	for index, deletion := range deletions {
		if deletion == nil || deletion.GetChannelName() == "" || deletion.GetMessageId() == "" {
			return fmt.Errorf("Channel deletion at index %d is invalid", index)
		}
	}
	return nil
}

func validateStepDecision(
	decision *dexpb.StepDecision,
	minimumHeartbeatTimeout time.Duration,
) error {
	if decision == nil {
		return fmt.Errorf("step decision is nil")
	}
	if len(decision.GetNextSteps()) == 0 && decision.GetCloseDecision() == nil {
		return fmt.Errorf("empty step decision is not supported")
	}
	for index, movement := range decision.GetNextSteps() {
		if movement == nil || movement.GetStepType() == "" {
			return fmt.Errorf("next step at index %d is invalid", index)
		}
		if err := service.ValidateStepType(movement.GetStepType()); err != nil {
			return fmt.Errorf("next step at index %d: %w", index, err)
		}
		if err := service.ValidateStepOptions(
			movement.GetStepOptions(),
			minimumHeartbeatTimeout,
		); err != nil {
			return fmt.Errorf("next step at index %d: %w", index, err)
		}
	}
	for index, stepType := range decision.GetCancelStepTypes() {
		if stepType == "" {
			return fmt.Errorf("cancel step type at index %d is invalid", index)
		}
		if err := service.ValidateStepType(stepType); err != nil {
			return fmt.Errorf("cancel step type at index %d: %w", index, err)
		}
	}
	for index, stepType := range decision.GetCancelSiblingStepTypes() {
		if stepType == "" {
			return fmt.Errorf("cancel sibling step type at index %d is invalid", index)
		}
		if err := service.ValidateStepType(stepType); err != nil {
			return fmt.Errorf("cancel sibling step type at index %d: %w", index, err)
		}
	}
	if closeDecision := decision.GetCloseDecision(); closeDecision != nil {
		return validateCloseDecision(closeDecision, len(decision.GetNextSteps()))
	}
	return nil
}

func validateCloseDecision(closeDecision *dexpb.CloseDecision, nextStepCount int) error {
	closeType := closeDecision.GetCloseDecisionType()
	channelNames := closeDecision.GetConditionalChannelNames()
	if closeType != dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_FORCE_COMPLETE_ON_CHANNELS_EMPTY {
		if nextStepCount > 0 {
			return fmt.Errorf("close decision cannot be combined with next steps")
		}
		if len(channelNames) > 0 {
			return fmt.Errorf("conditional channel names require a conditional close decision")
		}
	}
	switch closeType {
	case dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_FORCE_COMPLETE_ON_CHANNELS_EMPTY:
		if nextStepCount == 0 {
			return fmt.Errorf("conditional close decision requires at least one next step")
		}
		if len(channelNames) == 0 {
			return fmt.Errorf("conditional close decision requires at least one channel")
		}
		seenChannelNames := map[string]struct{}{}
		for _, channelName := range channelNames {
			if channelName == "" {
				return fmt.Errorf("conditional close channel name is empty")
			}
			if _, duplicated := seenChannelNames[channelName]; duplicated {
				return fmt.Errorf("duplicate conditional close channel %q", channelName)
			}
			seenChannelNames[channelName] = struct{}{}
		}
	case dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_GRACEFUL_COMPLETE,
		dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_FORCE_COMPLETE:
	case dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_FORCE_FAIL:
		if closeInput := closeDecision.GetCloseInput(); closeInput != nil {
			if _, isString := closeInput.GetKind().(*dexpb.Value_StringValue); !isString {
				return fmt.Errorf("force fail close input must be a string")
			}
		}
	case dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_DEAD_END:
		if closeDecision.GetCloseInput() != nil {
			return fmt.Errorf("dead end close decision cannot have close input")
		}
	default:
		return fmt.Errorf("close decision type is unspecified")
	}
	return nil
}

func validateWaitingCondition(
	waiting *dexpb.WaitingCondition,
	minimumHeartbeatTimeout time.Duration,
) error {
	if waiting == nil {
		return nil
	}

	declaredIds := map[string]bool{}
	conditionIdsRequired := waiting.GetWaitingConditionType() ==
		dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ANY_COMBINATION_COMPLETED
	for i, timerCondition := range waiting.GetTimerConditions() {
		if timerCondition == nil {
			return fmt.Errorf("timer condition at index %d is nil", i)
		}
		if err := registerWaitingConditionId(
			declaredIds,
			timerCondition.GetConditionId(),
			"timer",
			conditionIdsRequired,
		); err != nil {
			return err
		}
		if timerCondition.GetDurationSeconds() < 0 {
			return fmt.Errorf(
				"timer condition %q has negative duration_seconds %d",
				timerCondition.GetConditionId(),
				timerCondition.GetDurationSeconds(),
			)
		}
		if timerCondition.GetFiringUnixTimestampSeconds() != 0 {
			return fmt.Errorf(
				"timer condition %q sets server-owned firing_unix_timestamp_seconds",
				timerCondition.GetConditionId(),
			)
		}
	}

	for i, channelCondition := range waiting.GetChannelConditions() {
		if channelCondition == nil {
			return fmt.Errorf("channel condition at index %d is nil", i)
		}
		if err := registerWaitingConditionId(
			declaredIds,
			channelCondition.GetConditionId(),
			"channel",
			conditionIdsRequired,
		); err != nil {
			return err
		}
		if channelCondition.GetChannelName() == "" {
			return fmt.Errorf(
				"channel condition %q has an empty channel_name",
				channelCondition.GetConditionId(),
			)
		}
		if channelCondition.AtLeast != nil && channelCondition.GetAtLeast() < 0 {
			return fmt.Errorf(
				"channel condition %q has negative at_least %d",
				channelCondition.GetConditionId(),
				channelCondition.GetAtLeast(),
			)
		}
		if channelCondition.AtMost != nil && channelCondition.GetAtMost() < 0 {
			return fmt.Errorf(
				"channel condition %q has negative at_most %d",
				channelCondition.GetConditionId(),
				channelCondition.GetAtMost(),
			)
		}
		if channelCondition.AtMost != nil &&
			channelCondition.GetAtMost() < channelCondition.GetAtLeast() {
			return fmt.Errorf(
				"channel condition %q has at_most %d < at_least %d",
				channelCondition.GetConditionId(),
				channelCondition.GetAtMost(),
				channelCondition.GetAtLeast(),
			)
		}
	}

	for i, subFlowCondition := range waiting.GetSubFlowConditions() {
		if subFlowCondition == nil {
			return fmt.Errorf("SubFlow condition at index %d is nil", i)
		}
		if err := registerWaitingConditionId(
			declaredIds,
			subFlowCondition.GetConditionId(),
			"SubFlow",
			conditionIdsRequired,
		); err != nil {
			return err
		}
		if subFlowCondition.GetSubFlowType() == "" || subFlowCondition.GetStartStepType() == "" {
			return fmt.Errorf("SubFlow condition at index %d requires Flow and starting Step types", i)
		}
		if err := service.ValidateStepType(subFlowCondition.GetStartStepType()); err != nil {
			return fmt.Errorf("SubFlow condition at index %d: %w", i, err)
		}
		if err := service.ValidateStepOptions(
			subFlowCondition.GetStepOptions(),
			minimumHeartbeatTimeout,
		); err != nil {
			return fmt.Errorf("SubFlow condition at index %d: %w", i, err)
		}
		if subFlowCondition.GetSubFlowIndex() != int32(i) {
			return fmt.Errorf("SubFlow condition at index %d has unstable index %d", i, subFlowCondition.GetSubFlowIndex())
		}
		options := subFlowCondition.GetOptions()
		resolvedTimeoutPolicy, err := service.ResolveFlowTimeoutPolicy(
			options.GetFlowTimeoutSeconds(),
			options.GetFlowTimeoutPolicy(),
		)
		if err != nil {
			return fmt.Errorf("SubFlow condition at index %d: %w", i, err)
		}
		if err := service.ValidateFlowTimeoutHandlerOptions(
			options.GetFlowTimeoutSeconds(),
			resolvedTimeoutPolicy,
			options.GetTimeoutHandlerOptions(),
			minimumHeartbeatTimeout,
		); err != nil {
			return fmt.Errorf("SubFlow condition at index %d: %w", i, err)
		}
		if options.GetFlowStartDelaySeconds() < 0 {
			return fmt.Errorf("SubFlow condition at index %d has a negative start delay", i)
		}
		reusePolicy := options.GetReusePolicy()
		if _, known := dexpb.SubFlowReusePolicy_name[int32(reusePolicy)]; !known {
			return fmt.Errorf("SubFlow condition at index %d has an unknown reuse policy", i)
		}
		if err := workerclient.RejectWorkerBlobIDs(subFlowCondition.GetStepInput()); err != nil {
			return err
		}
		if err := workerclient.ValidateAttributeWrites(
			options.GetAttributes(),
		); err != nil {
			return err
		}
	}

	switch waiting.GetWaitingConditionType() {
	case dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ALL_COMPLETED,
		dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ANY_COMPLETED:
		if len(waiting.GetConditionCombinations()) > 0 {
			return fmt.Errorf("condition_combinations are only valid for ANY_COMBINATION_COMPLETED")
		}
	case dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ANY_COMBINATION_COMPLETED:
		if err := validateWaitingConditionCombinations(waiting, declaredIds); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown waiting_condition_type %d", waiting.GetWaitingConditionType())
	}
	return nil
}

func registerWaitingConditionId(
	declaredIds map[string]bool,
	conditionId string,
	kind string,
	required bool,
) error {
	if conditionId == "" {
		if required {
			return fmt.Errorf("%s condition has an empty condition_id", kind)
		}
		return nil
	}
	if declaredIds[conditionId] {
		return fmt.Errorf("duplicate condition_id %q", conditionId)
	}
	declaredIds[conditionId] = true
	return nil
}

func validateWaitingConditionCombinations(
	waiting *dexpb.WaitingCondition,
	declaredIds map[string]bool,
) error {
	combinations := waiting.GetConditionCombinations()
	if len(combinations) == 0 {
		return fmt.Errorf("ANY_COMBINATION_COMPLETED requires at least one condition_combination")
	}
	for i, combination := range combinations {
		if combination == nil || len(combination.GetConditionIds()) == 0 {
			return fmt.Errorf("condition_combination at index %d is empty", i)
		}
		seen := map[string]bool{}
		for _, conditionId := range combination.GetConditionIds() {
			if !declaredIds[conditionId] {
				return fmt.Errorf(
					"condition_combination at index %d references undeclared condition_id %q",
					i,
					conditionId,
				)
			}
			if seen[conditionId] {
				return fmt.Errorf(
					"condition_combination at index %d has duplicate condition_id %q",
					i,
					conditionId,
				)
			}
			seen[conditionId] = true
		}
	}
	return nil
}

func composeLocalActivityMetadata(ctx *dexpb.Context) *dexpb.LocalActivityMetadata {
	return &dexpb.LocalActivityMetadata{
		CurrentStepExecutionId: ctx.GetStepExecutionId(),
		FromStepExecutionId:    ctx.GetFromStepExecutionId(),
	}
}

func workerAddressForLogging(ctx context.Context, workerTarget *dexpb.WorkerTarget) string {
	if !workerTarget.GetIsHeadlessAddress() {
		return workerTarget.GetAddress()
	}
	resolvedAddress := workerclient.ResolvedWorkerAddressFromContext(ctx)
	if resolvedAddress == "" {
		return workerTarget.GetAddress()
	}
	return resolvedAddress
}

func printDebugMsg(logger interfaces.UnifiedLogger, err error, target string) {
	if os.Getenv(service.EnvNameDebugMode) != "" {
		logger.Info("check error at worker gRPC request", err, target)
	}
}

// Worker and server errors become InternalActivityError before backend-specific encoding.
func newWorkerSideActivityError(
	ctx context.Context,
	provider interfaces.ActivityProvider,
	backendType service.BackendType,
	err error,
	localActivityFailure *dexpb.InternalLocalStepActivityFailure,
	routing ...*dexpb.PullRouting,
) error {
	if ctx.Err() == context.Canceled {
		return context.Canceled
	}
	grpcStatus, ok := status.FromError(err)
	if !ok {
		return newServerSideActivityError(ctx, provider, err, localActivityFailure)
	}

	// Error() is "rpc error: code=… desc=<Message>"; do not concat both.
	detail := grpcStatus.Message()
	if detail == "" {
		detail = err.Error()
	}
	activityError := &dexpb.InternalActivityError{
		WorkerGrpcStatus: int32(grpcStatus.Code()),
	}
	if len(routing) > 0 {
		activityError.PullRouting = routing[0]
	}
	for _, statusDetail := range grpcStatus.Details() {
		workerError, ok := statusDetail.(*dexpb.WorkerErrorResponse)
		if !ok {
			continue
		}
		if backendType == service.BackendTypeCadence && workerError.GetRetryAfterSeconds() != 0 {
			invalidActivityError := &dexpb.InternalActivityError{
				ServerDetail: "WorkerErrorResponse.retry_after_seconds requires the Temporal backend",
			}
			return finalizeActivityError(
				provider,
				dexpb.FlowErrorType_FLOW_ERROR_TYPE_INVALID_USER_FLOW_CODE,
				invalidActivityError,
				localActivityFailure,
				0,
			)
		}
		activityError.WorkerError = &dexpb.InternalWorkerError{
			Detail:     workerError.GetDetail(),
			ErrorType:  workerError.GetErrorType(),
			StackTrace: workerError.GetStackTrace(),
		}
		if activityError.GetWorkerError().GetDetail() == "" {
			activityError.WorkerError.Detail = detail
		}
		retryAfterSeconds := workerError.GetRetryAfterSeconds()
		return finalizeActivityError(
			provider,
			dexpb.FlowErrorType_FLOW_ERROR_TYPE_WORKER_API_FAIL,
			activityError,
			localActivityFailure,
			retryAfterSeconds,
		)
	}
	activityError.ServerDetail = detail
	return finalizeActivityError(
		provider,
		dexpb.FlowErrorType_FLOW_ERROR_TYPE_WORKER_API_FAIL,
		activityError,
		localActivityFailure,
		0,
	)
}

func newServerSideActivityError(
	ctx context.Context,
	provider interfaces.ActivityProvider,
	err error,
	localActivityFailure *dexpb.InternalLocalStepActivityFailure,
) error {
	if ctx.Err() == context.Canceled {
		return context.Canceled
	}
	return finalizeActivityError(
		provider,
		dexpb.FlowErrorType_FLOW_ERROR_TYPE_INTERNAL,
		&dexpb.InternalActivityError{ServerDetail: err.Error()},
		localActivityFailure,
		0,
	)
}

func finalizeActivityError(
	provider interfaces.ActivityProvider,
	errorType dexpb.FlowErrorType,
	activityError *dexpb.InternalActivityError,
	failure *dexpb.InternalLocalStepActivityFailure,
	retryAfterSeconds int32,
) error {
	if activityError == nil {
		panic("activity error required")
	}
	if failure != nil {
		// Nesting keeps the local marker at one details payload.
		failure.ActivityError = activityError
		return provider.NewLocalActivityError(errorType, failure, retryAfterSeconds)
	}
	return provider.NewActivityError(errorType, activityError, retryAfterSeconds)
}

func (a *Activities) logLocalActivityWarn(
	logger interfaces.UnifiedLogger,
	activityInfo interfaces.ActivityInfo, name, stepExeId string, payload any, err error,
) {
	threshold := a.cfg.Interpreter.InterpreterActivityConfig.LogLocalActivityThresholdBytes
	if !activityInfo.IsLocalActivity || threshold <= 0 {
		return
	}
	payloadSize, sized := jsonPayloadSize(logger, payload)
	if !sized || payloadSize < threshold {
		return
	}
	logger.Warn(name+" local activity return on error",
		"workflowId", activityInfo.WorkflowExecution.ID,
		"stepExecutionId", stepExeId,
		"payloadSize", payloadSize,
		"error", err)
}

func jsonPayloadSize(logger interfaces.UnifiedLogger, payload any) (int, bool) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		logger.Error("Failed to serialize local activity payload", "error", err)
		return 0, false
	}
	return len(payloadBytes), true
}

func (a *Activities) emitStepWaitForMethodEvent(
	req *dexpb.InvokeWaitForMethodRequest, activityInfo interfaces.ActivityInfo, eventType event.EventType,
) {
	a.eventHandler(event.Event{
		FlowId:          activityInfo.WorkflowExecution.ID,
		RunId:           activityInfo.WorkflowExecution.RunID,
		FlowType:        req.GetFlowType(),
		StepType:        req.GetStepType(),
		StepExecutionId: req.GetContext().GetStepExecutionId(),
		EventType:       eventType,
	})
}

func (a *Activities) emitStepExecuteMethodEvent(
	req *dexpb.InvokeExecuteMethodRequest, activityInfo interfaces.ActivityInfo, eventType event.EventType,
) {
	a.eventHandler(event.Event{
		FlowId:          activityInfo.WorkflowExecution.ID,
		RunId:           activityInfo.WorkflowExecution.RunID,
		FlowType:        req.GetFlowType(),
		StepType:        req.GetStepType(),
		StepExecutionId: req.GetContext().GetStepExecutionId(),
		EventType:       eventType,
	})
}

// workerStreamDispatch is one admitted dispatch with its open stream.
type workerStreamDispatch[S any] struct {
	stream   S
	callCtx  context.Context
	release  func()
	isOpened bool
}

// openWorkerStream retries unavailable Workers within the wake budget. isOpened reports whether a stream call was attempted.
func openWorkerStream[S any](
	ctx context.Context,
	a *Activities,
	target *dexpb.WorkerTarget,
	flowID string,
	heartbeat func(),
	open func(dexpb.WorkerServiceClient, context.Context) (S, error),
) (workerStreamDispatch[S], error) {
	dispatch := workerStreamDispatch[S]{release: func() {}}
	hold := a.wakeNotifier.NewHold(target.GetAddress(), heartbeat)
	for {
		client, callCtx, releaseConnection, acquireErr := a.workerPool.Acquire(ctx, target, flowID)
		if acquireErr != nil {
			if hold.ShouldRedial(ctx, acquireErr) {
				continue
			}
			return dispatch, acquireErr
		}
		stream, openErr := open(client, callCtx)
		if openErr != nil && hold.ShouldRedial(ctx, openErr) {
			releaseConnection()
			continue
		}
		if openErr == nil {
			hold.RecordSuccess()
		}
		dispatch.stream, dispatch.callCtx, dispatch.isOpened = stream, callCtx, true
		dispatch.release = func() {
			releaseConnection()
		}
		return dispatch, openErr
	}
}

// dispatchHeartbeat keeps a waiting dispatch alive without losing the Step's last heartbeat value.
func (a *Activities) dispatchHeartbeat(
	ctx context.Context,
	activityInfo interfaces.ActivityInfo,
	workerContext *dexpb.Context,
) func() {
	if activityInfo.IsLocalActivity {
		return func() {}
	}
	lastHeartbeatValue := workerContext.GetLastHeartbeatValue()
	return func() {
		if lastHeartbeatValue == nil {
			a.activityProvider.RecordHeartbeat(ctx)
			return
		}
		a.activityProvider.RecordHeartbeat(ctx, lastHeartbeatValue)
	}
}
