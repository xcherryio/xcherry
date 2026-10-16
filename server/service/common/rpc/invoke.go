// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package rpc

import (
	"context"
	"fmt"
	"time"

	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/common/blobstore"
	"github.com/superdurable/dex/service/common/channelmessage"
	"github.com/superdurable/dex/service/common/pulldispatch"
	"github.com/superdurable/dex/service/common/utils"
	"github.com/superdurable/dex/service/common/workerclient"
	"github.com/superdurable/dex/service/wake"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// WorkerRequestTooLargeError reports an RPC request that cannot fit the Worker transport limit.
type WorkerRequestTooLargeError struct {
	MaxMessageBytes int
}

// InvokeWorkerRpc invokes a Worker RPC through the selected transport.
func InvokeWorkerRpc(
	ctx context.Context,
	pool *workerclient.WorkerClientPool,
	rpcPrep *dexpb.PrepareRpcQueryResponse,
	req *dexpb.InvokeRPCRequest,
	apiCfg *config.ApiConfig,
	interpreterActivityCfg *config.InterpreterActivityConfig,
	backendType service.BackendType,
	blobStore blobstore.BlobStore,
	invocationId string,
	blobStoreCfg *config.BlobStoreConfig,
	isOutputPersisted bool,
	wakeNotifier *wake.Notifier,
	pullDispatcher pulldispatch.Dispatcher,
	matchingCfg *config.MatchingConfig,
	isLocalActivity bool,
) (*dexpb.InvokeWorkerRPCResponse, string, error) {
	if apiCfg == nil || interpreterActivityCfg == nil {
		panic("InvokeWorkerRpc requires non-nil config sections")
	}
	workerInput := req.GetInput()
	if !blobStoreCfg.EffectiveLazyLoading() {
		if err := blobstore.HydrateKVs(ctx, req.GetFlowId(), rpcPrep.GetAttributes(), blobStore); err != nil {
			return nil, "", err
		}
		if err := blobstore.HydrateChannelValues(
			ctx,
			req.GetFlowId(),
			rpcPrep.GetLoadedChannelMessages(),
			blobStore,
		); err != nil {
			return nil, "", err
		}
		if workerInput != nil {
			// Use a new struct to preserve the request's blob reference
			// because eager hydration replaces Value.Kind in place.
			// The original input will need to be put into history which we don't
			// want it to be hydrated.
			workerInput = &dexpb.Value{Kind: workerInput.GetKind()}
		}
		if err := blobstore.HydrateValue(ctx, req.GetFlowId(), workerInput, blobStore); err != nil {
			return nil, "", err
		}
	}

	timeoutSeconds := req.GetTimeoutSeconds()
	var timeoutPtr *int32
	if timeoutSeconds > 0 {
		timeoutPtr = &timeoutSeconds
	}
	rpcCtx, cancel := utils.TrimContextByTimeoutWithCappedDDL(
		ctx,
		timeoutPtr,
		apiCfg.EffectiveMaxWaitSeconds(),
	)
	defer cancel()

	// Hydrate into new KVs so rpcPrep keeps hard-delete references only.
	attributes, err := blobstore.HydrateHardDeleteKVs(ctx, req.GetFlowId(), rpcPrep.GetAttributes(), blobStore)
	if err != nil {
		return nil, "", err
	}

	channelInfos := rpcPrep.GetChannelInfos()
	if channelInfos == nil {
		channelInfos = map[string]*dexpb.ChannelInfo{}
	}

	workerReq := &dexpb.InvokeWorkerRPCRequest{
		Context: &dexpb.Context{
			FlowId:               req.GetFlowId(),
			RunId:                rpcPrep.GetRunId(),
			FlowStartedTimestamp: rpcPrep.GetFlowStartedTimestamp(),
		},
		FlowType:                    rpcPrep.GetFlowType(),
		RpcName:                     req.GetRpcName(),
		Input:                       workerInput,
		Attributes:                  attributes,
		ChannelInfos:                channelInfos,
		LoadedChannelMessages:       rpcPrep.GetLoadedChannelMessages(),
		LoadedAttributeMapInstances: rpcPrep.GetLoadedAttributeMapInstances(),
		LoadedChannelNames:          rpcPrep.GetLoadedChannelNames(),
		LoadedChannelMapInstances:   rpcPrep.GetLoadedChannelMapInstances(),
	}
	// Classify oversized requests before gRPC wraps the error, without offloading transient payloads.
	if proto.Size(workerReq) > apiCfg.EffectiveGrpcMaxMessageBytes() {
		return nil, "", &WorkerRequestTooLargeError{MaxMessageBytes: apiCfg.EffectiveGrpcMaxMessageBytes()}
	}

	var resp *dexpb.InvokeWorkerRPCResponse
	var servedByAppWorkerID string
	if rpcPrep.GetWorkerTarget().GetIsPullMode() {
		resp, servedByAppWorkerID, err = invokePullWorkerRPC(
			rpcCtx, pullDispatcher, matchingCfg, wakeNotifier,
			rpcPrep.GetDexTaskQueue(), req.GetFlowId(), rpcPrep.GetFlowType(), workerReq, isLocalActivity,
		)
	} else {
		resp, err = invokeWorkerRPCWithinWakeBudget(rpcCtx, pool, wakeNotifier, rpcPrep.GetWorkerTarget(), req.GetFlowId(), workerReq)
	}
	if err != nil {
		return nil, "", err
	}

	if err := validateWorkerRpcResponse(
		resp,
		interpreterActivityCfg.EffectiveMinimumStepHeartbeatTimeout(),
		backendType,
	); err != nil {
		return nil, "", err
	}
	service.SetFromStepExecutionIDForStepDecision(
		resp.GetStepDecision(),
		service.GetFromStepExecutionIdForRPC(req.GetRpcName()),
	)
	if err := channelmessage.AssignIDs(resp.GetPublishToChannel()); err != nil {
		return nil, "", err
	}

	if err := blobstore.OffloadLargeAttributeWrites(
		ctx, resp.GetUpsertAttributes(), req.GetFlowId(), invocationId,
		blobStoreCfg.EffectiveThresholdInBytes(), blobStore, blobStoreCfg.EffectiveEnabled(),
	); err != nil {
		return nil, "", err
	}
	if isOutputPersisted {
		if err := blobstore.OffloadLargeValue(
			ctx, resp.GetOutput(), req.GetFlowId(), invocationId,
			blobStoreCfg.EffectiveThresholdInBytes(), blobStore, blobStoreCfg.EffectiveEnabled(),
		); err != nil {
			return nil, "", err
		}
	}
	if err := offloadRPCSideEffects(
		ctx,
		resp,
		req.GetFlowId(),
		invocationId,
		blobStore,
		blobStoreCfg,
	); err != nil {
		return nil, "", err
	}

	return resp, servedByAppWorkerID, nil
}

// GRPCStatus preserves the Worker transport error contract for server-side size checks.
func (err *WorkerRequestTooLargeError) GRPCStatus() *status.Status {
	return status.New(codes.ResourceExhausted, err.Error())
}

func (err *WorkerRequestTooLargeError) Error() string {
	return fmt.Sprintf("RPC Worker request exceeds the %d-byte gRPC message limit", err.MaxMessageBytes)
}

func offloadRPCSideEffects(
	ctx context.Context,
	response *dexpb.InvokeWorkerRPCResponse,
	flowID string,
	invocationID string,
	blobStore blobstore.BlobStore,
	blobStoreCfg *config.BlobStoreConfig,
) error {
	threshold := blobStoreCfg.EffectiveThresholdInBytes()
	enabled := blobStoreCfg.EffectiveEnabled()
	if err := blobstore.OffloadLargeKVs(
		ctx, response.GetRecordEvents(), flowID, invocationID, threshold, blobStore, enabled,
	); err != nil {
		return err
	}
	if err := blobstore.OffloadLargeChannelMessages(
		ctx, response.GetPublishToChannel(), flowID, invocationID, threshold, blobStore, enabled,
	); err != nil {
		return err
	}
	for _, movement := range response.GetStepDecision().GetNextSteps() {
		if err := blobstore.OffloadLargeValue(
			ctx, movement.GetStepInput(), flowID, invocationID, threshold, blobStore, enabled,
		); err != nil {
			return err
		}
	}
	closeDecision := response.GetStepDecision().GetCloseDecision()
	if closeDecision.GetCloseDecisionType() == dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_FORCE_FAIL {
		return nil
	}
	return blobstore.OffloadLargeValue(
		ctx, closeDecision.GetCloseInput(), flowID, invocationID, threshold, blobStore, enabled,
	)
}

func validateWorkerRpcResponse(
	resp *dexpb.InvokeWorkerRPCResponse,
	minimumHeartbeatTimeout time.Duration,
	backendType service.BackendType,
) error {
	if resp == nil {
		return fmt.Errorf("nil InvokeWorkerRPCResponse")
	}
	if err := workerclient.RejectWorkerBlobIDs(resp.GetOutput()); err != nil {
		return err
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
	if err := workerclient.RejectWorkerKVBlobIDs(resp.GetRecordEvents()); err != nil {
		return err
	}
	for index, message := range resp.GetPublishToChannel() {
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
	for index, deletion := range resp.GetDeleteFromChannel() {
		if deletion == nil || deletion.GetChannelName() == "" || deletion.GetMessageId() == "" {
			return fmt.Errorf("Channel deletion at index %d is invalid", index)
		}
	}
	decision := resp.GetStepDecision()
	if decision == nil {
		return nil
	}
	if decision.GetCloseDecision() != nil {
		return fmt.Errorf("closing flow in RPC is not supported yet")
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
	if len(decision.GetCancelSiblingStepTypes()) > 0 {
		return fmt.Errorf("canceling sibling step executions in RPC is not supported")
	}
	return nil
}

// invokeWorkerRPCWithinWakeBudget calls the Worker, redialing an unavailable Worker within the wake budget.
func invokeWorkerRPCWithinWakeBudget(
	ctx context.Context,
	pool *workerclient.WorkerClientPool,
	wakeNotifier *wake.Notifier,
	target *dexpb.WorkerTarget,
	flowID string,
	workerReq *dexpb.InvokeWorkerRPCRequest,
) (*dexpb.InvokeWorkerRPCResponse, error) {
	hold := wakeNotifier.NewHold(target.GetAddress(), func() {})
	for {
		client, callCtx, release, err := pool.Acquire(ctx, target, flowID)
		if err != nil {
			if hold.ShouldRedial(ctx, err) {
				continue
			}
			return nil, err
		}
		resp, err := client.InvokeWorkerRPC(callCtx, workerReq)
		release()
		if err != nil && hold.ShouldRedial(ctx, err) {
			continue
		}
		if err == nil {
			hold.RecordSuccess()
		}
		return resp, err
	}
}
