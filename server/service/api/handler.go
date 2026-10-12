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

	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service"
	uclient "github.com/superdurable/dex/service/client"
	"github.com/superdurable/dex/service/common/attributestore"
	"github.com/superdurable/dex/service/common/blobstore"
	"github.com/superdurable/dex/service/common/log"
	"github.com/superdurable/dex/service/common/streamstore"
	"github.com/superdurable/dex/service/common/workerclient"
	"github.com/superdurable/dex/service/tenancy"
	"google.golang.org/protobuf/types/known/emptypb"
)

// DumpFlowForContinueAsNewHeaderObserver is set by integration tests to capture
// incoming metadata on InternalService dump calls.
var DumpFlowForContinueAsNewHeaderObserver func(context.Context)

type handler struct {
	dexpb.UnimplementedFlowServiceServer
	dexpb.UnimplementedInternalServiceServer

	svc    ApiService
	logger log.Logger
}

func newHandler(
	apiCfg *config.ApiConfig,
	blobStoreCfg *config.BlobStoreConfig,
	interpreterCfg *config.Interpreter,
	client uclient.UnifiedClient,
	logger log.Logger,
	store blobstore.BlobStore,
	attributeStore *attributestore.Manager,
	streamStore *streamstore.Store,
	workerPool *workerclient.WorkerClientPool,
	dispatchGate *tenancy.DispatchGate,
) *handler {
	svc, err := NewApiService(
		apiCfg,
		blobStoreCfg,
		interpreterCfg,
		client,
		service.TaskQueue,
		logger,
		store,
		attributeStore,
		streamStore,
		workerPool,
		dispatchGate,
	)
	if err != nil {
		panic(err)
	}
	return &handler{
		svc:    svc,
		logger: logger,
	}
}

func (h *handler) close() {
	h.svc.Close()
}

func (h *handler) GetServerInfo(
	ctx context.Context,
	req *emptypb.Empty,
) (*dexpb.ServerInfo, error) {
	return h.svc.GetServerInfo(ctx, req)
}

func (h *handler) StartFlow(
	ctx context.Context,
	req *dexpb.StartFlowRequest,
) (*dexpb.StartFlowResponse, error) {
	return h.svc.StartFlow(ctx, req)
}

func (h *handler) PublishToChannel(
	ctx context.Context,
	req *dexpb.PublishToChannelRequest,
) (*emptypb.Empty, error) {
	return h.svc.PublishToChannel(ctx, req)
}

func (h *handler) GetChannelMessages(
	ctx context.Context,
	req *dexpb.GetChannelMessagesRequest,
) (*dexpb.GetChannelMessagesResponse, error) {
	return h.svc.GetChannelMessages(ctx, req)
}

func (h *handler) DeleteChannelMessage(
	ctx context.Context,
	req *dexpb.DeleteChannelMessageRequest,
) (*emptypb.Empty, error) {
	return h.svc.DeleteChannelMessage(ctx, req)
}

func (h *handler) WriteStream(
	ctx context.Context,
	req *dexpb.WriteStreamRequest,
) (*emptypb.Empty, error) {
	return h.svc.WriteStream(ctx, req)
}

func (h *handler) ReadStream(
	ctx context.Context,
	req *dexpb.ReadStreamRequest,
) (*dexpb.ReadStreamResponse, error) {
	return h.svc.ReadStream(ctx, req)
}

func (h *handler) ListStreamMessages(
	ctx context.Context,
	req *dexpb.ListStreamMessagesRequest,
) (*dexpb.ListStreamMessagesResponse, error) {
	return h.svc.ListStreamMessages(ctx, req)
}

func (h *handler) StopFlow(
	ctx context.Context,
	req *dexpb.StopFlowRequest,
) (*emptypb.Empty, error) {
	return h.svc.StopFlow(ctx, req)
}

func (h *handler) GetAttributes(
	ctx context.Context,
	req *dexpb.GetAttributesRequest,
) (*dexpb.GetAttributesResponse, error) {
	return h.svc.GetAttributes(ctx, req)
}

func (h *handler) SetAttributes(
	ctx context.Context,
	req *dexpb.SetAttributesRequest,
) (*emptypb.Empty, error) {
	return h.svc.SetAttributes(ctx, req)
}

func (h *handler) LoadBlobs(
	ctx context.Context,
	req *dexpb.LoadBlobsRequest,
) (*dexpb.LoadBlobsResponse, error) {
	return h.svc.LoadBlobs(ctx, req)
}

func (h *handler) WaitForFlow(
	ctx context.Context,
	req *dexpb.WaitForFlowRequest,
) (*dexpb.FlowResult, error) {
	return h.svc.WaitForFlow(ctx, req)
}

func (h *handler) SearchFlows(
	ctx context.Context,
	req *dexpb.SearchFlowsRequest,
) (*dexpb.SearchFlowsResponse, error) {
	return h.svc.SearchFlows(ctx, req)
}

func (h *handler) SyncAttributeIndexes(
	ctx context.Context,
	req *dexpb.SyncAttributeIndexRequest,
) (*dexpb.SyncAttributeIndexResponse, error) {
	return h.svc.SyncAttributeIndexes(ctx, req)
}

func (h *handler) GetFlowSummary(
	ctx context.Context,
	req *dexpb.GetFlowSummaryRequest,
) (*dexpb.GetFlowSummaryResponse, error) {
	return h.svc.GetFlowSummary(ctx, req)
}

func (h *handler) GetHistoryEvents(
	ctx context.Context,
	req *dexpb.GetHistoryEventsRequest,
) (*dexpb.GetHistoryEventsResponse, error) {
	return h.svc.GetHistoryEvents(ctx, req)
}

func (h *handler) WaitForHistoryEvent(
	ctx context.Context,
	req *dexpb.WaitForHistoryEventRequest,
) (*dexpb.WaitForHistoryEventResponse, error) {
	return h.svc.WaitForHistoryEvent(ctx, req)
}

func (h *handler) GetFlowState(
	ctx context.Context,
	req *dexpb.GetFlowStateRequest,
) (*dexpb.GetFlowStateResponse, error) {
	return h.svc.GetFlowState(ctx, req)
}

func (h *handler) ResetFlow(
	ctx context.Context,
	req *dexpb.ResetFlowRequest,
) (*dexpb.ResetFlowResponse, error) {
	return h.svc.ResetFlow(ctx, req)
}

func (h *handler) InvokeRPC(
	ctx context.Context,
	req *dexpb.InvokeRPCRequest,
) (*dexpb.InvokeRPCResponse, error) {
	return h.svc.InvokeRPC(ctx, req)
}

func (h *handler) SkipTimer(
	ctx context.Context,
	req *dexpb.SkipTimerRequest,
) (*emptypb.Empty, error) {
	return h.svc.SkipTimer(ctx, req)
}

func (h *handler) UpdateFlowConfig(
	ctx context.Context,
	req *dexpb.UpdateFlowConfigRequest,
) (*emptypb.Empty, error) {
	return h.svc.UpdateFlowConfig(ctx, req)
}

func (h *handler) WaitForStepCompletion(
	ctx context.Context,
	req *dexpb.WaitForStepCompletionRequest,
) (*dexpb.WaitForStepCompletionResponse, error) {
	return h.svc.WaitForStepCompletion(ctx, req)
}

func (h *handler) WaitForAttribute(
	ctx context.Context,
	req *dexpb.WaitForAttributeRequest,
) (*dexpb.WaitForAttributeResponse, error) {
	return h.svc.WaitForAttribute(ctx, req)
}

func (h *handler) TriggerContinueAsNew(
	ctx context.Context,
	req *dexpb.TriggerContinueAsNewRequest,
) (*emptypb.Empty, error) {
	return h.svc.TriggerContinueAsNew(ctx, req)
}

func (h *handler) HealthCheck(
	ctx context.Context,
	req *emptypb.Empty,
) (*dexpb.HealthInfo, error) {
	return h.svc.HealthCheck(ctx, req)
}

func (h *handler) DumpFlowForContinueAsNew(
	ctx context.Context,
	req *dexpb.ContinueAsNewDumpRequest,
) (*dexpb.ContinueAsNewDumpResponse, error) {
	if DumpFlowForContinueAsNewHeaderObserver != nil {
		DumpFlowForContinueAsNewHeaderObserver(ctx)
	}
	return h.svc.DumpFlowForContinueAsNew(ctx, req)
}
