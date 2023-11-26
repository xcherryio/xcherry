// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: BUSL-1.1

package api

import (
	"context"
	"net/http"
	"time"

	"github.com/xcherryio/apis/goapi/xcapi"
	"github.com/xcherryio/xcherry/common/decision"
	"github.com/xcherryio/xcherry/common/httperror"
	"github.com/xcherryio/xcherry/common/urlautofix"
	"github.com/xcherryio/xcherry/persistence/data_models"

	"github.com/xcherryio/xcherry/common/log"
	"github.com/xcherryio/xcherry/common/log/tag"
	"github.com/xcherryio/xcherry/common/ptr"
	"github.com/xcherryio/xcherry/config"
	persistence "github.com/xcherryio/xcherry/persistence"
)

type serviceImpl struct {
	cfg    config.Config
	store  persistence.ProcessStore
	logger log.Logger
}

func NewServiceImpl(cfg config.Config, store persistence.ProcessStore, logger log.Logger) Service {
	return &serviceImpl{
		cfg:    cfg,
		store:  store,
		logger: logger,
	}
}

func (s serviceImpl) StartProcess(
	ctx context.Context, request xcapi.ProcessExecutionStartRequest,
) (response *xcapi.ProcessExecutionStartResponse, retErr *ErrorWithStatus) {
	timeoutUnixSeconds := 0
	if request.ProcessStartConfig != nil && request.ProcessStartConfig.TimeoutSeconds != nil {
		timeoutUnixSeconds = int(request.ProcessStartConfig.GetTimeoutSeconds())
	}

	storeReq := data_models.StartProcessRequest{
		Request:        request,
		NewTaskShardId: persistence.DefaultShardId,
	}
	if timeoutUnixSeconds > 0 {
		storeReq.TimeoutTimeUnixSeconds = time.Now().Unix() + int64(timeoutUnixSeconds)
	}

	resp, perr := s.store.StartProcess(ctx, storeReq)
	if perr != nil {
		return nil, s.handleUnknownError(perr)
	}

	if resp.AlreadyStarted {
		return nil, NewErrorWithStatus(
			http.StatusConflict,
			"Process is already started, try use a different processId or a proper processIdReusePolicy")
	}
	if resp.FailedAtWriteInitGlobalAttributes {
		return nil, NewErrorWithStatus(
			http.StatusFailedDependency,
			"Failed to write global attributes, please check the error message for details: "+resp.GlobalAttributeWriteError.Error())
	}
	if resp.FailedAtWriteInitLocalAttributes {
		return nil, NewErrorWithStatus(
			http.StatusFailedDependency,
			"Failed to write local attributes, please check the error message for details: "+resp.LocalAttributeWriteError.Error())
	}
	if resp.HasNewImmediateTask {
		s.notifyRemoteImmediateTaskAsync(ctx, xcapi.NotifyImmediateTasksRequest{
			ShardId:            persistence.DefaultShardId,
			Namespace:          &request.Namespace,
			ProcessId:          &request.ProcessId,
			ProcessExecutionId: ptr.Any(resp.ProcessExecutionId.String()),
		})
	}

	if storeReq.TimeoutTimeUnixSeconds != 0 {
		s.notifyRemoteTimerTaskAsync(ctx, xcapi.NotifyTimerTasksRequest{
			ShardId:            persistence.DefaultShardId,
			Namespace:          &request.Namespace,
			ProcessId:          &request.ProcessId,
			ProcessExecutionId: ptr.Any(resp.ProcessExecutionId.String()),
			FireTimestamps:     []int64{storeReq.TimeoutTimeUnixSeconds},
		})
	}

	return &xcapi.ProcessExecutionStartResponse{
		ProcessExecutionId: resp.ProcessExecutionId.String(),
	}, nil
}

func (s serviceImpl) StopProcess(
	ctx context.Context, request xcapi.ProcessExecutionStopRequest,
) *ErrorWithStatus {
	resp, err := s.store.StopProcess(ctx, data_models.StopProcessRequest{
		Namespace:       request.GetNamespace(),
		ProcessId:       request.GetProcessId(),
		ProcessStopType: request.GetStopType(),
	})
	if err != nil {
		return s.handleUnknownError(err)
	}

	if resp.NotExists {
		return NewErrorWithStatus(http.StatusNotFound, "Process does not exist")
	}

	return nil
}

func (s serviceImpl) DescribeLatestProcess(
	ctx context.Context, request xcapi.ProcessExecutionDescribeRequest,
) (response *xcapi.ProcessExecutionDescribeResponse, retErr *ErrorWithStatus) {
	resp, perr := s.store.DescribeLatestProcess(ctx, data_models.DescribeLatestProcessRequest{
		Namespace: request.Namespace,
		ProcessId: request.ProcessId,
	})
	if perr != nil {
		return nil, s.handleUnknownError(perr)
	}
	if resp.NotExists {
		return nil, NewErrorWithStatus(http.StatusNotFound, "Process does not exist")
	}
	return resp.Response, nil
}

func (s serviceImpl) PublishToLocalQueue(
	ctx context.Context, request xcapi.PublishToLocalQueueRequest,
) *ErrorWithStatus {
	resp, err := s.store.PublishToLocalQueue(ctx, data_models.PublishToLocalQueueRequest{
		Namespace: request.GetNamespace(),
		ProcessId: request.GetProcessId(),
		Messages:  request.GetMessages(),
	})
	if err != nil {
		return s.handleUnknownError(err)
	}

	if resp.ProcessNotExists {
		return NewErrorWithStatus(http.StatusNotFound, "Process does not exist")
	}

	if resp.ProcessNotRunning {
		return NewErrorWithStatus(http.StatusMethodNotAllowed, "Process is not running")
	}

	if resp.HasNewImmediateTask {
		s.notifyRemoteImmediateTaskAsync(ctx, xcapi.NotifyImmediateTasksRequest{
			ShardId:            persistence.DefaultShardId,
			Namespace:          &request.Namespace,
			ProcessId:          &request.ProcessId,
			ProcessExecutionId: ptr.Any(resp.ProcessExecutionId.String()),
		})
	}

	return nil
}

func (s serviceImpl) Rpc(
	ctx context.Context, request xcapi.ProcessExecutionRpcRequest,
) (response *xcapi.ProcessExecutionRpcResponse, retErr *ErrorWithStatus) {
	latestPrcExe, err := s.store.GetLatestProcessExecution(ctx, data_models.GetLatestProcessExecutionRequest{
		Namespace: request.GetNamespace(),
		ProcessId: request.GetProcessId(),
	})
	if err != nil {
		return nil, s.handleUnknownError(err)
	}

	if latestPrcExe.NotExists {
		return nil, NewErrorWithStatus(http.StatusNotFound, "Process does not exist")
	}

	iwfWorkerBaseUrl := urlautofix.FixWorkerUrl(latestPrcExe.WorkerUrl)
	apiClient := xcapi.NewAPIClient(&xcapi.Configuration{
		Servers: []xcapi.ServerConfiguration{
			{
				URL: iwfWorkerBaseUrl,
			},
		},
	})

	loadGlobalAttributeResponse := xcapi.LoadGlobalAttributeResponse{}
	if latestPrcExe.GlobalAttributeConfig != nil {
		loadGlobalAttrResp, err := s.store.LoadGlobalAttributes(ctx, data_models.LoadGlobalAttributesRequest{
			TableConfig: *latestPrcExe.GlobalAttributeConfig,
			Request:     request.GetLoadGlobalAttributesRequest(),
		})
		if err != nil {
			return nil, s.handleUnknownError(err)
		}

		loadGlobalAttributeResponse = loadGlobalAttrResp.Response
	}

	workerApiCtx, cancF := s.createContextWithTimeoutForRpc(ctx, request.GetTimeoutSeconds())
	defer cancF()

	req := apiClient.DefaultAPI.ApiV1XcherryWorkerProcessRpcPost(workerApiCtx)
	resp, httpResp, err := req.ProcessRpcWorkerRequest(
		xcapi.ProcessRpcWorkerRequest{
			Context: xcapi.Context{
				ProcessId:               request.GetProcessId(),
				ProcessExecutionId:      latestPrcExe.ProcessExecutionId.String(),
				ProcessStartedTimestamp: latestPrcExe.StartTimestamp,
			},
			ProcessType:            latestPrcExe.ProcessType,
			RpcName:                request.GetRpcName(),
			Input:                  request.Input,
			LoadedGlobalAttributes: &loadGlobalAttributeResponse,
		},
	).Execute()
	if httpResp != nil {
		defer httpResp.Body.Close()
	}

	if httperror.CheckHttpResponseAndError(err, httpResp, s.logger) {
		return nil, NewErrorWithStatus(
			http.StatusFailedDependency,
			"Failed to call worker RPC method. Error: "+err.Error()+" Http response: "+httpResp.Status)
	}

	err = decision.ValidateDecision(resp.StateDecision)
	if err != nil {
		return nil, NewErrorWithStatus(
			http.StatusBadRequest, err.Error())
	}

	updateResp, err := s.store.UpdateProcessExecutionForRpc(ctx, data_models.UpdateProcessExecutionForRpcRequest{
		Namespace:          request.Namespace,
		ProcessId:          request.ProcessId,
		ProcessType:        latestPrcExe.ProcessType,
		ProcessExecutionId: latestPrcExe.ProcessExecutionId,

		StateDecision:       resp.GetStateDecision(),
		PublishToLocalQueue: resp.GetPublishToLocalQueue(),

		GlobalAttributeTableConfig: latestPrcExe.GlobalAttributeConfig,
		UpdateGlobalAttributes:     resp.WriteToGlobalAttributes,

		WorkerUrl:   latestPrcExe.WorkerUrl,
		TaskShardId: persistence.DefaultShardId,
	})
	if err != nil {
		return nil, s.handleUnknownError(err)
	}
	if updateResp.FailAtUpdatingGlobalAttributes {
		s.logger.Warn("failed to update global attributes")
		return nil, NewErrorWithStatus(
			http.StatusFailedDependency,
			"Failed to write global attributes, please check the error message for details: "+updateResp.UpdatingGlobalAttributesError.Error())
	}
	if updateResp.ProcessNotExists {
		return nil, NewErrorWithStatus(http.StatusNotFound, "Process does not exist")
	}

	if updateResp.HasNewImmediateTask {
		processExecutionIdString := latestPrcExe.ProcessExecutionId.String()

		s.notifyRemoteImmediateTaskAsync(ctx, xcapi.NotifyImmediateTasksRequest{
			ShardId:            persistence.DefaultShardId,
			Namespace:          &request.Namespace,
			ProcessId:          &request.ProcessId,
			ProcessExecutionId: &processExecutionIdString,
		})
	}

	return &xcapi.ProcessExecutionRpcResponse{
		Output: resp.Output,
	}, nil
}

func (s serviceImpl) notifyRemoteImmediateTaskAsync(_ context.Context, req xcapi.NotifyImmediateTasksRequest) {
	// execute in the background as best effort
	go func() {

		ctx, canf := context.WithTimeout(context.Background(), time.Second*10)
		defer canf()

		apiClient := xcapi.NewAPIClient(&xcapi.Configuration{
			Servers: []xcapi.ServerConfiguration{
				{
					URL: s.cfg.AsyncService.ClientAddress,
				},
			},
		})

		request := apiClient.DefaultAPI.InternalApiV1XcherryNotifyImmediateTasksPost(ctx)
		httpResp, err := request.NotifyImmediateTasksRequest(req).Execute()
		if httpResp != nil {
			defer httpResp.Body.Close()
		}
		if err != nil {
			s.logger.Error("failed to notify remote immediate task", tag.Error(err))
			// TODO add backoff and retry
			return
		}
	}()
}

func (s serviceImpl) notifyRemoteTimerTaskAsync(_ context.Context, req xcapi.NotifyTimerTasksRequest) {
	// execute in the background as best effort
	go func() {

		ctx, canf := context.WithTimeout(context.Background(), time.Second*10)
		defer canf()

		apiClient := xcapi.NewAPIClient(&xcapi.Configuration{
			Servers: []xcapi.ServerConfiguration{
				{
					URL: s.cfg.AsyncService.ClientAddress,
				},
			},
		})

		request := apiClient.DefaultAPI.InternalApiV1XcherryNotifyTimerTasksPost(ctx)
		httpResp, err := request.NotifyTimerTasksRequest(req).Execute()
		if httpResp != nil {
			defer httpResp.Body.Close()
		}
		if err != nil {
			s.logger.Error("failed to notify remote timer task", tag.Error(err))
			// TODO add backoff and retry
			return
		}
	}()
}

func (s serviceImpl) handleUnknownError(err error) *ErrorWithStatus {
	s.logger.Error("unknown error on operation", tag.Error(err))
	return NewErrorWithStatus(500, err.Error())
}

func (s serviceImpl) createContextWithTimeoutForRpc(ctx context.Context, timeoutFromRequest int32,
) (context.Context, context.CancelFunc) {
	qCfg := s.cfg.ApiService.Rpc

	timeout := qCfg.DefaultRpcAPITimeout

	if timeoutFromRequest > 0 {
		timeout = time.Duration(timeoutFromRequest) * time.Second
	}

	if timeout > qCfg.MaxRpcAPITimeout {
		timeout = qCfg.MaxRpcAPITimeout
	}

	return context.WithTimeout(ctx, timeout)
}
