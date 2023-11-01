// Copyright 2023 XDBLab organization
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"context"
	"net/http"
	"time"

	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/common/ptr"
	"github.com/xdblab/xdb/config"
	persistence "github.com/xdblab/xdb/persistence"
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
	ctx context.Context, request xdbapi.ProcessExecutionStartRequest,
) (response *xdbapi.ProcessExecutionStartResponse, retErr *ErrorWithStatus) {
	resp, perr := s.store.StartProcess(ctx, persistence.StartProcessRequest{
		Request:        request,
		NewTaskShardId: persistence.DefaultShardId,
	})
	if perr != nil {
		return nil, s.handleUnknownError(perr)
	}

	if resp.AlreadyStarted {
		return nil, NewErrorWithStatus(
			http.StatusConflict,
			"Process is already started, try use a different processId or a proper processIdReusePolicy")
	}
	if resp.GlobalAttributeWriteFailed {
		return nil, NewErrorWithStatus(
			http.StatusFailedDependency,
			"Failed to write global attributes, please check the error message for details: "+resp.GlobalAttributeWriteError.Error())
	}
	if resp.HasNewImmediateTask {
		s.notifyRemoteImmediateTaskAsync(ctx, xdbapi.NotifyImmediateTasksRequest{
			ShardId:            persistence.DefaultShardId,
			Namespace:          &request.Namespace,
			ProcessId:          &request.ProcessId,
			ProcessExecutionId: ptr.Any(resp.ProcessExecutionId.String()),
		})
	}
	return &xdbapi.ProcessExecutionStartResponse{
		ProcessExecutionId: resp.ProcessExecutionId.String(),
	}, nil
}

func (s serviceImpl) StopProcess(
	ctx context.Context, request xdbapi.ProcessExecutionStopRequest,
) *ErrorWithStatus {
	resp, err := s.store.StopProcess(ctx, persistence.StopProcessRequest{
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
	ctx context.Context, request xdbapi.ProcessExecutionDescribeRequest,
) (response *xdbapi.ProcessExecutionDescribeResponse, retErr *ErrorWithStatus) {
	resp, perr := s.store.DescribeLatestProcess(ctx, persistence.DescribeLatestProcessRequest{
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
	ctx context.Context, request xdbapi.PublishToLocalQueueRequest,
) *ErrorWithStatus {
	resp, err := s.store.PublishToLocalQueue(ctx, persistence.PublishToLocalQueueRequest{
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

	if resp.HasNewImmediateTask {
		s.notifyRemoteImmediateTaskAsync(ctx, xdbapi.NotifyImmediateTasksRequest{
			ShardId:            persistence.DefaultShardId,
			Namespace:          &request.Namespace,
			ProcessId:          &request.ProcessId,
			ProcessExecutionId: ptr.Any(resp.ProcessExecutionId.String()),
		})
	}

	return nil
}

func (s serviceImpl) notifyRemoteImmediateTaskAsync(_ context.Context, req xdbapi.NotifyImmediateTasksRequest) {
	// execute in the background as best effort
	go func() {

		ctx, canf := context.WithTimeout(context.Background(), time.Second*10)
		defer canf()

		apiClient := xdbapi.NewAPIClient(&xdbapi.Configuration{
			Servers: []xdbapi.ServerConfiguration{
				{
					URL: s.cfg.AsyncService.ClientAddress,
				},
			},
		})

		request := apiClient.DefaultAPI.InternalApiV1XdbNotifyImmediateTasksPost(ctx)
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

func (s serviceImpl) handleUnknownError(err error) *ErrorWithStatus {
	s.logger.Error("unknown error on operation", tag.Error(err))
	return NewErrorWithStatus(500, err.Error())
}
