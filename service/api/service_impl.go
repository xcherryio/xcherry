package api

import (
	"context"
	"fmt"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/config"
	persistence "github.com/xdblab/xdb/persistence"
	"github.com/xdblab/xdb/service/async"
	"io/ioutil"
	"net/http"
	"time"
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
	if resp.HasNewWorkerTask {
		s.notifyRemoteWorkerTask(ctx, persistence.DefaultShardId)
	}
	return &xdbapi.ProcessExecutionStartResponse{
		ProcessExecutionId: resp.ProcessExecutionId.String(),
	}, nil
}

func (s serviceImpl) handleUnknownError(err error) *ErrorWithStatus {
	s.logger.Error("unknown error on operation", tag.Error(err))
	return NewErrorWithStatus(500, err.Error())
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

func (s serviceImpl) notifyRemoteWorkerTask(_ context.Context, shardId int32) {
	// execute in the background as best effort
	go func() {
		url := fmt.Sprintf("%v%v?shardId=%v",
			s.cfg.AsyncService.ClientAddress, async.PathNotifyWorkerTask, shardId)
		ctx, canf := context.WithTimeout(context.Background(), time.Second*10)
		defer canf()

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			s.logger.Error("failed to create request to notify remote worker task",
				tag.Value(url), tag.Error(err))
			return
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode != http.StatusOK {
			statusCode := -1
			responseBody := "cannot read body from http response"
			if resp != nil {
				defer resp.Body.Close()
				statusCode = resp.StatusCode
				body, err := ioutil.ReadAll(resp.Body)
				if err == nil {
					responseBody = string(body)
				}
			}
			s.logger.Error("failed to notify remote worker task",
				tag.Shard(shardId), tag.Error(err), tag.StatusCode(statusCode),
				tag.Message(responseBody))
		}
	}()
}
