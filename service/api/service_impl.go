package api

import (
	"context"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/persistence"
	"net/http"
)

type serviceImpl struct {
	cfg        config.Config
	processOrm persistence.ProcessORM
	logger     log.Logger
}

func NewServiceImpl(cfg config.Config, processOrm persistence.ProcessORM, logger log.Logger) Service {
	return &serviceImpl{
		cfg:        cfg,
		processOrm: processOrm,
		logger:     logger,
	}
}

func (s serviceImpl) StartProcess(
	ctx context.Context, request xdbapi.ProcessExecutionStartRequest,
) (resp *xdbapi.ProcessExecutionStartResponse, err *ErrorWithStatus) {
	resp, alreadyStarted, perr := s.processOrm.StartProcess(ctx, request)
	if perr != nil {
		return nil, s.handleUnknownError(perr)
	}
	if alreadyStarted {
		return nil, NewErrorWithStatus(http.StatusConflict, "Process is already started, try use a different processId or a proper processIdReusePolicy")
	}
	return resp, nil
}

func (s serviceImpl) handleUnknownError(err error) *ErrorWithStatus {
	s.logger.Error("unknown error on operation", tag.Error(err))
	return NewErrorWithStatus(500, err.Error())
}

func (s serviceImpl) DescribeLatestProcess(
	ctx context.Context, request xdbapi.ProcessExecutionDescribeRequest,
) (resp *xdbapi.ProcessExecutionDescribeResponse, err *ErrorWithStatus) {
	resp, notExists, perr := s.processOrm.DescribeLatestProcess(ctx, request)
	if perr != nil {
		return nil, s.handleUnknownError(perr)
	}
	if notExists {
		return nil, NewErrorWithStatus(http.StatusNotFound, "Process does not exist")
	}
	return resp, nil
}
