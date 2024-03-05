// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package visibility

import (
	"context"
	"fmt"
	"github.com/xcherryio/xcherry/common/log"
	"github.com/xcherryio/xcherry/config"
	"github.com/xcherryio/xcherry/extensions"
	"github.com/xcherryio/xcherry/persistence"
	"github.com/xcherryio/xcherry/persistence/data_models"
	"time"
)

type sqlVisibilityStoreImpl struct {
	session extensions.SQLDBSession
	logger  log.Logger
}

func NewSqlVisibilityStore(sqlConfig config.SQL, logger log.Logger) (persistence.VisibilityStore, error) {
	session, err := extensions.NewSQLSession(&sqlConfig)
	return &sqlVisibilityStoreImpl{
		session: session,
		logger:  logger,
	}, err
}

func (p sqlVisibilityStoreImpl) Close() error {
	return p.session.Close()
}

func (p sqlVisibilityStoreImpl) RecordProcessExecutionStatus(
	ctx context.Context, req data_models.RecordProcessExecutionStatusRequest) error {
	if req.Status == data_models.ProcessExecutionStatusUndefined {
		return fmt.Errorf("process status is undefined")
	}

	if req.Status == data_models.ProcessExecutionStatusRunning {
		if req.StartTime == nil {
			return fmt.Errorf("start time is required for recording visibility for running process")
		}
		return p.session.InsertProcessExecutionStartForVisibility(ctx, extensions.ExecutionVisibilityRow{
			Namespace:          req.Namespace,
			ProcessId:          req.ProcessId,
			ProcessExecutionId: req.ProcessExecutionId,
			ProcessTypeName:    req.ProcessType,
			Status:             req.Status,
			StartTime:          time.Unix(*req.StartTime, 0),
		})
	}
	return p.session.UpdateProcessExecutionStatusForVisibility(ctx, extensions.ExecutionVisibilityRow{
		Namespace:          req.Namespace,
		ProcessId:          req.ProcessId,
		ProcessExecutionId: req.ProcessExecutionId,
		ProcessTypeName:    req.ProcessType,
		Status:             req.Status,
		CloseTime:          time.Unix(*req.CloseTime, 0),
	})
}
