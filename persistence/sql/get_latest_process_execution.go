// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package sql

import (
	"context"

	"github.com/xcherryio/xcherry/persistence/data_models"
)

func (p sqlProcessStoreImpl) GetLatestProcessExecution(
	ctx context.Context, request data_models.GetLatestProcessExecutionRequest,
) (*data_models.GetLatestProcessExecutionResponse, error) {
	row, err := p.session.SelectLatestProcessExecution(ctx, request.Namespace, request.ProcessId)
	if err != nil {
		if p.session.IsNotFoundError(err) {
			return &data_models.GetLatestProcessExecutionResponse{
				NotExists: true,
			}, nil
		}
		return nil, err
	}

	info, err := data_models.BytesToProcessExecutionInfo(row.Info)
	if err != nil {
		return nil, err
	}

	return &data_models.GetLatestProcessExecutionResponse{
		ProcessExecutionId: row.ProcessExecutionId,
		Status:             row.Status,
		StartTimestamp:     row.StartTime.Unix(),
		AppDatabaseConfig:  info.AppDatabaseConfig,

		ProcessType: info.ProcessType,
		WorkerUrl:   info.WorkerURL,
	}, nil
}
