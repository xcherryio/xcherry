// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package sql

import (
	"context"
	"github.com/xcherryio/xcherry/common/uuid"
	"github.com/xcherryio/xcherry/extensions"
	"github.com/xcherryio/xcherry/persistence/data_models"
)

func (p sqlProcessStoreImpl) recordProcessExecutionStatusForVisibility(
	ctx context.Context,
	tx extensions.SQLTransaction,
	request data_models.StartProcessRequest,
	processExecutionId uuid.UUID,
	status data_models.ProcessExecutionStatus,
	startTime int64,
	endTime int64) error {

	visibilityTaskInfo := data_models.ImmediateTaskInfoJson{
		VisibilityInfo: &data_models.VisibilityInfoJson{
			Namespace:   request.Request.Namespace,
			ProcessId:   request.Request.ProcessId,
			ProcessType: request.Request.ProcessType,
			Status:      status,
			StartTime:   startTime,
			EndTime:     endTime,
		},
	}

	visibilityTaskInfoBytes, err := data_models.FromImmediateTaskInfoIntoBytes(visibilityTaskInfo)
	if err != nil {
		return err
	}
	visibilityTask := extensions.ImmediateTaskRowForInsert{

		ShardId:            request.NewTaskShardId,
		TaskType:           data_models.ImmediateTaskTypeVisibility,
		ProcessExecutionId: processExecutionId,
		Info:               visibilityTaskInfoBytes,
	}
	err = tx.InsertImmediateTask(ctx, visibilityTask)
	return err
}
