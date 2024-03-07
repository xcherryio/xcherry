// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package process

import (
	"context"
	"github.com/xcherryio/xcherry/common/uuid"
	"github.com/xcherryio/xcherry/extensions"
	"github.com/xcherryio/xcherry/persistence/data_models"
)

func (p sqlProcessStoreImpl) AddVisibilityTaskRecordProcessExecutionStatus(
	ctx context.Context,
	tx extensions.SQLTransaction,
	shardId int32,
	namespace string,
	processId string,
	processType string,
	processExecutionId uuid.UUID,
	status data_models.ProcessExecutionStatus,
	startTime *int64,
	endTime *int64) error {

	visibilityTaskInfo := data_models.ImmediateTaskInfoJson{
		VisibilityInfo: &data_models.VisibilityInfoJson{
			Namespace:          namespace,
			ProcessId:          processId,
			ProcessType:        processType,
			ProcessExecutionId: processExecutionId,
			Status:             status,
			StartTime:          startTime,
			CloseTime:          endTime,
		},
	}

	visibilityTaskInfoBytes, err := data_models.FromImmediateTaskInfoIntoBytes(visibilityTaskInfo)
	if err != nil {
		return err
	}
	visibilityTask := extensions.ImmediateTaskRowForInsert{

		ShardId:            shardId,
		TaskType:           data_models.ImmediateTaskTypeVisibility,
		ProcessExecutionId: processExecutionId,
		Info:               visibilityTaskInfoBytes,
	}
	// TODO: upsert for starting process, update for closing process
	err = tx.InsertImmediateTask(ctx, visibilityTask)
	return err
}
