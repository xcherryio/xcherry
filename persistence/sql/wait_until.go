// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package sql

import (
	"context"
	"github.com/xcherryio/apis/goapi/xcapi"
	"github.com/xcherryio/xcherry/persistence/data_models"
	"time"

	"github.com/xcherryio/xcherry/common/log/tag"
	"github.com/xcherryio/xcherry/extensions"
)

func (p sqlProcessStoreImpl) ProcessWaitUntilExecution(
	ctx context.Context, request data_models.ProcessWaitUntilExecutionRequest,
) (*data_models.ProcessWaitUntilExecutionResponse, error) {
	tx, err := p.session.StartTransaction(ctx, defaultTxOpts)
	if err != nil {
		return nil, err
	}

	resp, err := p.doProcessWaitUntilExecutionTx(ctx, tx, request)
	if err != nil {
		err2 := tx.Rollback()
		if err2 != nil {
			p.logger.Error("error on rollback transaction", tag.Error(err2))
		}
	} else {
		err = tx.Commit()
		if err != nil {
			p.logger.Error("error on committing transaction", tag.Error(err))
			return nil, err
		}
	}
	return resp, err
}

func (p sqlProcessStoreImpl) doProcessWaitUntilExecutionTx(
	ctx context.Context, tx extensions.SQLTransaction, request data_models.ProcessWaitUntilExecutionRequest,
) (*data_models.ProcessWaitUntilExecutionResponse, error) {
	hasNewImmediateTask := false
	var fireTimestamps []int64

	if request.CommandRequest.GetWaitingType() == xcapi.EMPTY_COMMAND {
		hasNewImmediateTask = true
		err := p.completeWaitUntilExecution(ctx, tx, data_models.CompleteWaitUntilExecutionRequest{
			TaskShardId:        request.TaskShardId,
			ProcessExecutionId: request.ProcessExecutionId,
			StateExecutionId:   request.StateExecutionId,
			PreviousVersion:    request.Prepare.PreviousVersion,
		})
		if err != nil {
			return nil, err
		}
	} else {
		resp, err := p.updateWaitUntilExecution(ctx, tx, request)
		if err != nil {
			return nil, err
		}

		if resp.HasNewImmediateTask {
			hasNewImmediateTask = true
		}
		fireTimestamps = resp.FireTimestamps
	}

	hasNewImmediateTask2, err := p.publishToLocalQueue(ctx, tx, request.ProcessExecutionId, request.PublishToLocalQueue)
	if err != nil {
		return nil, err
	}
	if hasNewImmediateTask2 {
		hasNewImmediateTask = true
	}

	err = tx.DeleteImmediateTask(ctx, extensions.ImmediateTaskRowDeleteFilter{
		ShardId:      request.TaskShardId,
		TaskSequence: request.TaskSequence,
	})
	if err != nil {
		return nil, err
	}

	return &data_models.ProcessWaitUntilExecutionResponse{
		HasNewImmediateTask: hasNewImmediateTask,
		FireTimestamps:      fireTimestamps,
	}, nil
}

func (p sqlProcessStoreImpl) completeWaitUntilExecution(
	ctx context.Context, tx extensions.SQLTransaction, request data_models.CompleteWaitUntilExecutionRequest,
) error {
	stateRow := extensions.AsyncStateExecutionRowForUpdateWithoutCommands{
		ProcessExecutionId: request.ProcessExecutionId,
		StateId:            request.StateId,
		StateIdSequence:    request.StateIdSequence,
		Status:             data_models.StateExecutionStatusExecuteRunning,
		PreviousVersion:    request.PreviousVersion,
		LastFailure:        nil,
	}

	err := tx.UpdateAsyncStateExecutionWithoutCommands(ctx, stateRow)
	if err != nil {
		if p.session.IsConditionalUpdateFailure(err) {
			p.logger.Warn("UpdateAsyncStateExecutionWithoutCommands failed at conditional update")
		}
		return err
	}

	return tx.InsertImmediateTask(ctx, extensions.ImmediateTaskRowForInsert{
		ShardId:            request.TaskShardId,
		TaskType:           data_models.ImmediateTaskTypeExecute,
		ProcessExecutionId: request.ProcessExecutionId,
		StateId:            request.StateId,
		StateIdSequence:    request.StateIdSequence,
	})
}

func (p sqlProcessStoreImpl) updateWaitUntilExecution(
	ctx context.Context, tx extensions.SQLTransaction, request data_models.ProcessWaitUntilExecutionRequest,
) (*data_models.ProcessWaitUntilExecutionResponse, error) {
	hasLocalQueueCommands := len(request.CommandRequest.GetLocalQueueCommands()) > 0

	var prcRow *extensions.ProcessExecutionRowForUpdate
	var localQueues data_models.StateExecutionLocalQueuesJson
	var consumedMessagesMap map[int][]data_models.InternalLocalQueueMessage

	// Step 1: get localQueues from the process execution row,
	// update it with commands, and try to consume for the state execution
	if hasLocalQueueCommands {
		prcRow2, err := tx.SelectProcessExecutionForUpdate(ctx, request.ProcessExecutionId)
		if err != nil {
			return nil, err
		}

		prcRow = prcRow2

		localQueues, err = data_models.NewStateExecutionLocalQueuesFromBytes(prcRow.StateExecutionLocalQueues)
		if err != nil {
			return nil, err
		}

		localQueues.AddNewLocalQueueCommands(request.StateExecutionId, request.CommandRequest.GetLocalQueueCommands())

		consumedMessagesMap = localQueues.TryConsumeForStateExecution(
			request.StateExecutionId, request.CommandRequest.GetWaitingType())
	}

	// Step 2: update the state execution row
	stateRow, err := tx.SelectAsyncStateExecutionForUpdate(ctx, extensions.AsyncStateExecutionSelectFilter{
		ProcessExecutionId: request.ProcessExecutionId,
		StateId:            request.StateId,
		StateIdSequence:    request.StateIdSequence,
	})
	if err != nil {
		return nil, err
	}

	stateRow.Status = data_models.StateExecutionStatusWaitUntilWaiting
	stateRow.LastFailure = nil

	stateRow.WaitUntilCommands, err = data_models.FromCommandRequestToBytes(request.CommandRequest)
	if err != nil {
		return nil, err
	}

	commandResults, err := data_models.BytesToCommandResultsJson(stateRow.WaitUntilCommandResults)
	if err != nil {
		return nil, err
	}

	// Step 2 - 1: update local queue command results
	var allConsumedMessages []data_models.InternalLocalQueueMessage
	for _, consumedMessages := range consumedMessagesMap {
		allConsumedMessages = append(allConsumedMessages, consumedMessages...)
	}

	dedupIdToLocalQueueMessageMap, err := p.getDedupIdToLocalQueueMessageMap(ctx, request.ProcessExecutionId, allConsumedMessages)
	if err != nil {
		return nil, err
	}

	err = p.updateCommandResultsWithNewlyConsumedLocalQueueMessages(&commandResults, consumedMessagesMap, dedupIdToLocalQueueMessageMap)
	if err != nil {
		return nil, err
	}

	hasNewImmediateTask := false

	if hasLocalQueueCommands && p.hasCompletedWaitUntilWaiting(request.CommandRequest, commandResults) {
		hasNewImmediateTask = true

		err = p.updateWhenCompletedWaitUntilWaiting(ctx, tx, request.TaskShardId, &localQueues, stateRow)
		if err != nil {
			return nil, err
		}
	}

	stateRow.WaitUntilCommandResults, err = data_models.FromCommandResultsJsonToBytes(commandResults)
	if err != nil {
		return nil, err
	}

	err = tx.UpdateAsyncStateExecution(ctx, *stateRow)
	if err != nil {
		return nil, err
	}

	// Step 2 - 2: create timer command tasks
	var fireTimestamps []int64

	for idx, timerCommand := range request.CommandRequest.TimerCommands {
		if timerCommand.DelayInSeconds < 0 {
			timerCommand.DelayInSeconds = 0
		}

		timerTaskInfoJson := data_models.TimerTaskInfoJson{
			TimerCommandIndex: idx,
		}
		timerInfoBytes, err := timerTaskInfoJson.ToBytes()
		if err != nil {
			return nil, err
		}

		fireTimestamp := time.Now().Add(time.Second * time.Duration(timerCommand.DelayInSeconds)).Unix()
		err = tx.InsertTimerTask(ctx, extensions.TimerTaskRowForInsert{
			ShardId:             request.TaskShardId,
			FireTimeUnixSeconds: fireTimestamp,
			TaskType:            data_models.TimerTaskTypeTimerCommand,
			ProcessExecutionId:  request.ProcessExecutionId,
			StateId:             request.StateId,
			StateIdSequence:     request.StateIdSequence,
			Info:                timerInfoBytes,
		})
		if err != nil {
			return nil, err
		}

		fireTimestamps = append(fireTimestamps, fireTimestamp)
	}

	// Step 3: update process execution row, and submit
	if hasLocalQueueCommands {
		prcRow.StateExecutionLocalQueues, err = localQueues.ToBytes()
		if err != nil {
			return nil, err
		}

		err = tx.UpdateProcessExecution(ctx, *prcRow)
		if err != nil {
			return nil, err
		}
	}

	return &data_models.ProcessWaitUntilExecutionResponse{
		HasNewImmediateTask: hasNewImmediateTask,
		FireTimestamps:      fireTimestamps,
	}, nil
}
