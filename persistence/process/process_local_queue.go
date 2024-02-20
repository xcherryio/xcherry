// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package process

import (
	"context"
	"github.com/xcherryio/xcherry/common/log/tag"
	"github.com/xcherryio/xcherry/extensions"
	"github.com/xcherryio/xcherry/persistence/data_models"
)

func (p sqlProcessStoreImpl) ProcessLocalQueueMessages(
	ctx context.Context, request data_models.ProcessLocalQueueMessagesRequest,
) (*data_models.ProcessLocalQueueMessagesResponse, error) {
	tx, err := p.session.StartTransaction(ctx, defaultTxOpts)
	if err != nil {
		return nil, err
	}

	resp, err := p.doProcessLocalQueueMessagesTx(ctx, tx, request)
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

func (p sqlProcessStoreImpl) doProcessLocalQueueMessagesTx(
	ctx context.Context, tx extensions.SQLTransaction, request data_models.ProcessLocalQueueMessagesRequest,
) (*data_models.ProcessLocalQueueMessagesResponse, error) {
	assignedStateExecutionIdToMessagesMap := map[string]map[int][]data_models.InternalLocalQueueMessage{}

	// Step 1: get localQueues from the process execution row, and update it with messages
	prcRow, err := tx.SelectProcessExecutionForUpdate(ctx, request.ProcessExecutionId)
	if err != nil {
		return nil, err
	}

	localQueues, err := data_models.NewStateExecutionLocalQueuesFromBytes(prcRow.StateExecutionLocalQueues)
	if err != nil {
		return nil, err
	}

	for _, message := range request.Messages {
		assignedStateExecutionIdString, idx, consumedMessages := localQueues.AddMessageAndTryConsume(message)

		if assignedStateExecutionIdString == "" {
			continue
		}

		_, ok := assignedStateExecutionIdToMessagesMap[assignedStateExecutionIdString]
		if !ok {
			assignedStateExecutionIdToMessagesMap[assignedStateExecutionIdString] = map[int][]data_models.InternalLocalQueueMessage{}
		}
		assignedStateExecutionIdToMessagesMap[assignedStateExecutionIdString][idx] = consumedMessages
	}

	// Step 2: update assigned state execution rows
	hasNewImmediateTask := false

	if len(assignedStateExecutionIdToMessagesMap) > 0 {
		var allConsumedMessages []data_models.InternalLocalQueueMessage
		for _, consumedMessagesMap := range assignedStateExecutionIdToMessagesMap {
			for _, consumedMessages := range consumedMessagesMap {
				allConsumedMessages = append(allConsumedMessages, consumedMessages...)
			}
		}

		dedupIdToLocalQueueMessageMap, err := p.getDedupIdToLocalQueueMessageMap(ctx, prcRow.ProcessExecutionId, allConsumedMessages)
		if err != nil {
			return nil, err
		}

		for assignedStateExecutionIdString, consumedMessagesMap := range assignedStateExecutionIdToMessagesMap {
			stateExecutionId, err := data_models.NewStateExecutionIdFromString(assignedStateExecutionIdString)
			if err != nil {
				return nil, err
			}

			stateRow, err := tx.SelectAsyncStateExecutionForUpdate(ctx, extensions.AsyncStateExecutionSelectFilter{
				ProcessExecutionId: prcRow.ProcessExecutionId,
				StateId:            stateExecutionId.StateId,
				StateIdSequence:    stateExecutionId.StateIdSequence,
			})
			if err != nil {
				return nil, err
			}

			stateRow.LastFailure = nil

			commandRequest, err := data_models.BytesToCommandRequest(stateRow.WaitUntilCommands)
			if err != nil {
				return nil, err
			}

			commandResults, err := data_models.BytesToCommandResultsJson(stateRow.WaitUntilCommandResults)
			if err != nil {
				return nil, err
			}

			err = p.updateCommandResultsWithNewlyConsumedLocalQueueMessages(&commandResults, consumedMessagesMap, dedupIdToLocalQueueMessageMap)
			if err != nil {
				return nil, err
			}

			if p.hasCompletedWaitUntilWaiting(commandRequest, commandResults) {
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
		}
	}

	// Step 3: update process execution row, and submit
	prcRow.StateExecutionLocalQueues, err = localQueues.ToBytes()
	if err != nil {
		return nil, err
	}

	err = tx.UpdateProcessExecution(ctx, *prcRow)
	if err != nil {
		return nil, err
	}

	// Step 4: delete the task row
	err = tx.DeleteImmediateTask(ctx, extensions.ImmediateTaskRowDeleteFilter{
		ShardId:      request.TaskShardId,
		TaskSequence: request.TaskSequence,
	})
	if err != nil {
		return nil, err
	}

	return &data_models.ProcessLocalQueueMessagesResponse{
		HasNewImmediateTask: hasNewImmediateTask,
	}, nil
}
