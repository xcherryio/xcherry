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

package sql

import (
	"context"
	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/extensions"
	"github.com/xdblab/xdb/persistence"
)

func (p sqlProcessStoreImpl) ProcessLocalQueueMessages(
	ctx context.Context, request persistence.ProcessLocalQueueMessagesRequest,
) (*persistence.ProcessLocalQueueMessagesResponse, error) {
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
	ctx context.Context, tx extensions.SQLTransaction, request persistence.ProcessLocalQueueMessagesRequest,
) (*persistence.ProcessLocalQueueMessagesResponse, error) {
	assignedStateExecutionIdToMessagesMap := map[string][]persistence.InternalLocalQueueMessage{}

	// Step 1: get localQueues from the process execution row, and update it with messages
	prcRow, err := tx.SelectProcessExecutionForUpdate(ctx, request.ProcessExecutionId)
	if err != nil {
		return nil, err
	}

	localQueues, err := persistence.NewStateExecutionLocalQueuesFromBytes(prcRow.StateExecutionLocalQueues)
	if err != nil {
		return nil, err
	}

	for _, message := range request.Messages {
		assignedStateExecutionIdString, consumedMessages := localQueues.AddMessageAndTryConsume(message)

		if assignedStateExecutionIdString == "" {
			continue
		}

		assignedStateExecutionIdToMessagesMap[assignedStateExecutionIdString] =
			append(assignedStateExecutionIdToMessagesMap[assignedStateExecutionIdString], consumedMessages...)
	}

	// Step 2: update assigned state execution rows
	hasNewImmediateTask := false

	if len(assignedStateExecutionIdToMessagesMap) > 0 {
		var allConsumedMessages []persistence.InternalLocalQueueMessage
		for _, consumedMessages := range assignedStateExecutionIdToMessagesMap {
			allConsumedMessages = append(allConsumedMessages, consumedMessages...)
		}

		dedupIdToLocalQueueMessageMap, err := p.getDedupIdToLocalQueueMessageMap(ctx, prcRow.ProcessExecutionId, allConsumedMessages)
		if err != nil {
			return nil, err
		}

		for assignedStateExecutionIdString, consumedMessages := range assignedStateExecutionIdToMessagesMap {
			stateExecutionId, err := persistence.NewStateExecutionIdFromString(assignedStateExecutionIdString)
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

			commandRequest, err := persistence.BytesToCommandRequest(stateRow.WaitUntilCommands)
			if err != nil {
				return nil, err
			}

			commandResults, err := persistence.BytesToCommandResults(stateRow.WaitUntilCommandResults)
			if err != nil {
				return nil, err
			}

			err = p.updateCommandResultsWithConsumedLocalQueueMessages(&commandResults, consumedMessages, dedupIdToLocalQueueMessageMap)
			if err != nil {
				return nil, err
			}

			if p.hasCompletedWaitUntilWaiting(commandRequest, commandResults) {
				hasNewImmediateTask = true

				localQueues.CleanupFor(persistence.StateExecutionId{
					StateId:         stateRow.StateId,
					StateIdSequence: stateRow.StateIdSequence,
				})

				stateRow.Status = persistence.StateExecutionStatusExecuteRunning

				err = tx.InsertImmediateTask(ctx, extensions.ImmediateTaskRowForInsert{
					ShardId:            request.TaskShardId,
					TaskType:           persistence.ImmediateTaskTypeExecute,
					ProcessExecutionId: stateRow.ProcessExecutionId,
					StateId:            stateRow.StateId,
					StateIdSequence:    stateRow.StateIdSequence,
				})
				if err != nil {
					return nil, err
				}
			}

			stateRow.WaitUntilCommandResults, err = persistence.FromCommandResultsToBytes(commandResults)
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

	return &persistence.ProcessLocalQueueMessagesResponse{
		HasNewImmediateTask: hasNewImmediateTask,
	}, nil
}
