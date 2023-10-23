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
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
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

	resp, err := p.doProcessLocalQueueMessageTx(ctx, tx, request)
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

func (p sqlProcessStoreImpl) doProcessLocalQueueMessageTx(
	ctx context.Context, tx extensions.SQLTransaction, request persistence.ProcessLocalQueueMessagesRequest,
) (*persistence.ProcessLocalQueueMessagesResponse, error) {
	assignedStateExecutionIdToMessagesMap := map[string][]persistence.LocalQueueMessageInfoJson{}
	finishedStateExecutionIdToPreviousVersionMap := map[string]int32{}

	// Step 1: update process execution row
	prcRow, err := tx.SelectProcessExecutionForUpdate(ctx, request.ProcessExecutionId)
	if err != nil {
		return nil, err
	}

	waitingQueues, err := persistence.NewStateExecutionWaitingQueuesFromBytes(prcRow.StateExecutionWaitingQueues)
	if err != nil {
		return nil, err
	}

	// merge waitingQueues.UnconsumedMessages into request.Messages to consume
	messages := append(waitingQueues.UnconsumedMessages, request.Messages...)
	waitingQueues.ClearUnconsumedMessages()

	for _, message := range messages {
		assignedStateExecutionIdString, hasFinishedWaiting := waitingQueues.Consume(message)

		if assignedStateExecutionIdString == nil {
			continue
		}

		consumedMessages, ok := assignedStateExecutionIdToMessagesMap[*assignedStateExecutionIdString]
		if !ok {
			consumedMessages = []persistence.LocalQueueMessageInfoJson{}
		}
		consumedMessages = append(consumedMessages, message)
		assignedStateExecutionIdToMessagesMap[*assignedStateExecutionIdString] = consumedMessages

		if hasFinishedWaiting {
			finishedStateExecutionIdToPreviousVersionMap[*assignedStateExecutionIdString] = 0
		}
	}

	prcRow.StateExecutionWaitingQueues, err = waitingQueues.ToBytes()
	if err != nil {
		return nil, err
	}

	err = tx.UpdateProcessExecution(ctx, *prcRow)
	if err != nil {
		return nil, err
	}

	// Step 2: update state execution rows for assignedStateExecutionIdToMessagesMap
	for assignedStateExecutionIdString, messages := range assignedStateExecutionIdToMessagesMap {
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

		commandResults, err := persistence.BytesToCommandResults(stateRow.WaitUntilCommandResults)
		if err != nil {
			return nil, err
		}

		for _, message := range messages {
			dedupIdString := message.DedupId.String()
			payload := message.Payload
			commandResults.LocalQueueResults = append(commandResults.LocalQueueResults, xdbapi.LocalQueueMessage{
				QueueName: message.QueueName,
				DedupId:   &dedupIdString,
				Payload:   &payload,
			})
		}

		stateRow.WaitUntilCommandResults, err = persistence.FromCommandResultsToBytes(commandResults)
		if err != nil {
			return nil, err
		}

		err = tx.UpdateAsyncStateExecution(ctx, *stateRow)
		if err != nil {
			return nil, err
		}

		_, ok := finishedStateExecutionIdToPreviousVersionMap[assignedStateExecutionIdString]
		if ok {
			finishedStateExecutionIdToPreviousVersionMap[assignedStateExecutionIdString] = stateRow.PreviousVersion
		}
	}

	// will handle the completion logic in CompleteWaitUntilExecution later.

	// Step 3: handle finished wait_until task for finishedStateExecutionIdToPreviousVersionMap
	for stateExecutionIdString, previousVersion := range finishedStateExecutionIdToPreviousVersionMap {
		stateExecutionId, err := persistence.NewStateExecutionIdFromString(stateExecutionIdString)
		if err != nil {
			return nil, err
		}

		err = p.CompleteWaitUntilExecution(ctx, tx, persistence.CompleteWaitUntilExecutionRequest{
			TaskShardId:        request.TaskShardId,
			ProcessExecutionId: request.ProcessExecutionId,
			StateExecutionId:   *stateExecutionId,
			PreviousVersion:    previousVersion + 1,
		})
		if err != nil {
			return nil, err
		}
	}

	// Step 4: delete the task row
	if request.TaskSequence > 0 {
		err = tx.DeleteImmediateTask(ctx, extensions.ImmediateTaskRowDeleteFilter{
			ShardId:      request.TaskShardId,
			TaskSequence: request.TaskSequence,
		})
		if err != nil {
			return nil, err
		}
	}

	return &persistence.ProcessLocalQueueMessagesResponse{
		HasNewImmediateTask: len(finishedStateExecutionIdToPreviousVersionMap) > 0,
	}, nil
}
