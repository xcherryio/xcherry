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
	"github.com/xdblab/xdb/common/uuid"
	"github.com/xdblab/xdb/extensions"
	"github.com/xdblab/xdb/persistence"
)

func (p sqlProcessStoreImpl) ProcessLocalQueueCommandsAndMessages(
	ctx context.Context, request persistence.ProcessLocalQueueCommandsAndMessagesRequest,
) (*persistence.ProcessLocalQueueCommandsAndMessagesResponse, error) {
	tx, err := p.session.StartTransaction(ctx, defaultTxOpts)
	if err != nil {
		return nil, err
	}

	resp, err := p.doProcessLocalQueueCommandsAndMessagesTx(ctx, tx, request)
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

func (p sqlProcessStoreImpl) doProcessLocalQueueCommandsAndMessagesTx(
	ctx context.Context, tx extensions.SQLTransaction, request persistence.ProcessLocalQueueCommandsAndMessagesRequest,
) (*persistence.ProcessLocalQueueCommandsAndMessagesResponse, error) {
	assignedStateExecutionIdToMessagesMap := map[string][]persistence.InternalLocalQueueMessage{}

	// Step 1: get waitingQueues from the process execution row and update it with commands or messages
	prcRow, err := tx.SelectProcessExecutionForUpdate(ctx, request.ProcessExecutionId)
	if err != nil {
		return nil, err
	}

	waitingQueues, err := persistence.NewStateExecutionWaitingQueuesFromBytes(prcRow.StateExecutionWaitingQueues)
	if err != nil {
		return nil, err
	}

	canNewStateCompleteLocalQueueWaiting := false
	if request.ProcessLocalQueueCommandsAndTryConsumeRequest != nil {
		request2 := request.ProcessLocalQueueCommandsAndTryConsumeRequest

		for _, localQueueCommand := range request2.CommandRequest.GetLocalQueueCommands() {
			waitingQueues.AddNewLocalQueueCommand(request2.StateExecutionId, localQueueCommand)
		}

		canComplete, consumedMessages := waitingQueues.ConsumeWithCheckingLocalQueueWaitingComplete(
			request2.StateExecutionId, request2.CommandRequest.GetWaitingType())

		// put the StateExecutionId into the map no matter there is consumedMessages or not, because we need
		// to update the state status anyway.
		assignedStateExecutionIdToMessagesMap[request2.StateExecutionId.GetStateExecutionId()] = consumedMessages

		canNewStateCompleteLocalQueueWaiting = canComplete
	} else {
		request2 := request.ProcessLocalQueueMessagesRequest
		for _, message := range request2.Messages {
			assignedStateExecutionIdString, consumedMessages := waitingQueues.AddMessageAndTryConsume(message)

			if assignedStateExecutionIdString == "" {
				continue
			}

			assignedStateExecutionIdToMessagesMap[assignedStateExecutionIdString] =
				append(assignedStateExecutionIdToMessagesMap[assignedStateExecutionIdString], consumedMessages...)
		}
	}

	// Step 2: update assigned state execution rows
	hasNewImmediateTask := false

	if len(assignedStateExecutionIdToMessagesMap) > 0 {
		dedupIdToLocalQueueMessageMap, err := p.getDedupIdToLocalQueueMessageMap(ctx, prcRow.ProcessExecutionId, assignedStateExecutionIdToMessagesMap)
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

			var commandRequest xdbapi.CommandRequest
			var commandResults xdbapi.CommandResults

			if request.ProcessLocalQueueCommandsAndTryConsumeRequest != nil {
				stateRow.Status = persistence.StateExecutionStatusWaitUntilWaiting
				commandRequest = request.ProcessLocalQueueCommandsAndTryConsumeRequest.CommandRequest
				commandResults = xdbapi.CommandResults{}
			} else {
				commandRequest, err = persistence.BytesToCommandRequest(stateRow.WaitUntilCommands)
				if err != nil {
					return nil, err
				}

				commandResults, err = persistence.BytesToCommandResults(stateRow.WaitUntilCommandResults)
				if err != nil {
					return nil, err
				}
			}

			for _, consumedMessage := range consumedMessages {
				message, ok := dedupIdToLocalQueueMessageMap[consumedMessage.DedupId]
				if !ok {
					continue
				}

				dedupIdString := message.DedupId.String()
				payload, err := persistence.BytesToEncodedObject(message.Payload)
				if err != nil {
					return nil, err
				}

				commandResults.LocalQueueResults = append(commandResults.LocalQueueResults, xdbapi.LocalQueueMessage{
					QueueName: message.QueueName,
					DedupId:   &dedupIdString,
					Payload:   &payload,
				})
			}

			if (request.ProcessLocalQueueCommandsAndTryConsumeRequest != nil && canNewStateCompleteLocalQueueWaiting) ||
				(request.ProcessLocalQueueMessagesRequest != nil && p.hasCompletedWaitUntilWaiting(commandRequest, commandResults)) {
				hasNewImmediateTask = true

				waitingQueues.CleanupFor(*stateExecutionId)

				stateRow.Status = persistence.StateExecutionStatusExecuteRunning

				err = tx.InsertImmediateTask(ctx, extensions.ImmediateTaskRowForInsert{
					ShardId:            request.TaskShardId,
					TaskType:           persistence.ImmediateTaskTypeExecute,
					ProcessExecutionId: request.ProcessExecutionId,
					StateId:            stateExecutionId.StateId,
					StateIdSequence:    stateExecutionId.StateIdSequence,
				})
				if err != nil {
					return nil, err
				}
			}

			stateRow.WaitUntilCommands, err = persistence.FromCommandRequestToBytes(commandRequest)
			if err != nil {
				return nil, err
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
	prcRow.StateExecutionWaitingQueues, err = waitingQueues.ToBytes()
	if err != nil {
		return nil, err
	}

	err = tx.UpdateProcessExecution(ctx, *prcRow)
	if err != nil {
		return nil, err
	}

	// Step 4: delete the task row
	if request.ProcessLocalQueueMessagesRequest != nil {
		err = tx.DeleteImmediateTask(ctx, extensions.ImmediateTaskRowDeleteFilter{
			ShardId:      request.TaskShardId,
			TaskSequence: request.ProcessLocalQueueMessagesRequest.TaskSequence,
		})
		if err != nil {
			return nil, err
		}
	}

	return &persistence.ProcessLocalQueueCommandsAndMessagesResponse{
		HasNewImmediateTask: hasNewImmediateTask,
	}, nil
}

func (p sqlProcessStoreImpl) getDedupIdToLocalQueueMessageMap(ctx context.Context, processExecutionId uuid.UUID,
	assignedStateExecutionIdToMessagesMap map[string][]persistence.InternalLocalQueueMessage,
) (map[string]extensions.LocalQueueMessageRow, error) {

	var allConsumedDedupIdStrings []string
	for _, consumedMessages := range assignedStateExecutionIdToMessagesMap {
		for _, consumedMessage := range consumedMessages {
			allConsumedDedupIdStrings = append(allConsumedDedupIdStrings, consumedMessage.DedupId)
		}
	}

	if len(allConsumedDedupIdStrings) == 0 {
		return nil, nil
	}

	allConsumedLocalQueueMessages, err := p.session.SelectLocalQueueMessages(ctx, processExecutionId, allConsumedDedupIdStrings)
	if err != nil {
		return nil, err
	}

	dedupIdToLocalQueueMessageMap := map[string]extensions.LocalQueueMessageRow{}
	for _, message := range allConsumedLocalQueueMessages {
		dedupIdToLocalQueueMessageMap[message.DedupId.String()] = message
	}

	return dedupIdToLocalQueueMessageMap, nil
}

func (p sqlProcessStoreImpl) hasCompletedWaitUntilWaiting(commandRequest xdbapi.CommandRequest, commandResults xdbapi.CommandResults) bool {
	// TODO: currently, only consider the local queue results

	localQueueToMessageCountMap := map[string]int{}
	for _, localQueueMessage := range commandResults.LocalQueueResults {
		localQueueToMessageCountMap[localQueueMessage.QueueName] += 1
	}

	switch commandRequest.GetWaitingType() {
	case xdbapi.ANY_OF_COMPLETION:
		for _, localQueueCommand := range commandRequest.LocalQueueCommands {
			if localQueueToMessageCountMap[localQueueCommand.QueueName] == int(localQueueCommand.GetCount()) {
				return true
			}
		}
		return false
	case xdbapi.ALL_OF_COMPLETION:
		for _, localQueueCommand := range commandRequest.LocalQueueCommands {
			if localQueueToMessageCountMap[localQueueCommand.QueueName] < int(localQueueCommand.GetCount()) {
				return false
			}
		}
		return true
	case xdbapi.EMPTY_COMMAND:
		return true
	default:
		return true
	}
}
