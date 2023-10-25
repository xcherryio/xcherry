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
	assignedStateExecutionIdToMessageDedupIdsMap := map[string][]uuid.UUID{}
	finishedStateExecutionIdToPreviousVersionMap := map[string]int32{}

	// Step 1: update process execution row, but cannot submit at this step
	prcRow, err := tx.SelectProcessExecutionForUpdate(ctx, request.ProcessExecutionId)
	if err != nil {
		return nil, err
	}

	waitingQueues, err := persistence.NewStateExecutionWaitingQueuesFromBytes(prcRow.StateExecutionWaitingQueues)
	if err != nil {
		return nil, err
	}

	if request.TaskSequence == 0 {
		_, consumedDedupIds := waitingQueues.ConsumeFor(request.StateExecutionId, request.CommandWaitingType == xdbapi.ALL_OF_COMPLETION)
		assignedStateExecutionIdToMessageDedupIdsMap[request.StateExecutionId.GetStateExecutionId()] = consumedDedupIds
	}

	for _, message := range request.Messages {
		assignedStateExecutionIdString, dedupIds := waitingQueues.Consume(message)

		if assignedStateExecutionIdString == nil {
			continue
		}

		assignedStateExecutionIdToMessageDedupIdsMap[*assignedStateExecutionIdString] =
			append(assignedStateExecutionIdToMessageDedupIdsMap[*assignedStateExecutionIdString], dedupIds...)
	}

	// Step 2: update state execution rows for assignedStateExecutionIdToMessageDedupIdsMap
	dedupIdToLocalQueueMessageMap, err := p.getDedupIdToLocalQueueMessageMap(ctx, prcRow.ProcessExecutionId, assignedStateExecutionIdToMessageDedupIdsMap)
	if err != nil {
		return nil, err
	}

	for assignedStateExecutionIdString, dedupIds := range assignedStateExecutionIdToMessageDedupIdsMap {
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

		commandRequest, err := persistence.BytesToCommandRequest(stateRow.WaitUntilCommands)
		if err != nil {
			return nil, err
		}

		for _, dedupId := range dedupIds {
			message, ok := dedupIdToLocalQueueMessageMap[dedupId.String()]
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

		stateRow.WaitUntilCommandResults, err = persistence.FromCommandResultsToBytes(commandResults)
		if err != nil {
			return nil, err
		}

		err = tx.UpdateAsyncStateExecution(ctx, *stateRow)
		if err != nil {
			return nil, err
		}

		if p.hasFinishedWaitUntilWaiting(commandRequest, commandResults) {
			waitingQueues.CleanupFor(*stateExecutionId)
			finishedStateExecutionIdToPreviousVersionMap[assignedStateExecutionIdString] = stateRow.PreviousVersion
		}
	}

	// Step 2.5: update process execution row, and submit
	prcRow.StateExecutionWaitingQueues, err = waitingQueues.ToBytes()
	if err != nil {
		return nil, err
	}

	err = tx.UpdateProcessExecution(ctx, *prcRow)
	if err != nil {
		return nil, err
	}

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
		ProcessExecutionId:  prcRow.ProcessExecutionId,
	}, nil
}

func (p sqlProcessStoreImpl) getDedupIdToLocalQueueMessageMap(ctx context.Context, processExecutionId uuid.UUID,
	assignedStateExecutionIdToMessageDedupIdsMap map[string][]uuid.UUID) (map[string]extensions.LocalQueueRow, error) {

	var allConsumedDedupIds []uuid.UUID
	for _, dedupIds := range assignedStateExecutionIdToMessageDedupIdsMap {
		allConsumedDedupIds = append(allConsumedDedupIds, dedupIds...)
	}

	allConsumedLocalQueueMessages, err := p.session.SelectLocalQueueMessages(ctx, processExecutionId, allConsumedDedupIds)
	if err != nil {
		return map[string]extensions.LocalQueueRow{}, err
	}

	dedupIdToLocalQueueMessageMap := map[string]extensions.LocalQueueRow{}
	for _, message := range allConsumedLocalQueueMessages {
		dedupIdToLocalQueueMessageMap[message.DedupId.String()] = message
	}

	return dedupIdToLocalQueueMessageMap, nil
}

func (p sqlProcessStoreImpl) hasFinishedWaitUntilWaiting(commandRequest xdbapi.CommandRequest, commandResults xdbapi.CommandResults) bool {
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
	default:
		return true
	}
}
