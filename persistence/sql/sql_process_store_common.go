// Copyright (c) XDBLab
// SPDX-License-Identifier: BUSL-1.1

package sql

import (
	"context"
	"fmt"

	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/common/uuid"
	"github.com/xdblab/xdb/extensions"
	"github.com/xdblab/xdb/persistence"
)

func insertAsyncStateExecution(
	ctx context.Context,
	tx extensions.SQLTransaction,
	processExecutionId uuid.UUID,
	stateId string,
	stateIdSeq int,
	stateConfig *xdbapi.AsyncStateConfig,
	stateInput []byte,
	stateInfo []byte,
) error {
	commandResultsBytes, err := persistence.FromCommandResultsJsonToBytes(persistence.NewCommandResultsJson())
	if err != nil {
		return err
	}

	stateRow := extensions.AsyncStateExecutionRow{
		ProcessExecutionId: processExecutionId,
		StateId:            stateId,
		StateIdSequence:    int32(stateIdSeq),
		// the waitUntil/execute status will be set later

		WaitUntilCommands:       nil,
		WaitUntilCommandResults: commandResultsBytes,

		LastFailure:     nil,
		PreviousVersion: 1,
		Input:           stateInput,
		Info:            stateInfo,
	}

	if stateConfig.GetSkipWaitUntil() {
		stateRow.Status = persistence.StateExecutionStatusExecuteRunning
	} else {
		stateRow.Status = persistence.StateExecutionStatusWaitUntilRunning
	}

	return tx.InsertAsyncStateExecution(ctx, stateRow)
}

func insertImmediateTask(
	ctx context.Context,
	tx extensions.SQLTransaction,
	processExecutionId uuid.UUID,
	stateId string,
	stateIdSeq int,
	stateConfig *xdbapi.AsyncStateConfig,
	shardId int32,
) error {
	immediateTaskRow := extensions.ImmediateTaskRowForInsert{
		ShardId:            shardId,
		ProcessExecutionId: processExecutionId,
		StateId:            stateId,
		StateIdSequence:    int32(stateIdSeq),
	}
	if stateConfig.GetSkipWaitUntil() {
		immediateTaskRow.TaskType = persistence.ImmediateTaskTypeExecute
	} else {
		immediateTaskRow.TaskType = persistence.ImmediateTaskTypeWaitUntil
	}

	return tx.InsertImmediateTask(ctx, immediateTaskRow)
}

// publishToLocalQueue inserts len(valid_messages) rows into xdb_sys_local_queue_messages,
// and inserts only one row into xdb_sys_immediate_tasks with all the dedupIds for these messages.
// publishToLocalQueue returns (HasNewImmediateTask, error).
func (p sqlProcessStoreImpl) publishToLocalQueue(
	ctx context.Context, tx extensions.SQLTransaction, processExecutionId uuid.UUID, messages []xdbapi.LocalQueueMessage,
) (bool, error) {

	var localQueueMessageInfo []persistence.LocalQueueMessageInfoJson

	for _, message := range messages {
		dedupId := uuid.MustNewUUID()

		// dealing with user-customized dedupId
		if message.GetDedupId() != "" {
			dedupId2, err := uuid.ParseUUID(message.GetDedupId())
			if err != nil {
				return false, err
			}
			dedupId = dedupId2
		}

		// insert a row into xdb_sys_local_queue_messages

		payloadBytes, err := persistence.FromEncodedObjectIntoBytes(message.Payload)
		if err != nil {
			return false, err
		}

		insertSuccessfully, err := tx.InsertLocalQueueMessage(ctx, extensions.LocalQueueMessageRow{
			ProcessExecutionId: processExecutionId,
			QueueName:          message.GetQueueName(),
			DedupId:            dedupId,
			Payload:            payloadBytes,
		})
		if err != nil {
			return false, err
		}
		if !insertSuccessfully {
			continue
		}

		localQueueMessageInfo = append(localQueueMessageInfo, persistence.LocalQueueMessageInfoJson{
			QueueName: message.GetQueueName(),
			DedupId:   dedupId,
		})
	}

	// insert a row into xdb_sys_immediate_tasks

	if len(localQueueMessageInfo) == 0 {
		return false, nil
	}

	taskInfoBytes, err := persistence.FromImmediateTaskInfoIntoBytes(
		persistence.ImmediateTaskInfoJson{
			LocalQueueMessageInfo: localQueueMessageInfo,
		})
	if err != nil {
		return false, err
	}

	err = tx.InsertImmediateTask(ctx, extensions.ImmediateTaskRowForInsert{
		ShardId:  persistence.DefaultShardId,
		TaskType: persistence.ImmediateTaskTypeNewLocalQueueMessages,

		ProcessExecutionId: processExecutionId,
		StateId:            "",
		StateIdSequence:    0,
		Info:               taskInfoBytes,
	})
	if err != nil {
		return false, err
	}

	return true, nil
}

func (p sqlProcessStoreImpl) getDedupIdToLocalQueueMessageMap(
	ctx context.Context, processExecutionId uuid.UUID,
	consumedMessages []persistence.InternalLocalQueueMessage,
) (map[string]extensions.LocalQueueMessageRow, error) {
	if len(consumedMessages) == 0 {
		return map[string]extensions.LocalQueueMessageRow{}, nil
	}

	var allConsumedDedupIdStrings []string
	for _, consumedMessage := range consumedMessages {
		allConsumedDedupIdStrings = append(allConsumedDedupIdStrings, consumedMessage.DedupId)
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

func (p sqlProcessStoreImpl) updateCommandResultsWithNewlyConsumedLocalQueueMessages(
	commandResults *persistence.CommandResultsJson,
	newlyConsumedMessagesMap map[int][]persistence.InternalLocalQueueMessage,
	dedupIdToLocalQueueMessageMap map[string]extensions.LocalQueueMessageRow,
) error {

	for idx, consumedMessages := range newlyConsumedMessagesMap {
		var localQueueMessageResults []xdbapi.LocalQueueMessageResult
		for _, consumedMessage := range consumedMessages {
			message, ok := dedupIdToLocalQueueMessageMap[consumedMessage.DedupId]
			if !ok {
				panic(fmt.Sprintf("Something wrong with the dedupIdToLocalQueueMessageMap, key: %v", consumedMessage.DedupId))
			}

			dedupIdString := message.DedupId.String()
			payload, err := persistence.BytesToEncodedObject(message.Payload)
			if err != nil {
				return err
			}

			localQueueMessageResults = append(localQueueMessageResults, xdbapi.LocalQueueMessageResult{
				DedupId: dedupIdString,
				Payload: &payload,
			})
		}

		commandResults.LocalQueueResults[idx] = localQueueMessageResults
	}

	return nil
}

func (p sqlProcessStoreImpl) updateCommandResultsWithFiredTimerCommand(
	commandResults *persistence.CommandResultsJson, timerCommandIndex int,
) {
	commandResults.TimerResults[timerCommandIndex] = true
}

func (p sqlProcessStoreImpl) hasCompletedWaitUntilWaiting(
	commandRequest xdbapi.CommandRequest, commandResults persistence.CommandResultsJson,
) bool {
	switch commandRequest.GetWaitingType() {
	case xdbapi.ANY_OF_COMPLETION:
		return len(commandResults.LocalQueueResults)+len(commandResults.TimerResults) > 0
	case xdbapi.ALL_OF_COMPLETION:
		return len(commandResults.LocalQueueResults)+len(commandResults.TimerResults) ==
			len(commandRequest.LocalQueueCommands)+len(commandRequest.TimerCommands)
	case xdbapi.EMPTY_COMMAND:
		return true
	default:
		panic("this is not supported")
	}
}

func (p sqlProcessStoreImpl) updateWhenCompletedWaitUntilWaiting(
	ctx context.Context, tx extensions.SQLTransaction, shardId int32,
	localQueues *persistence.StateExecutionLocalQueuesJson, stateRow *extensions.AsyncStateExecutionRowForUpdate,
) error {
	localQueues.CleanupFor(persistence.StateExecutionId{
		StateId:         stateRow.StateId,
		StateIdSequence: stateRow.StateIdSequence,
	})

	stateRow.Status = persistence.StateExecutionStatusExecuteRunning

	return tx.InsertImmediateTask(ctx, extensions.ImmediateTaskRowForInsert{
		ShardId:            shardId,
		TaskType:           persistence.ImmediateTaskTypeExecute,
		ProcessExecutionId: stateRow.ProcessExecutionId,
		StateId:            stateRow.StateId,
		StateIdSequence:    stateRow.StateIdSequence,
	})
}
