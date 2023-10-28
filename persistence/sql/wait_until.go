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

func (p sqlProcessStoreImpl) ProcessWaitUntilExecution(
	ctx context.Context, request persistence.ProcessWaitUntilExecutionRequest,
) (*persistence.ProcessWaitUntilExecutionResponse, error) {
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
	ctx context.Context, tx extensions.SQLTransaction, request persistence.ProcessWaitUntilExecutionRequest,
) (*persistence.ProcessWaitUntilExecutionResponse, error) {
	hasNewImmediateTask := false

	if request.CommandRequest.GetWaitingType() == xdbapi.EMPTY_COMMAND {
		hasNewImmediateTask = true
		err := p.completeWaitUntilExecution(ctx, tx, persistence.CompleteWaitUntilExecutionRequest{
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
	}

	hasNewImmediateTask2, err := p.publishToLocalQueue(ctx, tx, request.ProcessExecutionId, request.PublishToLocalQueue)
	if err != nil {
		return nil, err
	}
	if hasNewImmediateTask2 {
		hasNewImmediateTask = true
	}

	return &persistence.ProcessWaitUntilExecutionResponse{
		HasNewImmediateTask: hasNewImmediateTask,
	}, nil
}

func (p sqlProcessStoreImpl) completeWaitUntilExecution(
	ctx context.Context, tx extensions.SQLTransaction, request persistence.CompleteWaitUntilExecutionRequest,
) error {
	stateRow := extensions.AsyncStateExecutionRowForUpdateWithoutCommands{
		ProcessExecutionId: request.ProcessExecutionId,
		StateId:            request.StateId,
		StateIdSequence:    request.StateIdSequence,
		Status:             persistence.StateExecutionStatusExecuteRunning,
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
		TaskType:           persistence.ImmediateTaskTypeExecute,
		ProcessExecutionId: request.ProcessExecutionId,
		StateId:            request.StateId,
		StateIdSequence:    request.StateIdSequence,
	})
}

func (p sqlProcessStoreImpl) updateWaitUntilExecution(
	ctx context.Context, tx extensions.SQLTransaction, request persistence.ProcessWaitUntilExecutionRequest,
) (*persistence.ProcessWaitUntilExecutionResponse, error) {
	// Step 1: get waitingQueues from the process execution row,
	// update it with commands, and try to consume for the state execution
	prcRow, err := tx.SelectProcessExecutionForUpdate(ctx, request.ProcessExecutionId)
	if err != nil {
		return nil, err
	}

	waitingQueues, err := persistence.NewStateExecutionWaitingQueuesFromBytes(prcRow.StateExecutionWaitingQueues)
	if err != nil {
		return nil, err
	}

	for _, localQueueCommand := range request.CommandRequest.GetLocalQueueCommands() {
		waitingQueues.AddNewLocalQueueCommand(request.StateExecutionId, localQueueCommand)
	}

	canComplete, consumedMessages := waitingQueues.ConsumeWithCheckingLocalQueueWaitingComplete(
		request.StateExecutionId, request.CommandRequest.GetWaitingType())

	// Step 2: update the state execution row
	stateRow, err := tx.SelectAsyncStateExecutionForUpdate(ctx, extensions.AsyncStateExecutionSelectFilter{
		ProcessExecutionId: prcRow.ProcessExecutionId,
		StateId:            request.StateId,
		StateIdSequence:    request.StateIdSequence,
	})
	if err != nil {
		return nil, err
	}

	stateRow.Status = persistence.StateExecutionStatusWaitUntilWaiting
	stateRow.LastFailure = nil

	stateRow.WaitUntilCommands, err = persistence.FromCommandRequestToBytes(request.CommandRequest)
	if err != nil {
		return nil, err
	}

	dedupIdToLocalQueueMessageMap, err := p.getDedupIdToLocalQueueMessageMap(ctx, prcRow.ProcessExecutionId, consumedMessages)
	if err != nil {
		return nil, err
	}

	commandResults := xdbapi.CommandResults{}

	err = p.updateCommandResultsWithConsumedLocalQueueMessages(&commandResults, consumedMessages, dedupIdToLocalQueueMessageMap)
	if err != nil {
		return nil, err
	}

	hasNewImmediateTask := false

	if canComplete {
		hasNewImmediateTask = true

		waitingQueues.CleanupFor(request.StateExecutionId)

		stateRow.Status = persistence.StateExecutionStatusExecuteRunning

		err = tx.InsertImmediateTask(ctx, extensions.ImmediateTaskRowForInsert{
			ShardId:            request.TaskShardId,
			TaskType:           persistence.ImmediateTaskTypeExecute,
			ProcessExecutionId: request.ProcessExecutionId,
			StateId:            request.StateId,
			StateIdSequence:    request.StateIdSequence,
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

	// Step 3: update process execution row, and submit
	prcRow.StateExecutionWaitingQueues, err = waitingQueues.ToBytes()
	if err != nil {
		return nil, err
	}

	err = tx.UpdateProcessExecution(ctx, *prcRow)
	if err != nil {
		return nil, err
	}

	return &persistence.ProcessWaitUntilExecutionResponse{
		HasNewImmediateTask: hasNewImmediateTask,
	}, nil
}
