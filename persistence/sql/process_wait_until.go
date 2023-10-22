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
		err := p.completeWaitUntilExecution(ctx, tx, request)
		if err != nil {
			return nil, err
		}
	} else {
		err := p.updateWaitUntilExecution(ctx, tx, request)
		if err != nil {
			return nil, err
		}
	}

	err := p.publishToLocalQueue(ctx, tx, request.ProcessExecutionId, request.PublishToLocalQueue)
	if err != nil {
		return nil, err
	}

	if len(request.PublishToLocalQueue) > 0 {
		hasNewImmediateTask = true
	}

	return &persistence.ProcessWaitUntilExecutionResponse{
		HasNewImmediateTask: hasNewImmediateTask,
	}, nil
}

func (p sqlProcessStoreImpl) completeWaitUntilExecution(
	ctx context.Context, tx extensions.SQLTransaction, request persistence.ProcessWaitUntilExecutionRequest,
) error {
	stateRow := extensions.AsyncStateExecutionRowForUpdateWithoutCommands{
		ProcessExecutionId: request.ProcessExecutionId,
		StateId:            request.StateId,
		StateIdSequence:    request.StateIdSequence,
		WaitUntilStatus:    persistence.StateExecutionStatusCompleted,
		ExecuteStatus:      persistence.StateExecutionStatusRunning,
		PreviousVersion:    request.Prepare.PreviousVersion,
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
) error {
	commandRequestBytes, err := persistence.FromCommandRequestToBytes(request.CommandRequest)
	if err != nil {
		return err
	}

	commandResultsBytes, err := persistence.FromCommandResultsToBytes(xdbapi.CommandResults{})
	if err != nil {
		return err
	}

	stateRow := extensions.AsyncStateExecutionRowForUpdateCommands{
		ProcessExecutionId:      request.ProcessExecutionId,
		StateId:                 request.StateId,
		StateIdSequence:         request.StateIdSequence,
		WaitUntilStatus:         persistence.StateExecutionStatusWaitingForCommands,
		WaitUntilCommands:       commandRequestBytes,
		WaitUntilCommandResults: commandResultsBytes,
		PreviousVersion:         request.Prepare.PreviousVersion,
	}

	localQueueCommands := request.CommandRequest.GetLocalQueueCommands()

	// lock and handle process execution row first
	if len(localQueueCommands) > 0 {
		prcRow, err := tx.SelectProcessExecutionForUpdate(ctx, request.ProcessExecutionId)
		if err != nil {
			return err
		}

		waitingQueues, err := persistence.NewStateExecutionWaitingQueuesFromBytes(prcRow.StateExecutionWaitingQueues)
		if err != nil {
			return err
		}

		for _, localQueueCommand := range localQueueCommands {
			waitingQueues.Add(request.StateExecutionId, localQueueCommand, request.CommandRequest.GetWaitingType() == xdbapi.ANY_OF_COMPLETION)
		}

		prcRow.StateExecutionWaitingQueues, err = waitingQueues.ToBytes()
		if err != nil {
			return err
		}

		err = tx.UpdateProcessExecution(ctx, *prcRow)
		if err != nil {
			return err
		}
	}

	// update async state execution
	err = tx.UpdateAsyncStateExecutionCommands(ctx, stateRow)
	if err != nil {
		if p.session.IsConditionalUpdateFailure(err) {
			p.logger.Warn("UpdateAsyncStateExecutionCommands failed at conditional update")
		}
		return err
	}

	return nil
}
