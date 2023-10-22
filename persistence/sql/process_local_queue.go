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

func (p sqlProcessStoreImpl) ProcessLocalQueueMessage(
	ctx context.Context, request persistence.ProcessLocalQueueMessageRequest,
) error {
	tx, err := p.session.StartTransaction(ctx, defaultTxOpts)
	if err != nil {
		return err
	}

	err = p.doProcessLocalQueueMessageTx(ctx, tx, request)
	if err != nil {
		err2 := tx.Rollback()
		if err2 != nil {
			p.logger.Error("error on rollback transaction", tag.Error(err2))
		}
	} else {
		err = tx.Commit()
		if err != nil {
			p.logger.Error("error on committing transaction", tag.Error(err))
			return err
		}
	}
	return err
}

func (p sqlProcessStoreImpl) doProcessLocalQueueMessageTx(
	ctx context.Context, tx extensions.SQLTransaction, request persistence.ProcessLocalQueueMessageRequest,
) error {
	// Step 1: update process execution row
	prcRow, err := tx.SelectProcessExecutionForUpdate(ctx, request.ProcessExecutionId)
	if err != nil {
		return err
	}

	waitingQueues, err := persistence.NewStateExecutionWaitingQueuesFromBytes(prcRow.StateExecutionWaitingQueues)
	if err != nil {
		return err
	}

	dedupIdString := request.DedupId.String()
	assignedStateExecutionId, hasFinishedWaiting := waitingQueues.Consume(xdbapi.LocalQueueMessage{
		QueueName: request.QueueName,
		DedupId:   &dedupIdString,
		Payload:   &request.Payload,
	})

	// early stop if no state can consume the message
	if assignedStateExecutionId == nil {
		return nil
	}

	prcRow.StateExecutionWaitingQueues, err = waitingQueues.ToBytes()
	if err != nil {
		return err
	}

	err = tx.UpdateProcessExecution(ctx, *prcRow)
	if err != nil {
		return err
	}

	// Step 2: update state execution row
	stateRow, err := tx.SelectAsyncStateExecutionForUpdate(ctx, extensions.AsyncStateExecutionSelectFilter{
		ProcessExecutionId: prcRow.ProcessExecutionId,
		StateId:            assignedStateExecutionId.StateId,
		StateIdSequence:    assignedStateExecutionId.StateIdSequence,
	})
	if err != nil {
		return err
	}

	commandResults, err := persistence.BytesToCommandResults(stateRow.WaitUntilCommandResults)
	if err != nil {
		return err
	}

	commandResults.LocalQueueResults = append(commandResults.LocalQueueResults, xdbapi.LocalQueueMessage{
		QueueName: request.QueueName,
		DedupId:   &dedupIdString,
		Payload:   &request.Payload,
	})

	stateRow.WaitUntilCommandResults, err = persistence.FromCommandResultsToBytes(commandResults)
	if err != nil {
		return err
	}

	err = tx.UpdateAsyncStateExecution(ctx, *stateRow)
	if err != nil {
		return err
	}

	// will handle the completion logic in CompleteWaitUntilExecution later.

	// Step 3: delete the task row
	err = tx.DeleteImmediateTask(ctx, extensions.ImmediateTaskRowDeleteFilter{
		ShardId:      request.TaskShardId,
		TaskSequence: request.TaskSequence,
	})
	if err != nil {
		return err
	}

	// Step 4: handle finished wait_until task
	if !hasFinishedWaiting {
		return nil
	}

	return p.CompleteWaitUntilExecution(ctx, tx, persistence.CompleteWaitUntilExecutionRequest{
		TaskShardId:        request.TaskShardId,
		ProcessExecutionId: request.ProcessExecutionId,
		StateExecutionId:   *assignedStateExecutionId,
		PreviousVersion:    stateRow.PreviousVersion + 1,
	})
}
