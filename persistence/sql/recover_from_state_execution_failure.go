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
	"fmt"

	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/extensions"
	"github.com/xdblab/xdb/persistence"
)

func (p sqlProcessStoreImpl) RecoverFromStateExecutionFailure(
	ctx context.Context,
	request persistence.RecoverFromStateExecutionFailureRequest) error {
	tx, err := p.session.StartTransaction(ctx, defaultTxOpts)
	if err != nil {
		return err
	}

	err = p.doRecoverFromStateExecutionFailureTx(ctx, tx, request)
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

func (p sqlProcessStoreImpl) doRecoverFromStateExecutionFailureTx(
	ctx context.Context,
	tx extensions.SQLTransaction,
	request persistence.RecoverFromStateExecutionFailureRequest,
) error {
	// lock process execution row first
	prcRow, err := tx.SelectProcessExecutionForUpdate(ctx, request.ProcessExecutionId)
	if err != nil {
		return err
	}

	// mark the current state as failed
	failureBytes, err := persistence.CreateStateExecutionFailureBytesForBackoff(
		request.LastFailureStatus, request.LastFailureDetails, request.LastFailureCompletedAttempts)

	if err != nil {
		return err
	}
	var currStateRow extensions.AsyncStateExecutionRowForUpdate
	currStateRow = extensions.AsyncStateExecutionRowForUpdate{
		ProcessExecutionId: request.ProcessExecutionId,
		StateId:            request.SourceStateExecutionId.StateId,
		StateIdSequence:    request.SourceStateExecutionId.StateIdSequence,
		PreviousVersion:    request.Prepare.PreviousVersion,
		LastFailure:        failureBytes,
	}

	if request.SourceFailedStateApi == xdbapi.WAIT_UNTIL_API {
		currStateRow.WaitUntilStatus = persistence.StateExecutionStatusFailed
		currStateRow.ExecuteStatus = persistence.StateExecutionStatusFailed
	} else if request.SourceFailedStateApi == xdbapi.EXECUTE_API {
		currStateRow.WaitUntilStatus = persistence.StateExecutionStatusCompleted
		currStateRow.ExecuteStatus = persistence.StateExecutionStatusFailed
	}

	err = tx.UpdateAsyncStateExecution(ctx, currStateRow)
	if err != nil {
		if p.session.IsConditionalUpdateFailure(err) {
			p.logger.Warn("UpdateAsyncStateExecution failed at conditional update")
		}
		return err
	}

	// update process info
	sequenceMaps, err := persistence.NewStateExecutionSequenceMapsFromBytes(prcRow.StateExecutionSequenceMaps)
	if err != nil {
		return err
	}

	// remove current state from PendingExecutionMap
	err = sequenceMaps.CompleteNewStateExecution(request.SourceStateExecutionId.StateId, int(request.SourceStateExecutionId.StateIdSequence))
	if err != nil {
		return fmt.Errorf("completing a non-existing state execution, maybe data is corrupted %v-%v, currentMap:%v, err:%w",
			request.SourceStateExecutionId.StateId, request.SourceStateExecutionId.StateIdSequence, sequenceMaps, err)
	}

	// abort other pending states
	if len(sequenceMaps.PendingExecutionMap) > 0 {
		err = tx.BatchUpdateAsyncStateExecutionsToAbortRunning(ctx, request.ProcessExecutionId)
		if err != nil {
			return err
		}
	}

	// start new state execution with state id from request
	stateInfo, err := persistence.FromAsyncStateExecutionInfoToBytes(request.Prepare.Info)
	if err != nil {
		return err
	}

	nextStateId := request.DestinationStateId
	nextStateIdSeq := sequenceMaps.StartNewStateExecution(request.DestinationStateId)
	stateConfig := request.DestinationStateConfig
	stateInput := request.DestinationStateInput
	stateInputBytes, err := persistence.FromEncodedObjectIntoBytes(&stateInput)
	if err != nil {
		return err
	}
	err = insertAsyncStateExecution(ctx, tx, request.ProcessExecutionId, nextStateId, nextStateIdSeq, stateConfig, stateInputBytes, stateInfo)
	if err != nil {
		return err
	}

	err = insertImmediateTask(ctx, tx, request.ProcessExecutionId, nextStateId, nextStateIdSeq, stateConfig, request.ShardId)
	if err != nil {
		return err
	}

	// update process execution row
	prcRow.StateExecutionSequenceMaps, err = sequenceMaps.ToBytes()
	if err != nil {
		return err
	}

	err = tx.UpdateProcessExecution(ctx, *prcRow)
	if err != nil {
		return err
	}

	return nil
}
