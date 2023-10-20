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

func (p sqlProcessStoreImpl) CompleteWaitUntilExecution(
	ctx context.Context, request persistence.CompleteWaitUntilExecutionRequest,
) (*persistence.CompleteWaitUntilExecutionResponse, error) {
	if request.CommandRequest.GetWaitingType() != xdbapi.EMPTY_COMMAND {
		// TODO set command request from resp
		return nil, fmt.Errorf("not supported command type %v", request.CommandRequest.GetWaitingType())
	}

	tx, err := p.session.StartTransaction(ctx, defaultTxOpts)
	if err != nil {
		return nil, err
	}

	resp, err := p.doCompleteWaitUntilExecutionTx(ctx, tx, request)
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

func (p sqlProcessStoreImpl) doCompleteWaitUntilExecutionTx(
	ctx context.Context, tx extensions.SQLTransaction, request persistence.CompleteWaitUntilExecutionRequest,
) (*persistence.CompleteWaitUntilExecutionResponse, error) {
	stateRow := extensions.AsyncStateExecutionRowForUpdate{
		ProcessExecutionId: request.ProcessExecutionId,
		StateId:            request.StateId,
		StateIdSequence:    request.StateIdSequence,
		WaitUntilStatus:    persistence.StateExecutionStatusCompleted,
		ExecuteStatus:      persistence.StateExecutionStatusRunning,
		PreviousVersion:    request.Prepare.PreviousVersion,
		LastFailure:        nil,
	}

	err := tx.UpdateAsyncStateExecution(ctx, stateRow)
	if err != nil {
		if p.session.IsConditionalUpdateFailure(err) {
			p.logger.Warn("UpdateAsyncStateExecution failed at conditional update")
		}
		return nil, err
	}
	err = tx.InsertWorkerTask(ctx, extensions.WorkerTaskRowForInsert{
		ShardId:            request.TaskShardId,
		TaskType:           persistence.WorkerTaskTypeExecute,
		ProcessExecutionId: request.ProcessExecutionId,
		StateId:            request.StateId,
		StateIdSequence:    request.StateIdSequence,
	})
	if err != nil {
		return nil, err
	}
	return &persistence.CompleteWaitUntilExecutionResponse{
		HasNewWorkerTask: true,
	}, nil
}
