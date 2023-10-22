// Copyright 2023 XDBLab organization

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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

func (p sqlProcessStoreImpl) PublishToLocalQueue(ctx context.Context, request persistence.PublishToLocalQueueRequest) (
	*persistence.PublishToLocalQueueResponse, error) {
	tx, err := p.session.StartTransaction(ctx, defaultTxOpts)
	if err != nil {
		return nil, err
	}

	resp, err := p.doPublishToLocalQueueTx(ctx, tx, request)

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

func (p sqlProcessStoreImpl) doPublishToLocalQueueTx(
	ctx context.Context, tx extensions.SQLTransaction, request persistence.PublishToLocalQueueRequest,
) (*persistence.PublishToLocalQueueResponse, error) {
	curProcExecRow, err := p.session.SelectLatestProcessExecution(ctx, request.Namespace, request.ProcessId)
	if err != nil {
		if p.session.IsNotFoundError(err) {
			// early stop when there is no such process running
			return &persistence.PublishToLocalQueueResponse{
				NotExists: true,
			}, nil
		}
		return nil, err
	}

	err = p.publishToLocalQueue(ctx, tx, curProcExecRow.ProcessExecutionId, request.Messages)
	if err != nil {
		return nil, err
	}

	return &persistence.PublishToLocalQueueResponse{
		ProcessExecutionId:  curProcExecRow.ProcessExecutionId,
		HasNewImmediateTask: len(request.Messages) > 0,
		NotExists:           false,
	}, nil
}
