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

	"github.com/xdblab/xdb/extensions"
	"github.com/xdblab/xdb/persistence"
)

func (p sqlProcessStoreImpl) PrepareStateExecution(
	ctx context.Context, request persistence.PrepareStateExecutionRequest,
) (*persistence.PrepareStateExecutionResponse, error) {
	stateRow, err := p.session.SelectAsyncStateExecution(
		ctx, extensions.AsyncStateExecutionSelectFilter{
			ProcessExecutionId: request.ProcessExecutionId,
			StateId:            request.StateId,
			StateIdSequence:    request.StateIdSequence,
		})
	if err != nil {
		return nil, err
	}
	info, err := persistence.BytesToAsyncStateExecutionInfo(stateRow.Info)
	if err != nil {
		return nil, err
	}
	input, err := persistence.BytesToEncodedObject(stateRow.Input)
	if err != nil {
		return nil, err
	}
	return &persistence.PrepareStateExecutionResponse{
		WaitUntilStatus: stateRow.WaitUntilStatus,
		ExecuteStatus:   stateRow.ExecuteStatus,
		PreviousVersion: stateRow.PreviousVersion,
		Info:            info,
		Input:           input,
	}, nil
}