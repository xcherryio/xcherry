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
	"github.com/xdblab/xdb/common/ptr"
	"github.com/xdblab/xdb/persistence"
)

func (p sqlProcessStoreImpl) DescribeLatestProcess(
	ctx context.Context, request persistence.DescribeLatestProcessRequest,
) (*persistence.DescribeLatestProcessResponse, error) {
	row, err := p.session.SelectLatestProcessExecution(ctx, request.Namespace, request.ProcessId)
	if err != nil {
		if p.session.IsNotFoundError(err) {
			return &persistence.DescribeLatestProcessResponse{
				NotExists: true,
			}, nil
		}
		return nil, err
	}

	info, err := persistence.BytesToProcessExecutionInfo(row.Info)
	if err != nil {
		return nil, err
	}

	return &persistence.DescribeLatestProcessResponse{
		Response: &xdbapi.ProcessExecutionDescribeResponse{
			ProcessExecutionId: ptr.Any(row.ProcessExecutionId.String()),
			ProcessType:        &info.ProcessType,
			WorkerUrl:          &info.WorkerURL,
			StartTimestamp:     ptr.Any(int32(row.StartTime.Unix())),
			Status:             xdbapi.ProcessStatus(row.Status.String()).Ptr(),
		},
	}, nil
}
