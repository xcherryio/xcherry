// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: BUSL-1.1

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
