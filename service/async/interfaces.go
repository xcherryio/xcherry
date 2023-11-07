// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: BUSL-1.1

package async

import (
	"context"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
)

type Server interface {
	// Start will start running on the background
	Start() error
	Stop(ctx context.Context) error
}

type Service interface {
	Start() error
	NotifyPollingImmediateTask(req xdbapi.NotifyImmediateTasksRequest) error
	NotifyPollingTimerTask(req xdbapi.NotifyTimerTasksRequest) error
	Stop(ctx context.Context) error
}
