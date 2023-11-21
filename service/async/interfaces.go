// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: BUSL-1.1

package async

import (
	"context"
	"github.com/xcherryio/apis/goapi/xcapi"
)

type Server interface {
	// Start will start running on the background
	Start() error
	Stop(ctx context.Context) error
}

type Service interface {
	Start() error
	NotifyPollingImmediateTask(req xcapi.NotifyImmediateTasksRequest) error
	NotifyPollingTimerTask(req xcapi.NotifyTimerTasksRequest) error
	Stop(ctx context.Context) error
}
