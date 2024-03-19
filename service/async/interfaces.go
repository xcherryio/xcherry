// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

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
	NotifyRemoteImmediateTaskAsyncInCluster(req xcapi.NotifyImmediateTasksRequest, serverAddress string)
	NotifyRemoteTimerTaskAsyncInCluster(req xcapi.NotifyTimerTasksRequest, serverAddress string)
	Stop(ctx context.Context) error
	ReBalance(assignedShardIds []int32)
}

type Membership interface {
	GetServerAddress() string
	GetServerAddressFor(shardId int32) string
}
