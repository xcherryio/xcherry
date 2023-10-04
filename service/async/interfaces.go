package async

import "context"

type Server interface {
	// Start will start running on the background
	Start() error
	Stop(ctx context.Context) error
}

type Service interface {
	Start() error
	NotifyPollingWorkerTask(shardId int32)
	Stop(ctx context.Context) error
}
