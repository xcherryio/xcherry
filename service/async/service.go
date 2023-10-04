package async

import "context"

type Service interface {
	Start() error
	NotifyPollingWorkerTask(shardId int32)
	Stop(ctx context.Context) error
}
