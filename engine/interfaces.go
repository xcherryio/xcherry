package engine

import (
	"context"
	"time"
)

// TaskQueue is task queue
type TaskQueue interface {
	Start() error
	TriggerPolling(pollTime time.Time)
	Stop(ctx context.Context) error
}
