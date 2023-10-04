package persistence

import (
	"context"
)

// ProcessStore is for operating on the database for process execution
type (
	ProcessStore interface {
		StartProcess(ctx context.Context, request StartProcessRequest) (*StartProcessResponse, error)
		DescribeLatestProcess(ctx context.Context, request DescribeLatestProcessRequest) (*DescribeLatestProcessResponse, error)
		Close() error
	}
)
