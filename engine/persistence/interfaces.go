package persistence

import (
	"context"
)

// Persistence is for operating on the database for process execution
type (
	Persistence interface {
		StartProcess(ctx context.Context, request StartProcessRequest) (*StartProcessResponse, error)
		DescribeLatestProcess(ctx context.Context, request DescribeLatestProcessRequest) (*DescribeLatestProcessResponse, error)
		Close() error
	}
)
