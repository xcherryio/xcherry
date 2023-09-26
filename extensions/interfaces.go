package extensions

import (
	"context"
	"github.com/xdblab/xdb/config"
)

type DBExtension interface {
	StartAdminDBSession(cfg *config.SQL) (AdminDBSession, error)
}

type AdminDBSession interface {
	CreateDatabase(ctx context.Context, database string) error
	Close() error
}
