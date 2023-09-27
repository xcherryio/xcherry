package extensions

import (
	"context"
	"github.com/xdblab/xdb/config"
)

type SQLDBExtension interface {
	StartAdminDBSession(cfg *config.SQL) (SQLAdminDBSession, error)
}

type SQLAdminDBSession interface {
	CreateDatabase(ctx context.Context, database string) error
	DropDatabase(ctx context.Context, database string) error
	ExecuteSchemaDDL(ctx context.Context, ddlQuery string) error
	Close() error
}
