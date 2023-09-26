package extensions

import "github.com/xdblab/xdb/config"

type DBExtension interface {
	StartAdminDBSession(cfg *config.SQL) (AdminDBSession, error)
}

type AdminDBSession interface {
	CreateDatabase(database string) error
	Close() error
}
