package postgres

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
)

// NOTE we have to use %v because somehow postgres doesn't work with ? here
// It's a small bug in sqlx library
const createDatabaseQuery = "CREATE database %v"

type adminDBSession struct {
	db *sqlx.DB
}

func (a adminDBSession) CreateDatabase(ctx context.Context, database string) error {
	_, err := a.db.ExecContext(ctx, fmt.Sprintf(createDatabaseQuery, database))
	return err
}

func (a adminDBSession) Close() error {
	return a.db.Close()
}

func newAdminDBSession(db *sqlx.DB) *adminDBSession {
	return &adminDBSession{
		db: db,
	}
}