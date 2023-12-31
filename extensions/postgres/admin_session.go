// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0
package postgres

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/xcherryio/xcherry/extensions"
)

// NOTE we have to use %v because somehow postgres doesn't work with ? here
// It's a small bug in sqlx library
const createDatabaseQuery = "CREATE database %v"

const dropDatabaseQuery = "Drop database %v"

type adminDBSession struct {
	db *sqlx.DB
}

var _ extensions.SQLAdminDBSession = (*adminDBSession)(nil)

func newAdminDBSession(db *sqlx.DB) *adminDBSession {
	return &adminDBSession{
		db: db,
	}
}

func (a adminDBSession) DropDatabase(ctx context.Context, database string) error {
	_, err := a.db.ExecContext(ctx, fmt.Sprintf(dropDatabaseQuery, database))
	return err
}

func (a adminDBSession) ExecuteSchemaDDL(ctx context.Context, ddlQuery string) error {
	_, err := a.db.ExecContext(ctx, ddlQuery)
	return err
}

func (a adminDBSession) CreateDatabase(ctx context.Context, database string) error {
	_, err := a.db.ExecContext(ctx, fmt.Sprintf(createDatabaseQuery, database))
	return err
}

func (a adminDBSession) Close() error {
	return a.db.Close()
}
