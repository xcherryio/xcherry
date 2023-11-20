// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: BUSL-1.1

package postgres

import (
	"context"
	"database/sql"
	"github.com/jmoiron/sqlx"
	"github.com/xcherryio/xcherry/extensions"
)

type dbSession struct {
	db *sqlx.DB
}

type dbTx struct {
	tx *sqlx.Tx
}

var _ extensions.SQLDBSession = (*dbSession)(nil)
var _ extensions.SQLTransaction = (*dbTx)(nil)

func newDBSession(db *sqlx.DB) *dbSession {
	return &dbSession{
		db: db,
	}
}

func (d dbSession) StartTransaction(ctx context.Context, opts *sql.TxOptions) (extensions.SQLTransaction, error) {
	tx, err := d.db.BeginTxx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return dbTx{
		tx: tx,
	}, nil
}

func (d dbSession) Close() error {
	return d.db.Close()
}

func (d dbTx) Commit() error {
	return d.tx.Commit()
}

func (d dbTx) Rollback() error {
	return d.tx.Rollback()
}
