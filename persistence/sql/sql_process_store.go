// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: BUSL-1.1

package sql

import (
	"database/sql"
	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/extensions"
	"github.com/xdblab/xdb/persistence"
)

type sqlProcessStoreImpl struct {
	session extensions.SQLDBSession
	logger  log.Logger
}

var defaultTxOpts *sql.TxOptions = &sql.TxOptions{
	Isolation: sql.LevelReadCommitted,
}

func NewSQLProcessStore(sqlConfig config.SQL, logger log.Logger) (persistence.ProcessStore, error) {
	session, err := extensions.NewSQLSession(&sqlConfig)
	return &sqlProcessStoreImpl{
		session: session,
		logger:  logger,
	}, err
}

func (p sqlProcessStoreImpl) Close() error {
	return p.session.Close()
}
