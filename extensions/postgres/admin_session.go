// Apache License 2.0

// Copyright (c) XDBLab organization

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.    

package postgres

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/xdblab/xdb/extensions"
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
