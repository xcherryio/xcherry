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
	"database/sql"
	"errors"
	"fmt"
	"github.com/lib/pq"
)

// ErrDupEntryCode indicates a duplicate primary key i.e. the row already exists,
// check http://www.postgresql.org/docs/9.3/static/errcodes-appendix.html
const ErrDupEntryCode = pq.ErrorCode("23505")

const ErrInsufficientResourcesCode = pq.ErrorCode("53000")
const ErrTooManyConnectionsCode = pq.ErrorCode("53300")

var conditionalUpdateFailure = fmt.Errorf("no affect on updating with conditional")

func (d dbSession) IsDupEntryError(err error) bool {
	sqlErr, ok := err.(*pq.Error)
	return ok && sqlErr.Code == ErrDupEntryCode
}

func (d dbSession) IsNotFoundError(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}

func (d dbSession) IsTimeoutError(err error) bool {
	return errors.Is(err, context.DeadlineExceeded)
}

func (d dbSession) IsThrottlingError(err error) bool {
	sqlErr, ok := err.(*pq.Error)
	if ok {
		if sqlErr.Code == ErrTooManyConnectionsCode ||
			sqlErr.Code == ErrInsufficientResourcesCode {
			return true
		}
	}
	return false
}

func (d dbSession) IsConditionalUpdateFailure(err error) bool {
	return errors.Is(err, conditionalUpdateFailure)
}
