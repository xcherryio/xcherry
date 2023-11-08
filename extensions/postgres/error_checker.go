// Copyright (c) 2019 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
