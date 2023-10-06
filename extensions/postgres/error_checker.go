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
