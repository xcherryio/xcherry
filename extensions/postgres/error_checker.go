package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/lib/pq"
)

// ErrDupEntry indicates a duplicate primary key i.e. the row already exists,
// check http://www.postgresql.org/docs/9.3/static/errcodes-appendix.html
const ErrDupEntry = "23505"

const ErrInsufficientResources = "53000"
const ErrTooManyConnections = "53300"

var conflictError = fmt.Errorf("conflict on updating")

func (d dbSession) IsDupEntryError(err error) bool {
	var sqlErr pq.Error
	ok := errors.As(err, &sqlErr)
	return ok && sqlErr.Code == ErrDupEntry
}

func (d dbSession) IsNotFoundError(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}

func (d dbSession) IsTimeoutError(err error) bool {
	return errors.Is(err, context.DeadlineExceeded)
}

func (d dbSession) IsThrottlingError(err error) bool {
	var sqlErr pq.Error
	ok := errors.As(err, &sqlErr)
	if ok {
		if sqlErr.Code == ErrTooManyConnections ||
			sqlErr.Code == ErrInsufficientResources {
			return true
		}
	}
	return false
}

func (d dbSession) IsConflictError(err error) bool {
	return errors.Is(err, conflictError)
}
