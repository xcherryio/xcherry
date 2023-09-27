package postgres

import (
	"context"
	"database/sql"
	"github.com/lib/pq"
)

// ErrDupEntry indicates a duplicate primary key i.e. the row already exists,
// check http://www.postgresql.org/docs/9.3/static/errcodes-appendix.html
const ErrDupEntry = "23505"

const ErrInsufficientResources = "53000"
const ErrTooManyConnections = "53300"

func (d *dbSession) IsDupEntryError(err error) bool {
	sqlErr, ok := err.(*pq.Error)
	return ok && sqlErr.Code == ErrDupEntry
}

func (d *dbSession) IsNotFoundError(err error) bool {
	return err == sql.ErrNoRows
}

func (d *dbSession) IsTimeoutError(err error) bool {
	return err == context.DeadlineExceeded
}

func (d *dbSession) IsThrottlingError(err error) bool {
	sqlErr, ok := err.(*pq.Error)
	if ok {
		if sqlErr.Code == ErrTooManyConnections ||
			sqlErr.Code == ErrInsufficientResources {
			return true
		}
	}
	return false
}
