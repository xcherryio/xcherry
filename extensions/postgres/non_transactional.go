package postgres

import (
	"context"
	"fmt"
	"github.com/xdblab/xdb/common/ptr"
	"github.com/xdblab/xdb/extensions"
)

func (d dbSession) SelectCurrentProcessExecution(ctx context.Context, namespace, processId string) (*extensions.ProcessExecutionRow, error) {
	var row []extensions.ProcessExecutionRow
	err := d.db.SelectContext(ctx, &row, selectCurrentExecutionQuery, namespace, processId)
	if len(row) > 1 {
		return nil, fmt.Errorf("SelectCurrentProcessExecution shouldn't return more than one rows")
	}
	return ptr.Any(row[0]), err
}
