// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: BUSL-1.1

package sql

import (
	"context"
	"fmt"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/persistence"
)

func (p sqlProcessStoreImpl) LoadGlobalAttributes(
	ctx context.Context, request persistence.LoadGlobalAttributesRequest,
) (*persistence.LoadGlobalAttributesResponse, error) {
	var tableResponses []xdbapi.TableReadResponse
	config := request.TableConfig
	for _, tableReq := range request.Request.TableRequests {
		if tableReq.GetLockingPolicy() != xdbapi.NO_LOCKING {
			// TODO support other locking policies
			return nil, fmt.Errorf("locking policy %v is not supported", tableReq.GetLockingPolicy())
		}
		pk, ok := config.TablePrimaryKeys[*tableReq.TableName]
		if !ok {
			return nil, fmt.Errorf("table %s is not configured properly with primary key", *tableReq.TableName)
		}
		var cols []string
		for _, field := range tableReq.Columns {
			cols = append(cols, field.DbColumn)
		}
		row, err := p.session.SelectCustomTableByPK(ctx, *tableReq.TableName, pk.DbColumn, pk.DbQueryValue, cols)
		if err != nil {
			return nil, err
		}

		var colsOut []xdbapi.TableColumnValue
		for fname, fvalue := range row.ColumnToValue {
			colsOut = append(colsOut, xdbapi.TableColumnValue{
				DbColumn:     fname,
				DbQueryValue: fvalue,
			})
		}
		tableResponses = append(tableResponses, xdbapi.TableReadResponse{
			TableName: tableReq.TableName,
			Columns:   colsOut,
		})
	}

	return &persistence.LoadGlobalAttributesResponse{
		Response: xdbapi.LoadGlobalAttributeResponse{
			TableResponses: tableResponses,
		},
	}, nil
}
