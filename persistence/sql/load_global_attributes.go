// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: BUSL-1.1

package sql

import (
	"context"
	"fmt"
	"github.com/xcherryio/apis/goapi/xcapi"
	"github.com/xcherryio/xcherry/persistence/data_models"
)

func (p sqlProcessStoreImpl) LoadGlobalAttributes(
	ctx context.Context, request data_models.LoadGlobalAttributesRequest,
) (*data_models.LoadGlobalAttributesResponse, error) {
	var tableResponses []xcapi.TableReadResponse
	config := request.TableConfig
	for _, tableReq := range request.Request.TableRequests {
		if tableReq.GetLockingPolicy() != xcapi.NO_LOCKING {
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

		var colsOut []xcapi.TableColumnValue
		for fname, fvalue := range row.ColumnToValue {
			colsOut = append(colsOut, xcapi.TableColumnValue{
				DbColumn:     fname,
				DbQueryValue: fvalue,
			})
		}
		tableResponses = append(tableResponses, xcapi.TableReadResponse{
			TableName: tableReq.TableName,
			Columns:   colsOut,
		})
	}

	return &data_models.LoadGlobalAttributesResponse{
		Response: xcapi.LoadGlobalAttributeResponse{
			TableResponses: tableResponses,
		},
	}, nil
}
