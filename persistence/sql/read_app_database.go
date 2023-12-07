// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: BUSL-1.1

package sql

import (
	"context"
	"fmt"
	"github.com/xcherryio/apis/goapi/xcapi"
	"github.com/xcherryio/xcherry/persistence/data_models"
)

func (p sqlProcessStoreImpl) ReadAppDatabase(
	ctx context.Context, request data_models.AppDatabaseReadRequest,
) (*data_models.AppDatabaseReadResponse, error) {
	var tableResponses []xcapi.AppDatabaseTableReadResponse
	config := request.AppDatabaseConfig

	for _, tableReq := range request.Request.Tables {
		if tableReq.GetLockType() != xcapi.NO_LOCKING {
			// TODO support other locking types
			return nil, fmt.Errorf("locking type %v is not supported", tableReq.GetLockType())
		}

		pk, ok := config.TablePrimaryKeys[tableReq.GetTableName()]
		if !ok {
			return nil, fmt.Errorf("table %s is not configured properly with primary key", tableReq.GetTableName())
		}

		rows, err := p.session.SelectAppDatabaseTableByPK(ctx, tableReq.GetTableName(), pk, tableReq.GetColumns())
		if err != nil {
			return nil, err
		}

		var rowReadResponse []xcapi.AppDatabaseRowReadResponse

		for _, row := range rows {
			var colsOut []xcapi.AppDatabaseColumnValue

			for fname, fvalue := range row.ColumnToValue {
				colsOut = append(colsOut, xcapi.AppDatabaseColumnValue{
					Column:     fname,
					QueryValue: fvalue,
				})
			}

			rowReadResponse = append(rowReadResponse, xcapi.AppDatabaseRowReadResponse{
				Columns: colsOut,
			})
		}

		tableResponses = append(tableResponses, xcapi.AppDatabaseTableReadResponse{
			TableName: tableReq.TableName,
			Rows:      rowReadResponse,
		})
	}

	return &data_models.AppDatabaseReadResponse{
		Response: xcapi.AppDatabaseReadResponse{
			Tables: tableResponses,
		},
	}, nil
}
