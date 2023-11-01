// Copyright 2023 XDBLab organization
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
