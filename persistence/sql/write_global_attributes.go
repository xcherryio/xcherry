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
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/extensions"
	"github.com/xdblab/xdb/persistence"
)

func (p sqlProcessStoreImpl) handleInitialGlobalAttributesWrite(
	ctx context.Context, tx extensions.SQLTransaction, req xdbapi.ProcessExecutionStartRequest,
) error {
	if req.ProcessStartConfig == nil || req.ProcessStartConfig.GlobalAttributeConfig == nil {
		return nil
	}

	for _, tblCfg := range req.ProcessStartConfig.GlobalAttributeConfig.TableConfigs {
		if len(tblCfg.InitialWrite) == 0 {
			continue
		}
		writeMode := xdbapi.RETURN_ERROR_ON_CONFLICT
		if tblCfg.InitialWriteMode != nil {
			writeMode = *tblCfg.InitialWriteMode
		}

		cols := map[string]string{}
		for _, field := range tblCfg.InitialWrite {
			cols[field.DbColumn] = field.DbQueryValue
		}
		row := extensions.CustomTableRowForInsert{
			TableName:       tblCfg.TableName,
			PrimaryKey:      tblCfg.PrimaryKey.DbColumn,
			PrimaryKeyValue: tblCfg.PrimaryKey.DbQueryValue,
			ColumnToValue:   cols,
		}
		var err error
		switch writeMode {
		case xdbapi.RETURN_ERROR_ON_CONFLICT:
			err = tx.InsertCustomTableErrorOnConflict(ctx, row)
		case xdbapi.IGNORE_CONFLICT:
			err = tx.InsertCustomTableIgnoreOnConflict(ctx, row)
		case xdbapi.OVERRIDE_ON_CONFLICT:
			err = tx.InsertCustomTableOverrideOnConflict(ctx, row)
		default:
			panic("unknown write mode " + string(writeMode))
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (p sqlProcessStoreImpl) updateGlobalAttributesIfNeeded(
	ctx context.Context, tx extensions.SQLTransaction, request persistence.CompleteExecuteExecutionRequest,
) error {
	tableConfig := request.GlobalAttributeTableConfig
	updates := request.UpdateGlobalAttributes

	if len(updates) > 0 {
		for _, update := range updates {
			pks := tableConfig.TablePrimaryKeys[update.TableName]
			cols := map[string]string{}
			for _, col := range update.UpdateColumns {
				cols[col.DbColumn] = col.DbQueryValue
			}
			err := tx.UpsertCustomTableByPK(ctx, update.TableName, pks.DbColumn, pks.DbQueryValue, cols)
			if err != nil {
				return err
			}
		}
	}
	return nil
}