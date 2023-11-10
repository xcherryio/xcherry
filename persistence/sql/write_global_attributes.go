// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: BUSL-1.1

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

		var pkNames, pkValues []string
		for _, col := range tblCfg.PrimaryKey {
			pkNames = append(pkNames, col.DbColumn)
			pkValues = append(pkValues, col.DbQueryValue)
		}

		row := extensions.CustomTableRowForInsert{
			TableName:     tblCfg.TableName,
			PKNames:       pkNames,
			PKValues:      pkValues,
			ColumnToValue: cols,
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
			pk := tableConfig.TablePrimaryKeys[update.TableName]

			var pkNames, pkValues []string
			for _, col := range pk {
				pkNames = append(pkNames, col.DbColumn)
				pkValues = append(pkValues, col.DbQueryValue)
			}

			cols := map[string]string{}
			for _, col := range update.UpdateColumns {
				cols[col.DbColumn] = col.DbQueryValue
			}
			err := tx.UpsertCustomTableByPK(ctx, update.TableName, pkNames, pkValues, cols)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
