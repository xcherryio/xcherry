// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: BUSL-1.1

package sql

import (
	"context"
	"github.com/xcherryio/xcherry/extensions"
	"github.com/xcherryio/xcherry/persistence/data_models"
)

func (p sqlProcessStoreImpl) handleInitialGlobalAttributesWrite(
	ctx context.Context, tx extensions.SQLTransaction, req xcapi.ProcessExecutionStartRequest,
) error {
	if req.ProcessStartConfig == nil || req.ProcessStartConfig.GlobalAttributeConfig == nil {
		return nil
	}

	for _, tblCfg := range req.ProcessStartConfig.GlobalAttributeConfig.TableConfigs {
		if len(tblCfg.InitialWrite) == 0 {
			continue
		}
		writeMode := xcapi.RETURN_ERROR_ON_CONFLICT
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
		case xcapi.RETURN_ERROR_ON_CONFLICT:
			err = tx.InsertCustomTableErrorOnConflict(ctx, row)
		case xcapi.IGNORE_CONFLICT:
			err = tx.InsertCustomTableIgnoreOnConflict(ctx, row)
		case xcapi.OVERRIDE_ON_CONFLICT:
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
	ctx context.Context, tx extensions.SQLTransaction, tableConfig *data_models.InternalGlobalAttributeConfig,
	globalAttributesToUpdate []xcapi.GlobalAttributeTableRowUpdate,
) error {
	if len(globalAttributesToUpdate) > 0 {
		for _, update := range globalAttributesToUpdate {
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
