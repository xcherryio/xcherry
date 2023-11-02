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
	ctx context.Context, tx extensions.SQLTransaction, tableConfig persistence.InternalGlobalAttributeConfig,
	updates []xdbapi.GlobalAttributeTableRowUpdate,
) error {
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
