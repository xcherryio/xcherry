package sql

import (
	"context"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/extensions"
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
