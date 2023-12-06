// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: BUSL-1.1

package sql

import (
	"context"
	"fmt"
	"github.com/xcherryio/apis/goapi/xcapi"
	"github.com/xcherryio/xcherry/extensions"
	"github.com/xcherryio/xcherry/persistence/data_models"
)

func (p sqlProcessStoreImpl) writeToAppDatabase(
	ctx context.Context, tx extensions.SQLTransaction, req xcapi.ProcessExecutionStartRequest,
) error {
	if req.ProcessStartConfig == nil || req.ProcessStartConfig.AppDatabaseConfig == nil {
		return nil
	}

	for _, tableConfig := range req.ProcessStartConfig.AppDatabaseConfig.Tables {
		for _, row := range tableConfig.Rows {
			writeMode := xcapi.RETURN_ERROR_ON_CONFLICT
			if row.ConflictMode != nil {
				writeMode = *row.ConflictMode
			}

			primaryKeyColumns := map[string]string{}
			otherColumns := map[string]string{}

			for _, primaryKeyColumn := range row.GetPrimaryKey() {
				primaryKeyColumns[primaryKeyColumn.GetColumn()] = primaryKeyColumn.GetQueryValue()
			}

			for _, otherColumn := range row.GetInitialWrite() {
				otherColumns[otherColumn.GetColumn()] = otherColumn.GetQueryValue()
			}

			rows := extensions.CustomTableRow{
				TableName:               tableConfig.TableName,
				PrimaryKeyColumnToValue: primaryKeyColumns,
				OtherColumnToValue:      otherColumns,
			}

			err := tx.InsertCustomTable(ctx, rows, writeMode)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (p sqlProcessStoreImpl) writeToAppDatabaseIfNeeded(
	ctx context.Context, tx extensions.SQLTransaction, tableConfig *data_models.InternalAppDatabaseConfig,
	appDatabaseWrite *xcapi.AppDatabaseWrite,
) error {
	for _, tableWrite := range appDatabaseWrite.GetTables() {
		allPrimaryKeys, ok := tableConfig.TablePrimaryKeys[tableWrite.GetTableName()]
		if !ok {
			return fmt.Errorf("table %s is not configured properly with primary key", tableWrite.GetTableName())
		}

		allPrimaryKeysMap := []map[string]string{}
		for _, primaryKey := range allPrimaryKeys {
			parimaryKeyMap := map[string]string{}
			for _, pk := range primaryKey {
				parimaryKeyMap[pk.GetColumn()] = pk.GetQueryValue()
			}
			allPrimaryKeysMap = append(allPrimaryKeysMap, parimaryKeyMap)
		}

		for _, rowWrite := range tableWrite.GetRows() {
			primaryKeyColumnToValue := map[string]string{}
			otherColumnToValue := map[string]string{}

			for _, pk := range rowWrite.GetPrimaryKey() {
				primaryKeyColumnToValue[pk.GetColumn()] = pk.GetQueryValue()
			}

			if !isValidPrimaryKey(allPrimaryKeysMap, primaryKeyColumnToValue) {
				return fmt.Errorf("table %s row %v is not configured properly with primary key", tableWrite.GetTableName(), rowWrite)
			}

			for _, other := range rowWrite.GetWriteColumns() {
				otherColumnToValue[other.GetColumn()] = other.GetQueryValue()
			}

			err := tx.UpsertCustomTableByPK(ctx, extensions.CustomTableRow{
				TableName:               tableWrite.GetTableName(),
				PrimaryKeyColumnToValue: primaryKeyColumnToValue,
				OtherColumnToValue:      otherColumnToValue,
			})
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func isValidPrimaryKey(allPrimaryKeys []map[string]string, targetPrimaryKeyColumnToValue map[string]string) bool {
	for _, primaryKeyMap := range allPrimaryKeys {
		isValidPK := true
		for k, v := range primaryKeyMap {
			vv, ok := targetPrimaryKeyColumnToValue[k]
			if !ok || vv != v {
				isValidPK = false
				break
			}
		}

		if isValidPK {
			return true
		}
	}

	return false
}
