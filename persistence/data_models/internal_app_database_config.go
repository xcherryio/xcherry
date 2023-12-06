// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: BUSL-1.1

package data_models

import "github.com/xcherryio/apis/goapi/xcapi"

type InternalAppDatabaseConfig struct {
	// key is the table name, value is the primary key names and values
	TablePrimaryKeys map[string][][]xcapi.AppDatabaseColumnValue `json:"tablePrimaryKeys"`
}

func getInternalAppDatabaseConfig(req xcapi.ProcessExecutionStartRequest) *InternalAppDatabaseConfig {
	if req.ProcessStartConfig != nil && req.ProcessStartConfig.AppDatabaseConfig != nil {
		tablePrimaryKeys := map[string][][]xcapi.AppDatabaseColumnValue{}

		for _, tableConfig := range req.ProcessStartConfig.AppDatabaseConfig.Tables {
			var primaryKeys [][]xcapi.AppDatabaseColumnValue

			for _, rowConfig := range tableConfig.Rows {
				primaryKeys = append(primaryKeys, rowConfig.GetPrimaryKey())
			}

			tablePrimaryKeys[tableConfig.GetTableName()] = primaryKeys
		}

		return &InternalAppDatabaseConfig{
			TablePrimaryKeys: tablePrimaryKeys,
		}
	}
	return nil
}
