// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: BUSL-1.1

package data_models

import "github.com/xcherryio/apis/goapi/xcapi"

type InternalGlobalAttributeConfig struct {
	// key is the table name, value is the primary key name and value
	TablePrimaryKeys map[string]xcapi.TableColumnValue `json:"tablePrimaryKeys"`
}

func getInternalGlobalAttributeConfig(req xdbapi.ProcessExecutionStartRequest) *InternalGlobalAttributeConfig {
	if req.ProcessStartConfig != nil && req.ProcessStartConfig.GlobalAttributeConfig != nil {
		primaryKeys := map[string]xdbapi.TableColumnValue{}
		for _, cfg := range req.ProcessStartConfig.GlobalAttributeConfig.TableConfigs {
			primaryKeys[cfg.TableName] = cfg.PrimaryKey
		}
		return &InternalGlobalAttributeConfig{
			TablePrimaryKeys: primaryKeys,
		}
	}
	return nil
}
