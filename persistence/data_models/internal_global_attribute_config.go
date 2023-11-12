// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: BUSL-1.1

package data_models

import "github.com/xdblab/xdb-apis/goapi/xdbapi"

type InternalGlobalAttributeConfig struct {
	// key is the table name, value is the primary key name and value
	TablePrimaryKeys map[string]xdbapi.TableColumnValue `json:"tablePrimaryKeys"`
}
