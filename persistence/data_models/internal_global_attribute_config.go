// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: BUSL-1.1

package data_models

type InternalGlobalAttributeConfig struct {
	// key is the table name, value is the primary key name and value
	TablePrimaryKeys map[string]xcapi.TableColumnValue `json:"tablePrimaryKeys"`
}
