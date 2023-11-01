package persistence

import "github.com/xdblab/xdb-apis/goapi/xdbapi"

type InternalGlobalAttributeConfig struct {
	// key is the table name, value is the primary key name and value
	TablePrimaryKeys map[string]xdbapi.TableColumnValue `json:"tablePrimaryKeys"`
}