// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package attributestore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service/common/log"
	"github.com/superdurable/dex/service/common/log/tag"
)

// mapInstanceValueColumn stores an instance value that is not a JSON object.
const mapInstanceValueColumn = "value"

// mapTableSchema describes the table whose rows are one AttributeMap's instances.
type mapTableSchema struct {
	reference         tableReference
	instanceKeyColumn string
	keyColumns        []string
	// replacedColumns are the non-key, non-generated columns that every instance write sets.
	replacedColumns []string
	columns         map[string]columnSchema
}

// instanceColumns holds the column values of one instance and the fields that have no column.
type instanceColumns struct {
	values              map[string]filteredValue
	missingColumnFields []string
}

func newMapTableSchema(
	reference tableReference,
	instanceKeyColumn string,
	parentKeyColumns []string,
	primaryKeys []string,
	columns map[string]columnSchema,
) (*mapTableSchema, error) {
	if slices.Contains(parentKeyColumns, instanceKeyColumn) {
		return nil, fmt.Errorf("instanceKeyColumn %q is also a primary-key column of the store", instanceKeyColumn)
	}
	table := &mapTableSchema{
		reference:         reference,
		instanceKeyColumn: instanceKeyColumn,
		keyColumns:        append(slices.Clone(parentKeyColumns), instanceKeyColumn),
		columns:           columns,
	}
	if !hasSameColumns(primaryKeys, table.keyColumns) {
		return nil, fmt.Errorf("table primary key %v must be exactly the columns %v", primaryKeys, table.keyColumns)
	}
	for _, name := range table.keyColumns {
		if !columns[name].acceptsString() {
			return nil, fmt.Errorf("primary-key column %q cannot store strings", name)
		}
	}
	for name, column := range columns {
		if slices.Contains(table.keyColumns, name) || column.generated {
			continue
		}
		if !column.nullable && !column.hasDefault {
			return nil, fmt.Errorf("column %q prevents partial row inserts", name)
		}
		table.replacedColumns = append(table.replacedColumns, name)
	}
	sort.Strings(table.replacedColumns)
	return table, nil
}

// filterInstanceValue returns the column values of one instance: a JSON object's top-level fields under the store's
// naming rule, or the value column for any other value, plus the fields that have no column. A field that maps to a
// key column is skipped, and logged when it differs from the key. Other unwritable fields are logged and left out.
func (t *mapTableSchema) filterInstanceValue(
	key string,
	keyValues []string,
	value *dexpb.Value,
	naming config.AttributeStoreColumnNaming,
	storeType config.AttributeStoreType,
	logger log.Logger,
) instanceColumns {
	result := instanceColumns{values: map[string]filteredValue{}}
	fields, isObject := jsonObjectFields(value)
	if !isObject {
		column, found := t.columns[mapInstanceValueColumn]
		if !found {
			result.missingColumnFields = append(result.missingColumnFields, mapInstanceValueColumn)
			return result
		}
		converted, err := column.convert(value, storeType)
		if err != nil {
			logger.Error("skip incompatible Attribute Store map instance", tag.AttributeName(key), tag.Error(err))
			return result
		}
		result.values[mapInstanceValueColumn] = filteredValue{column: column, value: converted}
		return result
	}
	fieldNamesByColumn := make(map[string][]string, len(fields))
	for fieldName := range fields {
		column := columnName(naming, fieldName)
		fieldNamesByColumn[column] = append(fieldNamesByColumn[column], fieldName)
	}
	for name, fieldNames := range fieldNamesByColumn {
		fieldKey := key + "." + fieldNames[0]
		if len(fieldNames) > 1 {
			sort.Strings(fieldNames)
			logger.Error("skip Attribute Store map instance fields: field names share one column",
				tag.AttributeName(key+"."+strings.Join(fieldNames, ", ")))
			continue
		}
		if keyIndex := slices.Index(t.keyColumns, name); keyIndex >= 0 {
			var fieldText string
			if json.Unmarshal(fields[fieldNames[0]], &fieldText) != nil || fieldText != keyValues[keyIndex] {
				logger.Error("skip Attribute Store map instance field: value differs from its primary-key column",
					tag.AttributeName(fieldKey))
			}
			continue
		}
		column, found := t.columns[name]
		if !found {
			result.missingColumnFields = append(result.missingColumnFields, fieldNames[0])
			continue
		}
		converted, err := column.convert(jsonFieldValue(fields[fieldNames[0]], column, storeType), storeType)
		if err != nil {
			logger.Error("skip incompatible Attribute Store map instance field", tag.AttributeName(fieldKey), tag.Error(err))
			continue
		}
		result.values[name] = filteredValue{column: column, value: converted}
	}
	return result
}

// unknownColumns returns the columns an instance value writes that this snapshot lacks; a delete writes none.
func (t *mapTableSchema) unknownColumns(value *dexpb.Value, naming config.AttributeStoreColumnNaming) []string {
	if _, isDelete := value.GetKind().(*dexpb.Value_NullValue); isDelete {
		return nil
	}
	fields, isObject := jsonObjectFields(value)
	if !isObject {
		if _, found := t.columns[mapInstanceValueColumn]; !found {
			return []string{mapInstanceValueColumn}
		}
		return nil
	}
	var unknown []string
	for fieldName := range fields {
		if column := columnName(naming, fieldName); t.columns[column].name == "" {
			unknown = append(unknown, column)
		}
	}
	return unknown
}

// jsonObjectFields returns the top-level fields of a JSON object value, and false for any other value.
func jsonObjectFields(value *dexpb.Value) (map[string]json.RawMessage, bool) {
	object := value.GetObjValue()
	if object == nil || object.GetEncoding() != "json" {
		return nil, false
	}
	payload := bytes.TrimSpace(object.GetPayload())
	if len(payload) == 0 || payload[0] != '{' {
		return nil, false
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return nil, false
	}
	return fields, true
}

// jsonFieldValue returns the Attribute value form of one JSON field for the column that stores it. JSON and
// timestamp columns receive the field as JSON; an integral number becomes an int64 only where the column takes one.
func jsonFieldValue(raw json.RawMessage, column columnSchema, storeType config.AttributeStoreType) *dexpb.Value {
	trimmed := bytes.TrimSpace(raw)
	if bytes.Equal(trimmed, []byte("null")) {
		return &dexpb.Value{Kind: &dexpb.Value_NullValue{}}
	}
	encodedJSON := &dexpb.Value{Kind: &dexpb.Value_ObjValue{
		ObjValue: &dexpb.EncodedObject{Encoding: "json", Payload: trimmed},
	}}
	if column.acceptsJSON(storeType) || column.acceptsTemporal(storeType) || len(trimmed) == 0 {
		return encodedJSON
	}
	switch trimmed[0] {
	case '"':
		var text string
		if err := json.Unmarshal(trimmed, &text); err != nil {
			return encodedJSON
		}
		return &dexpb.Value{Kind: &dexpb.Value_StringValue{StringValue: text}}
	case 't', 'f':
		return &dexpb.Value{Kind: &dexpb.Value_BoolValue{BoolValue: trimmed[0] == 't'}}
	case '{', '[':
		return encodedJSON
	}
	if integer, err := strconv.ParseInt(string(trimmed), 10, 64); err == nil && column.acceptsInt(integer, storeType) {
		return &dexpb.Value{Kind: &dexpb.Value_IntValue{IntValue: integer}}
	}
	number, err := strconv.ParseFloat(string(trimmed), 64)
	if err != nil {
		return encodedJSON
	}
	return &dexpb.Value{Kind: &dexpb.Value_DoubleValue{DoubleValue: number}}
}
