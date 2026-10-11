// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package attributestore

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestJitterIntervalUsesConfiguredRange(t *testing.T) {
	interval := time.Minute
	require.Equal(t, 54*time.Second, jitterInterval(interval, 0))
	require.Equal(t, 60*time.Second, jitterInterval(interval, 0.5))
	require.Equal(t, 66*time.Second, jitterInterval(interval, 1))
}

func TestRedactStorageSecretPreservesCause(t *testing.T) {
	source := fmt.Errorf("connect mongodb://user:secret@host: %w", context.Canceled)
	redacted := redactStorageSecret(source, "mongodb://user:secret@host")
	require.NotContains(t, redacted.Error(), "secret")
	require.Contains(t, redacted.Error(), "[REDACTED]")
	require.ErrorIs(t, redacted, context.Canceled)
}

func TestColumnConversionBoundaries(t *testing.T) {
	maximum := int64(3)
	textColumn := columnSchema{dataType: "varchar", characterMaximum: &maximum}
	value, err := textColumn.convert(stringValue("三字好"), config.AttributeStoreTypePostgres)
	require.NoError(t, err)
	require.Equal(t, "三字好", value)
	_, err = textColumn.convert(stringValue("four"), config.AttributeStoreTypePostgres)
	require.ErrorContains(t, err, "length")

	integerColumn := columnSchema{dataType: "smallint"}
	_, err = integerColumn.convert(intValue(32767), config.AttributeStoreTypePostgres)
	require.NoError(t, err)
	_, err = integerColumn.convert(intValue(32768), config.AttributeStoreTypePostgres)
	require.ErrorContains(t, err, "int64")

	mediumIntegerColumn := columnSchema{dataType: "mediumint", columnType: "mediumint"}
	_, err = mediumIntegerColumn.convert(intValue(8388607), config.AttributeStoreTypeMySQL)
	require.NoError(t, err)
	_, err = mediumIntegerColumn.convert(intValue(8388608), config.AttributeStoreTypeMySQL)
	require.ErrorContains(t, err, "int64")

	unsignedIntegerColumn := columnSchema{dataType: "mediumint", columnType: "mediumint unsigned"}
	_, err = unsignedIntegerColumn.convert(intValue(16777215), config.AttributeStoreTypeMySQL)
	require.NoError(t, err)
	_, err = unsignedIntegerColumn.convert(intValue(-1), config.AttributeStoreTypeMySQL)
	require.ErrorContains(t, err, "int64")
	_, err = unsignedIntegerColumn.convert(intValue(16777216), config.AttributeStoreTypeMySQL)
	require.ErrorContains(t, err, "int64")

	boolColumn := columnSchema{dataType: "tinyint", columnType: "tinyint(1)"}
	_, err = boolColumn.convert(boolValue(true), config.AttributeStoreTypeMySQL)
	require.NoError(t, err)
	_, err = boolColumn.convert(boolValue(true), config.AttributeStoreTypePostgres)
	require.ErrorContains(t, err, "bool")

	jsonColumn := columnSchema{dataType: "jsonb"}
	_, err = jsonColumn.convert(objectValue("json", `{"valid":true}`), config.AttributeStoreTypePostgres)
	require.NoError(t, err)
	_, err = jsonColumn.convert(objectValue("json", `{invalid`), config.AttributeStoreTypePostgres)
	require.ErrorContains(t, err, "invalid")

	postgresDatetime := columnSchema{dataType: "timestamp with time zone"}
	datetime, err := postgresDatetime.convert(
		stringValue("2026-08-11T15:30:00.123456789Z"),
		config.AttributeStoreTypePostgres,
	)
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, 8, 11, 15, 30, 0, 123456789, time.UTC), datetime)
	datetime, err = postgresDatetime.convert(
		objectValue("json", `"2026-08-11T15:30:00Z"`),
		config.AttributeStoreTypePostgres,
	)
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, 8, 11, 15, 30, 0, 0, time.UTC), datetime)
	datetime, err = postgresDatetime.convert(
		objectValue("json", `1786462200.123456789`),
		config.AttributeStoreTypePostgres,
	)
	require.NoError(t, err)
	require.Equal(t, time.Unix(1786462200, 123456789), datetime)
	_, err = postgresDatetime.convert(stringValue("not-a-datetime"), config.AttributeStoreTypePostgres)
	require.ErrorContains(t, err, "RFC3339")

	nonNullable := columnSchema{dataType: "text"}
	_, err = nonNullable.convert(nullValue(), config.AttributeStoreTypePostgres)
	require.ErrorContains(t, err, "nullable")
}

func TestBuildUpsertQuotesIdentifiersAndBindsValues(t *testing.T) {
	snapshot := &tableSchema{
		reference:    tableReference{namespace: "reporting", table: "flow attributes"},
		flowIDColumn: "FlowID",
	}
	values := map[string]filteredValue{
		"select": {value: int64(7)},
		"name":   {value: "flow"},
	}
	postgresDialect, err := newSQLDialect(config.AttributeStoreTypePostgres)
	require.NoError(t, err)
	postgres := &sqlStore{dialect: postgresDialect}
	query, arguments := postgres.buildUpsert(snapshot, []string{"flow-id"}, values)
	require.Equal(t, `INSERT INTO "reporting"."flow attributes" ("FlowID", "name", "select") VALUES ($1, $2, $3) ON CONFLICT ("FlowID") DO UPDATE SET "name" = EXCLUDED."name", "select" = EXCLUDED."select"`, query)
	require.Equal(t, []any{"flow-id", "flow", int64(7)}, arguments)

	mysqlDialect, err := newSQLDialect(config.AttributeStoreTypeMySQL)
	require.NoError(t, err)
	mysql := &sqlStore{dialect: mysqlDialect}
	query, arguments = mysql.buildUpsert(snapshot, []string{"flow-id"}, values)
	require.Equal(t, "INSERT INTO `reporting`.`flow attributes` (`FlowID`, `name`, `select`) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE `name` = VALUES(`name`), `select` = VALUES(`select`)", query)
	require.Equal(t, []any{"flow-id", "flow", int64(7)}, arguments)
}

func TestBuildWarehouseMergeQuotesIdentifiersAndBindsValues(t *testing.T) {
	snapshot := &tableSchema{
		reference: tableReference{
			catalog:   "analytics",
			namespace: "reporting",
			table:     "flow attributes",
		},
		flowIDColumn: "FlowID",
	}
	values := map[string]filteredValue{
		"profile": {column: columnSchema{dataType: "variant"}, value: `{"active":true}`},
		"name":    {column: columnSchema{dataType: "string"}, value: "flow"},
	}

	databricksDialect, err := newSQLDialect(config.AttributeStoreTypeDatabricks)
	require.NoError(t, err)
	databricks := &sqlStore{dialect: databricksDialect}
	query, arguments := databricks.buildUpsert(snapshot, []string{"flow-id"}, values)
	require.Equal(t, "MERGE INTO `analytics`.`reporting`.`flow attributes` AS target USING (SELECT CAST(? AS STRING) AS `FlowID`, ? AS `name`, PARSE_JSON(?) AS `profile`) AS source ON target.`FlowID` = source.`FlowID` WHEN MATCHED THEN UPDATE SET target.`name` = source.`name`, target.`profile` = source.`profile` WHEN NOT MATCHED THEN INSERT (`FlowID`, `name`, `profile`) VALUES (source.`FlowID`, source.`name`, source.`profile`)", query)
	require.Equal(t, []any{"flow-id", "flow", `{"active":true}`}, arguments)
	databricksTimestampQuery, databricksTimestampArguments := databricks.buildUpsert(snapshot, []string{"flow-id"}, map[string]filteredValue{
		"logged_at": {column: columnSchema{dataType: "timestamp_ntz"}, value: "2026-08-11T15:30:00Z"},
	})
	require.Contains(t, databricksTimestampQuery, "CAST(? AS TIMESTAMP_NTZ) AS `logged_at`")
	require.Equal(t, []any{"flow-id", "2026-08-11T15:30:00Z"}, databricksTimestampArguments)

	snowflakeDialect, err := newSQLDialect(config.AttributeStoreTypeSnowflake)
	require.NoError(t, err)
	snowflake := &sqlStore{dialect: snowflakeDialect}
	query, arguments = snowflake.buildUpsert(snapshot, []string{"flow-id"}, values)
	require.Equal(t, `MERGE INTO "analytics"."reporting"."flow attributes" AS target USING (SELECT CAST(? AS VARCHAR) AS "FlowID", ? AS "name", PARSE_JSON(?) AS "profile") AS source ON target."FlowID" = source."FlowID" WHEN MATCHED THEN UPDATE SET target."name" = source."name", target."profile" = source."profile" WHEN NOT MATCHED THEN INSERT ("FlowID", "name", "profile") VALUES (source."FlowID", source."name", source."profile")`, query)
	require.Equal(t, []any{"flow-id", "flow", `{"active":true}`}, arguments)
	snowflakeTimestampQuery, snowflakeTimestampArguments := snowflake.buildUpsert(snapshot, []string{"flow-id"}, map[string]filteredValue{
		"logged_at": {column: columnSchema{dataType: "timestamp_ntz"}, value: "2026-08-11T15:30:00Z"},
	})
	require.Contains(t, snowflakeTimestampQuery, `TO_TIMESTAMP_NTZ(?) AS "logged_at"`)
	require.Equal(t, []any{"flow-id", "2026-08-11T15:30:00Z"}, snowflakeTimestampArguments)
	snowflakeBinaryQuery, snowflakeBinaryArguments := snowflake.buildUpsert(snapshot, []string{"flow-id"}, map[string]filteredValue{
		"payload": {column: columnSchema{dataType: "binary"}, value: []byte{0x01, 0xab}},
	})
	require.Contains(t, snowflakeBinaryQuery, `TO_BINARY(?, 'HEX') AS "payload"`)
	require.Equal(t, []any{"flow-id", "01ab"}, snowflakeBinaryArguments)
	databricksBinaryQuery, databricksBinaryArguments := databricks.buildUpsert(snapshot, []string{"flow-id"}, map[string]filteredValue{
		"payload": {column: columnSchema{dataType: "binary"}, value: []byte{0x01, 0xab}},
	})
	require.Contains(t, databricksBinaryQuery, "UNHEX(?) AS `payload`")
	require.Equal(t, []any{"flow-id", "01ab"}, databricksBinaryArguments)

	maliciousName := `profile"; DROP TABLE audit; --`
	query, arguments = snowflake.buildUpsert(snapshot, []string{"flow-value'; DROP TABLE flows; --"}, map[string]filteredValue{
		maliciousName: {column: columnSchema{dataType: "varchar"}, value: "attribute-value'; DROP TABLE values; --"},
	})
	require.Contains(t, query, `"profile""; DROP TABLE audit; --"`)
	require.NotContains(t, query, "attribute-value")
	require.NotContains(t, query, "flow-value")
	require.Equal(t, []any{"flow-value'; DROP TABLE flows; --", "attribute-value'; DROP TABLE values; --"}, arguments)
}

func TestWarehouseColumnConversions(t *testing.T) {
	precision := int64(3)
	zeroScale := int64(0)
	tests := []struct {
		name      string
		storeType config.AttributeStoreType
		column    columnSchema
		value     *dexpb.Value
		expected  any
	}{
		{"databricks string", config.AttributeStoreTypeDatabricks, columnSchema{dataType: "string"}, stringValue("value"), "value"},
		{"databricks byte", config.AttributeStoreTypeDatabricks, columnSchema{dataType: "byteint"}, intValue(127), int64(127)},
		{"databricks decimal", config.AttributeStoreTypeDatabricks, columnSchema{dataType: "decimal", numericPrecision: &precision, numericScale: &zeroScale}, intValue(999), int64(999)},
		{"databricks double", config.AttributeStoreTypeDatabricks, columnSchema{dataType: "double"}, warehouseDoubleValue(2.5), 2.5},
		{"databricks bool", config.AttributeStoreTypeDatabricks, columnSchema{dataType: "boolean"}, boolValue(true), true},
		{"databricks timestamp", config.AttributeStoreTypeDatabricks, columnSchema{dataType: "timestamp_ntz"}, stringValue("2026-08-11T08:30:00-07:00"), "2026-08-11T15:30:00Z"},
		{"databricks variant", config.AttributeStoreTypeDatabricks, columnSchema{dataType: "variant"}, objectValue("json", `{"ok":true}`), `{"ok":true}`},
		{"databricks binary", config.AttributeStoreTypeDatabricks, columnSchema{dataType: "binary"}, objectValue("proto", "bytes"), []byte("bytes")},
		{"snowflake varchar", config.AttributeStoreTypeSnowflake, columnSchema{dataType: "varchar"}, stringValue("value"), "value"},
		{"snowflake fixed", config.AttributeStoreTypeSnowflake, columnSchema{dataType: "fixed", numericPrecision: &precision, numericScale: &zeroScale}, intValue(999), int64(999)},
		{"snowflake float", config.AttributeStoreTypeSnowflake, columnSchema{dataType: "float"}, warehouseDoubleValue(2.5), 2.5},
		{"snowflake bool", config.AttributeStoreTypeSnowflake, columnSchema{dataType: "boolean"}, boolValue(true), true},
		{"snowflake timestamp", config.AttributeStoreTypeSnowflake, columnSchema{dataType: "timestamp_tz"}, objectValue("json", `"2026-08-11T08:30:00-07:00"`), "2026-08-11T15:30:00Z"},
		{"snowflake variant", config.AttributeStoreTypeSnowflake, columnSchema{dataType: "variant"}, objectValue("json", `[1,true]`), `[1,true]`},
		{"snowflake binary", config.AttributeStoreTypeSnowflake, columnSchema{dataType: "binary"}, objectValue("proto", "bytes"), []byte("bytes")},
		{"nullable", config.AttributeStoreTypeSnowflake, columnSchema{dataType: "varchar", nullable: true}, nullValue(), nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			converted, err := test.column.convert(test.value, test.storeType)
			require.NoError(t, err)
			require.Equal(t, test.expected, converted)
		})
	}

	decimal := columnSchema{dataType: "number", numericPrecision: &precision, numericScale: &zeroScale}
	_, err := decimal.convert(intValue(1000), config.AttributeStoreTypeSnowflake)
	require.ErrorContains(t, err, "int64")
	_, err = (columnSchema{dataType: "float"}).convert(
		warehouseDoubleValue(math.Inf(1)), config.AttributeStoreTypeSnowflake,
	)
	require.ErrorContains(t, err, "double")
	_, err = (columnSchema{dataType: "number"}).convert(
		warehouseDoubleValue(2.5), config.AttributeStoreTypeSnowflake,
	)
	require.ErrorContains(t, err, "double")
}

func TestMongoValueConversion(t *testing.T) {
	converted, err := convertMongoValue(objectValue("json", `{"count":42,"ratio":2.5,"nested":{"active":true}}`))
	require.NoError(t, err)
	require.Equal(t, int64(42), converted.(bson.M)["count"])
	require.Equal(t, 2.5, converted.(bson.M)["ratio"])
	require.Equal(t, true, converted.(bson.M)["nested"].(bson.M)["active"])

	converted, err = convertMongoValue(objectValue("proto", "bytes"))
	require.NoError(t, err)
	require.Equal(t, bson.Binary{Subtype: 0, Data: []byte("bytes")}, converted)

	converted, err = convertMongoValue(objectValue("json", `[1,2.5,true,null,"value"]`))
	require.NoError(t, err)
	require.Equal(t, bson.A{int64(1), 2.5, true, nil, "value"}, converted)

	for _, test := range []struct {
		value    *dexpb.Value
		expected any
	}{
		{stringValue("value"), "value"},
		{intValue(42), int64(42)},
		{warehouseDoubleValue(2.5), 2.5},
		{boolValue(true), true},
		{nullValue(), nil},
	} {
		converted, err = convertMongoValue(test.value)
		require.NoError(t, err)
		require.Equal(t, test.expected, converted)
	}
	_, err = convertMongoValue(objectValue("json", `{invalid`))
	require.ErrorContains(t, err, "invalid")
	_, err = convertMongoValue(warehouseDoubleValue(math.NaN()))
	require.ErrorContains(t, err, "non-finite")

	for _, name := range []string{"_id", "$reserved", "nested.field", "nul\x00field", ""} {
		require.False(t, isValidMongoAttributeName(name), name)
	}
	require.True(t, isValidMongoAttributeName("display_name"))
}

func stringValue(value string) *dexpb.Value {
	return &dexpb.Value{Kind: &dexpb.Value_StringValue{StringValue: value}}
}

func intValue(value int64) *dexpb.Value {
	return &dexpb.Value{Kind: &dexpb.Value_IntValue{IntValue: value}}
}

func boolValue(value bool) *dexpb.Value {
	return &dexpb.Value{Kind: &dexpb.Value_BoolValue{BoolValue: value}}
}

func warehouseDoubleValue(value float64) *dexpb.Value {
	return &dexpb.Value{Kind: &dexpb.Value_DoubleValue{DoubleValue: value}}
}

func objectValue(encoding, payload string) *dexpb.Value {
	return &dexpb.Value{Kind: &dexpb.Value_ObjValue{ObjValue: &dexpb.EncodedObject{
		Encoding: encoding,
		Payload:  []byte(payload),
	}}}
}

func nullValue() *dexpb.Value {
	return &dexpb.Value{Kind: &dexpb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}
}
