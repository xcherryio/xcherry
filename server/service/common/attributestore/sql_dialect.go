// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package attributestore

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/superdurable/dex/config"
)

type sqlDialect interface {
	driverName() string
	resolveTableReference(context.Context, *sql.DB, string) (tableReference, error)
	describeColumnsQuery(tableReference) (string, []any)
	enrichColumns(context.Context, *sql.DB, tableReference, map[string]columnSchema) error
	describePrimaryKeys(context.Context, *sql.DB, tableReference) ([]string, error)
	buildUpsert(*tableSchema, []string, []string, map[string]filteredValue) (string, []any)
	buildKeyedUpsert(tableReference, []string, []string, []string, map[string]filteredValue) (string, []any)
	buildKeyedDelete(tableReference, []string, []string) (string, []any)
}

type builtInSQLDialect struct {
	storeType config.AttributeStoreType
}

func newSQLDialect(storeType config.AttributeStoreType) (sqlDialect, error) {
	switch storeType {
	case config.AttributeStoreTypeMySQL, config.AttributeStoreTypePostgres,
		config.AttributeStoreTypeDatabricks, config.AttributeStoreTypeSnowflake:
		return &builtInSQLDialect{storeType: storeType}, nil
	default:
		return nil, fmt.Errorf("unsupported SQL Attribute Store type %q", storeType)
	}
}

func (d *builtInSQLDialect) driverName() string {
	switch d.storeType {
	case config.AttributeStoreTypeMySQL:
		return "mysql"
	case config.AttributeStoreTypePostgres:
		return "pgx"
	case config.AttributeStoreTypeDatabricks:
		return "databricks"
	case config.AttributeStoreTypeSnowflake:
		return "snowflake"
	default:
		panic("unsupported SQL Attribute Store type")
	}
}

func (d *builtInSQLDialect) resolveTableReference(
	ctx context.Context,
	database *sql.DB,
	tableName string,
) (tableReference, error) {
	parts := strings.Split(tableName, ".")
	maximumParts := 2
	if d.storeType.IsWarehouse() {
		maximumParts = 3
	}
	if len(parts) == 0 || len(parts) > maximumParts {
		return tableReference{}, fmt.Errorf("tableName must contain at most %d identifiers", maximumParts)
	}
	for _, part := range parts {
		if part == "" || strings.IndexByte(part, 0) >= 0 {
			return tableReference{}, fmt.Errorf("tableName contains an invalid identifier")
		}
	}
	if !d.storeType.IsWarehouse() {
		if len(parts) == 2 {
			return tableReference{namespace: parts[0], table: parts[1]}, nil
		}
		namespace, err := d.currentNamespace(ctx, database)
		if err != nil {
			return tableReference{}, err
		}
		return tableReference{namespace: namespace, table: parts[0]}, nil
	}
	if len(parts) == 3 {
		return tableReference{catalog: parts[0], namespace: parts[1], table: parts[2]}, nil
	}
	catalog, namespace, err := d.currentWarehouseNamespace(ctx, database)
	if err != nil {
		return tableReference{}, err
	}
	if len(parts) == 2 {
		namespace = parts[0]
		parts = parts[1:]
	}
	return tableReference{catalog: catalog, namespace: namespace, table: parts[0]}, nil
}

func (d *builtInSQLDialect) currentNamespace(ctx context.Context, database *sql.DB) (string, error) {
	query := "SELECT current_schema()"
	if d.storeType == config.AttributeStoreTypeMySQL {
		query = "SELECT DATABASE()"
	}
	var namespace string
	if err := database.QueryRowContext(ctx, query).Scan(&namespace); err != nil {
		return "", fmt.Errorf("resolve table namespace: %w", err)
	}
	if namespace == "" {
		return "", fmt.Errorf("database has no active namespace")
	}
	return namespace, nil
}

func (d *builtInSQLDialect) currentWarehouseNamespace(
	ctx context.Context,
	database *sql.DB,
) (string, string, error) {
	query := "SELECT current_catalog(), current_schema()"
	if d.storeType == config.AttributeStoreTypeSnowflake {
		query = "SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()"
	}
	var catalog, namespace string
	if err := database.QueryRowContext(ctx, query).Scan(&catalog, &namespace); err != nil {
		return "", "", fmt.Errorf("resolve warehouse namespace: %w", err)
	}
	if catalog == "" || namespace == "" {
		return "", "", fmt.Errorf("warehouse has no active catalog/database and schema")
	}
	return catalog, namespace, nil
}

func (d *builtInSQLDialect) describeColumnsQuery(reference tableReference) (string, []any) {
	if d.storeType == config.AttributeStoreTypeMySQL {
		return `SELECT column_name, data_type, column_type, is_nullable,
character_maximum_length, numeric_precision, numeric_scale, column_default,
extra, generation_expression
FROM information_schema.columns
WHERE table_schema = ? AND table_name = ?
ORDER BY ordinal_position`, []any{reference.namespace, reference.table}
	}
	if d.storeType == config.AttributeStoreTypePostgres {
		return `SELECT column_name, data_type, udt_name, is_nullable,
character_maximum_length, numeric_precision, numeric_scale, column_default,
is_identity, is_generated
FROM information_schema.columns
WHERE table_schema = $1 AND table_name = $2
ORDER BY ordinal_position`, []any{reference.namespace, reference.table}
	}
	qualifiedInformationSchema := d.quoteIdentifier(reference.catalog) + "." +
		d.quoteIdentifier("information_schema") + "." + d.quoteIdentifier("columns")
	if d.storeType == config.AttributeStoreTypeSnowflake {
		return `SELECT column_name, data_type, COALESCE(data_type_alias, data_type), is_nullable,
character_maximum_length, numeric_precision, numeric_scale, column_default,
is_identity, expression
FROM ` + qualifiedInformationSchema + `
WHERE table_schema = ? AND table_name = ?
ORDER BY ordinal_position`, []any{reference.namespace, reference.table}
	}
	return `SELECT column_name, data_type, full_data_type, is_nullable,
character_maximum_length, numeric_precision, numeric_scale, column_default,
is_identity, is_generated
FROM ` + qualifiedInformationSchema + `
WHERE table_schema = ? AND table_name = ?
ORDER BY ordinal_position`, []any{reference.namespace, reference.table}
}

func (d *builtInSQLDialect) enrichColumns(
	ctx context.Context,
	database *sql.DB,
	reference tableReference,
	columns map[string]columnSchema,
) error {
	if d.storeType != config.AttributeStoreTypeDatabricks {
		return nil
	}
	var createStatement string
	if err := database.QueryRowContext(ctx, "SHOW CREATE TABLE "+d.qualifiedTable(reference)).Scan(&createStatement); err != nil {
		return fmt.Errorf("show Databricks table definition: %w", err)
	}
	definitions, err := parseDatabricksColumnDefinitions(createStatement, d.qualifiedTable(reference))
	if err != nil {
		return err
	}
	for name, column := range columns {
		definition, found := definitions[name]
		if !found {
			return fmt.Errorf("Databricks table definition is missing column %q", name)
		}
		definitionKeywords := sqlOutsideQuotedValues(definition)
		column.hasDefault = databricksDefaultPattern.MatchString(definitionKeywords)
		column.generated = databricksGeneratedPattern.MatchString(definitionKeywords)
		columns[name] = column
	}
	return nil
}

var (
	databricksDefaultPattern   = regexp.MustCompile(`(?i)\bDEFAULT\b`)
	databricksGeneratedPattern = regexp.MustCompile(`(?i)\bGENERATED\s+(?:ALWAYS|BY\s+DEFAULT)\s+AS\b`)
)

func parseDatabricksColumnDefinitions(createStatement, qualifiedTable string) (map[string]string, error) {
	tablePosition := strings.Index(createStatement, qualifiedTable)
	if tablePosition < 0 {
		return nil, fmt.Errorf("Databricks table definition does not identify %s", qualifiedTable)
	}
	openingPosition := strings.IndexByte(createStatement[tablePosition+len(qualifiedTable):], '(')
	if openingPosition < 0 {
		return nil, fmt.Errorf("Databricks table definition has no column list")
	}
	openingPosition += tablePosition + len(qualifiedTable)
	columnList, err := extractDelimitedSQL(createStatement, openingPosition)
	if err != nil {
		return nil, err
	}
	definitions := map[string]string{}
	for _, definition := range splitTopLevelSQL(columnList) {
		name, remainder, isColumn := parseDatabricksColumnDefinition(definition)
		if isColumn {
			definitions[name] = remainder
		}
	}
	return definitions, nil
}

func extractDelimitedSQL(statement string, openingPosition int) (string, error) {
	depth := 0
	quote := byte(0)
	for position := openingPosition; position < len(statement); position++ {
		character := statement[position]
		if quote != 0 {
			if character == quote {
				if position+1 < len(statement) && statement[position+1] == quote {
					position++
					continue
				}
				quote = 0
			}
			continue
		}
		switch character {
		case '\'', '"', '`':
			quote = character
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return statement[openingPosition+1 : position], nil
			}
		}
	}
	return "", fmt.Errorf("Databricks table definition has an unterminated column list")
}

func splitTopLevelSQL(value string) []string {
	parts := make([]string, 0)
	start := 0
	parentheses, angles, brackets := 0, 0, 0
	quote := byte(0)
	for position := 0; position < len(value); position++ {
		character := value[position]
		if quote != 0 {
			if character == quote {
				if position+1 < len(value) && value[position+1] == quote {
					position++
					continue
				}
				quote = 0
			}
			continue
		}
		switch character {
		case '\'', '"', '`':
			quote = character
		case '(':
			parentheses++
		case ')':
			parentheses--
		case '<':
			angles++
		case '>':
			angles--
		case '[':
			brackets++
		case ']':
			brackets--
		case ',':
			if parentheses == 0 && angles == 0 && brackets == 0 {
				parts = append(parts, strings.TrimSpace(value[start:position]))
				start = position + 1
			}
		}
	}
	parts = append(parts, strings.TrimSpace(value[start:]))
	return parts
}

func parseDatabricksColumnDefinition(definition string) (string, string, bool) {
	definition = strings.TrimSpace(definition)
	if definition == "" {
		return "", "", false
	}
	if definition[0] != '`' {
		end := strings.IndexAny(definition, " \t\r\n")
		if end < 0 {
			return "", "", false
		}
		name := definition[:end]
		switch strings.ToUpper(name) {
		case "CONSTRAINT", "PRIMARY", "FOREIGN", "UNIQUE", "CHECK":
			return "", "", false
		default:
			return name, strings.TrimSpace(definition[end:]), true
		}
	}
	var name strings.Builder
	for position := 1; position < len(definition); position++ {
		if definition[position] != '`' {
			name.WriteByte(definition[position])
			continue
		}
		if position+1 < len(definition) && definition[position+1] == '`' {
			name.WriteByte('`')
			position++
			continue
		}
		return name.String(), strings.TrimSpace(definition[position+1:]), true
	}
	return "", "", false
}

func sqlOutsideQuotedValues(value string) string {
	result := []byte(value)
	quote := byte(0)
	for position := 0; position < len(result); position++ {
		character := result[position]
		if quote == 0 {
			if character == '\'' || character == '"' || character == '`' {
				quote = character
				result[position] = ' '
			}
			continue
		}
		result[position] = ' '
		if character != quote {
			continue
		}
		if position+1 < len(result) && result[position+1] == quote {
			result[position+1] = ' '
			position++
			continue
		}
		quote = 0
	}
	return string(result)
}

func (d *builtInSQLDialect) describePrimaryKeys(
	ctx context.Context,
	database *sql.DB,
	reference tableReference,
) (keys []string, err error) {
	query := `SELECT kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name
 AND tc.table_schema = kcu.table_schema
 AND tc.table_name = kcu.table_name
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND tc.table_schema = $1 AND tc.table_name = $2
ORDER BY kcu.ordinal_position`
	if d.storeType == config.AttributeStoreTypeMySQL {
		query = `SELECT column_name
FROM information_schema.key_column_usage
WHERE constraint_name = 'PRIMARY' AND table_schema = ? AND table_name = ?
ORDER BY ordinal_position`
	}
	rows, err := database.QueryContext(ctx, query, reference.namespace, reference.table)
	if err != nil {
		return nil, fmt.Errorf("describe primary key: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close primary-key rows: %w", closeErr))
		}
	}()
	for rows.Next() {
		var key string
		if scanErr := rows.Scan(&key); scanErr != nil {
			return nil, fmt.Errorf("scan primary key: %w", scanErr)
		}
		keys = append(keys, key)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, fmt.Errorf("iterate primary keys: %w", rowsErr)
	}
	return keys, nil
}

func (d *builtInSQLDialect) buildUpsert(
	snapshot *tableSchema,
	keyValues []string,
	columnNames []string,
	values map[string]filteredValue,
) (string, []any) {
	if d.storeType.IsWarehouse() {
		return d.buildWarehouseMerge(snapshot, keyValues[0], columnNames, values)
	}
	return d.buildKeyedUpsert(snapshot.reference, snapshot.keyColumns(), keyValues, columnNames, values)
}

// buildKeyedUpsert returns a PostgreSQL or MySQL upsert of one row that sets columnNames, writing DEFAULT to a column
// without a value; a row of key columns alone is inserted once and left unchanged afterwards.
func (d *builtInSQLDialect) buildKeyedUpsert(
	reference tableReference,
	keyColumns []string,
	keyValues []string,
	columnNames []string,
	values map[string]filteredValue,
) (string, []any) {
	quotedTable := d.qualifiedTable(reference)
	quotedKeys := make([]string, 0, len(keyColumns))
	columns := make([]string, 0, len(keyColumns)+len(columnNames))
	placeholders := make([]string, 0, len(keyColumns)+len(columnNames))
	updates := make([]string, 0, len(columnNames))
	arguments := make([]any, 0, len(keyColumns)+len(columnNames))
	for index, keyColumn := range keyColumns {
		quotedKeys = append(quotedKeys, d.quoteIdentifier(keyColumn))
		placeholders = append(placeholders, d.placeholder(index+1))
		arguments = append(arguments, keyValues[index])
	}
	columns = append(columns, quotedKeys...)
	for _, name := range columnNames {
		quoted := d.quoteIdentifier(name)
		columns = append(columns, quoted)
		if value, hasValue := values[name]; hasValue {
			arguments = append(arguments, d.argumentValue(value.column, value.value))
			placeholders = append(placeholders, d.valueExpression(d.placeholder(len(arguments)), value.column))
		} else {
			placeholders = append(placeholders, "DEFAULT")
		}
		if d.storeType == config.AttributeStoreTypePostgres {
			updates = append(updates, quoted+" = EXCLUDED."+quoted)
		} else {
			updates = append(updates, quoted+" = VALUES("+quoted+")")
		}
	}
	query := "INSERT INTO " + quotedTable + " (" + strings.Join(columns, ", ") + ") VALUES (" +
		strings.Join(placeholders, ", ") + ")"
	lastKey := quotedKeys[len(quotedKeys)-1]
	switch {
	case d.storeType == config.AttributeStoreTypePostgres && len(updates) == 0:
		query += " ON CONFLICT (" + strings.Join(quotedKeys, ", ") + ") DO NOTHING"
	case d.storeType == config.AttributeStoreTypePostgres:
		query += " ON CONFLICT (" + strings.Join(quotedKeys, ", ") + ") DO UPDATE SET " +
			strings.Join(updates, ", ")
	case len(updates) == 0:
		query += " ON DUPLICATE KEY UPDATE " + lastKey + " = VALUES(" + lastKey + ")"
	default:
		query += " ON DUPLICATE KEY UPDATE " + strings.Join(updates, ", ")
	}
	return query, arguments
}

// buildKeyedDelete returns a PostgreSQL or MySQL delete of the one row with keyValues.
func (d *builtInSQLDialect) buildKeyedDelete(
	reference tableReference,
	keyColumns []string,
	keyValues []string,
) (string, []any) {
	conditions := make([]string, 0, len(keyColumns))
	arguments := make([]any, 0, len(keyColumns))
	for index, keyColumn := range keyColumns {
		conditions = append(conditions, d.quoteIdentifier(keyColumn)+" = "+d.placeholder(index+1))
		arguments = append(arguments, keyValues[index])
	}
	return "DELETE FROM " + d.qualifiedTable(reference) + " WHERE " + strings.Join(conditions, " AND "), arguments
}

func (d *builtInSQLDialect) buildWarehouseMerge(
	snapshot *tableSchema,
	flowID string,
	columnNames []string,
	values map[string]filteredValue,
) (string, []any) {
	flowIDColumn := d.quoteIdentifier(snapshot.flowIDColumn)
	sourceExpressions := []string{d.flowIDExpression("?") + " AS " + flowIDColumn}
	insertColumns := []string{flowIDColumn}
	insertValues := []string{"source." + flowIDColumn}
	updates := make([]string, 0, len(columnNames))
	arguments := []any{flowID}
	for _, name := range columnNames {
		quoted := d.quoteIdentifier(name)
		sourceExpressions = append(sourceExpressions, d.valueExpression("?", values[name].column)+" AS "+quoted)
		insertColumns = append(insertColumns, quoted)
		insertValues = append(insertValues, "source."+quoted)
		updates = append(updates, "target."+quoted+" = source."+quoted)
		arguments = append(arguments, d.argumentValue(values[name].column, values[name].value))
	}
	query := "MERGE INTO " + d.qualifiedTable(snapshot.reference) + " AS target USING (SELECT " +
		strings.Join(sourceExpressions, ", ") + ") AS source ON target." + flowIDColumn + " = source." +
		flowIDColumn + " WHEN MATCHED THEN UPDATE SET " + strings.Join(updates, ", ") +
		" WHEN NOT MATCHED THEN INSERT (" + strings.Join(insertColumns, ", ") + ") VALUES (" +
		strings.Join(insertValues, ", ") + ")"
	return query, arguments
}

func (d *builtInSQLDialect) flowIDExpression(placeholder string) string {
	if d.storeType == config.AttributeStoreTypeDatabricks {
		return "CAST(" + placeholder + " AS STRING)"
	}
	return "CAST(" + placeholder + " AS VARCHAR)"
}

func (d *builtInSQLDialect) valueExpression(placeholder string, column columnSchema) string {
	if column.acceptsBinary(d.storeType) && d.storeType == config.AttributeStoreTypeDatabricks {
		return "UNHEX(" + placeholder + ")"
	}
	if column.acceptsBinary(d.storeType) && d.storeType == config.AttributeStoreTypeSnowflake {
		return "TO_BINARY(" + placeholder + ", 'HEX')"
	}
	if column.dataType == "variant" && d.storeType.IsWarehouse() {
		return "PARSE_JSON(" + placeholder + ")"
	}
	if !column.acceptsTemporal(d.storeType) || !d.storeType.IsWarehouse() {
		return placeholder
	}
	if d.storeType == config.AttributeStoreTypeDatabricks {
		if column.dataType == "timestamp_ntz" || column.dataType == "timestamp without time zone" {
			return "CAST(" + placeholder + " AS TIMESTAMP_NTZ)"
		}
		return "CAST(" + placeholder + " AS TIMESTAMP)"
	}
	switch column.dataType {
	case "timestamp with time zone", "timestamp_tz":
		return "TO_TIMESTAMP_TZ(" + placeholder + ")"
	case "timestamp with local time zone", "timestamp_ltz":
		return "TO_TIMESTAMP_LTZ(" + placeholder + ")"
	default:
		return "TO_TIMESTAMP_NTZ(" + placeholder + ")"
	}
}

func (d *builtInSQLDialect) argumentValue(column columnSchema, value any) any {
	if d.storeType.IsWarehouse() && column.acceptsBinary(d.storeType) {
		if binaryValue, isBinary := value.([]byte); isBinary {
			return hex.EncodeToString(binaryValue)
		}
	}
	return value
}

func (d *builtInSQLDialect) placeholder(position int) string {
	if d.storeType == config.AttributeStoreTypePostgres {
		return "$" + strconv.Itoa(position)
	}
	return "?"
}

func (d *builtInSQLDialect) qualifiedTable(reference tableReference) string {
	parts := make([]string, 0, 3)
	if reference.catalog != "" {
		parts = append(parts, d.quoteIdentifier(reference.catalog))
	}
	parts = append(parts, d.quoteIdentifier(reference.namespace), d.quoteIdentifier(reference.table))
	return strings.Join(parts, ".")
}

func (d *builtInSQLDialect) quoteIdentifier(identifier string) string {
	if d.storeType == config.AttributeStoreTypeMySQL || d.storeType == config.AttributeStoreTypeDatabricks {
		return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
	}
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}
