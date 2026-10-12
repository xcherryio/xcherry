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
	"errors"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/databricks/databricks-sql-go"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/snowflakedb/gosnowflake/v2"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/common/log"
	"github.com/superdurable/dex/service/common/log/tag"
)

type sqlStore struct {
	cfg     config.AttributeStoreConfigEntry
	dialect sqlDialect
	db      *sql.DB
	table   tableReference
	rowKey  *rowKeyTemplate
	// mapTables holds the resolved table of each configured AttributeMap.
	mapTables map[string]tableReference
	// missingColumnWarnings remembers the AttributeMap fields already logged for having no column.
	missingColumnWarnings     sync.Map
	missingColumnWarningCount atomic.Int64
	// columnRefreshMutex guards the two fields below and serializes refreshes for unknown columns.
	columnRefreshMutex sync.Mutex
	lastColumnRefresh  time.Time
	// confirmedMissingColumns holds the columns a refresh found missing.
	confirmedMissingColumns map[string]bool
	schema                  atomic.Pointer[tableSchema]
	logger                  log.Logger
}

type tableReference struct {
	catalog   string
	namespace string
	table     string
}

// tableSchema stores the whole Flow ID in flowIDColumn unless a configured rowKey splits it into key columns.
type tableSchema struct {
	reference    tableReference
	flowIDColumn string
	rowKey       *rowKeyTemplate
	columns      map[string]columnSchema
	mapTables    map[string]*mapTableSchema
}

// columnRefreshInterval bounds how often unremembered names refresh once maximumConfirmedMissingColumns is reached.
const columnRefreshInterval = 30 * time.Second

// maximumConfirmedMissingColumns bounds the remembered missing columns; beyond it, unremembered names share
// columnRefreshInterval.
const maximumConfirmedMissingColumns = 1000

// maximumMissingColumnWarnings bounds the remembered fields when instance values have dynamic top-level keys.
const maximumMissingColumnWarnings = 1000

type sqlStatement struct {
	query     string
	arguments []any
}

type columnSchema struct {
	name             string
	dataType         string
	columnType       string
	nullable         bool
	characterMaximum *int64
	numericPrecision *int64
	numericScale     *int64
	hasDefault       bool
	generated        bool
}

type filteredValue struct {
	column columnSchema
	value  any
}

func openSQLStore(
	ctx context.Context,
	cfg config.AttributeStoreConfigEntry,
	logger log.Logger,
) (*sqlStore, error) {
	dialect, err := newSQLDialect(cfg.Type)
	if err != nil {
		return nil, err
	}
	database, err := sql.Open(dialect.driverName(), cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("open connection: %w", err)
	}
	entry, err := initializeSQLStore(ctx, cfg, logger, dialect, database)
	if err != nil {
		return nil, closeSQLStoreOnError(database, err)
	}
	return entry, nil
}

func initializeSQLStore(
	ctx context.Context,
	cfg config.AttributeStoreConfigEntry,
	logger log.Logger,
	dialect sqlDialect,
	database *sql.DB,
) (*sqlStore, error) {
	entry := &sqlStore{cfg: cfg, dialect: dialect, db: database, logger: logger}
	if rowKeyText := effectiveRowKey(cfg); rowKeyText != "" {
		rowKey, err := parseRowKeyTemplate(rowKeyText)
		if err != nil {
			return nil, err
		}
		entry.rowKey = rowKey
	}
	if err := database.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping database: %w", err)
	}
	reference, err := dialect.resolveTableReference(ctx, database, cfg.TableName)
	if err != nil {
		return nil, err
	}
	entry.table = reference
	entry.mapTables = make(map[string]tableReference, len(cfg.AttributeMaps))
	for mapName, mapTable := range cfg.AttributeMaps {
		mapReference, err := dialect.resolveTableReference(ctx, database, mapTable.TableName)
		if err != nil {
			return nil, fmt.Errorf("AttributeMap %q: %w", mapName, err)
		}
		entry.mapTables[mapName] = mapReference
	}
	snapshot, err := entry.describe(ctx)
	if err != nil {
		return nil, err
	}
	entry.schema.Store(snapshot)
	return entry, nil
}

func closeSQLStoreOnError(database *sql.DB, sourceErr error) error {
	if closeErr := database.Close(); closeErr != nil {
		return errors.Join(sourceErr, closeErr)
	}
	return sourceErr
}

func (s *sqlStore) describe(ctx context.Context) (*tableSchema, error) {
	columns, err := s.describeColumns(ctx, s.table)
	if err != nil {
		return nil, err
	}
	snapshot := &tableSchema{
		reference: s.table,
		rowKey:    s.rowKey,
		columns:   columns,
		mapTables: make(map[string]*mapTableSchema, len(s.mapTables)),
	}
	if s.rowKey == nil {
		flowIDColumn, err := s.resolveFlowIDColumn(ctx, columns)
		if err != nil {
			return nil, err
		}
		snapshot.flowIDColumn = flowIDColumn
	} else if err := s.validateRowKeyColumns(ctx, columns); err != nil {
		return nil, err
	}
	for name, column := range columns {
		if snapshot.isKeyColumn(name) {
			continue
		}
		if !column.nullable && !column.hasDefault && !column.generated {
			return nil, fmt.Errorf("column %q prevents partial row inserts", name)
		}
	}
	for mapName, reference := range s.mapTables {
		mapTable, err := s.describeMapTable(ctx, reference, s.cfg.AttributeMaps[mapName].InstanceKeyColumn, snapshot)
		if err != nil {
			return nil, fmt.Errorf("AttributeMap %q: %w", mapName, err)
		}
		snapshot.mapTables[mapName] = mapTable
	}
	return snapshot, nil
}

func (s *sqlStore) describeMapTable(
	ctx context.Context,
	reference tableReference,
	instanceKeyColumn string,
	snapshot *tableSchema,
) (*mapTableSchema, error) {
	columns, err := s.describeColumns(ctx, reference)
	if err != nil {
		return nil, err
	}
	primaryKeys, err := s.dialect.describePrimaryKeys(ctx, s.db, reference)
	if err != nil {
		return nil, err
	}
	return newMapTableSchema(reference, instanceKeyColumn, snapshot.keyColumns(), primaryKeys, columns)
}

func (s *sqlStore) validateRowKeyColumns(ctx context.Context, columns map[string]columnSchema) error {
	primaryKeys, err := s.dialect.describePrimaryKeys(ctx, s.db, s.table)
	if err != nil {
		return err
	}
	expected := s.rowKey.columns()
	if !hasSameColumns(primaryKeys, expected) {
		return fmt.Errorf("table primary key %v must be exactly the rowKey columns %v", primaryKeys, expected)
	}
	for _, name := range expected {
		if !columns[name].acceptsString() {
			return fmt.Errorf("rowKey column %q cannot store strings", name)
		}
	}
	return nil
}

func hasSameColumns(actual, expected []string) bool {
	sortedActual := slices.Sorted(slices.Values(actual))
	sortedExpected := slices.Sorted(slices.Values(expected))
	return slices.Equal(sortedActual, sortedExpected)
}

func (s *sqlStore) resolveFlowIDColumn(
	ctx context.Context,
	columns map[string]columnSchema,
) (string, error) {
	if s.cfg.Type.IsWarehouse() {
		column, found := columns[s.cfg.FlowIDColumn]
		if !found {
			return "", fmt.Errorf("flowIdColumn %q does not exist", s.cfg.FlowIDColumn)
		}
		if !column.acceptsString() || column.nullable || column.generated {
			return "", fmt.Errorf("flowIdColumn %q must be a non-null, non-generated string column", column.name)
		}
		return column.name, nil
	}
	primaryKeys, err := s.dialect.describePrimaryKeys(ctx, s.db, s.table)
	if err != nil {
		return "", err
	}
	if len(primaryKeys) != 1 {
		return "", fmt.Errorf("table must have exactly one primary-key column")
	}
	primaryKey, found := columns[primaryKeys[0]]
	if !found {
		return "", fmt.Errorf("primary-key column was not described")
	}
	if !primaryKey.acceptsString() {
		return "", fmt.Errorf("primary-key column %q cannot store FlowID strings", primaryKey.name)
	}
	return primaryKey.name, nil
}

func (s *sqlStore) describeColumns(ctx context.Context, reference tableReference) (map[string]columnSchema, error) {
	query, arguments := s.dialect.describeColumnsQuery(reference)
	rows, err := s.db.QueryContext(ctx, query, arguments...)
	if err != nil {
		return nil, fmt.Errorf("describe columns: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			s.logger.Error("close Attribute Store describe rows", tag.Error(closeErr))
		}
	}()
	columns := map[string]columnSchema{}
	for rows.Next() {
		column, scanErr := scanColumn(rows, s.cfg.Type)
		if scanErr != nil {
			return nil, scanErr
		}
		columns[column.name] = column
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate described columns: %w", err)
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("table does not exist or has no columns")
	}
	if err := s.dialect.enrichColumns(ctx, s.db, reference, columns); err != nil {
		return nil, err
	}
	return columns, nil
}

func scanColumn(rows *sql.Rows, storeType config.AttributeStoreType) (columnSchema, error) {
	var (
		column                               columnSchema
		dataType, columnType, nullable       string
		characterMaximum, precision, scale   sql.NullInt64
		defaultValue, generatedA, generatedB sql.NullString
	)
	if err := rows.Scan(
		&column.name,
		&dataType,
		&columnType,
		&nullable,
		&characterMaximum,
		&precision,
		&scale,
		&defaultValue,
		&generatedA,
		&generatedB,
	); err != nil {
		return columnSchema{}, fmt.Errorf("scan described column: %w", err)
	}
	column.dataType = strings.ToLower(dataType)
	column.columnType = strings.ToLower(columnType)
	column.nullable = strings.EqualFold(nullable, "YES")
	column.characterMaximum = nullableInt64(characterMaximum)
	column.numericPrecision = nullableInt64(precision)
	column.numericScale = nullableInt64(scale)
	column.hasDefault = defaultValue.Valid
	column.generated = isGeneratedColumn(storeType, generatedA.String, generatedB.String)
	return column, nil
}

func nullableInt64(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	return &value.Int64
}

func isGeneratedColumn(storeType config.AttributeStoreType, generatedA, generatedB string) bool {
	switch storeType {
	case config.AttributeStoreTypeSnowflake:
		return strings.EqualFold(generatedA, "YES") || generatedB != ""
	case config.AttributeStoreTypePostgres, config.AttributeStoreTypeDatabricks:
		return strings.EqualFold(generatedA, "YES") || strings.EqualFold(generatedA, "ALWAYS") ||
			strings.EqualFold(generatedB, "YES") || strings.EqualFold(generatedB, "ALWAYS")
	default:
		return generatedA != "" || generatedB != ""
	}
}

func (s *sqlStore) refreshSchema(ctx context.Context) error {
	snapshot, err := s.describe(ctx)
	if err != nil {
		return redactStorageSecret(err, s.cfg.DSN)
	}
	s.schema.Store(snapshot)
	return nil
}

func (s *sqlStore) writeBatch(
	ctx context.Context,
	flowID string,
	items []*dexpb.AttributeSyncItem,
) error {
	snapshot := s.schema.Load()
	if snapshot == nil {
		return fmt.Errorf("Attribute Store schema is unavailable")
	}
	keyValues, matches := snapshot.keyValues(flowID)
	if !matches {
		s.logger.Error("skip Attribute Store batch: Flow ID does not match rowKey", tag.WorkflowID(flowID))
		return nil
	}
	latest := make(map[string]*dexpb.Value, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		latest[item.GetKey()] = item.GetValue()
	}
	snapshot = s.snapshotWithColumns(ctx, snapshot, latest)
	statements := s.buildStatements(snapshot, keyValues, latest)
	if len(statements) == 0 {
		return nil
	}
	return redactStorageSecret(s.execute(ctx, statements), s.cfg.DSN)
}

// snapshotWithColumns returns a schema snapshot for the batch. When the batch writes a column the snapshot lacks and
// no refresh has confirmed that column missing, it refreshes the schema first; concurrent batches wait for that
// refresh instead of skipping their values. Confirmed missing columns wait for the periodic refresh.
func (s *sqlStore) snapshotWithColumns(
	ctx context.Context,
	snapshot *tableSchema,
	latest map[string]*dexpb.Value,
) *tableSchema {
	if len(s.unknownColumns(snapshot, latest)) == 0 {
		return snapshot
	}
	s.columnRefreshMutex.Lock()
	defer s.columnRefreshMutex.Unlock()
	snapshot = s.schema.Load()
	unknown := s.unknownColumns(snapshot, latest)
	now := time.Now()
	if !slices.ContainsFunc(unknown, func(column string) bool { return s.needsColumnRefresh(column, now) }) {
		return snapshot
	}
	if err := s.refreshSchema(ctx); err != nil {
		s.logger.Error("refresh Attribute Store schema for an unknown column", tag.Error(err))
		return s.schema.Load()
	}
	s.lastColumnRefresh = now
	if s.confirmedMissingColumns == nil {
		s.confirmedMissingColumns = map[string]bool{}
	}
	snapshot = s.schema.Load()
	for _, column := range s.unknownColumns(snapshot, latest) {
		_, isRemembered := s.confirmedMissingColumns[column]
		if isRemembered || len(s.confirmedMissingColumns) < maximumConfirmedMissingColumns {
			s.confirmedMissingColumns[column] = true
		}
	}
	return snapshot
}

// needsColumnRefresh reports whether an unknown column warrants a refresh: one no refresh has confirmed missing
// does, and a confirmed one never does, because the periodic refresh checks it again.
func (s *sqlStore) needsColumnRefresh(column string, now time.Time) bool {
	if _, isConfirmed := s.confirmedMissingColumns[column]; isConfirmed {
		return false
	}
	if len(s.confirmedMissingColumns) >= maximumConfirmedMissingColumns {
		return now.Sub(s.lastColumnRefresh) >= columnRefreshInterval
	}
	return true
}

// unknownColumns returns the store columns, and the AttributeMap table columns as <map>.<column>, that the batch
// writes and snapshot lacks.
func (s *sqlStore) unknownColumns(snapshot *tableSchema, latest map[string]*dexpb.Value) []string {
	var unknown []string
	for key, value := range latest {
		mapName, _, isInstanceKey := strings.Cut(key, "/")
		if mapTable, isMapTable := snapshot.mapTables[mapName]; isInstanceKey && isMapTable {
			for _, column := range mapTable.unknownColumns(value, s.cfg.ColumnNaming) {
				unknown = append(unknown, mapName+"."+column)
			}
			continue
		}
		if column := columnName(s.cfg.ColumnNaming, key); snapshot.columns[column].name == "" {
			unknown = append(unknown, column)
		}
	}
	return unknown
}

// buildStatements returns the store row's upsert, when any Attribute survives filtering, followed by one upsert or
// delete for each AttributeMap instance, ordered by map name and instance key.
func (s *sqlStore) buildStatements(
	snapshot *tableSchema,
	keyValues []string,
	latest map[string]*dexpb.Value,
) []sqlStatement {
	attributeValues := make(map[string]*dexpb.Value, len(latest))
	instanceValues := map[string]map[string]*dexpb.Value{}
	for key, value := range latest {
		mapName, escapedInstance, isInstanceKey := strings.Cut(key, "/")
		if _, isMapTable := snapshot.mapTables[mapName]; !isInstanceKey || !isMapTable {
			attributeValues[key] = value
			continue
		}
		instance, err := url.PathUnescape(escapedInstance)
		if err != nil || instance == "" {
			s.logger.Error("skip Attribute Store map instance: invalid instance key", tag.AttributeName(key))
			continue
		}
		if instanceValues[mapName] == nil {
			instanceValues[mapName] = map[string]*dexpb.Value{}
		}
		instanceValues[mapName][instance] = value
	}
	statements := make([]sqlStatement, 0, 1+len(instanceValues))
	if filtered := s.filterValues(snapshot, attributeValues); len(filtered) > 0 {
		query, arguments := s.buildUpsert(snapshot, keyValues, filtered)
		statements = append(statements, sqlStatement{query: query, arguments: arguments})
	}
	for _, mapName := range slices.Sorted(maps.Keys(instanceValues)) {
		mapTable := snapshot.mapTables[mapName]
		for _, instance := range slices.Sorted(maps.Keys(instanceValues[mapName])) {
			statements = append(statements, s.buildInstanceStatement(
				mapTable, mapName, append(slices.Clone(keyValues), instance), instanceValues[mapName][instance],
			))
		}
	}
	return statements
}

func (s *sqlStore) buildInstanceStatement(
	mapTable *mapTableSchema,
	mapName string,
	keyValues []string,
	value *dexpb.Value,
) sqlStatement {
	if _, isDelete := value.GetKind().(*dexpb.Value_NullValue); isDelete {
		query, arguments := s.dialect.buildKeyedDelete(mapTable.reference, mapTable.keyColumns, keyValues)
		return sqlStatement{query: query, arguments: arguments}
	}
	key := mapName + "/" + url.PathEscape(keyValues[len(keyValues)-1])
	columns := mapTable.filterInstanceValue(key, keyValues, value, s.cfg.ColumnNaming, s.cfg.Type, s.logger)
	for _, field := range columns.missingColumnFields {
		s.warnMissingColumnOnce(mapName + "." + field)
	}
	query, arguments := s.dialect.buildKeyedUpsert(
		mapTable.reference, mapTable.keyColumns, keyValues, mapTable.replacedColumns, columns.values,
	)
	return sqlStatement{query: query, arguments: arguments}
}

// warnMissingColumnOnce logs a field without a column once; dynamically keyed values stop new warnings at the cap.
func (s *sqlStore) warnMissingColumnOnce(field string) {
	if s.missingColumnWarningCount.Load() >= maximumMissingColumnWarnings {
		return
	}
	if _, isWarned := s.missingColumnWarnings.LoadOrStore(field, true); isWarned {
		return
	}
	s.missingColumnWarningCount.Add(1)
	s.logger.Warn("skip Attribute Store map instance field without a column; later writes skip it silently",
		tag.AttributeName(field))
}

// execute runs one statement directly, and several statements in one transaction.
func (s *sqlStore) execute(ctx context.Context, statements []sqlStatement) (err error) {
	if len(statements) == 1 {
		if _, err := s.db.ExecContext(ctx, statements[0].query, statements[0].arguments...); err != nil {
			return fmt.Errorf("execute Attribute Store upsert: %w", err)
		}
		return nil
	}
	transaction, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin Attribute Store transaction: %w", err)
	}
	defer func() {
		if err == nil {
			return
		}
		if rollbackErr := transaction.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, fmt.Errorf("roll back Attribute Store transaction: %w", rollbackErr))
		}
	}()
	for _, statement := range statements {
		if _, err := transaction.ExecContext(ctx, statement.query, statement.arguments...); err != nil {
			return fmt.Errorf("execute Attribute Store batch: %w", err)
		}
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit Attribute Store transaction: %w", err)
	}
	return nil
}

func (s *sqlStore) filterValues(
	snapshot *tableSchema,
	latest map[string]*dexpb.Value,
) map[string]filteredValue {
	attributeNamesByColumn := make(map[string][]string, len(latest))
	for name := range latest {
		column := columnName(s.cfg.ColumnNaming, name)
		attributeNamesByColumn[column] = append(attributeNamesByColumn[column], name)
	}
	filtered := make(map[string]filteredValue, len(attributeNamesByColumn))
	for name, attributeNames := range attributeNamesByColumn {
		if len(attributeNames) > 1 {
			sort.Strings(attributeNames)
			s.logger.Error("skip Attribute Store items: Attribute names share one column",
				tag.AttributeName(strings.Join(attributeNames, ", ")))
			continue
		}
		column, found := snapshot.columns[name]
		if !found {
			s.logger.Error("skip Attribute Store item: column does not exist", tag.AttributeName(attributeNames[0]))
			continue
		}
		if snapshot.isKeyColumn(name) {
			s.logger.Error("skip Attribute Store item: Flow ID column is immutable", tag.AttributeName(attributeNames[0]))
			continue
		}
		converted, err := column.convert(latest[attributeNames[0]], s.cfg.Type)
		if err != nil {
			s.logger.Error("skip incompatible Attribute Store item", tag.AttributeName(attributeNames[0]), tag.Error(err))
			continue
		}
		filtered[name] = filteredValue{column: column, value: converted}
	}
	return filtered
}

func (s *sqlStore) buildUpsert(
	snapshot *tableSchema,
	keyValues []string,
	values map[string]filteredValue,
) (string, []any) {
	columnNames := make([]string, 0, len(values))
	for name := range values {
		columnNames = append(columnNames, name)
	}
	sort.Strings(columnNames)
	return s.dialect.buildUpsert(snapshot, keyValues, columnNames, values)
}

// deleteTenantRows removes a tenant's rows from the store table and its AttributeMap tables in one transaction.
func (s *sqlStore) deleteTenantRows(ctx context.Context, tenantID string) error {
	if s.cfg.TenantColumn == "" {
		return nil
	}
	statements := []sqlStatement{s.tenantDeleteStatement(s.table, tenantID)}
	for _, reference := range s.mapTables {
		statements = append(statements, s.tenantDeleteStatement(reference, tenantID))
	}
	return s.execute(ctx, statements)
}

func (s *sqlStore) tenantDeleteStatement(reference tableReference, tenantID string) sqlStatement {
	query, arguments := s.dialect.buildKeyedDelete(reference, []string{s.cfg.TenantColumn}, []string{tenantID})
	return sqlStatement{query: query, arguments: arguments}
}

func (s *sqlStore) close() error {
	return redactStorageSecret(s.db.Close(), s.cfg.DSN)
}

// keyColumns returns the primary-key columns in rowKey order, or the single Flow ID column.
func (t *tableSchema) keyColumns() []string {
	if t.rowKey == nil {
		return []string{t.flowIDColumn}
	}
	return t.rowKey.columns()
}

// keyValues returns the value of each key column for flowID, and false when flowID does not match the rowKey.
func (t *tableSchema) keyValues(flowID string) ([]string, bool) {
	if t.rowKey == nil {
		return []string{flowID}, true
	}
	return t.rowKey.match(flowID)
}

func (t *tableSchema) isKeyColumn(name string) bool {
	return slices.Contains(t.keyColumns(), name)
}

// effectiveRowKey prepends the tenant column to the row key of a tenant-scoped store, whose rows are keyed by the
// stored Flow ID T:<tenant>:<application Flow ID>.
func effectiveRowKey(cfg config.AttributeStoreConfigEntry) string {
	if cfg.TenantColumn == "" {
		return cfg.RowKey
	}
	return service.TenantStoredFlowID("{"+cfg.TenantColumn+"}", cfg.RowKey)
}
