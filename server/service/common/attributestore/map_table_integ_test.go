// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//go:build attributestore_integration

package attributestore

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service/common/log"
)

func TestMySQLAndPostgresAttributeStoreMapTablesIntegration(t *testing.T) {
	postgresDSN := environmentOrDefault(
		"DEX_ATTRIBUTE_STORE_POSTGRES_DSN",
		"postgres://dex:dex@127.0.0.1:55432/dex?sslmode=disable",
	)
	mysqlDSN := environmentOrDefault(
		"DEX_ATTRIBUTE_STORE_MYSQL_DSN",
		"dex:dex@tcp(127.0.0.1:53306)/dex?parseTime=true",
	)
	for _, destination := range []rowKeyIntegrationDatabase{
		{config.AttributeStoreTypePostgres, postgresDSN, openIntegrationDatabase(t, "pgx", postgresDSN), "TEXT"},
		{config.AttributeStoreTypeMySQL, mysqlDSN, openIntegrationDatabase(t, "mysql", mysqlDSN), "VARCHAR(64)"},
	} {
		t.Run(string(destination.storeType), func(t *testing.T) {
			key := destination.textType
			timestampType, doubleType, jsonType := "TIMESTAMPTZ", "DOUBLE PRECISION", "JSONB"
			if destination.storeType == config.AttributeStoreTypeMySQL {
				timestampType, doubleType, jsonType = "TIMESTAMP NULL", "DOUBLE", "JSON"
			}
			destination.exec(t,
				`DROP TABLE IF EXISTS map_groups`,
				`CREATE TABLE map_groups (group_id `+key+` PRIMARY KEY, display_name TEXT)`,
				`DROP TABLE IF EXISTS map_group_members`,
				`CREATE TABLE map_group_members (group_id `+key+`, account_id `+key+`, role TEXT,
added_at `+timestampType+`, added_by_account_id TEXT, score `+doubleType+`, profile `+jsonType+`,
PRIMARY KEY (group_id, account_id))`,
				`DROP TABLE IF EXISTS map_group_labels`,
				`CREATE TABLE map_group_labels (group_id `+key+`, label_key `+key+`, value TEXT,
PRIMARY KEY (group_id, label_key))`,
				`DROP TABLE IF EXISTS map_plain`,
				`CREATE TABLE map_plain (flow_id `+key+` PRIMARY KEY, note TEXT)`,
				`DROP TABLE IF EXISTS map_plain_items`,
				`CREATE TABLE map_plain_items (flow_id `+key+`, item_id `+key+`, value TEXT, PRIMARY KEY (flow_id, item_id))`,
			)
			assertMapTableStartupFailures(t, destination)
			manager, err := NewManager(context.Background(), &config.AttributeStoreConfig{
				Stores: map[string]config.AttributeStoreConfigEntry{
					"groups": {
						Type: destination.storeType, DSN: destination.dsn, TableName: "map_groups",
						RowKey: "group-{group_id}", ColumnNaming: config.AttributeStoreColumnNamingSnakeCase,
						AttributeMaps: map[string]config.AttributeStoreMapTableConfig{
							"Members": {TableName: "map_group_members", InstanceKeyColumn: "account_id"},
							"Labels":  {TableName: "map_group_labels", InstanceKeyColumn: "label_key"},
						},
					},
					"plain": {
						Type: destination.storeType, DSN: destination.dsn, TableName: "map_plain",
						AttributeMaps: map[string]config.AttributeStoreMapTableConfig{
							"Items": {TableName: "map_plain_items", InstanceKeyColumn: "item_id"},
						},
					},
				},
			}, log.NewNoop())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, manager.Close()) })

			writeRowKeyBatch(t, manager, "groups", "group-g1",
				"DisplayName", stringValue("Core"),
				"Members/account%2F1", objectValue("json", `{"Role":"ADMIN","AddedAt":"2026-08-11T15:30:00Z",
"AddedByAccountID":"account/1","Score":3,"Profile":{"tags":["a"]},"Unknown":"skipped","GroupID":"other"}`),
				"Members/account-2", objectValue("json", `{"Role":"MEMBER","AddedAt":"2026-08-11T15:31:00Z","Score":2.5}`),
				"Labels/color", stringValue("blue"),
			)
			require.Equal(t, "Core", destination.groupDisplayName(t))
			first := destination.member(t, "account/1")
			require.Equal(t, "ADMIN", first.role.String)
			require.Equal(t, time.Date(2026, 8, 11, 15, 30, 0, 0, time.UTC), first.addedAt.Time.UTC())
			require.Equal(t, "account/1", first.addedBy.String)
			require.Equal(t, 3.0, first.score.Float64)
			require.JSONEq(t, `{"tags":["a"]}`, first.profile.String)
			require.Equal(t, 2.5, destination.member(t, "account-2").score.Float64)
			require.Equal(t, []string{"color=blue"}, destination.labels(t))

			writeRowKeyBatch(t, manager, "groups", "group-g1",
				"Members/account%2F1", nullValue(),
				"Members/account-2", objectValue("json", `{"Role":"ADMIN"}`),
				"Members/account-3", objectValue("json", `{}`),
				"Members/%ZZ", objectValue("json", `{"Role":"INVALID"}`),
				"Labels/color", nullValue(),
			)
			require.Equal(t, []string{"account-2", "account-3"}, destination.memberIDs(t))
			second := destination.member(t, "account-2")
			require.Equal(t, "ADMIN", second.role.String)
			require.Equal(t, 2.5, second.score.Float64)
			require.False(t, destination.member(t, "account-3").role.Valid)
			require.Empty(t, destination.labels(t))

			writeRowKeyBatch(t, manager, "groups", "group-g1",
				"Members/account-3", objectValue("json", `{}`),
				"Members/account-4", objectValue("json", `{"Role":"MEMBER"}`),
			)
			require.Equal(t, []string{"account-2", "account-3", "account-4"}, destination.memberIDs(t))
			require.Equal(t, "Core", destination.groupDisplayName(t))

			destination.exec(t, `ALTER TABLE map_group_members DROP COLUMN added_by_account_id`)
			err = manager.WriteBatch(context.Background(), &dexpb.SyncAttributeBatchActivityInput{
				FlowId: "group-g1", ConfigName: "groups", Items: []*dexpb.AttributeSyncItem{
					{ConfigName: "groups", Key: "DisplayName", Value: stringValue("Rolled back")},
					{ConfigName: "groups", Key: "Members/account-5", Value: objectValue("json", `{"AddedByAccountID":"x"}`)},
				},
			})
			require.Error(t, err)
			require.Equal(t, "Core", destination.groupDisplayName(t))
			require.Equal(t, []string{"account-2", "account-3", "account-4"}, destination.memberIDs(t))
			destination.exec(t, `ALTER TABLE map_group_members ADD COLUMN added_by_account_id TEXT`)

			writeRowKeyBatch(t, manager, "plain", "plain-flow-1",
				"note", stringValue("n"),
				"Items/i1", stringValue("v1"),
				"Members/account-9", objectValue("json", `{"Role":"ADMIN"}`),
			)
			var itemValue string
			require.NoError(t, destination.database.QueryRowContext(context.Background(),
				`SELECT value FROM map_plain_items WHERE flow_id = `+destination.placeholder(1)+` AND item_id = `+
					destination.placeholder(2), "plain-flow-1", "i1").Scan(&itemValue))
			require.Equal(t, "v1", itemValue)
			require.Equal(t, 1, destination.count(t, "map_plain"))
			require.Equal(t, []string{"account-2", "account-3", "account-4"}, destination.memberIDs(t))
		})
	}
}

func assertMapTableStartupFailures(t *testing.T, destination rowKeyIntegrationDatabase) {
	t.Helper()
	key := destination.textType
	destination.exec(t,
		`DROP TABLE IF EXISTS map_bad_key`,
		`CREATE TABLE map_bad_key (account_id `+key+` PRIMARY KEY, role TEXT)`,
		`DROP TABLE IF EXISTS map_integer_key`,
		`CREATE TABLE map_integer_key (group_id `+key+`, account_id BIGINT, PRIMARY KEY (group_id, account_id))`,
		`DROP TABLE IF EXISTS map_required_column`,
		`CREATE TABLE map_required_column (group_id `+key+`, account_id `+key+`, role TEXT NOT NULL,
PRIMARY KEY (group_id, account_id))`,
	)
	failures := []struct {
		mapName string
		table   config.AttributeStoreMapTableConfig
		message string
	}{
		{"Members", config.AttributeStoreMapTableConfig{TableName: "map_missing", InstanceKeyColumn: "account_id"}, "does not exist"},
		{"Members", config.AttributeStoreMapTableConfig{TableName: "map_bad_key", InstanceKeyColumn: "account_id"}, "must be exactly the columns"},
		{"Members", config.AttributeStoreMapTableConfig{TableName: "map_integer_key", InstanceKeyColumn: "account_id"}, "cannot store strings"},
		{"Members", config.AttributeStoreMapTableConfig{TableName: "map_required_column", InstanceKeyColumn: "account_id"}, "prevents partial row inserts"},
		{"Members", config.AttributeStoreMapTableConfig{TableName: "map_group_members", InstanceKeyColumn: "group_id"}, "also a primary-key column"},
		{"Members", config.AttributeStoreMapTableConfig{InstanceKeyColumn: "account_id"}, "requires tableName"},
		{"Members/All", config.AttributeStoreMapTableConfig{TableName: "map_group_members", InstanceKeyColumn: "account_id"}, "contain no '/'"},
	}
	for _, failure := range failures {
		_, err := NewManager(context.Background(), &config.AttributeStoreConfig{
			Stores: map[string]config.AttributeStoreConfigEntry{
				"invalid": {
					Type: destination.storeType, DSN: destination.dsn, TableName: "map_groups", RowKey: "group-{group_id}",
					AttributeMaps: map[string]config.AttributeStoreMapTableConfig{failure.mapName: failure.table},
				},
			},
		}, log.NewNoop())
		require.ErrorContains(t, err, failure.message, failure.table.TableName)
	}
	attributeMaps := map[string]config.AttributeStoreMapTableConfig{
		"Members": {TableName: "a.b.c", InstanceKeyColumn: "account_id"},
	}
	for _, store := range []config.AttributeStoreConfigEntry{
		{Type: config.AttributeStoreTypeSnowflake, DSN: "unused", TableName: "a.b.c", FlowIDColumn: "flow_id", AttributeMaps: attributeMaps},
		{Type: config.AttributeStoreTypeMongoDB, DSN: "unused", DatabaseName: "dex", CollectionName: "c", AttributeMaps: attributeMaps},
	} {
		_, err := NewManager(context.Background(), &config.AttributeStoreConfig{
			Stores: map[string]config.AttributeStoreConfigEntry{"invalid": store},
		}, log.NewNoop())
		require.ErrorContains(t, err, "attributeMaps", string(store.Type))
	}
}

type memberRow struct {
	role, addedBy, profile sql.NullString
	addedAt                sql.NullTime
	score                  sql.NullFloat64
}

func (d rowKeyIntegrationDatabase) member(t *testing.T, accountID string) memberRow {
	t.Helper()
	var row memberRow
	var profile []byte
	require.NoError(t, d.database.QueryRowContext(context.Background(),
		`SELECT role, added_at, added_by_account_id, score, profile FROM map_group_members
WHERE group_id = `+d.placeholder(1)+` AND account_id = `+d.placeholder(2), "g1", accountID,
	).Scan(&row.role, &row.addedAt, &row.addedBy, &row.score, &profile))
	row.profile = sql.NullString{String: string(profile), Valid: profile != nil}
	return row
}

func (d rowKeyIntegrationDatabase) memberIDs(t *testing.T) []string {
	t.Helper()
	return d.strings(t, `SELECT account_id FROM map_group_members WHERE group_id = `+d.placeholder(1)+
		` ORDER BY account_id`, "g1")
}

func (d rowKeyIntegrationDatabase) labels(t *testing.T) []string {
	t.Helper()
	return d.strings(t, `SELECT CONCAT(label_key, '=', value) FROM map_group_labels WHERE group_id = `+
		d.placeholder(1)+` ORDER BY label_key`, "g1")
}

func (d rowKeyIntegrationDatabase) groupDisplayName(t *testing.T) string {
	t.Helper()
	var name string
	require.NoError(t, d.database.QueryRowContext(context.Background(),
		`SELECT display_name FROM map_groups WHERE group_id = `+d.placeholder(1), "g1").Scan(&name))
	return name
}

func (d rowKeyIntegrationDatabase) strings(t *testing.T, query string, arguments ...any) []string {
	t.Helper()
	rows, err := d.database.QueryContext(context.Background(), query, arguments...)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	values := []string{}
	for rows.Next() {
		var value string
		require.NoError(t, rows.Scan(&value))
		values = append(values, value)
	}
	require.NoError(t, rows.Err())
	return values
}
