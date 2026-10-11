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
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service/common/log"
)

type rowKeyIntegrationDatabase struct {
	storeType config.AttributeStoreType
	dsn       string
	database  *sql.DB
	textType  string
}

func TestMySQLAndPostgresAttributeStoreRowKeysIntegration(t *testing.T) {
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
			destination.exec(t,
				`DROP TABLE IF EXISTS row_key_orders`,
				`CREATE TABLE row_key_orders (customer_id `+destination.textType+`, order_id `+destination.textType+`,
order_status TEXT, shipping_url TEXT, s3_bucket TEXT, url_path TEXT, item_count BIGINT,
PRIMARY KEY (customer_id, order_id))`,
				`DROP TABLE IF EXISTS row_key_customers`,
				`CREATE TABLE row_key_customers (customer_id `+destination.textType+` PRIMARY KEY,
display_name TEXT, onboarding_status TEXT)`,
			)
			assertRowKeyStartupFailures(t, destination)
			manager, err := NewManager(context.Background(), &config.AttributeStoreConfig{
				Stores: map[string]config.AttributeStoreConfigEntry{
					"orders": {
						Type: destination.storeType, DSN: destination.dsn, TableName: "row_key_orders",
						RowKey: "order-{customer_id}-{order_id}", ColumnNaming: config.AttributeStoreColumnNamingSnakeCase,
					},
					"customers": {
						Type: destination.storeType, DSN: destination.dsn, TableName: "row_key_customers",
						RowKey: "customer-{customer_id}", ColumnNaming: config.AttributeStoreColumnNamingSnakeCase,
					},
					"onboarding": {
						Type: destination.storeType, DSN: destination.dsn, TableName: "row_key_customers",
						RowKey: "onboarding-{customer_id}-{*}", ColumnNaming: config.AttributeStoreColumnNamingSnakeCase,
					},
				},
			}, log.NewNoop())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, manager.Close()) })

			writeRowKeyBatch(t, manager, "orders", "order-c1-o-2-x",
				"OrderStatus", stringValue("PLACED"),
				"ShippingURL", stringValue("https://example.com/c1"),
				"S3Bucket", stringValue("bucket"),
				"URLPath", stringValue("/orders"),
				"ItemCount", intValue(3),
			)
			require.Equal(t,
				[]string{"PLACED", "https://example.com/c1", "bucket", "/orders", "3"},
				destination.orderRow(t, "c1", "o-2-x"),
			)

			writeRowKeyBatch(t, manager, "orders", "order-c1-o-2-x",
				"CustomerID", stringValue("c2"),
				"ShippingURL", stringValue("https://example.com/ignored"),
				"shipping_url", stringValue("https://example.com/also-ignored"),
				"OrderStatus", stringValue("SHIPPED"),
			)
			require.Equal(t,
				[]string{"SHIPPED", "https://example.com/c1", "bucket", "/orders", "3"},
				destination.orderRow(t, "c1", "o-2-x"),
			)

			writeRowKeyBatch(t, manager, "orders", "invoice-c1-o-3", "OrderStatus", stringValue("PLACED"))
			writeRowKeyBatch(t, manager, "orders", "order-c1-", "OrderStatus", stringValue("PLACED"))
			require.Equal(t, 1, destination.count(t, "row_key_orders"))

			writeRowKeyBatch(t, manager, "customers", "customer-c1", "DisplayName", stringValue("Ada"))
			writeRowKeyBatch(t, manager, "onboarding", "onboarding-c1-request-7", "OnboardingStatus", stringValue("READY"))
			writeRowKeyBatch(t, manager, "customers", "customer-c1", "DisplayName", stringValue("Ada Lovelace"))
			var displayName, onboardingStatus string
			require.NoError(t, destination.database.QueryRowContext(context.Background(),
				`SELECT display_name, onboarding_status FROM row_key_customers WHERE customer_id = `+
					destination.placeholder(1), "c1").Scan(&displayName, &onboardingStatus))
			require.Equal(t, "Ada Lovelace", displayName)
			require.Equal(t, "READY", onboardingStatus)
		})
	}
}

func assertRowKeyStartupFailures(t *testing.T, destination rowKeyIntegrationDatabase) {
	t.Helper()
	destination.exec(t,
		`DROP TABLE IF EXISTS row_key_integer_key`,
		`CREATE TABLE row_key_integer_key (customer_id BIGINT, order_id `+destination.textType+`,
PRIMARY KEY (customer_id, order_id))`,
	)
	failures := []struct {
		tableName    string
		rowKey       string
		columnNaming config.AttributeStoreColumnNaming
		message      string
	}{
		{"row_key_customers", "order-{customer_id}-{order_id}", "", "must be exactly the rowKey columns"},
		{"row_key_orders", "order-{customer_id}", "", "must be exactly the rowKey columns"},
		{"row_key_integer_key", "order-{customer_id}-{order_id}", "", "cannot store strings"},
		{"row_key_orders", "order-{customer_id}{order_id}", "", "separated by literal text"},
		{"row_key_orders", "order-{customer_id", "", "unterminated placeholder"},
		{"row_key_orders", "order-{customer_id}-{customer_id}", "", "more than once"},
		{"row_key_orders", "order-{*}", "", "at least one column"},
		{"row_key_orders", "order-}{customer_id}", "", "unmatched"},
		{"row_key_orders", "order-{}-{order_id}", "", "empty placeholder"},
		{"row_key_orders", "order-{customer_id}-{order_id}", "camelCase", "unsupported columnNaming"},
	}
	for _, failure := range failures {
		_, err := NewManager(context.Background(), &config.AttributeStoreConfig{
			Stores: map[string]config.AttributeStoreConfigEntry{
				"invalid": {
					Type: destination.storeType, DSN: destination.dsn, TableName: failure.tableName,
					RowKey: failure.rowKey, ColumnNaming: failure.columnNaming,
				},
			},
		}, log.NewNoop())
		require.ErrorContains(t, err, failure.message, failure.rowKey)
	}
	for _, store := range []config.AttributeStoreConfigEntry{
		{Type: config.AttributeStoreTypeSnowflake, DSN: "unused", TableName: "a.b.c", FlowIDColumn: "flow_id", RowKey: "order-{order_id}"},
		{Type: config.AttributeStoreTypeDatabricks, DSN: "unused", TableName: "a.b.c", FlowIDColumn: "flow_id", ColumnNaming: config.AttributeStoreColumnNamingSnakeCase},
		{Type: config.AttributeStoreTypeMongoDB, DSN: "unused", DatabaseName: "dex", CollectionName: "c", RowKey: "order-{order_id}"},
	} {
		_, err := NewManager(context.Background(), &config.AttributeStoreConfig{
			Stores: map[string]config.AttributeStoreConfigEntry{"invalid": store},
		}, log.NewNoop())
		require.ErrorContains(t, err, "rowKey", string(store.Type))
	}
}

func writeRowKeyBatch(t *testing.T, manager *Manager, configName, flowID string, keysAndValues ...any) {
	t.Helper()
	items := make([]*dexpb.AttributeSyncItem, 0, len(keysAndValues)/2)
	for index := 0; index < len(keysAndValues); index += 2 {
		items = append(items, &dexpb.AttributeSyncItem{
			ConfigName: configName,
			Key:        keysAndValues[index].(string),
			Value:      keysAndValues[index+1].(*dexpb.Value),
		})
	}
	require.NoError(t, manager.WriteBatch(context.Background(), &dexpb.SyncAttributeBatchActivityInput{
		FlowId: flowID, ConfigName: configName, Items: items,
	}))
}

func (d rowKeyIntegrationDatabase) exec(t *testing.T, statements ...string) {
	t.Helper()
	for _, statement := range statements {
		_, err := d.database.ExecContext(context.Background(), statement)
		require.NoError(t, err, statement)
	}
}

func (d rowKeyIntegrationDatabase) orderRow(t *testing.T, customerID, orderID string) []string {
	t.Helper()
	var status, shippingURL, bucket, path string
	var itemCount int64
	require.NoError(t, d.database.QueryRowContext(context.Background(),
		`SELECT order_status, shipping_url, s3_bucket, url_path, item_count FROM row_key_orders
WHERE customer_id = `+d.placeholder(1)+` AND order_id = `+d.placeholder(2), customerID, orderID,
	).Scan(&status, &shippingURL, &bucket, &path, &itemCount))
	return []string{status, shippingURL, bucket, path, strconv.FormatInt(itemCount, 10)}
}

func (d rowKeyIntegrationDatabase) count(t *testing.T, tableName string) int {
	t.Helper()
	var count int
	require.NoError(t, d.database.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM "+tableName).Scan(&count))
	return count
}

func (d rowKeyIntegrationDatabase) placeholder(position int) string {
	if d.storeType == config.AttributeStoreTypePostgres {
		return "$" + strconv.Itoa(position)
	}
	return "?"
}
