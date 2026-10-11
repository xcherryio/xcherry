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
	"github.com/superdurable/dex/service/common/log"
)

func TestMySQLAndPostgresAttributeStoreColumnRefreshIntegration(t *testing.T) {
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
			destination.exec(t,
				`DROP TABLE IF EXISTS refresh_orders`,
				`CREATE TABLE refresh_orders (order_id `+key+` PRIMARY KEY, status TEXT)`,
				`DROP TABLE IF EXISTS refresh_order_items`,
				`CREATE TABLE refresh_order_items (order_id `+key+`, item_id `+key+`, PRIMARY KEY (order_id, item_id))`,
			)
			manager, err := NewManager(context.Background(), &config.AttributeStoreConfig{
				SchemaSyncInterval: time.Hour,
				Stores: map[string]config.AttributeStoreConfigEntry{
					"orders": {
						Type: destination.storeType, DSN: destination.dsn, TableName: "refresh_orders",
						ColumnNaming: config.AttributeStoreColumnNamingSnakeCase,
						AttributeMaps: map[string]config.AttributeStoreMapTableConfig{
							"Items": {TableName: "refresh_order_items", InstanceKeyColumn: "item_id"},
						},
					},
				},
			}, log.NewNoop())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, manager.Close()) })
			writeRowKeyBatch(t, manager, "orders", "o1", "Status", stringValue("PLACED"))

			destination.exec(t,
				`ALTER TABLE refresh_orders ADD COLUMN shipping_note TEXT`,
				`ALTER TABLE refresh_order_items ADD COLUMN quantity BIGINT`,
			)
			writeRowKeyBatch(t, manager, "orders", "o1",
				"ShippingNote", stringValue("leave at door"),
				"Items/i1", objectValue("json", `{"Quantity":2}`),
			)
			var note string
			require.NoError(t, destination.database.QueryRowContext(context.Background(),
				`SELECT shipping_note FROM refresh_orders WHERE order_id = `+destination.placeholder(1), "o1",
			).Scan(&note))
			require.Equal(t, "leave at door", note)
			var quantity int64
			require.NoError(t, destination.database.QueryRowContext(context.Background(),
				`SELECT quantity FROM refresh_order_items WHERE order_id = `+destination.placeholder(1)+
					` AND item_id = `+destination.placeholder(2), "o1", "i1",
			).Scan(&quantity))
			require.Equal(t, int64(2), quantity)

			destination.exec(t, `ALTER TABLE refresh_orders ADD COLUMN gift_message TEXT`)
			writeRowKeyBatch(t, manager, "orders", "o1", "GiftMessage", stringValue("rate limited"))
			var giftMessage sql.NullString
			require.NoError(t, destination.database.QueryRowContext(context.Background(),
				`SELECT gift_message FROM refresh_orders WHERE order_id = `+destination.placeholder(1), "o1",
			).Scan(&giftMessage))
			require.False(t, giftMessage.Valid)
		})
	}
}
