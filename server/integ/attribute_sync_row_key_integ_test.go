// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//go:build attributestore_integration

package integ

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/integ/workflow/signal"
	"github.com/superdurable/dex/service"
)

func TestAttributeSyncRowKeysTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	doTestAttributeSyncRowKeys(t, service.BackendTypeTemporal)
}

func TestAttributeSyncRowKeysCadence(t *testing.T) {
	if !*cadenceIntegTest {
		t.Skip()
	}
	doTestAttributeSyncRowKeys(t, service.BackendTypeCadence)
}

func doTestAttributeSyncRowKeys(t *testing.T, backendType service.BackendType) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	postgresDSN := os.Getenv("DEX_ATTRIBUTE_STORE_POSTGRES_DSN")
	if postgresDSN == "" {
		postgresDSN = "postgres://dex:dex@127.0.0.1:55432/dex?sslmode=disable"
	}
	database, err := sql.Open("pgx", postgresDSN)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })
	suffix := strings.ReplaceAll(newRequestID(), "-", "")
	ordersTable := "row_key_orders_" + suffix
	customersTable := "row_key_customers_" + suffix
	for _, statement := range []string{
		`CREATE TABLE public.` + ordersTable + ` (customer_id TEXT, order_id TEXT, order_status TEXT,
shipping_url TEXT, PRIMARY KEY (customer_id, order_id))`,
		`CREATE TABLE public.` + customersTable + ` (customer_id TEXT PRIMARY KEY, display_name TEXT,
onboarding_status TEXT)`,
	} {
		_, err = database.ExecContext(ctx, statement)
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		for _, tableName := range []string{ordersTable, customersTable} {
			_, dropErr := database.ExecContext(context.Background(), "DROP TABLE IF EXISTS public."+tableName)
			require.NoError(t, dropErr)
		}
	})

	runtime := startDexService(t, DexServiceTestConfig{
		BackendType:        backendType,
		S3TestThreshold:    1,
		BlobCacheDirectory: t.TempDir(),
		AttributeStore: config.AttributeStoreConfig{
			Stores: map[string]config.AttributeStoreConfigEntry{
				"orders": {
					Type: config.AttributeStoreTypePostgres, DSN: postgresDSN, TableName: "public." + ordersTable,
					RowKey: "order-{customer_id}-{order_id}", ColumnNaming: config.AttributeStoreColumnNamingSnakeCase,
				},
				"customers": {
					Type: config.AttributeStoreTypePostgres, DSN: postgresDSN, TableName: "public." + customersTable,
					RowKey: "customer-{customer_id}", ColumnNaming: config.AttributeStoreColumnNamingSnakeCase,
				},
				"onboarding": {
					Type: config.AttributeStoreTypePostgres, DSN: postgresDSN, TableName: "public." + customersTable,
					RowKey: "onboarding-{customer_id}-{*}", ColumnNaming: config.AttributeStoreColumnNamingSnakeCase,
				},
			},
		},
	})
	workerTarget := startWorker(t, signal.NewHandler())
	customerID := "c" + suffix
	orderFlowID := "order-" + customerID + "-o-" + suffix
	flows := []struct {
		flowID     string
		storeName  string
		attributes []*dexpb.AttributeWrite
	}{
		{orderFlowID, "orders", []*dexpb.AttributeWrite{
			syncedStringAttribute("OrderStatus", "PLACED"),
			syncedStringAttribute("ShippingURL", "https://example.com/"+strings.Repeat("long-path/", 8)),
		}},
		{"customer-" + customerID, "customers", []*dexpb.AttributeWrite{
			syncedStringAttribute("DisplayName", "Ada"),
		}},
		{"onboarding-" + customerID + "-request-" + suffix, "onboarding", []*dexpb.AttributeWrite{
			syncedStringAttribute("OnboardingStatus", "READY"),
		}},
		{"invoice-" + customerID, "orders", []*dexpb.AttributeWrite{
			syncedStringAttribute("OrderStatus", "UNMATCHED"),
		}},
	}
	for _, flow := range flows {
		_, err = runtime.FlowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
			RequestId:          newRequestID(),
			FlowId:             flow.flowID,
			FlowType:           "attribute-sync",
			FlowTimeoutSeconds: 45,
			FlowStartOptions: &dexpb.FlowStartOptions{
				Attributes: flow.attributes,
				FlowConfigOverride: &dexpb.FlowConfig{
					AttributeStoreNames: &dexpb.AttributeStoreNames{Names: []string{flow.storeName}},
					WorkerTarget:        workerTarget,
				},
			},
		})
		require.NoError(t, err)
	}
	_, err = runtime.FlowClient.SetAttributes(ctx, &dexpb.SetAttributesRequest{
		RequestId: newRequestID(),
		FlowId:    orderFlowID,
		Attributes: []*dexpb.AttributeWrite{
			syncedStringAttribute("OrderStatus", "SHIPPED"),
			syncedStringAttribute("CustomerID", "replacement"),
		},
	})
	require.NoError(t, err)
	for _, flow := range flows {
		_, err = runtime.FlowClient.StopFlow(ctx, &dexpb.StopFlowRequest{
			FlowId:   flow.flowID,
			StopType: dexpb.StopType_STOP_TYPE_CANCEL,
		})
		require.NoError(t, err)
		response, waitErr := runtime.FlowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flow.flowID})
		require.NoError(t, waitErr)
		require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_CANCELED, response.GetFlowStatus())
	}

	var orderStatus, shippingURL string
	require.NoError(t, database.QueryRowContext(ctx,
		"SELECT order_status, shipping_url FROM public."+ordersTable+" WHERE customer_id = $1 AND order_id = $2",
		customerID, "o-"+suffix,
	).Scan(&orderStatus, &shippingURL))
	require.Equal(t, "SHIPPED", orderStatus)
	require.Equal(t, "https://example.com/"+strings.Repeat("long-path/", 8), shippingURL)
	var orderCount int
	require.NoError(t, database.QueryRowContext(ctx, "SELECT COUNT(*) FROM public."+ordersTable).Scan(&orderCount))
	require.Equal(t, 1, orderCount)
	var displayName, onboardingStatus string
	require.NoError(t, database.QueryRowContext(ctx,
		"SELECT display_name, onboarding_status FROM public."+customersTable+" WHERE customer_id = $1",
		customerID,
	).Scan(&displayName, &onboardingStatus))
	require.Equal(t, "Ada", displayName)
	require.Equal(t, "READY", onboardingStatus)
}
