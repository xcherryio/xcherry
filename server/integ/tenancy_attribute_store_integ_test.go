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
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAttributeSyncTenantScopedStoreTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	postgresDSN := os.Getenv("DEX_ATTRIBUTE_STORE_POSTGRES_DSN")
	if postgresDSN == "" {
		postgresDSN = "postgres://dex:dex@127.0.0.1:55432/dex?sslmode=disable"
	}
	database, err := sql.Open("pgx", postgresDSN)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })
	tableSuffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	tables := map[string]string{
		"reporting": "tenant_attributes_" + tableSuffix,
		"other":     "tenant_attributes_other_" + tableSuffix,
	}
	stores := map[string]config.AttributeStoreConfigEntry{}
	for storeName, tableName := range tables {
		_, err = database.ExecContext(ctx, fmt.Sprintf(
			`CREATE TABLE public.%s (tenant_id TEXT, flow_id TEXT, message TEXT, PRIMARY KEY (tenant_id, flow_id))`,
			tableName,
		))
		require.NoError(t, err)
		t.Cleanup(func() {
			_, dropErr := database.ExecContext(context.Background(), "DROP TABLE IF EXISTS public."+tableName)
			require.NoError(t, dropErr)
		})
		stores[storeName] = config.AttributeStoreConfigEntry{
			Type:         config.AttributeStoreTypePostgres,
			DSN:          postgresDSN,
			TableName:    "public." + tableName,
			TenantColumn: "tenant_id",
			RowKey:       "{flow_id}",
		}
	}
	pair := startTenantPair(t, func(testConfig *DexServiceTestConfig) {
		testConfig.AttributeStore = config.AttributeStoreConfig{Stores: stores}
		testConfig.Tenancy.AttributeStores = []string{"reporting"}
		testConfig.Tenancy.TombstoneRefreshInterval = time.Second
		testConfig.Tenancy.PurgeConfirmationDelay = time.Second
	})
	startWithStore := func(client dexpb.FlowServiceClient, target *dexpb.WorkerTarget, flowID, storeName, message string) error {
		_, err := client.StartFlow(ctx, &dexpb.StartFlowRequest{
			RequestId: newRequestID(), FlowId: flowID, FlowType: tenantProbeFlowType, FlowTimeoutSeconds: 60,
			StartStepType: tenantProbeStep,
			FlowStartOptions: &dexpb.FlowStartOptions{
				Attributes: []*dexpb.AttributeWrite{syncedStringAttribute("message", message)},
				FlowConfigOverride: &dexpb.FlowConfig{
					AttributeStoreNames: &dexpb.AttributeStoreNames{Names: []string{storeName}},
					WorkerTarget:        target,
				},
			},
		})
		return err
	}

	flowID := "rows-" + uuid.NewString()
	require.NoError(t, startWithStore(pair.alpha, pair.alphaTarget, flowID, "reporting", "from-alpha"))
	require.NoError(t, startWithStore(pair.beta, pair.betaTarget, flowID, "reporting", "from-beta"))
	require.Eventually(t, func() bool {
		rows, queryErr := database.QueryContext(ctx, fmt.Sprintf(
			`SELECT tenant_id, message FROM public.%s WHERE flow_id = $1 ORDER BY tenant_id`, tables["reporting"],
		), flowID)
		require.NoError(t, queryErr)
		defer func() { require.NoError(t, rows.Close()) }()
		found := []string{}
		for rows.Next() {
			var tenantID, message string
			require.NoError(t, rows.Scan(&tenantID, &message))
			found = append(found, tenantID+"="+message)
		}
		return fmt.Sprint(found) == fmt.Sprint([]string{"alpha=from-alpha", "beta=from-beta"})
	}, 30*time.Second, 200*time.Millisecond, "each tenant writes its own row keyed by tenant and application Flow ID")

	err = startWithStore(pair.alpha, pair.alphaTarget, "refused-"+uuid.NewString(), "other", "x")
	require.Equal(t, codes.InvalidArgument, status.Code(err), "a store outside tenancy.attributeStores is refused: %v", err)

	operator := dexpb.NewTenantAdminServiceClient(newDexClientConnectionWithCredential(
		t, pair.runtime.PublicAddress, pair.environment.OperatorToken, config.DefaultGrpcMaxMessageBytes,
	))
	_, err = operator.TombstoneTenant(ctx, &dexpb.TombstoneTenantRequest{TenantId: "alpha"})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		response, purgeErr := operator.PurgeTenant(ctx, &dexpb.PurgeTenantRequest{TenantId: "alpha"})
		require.NoError(t, purgeErr)
		return response.GetDone()
	}, 60*time.Second, 500*time.Millisecond)
	var remaining []string
	rows, err := database.QueryContext(ctx, fmt.Sprintf(
		`SELECT tenant_id FROM public.%s WHERE flow_id = $1`, tables["reporting"],
	), flowID)
	require.NoError(t, err)
	for rows.Next() {
		var tenantID string
		require.NoError(t, rows.Scan(&tenantID))
		remaining = append(remaining, tenantID)
	}
	require.NoError(t, rows.Close())
	require.Equal(t, []string{"beta"}, remaining, "the purge deletes only the purged tenant's rows")
}
