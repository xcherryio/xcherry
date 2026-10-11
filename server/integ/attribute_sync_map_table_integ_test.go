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

func TestAttributeSyncMapTablesTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	doTestAttributeSyncMapTables(t, service.BackendTypeTemporal)
}

func TestAttributeSyncMapTablesCadence(t *testing.T) {
	if !*cadenceIntegTest {
		t.Skip()
	}
	doTestAttributeSyncMapTables(t, service.BackendTypeCadence)
}

func doTestAttributeSyncMapTables(t *testing.T, backendType service.BackendType) {
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
	groupsTable := "map_groups_" + suffix
	membersTable := "map_group_members_" + suffix
	for _, statement := range []string{
		`CREATE TABLE public.` + groupsTable + ` (group_id TEXT PRIMARY KEY, display_name TEXT)`,
		`CREATE TABLE public.` + membersTable + ` (group_id TEXT, account_id TEXT, role TEXT, note TEXT,
PRIMARY KEY (group_id, account_id))`,
	} {
		_, err = database.ExecContext(ctx, statement)
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		for _, tableName := range []string{groupsTable, membersTable} {
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
				"groups": {
					Type: config.AttributeStoreTypePostgres, DSN: postgresDSN, TableName: "public." + groupsTable,
					RowKey: "group-{group_id}", ColumnNaming: config.AttributeStoreColumnNamingSnakeCase,
					AttributeMaps: map[string]config.AttributeStoreMapTableConfig{
						"Members": {TableName: "public." + membersTable, InstanceKeyColumn: "account_id"},
					},
				},
			},
		},
	})
	workerTarget := startWorker(t, signal.NewHandler())
	groupID := "g" + suffix
	flowID := "group-" + groupID
	longNote := strings.Repeat("blob-backed-note ", 8)
	_, err = runtime.FlowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             flowID,
		FlowType:           "attribute-sync",
		FlowTimeoutSeconds: 45,
		FlowStartOptions: &dexpb.FlowStartOptions{
			Attributes: []*dexpb.AttributeWrite{
				syncedStringAttribute("DisplayName", "Core"),
				syncedObjectAttribute("Members/account%2F1", `{"Role":"ADMIN","Note":"`+longNote+`"}`),
				syncedObjectAttribute("Members/account-2", `{"Role":"MEMBER","Note":"replaced"}`),
			},
			FlowConfigOverride: &dexpb.FlowConfig{
				AttributeStoreNames: &dexpb.AttributeStoreNames{Names: []string{"groups"}},
				WorkerTarget:        workerTarget,
			},
		},
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return strings.Join(groupMemberRoles(ctx, database, membersTable, groupID), ",") ==
			"account-2=MEMBER,account/1=ADMIN"
	}, 20*time.Second, 50*time.Millisecond)
	var note string
	require.NoError(t, database.QueryRowContext(ctx,
		"SELECT note FROM public."+membersTable+" WHERE group_id = $1 AND account_id = $2", groupID, "account/1",
	).Scan(&note))
	require.Equal(t, longNote, note)

	_, err = runtime.FlowClient.SetAttributes(ctx, &dexpb.SetAttributesRequest{
		RequestId: newRequestID(),
		FlowId:    flowID,
		Attributes: []*dexpb.AttributeWrite{
			{Key: "Members/account%2F1", Value: nullValue(), SyncConfig: &dexpb.AttributeSyncConfig{Enabled: true}},
			syncedObjectAttribute("Members/account-2", `{"Role":"ADMIN"}`),
			syncedObjectAttribute("Members/account-3", `{"Role":"MEMBER"}`),
		},
	})
	require.NoError(t, err)
	_, err = runtime.FlowClient.StopFlow(ctx, &dexpb.StopFlowRequest{
		FlowId:   flowID,
		StopType: dexpb.StopType_STOP_TYPE_CANCEL,
	})
	require.NoError(t, err)
	response, err := runtime.FlowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_CANCELED, response.GetFlowStatus())

	require.Equal(t, []string{"account-2=ADMIN", "account-3=MEMBER"},
		groupMemberRoles(ctx, database, membersTable, groupID))
	var replacedNote sql.NullString
	require.NoError(t, database.QueryRowContext(ctx,
		"SELECT note FROM public."+membersTable+" WHERE group_id = $1 AND account_id = $2", groupID, "account-2",
	).Scan(&replacedNote))
	require.False(t, replacedNote.Valid)
	var displayName string
	require.NoError(t, database.QueryRowContext(ctx,
		"SELECT display_name FROM public."+groupsTable+" WHERE group_id = $1", groupID).Scan(&displayName))
	require.Equal(t, "Core", displayName)
}

func groupMemberRoles(ctx context.Context, database *sql.DB, membersTable, groupID string) []string {
	rows, err := database.QueryContext(ctx,
		"SELECT account_id || '=' || role FROM public."+membersTable+" WHERE group_id = $1 ORDER BY account_id COLLATE \"C\"",
		groupID,
	)
	if err != nil {
		return nil
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			panic(closeErr)
		}
	}()
	roles := []string{}
	for rows.Next() {
		var role string
		if scanErr := rows.Scan(&role); scanErr != nil {
			return nil
		}
		roles = append(roles, role)
	}
	if rows.Err() != nil {
		return nil
	}
	return roles
}
