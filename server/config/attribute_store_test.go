// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/service/common/ptr"
)

func TestAttributeStoreConfigDecodeAndDefaults(t *testing.T) {
	path := writeTestConfig(t, `
blobStore:
  enabled: false
attributeStore:
  stores:
    reporting:
      type: postgres
      dsn: postgres://localhost/reporting
      tableName: public.flow_attributes
  schemaSyncInterval: 2m
  syncBatchSize: 25
  syncAttemptTimeout: 45s
  syncRetryPolicy:
    initialInterval: 250ms
    maximumInterval: 20s
    backoffCoefficient: 1.5
    maximumAttempts: 4
    totalDuration: 1m30s
`)
	cfg, err := NewConfig(path)
	require.NoError(t, err)
	require.False(t, cfg.BlobStore.EffectiveEnabled())
	require.Equal(t, DefaultBlobStoreThresholdInBytes, cfg.BlobStore.EffectiveThresholdInBytes())
	require.Equal(t, 2*time.Minute, cfg.AttributeStore.EffectiveSchemaSyncInterval())
	require.Equal(t, 25, cfg.AttributeStore.EffectiveSyncBatchSize())
	require.Equal(t, 45*time.Second, cfg.AttributeStore.EffectiveSyncAttemptTimeout())
	policy := cfg.AttributeStore.EffectiveSyncRetryPolicy()
	require.Equal(t, 250*time.Millisecond, policy.InitialInterval)
	require.Equal(t, 20*time.Second, policy.MaximumInterval)
	require.Equal(t, 1.5, policy.BackoffCoefficient)
	require.Equal(t, int32(4), policy.MaximumAttempts)
	require.Equal(t, 90*time.Second, policy.TotalDuration)
	require.NoError(t, cfg.AttributeStore.Validate())
}

func TestAttributeStoreConfigNewBackends(t *testing.T) {
	path := writeTestConfig(t, `
attributeStore:
  stores:
    lakehouse:
      type: databricks
      dsn: token:secret@workspace:443/sql/1.0/warehouses/id
      tableName: analytics.reporting.flow_attributes
      flowIdColumn: flow_id
    warehouse:
      type: snowflake
      dsn: user:password@account/analytics/reporting
      tableName: analytics.reporting.flow_attributes
      flowIdColumn: flow_id
    documents:
      type: mongodb
      dsn: mongodb://localhost:27017
      databaseName: analytics
      collectionName: flow_attributes
`)
	cfg, err := NewConfig(path)
	require.NoError(t, err)
	require.NoError(t, cfg.AttributeStore.Validate())
	require.Equal(t, AttributeStoreTypeDatabricks, cfg.AttributeStore.Stores["lakehouse"].Type)
	require.Equal(t, "flow_id", cfg.AttributeStore.Stores["warehouse"].FlowIDColumn)
	require.Equal(t, "flow_attributes", cfg.AttributeStore.Stores["documents"].CollectionName)
}

func TestAttributeStoreConfigRedis(t *testing.T) {
	path := writeTestConfig(t, `
attributeStore:
  stores:
    cache:
      type: redis
      dsn: rediss://user:password@redis.example.com:6379/0
    prefixed:
      type: redis
      dsn: redis://localhost:6379/1
      keyPrefix: "orders:"
`)
	cfg, err := NewConfig(path)
	require.NoError(t, err)
	require.Equal(t, AttributeStoreTypeRedis, cfg.AttributeStore.Stores["cache"].Type)
	require.Equal(t, DefaultAttributeStoreRedisKeyPrefix, cfg.AttributeStore.Stores["cache"].EffectiveKeyPrefix())
	require.Equal(t, "orders:", cfg.AttributeStore.Stores["prefixed"].EffectiveKeyPrefix())
	require.NoError(t, cfg.AttributeStore.Validate())

	relationalPath := writeTestConfig(t, `
attributeStore:
  stores:
    cache:
      type: redis
      dsn: redis://localhost:6379/0
      rowKey: order-{order_id}
      columnNaming: snake_case
      attributeMaps:
        LineItems:
          tableName: line_items
          instanceKeyColumn: line_item_id
`)
	relationalConfig, err := NewConfig(relationalPath)
	require.NoError(t, err)
	require.ErrorContains(
		t,
		relationalConfig.AttributeStore.Validate(),
		"only mysql and postgres accept rowKey, columnNaming and attributeMaps",
	)
}

func TestAttributeStoreConfigBackendSpecificValidation(t *testing.T) {
	tests := []struct {
		name  string
		store AttributeStoreConfigEntry
		error string
	}{
		{
			name:  "unsupported",
			store: AttributeStoreConfigEntry{Type: "unknown", DSN: "dsn", TableName: "table"},
			error: "unsupported type",
		},
		{
			name:  "missing dsn",
			store: AttributeStoreConfigEntry{Type: AttributeStoreTypeSnowflake, TableName: "table", FlowIDColumn: "flow_id"},
			error: "requires dsn",
		},
		{
			name:  "missing table",
			store: AttributeStoreConfigEntry{Type: AttributeStoreTypeSnowflake, DSN: "dsn", FlowIDColumn: "flow_id"},
			error: "requires tableName",
		},
		{
			name:  "databricks flow id",
			store: AttributeStoreConfigEntry{Type: AttributeStoreTypeDatabricks, DSN: "dsn", TableName: "table"},
			error: "flowIdColumn",
		},
		{
			name: "mongodb target",
			store: AttributeStoreConfigEntry{
				Type: AttributeStoreTypeMongoDB, DSN: "dsn", DatabaseName: "database",
			},
			error: "collectionName",
		},
		{
			name: "mongodb sql fields",
			store: AttributeStoreConfigEntry{
				Type: AttributeStoreTypeMongoDB, DSN: "dsn", DatabaseName: "database",
				CollectionName: "collection", TableName: "table",
			},
			error: "does not accept tableName",
		},
		{
			name: "sql mongo fields",
			store: AttributeStoreConfigEntry{
				Type: AttributeStoreTypePostgres, DSN: "dsn", TableName: "table", DatabaseName: "database",
			},
			error: "does not accept databaseName",
		},
		{
			name: "postgres flow id",
			store: AttributeStoreConfigEntry{
				Type: AttributeStoreTypePostgres, DSN: "dsn", TableName: "table", FlowIDColumn: "flow_id",
			},
			error: "only warehouses",
		},
		{
			name: "warehouse table has too many parts",
			store: AttributeStoreConfigEntry{
				Type: AttributeStoreTypeSnowflake, DSN: "dsn",
				TableName: "one.two.three.four", FlowIDColumn: "flow_id",
			},
			error: "at most 3",
		},
		{
			name: "redis sql fields",
			store: AttributeStoreConfigEntry{
				Type: AttributeStoreTypeRedis, DSN: "redis://localhost:6379/0", TableName: "table",
			},
			error: "redis configuration does not accept tableName",
		},
		{
			name: "redis row key",
			store: AttributeStoreConfigEntry{
				Type: AttributeStoreTypeRedis, DSN: "redis://localhost:6379/0", RowKey: "order-{order_id}",
			},
			error: "only mysql and postgres accept rowKey, columnNaming and attributeMaps",
		},
		{
			name: "redis column naming",
			store: AttributeStoreConfigEntry{
				Type: AttributeStoreTypeRedis, DSN: "redis://localhost:6379/0",
				ColumnNaming: AttributeStoreColumnNamingSnakeCase,
			},
			error: "only mysql and postgres accept rowKey, columnNaming and attributeMaps",
		},
		{
			name: "redis attribute maps",
			store: AttributeStoreConfigEntry{
				Type: AttributeStoreTypeRedis, DSN: "redis://localhost:6379/0",
				AttributeMaps: map[string]AttributeStoreMapTableConfig{
					"LineItems": {TableName: "line_items", InstanceKeyColumn: "line_item_id"},
				},
			},
			error: "only mysql and postgres accept rowKey, columnNaming and attributeMaps",
		},
		{
			name: "postgres key prefix",
			store: AttributeStoreConfigEntry{
				Type: AttributeStoreTypePostgres, DSN: "dsn", TableName: "table", KeyPrefix: "dex:",
			},
			error: "only redis accepts keyPrefix",
		},
		{
			name: "relational table has too many parts",
			store: AttributeStoreConfigEntry{
				Type: AttributeStoreTypePostgres, DSN: "dsn", TableName: "one.two.three",
			},
			error: "at most 2",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := (AttributeStoreConfig{Stores: map[string]AttributeStoreConfigEntry{
				"target": test.store,
			}}).Validate()
			require.ErrorContains(t, err, test.error)
		})
	}
}

func TestAttributeStoreConfigQualifiedTableNames(t *testing.T) {
	tests := []struct {
		storeType AttributeStoreType
		tableName string
	}{
		{AttributeStoreTypePostgres, "flow_attributes"},
		{AttributeStoreTypeMySQL, "reporting.flow_attributes"},
		{AttributeStoreTypeDatabricks, "flow_attributes"},
		{AttributeStoreTypeSnowflake, "reporting.flow_attributes"},
		{AttributeStoreTypeDatabricks, "analytics.reporting.flow_attributes"},
	}
	for _, test := range tests {
		store := AttributeStoreConfigEntry{Type: test.storeType, DSN: "dsn", TableName: test.tableName}
		if test.storeType.IsWarehouse() {
			store.FlowIDColumn = "flow_id"
		}
		require.NoError(t, (AttributeStoreConfig{Stores: map[string]AttributeStoreConfigEntry{
			"target": store,
		}}).Validate(), test.tableName)
	}
}

func TestBlobStoreDefaults(t *testing.T) {
	path := writeTestConfig(t, "{}\n")
	cfg, err := NewConfig(path)
	require.NoError(t, err)
	require.True(t, cfg.BlobStore.EffectiveEnabled())
	require.True(t, cfg.BlobStore.EffectiveAsyncStepInputSnapshotsEnabled())
	require.Equal(t, 100, cfg.BlobStore.EffectiveThresholdInBytes())
	require.Equal(t, DefaultBlobStoreObjectIDLength, cfg.BlobStore.EffectiveObjectIDLength())
	require.Equal(t, 100*time.Millisecond, cfg.AttributeStore.EffectiveSyncRetryPolicy().InitialInterval)
}

func TestBlobStoreObjectIDLengthValidation(t *testing.T) {
	for _, objectIDLength := range []int{0, 1, 9, 10, 12, 16, 22, 50, 51} {
		require.NoError(t, (BlobStoreConfig{ObjectIDLength: objectIDLength}).Validate())
	}
	err := (BlobStoreConfig{ObjectIDLength: -1}).Validate()
	require.ErrorContains(t, err, "objectIdLength")
}

func TestBlobStoreAsyncStepInputSnapshotsRequireBlobStore(t *testing.T) {
	require.NoError(t, (BlobStoreConfig{}).Validate())
	require.NoError(t, (BlobStoreConfig{Enabled: ptr.Any(false)}).Validate())
	require.NoError(t, (BlobStoreConfig{AsyncStepInputSnapshotsEnabled: ptr.Any(true)}).Validate())
	require.False(t, (BlobStoreConfig{Enabled: ptr.Any(false)}).EffectiveAsyncStepInputSnapshotsEnabled())

	err := (BlobStoreConfig{
		Enabled:                        ptr.Any(false),
		AsyncStepInputSnapshotsEnabled: ptr.Any(true),
	}).Validate()
	require.ErrorContains(t, err, "asyncStepInputSnapshotsEnabled")
}

func TestExternalStorageConfigKeyIsRejected(t *testing.T) {
	path := writeTestConfig(t, "externalStorage:\n  enabled: true\n")
	_, err := NewConfig(path)
	require.ErrorContains(t, err, "externalStorage")
}

func writeTestConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
	return path
}
