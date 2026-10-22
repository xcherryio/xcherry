// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

//go:build attributestore_integration

package attributestore

import (
	"context"
	"database/sql"
	"math"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service/common/log"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestMySQLAndPostgresAttributeStoreIntegration(t *testing.T) {
	ctx := context.Background()
	postgresDSN := environmentOrDefault(
		"DEX_ATTRIBUTE_STORE_POSTGRES_DSN",
		"postgres://dex:dex@127.0.0.1:55432/dex?sslmode=disable",
	)
	mysqlDSN := environmentOrDefault(
		"DEX_ATTRIBUTE_STORE_MYSQL_DSN",
		"dex:dex@tcp(127.0.0.1:53306)/dex?parseTime=true",
	)
	postgres := openIntegrationDatabase(t, "pgx", postgresDSN)
	mysql := openIntegrationDatabase(t, "mysql", mysqlDSN)
	prepareIntegrationTables(t, postgres, mysql)
	assertInvalidSchemasFailStartup(t, postgresDSN, postgres)

	manager, err := NewManager(ctx, &config.AttributeStoreConfig{
		SchemaSyncInterval: 50 * time.Millisecond,
		Stores: map[string]config.AttributeStoreConfigEntry{
			"reporting": {
				Type:      config.AttributeStoreTypePostgres,
				DSN:       postgresDSN,
				TableName: "public.flow_attributes",
			},
			"operational": {
				Type:      config.AttributeStoreTypeMySQL,
				DSN:       mysqlDSN,
				TableName: "dex.flow_attributes",
			},
		},
	}, log.NewNoop())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })

	writeIntegrationBatch(t, manager, "reporting", "postgres-flow")
	writeIntegrationBatch(t, manager, "operational", "mysql-flow")
	assertIntegrationRow(t, postgres, "postgres-flow")
	assertIntegrationRow(t, mysql, "mysql-flow")

	reportingStore := manager.entries["reporting"].(*sqlStore)
	previousSnapshot := reportingStore.schema.Load()
	_, err = postgres.ExecContext(ctx, `ALTER TABLE flow_attributes RENAME TO flow_attributes_hidden`)
	require.NoError(t, err)
	require.Never(t, func() bool {
		return !reflect.DeepEqual(
			reportingStore.schema.Load(),
			previousSnapshot,
		)
	}, 200*time.Millisecond, 20*time.Millisecond)
	_, err = postgres.ExecContext(ctx, `ALTER TABLE flow_attributes_hidden RENAME TO flow_attributes`)
	require.NoError(t, err)
	_, err = postgres.ExecContext(ctx, `ALTER TABLE flow_attributes ADD COLUMN late_column TEXT`)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return reportingStore.schema.Load().columns["late_column"].name == "late_column"
	}, 3*time.Second, 20*time.Millisecond)
	err = manager.WriteBatch(ctx, &dexpb.SyncAttributeBatchActivityInput{
		FlowId:     "postgres-flow",
		ConfigName: "reporting",
		Items: []*dexpb.AttributeSyncItem{{
			ConfigName: "reporting",
			Key:        "late_column",
			Value:      stringValue("available"),
		}},
	})
	require.NoError(t, err)
	var lateColumn string
	require.NoError(t, postgres.QueryRowContext(ctx,
		`SELECT late_column FROM flow_attributes WHERE flow_id = $1`, "postgres-flow").Scan(&lateColumn))
	require.Equal(t, "available", lateColumn)
}

func TestMongoDBAttributeStoreIntegration(t *testing.T) {
	ctx := context.Background()
	mongodbDSN := environmentOrDefault(
		"DEX_ATTRIBUTE_STORE_MONGODB_DSN",
		"mongodb://127.0.0.1:57017",
	)
	mongodbClient := openIntegrationMongoDB(t, mongodbDSN)
	prepareIntegrationCollection(t, mongodbClient)
	assertMissingCollectionFailsStartup(t, mongodbDSN)

	manager, err := NewManager(ctx, &config.AttributeStoreConfig{
		Stores: map[string]config.AttributeStoreConfigEntry{
			"documents": {
				Type:           config.AttributeStoreTypeMongoDB,
				DSN:            mongodbDSN,
				DatabaseName:   "dex",
				CollectionName: "flow_attributes",
			},
		},
	}, log.NewNoop())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })

	writeIntegrationBatch(t, manager, "documents", "mongodb-flow")
	assertMongoDBIntegrationDocument(t, mongodbClient, "mongodb-flow")
	assertMongoDBPartialUpdateAndInvalidFieldFiltering(t, manager, mongodbClient)
}

func TestRedisAttributeStoreIntegration(t *testing.T) {
	ctx := context.Background()
	redisURL := environmentOrDefault("DEX_ATTRIBUTE_STORE_REDIS_URL", "redis://127.0.0.1:56379/0")
	redisOptions, err := redis.ParseURL(redisURL)
	require.NoError(t, err)
	redisClient := redis.NewClient(redisOptions)
	require.NoError(t, redisClient.Ping(ctx).Err())
	t.Cleanup(func() { require.NoError(t, redisClient.Close()) })
	keyPrefix := "dex-store-integ:" + time.Now().UTC().Format("20060102T150405.000000000") + ":"
	t.Cleanup(func() { require.NoError(t, redisClient.Del(context.Background(), keyPrefix+"redis-flow").Err()) })

	_, err = NewManager(ctx, &config.AttributeStoreConfig{
		Stores: map[string]config.AttributeStoreConfigEntry{
			"unreachable": {Type: config.AttributeStoreTypeRedis, DSN: "redis://127.0.0.1:1/0"},
		},
	}, log.NewNoop())
	require.ErrorContains(t, err, "ping Redis")

	manager, err := NewManager(ctx, &config.AttributeStoreConfig{
		Stores: map[string]config.AttributeStoreConfigEntry{
			"cache": {Type: config.AttributeStoreTypeRedis, DSN: redisURL, KeyPrefix: keyPrefix},
		},
	}, log.NewNoop())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })

	writeIntegrationBatch(t, manager, "cache", "redis-flow")
	fields, err := redisClient.HGetAll(ctx, keyPrefix+"redis-flow").Result()
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"name":           `"latest"`,
		"count_value":    `42`,
		"ratio":          `2.5`,
		"active":         `true`,
		"logged_at":      `"2026-08-11T15:30:00Z"`,
		"json_value":     `{"ok":true}`,
		"binary_value":   `bytes`,
		"nullable_value": `null`,
		"missing":        `"filtered"`,
	}, fields)

	err = manager.WriteBatch(ctx, &dexpb.SyncAttributeBatchActivityInput{
		FlowId:     "redis-flow",
		ConfigName: "cache",
		Items: []*dexpb.AttributeSyncItem{
			{ConfigName: "cache", Key: "name", Value: stringValue("updated")},
			{ConfigName: "cache", Key: "ratio", Value: doubleValue(math.Inf(1))},
			{ConfigName: "cache", Key: "json_value", Value: objectValue("json", `{broken`)},
		},
	})
	require.NoError(t, err)
	fields, err = redisClient.HGetAll(ctx, keyPrefix+"redis-flow").Result()
	require.NoError(t, err)
	require.Equal(t, `"updated"`, fields["name"])
	require.Equal(t, `2.5`, fields["ratio"])
	require.Equal(t, `{"ok":true}`, fields["json_value"])
	require.Equal(t, `42`, fields["count_value"])
}

func openIntegrationMongoDB(t *testing.T, uri string) *mongo.Client {
	t.Helper()
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	require.NoError(t, err)
	require.NoError(t, client.Ping(context.Background(), nil))
	t.Cleanup(func() { require.NoError(t, client.Disconnect(context.Background())) })
	return client
}

func prepareIntegrationCollection(t *testing.T, client *mongo.Client) {
	t.Helper()
	database := client.Database("dex")
	require.NoError(t, database.Collection("flow_attributes").Drop(context.Background()))
	require.NoError(t, database.CreateCollection(context.Background(), "flow_attributes"))
}

func assertMissingCollectionFailsStartup(t *testing.T, uri string) {
	t.Helper()
	_, err := NewManager(context.Background(), &config.AttributeStoreConfig{
		Stores: map[string]config.AttributeStoreConfigEntry{
			"invalid": {
				Type:           config.AttributeStoreTypeMongoDB,
				DSN:            uri,
				DatabaseName:   "dex",
				CollectionName: "missing_collection",
			},
		},
	}, log.NewNoop())
	require.ErrorContains(t, err, "does not exist")
}

func assertInvalidSchemasFailStartup(t *testing.T, dsn string, postgres *sql.DB) {
	t.Helper()
	statements := []string{
		`DROP TABLE IF EXISTS no_primary_key`,
		`CREATE TABLE no_primary_key (flow_id TEXT)`,
		`DROP TABLE IF EXISTS composite_primary_key`,
		`CREATE TABLE composite_primary_key (first_id TEXT, second_id TEXT, PRIMARY KEY (first_id, second_id))`,
		`DROP TABLE IF EXISTS integer_primary_key`,
		`CREATE TABLE integer_primary_key (flow_id BIGINT PRIMARY KEY)`,
		`DROP TABLE IF EXISTS required_column`,
		`CREATE TABLE required_column (flow_id TEXT PRIMARY KEY, required_value TEXT NOT NULL)`,
	}
	for _, statement := range statements {
		_, err := postgres.ExecContext(context.Background(), statement)
		require.NoError(t, err)
	}
	for _, tableName := range []string{
		"missing_table",
		"no_primary_key",
		"composite_primary_key",
		"integer_primary_key",
		"required_column",
	} {
		_, err := NewManager(context.Background(), &config.AttributeStoreConfig{
			Stores: map[string]config.AttributeStoreConfigEntry{
				"invalid": {
					Type:      config.AttributeStoreTypePostgres,
					DSN:       dsn,
					TableName: "public." + tableName,
				},
			},
		}, log.NewNoop())
		require.Error(t, err, tableName)
	}
}

func openIntegrationDatabase(t *testing.T, driver, dsn string) *sql.DB {
	t.Helper()
	database, err := sql.Open(driver, dsn)
	require.NoError(t, err)
	require.NoError(t, database.PingContext(context.Background()))
	t.Cleanup(func() { require.NoError(t, database.Close()) })
	return database
}

func prepareIntegrationTables(t *testing.T, postgres, mysql *sql.DB) {
	t.Helper()
	statements := []struct {
		database *sql.DB
		query    string
	}{
		{postgres, `DROP TABLE IF EXISTS flow_attributes_hidden`},
		{postgres, `DROP TABLE IF EXISTS flow_attributes`},
		{postgres, `CREATE TABLE flow_attributes (
flow_id TEXT PRIMARY KEY,
name VARCHAR(32), count_value BIGINT, ratio DOUBLE PRECISION, active BOOLEAN,
logged_at TIMESTAMPTZ, json_value JSONB, binary_value BYTEA, nullable_value TEXT)`},
		{mysql, `DROP TABLE IF EXISTS flow_attributes`},
		{mysql, `CREATE TABLE flow_attributes (
flow_id VARCHAR(255) PRIMARY KEY,
name VARCHAR(32), count_value BIGINT, ratio DOUBLE, active BOOLEAN,
logged_at TIMESTAMP NULL, json_value JSON, binary_value BLOB, nullable_value TEXT)`},
	}
	for _, statement := range statements {
		_, err := statement.database.ExecContext(context.Background(), statement.query)
		require.NoError(t, err)
	}
}

func writeIntegrationBatch(t *testing.T, manager *Manager, configName, flowID string) {
	t.Helper()
	err := manager.WriteBatch(context.Background(), &dexpb.SyncAttributeBatchActivityInput{
		FlowId:     flowID,
		ConfigName: configName,
		Items: []*dexpb.AttributeSyncItem{
			{ConfigName: configName, Key: "name", Value: stringValue("old")},
			{ConfigName: configName, Key: "count_value", Value: intValue(42)},
			{ConfigName: configName, Key: "ratio", Value: doubleValue(2.5)},
			{ConfigName: configName, Key: "active", Value: boolValue(true)},
			{ConfigName: configName, Key: "logged_at", Value: objectValue("json", `"2026-08-11T15:30:00Z"`)},
			{ConfigName: configName, Key: "json_value", Value: objectValue("json", `{"ok":true}`)},
			{ConfigName: configName, Key: "binary_value", Value: objectValue("proto", "bytes")},
			{ConfigName: configName, Key: "nullable_value", Value: nullValue()},
			{ConfigName: configName, Key: "missing", Value: stringValue("filtered")},
			{ConfigName: configName, Key: "name", Value: stringValue("latest")},
		},
	})
	require.NoError(t, err)
}

func assertIntegrationRow(t *testing.T, database *sql.DB, flowID string) {
	t.Helper()
	placeholder := "?"
	if flowID == "postgres-flow" {
		placeholder = "$1"
	}
	var (
		name          string
		countValue    int64
		ratio         float64
		active        bool
		loggedAt      time.Time
		jsonValue     []byte
		binaryValue   []byte
		nullableValue sql.NullString
	)
	err := database.QueryRowContext(context.Background(),
		"SELECT name, count_value, ratio, active, logged_at, json_value, binary_value, nullable_value FROM flow_attributes WHERE flow_id = "+placeholder,
		flowID,
	).Scan(&name, &countValue, &ratio, &active, &loggedAt, &jsonValue, &binaryValue, &nullableValue)
	require.NoError(t, err)
	require.Equal(t, "latest", name)
	require.Equal(t, int64(42), countValue)
	require.Equal(t, 2.5, ratio)
	require.True(t, active)
	require.Equal(t, time.Date(2026, 8, 11, 15, 30, 0, 0, time.UTC), loggedAt.UTC())
	require.JSONEq(t, `{"ok":true}`, string(jsonValue))
	require.Equal(t, []byte("bytes"), binaryValue)
	require.False(t, nullableValue.Valid)
}

func assertMongoDBIntegrationDocument(t *testing.T, client *mongo.Client, flowID string) {
	t.Helper()
	var document struct {
		ID         string  `bson:"_id"`
		Name       string  `bson:"name"`
		CountValue int64   `bson:"count_value"`
		Ratio      float64 `bson:"ratio"`
		Active     bool    `bson:"active"`
		LoggedAt   string  `bson:"logged_at"`
		JSONValue  struct {
			OK bool `bson:"ok"`
		} `bson:"json_value"`
		BinaryValue   bson.Binary `bson:"binary_value"`
		NullableValue any         `bson:"nullable_value"`
	}
	err := client.Database("dex").Collection("flow_attributes").
		FindOne(context.Background(), bson.D{{Key: "_id", Value: flowID}}).Decode(&document)
	require.NoError(t, err)
	require.Equal(t, flowID, document.ID)
	require.Equal(t, "latest", document.Name)
	require.Equal(t, int64(42), document.CountValue)
	require.Equal(t, 2.5, document.Ratio)
	require.True(t, document.Active)
	require.Equal(t, "2026-08-11T15:30:00Z", document.LoggedAt)
	require.True(t, document.JSONValue.OK)
	require.Equal(t, bson.Binary{Subtype: 0, Data: []byte("bytes")}, document.BinaryValue)
	require.Nil(t, document.NullableValue)
}

func assertMongoDBPartialUpdateAndInvalidFieldFiltering(
	t *testing.T,
	manager *Manager,
	client *mongo.Client,
) {
	t.Helper()
	err := manager.WriteBatch(context.Background(), &dexpb.SyncAttributeBatchActivityInput{
		FlowId:     "mongodb-flow",
		ConfigName: "documents",
		Items: []*dexpb.AttributeSyncItem{
			{ConfigName: "documents", Key: "name", Value: stringValue("updated")},
			{ConfigName: "documents", Key: "name", Value: stringValue("latest-update")},
			{ConfigName: "documents", Key: "_id", Value: stringValue("replacement")},
			{ConfigName: "documents", Key: "nested.path", Value: stringValue("invalid")},
		},
	})
	require.NoError(t, err)
	var document bson.M
	err = client.Database("dex").Collection("flow_attributes").
		FindOne(context.Background(), bson.D{{Key: "_id", Value: "mongodb-flow"}}).Decode(&document)
	require.NoError(t, err)
	require.Equal(t, "latest-update", document["name"])
	require.Equal(t, int64(42), document["count_value"])
	require.NotContains(t, document, "nested.path")
}

func doubleValue(value float64) *dexpb.Value {
	return &dexpb.Value{Kind: &dexpb.Value_DoubleValue{DoubleValue: value}}
}

func environmentOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
