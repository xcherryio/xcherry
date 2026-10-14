// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package allinone

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	dexconfig "github.com/superdurable/dex/config"
	temporalconfig "github.com/superdurable/temporal-server/common/config"
)

const (
	defaultFrontendPort = 7233
	defaultDexPort      = 8801
	defaultRetention    = 3
)

type Config struct {
	Temporal       *temporalconfig.Config
	Dex            *dexconfig.Config
	Database       DatabaseConfig
	Namespace      string
	Retention      time.Duration
	FrontendTarget string
	StartupTimeout time.Duration
}

type DatabaseConfig struct {
	Host            string
	Port            int
	User            string
	Password        string
	AdminDatabase   string
	TemporalDB      string
	VisibilityDB    string
	Plugin          string
	TLS             bool
	TLSCertFile     string
	TLSKeyFile      string
	TLSCAFile       string
	TLSServerName   string
	TLSVerifyHost   bool
	SkipCreate      bool
	SkipSchemaSetup bool
	InitTimeout     time.Duration
	SQLToolPath     string
}

func LoadConfig() (*Config, error) {
	if err := setTemporalDefaults(); err != nil {
		return nil, err
	}
	database, err := loadDatabaseConfig()
	if err != nil {
		return nil, err
	}
	temporalCfg, err := temporalconfig.Load(temporalconfig.WithEmbedded())
	if err != nil {
		return nil, fmt.Errorf("load Temporal config: %w", err)
	}
	temporalCfg.DynamicConfigClient = nil
	frontendPort, err := envInt("FRONTEND_GRPC_PORT", defaultFrontendPort)
	if err != nil {
		return nil, err
	}
	clusterFrontend := envDefault("TEMPORAL_CLUSTER_FRONTEND_ADDRESS", fmt.Sprintf("127.0.0.1:%d", frontendPort))
	clusterName := temporalCfg.ClusterMetadata.CurrentClusterName
	clusterInfo := temporalCfg.ClusterMetadata.ClusterInformation[clusterName]
	clusterInfo.RPCAddress = clusterFrontend
	temporalCfg.ClusterMetadata.ClusterInformation[clusterName] = clusterInfo

	frontendTarget := fmt.Sprintf("127.0.0.1:%d", frontendPort)
	dexCfg, namespace, retention, err := loadDexConfig(frontendTarget)
	if err != nil {
		return nil, err
	}
	startupTimeout, err := envDuration("STARTUP_TIMEOUT", 2*time.Minute)
	if err != nil {
		return nil, err
	}
	return &Config{
		Temporal:       temporalCfg,
		Dex:            dexCfg,
		Database:       database,
		Namespace:      namespace,
		Retention:      retention,
		FrontendTarget: frontendTarget,
		StartupTimeout: startupTimeout,
	}, nil
}

func setTemporalDefaults() error {
	database := envDefault("DB", "postgres12_pgx")
	if database != "postgres12" && database != "postgres12_pgx" {
		return fmt.Errorf("DB must be postgres12 or postgres12_pgx, got %q", database)
	}
	if strings.EqualFold(os.Getenv("ENABLE_ES"), "true") {
		return fmt.Errorf("Elasticsearch visibility is not supported")
	}
	setDefaultEnv("DB", database)
	setDefaultEnv("ENABLE_ES", "false")
	setDefaultEnv("BIND_ON_IP", "0.0.0.0")
	if os.Getenv("TEMPORAL_BROADCAST_ADDRESS") == "" {
		address, err := discoverPrivateIP()
		if err != nil {
			return err
		}
		if err := os.Setenv("TEMPORAL_BROADCAST_ADDRESS", address); err != nil {
			return fmt.Errorf("set TEMPORAL_BROADCAST_ADDRESS: %w", err)
		}
	}
	return nil
}

func loadDatabaseConfig() (DatabaseConfig, error) {
	host := strings.TrimSpace(os.Getenv("POSTGRES_SEEDS"))
	if host == "" {
		return DatabaseConfig{}, fmt.Errorf("POSTGRES_SEEDS is required")
	}
	if strings.Contains(host, ",") {
		return DatabaseConfig{}, fmt.Errorf("POSTGRES_SEEDS must contain exactly one host")
	}
	port, err := envInt("DB_PORT", 5432)
	if err != nil {
		return DatabaseConfig{}, err
	}
	initTimeout, err := envDuration("DB_INIT_TIMEOUT", 2*time.Minute)
	if err != nil {
		return DatabaseConfig{}, err
	}
	skipCreate, err := envBool("SKIP_DB_CREATE", false)
	if err != nil {
		return DatabaseConfig{}, err
	}
	skipSchema, err := envBool("SKIP_SCHEMA_SETUP", false)
	if err != nil {
		return DatabaseConfig{}, err
	}
	tlsEnabled, err := envBool("SQL_TLS_ENABLED", false)
	if err != nil {
		return DatabaseConfig{}, err
	}
	verifyHost, err := envBool("SQL_HOST_VERIFICATION", false)
	if err != nil {
		return DatabaseConfig{}, err
	}
	return DatabaseConfig{
		Host:            host,
		Port:            port,
		User:            os.Getenv("POSTGRES_USER"),
		Password:        os.Getenv("POSTGRES_PWD"),
		AdminDatabase:   envDefault("POSTGRES_ADMIN_DB", "postgres"),
		TemporalDB:      envDefault("DBNAME", "temporal"),
		VisibilityDB:    envDefault("VISIBILITY_DBNAME", "temporal_visibility"),
		Plugin:          envDefault("DB", "postgres12_pgx"),
		TLS:             tlsEnabled,
		TLSCertFile:     os.Getenv("SQL_CERT"),
		TLSKeyFile:      os.Getenv("SQL_CERT_KEY"),
		TLSCAFile:       os.Getenv("SQL_CA"),
		TLSServerName:   os.Getenv("SQL_HOST_NAME"),
		TLSVerifyHost:   verifyHost,
		SkipCreate:      skipCreate,
		SkipSchemaSetup: skipSchema,
		InitTimeout:     initTimeout,
		SQLToolPath:     os.Getenv("TEMPORAL_SQL_TOOL"),
	}, nil
}

func loadDexConfig(frontendTarget string) (*dexconfig.Config, string, time.Duration, error) {
	if path := os.Getenv("DEX_CONFIG_FILE"); path != "" {
		cfg, err := dexconfig.NewConfig(path)
		if err != nil {
			return nil, "", 0, fmt.Errorf("load Dex config: %w", err)
		}
		if cfg.Interpreter.Temporal == nil {
			return nil, "", 0, fmt.Errorf("DEX_CONFIG_FILE must configure the Temporal interpreter")
		}
		cfg.Interpreter.Temporal.HostPort = frontendTarget
		namespace := envDefault("DEX_NAMESPACE", envDefaultValue(cfg.Interpreter.Temporal.Namespace, "default"))
		cfg.Interpreter.Temporal.Namespace = namespace
		retention := time.Duration(cfg.BlobStore.HistoryRetentionInDays) * 24 * time.Hour
		if retention <= 0 {
			retention = defaultRetention * 24 * time.Hour
		}
		return cfg, namespace, retention, nil
	}

	required := []string{"DEX_S3_ENDPOINT", "DEX_S3_BUCKET", "DEX_S3_ACCESS_KEY", "DEX_S3_SECRET_KEY"}
	for _, name := range required {
		if os.Getenv(name) == "" {
			return nil, "", 0, fmt.Errorf("%s is required when DEX_CONFIG_FILE is not set", name)
		}
	}
	apiPort, err := envInt("DEX_API_PORT", defaultDexPort)
	if err != nil {
		return nil, "", 0, err
	}
	retentionDays, err := envInt("DEX_HISTORY_RETENTION_DAYS", defaultRetention)
	if err != nil || retentionDays <= 0 {
		return nil, "", 0, fmt.Errorf("DEX_HISTORY_RETENTION_DAYS must be positive")
	}
	thresholdBytes, err := envInt("DEX_BLOB_THRESHOLD_BYTES", 1024)
	if err != nil {
		return nil, "", 0, err
	}
	namespace := envDefault("DEX_NAMESPACE", "default")
	enabled := true
	cfg := &dexconfig.Config{
		Log: dexconfig.Logger{Stdout: true, Level: envDefault("LOG_LEVEL", "info")},
		Api: dexconfig.ApiConfig{Port: apiPort},
		Interpreter: dexconfig.Interpreter{
			Temporal: &dexconfig.TemporalConfig{HostPort: frontendTarget, Namespace: namespace},
		},
		BlobStore: dexconfig.BlobStoreConfig{
			Enabled:                &enabled,
			ThresholdInBytes:       thresholdBytes,
			HistoryRetentionInDays: retentionDays,
			SupportedStorages: []dexconfig.BlobStoreConfigEntry{{
				Status:      dexconfig.StorageStatusActive,
				StorageId:   envDefault("DEX_S3_STORAGE_ID", "primary"),
				StorageType: dexconfig.StorageTypeS3,
				S3Endpoint:  os.Getenv("DEX_S3_ENDPOINT"),
				S3Bucket:    os.Getenv("DEX_S3_BUCKET"),
				S3Region:    envDefault("DEX_S3_REGION", "us-east-1"),
				S3AccessKey: os.Getenv("DEX_S3_ACCESS_KEY"),
				S3SecretKey: os.Getenv("DEX_S3_SECRET_KEY"),
			}},
		},
	}
	return cfg, namespace, time.Duration(retentionDays) * 24 * time.Hour, nil
}

func discoverPrivateIP() (string, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return "", fmt.Errorf("list network interfaces: %w", err)
	}
	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addresses, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, address := range addresses {
			ip, _, err := net.ParseCIDR(address.String())
			if err == nil && ip.IsPrivate() && ip.To4() != nil {
				return ip.String(), nil
			}
		}
	}
	return "", fmt.Errorf("TEMPORAL_BROADCAST_ADDRESS is required when no private IPv4 address can be discovered")
}

func setDefaultEnv(name, value string) {
	if os.Getenv(name) == "" {
		_ = os.Setenv(name, value)
	}
}

func envDefault(name, fallback string) string {
	return envDefaultValue(os.Getenv(name), fallback)
}

func envDefaultValue(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func envInt(name string, fallback int) (int, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return parsed, nil
}

func envIntOrDefault(name string, fallback int) int {
	value, err := envInt(name, fallback)
	if err != nil {
		return fallback
	}
	return value
}

func envBool(name string, fallback bool) (bool, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean", name)
	}
	return parsed, nil
}

func envDuration(name string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", name)
	}
	return parsed, nil
}
