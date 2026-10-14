// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package allinone

import (
	"testing"
	"time"
)

func TestLoadConfigBuildsPostgresHAConfig(t *testing.T) {
	t.Setenv("DB", "postgres12_pgx")
	t.Setenv("ENABLE_ES", "false")
	t.Setenv("POSTGRES_SEEDS", "postgres")
	t.Setenv("POSTGRES_USER", "temporal")
	t.Setenv("POSTGRES_PWD", "secret")
	t.Setenv("BIND_ON_IP", "0.0.0.0")
	t.Setenv("TEMPORAL_BROADCAST_ADDRESS", "10.0.0.12")
	t.Setenv("TEMPORAL_CLUSTER_FRONTEND_ADDRESS", "temporal-frontend:7233")
	t.Setenv("DEX_CONFIG_FILE", "")
	t.Setenv("DEX_S3_ENDPOINT", "http://minio:9000")
	t.Setenv("DEX_S3_BUCKET", "dex")
	t.Setenv("DEX_S3_ACCESS_KEY", "access")
	t.Setenv("DEX_S3_SECRET_KEY", "secret")
	t.Setenv("DEX_NAMESPACE", "dex")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if cfg.Database.Plugin != "postgres12_pgx" {
		t.Fatalf("database plugin = %q", cfg.Database.Plugin)
	}
	if cfg.Temporal.Global.Membership.BroadcastAddress != "10.0.0.12" {
		t.Fatalf("broadcast address = %q", cfg.Temporal.Global.Membership.BroadcastAddress)
	}
	for name, service := range cfg.Temporal.Services {
		if service.RPC.BindOnIP != "0.0.0.0" {
			t.Fatalf("service %s bind address = %q", name, service.RPC.BindOnIP)
		}
	}
	cluster := cfg.Temporal.ClusterMetadata.ClusterInformation[cfg.Temporal.ClusterMetadata.CurrentClusterName]
	if cluster.RPCAddress != "temporal-frontend:7233" {
		t.Fatalf("cluster frontend address = %q", cluster.RPCAddress)
	}
	if cfg.Dex.Interpreter.Temporal.HostPort != "127.0.0.1:7233" {
		t.Fatalf("Dex Temporal target = %q", cfg.Dex.Interpreter.Temporal.HostPort)
	}
	if cfg.Namespace != "dex" || cfg.Retention != 72*time.Hour {
		t.Fatalf("namespace = %q, retention = %v", cfg.Namespace, cfg.Retention)
	}
}

func TestSetTemporalDefaultsRejectsOtherDatabases(t *testing.T) {
	t.Setenv("DB", "mysql8")
	if err := setTemporalDefaults(); err == nil {
		t.Fatal("setTemporalDefaults() accepted mysql8")
	}
}

func TestLoadDexConfigRequiresS3(t *testing.T) {
	t.Setenv("DEX_CONFIG_FILE", "")
	t.Setenv("DEX_S3_ENDPOINT", "")
	if _, _, _, err := loadDexConfig("127.0.0.1:7233"); err == nil {
		t.Fatal("loadDexConfig() accepted missing S3 config")
	}
}

func TestLoadDatabaseConfigRejectsMultipleHosts(t *testing.T) {
	t.Setenv("POSTGRES_SEEDS", "postgres-a,postgres-b")
	if _, err := loadDatabaseConfig(); err == nil {
		t.Fatal("loadDatabaseConfig() accepted multiple PostgreSQL hosts")
	}
}
