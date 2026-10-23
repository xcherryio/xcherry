// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package dev

import (
	"bytes"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/superdurable/dex/config"
)

func TestAttributeStoreConfigFlagLoadsStandardDexYAML(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "attribute-store.yaml")
	contents := []byte(`attributeStore:
  stores:
    entityStore:
      type: postgres
      dsn: postgres://entity_store:entity_store@localhost:55432/entity_store?sslmode=disable
      tableName: public.user_profiles
  syncBatchSize: 25
`)
	if err := os.WriteFile(configPath, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	cliConfig, err := parseConfig([]string{"--attribute-store-config", configPath}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	attributeStore, err := cliConfig.loadAttributeStoreConfig()
	if err != nil {
		t.Fatal(err)
	}
	if attributeStore.SyncBatchSize != 25 {
		t.Fatalf("unexpected sync batch size: %d", attributeStore.SyncBatchSize)
	}
	store := attributeStore.Stores["entityStore"]
	if store.Type != config.AttributeStoreTypePostgres ||
		store.DSN != "postgres://entity_store:entity_store@localhost:55432/entity_store?sslmode=disable" ||
		store.TableName != "public.user_profiles" {
		t.Fatalf("unexpected store config: %+v", store)
	}
}

func TestAttributeStoreConfigFlagLoadsWarehouseAndMongoDB(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "attribute-store.yaml")
	contents := []byte(`attributeStore:
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
	if err := os.WriteFile(configPath, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	cliConfig, err := parseConfig([]string{"--attribute-store-config", configPath}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	attributeStore, err := cliConfig.loadAttributeStoreConfig()
	if err != nil {
		t.Fatal(err)
	}
	if attributeStore.Stores["lakehouse"].FlowIDColumn != "flow_id" ||
		attributeStore.Stores["warehouse"].Type != config.AttributeStoreTypeSnowflake ||
		attributeStore.Stores["documents"].CollectionName != "flow_attributes" {
		t.Fatalf("unexpected Attribute Store config: %+v", attributeStore.Stores)
	}
}

func TestAttributeStoreConfigRejectsUnknownFields(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "attribute-store.yaml")
	if err := os.WriteFile(configPath, []byte("attributeStore:\n  unknown: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cliConfig := &Config{AttributeStoreConfigPath: configPath}
	if _, err := cliConfig.loadAttributeStoreConfig(); err == nil {
		t.Fatal("expected unknown Attribute Store field to fail")
	}
}

func TestAttributeStoreConfigRequiresStore(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "attribute-store.yaml")
	if err := os.WriteFile(configPath, []byte("attributeStore: {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cliConfig := &Config{AttributeStoreConfigPath: configPath}
	if _, err := cliConfig.loadAttributeStoreConfig(); err == nil {
		t.Fatal("expected empty Attribute Store config to fail")
	}
}

func TestServerLogFolderFlag(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "logs")
	cfg, err := parseConfig([]string{"--server-log-folder", directory}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.LogDirectory != directory {
		t.Fatalf("unexpected server log folder: %q", cfg.LogDirectory)
	}
}

func TestFlowRenderingDirectoryFlag(t *testing.T) {
	directory := t.TempDir()
	cfg, err := parseConfig([]string{"--flow-rendering-dir", directory}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.FlowRenderingDirectory != directory {
		t.Fatalf("unexpected Flow rendering directory: %q", cfg.FlowRenderingDirectory)
	}
}

func TestConnectorConfigDirectoryFlagResolvesAbsolutePath(t *testing.T) {
	directory := filepath.Join("testdata", "connector-secrets")
	cfg, err := parseConfig([]string{"--connector-config-dir", directory}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	expected, err := filepath.Abs(directory)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnectorConfigDirectory != expected {
		t.Fatalf("connector config directory = %q, want %q", cfg.ConnectorConfigDirectory, expected)
	}
}

func TestConnectorConfigDirectoryDefaultsUnderDexHome(t *testing.T) {
	cfg, err := parseConfig(nil, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	homeDirectory, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	expected := filepath.Join(homeDirectory, ".dex", "connectors")
	if cfg.ConnectorConfigDirectory != expected {
		t.Fatalf("connector config directory = %q, want %q", cfg.ConnectorConfigDirectory, expected)
	}
}

func TestConnectorReleaseOverrideFlagResolvesRepeatableDirectories(t *testing.T) {
	firstDirectory := filepath.Join("testdata", "slack-release")
	secondDirectory := filepath.Join("testdata", "gmail-release")
	cfg, err := parseConfig([]string{
		"--connector-release-override", "slack=" + firstDirectory,
		"--connector-release-override", "gmail=" + secondDirectory,
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	firstAbsolute, _ := filepath.Abs(firstDirectory)
	secondAbsolute, _ := filepath.Abs(secondDirectory)
	if cfg.ConnectorReleaseOverrides["slack"] != firstAbsolute || cfg.ConnectorReleaseOverrides["gmail"] != secondAbsolute {
		t.Fatalf("Connector release overrides = %+v", cfg.ConnectorReleaseOverrides)
	}
}

func TestConnectorReleaseOverrideFlagRejectsInvalidAndDuplicateIDs(t *testing.T) {
	for _, arguments := range [][]string{
		{"--connector-release-override", "Slack=/tmp/release"},
		{"--connector-release-override", "slack"},
		{"--connector-release-override", "slack=/tmp/one", "--connector-release-override", "slack=/tmp/two"},
	} {
		if _, err := parseConfig(arguments, &bytes.Buffer{}); err == nil {
			t.Fatalf("parseConfig(%v) succeeded", arguments)
		}
	}
}

func TestVerboseEngineLogFlag(t *testing.T) {
	cfg, err := parseConfig([]string{"--verbose-engine-log"}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.VerboseEngineLog {
		t.Fatal("expected verbose engine logs to be enabled")
	}
}

func TestVerboseEngineLogRejectedWithExternalAddress(t *testing.T) {
	_, err := parseConfig([]string{
		"--external-temporal-address", "127.0.0.1:7233",
		"--verbose-engine-log",
	}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("expected --verbose-engine-log to fail with --external-temporal-address")
	}
}

func TestSQLiteDBFilenameRejectedWithExternalAddress(t *testing.T) {
	_, err := parseConfig([]string{
		"--external-temporal-address", "127.0.0.1:7233",
		"--sqlite-db-filename", "dex.sqlite.db",
	}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("expected --sqlite-db-filename to fail with --external-temporal-address")
	}
}

func TestExternalTemporalNamespaceRequiresAddress(t *testing.T) {
	_, err := parseConfig([]string{"--external-temporal-namespace", "custom"}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("expected --external-temporal-namespace to fail without --external-temporal-address")
	}
}

func TestHelpListsFlagsInOrder(t *testing.T) {
	var output bytes.Buffer
	_, err := parseConfig([]string{"-h"}, &output)
	if err != nil && !errors.Is(err, flag.ErrHelp) {
		t.Fatal(err)
	}
	text := output.String()
	last := -1
	for _, name := range flagOrder {
		idx := strings.Index(text, "-"+name)
		if idx < 0 {
			t.Fatalf("missing flag %s in help:\n%s", name, text)
		}
		if idx < last {
			t.Fatalf("flag %s is out of order in help:\n%s", name, text)
		}
		last = idx
	}
	for _, name := range []string{
		"-temporal-port",
		"-temporal-ui-port",
		"-temporal-address",
		"-temporal-db-filename",
		"-temporal-namespace",
		"-temporal-log-file",
		"-log-dir",
	} {
		if strings.Contains(text, "  "+name+" ") || strings.Contains(text, "  "+name+"\n") {
			t.Fatalf("removed flag %s still documented:\n%s", name, text)
		}
	}
	if !strings.Contains(text, "open Dex Web after startup (default true)") {
		t.Fatalf("missing --open default in help:\n%s", text)
	}
}

func TestOpenBrowserDefaultsTrue(t *testing.T) {
	cfg, err := parseConfig(nil, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.OpenBrowser {
		t.Fatal("expected --open to default true")
	}
}

func TestOpenFalseDisablesBrowser(t *testing.T) {
	cfg, err := parseConfig([]string{"--open=false"}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.OpenBrowser {
		t.Fatal("expected --open=false to disable Dex Web")
	}
}

func TestOpenFalseSeparateTokenIsRejected(t *testing.T) {
	_, err := parseConfig([]string{"--open", "false"}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("expected --open false to fail")
	}
}

func TestRemovedTemporalPortFlagsAreRejected(t *testing.T) {
	for _, name := range []string{
		"temporal-port",
		"temporal-ui-port",
		"temporal-address",
		"temporal-db-filename",
		"temporal-namespace",
		"temporal-log-file",
		"log-dir",
	} {
		if _, err := parseConfig([]string{"--" + name, "1"}, &bytes.Buffer{}); err == nil {
			t.Fatalf("expected unknown flag %s to fail", name)
		}
	}
}

func TestWriteTemporalStartupRecordIncludesPortsAndDatabase(t *testing.T) {
	cfg := testConfig(t)
	cfg.TemporalPort = 7234
	cfg.TemporalUIPort = 8234
	cfg.SQLiteDBFilename = filepath.Join(cfg.StateDirectory, "dev", "7234", localSQLiteFileName)
	var output bytes.Buffer
	if err := cfg.writeTemporalStartupRecord(&output); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	if !strings.Contains(text, "127.0.0.1:7234") {
		t.Fatalf("missing Temporal gRPC address: %s", text)
	}
	if !strings.Contains(text, "http://127.0.0.1:8234") {
		t.Fatalf("missing Temporal Web address: %s", text)
	}
	if !strings.Contains(text, filepath.Join(cfg.StateDirectory, "dev", "7234")) {
		t.Fatalf("missing Temporal DB directory: %s", text)
	}
}
