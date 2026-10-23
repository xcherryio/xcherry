// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package dev

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/superdurable/dex/config"
)

const (
	defaultBindAddress        = "127.0.0.1"
	defaultDexPort            = 8801
	defaultWebPort            = 8802
	defaultTemporalPort       = 7233
	defaultTemporalUIPort     = 8233
	defaultTemporalNamespace  = "default"
	localSQLiteFileName       = "dex.sqlite.db"
	localBlobStoreName        = "dex.blobs"
	dexServerLogFileName      = "dex-server.log"
	temporalEngineLogFileName = "temporal-engine-server.log"
)

var flagOrder = []string{
	"attribute-store-config",
	"bind-address",
	"engine",
	"zd-binary",
	"blob-store-dir",
	"dex-port",
	"flow-rendering-dir",
	"connector-config-dir",
	"connector-release-override",
	"open",
	"web-port",
	"sqlite-db-filename",
	"server-log-folder",
	"verbose-engine-log",
	"external-temporal-address",
	"external-temporal-namespace",
}

type Config struct {
	// Engine defaults to temporal and selects the local Dex Server implementation.
	Engine string
	// ZDBinary defaults to DEX_SERVER_ZD_BINARY or dex-server-zd on PATH.
	ZDBinary string
	// BindAddress defaults to 127.0.0.1 for all owned listeners.
	BindAddress string
	// DexPort defaults to 8801 for the Dex gRPC server.
	DexPort int
	// WebPort defaults to 8802 for Dex Web.
	WebPort int
	// ExternalTemporalAddress selects external Temporal when non-empty. Default is local mode.
	ExternalTemporalAddress string
	// TemporalNamespace defaults to default.
	TemporalNamespace string
	// TemporalPort defaults to 7233 in local mode.
	TemporalPort int
	// TemporalUIPort defaults to 8233 in local mode.
	TemporalUIPort int
	// SQLiteDBFilename defaults to $HOME/.dex/dev/<temporal-port>/dex.sqlite.db in local mode.
	SQLiteDBFilename string
	// LogDirectory defaults to a temp directory deleted on clean exit.
	LogDirectory string
	// VerboseEngineLog defaults to false and writes Temporal Server info logs to temporal-engine-server.log in local mode.
	VerboseEngineLog bool
	// StateDirectory defaults to $HOME/.dex and stores auto-assigned Temporal SQLite files.
	StateDirectory string
	// BlobStoreDirectory defaults to $HOME/.dex/blobs unless SQLiteDBFilename selects its adjacent dex.blobs store.
	BlobStoreDirectory string
	// AttributeStoreConfigPath defaults empty and loads Attribute Store settings from standard Dex YAML when set.
	AttributeStoreConfigPath string
	// FlowRenderingDirectory defaults empty and supplies Flow Definition Graph JSON files to Dex Web.
	FlowRenderingDirectory string
	// ConnectorConfigDirectory defaults to $HOME/.dex/connectors and stores local connector credentials.
	ConnectorConfigDirectory string
	// ConnectorReleaseOverrides maps Connector IDs to local release artifact directories.
	ConnectorReleaseOverrides map[string]string
	// OpenBrowser defaults true and opens Dex Web after readiness.
	OpenBrowser bool
	// StartupTimeout defaults to 45 seconds.
	StartupTimeout time.Duration
	// ShutdownTimeout defaults to 10 seconds.
	ShutdownTimeout time.Duration

	explicitLocalFlags map[string]bool
	// blobStoreDirectoryDefault defaults true and allows SQLiteDBFilename to select its adjacent store.
	blobStoreDirectoryDefault bool
	version                   string
}

func parseConfig(args []string, output io.Writer) (*Config, error) {
	cfg, err := defaultConfig()
	if err != nil {
		return nil, err
	}
	flags := flag.NewFlagSet("dexcli dev", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.StringVar(
		&cfg.AttributeStoreConfigPath,
		"attribute-store-config",
		"",
		"Dex YAML file supplying attributeStore settings",
	)
	flags.StringVar(&cfg.BindAddress, "bind-address", cfg.BindAddress, "address for local services")
	flags.StringVar(&cfg.Engine, "engine", cfg.Engine, "Dex Server engine: temporal or zd")
	flags.StringVar(&cfg.ZDBinary, "zd-binary", cfg.ZDBinary, "dex-server-zd executable (default DEX_SERVER_ZD_BINARY or PATH)")
	flags.StringVar(
		&cfg.BlobStoreDirectory,
		"blob-store-dir",
		cfg.BlobStoreDirectory,
		"Dex blob storage directory",
	)
	flags.IntVar(&cfg.DexPort, "dex-port", cfg.DexPort, "Dex gRPC port")
	flags.StringVar(
		&cfg.FlowRenderingDirectory,
		"flow-rendering-dir",
		"",
		"directory containing Flow Definition Graph JSON files",
	)
	flags.StringVar(
		&cfg.ConnectorConfigDirectory,
		"connector-config-dir",
		cfg.ConnectorConfigDirectory,
		"directory containing local Connector configuration",
	)
	connectorReleaseOverrides := connectorReleaseOverrideFlag{values: cfg.ConnectorReleaseOverrides}
	flags.Var(
		&connectorReleaseOverrides,
		"connector-release-override",
		"local Connector release override as connector-id=artifact-directory (repeatable)",
	)
	flags.BoolVar(&cfg.OpenBrowser, "open", true, "open Dex Web after startup")
	flags.IntVar(&cfg.WebPort, "web-port", cfg.WebPort, "Dex Web port")
	flags.StringVar(
		&cfg.SQLiteDBFilename,
		"sqlite-db-filename",
		"",
		"local SQLite file (default $HOME/.dex/dev/<port>/dex.sqlite.db)",
	)
	flags.StringVar(
		&cfg.LogDirectory,
		"server-log-folder",
		"",
		"keep server logs in this folder (default temp folder, deleted on exit)",
	)
	flags.BoolVar(
		&cfg.VerboseEngineLog,
		"verbose-engine-log",
		false,
		"write Temporal Server logs to temporal-engine-server.log",
	)
	flags.StringVar(&cfg.ExternalTemporalAddress, "external-temporal-address", "", "external Temporal host:port")
	flags.StringVar(
		&cfg.TemporalNamespace,
		"external-temporal-namespace",
		cfg.TemporalNamespace,
		"external Temporal namespace",
	)
	flags.Usage = func() {
		fmt.Fprintln(output, "Usage: dexcli dev [flags]")
		printFlags(output, flags, flagOrder)
	}
	if err := flags.Parse(args); err != nil {
		return nil, err
	}
	if flags.NArg() != 0 {
		return nil, fmt.Errorf("unexpected arguments: %s", strings.Join(flags.Args(), " "))
	}
	cfg.ConnectorReleaseOverrides = connectorReleaseOverrides.values
	cfg.explicitLocalFlags = make(map[string]bool)
	flags.Visit(func(item *flag.Flag) {
		switch item.Name {
		case "dex-port", "web-port", "sqlite-db-filename", "server-log-folder", "verbose-engine-log", "external-temporal-address", "external-temporal-namespace", "attribute-store-config", "zd-binary":
			cfg.explicitLocalFlags[item.Name] = true
		case "blob-store-dir":
			cfg.explicitLocalFlags[item.Name] = true
			cfg.blobStoreDirectoryDefault = false
		}
	})
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func defaultConfig() (*Config, error) {
	stateDirectory, err := defaultStateDirectory()
	if err != nil {
		return nil, err
	}
	return &Config{
		Engine:                    "temporal",
		ZDBinary:                  os.Getenv("DEX_SERVER_ZD_BINARY"),
		BindAddress:               defaultBindAddress,
		DexPort:                   defaultDexPort,
		WebPort:                   defaultWebPort,
		TemporalNamespace:         defaultTemporalNamespace,
		TemporalPort:              defaultTemporalPort,
		TemporalUIPort:            defaultTemporalUIPort,
		StateDirectory:            stateDirectory,
		BlobStoreDirectory:        filepath.Join(stateDirectory, "blobs"),
		ConnectorConfigDirectory:  filepath.Join(stateDirectory, "connectors"),
		ConnectorReleaseOverrides: make(map[string]string),
		OpenBrowser:               true,
		StartupTimeout:            45 * time.Second,
		ShutdownTimeout:           10 * time.Second,
		explicitLocalFlags:        make(map[string]bool),
		blobStoreDirectoryDefault: true,
	}, nil
}

func defaultStateDirectory() (string, error) {
	homeDirectory, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home directory for Dex state: %w", err)
	}
	return filepath.Join(homeDirectory, ".dex"), nil
}

func defaultBlobStoreDirectory() (string, error) {
	stateDirectory, err := defaultStateDirectory()
	if err != nil {
		return "", err
	}
	return filepath.Join(stateDirectory, "blobs"), nil
}

func (c *Config) validate() error {
	if c.Engine != "temporal" && c.Engine != "zd" {
		return fmt.Errorf("engine must be temporal or zd: %q", c.Engine)
	}
	if c.Engine == "zd" {
		for _, name := range []string{"external-temporal-address", "external-temporal-namespace", "sqlite-db-filename", "verbose-engine-log", "attribute-store-config", "blob-store-dir"} {
			if c.isExplicit(name) {
				return fmt.Errorf("--%s cannot be used with --engine zd", name)
			}
		}
		if c.ZDBinary == "" {
			c.ZDBinary = "dex-server-zd"
		}
		binaryPath, err := exec.LookPath(c.ZDBinary)
		if err != nil {
			return fmt.Errorf("dex-server-zd binary %q was not found; use --zd-binary or DEX_SERVER_ZD_BINARY: %w", c.ZDBinary, err)
		}
		c.ZDBinary = binaryPath
	} else if c.isExplicit("zd-binary") {
		return fmt.Errorf("--zd-binary requires --engine zd")
	}
	if net.ParseIP(c.BindAddress) == nil {
		return fmt.Errorf("bind address must be an IP address: %q", c.BindAddress)
	}
	if strings.TrimSpace(c.BlobStoreDirectory) == "" {
		return fmt.Errorf("blob store directory is required")
	}
	c.FlowRenderingDirectory = strings.TrimSpace(c.FlowRenderingDirectory)
	c.ConnectorConfigDirectory = strings.TrimSpace(c.ConnectorConfigDirectory)
	if c.ConnectorConfigDirectory == "" {
		return fmt.Errorf("connector config directory is required")
	}
	absoluteConnectorConfigDirectory, err := filepath.Abs(c.ConnectorConfigDirectory)
	if err != nil {
		return fmt.Errorf("resolve connector config directory: %w", err)
	}
	c.ConnectorConfigDirectory = absoluteConnectorConfigDirectory
	for connectorID, directory := range c.ConnectorReleaseOverrides {
		absoluteDirectory, err := filepath.Abs(strings.TrimSpace(directory))
		if err != nil {
			return fmt.Errorf("resolve Connector release override %q: %w", connectorID, err)
		}
		c.ConnectorReleaseOverrides[connectorID] = absoluteDirectory
	}
	for name, port := range map[string]int{
		"dex-port":         c.DexPort,
		"web-port":         c.WebPort,
		"Temporal port":    c.TemporalPort,
		"Temporal UI port": c.TemporalUIPort,
	} {
		if port < 1 || port > 65535 {
			return fmt.Errorf("%s must be between 1 and 65535", name)
		}
	}
	if c.TemporalNamespace == "" {
		return fmt.Errorf("temporal namespace is required")
	}
	if c.Engine == "temporal" && c.ExternalTemporalAddress != "" {
		if strings.Contains(c.ExternalTemporalAddress, "://") {
			return fmt.Errorf("external Temporal address must be host:port, not a URL")
		}
		if _, _, err := net.SplitHostPort(c.ExternalTemporalAddress); err != nil {
			return fmt.Errorf("external Temporal address must be host:port: %w", err)
		}
		for _, name := range []string{"sqlite-db-filename", "verbose-engine-log"} {
			if c.explicitLocalFlags[name] {
				return fmt.Errorf("--%s cannot be used with --external-temporal-address", name)
			}
		}
	} else if c.Engine == "temporal" && c.explicitLocalFlags["external-temporal-namespace"] {
		return fmt.Errorf("--external-temporal-namespace cannot be used without --external-temporal-address")
	}
	if c.explicitLocalFlags["server-log-folder"] {
		c.LogDirectory = strings.TrimSpace(c.LogDirectory)
		if c.LogDirectory == "" {
			return fmt.Errorf("server log folder is required")
		}
	}
	return validateDistinctAddresses(c.ownedAddresses())
}

type connectorReleaseOverrideFlag struct {
	values map[string]string
}

func (value *connectorReleaseOverrideFlag) String() string {
	return ""
}

func (value *connectorReleaseOverrideFlag) Set(raw string) error {
	connectorID, directory, found := strings.Cut(raw, "=")
	connectorID = strings.TrimSpace(connectorID)
	directory = strings.TrimSpace(directory)
	if !found || !validConnectorOverrideID(connectorID) || directory == "" {
		return fmt.Errorf("Connector release override must be connector-id=artifact-directory")
	}
	if value.values == nil {
		value.values = make(map[string]string)
	}
	if _, exists := value.values[connectorID]; exists {
		return fmt.Errorf("Connector release override %q is duplicated", connectorID)
	}
	value.values[connectorID] = directory
	return nil
}

func validConnectorOverrideID(value string) bool {
	if len(value) < 2 || len(value) > 63 || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for _, character := range value[1:] {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '-' {
			return false
		}
	}
	return true
}

func (c *Config) ownedAddresses() map[string]string {
	addresses := map[string]string{
		"Dex Server": net.JoinHostPort(c.BindAddress, strconv.Itoa(c.DexPort)),
		"Dex Web":    net.JoinHostPort(c.BindAddress, strconv.Itoa(c.WebPort)),
	}
	if c.Engine == "temporal" && c.ExternalTemporalAddress == "" {
		addresses["Temporal"] = net.JoinHostPort(c.BindAddress, strconv.Itoa(c.TemporalPort))
		addresses["Temporal Web"] = net.JoinHostPort(c.BindAddress, strconv.Itoa(c.TemporalUIPort))
	}
	return addresses
}

func (c *Config) temporalAddress() string {
	if c.ExternalTemporalAddress != "" {
		return c.ExternalTemporalAddress
	}
	return net.JoinHostPort(c.BindAddress, strconv.Itoa(c.TemporalPort))
}

func (c *Config) loadAttributeStoreConfig() (config.AttributeStoreConfig, error) {
	if c.AttributeStoreConfigPath == "" {
		return config.AttributeStoreConfig{}, nil
	}
	serverConfig, err := config.NewConfig(c.AttributeStoreConfigPath)
	if err != nil {
		return config.AttributeStoreConfig{}, fmt.Errorf("load Attribute Store config: %w", err)
	}
	if err := serverConfig.AttributeStore.Validate(); err != nil {
		return config.AttributeStoreConfig{}, fmt.Errorf("validate Attribute Store config: %w", err)
	}
	if len(serverConfig.AttributeStore.Stores) == 0 {
		return config.AttributeStoreConfig{}, fmt.Errorf("Attribute Store config does not define any stores")
	}
	return serverConfig.AttributeStore, nil
}

func validateDistinctAddresses(addresses map[string]string) error {
	owners := make(map[string]string, len(addresses))
	for name, address := range addresses {
		if owner, exists := owners[address]; exists {
			return fmt.Errorf("%s and %s cannot both use %s", owner, name, address)
		}
		owners[address] = name
	}
	return nil
}

func (c *Config) isExplicit(flagName string) bool {
	return c.explicitLocalFlags[flagName]
}

func (c *Config) autoSQLiteDBFilename() string {
	return filepath.Join(c.StateDirectory, "dev", strconv.Itoa(c.TemporalPort), localSQLiteFileName)
}

func (c *Config) adjacentBlobStoreDirectory() string {
	return filepath.Join(filepath.Dir(c.SQLiteDBFilename), localBlobStoreName)
}

func (c *Config) dexServerLogPath() string {
	return filepath.Join(c.LogDirectory, dexServerLogFileName)
}

func (c *Config) temporalEngineLogPath() string {
	return filepath.Join(c.LogDirectory, temporalEngineLogFileName)
}

func (c *Config) writeTemporalStartupRecord(logs io.Writer) error {
	database := c.sqliteDBDirectory()
	if database == "" {
		database = "in-memory"
	}
	_, err := fmt.Fprintf(
		logs,
		"Temporal:     %s\nTemporal Web: %s\nTemporal DB:  %s\n",
		c.temporalAddress(),
		"http://"+net.JoinHostPort(c.BindAddress, strconv.Itoa(c.TemporalUIPort)),
		database,
	)
	return err
}

func (c *Config) sqliteDBDirectory() string {
	if c.SQLiteDBFilename == "" {
		return ""
	}
	return filepath.Dir(c.SQLiteDBFilename)
}

func printFlags(output io.Writer, flags *flag.FlagSet, names []string) {
	for _, name := range names {
		item := flags.Lookup(name)
		if item == nil {
			panic("missing flag " + name)
		}
		typeName, usage := flag.UnquoteUsage(item)
		fmt.Fprintf(output, "  -%s", item.Name)
		if typeName != "" {
			fmt.Fprintf(output, " %s", typeName)
		}
		fmt.Fprintf(output, "\n        %s", usage)
		if item.DefValue != "" && item.DefValue != "false" && item.DefValue != "0" {
			if typeName == "string" {
				fmt.Fprintf(output, " (default %q)", item.DefValue)
			} else {
				fmt.Fprintf(output, " (default %s)", item.DefValue)
			}
		}
		fmt.Fprintln(output)
	}
}
