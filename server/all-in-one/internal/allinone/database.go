// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package allinone

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	schemaLockID int64 = 0x646578736368656d
	s3LockID     int64 = 0x6465787333627563
)

func initializeDatabase(ctx context.Context, cfg DatabaseConfig) error {
	if cfg.SkipCreate && cfg.SkipSchemaSetup {
		slog.Info("skipping Temporal database initialization")
		return nil
	}
	initCtx, cancel := context.WithTimeout(ctx, cfg.InitTimeout)
	defer cancel()
	return withDatabaseLock(initCtx, cfg, schemaLockID, func() error {
		if !cfg.SkipCreate {
			for _, database := range []string{cfg.TemporalDB, cfg.VisibilityDB} {
				if err := runSQLTool(initCtx, cfg, database, "create-database", "--defaultdb", cfg.AdminDatabase); err != nil {
					return err
				}
			}
		}
		if cfg.SkipSchemaSetup {
			return nil
		}
		for _, schema := range []struct {
			database string
			name     string
		}{
			{cfg.TemporalDB, "postgresql/v12/temporal"},
			{cfg.VisibilityDB, "postgresql/v12/visibility"},
		} {
			if err := runSQLTool(initCtx, cfg, schema.database, "setup-schema", "--version", "0.0"); err != nil {
				return err
			}
			if err := runSQLTool(initCtx, cfg, schema.database, "update-schema", "--schema-name", schema.name); err != nil {
				return err
			}
		}
		return nil
	})
}

func withDatabaseLock(ctx context.Context, cfg DatabaseConfig, lockID int64, fn func() error) error {
	conn, err := connectDatabase(ctx, cfg)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", lockID); err != nil {
		return fmt.Errorf("acquire PostgreSQL advisory lock: %w", err)
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = conn.Exec(unlockCtx, "SELECT pg_advisory_unlock($1)", lockID)
	}()
	return fn()
}

func connectDatabase(ctx context.Context, cfg DatabaseConfig) (*pgx.Conn, error) {
	connectionURL := &url.URL{
		Scheme: "postgres",
		Host:   net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
		Path:   cfg.AdminDatabase,
	}
	if cfg.User != "" {
		connectionURL.User = url.UserPassword(cfg.User, cfg.Password)
	}
	query := connectionURL.Query()
	if !cfg.TLS {
		query.Set("sslmode", "disable")
	} else if cfg.TLSVerifyHost {
		query.Set("sslmode", "verify-full")
	} else {
		query.Set("sslmode", "require")
	}
	if cfg.TLSCAFile != "" {
		query.Set("sslrootcert", cfg.TLSCAFile)
	}
	if cfg.TLSCertFile != "" {
		query.Set("sslcert", cfg.TLSCertFile)
	}
	if cfg.TLSKeyFile != "" {
		query.Set("sslkey", cfg.TLSKeyFile)
	}
	connectionURL.RawQuery = query.Encode()

	pgxConfig, err := pgx.ParseConfig(connectionURL.String())
	if err != nil {
		return nil, fmt.Errorf("parse PostgreSQL connection config: %w", err)
	}
	if pgxConfig.TLSConfig != nil && cfg.TLSServerName != "" {
		pgxConfig.TLSConfig.ServerName = cfg.TLSServerName
	}

	var lastErr error
	for ctx.Err() == nil {
		conn, err := pgx.ConnectConfig(ctx, pgxConfig)
		if err == nil {
			return conn, nil
		}
		lastErr = err
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
	if lastErr == nil {
		lastErr = ctx.Err()
	}
	return nil, fmt.Errorf("connect to PostgreSQL: %w", lastErr)
}

func runSQLTool(ctx context.Context, cfg DatabaseConfig, database string, args ...string) error {
	toolPath := cfg.SQLToolPath
	if toolPath == "" {
		executable, err := os.Executable()
		if err != nil {
			return fmt.Errorf("find executable: %w", err)
		}
		toolPath = filepath.Join(filepath.Dir(executable), "temporal-sql-tool")
	}
	command := exec.CommandContext(ctx, toolPath, args...)
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	command.Env = append(os.Environ(),
		"SQL_HOST="+cfg.Host,
		"SQL_PORT="+strconv.Itoa(cfg.Port),
		"SQL_USER="+cfg.User,
		"SQL_PASSWORD="+cfg.Password,
		"SQL_DATABASE="+database,
		"SQL_PLUGIN="+cfg.Plugin,
		"SQL_TLS="+strconv.FormatBool(cfg.TLS),
		"SQL_TLS_CERT_FILE="+cfg.TLSCertFile,
		"SQL_TLS_KEY_FILE="+cfg.TLSKeyFile,
		"SQL_TLS_CA_FILE="+cfg.TLSCAFile,
		"SQL_TLS_SERVER_NAME="+cfg.TLSServerName,
		"SQL_TLS_DISABLE_HOST_VERIFICATION="+strconv.FormatBool(!cfg.TLSVerifyHost),
	)
	slog.Info("running Temporal schema command", "database", database, "command", args[0])
	if err := command.Run(); err != nil {
		return fmt.Errorf("temporal-sql-tool %s for %s: %w", args[0], database, err)
	}
	return nil
}
