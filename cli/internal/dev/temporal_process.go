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
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

type temporalProcess = childProcess

func startTemporalProcess(cfg *Config, logs io.Writer) (*temporalProcess, error) {
	temporalPath, err := exec.LookPath("temporal")
	if err != nil {
		return nil, fmt.Errorf("Temporal CLI was not found; reinstall dexcli with Homebrew: %w", err)
	}
	arguments := []string{
		"server", "start-dev",
		"--ip", cfg.BindAddress,
		"--port", strconv.Itoa(cfg.TemporalPort),
		"--ui-ip", cfg.BindAddress,
		"--ui-port", strconv.Itoa(cfg.TemporalUIPort),
	}
	if cfg.TemporalNamespace != defaultTemporalNamespace {
		arguments = append(arguments, "--namespace", cfg.TemporalNamespace)
	}
	if cfg.SQLiteDBFilename != "" {
		arguments = append(arguments, "--db-filename", cfg.SQLiteDBFilename)
	}
	if cfg.VerboseEngineLog {
		arguments = append(arguments, "--log-level", "info")
	}
	command := exec.Command(temporalPath, arguments...)
	return startChildProcess(command, logs, "workflow backend", false)
}

type serverLogDir struct {
	directory   string
	engineLog   *os.File
	isEphemeral bool
}

func openServerLogDir(cfg *Config) (*serverLogDir, error) {
	directory := strings.TrimSpace(cfg.LogDirectory)
	isEphemeral := directory == ""
	if isEphemeral {
		created, err := os.MkdirTemp("", "dexcli-logs-*")
		if err != nil {
			return nil, fmt.Errorf("create server log directory: %w", err)
		}
		directory = created
	} else {
		absolute, err := filepath.Abs(directory)
		if err != nil {
			return nil, fmt.Errorf("resolve server log directory: %w", err)
		}
		if err := os.MkdirAll(absolute, 0o700); err != nil {
			return nil, fmt.Errorf("create server log directory: %w", err)
		}
		directory = absolute
	}
	cfg.LogDirectory = directory
	logs := &serverLogDir{directory: directory, isEphemeral: isEphemeral}
	if cfg.Engine == "zd" || cfg.ExternalTemporalAddress != "" {
		return logs, nil
	}
	file, err := os.OpenFile(cfg.temporalEngineLogPath(), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("open workflow engine log file: %w", err), logs.Close(isEphemeral))
	}
	logs.engineLog = file
	return logs, nil
}

func (d *serverLogDir) Close(shouldRemove bool) error {
	var err error
	if d.engineLog != nil {
		err = d.engineLog.Close()
		d.engineLog = nil
	}
	if shouldRemove {
		err = errors.Join(err, os.RemoveAll(d.directory))
	}
	return err
}
