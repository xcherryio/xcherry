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
	"sync"
	"syscall"
	"time"
)

type temporalProcess struct {
	command *exec.Cmd
	done    chan struct{}
	mu      sync.Mutex
	waitErr error
}

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
	if logs == nil {
		logs = io.Discard
	}
	command := exec.Command(temporalPath, arguments...)
	command.Stdout = logs
	command.Stderr = logs
	if err := command.Start(); err != nil {
		return nil, fmt.Errorf("start workflow backend: %w", err)
	}
	process := &temporalProcess{
		command: command,
		done:    make(chan struct{}),
	}
	go process.wait()
	return process, nil
}

func (p *temporalProcess) wait() {
	err := p.command.Wait()
	p.mu.Lock()
	p.waitErr = err
	p.mu.Unlock()
	close(p.done)
}

func (p *temporalProcess) Done() <-chan struct{} {
	return p.done
}

func (p *temporalProcess) Err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.waitErr
}

func (p *temporalProcess) Stop(timeout time.Duration) error {
	select {
	case <-p.done:
		return nil
	default:
	}
	if err := p.command.Process.Signal(syscall.SIGTERM); err != nil {
		select {
		case <-p.done:
			return nil
		default:
			return fmt.Errorf("signal workflow backend: %w", err)
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-p.done:
		return nil
	case <-timer.C:
		if err := p.command.Process.Kill(); err != nil {
			select {
			case <-p.done:
				return nil
			default:
				return fmt.Errorf("kill workflow backend: %w", err)
			}
		}
		<-p.done
		return nil
	}
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
