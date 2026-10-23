// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package dev

import (
	"errors"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

type childProcess struct {
	command            *exec.Cmd
	name               string
	reportShutdownExit bool
	done               chan struct{}
	mu                 sync.Mutex
	waitErr            error
}

func startChildProcess(command *exec.Cmd, logs io.Writer, name string, reportShutdownExit bool) (*childProcess, error) {
	if logs == nil {
		logs = io.Discard
	}
	command.Stdout = logs
	command.Stderr = logs
	if err := command.Start(); err != nil {
		return nil, fmt.Errorf("start %s: %w", name, err)
	}
	process := &childProcess{
		command:            command,
		name:               name,
		reportShutdownExit: reportShutdownExit,
		done:               make(chan struct{}),
	}
	go process.wait()
	return process, nil
}

func (p *childProcess) wait() {
	err := p.command.Wait()
	p.mu.Lock()
	p.waitErr = err
	p.mu.Unlock()
	close(p.done)
}

func (p *childProcess) Done() <-chan struct{} {
	return p.done
}

func (p *childProcess) Err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.waitErr
}

func (p *childProcess) Stop(timeout time.Duration) error {
	select {
	case <-p.done:
		return nil
	default:
	}
	if err := p.command.Process.Signal(syscall.SIGTERM); err != nil {
		select {
		case <-p.done:
			return p.shutdownExitError()
		default:
			return fmt.Errorf("signal %s: %w", p.name, err)
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-p.done:
		return p.shutdownExitError()
	case <-timer.C:
		if err := p.command.Process.Kill(); err != nil {
			select {
			case <-p.done:
				return p.shutdownExitError()
			default:
				return fmt.Errorf("kill %s: %w", p.name, err)
			}
		}
		<-p.done
		return p.shutdownExitError()
	}
}

func (p *childProcess) shutdownExitError() error {
	if !p.reportShutdownExit {
		return nil
	}
	err := p.Err()
	if err == nil {
		return nil
	}
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		if status, ok := exitError.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			if status.Signal() == syscall.SIGTERM || status.Signal() == syscall.SIGKILL {
				return nil
			}
		}
	}
	return fmt.Errorf("%s exited during shutdown: %w", p.name, err)
}
