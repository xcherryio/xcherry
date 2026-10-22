// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package command

import (
	"flag"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
)

const defaultServerAddress = "127.0.0.1:8801"

type options struct {
	server         string
	credentialFile string
	credential     string
	hasFile        bool
	hasCredential  bool
	output         string
	timeout        time.Duration
	noHydrate      bool
}

func defaultOptions(lookupEnv func(string) (string, bool)) options {
	serverValue, _ := lookupEnv("DEX_FLOW_SERVICE_ADDRESS")
	server := strings.TrimSpace(serverValue)
	if server == "" {
		server = defaultServerAddress
	}
	credentialFile, hasFile := lookupEnv("DEX_CREDENTIAL_FILE")
	credentialValue, hasCredential := lookupEnv("DEX_CREDENTIAL")
	hasFile = hasFile && strings.TrimSpace(credentialFile) != ""
	hasCredential = hasCredential && strings.TrimSpace(credentialValue) != ""
	if !hasFile {
		credentialFile = ""
	}
	if !hasCredential {
		credentialValue = ""
	}
	return options{
		server:         server,
		credentialFile: credentialFile,
		credential:     credentialValue,
		hasFile:        hasFile,
		hasCredential:  hasCredential,
		output:         "json",
		timeout:        30 * time.Second,
	}
}

func parseRootOptions(args []string, options *options) ([]string, error) {
	flags := newFlagSet("dexcli", io.Discard)
	addCommonFlags(flags, options)
	if err := flags.Parse(args); err != nil {
		return nil, err
	}
	return flags.Args(), nil
}

func addCommonFlags(flags *flag.FlagSet, options *options) {
	flags.StringVar(&options.server, "server", options.server, "Dex FlowService host:port")
	flags.Var(credentialFileFlag{options: options}, "credential-file", "bearer token file; use DEX_CREDENTIAL for a token value (no token flag, to avoid shell history and process listings)")
	flags.StringVar(&options.output, "output", options.output, "json or table")
	flags.DurationVar(&options.timeout, "timeout", options.timeout, "request timeout; 0 disables")
	flags.BoolVar(&options.noHydrate, "no-hydrate", options.noHydrate, "return blob references")
}

type credentialFileFlag struct{ options *options }

func (c credentialFileFlag) String() string {
	// flag.PrintDefaults calls String on a zero value to detect defaults.
	if c.options == nil {
		return ""
	}
	return c.options.credentialFile
}

func (c credentialFileFlag) Set(path string) error {
	c.options.credentialFile = path
	c.options.hasFile = true
	return nil
}

func newFlagSet(name string, output io.Writer) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(output)
	return flags
}

func (o options) validate() error {
	if strings.Contains(o.server, "://") {
		return fmt.Errorf("server must be host:port, not a URL")
	}
	if _, _, err := net.SplitHostPort(o.server); err != nil {
		return fmt.Errorf("server must be host:port: %w", err)
	}
	switch o.output {
	case "json", "table":
	default:
		return fmt.Errorf("output must be json or table")
	}
	if o.timeout < 0 {
		return fmt.Errorf("timeout must not be negative")
	}
	return nil
}
