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
	"os"
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
	apiKeyFile     string
	apiKey         string
	hasAPIKeyFile  bool
	hasAPIKey      bool
	tokenURL       string
	serverID       string
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
	apiKeyValue, hasAPIKey := lookupEnv("DEX_API_KEY")
	hasAPIKey = hasAPIKey && strings.TrimSpace(apiKeyValue) != ""
	if !hasAPIKey {
		apiKeyValue = ""
	}
	tokenURL, _ := lookupEnv("DEX_TOKEN_URL")
	serverID, _ := lookupEnv("DEX_SERVER_ID")
	return options{
		server:         server,
		credentialFile: credentialFile,
		credential:     credentialValue,
		hasFile:        hasFile,
		hasCredential:  hasCredential,
		apiKey:         apiKeyValue,
		hasAPIKey:      hasAPIKey,
		tokenURL:       strings.TrimSpace(tokenURL),
		serverID:       strings.TrimSpace(serverID),
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
	flags.Var(apiKeyFileFlag{options: options}, "api-key-file", "API key file exchanged at --token-url for access tokens; use DEX_API_KEY for a key value (no key flag, to avoid shell history and process listings)")
	flags.StringVar(&options.tokenURL, "token-url", options.tokenURL, "token endpoint that exchanges the API key; https, or http only for loopback hosts")
	flags.StringVar(&options.serverID, "server-id", options.serverID, "server ID that the token endpoint scopes access tokens to")
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

type apiKeyFileFlag struct{ options *options }

func (c apiKeyFileFlag) String() string {
	// flag.PrintDefaults calls String on a zero value to detect defaults.
	if c.options == nil {
		return ""
	}
	return c.options.apiKeyFile
}

func (c apiKeyFileFlag) Set(path string) error {
	c.options.apiKeyFile = path
	c.options.hasAPIKeyFile = true
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

func (o options) hasBearerCredentialSettings() bool {
	return o.hasFile || o.credentialFile != "" || o.hasCredential || o.credential != ""
}

func (o options) hasAPIKeySettings() bool {
	return o.hasAPIKeyFile || o.apiKeyFile != "" || o.hasAPIKey || o.apiKey != ""
}

// validateAPIKeySettings runs at connect time because global flags may also follow the subcommand.
func (o options) validateAPIKeySettings() error {
	if !o.hasAPIKeySettings() {
		if o.tokenURL != "" || o.serverID != "" {
			return fmt.Errorf("--token-url and --server-id (DEX_TOKEN_URL, DEX_SERVER_ID) require an API key from --api-key-file or DEX_API_KEY")
		}
		return nil
	}
	if o.hasBearerCredentialSettings() {
		return fmt.Errorf("--api-key-file and DEX_API_KEY cannot be combined with --credential-file, DEX_CREDENTIAL_FILE, or DEX_CREDENTIAL; configure one credential")
	}
	if o.tokenURL == "" {
		return fmt.Errorf("an API key requires --token-url or DEX_TOKEN_URL")
	}
	if o.serverID == "" {
		return fmt.Errorf("an API key requires --server-id or DEX_SERVER_ID")
	}
	if _, err := parseAPIKeyTokenURL(o.tokenURL); err != nil {
		return err
	}
	return nil
}

func (o options) readAPIKey() (string, error) {
	if !o.hasAPIKeyFile && o.apiKeyFile == "" {
		return strings.TrimSpace(o.apiKey), nil
	}
	if o.apiKeyFile == "" {
		return "", fmt.Errorf("API key file path is empty")
	}
	data, err := os.ReadFile(o.apiKeyFile)
	if err != nil {
		return "", fmt.Errorf("read API key file: %w", err)
	}
	apiKey := strings.TrimSpace(string(data))
	if apiKey == "" {
		return "", fmt.Errorf("API key file %s is empty", o.apiKeyFile)
	}
	return apiKey, nil
}
