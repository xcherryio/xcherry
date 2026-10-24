// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package command

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
)

type App struct {
	stdin           io.Reader
	stdout          io.Writer
	stderr          io.Writer
	lookupEnv       func(string) (string, bool)
	openBrowser     func(string) error
	artifactVersion string
}

func NewApp(stdin io.Reader, stdout io.Writer, stderr io.Writer, artifactVersions ...string) *App {
	if stdin == nil {
		panic("command stdin must not be nil")
	}
	if stdout == nil {
		panic("command stdout must not be nil")
	}
	if stderr == nil {
		panic("command stderr must not be nil")
	}
	artifactVersion := "dev"
	if len(artifactVersions) > 1 {
		panic("command accepts at most one artifact version")
	}
	if len(artifactVersions) == 1 {
		artifactVersion = artifactVersions[0]
	}
	return &App{
		stdin:           stdin,
		stdout:          stdout,
		stderr:          stderr,
		lookupEnv:       os.LookupEnv,
		openBrowser:     openVisualizationBrowser,
		artifactVersion: artifactVersion,
	}
}

func (a *App) Execute(ctx context.Context, args []string) error {
	options := defaultOptions(a.lookupEnv)
	remaining, err := parseRootOptions(args, &options)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			a.printUsage()
			return nil
		}
		return newUsageError("dexcli", err)
	}
	if len(remaining) == 0 {
		a.printUsage()
		return nil
	}
	if remaining[0] == "visualize" {
		return a.executeVisualize(ctx, remaining[1:])
	}
	if err := options.validate(); err != nil {
		return newUsageError("dexcli", err)
	}
	switch remaining[0] {
	case "health":
		return a.executeHealth(ctx, remaining[1:], options)
	case "flow":
		return newFlowCommand(a.stdin, a.stdout, a.stderr).Execute(ctx, remaining[1:], options)
	case "api":
		return newAPICommand(a.stdin, a.stdout, a.stderr).Execute(ctx, remaining[1:], options)
	case "version":
		return a.executeVersion(ctx, remaining[1:], options)
	case "help", "--help", "-h":
		a.printUsage()
		return nil
	default:
		return newUsageError("dexcli", fmt.Errorf("unknown command %q", remaining[0]))
	}
}

func (a *App) executeHealth(ctx context.Context, args []string, options options) error {
	flags := newFlagSet("dexcli health", a.stderr)
	addCommonFlags(flags, &options)
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprintln(a.stdout, "Usage: dexcli health [global flags]")
			return nil
		}
		return newUsageError("health", err)
	}
	if flags.NArg() != 0 {
		return newUsageError("health", fmt.Errorf("unexpected arguments: %v", flags.Args()))
	}
	if err := options.validate(); err != nil {
		return newUsageError("health", err)
	}
	return withFlowService(ctx, options, func(callCtx context.Context, client *flowService) error {
		response, err := client.service.HealthCheck(callCtx, emptyRequest())
		if err != nil {
			return newOperationError("health", err)
		}
		result := map[string]any{
			"condition": response.GetCondition(),
			"hostname":  response.GetHostname(),
			"duration":  response.GetDuration(),
		}
		return writeOutput(a.stdout, options.output, result)
	})
}

func (a *App) printUsage() {
	fmt.Fprintln(a.stdout, "Usage: dexcli [global flags] <command>")
	fmt.Fprintln(a.stdout)
	fmt.Fprintln(a.stdout, "Commands:")
	fmt.Fprintln(a.stdout, "  health    Check Dex FlowService health")
	fmt.Fprintln(a.stdout, "  visualize Render a static Flow graph from Go or Python source")
	fmt.Fprintln(a.stdout, "  flow      Start, operate, inspect, or watch Flows")
	fmt.Fprintln(a.stdout, "  api       List, describe, or call FlowService RPCs")
	fmt.Fprintln(a.stdout, "  version   Check dexcli and Server protocol compatibility")
	fmt.Fprintln(a.stdout)
	fmt.Fprintln(a.stdout, "Global flags:")
	fmt.Fprintln(a.stdout, "  --server host:port                 Dex FlowService target")
	fmt.Fprintln(a.stdout, "  --credential-file path             bearer token file (overrides DEX_CREDENTIAL_FILE and DEX_CREDENTIAL)")
	fmt.Fprintln(a.stdout, "  DEX_CREDENTIAL_FILE                fallback bearer token file")
	fmt.Fprintln(a.stdout, "  DEX_CREDENTIAL                     fallback bearer token value; no token flag to avoid shell history and process listings")
	fmt.Fprintln(a.stdout, "  --api-key-file path                API key file exchanged for access tokens (overrides DEX_API_KEY)")
	fmt.Fprintln(a.stdout, "  DEX_API_KEY                        fallback API key value; no key flag to avoid shell history and process listings")
	fmt.Fprintln(a.stdout, "  --token-url url                    API key token endpoint (default DEX_TOKEN_URL); https, or http only on loopback")
	fmt.Fprintln(a.stdout, "  --server-id id                     server ID the token endpoint scopes tokens to (default DEX_SERVER_ID)")
	fmt.Fprintln(a.stdout, "                                     API key settings cannot be combined with bearer token settings")
	fmt.Fprintln(a.stdout, "  --output json|table                output format (default json)")
	fmt.Fprintln(a.stdout, "  --timeout duration                 request timeout (default 30s; 0 disables)")
	fmt.Fprintln(a.stdout, "  --no-hydrate                       return blob references without loading values")
}
