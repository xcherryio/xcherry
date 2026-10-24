// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/superdurable/dex/cli/internal/command"
	"github.com/superdurable/dex/cli/internal/dev"
	"github.com/superdurable/dex/cli/internal/update"
	"github.com/superdurable/dex/protos/codec-server"
)

var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer cancel()
	if err := run(ctx, os.Args[1:]); err != nil {
		command.WriteError(os.Stderr, err)
		os.Exit(command.ExitCode(err))
	}
}

func run(ctx context.Context, args []string) error {
	return runWithUpdateNotice(ctx, args, update.PrintNotice)
}

func runWithUpdateNotice(ctx context.Context, args []string, printUpdateNotice func(context.Context, io.Writer, string)) error {
	if len(args) == 0 {
		printUsage(os.Stdout)
		return nil
	}
	switch args[0] {
	case "dev":
		return dev.Execute(ctx, args[1:], os.Stdout, os.Stderr, version)
	case "codec-server":
		return codecserver.Execute(ctx, args[1:], os.Stdout, os.Stderr)
	case "version":
		if len(args) > 1 {
			printUpdateNotice(ctx, os.Stderr, version)
			return command.NewApp(os.Stdin, os.Stdout, os.Stderr, version).Execute(ctx, args)
		}
		fmt.Fprintf(os.Stdout, "dexcli %s (commit %s, built %s)\n", version, commit, date)
		printUpdateNotice(ctx, os.Stderr, version)
		return nil
	case "--version", "-v":
		fmt.Fprintf(os.Stdout, "dexcli %s (commit %s, built %s)\n", version, commit, date)
		printUpdateNotice(ctx, os.Stderr, version)
		return nil
	case "help", "--help", "-h":
		printUsage(os.Stdout)
		return nil
	default:
		return command.NewApp(os.Stdin, os.Stdout, os.Stderr, version).Execute(ctx, args)
	}
}

func printUsage(output io.Writer) {
	fmt.Fprintln(output, "Usage: dexcli <command>")
	fmt.Fprintln(output)
	fmt.Fprintln(output, "Commands:")
	fmt.Fprintln(output, "  dev       Start a local Dex development environment")
	fmt.Fprintln(output, "  codec-server Start a local Temporal protobuf Codec Server")
	fmt.Fprintln(output, "  health    Check Dex FlowService health")
	fmt.Fprintln(output, "  visualize Render a static Flow graph from Go or Python source")
	fmt.Fprintln(output, "  flow      Search, inspect, watch, stop, or reset Flows")
	fmt.Fprintln(output, "  api       List, describe, or call FlowService RPCs")
	fmt.Fprintln(output, "  version   Print version information or check Server compatibility")
	fmt.Fprintln(output)
	fmt.Fprintln(output, "FlowService credentials:")
	fmt.Fprintln(output, "  --credential-file path  token file; overrides DEX_CREDENTIAL_FILE and DEX_CREDENTIAL")
	fmt.Fprintln(output, "  DEX_CREDENTIAL_FILE     fallback token file path")
	fmt.Fprintln(output, "  DEX_CREDENTIAL          fallback token value; no raw-token flag to avoid shell history and process listings")
	fmt.Fprintln(output, "  --api-key-file path     API key file exchanged for access tokens; overrides DEX_API_KEY")
	fmt.Fprintln(output, "  DEX_API_KEY             fallback API key value; no raw-key flag to avoid shell history and process listings")
	fmt.Fprintln(output, "  --token-url url         API key token endpoint (default DEX_TOKEN_URL)")
	fmt.Fprintln(output, "  --server-id id          server ID the token endpoint scopes tokens to (default DEX_SERVER_ID)")
	fmt.Fprintln(output, "  Use either a bearer token or an API key, not both.")
}
