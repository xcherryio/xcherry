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
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/superdurable/dex/gen/dexpb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

type apiCommand struct {
	stdin   io.Reader
	stdout  io.Writer
	stderr  io.Writer
	service protoreflect.ServiceDescriptor
}

func newAPICommand(stdin io.Reader, stdout io.Writer, stderr io.Writer) *apiCommand {
	service := dexpb.File_dex_proto.Services().ByName("FlowService")
	if service == nil {
		panic("FlowService descriptor is unavailable")
	}
	return &apiCommand{stdin: stdin, stdout: stdout, stderr: stderr, service: service}
}

func (c *apiCommand) Execute(ctx context.Context, args []string, options options) error {
	if len(args) == 0 {
		c.printUsage()
		return nil
	}
	switch args[0] {
	case "list":
		return c.list(args[1:], options)
	case "describe":
		return c.describe(args[1:], options)
	case "call":
		return c.call(ctx, args[1:], options)
	case "help", "--help", "-h":
		c.printUsage()
		return nil
	default:
		return newUsageError("api", fmt.Errorf("unknown command %q", args[0]))
	}
}

func (c *apiCommand) list(args []string, options options) error {
	flags := newFlagSet("dexcli api list", c.stderr)
	addCommonFlags(flags, &options)
	if done, err := parseAPIFlags(flags, args, c.stdout, "dexcli api list [flags]"); done || err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return newUsageError("api list", fmt.Errorf("unexpected arguments: %v", flags.Args()))
	}
	methods := make([]any, 0, c.service.Methods().Len())
	for index := 0; index < c.service.Methods().Len(); index++ {
		method := c.service.Methods().Get(index)
		methods = append(methods, map[string]any{
			"name": string(method.Name()), "fullMethod": fullMethodName(c.service, method),
			"requestType":  string(method.Input().FullName()),
			"responseType": string(method.Output().FullName()),
			"mutating":     isMutatingMethod(method.Name()),
		})
	}
	return writeOutput(c.stdout, options.output, map[string]any{
		"service": string(c.service.FullName()), "methods": methods,
	})
}

func (c *apiCommand) describe(args []string, options options) error {
	flags := newFlagSet("dexcli api describe", c.stderr)
	addCommonFlags(flags, &options)
	if done, err := parseAPIFlags(flags, args, c.stdout, "dexcli api describe METHOD [flags]"); done || err != nil {
		return err
	}
	method, err := c.oneMethod(flags, "api describe")
	if err != nil {
		return err
	}
	messages := make(map[protoreflect.FullName]map[string]any)
	describeMessage(method.Input(), messages)
	describeMessage(method.Output(), messages)
	names := make([]string, 0, len(messages))
	for name := range messages {
		names = append(names, string(name))
	}
	sort.Strings(names)
	described := make([]any, 0, len(names))
	for _, name := range names {
		described = append(described, messages[protoreflect.FullName(name)])
	}
	return writeOutput(c.stdout, options.output, map[string]any{
		"name": string(method.Name()), "fullMethod": fullMethodName(c.service, method),
		"requestType":  string(method.Input().FullName()),
		"responseType": string(method.Output().FullName()),
		"mutating":     isMutatingMethod(method.Name()), "messages": described,
	})
}

func (c *apiCommand) call(ctx context.Context, args []string, options options) error {
	flags := newFlagSet("dexcli api call", c.stderr)
	dataSource := flags.String("data", "{}", "protobuf JSON, @file, or - for stdin")
	yes := flags.Bool("yes", false, "confirm a mutating RPC")
	addCommonFlags(flags, &options)
	if done, err := parseAPIFlags(flags, args, c.stdout, "dexcli api call METHOD [--data JSON|@FILE|-] [--yes]"); done || err != nil {
		return err
	}
	method, err := c.oneMethod(flags, "api call")
	if err != nil {
		return err
	}
	if isMutatingMethod(method.Name()) && !*yes {
		return newConfirmationError("api call " + string(method.Name()))
	}
	data, err := c.readData(*dataSource)
	if err != nil {
		return newUsageError("api call "+string(method.Name()), err)
	}
	request := dynamicpb.NewMessage(method.Input())
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(data, request); err != nil {
		return newUsageError("api call "+string(method.Name()), fmt.Errorf("invalid protobuf JSON: %w", err))
	}
	return withFlowService(ctx, options, func(callCtx context.Context, client *flowService) error {
		response := dynamicpb.NewMessage(method.Output())
		if invokeErr := client.connection.Invoke(callCtx, fullMethodName(c.service, method), request, response); invokeErr != nil {
			return newOperationError("api call "+string(method.Name()), invokeErr)
		}
		mapped, mapErr := strictMessageMap(response)
		if mapErr != nil {
			return newOperationError("api call "+string(method.Name()), mapErr)
		}
		return writeOutput(c.stdout, options.output, mapped)
	})
}

func parseAPIFlags(flags *flag.FlagSet, args []string, output io.Writer, usage string) (bool, error) {
	if err := flags.Parse(interspersedArgs(flags, args)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprintln(output, "Usage:", usage)
			return true, nil
		}
		return false, newUsageError(strings.TrimPrefix(flags.Name(), "dexcli "), err)
	}
	parsedOptions, err := optionsFromFlags(flags)
	if err != nil {
		return false, newUsageError(strings.TrimPrefix(flags.Name(), "dexcli "), err)
	}
	if err := parsedOptions.validate(); err != nil {
		return false, newUsageError(strings.TrimPrefix(flags.Name(), "dexcli "), err)
	}
	return false, nil
}

func optionsFromFlags(flags *flag.FlagSet) (options, error) {
	server := flags.Lookup("server").Value.String()
	credentialFile := flags.Lookup("credential-file").Value.String()
	output := flags.Lookup("output").Value.String()
	timeout, err := time.ParseDuration(flags.Lookup("timeout").Value.String())
	if err != nil {
		return options{}, fmt.Errorf("parse timeout: %w", err)
	}
	getter, ok := flags.Lookup("no-hydrate").Value.(flag.Getter)
	if !ok {
		return options{}, fmt.Errorf("read no-hydrate flag")
	}
	noHydrate, ok := getter.Get().(bool)
	if !ok {
		return options{}, fmt.Errorf("no-hydrate flag is not boolean")
	}
	return options{server: server, credentialFile: credentialFile, output: output, timeout: timeout, noHydrate: noHydrate}, nil
}

func (c *apiCommand) oneMethod(flags *flag.FlagSet, operation string) (protoreflect.MethodDescriptor, error) {
	if flags.NArg() != 1 {
		return nil, newUsageError(operation, fmt.Errorf("exactly one METHOD is required"))
	}
	method := c.findMethod(flags.Arg(0))
	if method == nil {
		return nil, newUsageError(operation, fmt.Errorf("unknown FlowService method %q", flags.Arg(0)))
	}
	return method, nil
}

func (c *apiCommand) findMethod(name string) protoreflect.MethodDescriptor {
	trimmed := strings.TrimPrefix(name, "/dex.FlowService/")
	for index := 0; index < c.service.Methods().Len(); index++ {
		method := c.service.Methods().Get(index)
		if strings.EqualFold(trimmed, string(method.Name())) {
			return method
		}
	}
	return nil
}

func (c *apiCommand) readData(source string) ([]byte, error) {
	switch {
	case source == "-":
		data, err := io.ReadAll(c.stdin)
		if err != nil {
			return nil, fmt.Errorf("read stdin: %w", err)
		}
		return data, nil
	case strings.HasPrefix(source, "@"):
		path := strings.TrimPrefix(source, "@")
		if path == "" {
			return nil, fmt.Errorf("data file path must not be empty")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read data file: %w", err)
		}
		return data, nil
	default:
		return []byte(source), nil
	}
}

func fullMethodName(service protoreflect.ServiceDescriptor, method protoreflect.MethodDescriptor) string {
	return "/" + string(service.FullName()) + "/" + string(method.Name())
}

func isMutatingMethod(name protoreflect.Name) bool {
	switch name {
	case "StartFlow", "PublishToChannel", "WriteStream", "StopFlow", "SetAttributes", "SyncAttributeIndexes",
		"ResetFlow", "InvokeRPC", "SkipTimer", "UpdateFlowConfig", "TriggerContinueAsNew", "DeleteChannelMessage":
		return true
	default:
		return false
	}
}

func strictMessageMap(message *dynamicpb.Message) (map[string]any, error) {
	data, err := (protojson.MarshalOptions{UseProtoNames: false}).Marshal(message)
	if err != nil {
		return nil, err
	}
	result := make(map[string]any)
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func describeMessage(descriptor protoreflect.MessageDescriptor, messages map[protoreflect.FullName]map[string]any) {
	if descriptor == nil {
		return
	}
	if _, found := messages[descriptor.FullName()]; found {
		return
	}
	result := map[string]any{"name": string(descriptor.FullName())}
	messages[descriptor.FullName()] = result
	fields := make([]any, 0, descriptor.Fields().Len())
	for index := 0; index < descriptor.Fields().Len(); index++ {
		field := descriptor.Fields().Get(index)
		item := map[string]any{
			"name": field.JSONName(), "protoName": string(field.Name()),
			"number": field.Number(), "kind": field.Kind().String(),
			"repeated": field.Cardinality() == protoreflect.Repeated,
			"required": field.Cardinality() == protoreflect.Required,
		}
		if field.ContainingOneof() != nil {
			item["oneof"] = string(field.ContainingOneof().Name())
		}
		if field.IsMap() {
			item["map"] = true
		}
		if field.Message() != nil {
			item["messageType"] = string(field.Message().FullName())
			describeMessage(field.Message(), messages)
		}
		if field.Enum() != nil {
			item["enumType"] = string(field.Enum().FullName())
			item["enumValues"] = enumValues(field.Enum())
		}
		fields = append(fields, item)
	}
	result["fields"] = fields
}

func enumValues(descriptor protoreflect.EnumDescriptor) []any {
	values := make([]any, 0, descriptor.Values().Len())
	for index := 0; index < descriptor.Values().Len(); index++ {
		value := descriptor.Values().Get(index)
		values = append(values, map[string]any{"name": string(value.Name()), "number": value.Number()})
	}
	return values
}

func (c *apiCommand) printUsage() {
	fmt.Fprintln(c.stdout, "Usage: dexcli api <command>")
	fmt.Fprintln(c.stdout)
	fmt.Fprintln(c.stdout, "Commands: list, describe, call")
}
