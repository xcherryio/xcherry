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
	"errors"
	"fmt"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/types/known/durationpb"
)

func ensureNamespace(ctx context.Context, target, namespace string, retention time.Duration) error {
	namespaceClient, err := client.NewNamespaceClient(client.Options{HostPort: target})
	if err != nil {
		return fmt.Errorf("create Temporal namespace client: %w", err)
	}
	defer namespaceClient.Close()

	request := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		WorkflowExecutionRetentionPeriod: durationpb.New(retention),
	}
	for ctx.Err() == nil {
		err = namespaceClient.Register(ctx, request)
		var exists *serviceerror.NamespaceAlreadyExists
		if err == nil || errors.As(err, &exists) {
			break
		}
		if !retryableTemporalError(err) {
			return fmt.Errorf("register Temporal namespace %q: %w", namespace, err)
		}
		if err := waitRetry(ctx); err != nil {
			return err
		}
	}
	for ctx.Err() == nil {
		_, err = namespaceClient.Describe(ctx, namespace)
		if err == nil {
			return nil
		}
		if !retryableTemporalError(err) {
			var notFound *serviceerror.NamespaceNotFound
			if !errors.As(err, &notFound) {
				return fmt.Errorf("describe Temporal namespace %q: %w", namespace, err)
			}
		}
		if err := waitRetry(ctx); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func retryableTemporalError(err error) bool {
	var unavailable *serviceerror.Unavailable
	var deadline *serviceerror.DeadlineExceeded
	return errors.As(err, &unavailable) || errors.As(err, &deadline)
}

func waitRetry(ctx context.Context) error {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
