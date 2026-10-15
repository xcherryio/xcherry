// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package temporal

import (
	"context"
	"fmt"
	"io"

	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	cloudservice "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	namespacepb "go.temporal.io/cloud-sdk/api/namespace/v1"
	"go.temporal.io/cloud-sdk/cloudclient"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// CloudAttributeIndexClient manages attribute indexes through Temporal Cloud Operations.
type CloudAttributeIndexClient struct {
	namespace string
	service   cloudservice.CloudServiceClient
	closer    io.Closer
}

// NewCloudAttributeIndexClient creates a namespace-scoped Cloud Operations client.
func NewCloudAttributeIndexClient(cfg *config.TemporalConfig) (*CloudAttributeIndexClient, error) {
	return NewCloudAttributeIndexClientWithAPIKeyReader(cfg, nil)
}

// NewCloudAttributeIndexClientWithAPIKeyReader creates a namespace-scoped Cloud Operations client with rotating credentials.
func NewCloudAttributeIndexClientWithAPIKeyReader(cfg *config.TemporalConfig, reader cloudclient.APIKeyReader) (*CloudAttributeIndexClient, error) {
	if cfg == nil || cfg.CloudOps == nil {
		return nil, fmt.Errorf("Temporal Cloud Operations config must not be nil")
	}
	options := cloudclient.Options{
		APIKey:     cfg.CloudAPIKey,
		HostPort:   cfg.CloudOps.HostPort,
		APIVersion: cfg.CloudOps.APIVersion,
		UserAgent:  "dex-server",
	}
	if reader != nil {
		options.APIKeyReader = reader
	}
	client, err := cloudclient.New(options)
	if err != nil {
		return nil, fmt.Errorf("create Temporal Cloud Operations client: %w", err)
	}
	return newCloudAttributeIndexClient(cfg.Namespace, client.CloudService(), client), nil
}

func newCloudAttributeIndexClient(
	namespace string,
	service cloudservice.CloudServiceClient,
	closer io.Closer,
) *CloudAttributeIndexClient {
	return &CloudAttributeIndexClient{namespace: namespace, service: service, closer: closer}
}

func (c *CloudAttributeIndexClient) Close() error {
	if c.closer == nil {
		return nil
	}
	return c.closer.Close()
}

func (c *CloudAttributeIndexClient) ListAttributeIndexes(
	ctx context.Context,
) (map[string]dexpb.IndexType, error) {
	namespace, err := c.getNamespace(ctx)
	if err != nil {
		return nil, err
	}
	return mapCloudSearchAttributes(namespace.GetSpec().GetSearchAttributes()), nil
}

func (c *CloudAttributeIndexClient) AddAttributeIndexes(
	ctx context.Context,
	requested map[string]dexpb.IndexType,
) error {
	before, err := c.getNamespace(ctx)
	if err != nil {
		return err
	}
	updatedSpec, changed, err := mergeCloudSearchAttributes(before.GetSpec(), requested)
	if err != nil || !changed {
		return err
	}
	_, updateErr := c.service.UpdateNamespace(ctx, &cloudservice.UpdateNamespaceRequest{
		Namespace:       c.namespace,
		Spec:            updatedSpec,
		ResourceVersion: before.GetResourceVersion(),
	})
	if updateErr == nil {
		return nil
	}

	after, readErr := c.getNamespace(ctx)
	if readErr != nil {
		return updateErr
	}
	_, stillNeedsUpdate, compareErr := mergeCloudSearchAttributes(after.GetSpec(), requested)
	if compareErr != nil || !stillNeedsUpdate {
		return compareErr
	}
	return updateErr
}

func (c *CloudAttributeIndexClient) NormalizeAttributeIndexType(indexType dexpb.IndexType) dexpb.IndexType {
	return indexType
}

func (c *CloudAttributeIndexClient) getNamespace(ctx context.Context) (*namespacepb.Namespace, error) {
	response, err := c.service.GetNamespace(ctx, &cloudservice.GetNamespaceRequest{Namespace: c.namespace})
	if err != nil {
		return nil, err
	}
	if response.GetNamespace() == nil || response.GetNamespace().GetSpec() == nil {
		return nil, status.Error(codes.Internal, "Temporal Cloud returned an incomplete namespace")
	}
	return response.GetNamespace(), nil
}

func mergeCloudSearchAttributes(
	spec *namespacepb.NamespaceSpec,
	requested map[string]dexpb.IndexType,
) (*namespacepb.NamespaceSpec, bool, error) {
	if spec == nil {
		return nil, false, status.Error(codes.Internal, "Temporal Cloud namespace spec is missing")
	}
	updated := proto.Clone(spec).(*namespacepb.NamespaceSpec)
	if updated.SearchAttributes == nil {
		updated.SearchAttributes = make(map[string]namespacepb.NamespaceSpec_SearchAttributeType)
	}
	changed := false
	for name, requestedType := range requested {
		cloudType := mapToCloudSearchAttributeType(requestedType)
		if cloudType == namespacepb.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_UNSPECIFIED {
			return nil, false, status.Errorf(codes.InvalidArgument, "attribute index %q has invalid type %s", name, requestedType)
		}
		existingType, found := updated.SearchAttributes[name]
		if found && existingType != cloudType {
			return nil, false, status.Errorf(
				codes.FailedPrecondition,
				"attribute index %q has type %s; requested %s",
				name,
				mapCloudSearchAttributeType(existingType),
				requestedType,
			)
		}
		if !found {
			updated.SearchAttributes[name] = cloudType
			changed = true
		}
	}
	return updated, changed, nil
}

func mapCloudSearchAttributes(
	searchAttributes map[string]namespacepb.NamespaceSpec_SearchAttributeType,
) map[string]dexpb.IndexType {
	indexes := make(map[string]dexpb.IndexType, len(searchAttributes))
	for name, indexType := range searchAttributes {
		indexes[name] = mapCloudSearchAttributeType(indexType)
	}
	return indexes
}

func mapCloudSearchAttributeType(indexType namespacepb.NamespaceSpec_SearchAttributeType) dexpb.IndexType {
	switch indexType {
	case namespacepb.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_TEXT:
		return dexpb.IndexType_INDEX_TYPE_TEXT
	case namespacepb.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_KEYWORD:
		return dexpb.IndexType_INDEX_TYPE_KEYWORD
	case namespacepb.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_KEYWORD_LIST:
		return dexpb.IndexType_INDEX_TYPE_KEYWORD_ARRAY
	case namespacepb.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_INT:
		return dexpb.IndexType_INDEX_TYPE_INT
	case namespacepb.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_DOUBLE:
		return dexpb.IndexType_INDEX_TYPE_DOUBLE
	case namespacepb.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_BOOL:
		return dexpb.IndexType_INDEX_TYPE_BOOL
	case namespacepb.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_DATETIME:
		return dexpb.IndexType_INDEX_TYPE_DATETIME
	default:
		return dexpb.IndexType_INDEX_TYPE_UNSPECIFIED
	}
}

func mapToCloudSearchAttributeType(indexType dexpb.IndexType) namespacepb.NamespaceSpec_SearchAttributeType {
	switch indexType {
	case dexpb.IndexType_INDEX_TYPE_TEXT:
		return namespacepb.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_TEXT
	case dexpb.IndexType_INDEX_TYPE_KEYWORD:
		return namespacepb.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_KEYWORD
	case dexpb.IndexType_INDEX_TYPE_KEYWORD_ARRAY:
		return namespacepb.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_KEYWORD_LIST
	case dexpb.IndexType_INDEX_TYPE_INT:
		return namespacepb.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_INT
	case dexpb.IndexType_INDEX_TYPE_DOUBLE:
		return namespacepb.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_DOUBLE
	case dexpb.IndexType_INDEX_TYPE_BOOL:
		return namespacepb.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_BOOL
	case dexpb.IndexType_INDEX_TYPE_DATETIME:
		return namespacepb.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_DATETIME
	default:
		return namespacepb.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_UNSPECIFIED
	}
}
