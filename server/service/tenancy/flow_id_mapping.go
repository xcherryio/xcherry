// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package tenancy

import (
	"context"

	"github.com/superdurable/dex/config"
	"strings"

	"github.com/superdurable/dex/gen/dexpb"
	serviceerrors "github.com/superdurable/dex/service/common/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// flowIDFieldNames are the protocol fields that carry a Flow ID, at any depth of a FlowService message.
var flowIDFieldNames = map[protoreflect.Name]bool{
	"flow_id":        true,
	"parent_flow_id": true,
	"sub_flow_id":    true,
}

// FlowIDMapper maps a tenant's Flow IDs to stored IDs in requests and back in responses.
// A response carrying another tenant's Flow ID is NotFound; a search result entry of another tenant is dropped.
type FlowIDMapper struct {
	maxStoredFlowIDBytes int
}

func NewFlowIDMapper(cfg *config.TenancyConfig) *FlowIDMapper {
	if cfg == nil {
		panic("tenancy config must not be nil")
	}
	return &FlowIDMapper{maxStoredFlowIDBytes: cfg.EffectiveMaxStoredFlowIDBytes()}
}

func (m *FlowIDMapper) Unary(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	tenantID := TenantID(ctx)
	if tenantID == "" {
		return handler(ctx, req)
	}
	if request, ok := req.(proto.Message); ok {
		rewriteFlowIDs(request.ProtoReflect(), func(flowID string) (string, bool) {
			return StoredFlowID(tenantID, flowID), true
		})
	}
	if start, ok := req.(*dexpb.StartFlowRequest); ok {
		if err := ValidateStoredFlowIDLength(start.GetFlowId(), m.maxStoredFlowIDBytes); err != nil {
			return nil, serviceerrors.InvalidArgument(
				dexpb.ErrorSubStatus_ERROR_SUB_STATUS_UNCATEGORIZED,
				strings.ReplaceAll(err.Error(), StoredFlowID(tenantID, ""), ""),
			).ToGRPCError()
		}
	}
	resp, err := handler(ctx, req)
	if err != nil {
		return resp, withApplicationFlowIDsInError(tenantID, err)
	}
	if searchResponse, ok := resp.(*dexpb.SearchFlowsResponse); ok {
		searchResponse.FlowRuns = applicationSearchEntries(tenantID, searchResponse.GetFlowRuns())
		return searchResponse, nil
	}
	if response, ok := resp.(proto.Message); ok && !MapResponseFlowIDsOut(tenantID, response) {
		return nil, serviceerrors.NotFound("flow not found").ToGRPCError()
	}
	return resp, nil
}

// MapResponseFlowIDsOut rewrites every stored Flow ID in message to its application form.
// It returns false when any Flow ID belongs to another tenant; message is then partially rewritten and must be dropped.
func MapResponseFlowIDsOut(tenantID string, message proto.Message) bool {
	return rewriteFlowIDs(message.ProtoReflect(), func(flowID string) (string, bool) {
		return ApplicationFlowID(tenantID, flowID)
	})
}

// withApplicationFlowIDsInError removes the caller's own tenant prefix from error text, which names stored Flow IDs.
func withApplicationFlowIDsInError(tenantID string, err error) error {
	original, ok := status.FromError(err)
	if !ok {
		return err
	}
	prefix := StoredFlowID(tenantID, "")
	if !strings.Contains(original.Message(), prefix) {
		return err
	}
	rewritten := status.New(original.Code(), strings.ReplaceAll(original.Message(), prefix, ""))
	for _, detail := range original.Details() {
		errorResponse, isErrorResponse := detail.(*dexpb.ServiceErrorResponse)
		if !isErrorResponse {
			continue
		}
		errorResponse.Detail = strings.ReplaceAll(errorResponse.GetDetail(), prefix, "")
		if withDetails, detailErr := rewritten.WithDetails(errorResponse); detailErr == nil {
			rewritten = withDetails
		}
	}
	return rewritten.Err()
}

func applicationSearchEntries(
	tenantID string,
	entries []*dexpb.SearchFlowsResponseEntry,
) []*dexpb.SearchFlowsResponseEntry {
	kept := entries[:0]
	for _, entry := range entries {
		if MapResponseFlowIDsOut(tenantID, entry) {
			kept = append(kept, entry)
		}
	}
	return kept
}

// rewriteFlowIDs applies rewrite to every non-empty Flow ID field and reports whether every rewrite succeeded.
func rewriteFlowIDs(message protoreflect.Message, rewrite func(string) (string, bool)) bool {
	isRewritten := true
	rewrittenFields := map[protoreflect.FieldDescriptor]string{}
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		switch {
		case field.Kind() == protoreflect.StringKind && field.Cardinality() != protoreflect.Repeated:
			if !flowIDFieldNames[field.Name()] || value.String() == "" {
				return true
			}
			rewritten, ok := rewrite(value.String())
			if !ok {
				isRewritten = false
				return false
			}
			rewrittenFields[field] = rewritten
		case field.Kind() != protoreflect.MessageKind && field.Kind() != protoreflect.GroupKind:
		case field.IsList():
			list := value.List()
			for i := 0; i < list.Len() && isRewritten; i++ {
				isRewritten = rewriteFlowIDs(list.Get(i).Message(), rewrite)
			}
		case field.IsMap():
			if field.MapValue().Kind() != protoreflect.MessageKind {
				return true
			}
			value.Map().Range(func(_ protoreflect.MapKey, entry protoreflect.Value) bool {
				isRewritten = rewriteFlowIDs(entry.Message(), rewrite)
				return isRewritten
			})
		default:
			isRewritten = rewriteFlowIDs(value.Message(), rewrite)
		}
		return isRewritten
	})
	for field, rewritten := range rewrittenFields {
		message.Set(field, protoreflect.ValueOfString(rewritten))
	}
	return isRewritten
}
