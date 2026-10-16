// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package workerclient

import (
	"fmt"
	"math"
	"regexp"

	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service"
)

var actionPermissionPattern = regexp.MustCompile(
	`^[a-z][a-z0-9]*(?:[.-][a-z0-9]+)*$`,
)

// ValidateAttributeWrites validates untrusted start Attributes and rejects server-owned blob IDs.
func ValidateAttributeWrites(attributes []*dexpb.AttributeWrite) error {
	seenKeys := make(map[string]bool, len(attributes))
	for index, attribute := range attributes {
		if attribute == nil || attribute.GetKey() == "" || attribute.GetValue() == nil ||
			attribute.GetValue().GetKind() == nil {
			return fmt.Errorf("attribute %d is invalid", index)
		}
		if seenKeys[attribute.GetKey()] {
			return fmt.Errorf("attribute keys must be unique")
		}
		seenKeys[attribute.GetKey()] = true
		if err := RejectWorkerBlobIDs(attribute.GetValue()); err != nil {
			return err
		}
	}
	return ValidateHardDeleteAttributeWrites(attributes)
}

// ValidateHardDeleteAttributeWrites validates untrusted hard-delete writes before they reach blob storage.
func ValidateHardDeleteAttributeWrites(writes []*dexpb.AttributeWrite) error {
	for _, write := range writes {
		if !write.GetIsHardDeleteOnNull() {
			continue
		}
		switch write.GetValue().GetKind().(type) {
		case nil, *dexpb.Value_NullValue, *dexpb.Value_StringValue, *dexpb.Value_ObjValue:
		default:
			return fmt.Errorf("hard-delete attribute %q must be a string, an object, or null", write.GetKey())
		}
		if write.GetIndexConfig().GetEnable() {
			return fmt.Errorf("hard-delete attribute %q must not be indexed", write.GetKey())
		}
		if write.GetSyncConfig().GetEnabled() {
			return fmt.Errorf("hard-delete attribute %q must not sync to an Attribute Store", write.GetKey())
		}
	}
	return nil
}

// RejectWorkerBlobIDs rejects server-minted blob-id arms on worker responses (untrusted).
func RejectWorkerBlobIDs(values ...*dexpb.Value) error {
	for _, value := range values {
		if value == nil {
			continue
		}
		switch value.GetKind().(type) {
		case *dexpb.Value_InternalBlobIdForStringValue, *dexpb.Value_InternalBlobIdForObjValue:
			return fmt.Errorf("worker response must not contain internal_blob_id arms")
		}
	}
	return nil
}

// RejectWorkerAttributeWriteBlobIDs rejects blob-id arms on AttributeWrite values.
func RejectWorkerAttributeWriteBlobIDs(writes []*dexpb.AttributeWrite) error {
	for _, write := range writes {
		if write == nil {
			continue
		}
		if err := RejectWorkerBlobIDs(write.GetValue()); err != nil {
			return err
		}
	}
	return nil
}

// ValidateRuntimeAttributeWrites rejects direct writes to the server-managed permission projection.
func ValidateRuntimeAttributeWrites(writes []*dexpb.AttributeWrite) error {
	for _, write := range writes {
		if write == nil {
			continue
		}
		if write.GetKey() == service.SearchAttributeDexWorkQueuePermissions ||
			write.GetIndexConfig().GetIndexKey() == service.SearchAttributeDexWorkQueuePermissions {
			return fmt.Errorf(
				"%s is managed by the Server",
				service.SearchAttributeDexWorkQueuePermissions,
			)
		}
	}
	return nil
}

// ValidateActionPermissionMappings validates untrusted Action projection inputs.
func ValidateActionPermissionMappings(
	mappings *dexpb.ActionPermissionMappings,
	backendType service.BackendType,
) error {
	if mappings == nil {
		return nil
	}
	if backendType == service.BackendTypeCadence && len(mappings.GetMappings()) > 0 {
		return fmt.Errorf("Action permission mappings require the Temporal backend")
	}
	for mappingIndex, mapping := range mappings.GetMappings() {
		if mapping == nil || mapping.GetAttributeKey() == "" {
			return fmt.Errorf("Action permission mapping %d has an empty Attribute key", mappingIndex)
		}
		if mapping.GetAttributeKey() == service.SearchAttributeDexWorkQueuePermissions {
			return fmt.Errorf(
				"Action permission mapping %d uses server-managed Attribute %s",
				mappingIndex,
				service.SearchAttributeDexWorkQueuePermissions,
			)
		}
		if !actionPermissionPattern.MatchString(mapping.GetRequiredPermission()) {
			return fmt.Errorf(
				"Action permission mapping %d has invalid permission %q",
				mappingIndex,
				mapping.GetRequiredPermission(),
			)
		}
		if len(mapping.GetEqualValues()) == 0 {
			return fmt.Errorf("Action permission mapping %d has no equal values", mappingIndex)
		}
		for valueIndex, value := range mapping.GetEqualValues() {
			if !isValidActionPermissionEqualValue(value) {
				return fmt.Errorf(
					"Action permission mapping %d equal value %d is not a finite scalar",
					mappingIndex,
					valueIndex,
				)
			}
		}
	}
	return nil
}

func isValidActionPermissionEqualValue(value *dexpb.Value) bool {
	if value == nil {
		return false
	}
	switch typedValue := value.GetKind().(type) {
	case *dexpb.Value_StringValue, *dexpb.Value_IntValue, *dexpb.Value_BoolValue:
		return true
	case *dexpb.Value_DoubleValue:
		return !math.IsNaN(typedValue.DoubleValue) && !math.IsInf(typedValue.DoubleValue, 0)
	default:
		return false
	}
}

// RejectWorkerKVBlobIDs rejects blob-id arms on KV values.
func RejectWorkerKVBlobIDs(kvs []*dexpb.KV) error {
	for _, kv := range kvs {
		if kv == nil {
			continue
		}
		if err := RejectWorkerBlobIDs(kv.GetValue()); err != nil {
			return err
		}
	}
	return nil
}
