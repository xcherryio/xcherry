// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package blobstore

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service/common/utils"
	"google.golang.org/protobuf/proto"
)

// ErrHardDeleteRequiresBlobStore reports a hard-delete Attribute write while the blob store is disabled.
var ErrHardDeleteRequiresBlobStore = errors.New("hard-delete Attributes require an enabled blob store")

// OffloadLargeAttributeWrites replaces oversized string/object Value arms with server-minted blob ids.
// Hard-delete writes always replace their value with a fixed blob id, and their null writes delete it.
func OffloadLargeAttributeWrites(
	ctx context.Context,
	writes []*dexpb.AttributeWrite,
	flowId string,
	invocationId string,
	threshold int,
	blobStore BlobStore,
	enabled bool,
) error {
	for _, write := range writes {
		if write == nil {
			continue
		}
		if write.GetIsHardDeleteOnNull() {
			if !enabled || blobStore == nil {
				return ErrHardDeleteRequiresBlobStore
			}
			if err := offloadHardDeleteAttributeWrite(ctx, write, flowId, blobStore); err != nil {
				return err
			}
			continue
		}
		if !enabled || threshold <= 0 || write.GetValue() == nil {
			continue
		}
		if err := offloadValue(ctx, write.Value, flowId, invocationId, threshold, blobStore); err != nil {
			return err
		}
	}
	return nil
}

func offloadHardDeleteAttributeWrite(
	ctx context.Context,
	write *dexpb.AttributeWrite,
	flowID string,
	blobStore BlobStore,
) error {
	switch kind := write.GetValue().GetKind().(type) {
	case nil, *dexpb.Value_NullValue:
		return blobStore.DeleteHardDeleteObject(ctx, flowID, write.GetKey())
	case *dexpb.Value_StringValue:
		storeID, locator, err := blobStore.WriteHardDeleteObject(
			ctx, flowID, write.GetKey(), []byte(kind.StringValue),
		)
		if err != nil {
			return err
		}
		write.Value.Kind = &dexpb.Value_InternalBlobIdForStringValue{
			InternalBlobIdForStringValue: formatBlobID(storeID, locator),
		}
		return nil
	case *dexpb.Value_ObjValue:
		encodedObject, err := proto.MarshalOptions{Deterministic: true}.Marshal(kind.ObjValue)
		if err != nil {
			return fmt.Errorf("marshal EncodedObject: %w", err)
		}
		storeID, locator, err := blobStore.WriteHardDeleteObject(ctx, flowID, write.GetKey(), encodedObject)
		if err != nil {
			return err
		}
		write.Value.Kind = &dexpb.Value_InternalBlobIdForObjValue{
			InternalBlobIdForObjValue: formatBlobID(storeID, locator),
		}
		return nil
	default:
		return fmt.Errorf("hard-delete Attribute %q must be a string, an object, or null", write.GetKey())
	}
}

// OffloadLargeKVs replaces oversized KV values with server-minted blob ids.
func OffloadLargeKVs(
	ctx context.Context,
	values []*dexpb.KV,
	flowId string,
	invocationId string,
	threshold int,
	blobStore BlobStore,
	enabled bool,
) error {
	if !enabled || threshold <= 0 {
		return nil
	}
	for _, value := range values {
		if value == nil || value.GetValue() == nil {
			continue
		}
		if err := offloadValue(ctx, value.Value, flowId, invocationId, threshold, blobStore); err != nil {
			return err
		}
	}
	return nil
}

// OffloadLargeChannelMessages replaces oversized channel values with blob ids.
func OffloadLargeChannelMessages(
	ctx context.Context,
	messages []*dexpb.ChannelMessage,
	flowId string,
	invocationId string,
	threshold int,
	blobStore BlobStore,
	enabled bool,
) error {
	if !enabled || threshold <= 0 {
		return nil
	}
	for _, message := range messages {
		if message == nil || message.GetValue() == nil {
			continue
		}
		if err := offloadValue(ctx, message.Value, flowId, invocationId, threshold, blobStore); err != nil {
			return err
		}
	}
	return nil
}

// OffloadLargeValue offloads a single Value when over threshold.
func OffloadLargeValue(
	ctx context.Context,
	value *dexpb.Value,
	flowId string,
	invocationId string,
	threshold int,
	blobStore BlobStore,
	enabled bool,
) error {
	if !enabled || threshold <= 0 || value == nil {
		return nil
	}
	return offloadValue(ctx, value, flowId, invocationId, threshold, blobStore)
}

func offloadValue(
	ctx context.Context,
	value *dexpb.Value,
	flowId string,
	invocationId string,
	threshold int,
	blobStore BlobStore,
) error {
	switch kind := value.GetKind().(type) {
	case *dexpb.Value_StringValue:
		if len(kind.StringValue) <= threshold {
			return nil
		}
		storeId, path, err := blobStore.WriteObject(ctx, flowId, invocationId, []byte(kind.StringValue))
		if err != nil {
			return err
		}
		blobId := formatBlobID(storeId, path)
		value.Kind = &dexpb.Value_InternalBlobIdForStringValue{InternalBlobIdForStringValue: blobId}
		return nil
	case *dexpb.Value_ObjValue:
		if kind.ObjValue == nil || len(kind.ObjValue.GetPayload()) <= threshold {
			return nil
		}
		encodedObject, err := proto.MarshalOptions{Deterministic: true}.Marshal(kind.ObjValue)
		if err != nil {
			return fmt.Errorf("marshal EncodedObject: %w", err)
		}
		storeId, path, err := blobStore.WriteObject(ctx, flowId, invocationId, encodedObject)
		if err != nil {
			return err
		}
		blobId := formatBlobID(storeId, path)
		value.Kind = &dexpb.Value_InternalBlobIdForObjValue{InternalBlobIdForObjValue: blobId}
		return nil
	default:
		return nil
	}
}

// HydrateFlowValues replaces Blob references with values owned by flowID.
func HydrateFlowValues(ctx context.Context, flowID string, values []*dexpb.Value, blobStore BlobStore) error {
	for _, value := range values {
		if err := HydrateValue(ctx, flowID, value, blobStore); err != nil {
			return err
		}
	}
	return nil
}

// HydrateAttributeWrites hydrates Value arms on AttributeWrites / KVs.
func HydrateAttributeWrites(
	ctx context.Context,
	flowID string,
	writes []*dexpb.AttributeWrite,
	blobStore BlobStore,
) error {
	for _, write := range writes {
		if write == nil {
			continue
		}
		if err := HydrateValue(ctx, flowID, write.GetValue(), blobStore); err != nil {
			return err
		}
	}
	return nil
}

// HydrateKVs hydrates Value arms on KV pairs.
// It skips hard-delete Attributes, which only HydrateHardDeleteKVs hydrates into new KVs.
func HydrateKVs(ctx context.Context, flowID string, kvs []*dexpb.KV, blobStore BlobStore) error {
	for _, kv := range kvs {
		if kv == nil || isHardDeleteKV(kv) {
			continue
		}
		if err := HydrateValue(ctx, flowID, kv.GetValue(), blobStore); err != nil {
			return err
		}
	}
	return nil
}

// HydrateKVsCopy returns KVs with blob-backed Values hydrated without changing kvs.
func HydrateKVsCopy(ctx context.Context, flowID string, kvs []*dexpb.KV, blobStore BlobStore) ([]*dexpb.KV, error) {
	var hydratedKVs []*dexpb.KV
	for index, kv := range kvs {
		if kv == nil || isHardDeleteKV(kv) || !hasBlobValue(kv.GetValue()) {
			continue
		}
		if hydratedKVs == nil {
			hydratedKVs = append([]*dexpb.KV(nil), kvs...)
		}
		value, err := HydrateValueCopy(ctx, flowID, kv.GetValue(), blobStore)
		if err != nil {
			return nil, err
		}
		hydratedKV := utils.ShallowCopyMessage(kv)
		hydratedKV.Value = value
		hydratedKVs[index] = hydratedKV
	}
	if hydratedKVs == nil {
		return kvs, nil
	}
	return hydratedKVs, nil
}

// HydrateHardDeleteKVs returns kvs with every hard-delete Attribute replaced by a new hydrated KV.
// It never mutates kvs or their Values, so callers can keep the input free of payloads.
// A deleted hard-delete object omits its Attribute. Other KVs keep their original pointers.
// It returns kvs itself when no KV references a hard-delete object.
func HydrateHardDeleteKVs(
	ctx context.Context,
	flowID string,
	kvs []*dexpb.KV,
	blobStore BlobStore,
) ([]*dexpb.KV, error) {
	if !HasHardDeleteKV(kvs) {
		return kvs, nil
	}
	if blobStore == nil {
		return nil, ErrHardDeleteRequiresBlobStore
	}
	hydrated := make([]*dexpb.KV, 0, len(kvs))
	for _, kv := range kvs {
		if !isHardDeleteKV(kv) {
			hydrated = append(hydrated, kv)
			continue
		}
		value := &dexpb.Value{Kind: kv.GetValue().GetKind()}
		if err := HydrateValue(ctx, flowID, value, blobStore); err != nil {
			if IsObjectNotFound(err) {
				continue
			}
			return nil, err
		}
		hydrated = append(hydrated, &dexpb.KV{Key: kv.GetKey(), Value: value})
	}
	return hydrated, nil
}

// HasHardDeleteKV reports whether any KV references a hard-delete Attribute object.
func HasHardDeleteKV(kvs []*dexpb.KV) bool {
	return slices.ContainsFunc(kvs, isHardDeleteKV)
}

func isHardDeleteKV(kv *dexpb.KV) bool {
	return IsHardDeleteValue(kv.GetValue())
}

// IsHardDeleteValue reports whether value references a hard-delete Attribute object.
func IsHardDeleteValue(value *dexpb.Value) bool {
	blobID, isBlobReference, err := blobIDFromValue(value)
	return err == nil && isBlobReference && IsHardDeleteBlobID(blobID)
}

// HydrateChannelValues hydrates every pending Channel message Value.
func HydrateChannelValues(
	ctx context.Context,
	flowID string,
	channels map[string]*dexpb.ChannelValues,
	blobStore BlobStore,
) error {
	for _, channel := range channels {
		if channel == nil {
			continue
		}
		for _, message := range channel.GetMessages() {
			if message == nil {
				continue
			}
			if err := HydrateValue(ctx, flowID, message.GetValue(), blobStore); err != nil {
				return err
			}
		}
	}
	return nil
}

// HydrateChannelValuesCopy returns Channel Values with blob-backed messages hydrated without changing channels.
func HydrateChannelValuesCopy(
	ctx context.Context,
	flowID string,
	channels map[string]*dexpb.ChannelValues,
	blobStore BlobStore,
) (map[string]*dexpb.ChannelValues, error) {
	var hydratedChannels map[string]*dexpb.ChannelValues
	for channelName, channel := range channels {
		if channel == nil {
			continue
		}
		var hydratedMessages []*dexpb.ChannelMessage
		for index, message := range channel.GetMessages() {
			if message == nil || !hasBlobValue(message.GetValue()) {
				continue
			}
			if hydratedMessages == nil {
				hydratedMessages = append([]*dexpb.ChannelMessage(nil), channel.GetMessages()...)
			}
			value, err := HydrateValueCopy(ctx, flowID, message.GetValue(), blobStore)
			if err != nil {
				return nil, err
			}
			hydratedMessage := utils.ShallowCopyMessage(message)
			hydratedMessage.Value = value
			hydratedMessages[index] = hydratedMessage
		}
		if hydratedMessages == nil {
			continue
		}
		if hydratedChannels == nil {
			hydratedChannels = make(map[string]*dexpb.ChannelValues, len(channels))
			for name, values := range channels {
				hydratedChannels[name] = values
			}
		}
		hydratedChannel := utils.ShallowCopyMessage(channel)
		hydratedChannel.Messages = hydratedMessages
		hydratedChannels[channelName] = hydratedChannel
	}
	if hydratedChannels == nil {
		return channels, nil
	}
	return hydratedChannels, nil
}

func HydrateConditionResults(
	ctx context.Context,
	flowID string,
	results *dexpb.ConditionResults,
	blobStore BlobStore,
) error {
	for _, result := range results.GetChannelResults() {
		if err := HydrateFlowValues(ctx, flowID, result.GetValues(), blobStore); err != nil {
			return err
		}
	}
	return nil
}

// HydrateConditionResultsCopy returns Condition results with blob-backed Values hydrated without changing results.
func HydrateConditionResultsCopy(
	ctx context.Context,
	flowID string,
	results *dexpb.ConditionResults,
	blobStore BlobStore,
) (*dexpb.ConditionResults, error) {
	if results == nil {
		return nil, nil
	}
	var hydratedResults []*dexpb.ChannelResult
	for index, result := range results.GetChannelResults() {
		if result == nil {
			continue
		}
		var hydratedValues []*dexpb.Value
		for valueIndex, value := range result.GetValues() {
			if !hasBlobValue(value) {
				continue
			}
			if hydratedValues == nil {
				hydratedValues = append([]*dexpb.Value(nil), result.GetValues()...)
			}
			hydratedValue, err := HydrateValueCopy(ctx, flowID, value, blobStore)
			if err != nil {
				return nil, err
			}
			hydratedValues[valueIndex] = hydratedValue
		}
		if hydratedValues == nil {
			continue
		}
		if hydratedResults == nil {
			hydratedResults = append([]*dexpb.ChannelResult(nil), results.GetChannelResults()...)
		}
		hydratedResult := utils.ShallowCopyMessage(result)
		hydratedResult.Values = hydratedValues
		hydratedResults[index] = hydratedResult
	}
	if hydratedResults == nil {
		return results, nil
	}
	hydratedConditionResults := utils.ShallowCopyMessage(results)
	hydratedConditionResults.ChannelResults = hydratedResults
	return hydratedConditionResults, nil
}

// HydrateValue hydrates a Flow-owned Value in place.
func HydrateValue(ctx context.Context, flowID string, value *dexpb.Value, blobStore BlobStore) error {
	if value == nil {
		return nil
	}
	switch kind := value.GetKind().(type) {
	case *dexpb.Value_InternalBlobIdForStringValue:
		storeId, path, err := parseBlobID(kind.InternalBlobIdForStringValue)
		if err != nil {
			return err
		}
		data, err := blobStore.ReadObject(ctx, storeId, flowID, path)
		if err != nil {
			return err
		}
		value.Kind = &dexpb.Value_StringValue{StringValue: string(data)}
		return nil
	case *dexpb.Value_InternalBlobIdForObjValue:
		storeId, path, err := parseBlobID(kind.InternalBlobIdForObjValue)
		if err != nil {
			return err
		}
		data, err := blobStore.ReadObject(ctx, storeId, flowID, path)
		if err != nil {
			return err
		}
		encodedObject := &dexpb.EncodedObject{}
		if err := proto.Unmarshal(data, encodedObject); err != nil {
			return fmt.Errorf("unmarshal EncodedObject: %w", err)
		}
		value.Kind = &dexpb.Value_ObjValue{ObjValue: encodedObject}
		return nil
	default:
		return nil
	}
}

// HydrateValueCopy returns a hydrated Value without changing value.
func HydrateValueCopy(ctx context.Context, flowID string, value *dexpb.Value, blobStore BlobStore) (*dexpb.Value, error) {
	if value == nil || !hasBlobValue(value) {
		return value, nil
	}
	hydrated := utils.ShallowCopyMessage(value)
	if err := HydrateValue(ctx, flowID, hydrated, blobStore); err != nil {
		return nil, err
	}
	return hydrated, nil
}

func hasBlobValue(value *dexpb.Value) bool {
	switch value.GetKind().(type) {
	case *dexpb.Value_InternalBlobIdForStringValue, *dexpb.Value_InternalBlobIdForObjValue:
		return true
	default:
		return false
	}
}

// TransferValueBlobOwnership ensures value is inline or owned by destinationFlowID.
func TransferValueBlobOwnership(
	ctx context.Context,
	value *dexpb.Value,
	sourceFlowID string,
	destinationFlowID string,
	invocationID string,
	threshold int,
	blobStore BlobStore,
	enabled bool,
) error {
	if value == nil {
		return nil
	}
	if destinationFlowID == "" {
		return fmt.Errorf("destination Flow ID is required to transfer Blob ownership")
	}
	_, isBlobReference, err := blobIDFromValue(value)
	if err != nil {
		return err
	}
	if isBlobReference {
		if sourceFlowID == "" {
			return fmt.Errorf("source Flow ID is required to transfer Blob ownership")
		}
		if sourceFlowID == destinationFlowID {
			return nil
		}
		if err := HydrateValue(ctx, sourceFlowID, value, blobStore); err != nil {
			return err
		}
	}
	return OffloadLargeValue(ctx, value, destinationFlowID, invocationID, threshold, blobStore, enabled)
}

// TransferFlowResultBlobOwnership transfers every completed Step output in result.
func TransferFlowResultBlobOwnership(
	ctx context.Context,
	result *dexpb.FlowResult,
	sourceFlowID string,
	destinationFlowID string,
	invocationID string,
	threshold int,
	blobStore BlobStore,
	enabled bool,
) error {
	if result == nil {
		return nil
	}
	for _, completion := range result.GetResults() {
		if completion == nil {
			continue
		}
		if err := TransferValueBlobOwnership(
			ctx,
			completion.GetCompletedStepOutput(),
			sourceFlowID,
			destinationFlowID,
			invocationID,
			threshold,
			blobStore,
			enabled,
		); err != nil {
			return err
		}
	}
	return nil
}

func blobIDFromValue(value *dexpb.Value) (string, bool, error) {
	switch kind := value.GetKind().(type) {
	case *dexpb.Value_InternalBlobIdForStringValue:
		if kind.InternalBlobIdForStringValue == "" {
			return "", false, fmt.Errorf("Blob ID is required")
		}
		return kind.InternalBlobIdForStringValue, true, nil
	case *dexpb.Value_InternalBlobIdForObjValue:
		if kind.InternalBlobIdForObjValue == "" {
			return "", false, fmt.Errorf("Blob ID is required")
		}
		return kind.InternalBlobIdForObjValue, true, nil
	default:
		return "", false, nil
	}
}

func formatBlobID(storeId, path string) string {
	return storeId + "|" + path
}

func parseBlobID(blobID string) (storeID, path string, err error) {
	storeID, path, found := strings.Cut(blobID, "|")
	if !found || storeID == "" || path == "" || strings.Contains(path, "|") {
		return "", "", fmt.Errorf("invalid Blob ID %q", blobID)
	}
	return storeID, path, nil
}
