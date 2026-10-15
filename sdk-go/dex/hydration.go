// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package dex

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"unicode/utf8"

	"github.com/superdurable/dex/blob-cache-go/blobcache"
	"github.com/superdurable/dex/sdk-go/gen/dexpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type valueHydrator interface {
	HydrateValuesInPlace(context.Context, []flowValuePointer) error
}

type flowValuePointer struct {
	flowID           string
	valueDescription string
	valuePointer     **dexpb.Value
}

type valueHydratorImpl struct {
	client             dexpb.FlowServiceClient
	cache              *blobcache.Cache
	logger             Logger
	flowServiceAddress string
}

type blobIDDef struct {
	value            string
	isObjectOrString bool
}

type pendingBlob struct {
	flowID            string
	blobID            blobIDDef
	blobIDValue       *dexpb.Value
	hydratedValue     *dexpb.Value
	valuePointers     []**dexpb.Value
	valueDescriptions []string
}

func newValueHydrator(
	client dexpb.FlowServiceClient,
	cache *blobcache.Cache,
	logger Logger,
	flowServiceAddress string,
) valueHydrator {
	if client == nil {
		panic("dex: value hydrator requires FlowService client")
	}
	if cache == nil {
		panic("dex: value hydrator requires BlobCache")
	}
	return &valueHydratorImpl{
		client:             client,
		cache:              cache,
		logger:             resolveLogger(logger, cache.Logger()),
		flowServiceAddress: flowServiceAddress,
	}
}

func (hydrator *valueHydratorImpl) HydrateValuesInPlace(
	ctx context.Context,
	flowValuePointers []flowValuePointer,
) error {
	type flowBlobID struct {
		flowID string
		blobID blobIDDef
	}
	pendingBlobs := make([]*pendingBlob, 0, len(flowValuePointers))
	pendingBlobsByID := make(map[flowBlobID]*pendingBlob, len(flowValuePointers))
	for index, target := range flowValuePointers {
		valuePointer := target.valuePointer
		if target.flowID == "" {
			return newWorkerFailure(codes.InvalidArgument, fmt.Errorf("dex: Flow ID at index %d is empty", index))
		}
		if valuePointer == nil {
			return newWorkerFailure(
				codes.InvalidArgument,
				fmt.Errorf("dex: value pointer at index %d is nil", index),
			)
		}
		blobID, found, err := getBlobID(*valuePointer)
		if err != nil {
			return newWorkerFailure(codes.InvalidArgument, err)
		}
		if !found {
			if err := validateConcreteValue(*valuePointer); err != nil {
				return newWorkerFailure(codes.InvalidArgument, err)
			}
			continue
		}
		key := flowBlobID{flowID: target.flowID, blobID: blobID}
		pending, found := pendingBlobsByID[key]
		if !found {
			pending = &pendingBlob{
				flowID:      target.flowID,
				blobID:      blobID,
				blobIDValue: *valuePointer,
			}
			pendingBlobsByID[key] = pending
			pendingBlobs = append(pendingBlobs, pending)
		}
		pending.valuePointers = append(pending.valuePointers, valuePointer)
		valueDescription := target.valueDescription
		if valueDescription == "" {
			valueDescription = fmt.Sprintf("value at index %d", index)
		}
		pending.valueDescriptions = append(pending.valueDescriptions, valueDescription)
	}
	if len(pendingBlobs) == 0 {
		return nil
	}

	if err := hydrator.hydrateBlobValues(ctx, pendingBlobs); err != nil {
		return err
	}
	for _, pending := range pendingBlobs {
		for _, valuePointer := range pending.valuePointers {
			*valuePointer = pending.hydratedValue
		}
	}
	return nil
}

func (hydrator *valueHydratorImpl) hydrateBlobValues(
	ctx context.Context,
	pendingBlobs []*pendingBlob,
) error {
	misses := make([]*pendingBlob, 0, len(pendingBlobs))
	for _, pending := range pendingBlobs {
		cached, found := hydrator.loadCached(pending.flowID, pending.blobIDValue, pending.blobID)
		if found {
			pending.hydratedValue = cached
			continue
		}
		misses = append(misses, pending)
	}
	if len(misses) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	values, err := hydrator.loadBlobValues(ctx, misses)
	if err != nil {
		return err
	}
	omittedFlowIDs := make([]string, 0)
	omittedBlobsByFlow := make(map[string][]string)
	for _, miss := range misses {
		if _, found := values[miss.blobID.value]; !found {
			if _, found := omittedBlobsByFlow[miss.flowID]; !found {
				omittedFlowIDs = append(omittedFlowIDs, miss.flowID)
			}
			omittedBlobsByFlow[miss.flowID] = append(omittedBlobsByFlow[miss.flowID], fmt.Sprintf(
				"blob %q (%s)", miss.blobID.value, strings.Join(miss.valueDescriptions, ", "),
			))
		}
	}
	if len(omittedFlowIDs) > 0 {
		omittedFlows := make([]string, 0, len(omittedFlowIDs))
		for _, flowID := range omittedFlowIDs {
			omittedFlows = append(omittedFlows, fmt.Sprintf(
				"Flow ID %q: %s", flowID, strings.Join(omittedBlobsByFlow[flowID], "; "),
			))
		}
		omissionError := fmt.Errorf(
			"dex: LoadBlobs omitted blobs; FlowServiceAddress %q; for Worker calls, the Worker's Flow service may differ from the dispatching Server; verify that they match; %s",
			hydrator.flowServiceAddress, strings.Join(omittedFlows, "; "),
		)
		if len(omissionError.Error()) > maxWorkerErrorDetailBytes {
			hydrator.logger.Error("LoadBlobs omitted blobs", "error", omissionError)
		}
		return newWorkerFailure(codes.Internal, omissionError)
	}
	for _, miss := range misses {
		concrete := values[miss.blobID.value]
		if err := validateHydratedValue(miss.blobID, concrete); err != nil {
			return newWorkerFailure(codes.Internal, err)
		}
		miss.hydratedValue = concrete
		hydrator.storeCached(miss.flowID, miss.blobIDValue, miss.blobID, concrete)
	}
	return nil
}

// loadBlobValues returns the hydrated values keyed by blob ID.
// A batch whose response exceeds a gRPC message size limit is split in half,
// so N misses take at most 2N-1 LoadBlobs calls.
// It returns a BlobMessageTooLargeError when one blob alone exceeds the limit.
func (hydrator *valueHydratorImpl) loadBlobValues(
	ctx context.Context,
	misses []*pendingBlob,
) (map[string]*dexpb.Value, error) {
	entries := make([]*dexpb.LoadBlobRequestEntry, 0, len(misses))
	for _, miss := range misses {
		entries = append(entries, &dexpb.LoadBlobRequestEntry{
			FlowId:    miss.flowID,
			BlobValue: miss.blobIDValue,
		})
	}
	response, err := hydrator.client.LoadBlobs(ctx, &dexpb.LoadBlobsRequest{
		Entries: entries,
	})
	if err == nil {
		if response == nil {
			return nil, newWorkerFailure(
				codes.Internal,
				fmt.Errorf("dex: LoadBlobs returned a nil response"),
			)
		}
		return response.Values, nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if !isGrpcMessageSizeError(err) {
		return nil, newWorkerFailure(
			codes.Internal,
			fmt.Errorf("dex: LoadBlobs: %w", err),
		)
	}
	if len(misses) == 1 {
		return nil, newWorkerFailure(codes.Internal, &BlobMessageTooLargeError{
			FlowID: misses[0].flowID,
			BlobID: misses[0].blobID.value,
			Err:    err,
		})
	}
	half := len(misses) / 2
	values, err := hydrator.loadBlobValues(ctx, misses[:half])
	if err != nil {
		return nil, err
	}
	secondValues, err := hydrator.loadBlobValues(ctx, misses[half:])
	if err != nil {
		return nil, err
	}
	merged := make(map[string]*dexpb.Value, len(values)+len(secondValues))
	maps.Copy(merged, values)
	maps.Copy(merged, secondValues)
	return merged, nil
}

func (hydrator *valueHydratorImpl) loadCached(
	flowID string,
	request *dexpb.Value,
	blobID blobIDDef,
) (*dexpb.Value, bool) {
	cacheKey := flowBlobCacheKey(flowID, blobID.value)
	payload, found, err := hydrator.cache.Get(cacheKey)
	if err != nil {
		hydrator.logger.Warn("read blob cache", "blob_id", blobID.value, "error", err)
		return nil, false
	}
	if !found {
		return nil, false
	}
	value, err := unmarshalBlobCachePayload(request, payload)
	if err == nil {
		return value, true
	}
	hydrator.logger.Warn("decode blob cache", "blob_id", blobID.value, "error", err)
	if deleteErr := hydrator.cache.Delete(cacheKey); deleteErr != nil {
		hydrator.logger.Warn(
			"delete blob cache entry",
			"blob_id", blobID.value,
			"error", deleteErr,
		)
	}
	return nil, false
}

func (hydrator *valueHydratorImpl) storeCached(
	flowID string,
	request *dexpb.Value,
	blobID blobIDDef,
	concrete *dexpb.Value,
) {
	payload, err := marshalBlobCachePayload(request, concrete)
	if err != nil {
		hydrator.logger.Warn("encode blob cache", "blob_id", blobID.value, "error", err)
		return
	}
	cached, err := hydrator.cache.Put(flowBlobCacheKey(flowID, blobID.value), payload)
	if err != nil {
		hydrator.logger.Warn("write blob cache", "blob_id", blobID.value, "error", err)
		return
	}
	if !cached {
		hydrator.logger.Debug("blob cache rejected entry", "blob_id", blobID.value)
	}
}

// gRPC reports a message size violation as RESOURCE_EXHAUSTED without details; Dex errors,
// tenant rate limits included, always carry details. The status code decides, never the message text.
func isGrpcMessageSizeError(err error) bool {
	rpcStatus, ok := status.FromError(err)
	return ok && rpcStatus.Code() == codes.ResourceExhausted && len(rpcStatus.Details()) == 0
}

func flowBlobCacheKey(flowID string, blobID string) string {
	return fmt.Sprintf("%d:%s%s", len(flowID), flowID, blobID)
}

func valuePointersForFlow(flowID string, valuePointers []**dexpb.Value) []flowValuePointer {
	targets := make([]flowValuePointer, 0, len(valuePointers))
	for _, valuePointer := range valuePointers {
		targets = append(targets, flowValuePointer{flowID: flowID, valuePointer: valuePointer})
	}
	return targets
}

func marshalBlobCachePayload(
	blobIDValue *dexpb.Value,
	concreteValue *dexpb.Value,
) ([]byte, error) {
	blobID, found, err := getBlobID(blobIDValue)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("dex: cache payload requires a blob ID")
	}
	if err := validateHydratedValue(blobID, concreteValue); err != nil {
		return nil, err
	}
	if blobID.isObjectOrString {
		return proto.MarshalOptions{Deterministic: true}.Marshal(
			concreteValue.GetObjValue(),
		)
	}
	return []byte(concreteValue.GetStringValue()), nil
}

func unmarshalBlobCachePayload(
	blobIDValue *dexpb.Value,
	payload []byte,
) (*dexpb.Value, error) {
	blobID, found, err := getBlobID(blobIDValue)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("dex: cache payload requires a blob ID")
	}
	if !blobID.isObjectOrString {
		if !utf8.Valid(payload) {
			return nil, fmt.Errorf("dex: cached string blob is not valid UTF-8")
		}
		return &dexpb.Value{
			Kind: &dexpb.Value_StringValue{StringValue: string(payload)},
		}, nil
	}
	object := &dexpb.EncodedObject{}
	if err := proto.Unmarshal(payload, object); err != nil {
		return nil, fmt.Errorf("dex: decode cached object blob: %w", err)
	}
	value := &dexpb.Value{
		Kind: &dexpb.Value_ObjValue{ObjValue: object},
	}
	if err := validateConcreteValue(value); err != nil {
		return nil, err
	}
	return value, nil
}

func getBlobID(value *dexpb.Value) (blobIDDef, bool, error) {
	if value == nil || value.Kind == nil {
		return blobIDDef{}, false, fmt.Errorf("dex: value has no kind")
	}
	switch kind := value.Kind.(type) {
	case *dexpb.Value_InternalBlobIdForStringValue:
		if kind.InternalBlobIdForStringValue == "" {
			return blobIDDef{}, false, fmt.Errorf("dex: string blob ID is empty")
		}
		return blobIDDef{value: kind.InternalBlobIdForStringValue}, true, nil
	case *dexpb.Value_InternalBlobIdForObjValue:
		if kind.InternalBlobIdForObjValue == "" {
			return blobIDDef{}, false, fmt.Errorf("dex: object blob ID is empty")
		}
		return blobIDDef{
			value:            kind.InternalBlobIdForObjValue,
			isObjectOrString: true,
		}, true, nil
	default:
		return blobIDDef{}, false, nil
	}
}

func validateHydratedValue(
	blobID blobIDDef,
	value *dexpb.Value,
) error {
	if blobID.isObjectOrString {
		if _, ok := value.GetKind().(*dexpb.Value_ObjValue); !ok {
			return fmt.Errorf("dex: object blob %q hydrated to %T", blobID.value, value.GetKind())
		}
	} else {
		if _, ok := value.GetKind().(*dexpb.Value_StringValue); !ok {
			return fmt.Errorf("dex: string blob %q hydrated to %T", blobID.value, value.GetKind())
		}
	}
	return validateConcreteValue(value)
}
