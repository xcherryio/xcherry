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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/blob-cache-go/blobcache"
	"github.com/superdurable/dex/sdk-go/gen/dexpb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type fakeHydrationFlowServiceClient struct {
	dexpb.FlowServiceClient
	requests []*dexpb.LoadBlobRequestEntry
	values   map[string]*dexpb.Value
	err      error
}

func (client *fakeHydrationFlowServiceClient) LoadBlobs(
	_ context.Context,
	request *dexpb.LoadBlobsRequest,
	_ ...grpc.CallOption,
) (*dexpb.LoadBlobsResponse, error) {
	client.requests = request.Entries
	if client.err != nil {
		return nil, client.err
	}
	return &dexpb.LoadBlobsResponse{Values: client.values}, nil
}

func TestHydrateValuesDeduplicatesAndPreservesOrder(t *testing.T) {
	stringBlob := &dexpb.Value{
		Kind: &dexpb.Value_InternalBlobIdForStringValue{
			InternalBlobIdForStringValue: "string-blob",
		},
	}
	objectBlob := &dexpb.Value{
		Kind: &dexpb.Value_InternalBlobIdForObjValue{
			InternalBlobIdForObjValue: "object-blob",
		},
	}
	concrete := &dexpb.Value{
		Kind: &dexpb.Value_IntValue{IntValue: 7},
	}
	client := &fakeHydrationFlowServiceClient{
		values: map[string]*dexpb.Value{
			"string-blob": {Kind: &dexpb.Value_StringValue{StringValue: "loaded"}},
			"object-blob": {Kind: &dexpb.Value_ObjValue{ObjValue: &dexpb.EncodedObject{
				Encoding: jsonEncoding,
				Payload:  []byte(`{"value":1}`),
			}}},
		},
	}
	hydrator := newValueHydrator(client, newHydrationCache(t), nil, "flow-test")
	values := []*dexpb.Value{stringBlob, concrete, stringBlob, objectBlob}

	err := hydrator.HydrateValuesInPlace(
		context.Background(),
		valuePointers("flow-1", values),
	)
	require.NoError(t, err)
	require.Len(t, client.requests, 2)
	require.Equal(t, "loaded", values[0].GetStringValue())
	require.Same(t, concrete, values[1])
	require.Equal(t, "loaded", values[2].GetStringValue())
	require.JSONEq(t, `{"value":1}`, string(values[3].GetObjValue().Payload))
}

func TestHydrateHardDeleteAttributeUsesUpdatedValue(t *testing.T) {
	for _, blobID := range []string{"hd/attribute-object", "hd1|run-id|attribute-object"} {
		t.Run(blobID, func(t *testing.T) {
			cache := newHydrationCache(t)
			client := &fakeHydrationFlowServiceClient{values: map[string]*dexpb.Value{
				blobID: {Kind: &dexpb.Value_StringValue{StringValue: "original"}},
			}}
			hydrator := newValueHydrator(client, cache, nil, "flow-test")
			blobReference := &dexpb.Value{Kind: &dexpb.Value_InternalBlobIdForStringValue{
				InternalBlobIdForStringValue: blobID,
			}}
			first := []*dexpb.Value{blobReference}
			require.NoError(t, hydrator.HydrateValuesInPlace(context.Background(), valuePointers("flow-1", first)))
			require.Equal(t, "original", first[0].GetStringValue())

			client.values[blobID] = &dexpb.Value{Kind: &dexpb.Value_StringValue{StringValue: "updated"}}
			second := []*dexpb.Value{blobReference}
			require.NoError(t, hydrator.HydrateValuesInPlace(context.Background(), valuePointers("flow-1", second)))
			require.Equal(t, "updated", second[0].GetStringValue())
			require.Len(t, client.requests, 1)
			_, found, err := cache.Get(flowBlobCacheKey("flow-1", blobID))
			require.NoError(t, err)
			require.False(t, found)
		})
	}
}

func TestHydrateValuesValidatesResponses(t *testing.T) {
	stringBlob := &dexpb.Value{
		Kind: &dexpb.Value_InternalBlobIdForStringValue{
			InternalBlobIdForStringValue: "blob",
		},
	}
	cache := newHydrationCache(t)
	require.Panics(t, func() { newValueHydrator(nil, cache, nil, "flow-test") })
	require.Panics(t, func() {
		newValueHydrator(&fakeHydrationFlowServiceClient{}, nil, nil, "flow-test")
	})

	values := []*dexpb.Value{stringBlob}
	err := newValueHydrator(
		&fakeHydrationFlowServiceClient{},
		cache,
		nil,
		"flow-test",
	).HydrateValuesInPlace(
		context.Background(), valuePointers("flow-1", values),
	)
	require.ErrorContains(t, err, "omitted blob")
	require.Same(t, stringBlob, values[0])

	values = []*dexpb.Value{stringBlob}
	err = newValueHydrator(
		&fakeHydrationFlowServiceClient{values: map[string]*dexpb.Value{
			"blob": {Kind: &dexpb.Value_IntValue{IntValue: 1}},
		}},
		cache,
		nil,
		"flow-test",
	).HydrateValuesInPlace(
		context.Background(), valuePointers("flow-1", values),
	)
	require.ErrorContains(t, err, "hydrated to")
	require.Same(t, stringBlob, values[0])

	values = []*dexpb.Value{stringBlob}
	err = newValueHydrator(
		&fakeHydrationFlowServiceClient{err: errors.New("load failed")},
		cache,
		nil,
		"flow-test",
	).HydrateValuesInPlace(
		context.Background(), valuePointers("flow-1", values),
	)
	require.ErrorContains(t, err, "load failed")
	require.Same(t, stringBlob, values[0])
}

func TestBlobCachePayloadRoundTrip(t *testing.T) {
	stringBlobID := &dexpb.Value{
		Kind: &dexpb.Value_InternalBlobIdForStringValue{
			InternalBlobIdForStringValue: "string",
		},
	}
	stringValue := &dexpb.Value{
		Kind: &dexpb.Value_StringValue{StringValue: "payload"},
	}
	payload, err := marshalBlobCachePayload(stringBlobID, stringValue)
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), payload)
	decoded, err := unmarshalBlobCachePayload(stringBlobID, payload)
	require.NoError(t, err)
	require.Equal(t, "payload", decoded.GetStringValue())
	_, err = unmarshalBlobCachePayload(stringBlobID, []byte{0xff})
	require.ErrorContains(t, err, "UTF-8")

	objectBlobID := &dexpb.Value{
		Kind: &dexpb.Value_InternalBlobIdForObjValue{
			InternalBlobIdForObjValue: "object",
		},
	}
	objectValue := &dexpb.Value{
		Kind: &dexpb.Value_ObjValue{ObjValue: &dexpb.EncodedObject{
			Encoding: rawBytesEncoding,
			Payload:  []byte{0x00, 0xff},
		}},
	}
	first, err := marshalBlobCachePayload(objectBlobID, objectValue)
	require.NoError(t, err)
	second, err := marshalBlobCachePayload(objectBlobID, objectValue)
	require.NoError(t, err)
	require.Equal(t, first, second)
	decoded, err = unmarshalBlobCachePayload(objectBlobID, first)
	require.NoError(t, err)
	require.True(t, proto.Equal(objectValue.GetObjValue(), decoded.GetObjValue()))
}

func valuePointers(flowID string, values []*dexpb.Value) []flowValuePointer {
	pointers := make([]flowValuePointer, len(values))
	for index := range values {
		pointers[index] = flowValuePointer{flowID: flowID, valuePointer: &values[index]}
	}
	return pointers
}

func newHydrationCache(t *testing.T) *blobcache.Cache {
	t.Helper()
	cache, err := blobcache.New(&blobcache.Config{
		Dir:      t.TempDir(),
		MaxBytes: 1 << 20,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cache.Close()) })
	return cache
}
