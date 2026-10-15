// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package integ

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/common/log"
	"github.com/superdurable/dex/service/common/streamstore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	streamTestFlowType = "stream-test-flow"
	streamTestRedisURL = "redis://127.0.0.1:6379/15"
)

func TestStreamStoreReversePaginationRedis(t *testing.T) {
	store, redisClient := newStreamTestStore(t)
	streamName := "reverse-pagination-" + newRequestID()
	t.Cleanup(func() { deleteStreamTestKeys(t, redisClient, streamName) })
	testStreamStoreReversePagination(t, store, streamName)
}

func TestStreamStoreReversePaginationMemory(t *testing.T) {
	store := newMemoryStreamTestStore(t)
	testStreamStoreReversePagination(t, store, "memory-reverse-pagination-"+newRequestID())
}

func testStreamStoreReversePagination(t *testing.T, store *streamstore.Store, streamName string) {
	for index := 0; index < 5; index++ {
		input := streamInput("flow-a", streamName, index, fmt.Sprintf("value-%d", index))
		input.StreamCapacityBytes = 1 << 20
		require.NoError(t, store.Write(context.Background(), input))
	}

	latest, hasMore, err := store.List(
		context.Background(), streamTestFlowType, "flow-a", streamName, 2, "",
	)
	require.NoError(t, err)
	require.True(t, hasMore)
	require.Equal(t, []string{"public-04", "public-03"}, messageKeys(latest))
	beforeToken, err := streamstore.EncodeResumeToken(
		streamTestFlowType, "flow-a", streamName, latest[len(latest)-1].MessageID,
	)
	require.NoError(t, err)

	newInput := streamInput("flow-a", streamName, 5, "new-after-first-page")
	newInput.StreamCapacityBytes = 1 << 20
	require.NoError(t, store.Write(context.Background(), newInput))
	older, hasMore, err := store.List(
		context.Background(), streamTestFlowType, "flow-a", streamName, 2, beforeToken,
	)
	require.NoError(t, err)
	require.True(t, hasMore)
	require.Equal(t, []string{"public-02", "public-01"}, messageKeys(older))
	beforeToken, err = streamstore.EncodeResumeToken(
		streamTestFlowType, "flow-a", streamName, older[len(older)-1].MessageID,
	)
	require.NoError(t, err)

	oldest, hasMore, err := store.List(
		context.Background(), streamTestFlowType, "flow-a", streamName, 2, beforeToken,
	)
	require.NoError(t, err)
	require.False(t, hasMore)
	require.Equal(t, []string{"public-00"}, messageKeys(oldest))
	empty, hasMore, err := store.List(
		context.Background(), streamTestFlowType, "empty-flow", streamName, 2, "",
	)
	require.NoError(t, err)
	require.False(t, hasMore)
	require.Empty(t, empty)

	wrongScopeToken, err := streamstore.EncodeResumeToken(
		streamTestFlowType, "flow-b", streamName, oldest[0].MessageID,
	)
	require.NoError(t, err)
	_, _, err = store.List(
		context.Background(), streamTestFlowType, "flow-a", streamName, 2, wrongScopeToken,
	)
	require.ErrorIs(t, err, streamstore.ErrInvalidResumeToken)
	_, _, err = store.List(
		context.Background(), streamTestFlowType, "flow-a", streamName, 0, "",
	)
	require.ErrorIs(t, err, streamstore.ErrInvalidPageSize)
	_, _, err = store.List(
		context.Background(), streamTestFlowType, "flow-a", streamName, 1001, "",
	)
	require.ErrorIs(t, err, streamstore.ErrInvalidPageSize)
}

func TestStreamStoreGlobalFIFOResumeAndRepeatedSource(t *testing.T) {
	store, redisClient := newStreamTestStore(t)
	streamName := "global-fifo-" + newRequestID()
	t.Cleanup(func() { deleteStreamTestKeys(t, redisClient, streamName) })

	inputs := make([]streamstore.WriteInput, 11)
	for index := range inputs {
		flowID := "flow-a"
		if index%2 == 1 {
			flowID = "flow-b"
		}
		inputs[index] = streamInput(flowID, streamName, index, "value-0000")
	}
	charge := estimatedCharge(inputs[0], 1)
	leaseKey := streamTestBaseKey(streamName) + ":trim-lease"
	require.NoError(t, redisClient.Set(context.Background(), leaseKey, "held", 10*time.Second).Err())
	for index := 0; index < 10; index++ {
		inputs[index].StreamCapacityBytes = charge * 10
		require.NoError(t, store.Write(context.Background(), inputs[index]))
	}
	inputs[10].StreamCapacityBytes = charge * 10
	require.ErrorIs(t, store.Write(context.Background(), inputs[10]), streamstore.ErrCapacityExceeded)
	require.Equal(t, int64(10), streamLength(t, redisClient, streamName))
	require.NoError(t, redisClient.Del(context.Background(), leaseKey).Err())
	require.Eventually(t, func() bool {
		return streamLength(t, redisClient, streamName) == 8
	}, 3*time.Second, 10*time.Millisecond)

	flowAMessages := readAvailableMessages(t, store, "flow-a", streamName)
	flowBMessages := readAvailableMessages(t, store, "flow-b", streamName)
	requireStreamAccountingConsistent(t, redisClient, streamName)
	require.Equal(t, []string{"public-02", "public-04", "public-06", "public-08"}, messageKeys(flowAMessages))
	require.Equal(t, []string{"public-03", "public-05", "public-07", "public-09"}, messageKeys(flowBMessages))
	require.WithinDuration(t, time.Now(), flowAMessages[0].CreatedTime, 5*time.Second)

	firstToken, err := streamstore.EncodeResumeToken(
		streamTestFlowType,
		"flow-a",
		streamName,
		flowAMessages[0].MessageID,
	)
	require.NoError(t, err)
	nextMessage := readOneMessage(t, store, "flow-a", streamName, firstToken)
	require.Equal(t, "public-04", nextMessage.Source)

	duplicate := inputs[8]
	duplicate.Value = &dexpb.Value{Kind: &dexpb.Value_StringValue{StringValue: "different"}}
	require.NoError(t, store.Write(context.Background(), duplicate))
	require.Equal(t, int64(9), streamLength(t, redisClient, streamName))
	flowAMessages = readAvailableMessages(t, store, "flow-a", streamName)
	require.Equal(
		t,
		[]string{"public-02", "public-04", "public-06", "public-08", "public-08"},
		messageKeys(flowAMessages),
	)
	require.Equal(t, "value-0000", flowAMessages[3].Value.GetStringValue())
	require.Equal(t, "different", flowAMessages[4].Value.GetStringValue())
	require.Zero(t, redisClient.Exists(context.Background(), streamTestBaseKey(streamName)+":idem").Val())

	trimTrigger := inputs[10]
	trimTrigger.StreamCapacityBytes = charge * 3
	require.ErrorIs(t, store.Write(context.Background(), trimTrigger), streamstore.ErrCapacityExceeded)
	require.Eventually(t, func() bool {
		return streamLength(t, redisClient, streamName) <= 2
	}, 3*time.Second, 10*time.Millisecond)
	require.NoError(t, store.Write(context.Background(), trimTrigger))
	require.Eventually(t, func() bool {
		return streamLength(t, redisClient, streamName) <= 2
	}, 3*time.Second, 10*time.Millisecond)
	requireStreamAccountingConsistent(t, redisClient, streamName)
	oldTokenMessage := readOneMessage(t, store, "flow-a", streamName, firstToken)
	require.Equal(t, "public-08", oldTokenMessage.Source)
	require.Equal(t, "different", oldTokenMessage.Value.GetStringValue())
	duplicateToken, err := streamstore.EncodeResumeToken(
		streamTestFlowType,
		"flow-a",
		streamName,
		oldTokenMessage.MessageID,
	)
	require.NoError(t, err)
	require.Equal(t, "public-10", readOneMessage(t, store, "flow-a", streamName, duplicateToken).Source)

	wrongScopeToken, err := streamstore.EncodeResumeToken(
		streamTestFlowType,
		"flow-b",
		streamName,
		flowBMessages[0].MessageID,
	)
	require.NoError(t, err)
	readCtx, cancelRead := context.WithTimeout(context.Background(), time.Second)
	defer cancelRead()
	_, err = store.Read(readCtx, streamTestFlowType, "flow-a", streamName, wrongScopeToken)
	require.ErrorIs(t, err, streamstore.ErrInvalidResumeToken)
	trimmedPage, hasMore, err := store.List(
		context.Background(), streamTestFlowType, "flow-a", streamName, 2, firstToken,
	)
	require.NoError(t, err)
	require.False(t, hasMore)
	require.Empty(t, trimmedPage)
}

func TestStreamStoreFlowTypesIsolateStreamScopes(t *testing.T) {
	store, redisClient := newStreamTestStore(t)
	streamName := "flow-type-scope-" + newRequestID()
	firstFlowType := "first-flow-type"
	secondFlowType := "second-flow-type"
	t.Cleanup(func() { deleteStreamTestKeysForFlowType(t, redisClient, firstFlowType, streamName) })
	t.Cleanup(func() { deleteStreamTestKeysForFlowType(t, redisClient, secondFlowType, streamName) })

	baseInput := streamInput("shared-flow", streamName, 0, "payload-0000")
	charge := estimatedCharge(baseInput, 1)
	for _, flowType := range []string{firstFlowType, secondFlowType} {
		leaseKey := streamTestBaseKeyForFlowType(flowType, streamName) + ":trim-lease"
		require.NoError(t, redisClient.Set(context.Background(), leaseKey, "held", 10*time.Second).Err())
	}
	for index := 0; index < 2; index++ {
		firstInput := streamInput("shared-flow", streamName, index, "payload-0000")
		firstInput.FlowType = firstFlowType
		firstInput.StreamCapacityBytes = charge * 2
		require.NoError(t, store.Write(context.Background(), firstInput))

		secondInput := streamInput("shared-flow", streamName, index, "payload-0000")
		secondInput.FlowType = secondFlowType
		secondInput.StreamCapacityBytes = charge * 3
		require.NoError(t, store.Write(context.Background(), secondInput))
	}
	firstThird := streamInput("shared-flow", streamName, 2, "payload-0000")
	firstThird.FlowType = firstFlowType
	firstThird.StreamCapacityBytes = charge * 2
	require.ErrorIs(t, store.Write(context.Background(), firstThird), streamstore.ErrCapacityExceeded)
	secondThird := streamInput("shared-flow", streamName, 2, "payload-0000")
	secondThird.FlowType = secondFlowType
	secondThird.StreamCapacityBytes = charge * 3
	require.NoError(t, store.Write(context.Background(), secondThird))

	require.Equal(t, int64(2), streamLengthForFlowType(t, redisClient, firstFlowType, streamName))
	require.Equal(t, int64(3), streamLengthForFlowType(t, redisClient, secondFlowType, streamName))
	require.NotEqual(
		t,
		streamTestBaseKeyForFlowType(firstFlowType, streamName),
		streamTestBaseKeyForFlowType(secondFlowType, streamName),
	)
	readCtx, cancelRead := context.WithTimeout(context.Background(), time.Second)
	firstMessage, err := store.Read(readCtx, firstFlowType, "shared-flow", streamName, "")
	cancelRead()
	require.NoError(t, err)
	firstToken, err := streamstore.EncodeResumeToken(
		firstFlowType,
		"shared-flow",
		streamName,
		firstMessage.MessageID,
	)
	require.NoError(t, err)
	wrongTypeCtx, cancelWrongTypeRead := context.WithTimeout(context.Background(), time.Second)
	defer cancelWrongTypeRead()
	_, err = store.Read(wrongTypeCtx, secondFlowType, "shared-flow", streamName, firstToken)
	require.ErrorIs(t, err, streamstore.ErrInvalidResumeToken)
}

func TestStreamStoreTrimWatermarks(t *testing.T) {
	store, redisClient := newStreamTestStore(t)
	streamName := "trim-watermarks-" + newRequestID()
	t.Cleanup(func() { deleteStreamTestKeys(t, redisClient, streamName) })

	baseInput := streamInput("flow-a", streamName, 0, "payload-0000")
	charge := estimatedCharge(baseInput, 1)
	capacity := charge * 10
	for index := 0; index < 8; index++ {
		input := streamInput("flow-a", streamName, index, "payload-0000")
		input.StreamCapacityBytes = capacity
		require.NoError(t, store.Write(context.Background(), input))
	}
	require.Never(t, func() bool {
		return streamLength(t, redisClient, streamName) != 8
	}, 200*time.Millisecond, 10*time.Millisecond)

	trigger := streamInput("flow-a", streamName, 8, "payload-0000")
	trigger.StreamCapacityBytes = capacity
	require.NoError(t, store.Write(context.Background(), trigger))
	require.Eventually(t, func() bool {
		return streamLength(t, redisClient, streamName) == 8
	}, 3*time.Second, 10*time.Millisecond)
	require.Equal(t, []string{
		"public-01",
		"public-02",
		"public-03",
		"public-04",
		"public-05",
		"public-06",
		"public-07",
		"public-08",
	}, messageKeys(readAvailableMessages(t, store, "flow-a", streamName)))
	requireStreamAccountingConsistent(t, redisClient, streamName)
}

func TestStreamStoreTrimmedSourceCanWriteAgain(t *testing.T) {
	store, redisClient := newStreamTestStore(t)
	streamName := "trimmed-idem-" + newRequestID()
	t.Cleanup(func() { deleteStreamTestKeys(t, redisClient, streamName) })

	original := streamInput("flow-a", streamName, 0, "original-00")
	charge := estimatedCharge(original, 1)
	original.StreamCapacityBytes = charge * 100
	require.NoError(t, store.Write(context.Background(), original))
	for index := 1; index < 80; index++ {
		input := streamInput("flow-a", streamName, index, "filler-0000")
		input.StreamCapacityBytes = charge * 100
		require.NoError(t, store.Write(context.Background(), input))
	}
	trigger := streamInput("flow-a", streamName, 80, "trigger-000")
	trigger.StreamCapacityBytes = charge * 2
	require.ErrorIs(t, store.Write(context.Background(), trigger), streamstore.ErrCapacityExceeded)
	require.Eventually(t, func() bool {
		return streamLength(t, redisClient, streamName) <= 1
	}, 3*time.Second, 10*time.Millisecond)
	requireStreamAccountingConsistent(t, redisClient, streamName)

	original.Value = &dexpb.Value{Kind: &dexpb.Value_StringValue{StringValue: "rewritten-0"}}
	original.StreamCapacityBytes = charge * 2
	require.NoError(t, store.Write(context.Background(), original))
	require.Eventually(t, func() bool {
		return streamLength(t, redisClient, streamName) <= 1
	}, 3*time.Second, 10*time.Millisecond)
	messages := readAvailableMessages(t, store, "flow-a", streamName)
	foundRewritten := false
	for _, message := range messages {
		if message.Source == original.Source {
			foundRewritten = message.Value.GetStringValue() == "rewritten-0"
		}
	}
	require.True(t, foundRewritten)
}

func TestStreamStoreConcurrentTriggersUseSingletonTrimLease(t *testing.T) {
	firstStore, redisClient := newStreamTestStore(t)
	secondStore, _ := newStreamTestStore(t)
	streamName := "concurrent-trim-" + newRequestID()
	t.Cleanup(func() { deleteStreamTestKeys(t, redisClient, streamName) })

	baseInput := streamInput("flow-a", streamName, 0, "payload-0000")
	baseInput.Source = "run#shared-step"
	charge := estimatedCharge(baseInput, 1)
	var writers sync.WaitGroup
	writeErrors := make(chan error, 80)
	for index := 0; index < 80; index++ {
		writers.Add(1)
		go func(messageIndex int) {
			defer writers.Done()
			input := streamInput("flow-a", streamName, messageIndex, "payload-0000")
			input.Source = "run#shared-step"
			input.StreamCapacityBytes = charge * 100
			selectedStore := firstStore
			if messageIndex%2 == 1 {
				selectedStore = secondStore
			}
			writeErrors <- selectedStore.Write(context.Background(), input)
		}(index)
	}
	writers.Wait()
	close(writeErrors)
	for writeErr := range writeErrors {
		require.NoError(t, writeErr)
	}
	require.Equal(t, int64(80), streamLength(t, redisClient, streamName))
	for _, message := range readAvailableMessages(t, firstStore, "flow-a", streamName) {
		require.Equal(t, "run#shared-step", message.Source)
	}

	leaseKey := streamTestBaseKey(streamName) + ":trim-lease"
	require.NoError(t, redisClient.Set(context.Background(), leaseKey, "failed-owner", 250*time.Millisecond).Err())
	firstTrigger := streamInput("flow-a", streamName, 80, "payload-0000")
	firstTrigger.Source = "run#shared-step"
	firstTrigger.StreamCapacityBytes = charge * 10
	secondTrigger := streamInput("flow-a", streamName, 81, "payload-0000")
	secondTrigger.Source = "run#shared-step"
	secondTrigger.StreamCapacityBytes = charge * 10
	triggerStart := make(chan struct{})
	triggerErrors := make(chan error, 2)
	go func() {
		<-triggerStart
		triggerErrors <- firstStore.Write(context.Background(), firstTrigger)
	}()
	go func() {
		<-triggerStart
		triggerErrors <- secondStore.Write(context.Background(), secondTrigger)
	}()
	close(triggerStart)
	require.ErrorIs(t, <-triggerErrors, streamstore.ErrCapacityExceeded)
	require.ErrorIs(t, <-triggerErrors, streamstore.ErrCapacityExceeded)
	require.Equal(t, int64(80), streamLength(t, redisClient, streamName))
	require.Eventually(t, func() bool {
		return streamLength(t, redisClient, streamName) == 8
	}, 4*time.Second, 10*time.Millisecond)
	require.NoError(t, firstStore.Write(context.Background(), firstTrigger))
	require.Eventually(t, func() bool {
		return streamLength(t, redisClient, streamName) == 8
	}, 4*time.Second, 10*time.Millisecond)
	requireStreamAccountingConsistent(t, redisClient, streamName)
}

func TestStreamStoreMessageSizeLimit(t *testing.T) {
	store, redisClient := newStreamTestStoreWithConfig(t, config.StreamStoreConfig{
		Backend:                       config.StreamStoreBackendRedis,
		RedisURL:                      streamTestRedisURL,
		MaxMessageBytes:               32,
		EstimatedMessageOverheadBytes: 1,
		TrimTriggerPercent:            90,
		TrimTargetPercent:             80,
		TrimWorkers:                   2,
	})
	streamName := "message-size-" + newRequestID()
	t.Cleanup(func() { deleteStreamTestKeys(t, redisClient, streamName) })

	acceptedInput := streamInput("flow-a", streamName, 0, "accepted")
	acceptedInput.StreamCapacityBytes = 1 << 20
	require.NoError(t, store.Write(context.Background(), acceptedInput))

	oversizedInput := streamInput("flow-a", streamName, 1, string(make([]byte, 32)))
	oversizedInput.StreamCapacityBytes = 1 << 20
	require.ErrorIs(t, store.Write(context.Background(), oversizedInput), streamstore.ErrMessageTooLarge)
	require.Equal(t, int64(1), streamLength(t, redisClient, streamName))

	defaultStore, _ := newStreamTestStore(t)
	defaultStreamName := "default-message-size-" + newRequestID()
	t.Cleanup(func() { deleteStreamTestKeys(t, redisClient, defaultStreamName) })
	defaultInput := streamInput(
		"flow-a",
		defaultStreamName,
		0,
		string(make([]byte, config.DefaultStreamMaxMessageBytes)),
	)
	defaultInput.StreamCapacityBytes = 1 << 20
	require.ErrorIs(t, defaultStore.Write(context.Background(), defaultInput), streamstore.ErrMessageTooLarge)
	require.Equal(t, int64(0), streamLength(t, redisClient, defaultStreamName))
}

func TestStreamStoreMemoryResumeRepeatedSourceAndBlockingRead(t *testing.T) {
	store := newMemoryStreamTestStore(t)
	streamName := "memory-resume-" + newRequestID()
	firstInput := streamInput("flow-a", streamName, 0, "first")
	firstInput.Source = "run#step"
	firstInput.StreamCapacityBytes = 1 << 20
	require.NoError(t, store.Write(context.Background(), firstInput))

	firstMessage := readOneMessage(t, store, "flow-a", streamName, "")
	require.Equal(t, "first", firstMessage.Value.GetStringValue())
	require.Equal(t, firstInput.Source, firstMessage.Source)
	firstToken, err := streamstore.EncodeResumeToken(
		streamTestFlowType,
		"flow-a",
		streamName,
		firstMessage.MessageID,
	)
	require.NoError(t, err)

	duplicate := firstInput
	duplicate.Value = stringValue("duplicate")
	require.NoError(t, store.Write(context.Background(), duplicate))
	require.Len(t, readAvailableMessages(t, store, "flow-a", streamName), 2)
	duplicateMessage := readOneMessage(t, store, "flow-a", streamName, firstToken)
	require.Equal(t, "duplicate", duplicateMessage.Value.GetStringValue())
	require.Equal(t, "run#step", duplicateMessage.Source)
	duplicateToken, err := streamstore.EncodeResumeToken(
		streamTestFlowType,
		"flow-a",
		streamName,
		duplicateMessage.MessageID,
	)
	require.NoError(t, err)

	readResult := make(chan *streamstore.Message, 1)
	readErrors := make(chan error, 1)
	readCtx, cancelRead := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelRead()
	go func() {
		message, readErr := store.Read(readCtx, streamTestFlowType, "flow-a", streamName, duplicateToken)
		if readErr != nil {
			readErrors <- readErr
			return
		}
		readResult <- message
	}()
	otherFlow := streamInput("flow-b", streamName, 1, "other-flow")
	otherFlow.StreamCapacityBytes = 1 << 20
	require.NoError(t, store.Write(context.Background(), otherFlow))
	require.Never(t, func() bool {
		return len(readResult) != 0 || len(readErrors) != 0
	}, 200*time.Millisecond, 10*time.Millisecond)
	secondInput := streamInput("flow-a", streamName, 2, "second")
	secondInput.StreamCapacityBytes = 1 << 20
	require.NoError(t, store.Write(context.Background(), secondInput))
	select {
	case readErr := <-readErrors:
		require.NoError(t, readErr)
	case message := <-readResult:
		require.Equal(t, "second", message.Value.GetStringValue())
	case <-readCtx.Done():
		require.FailNow(t, "Memory Stream read did not resume", readCtx.Err())
	}
}

func TestStreamStoreMemoryTrimWatermarks(t *testing.T) {
	store := newMemoryStreamTestStore(t)
	streamName := "memory-watermarks-" + newRequestID()
	baseInput := streamInput("flow-a", streamName, 0, "payload-0000")
	charge := estimatedCharge(baseInput, 1)
	capacity := charge * 10
	for index := 0; index < 8; index++ {
		input := streamInput("flow-a", streamName, index, "payload-0000")
		input.StreamCapacityBytes = capacity
		require.NoError(t, store.Write(context.Background(), input))
	}
	require.Len(t, readAvailableMessages(t, store, "flow-a", streamName), 8)
	oldestBeforeTrim := readOneMessage(t, store, "flow-a", streamName, "")
	oldestBeforeTrimToken, err := streamstore.EncodeResumeToken(
		streamTestFlowType, "flow-a", streamName, oldestBeforeTrim.MessageID,
	)
	require.NoError(t, err)

	trigger := streamInput("flow-a", streamName, 8, "payload-0000")
	trigger.StreamCapacityBytes = capacity
	require.NoError(t, store.Write(context.Background(), trigger))
	require.Eventually(t, func() bool {
		readCtx, cancelRead := context.WithTimeout(context.Background(), 100*time.Millisecond)
		message, readErr := store.Read(readCtx, streamTestFlowType, "flow-a", streamName, "")
		cancelRead()
		return readErr == nil && message.Source == "public-01"
	}, 3*time.Second, 10*time.Millisecond)
	require.Equal(t, []string{
		"public-01",
		"public-02",
		"public-03",
		"public-04",
		"public-05",
		"public-06",
		"public-07",
		"public-08",
	}, messageKeys(readAvailableMessages(t, store, "flow-a", streamName)))
	trimmedPage, hasMore, err := store.List(
		context.Background(), streamTestFlowType, "flow-a", streamName, 2, oldestBeforeTrimToken,
	)
	require.NoError(t, err)
	require.False(t, hasMore)
	require.Empty(t, trimmedPage)
}

func TestStreamStoreMemoryConcurrentCapacityTriggersDoNotOverTrim(t *testing.T) {
	store := newMemoryStreamTestStore(t)
	streamName := "memory-trim-" + newRequestID()
	baseInput := streamInput("flow-a", streamName, 0, "payload-0000")
	baseInput.Source = "run#shared-step"
	charge := estimatedCharge(baseInput, 1)
	for index := 0; index < 80; index++ {
		input := streamInput("flow-a", streamName, index, "payload-0000")
		input.Source = "run#shared-step"
		input.StreamCapacityBytes = charge * 100
		require.NoError(t, store.Write(context.Background(), input))
	}
	require.Len(t, readAvailableMessages(t, store, "flow-a", streamName), 80)

	triggerStart := make(chan struct{})
	triggerErrors := make(chan error, 20)
	var writers sync.WaitGroup
	for index := 80; index < 100; index++ {
		writers.Add(1)
		go func(messageIndex int) {
			defer writers.Done()
			<-triggerStart
			input := streamInput("flow-a", streamName, messageIndex, "payload-0000")
			input.Source = "run#shared-step"
			input.StreamCapacityBytes = charge * 10
			triggerErrors <- store.Write(context.Background(), input)
		}(index)
	}
	close(triggerStart)
	writers.Wait()
	close(triggerErrors)
	rejectedWrites := 0
	for writeErr := range triggerErrors {
		if writeErr == nil {
			continue
		}
		require.ErrorIs(t, writeErr, streamstore.ErrCapacityExceeded)
		rejectedWrites++
	}
	require.Positive(t, rejectedWrites)
	retained := waitForStreamMessageLimit(t, store, "flow-a", streamName, 9)
	require.NotEmpty(t, retained)
	require.GreaterOrEqual(t, len(retained), 8)
	require.LessOrEqual(t, len(retained), 9)

	lastToken, err := streamstore.EncodeResumeToken(
		streamTestFlowType,
		"flow-a",
		streamName,
		retained[len(retained)-1].MessageID,
	)
	require.NoError(t, err)
	rewritten := baseInput
	rewritten.Value = stringValue("rewritten")
	rewritten.StreamCapacityBytes = charge * 10
	require.NoError(t, store.Write(context.Background(), rewritten))
	rewrittenMessage := readOneMessage(t, store, "flow-a", streamName, lastToken)
	require.Equal(t, "rewritten", rewrittenMessage.Value.GetStringValue())
}

func TestStreamStoreMemoryResumeTokenSurvivesProcessRestartBestEffort(t *testing.T) {
	streamName := "memory-restart-" + newRequestID()
	firstStore := newMemoryStreamTestStore(t)
	firstInput := streamInput("flow-a", streamName, 0, "before-restart")
	firstInput.StreamCapacityBytes = 1 << 20
	require.NoError(t, firstStore.Write(context.Background(), firstInput))
	firstMessage := readOneMessage(t, firstStore, "flow-a", streamName, "")
	resumeToken, err := streamstore.EncodeResumeToken(
		streamTestFlowType,
		"flow-a",
		streamName,
		firstMessage.MessageID,
	)
	require.NoError(t, err)
	require.NoError(t, firstStore.Close())
	require.Eventually(t, func() bool {
		return time.Now().UnixMilli() > firstMessage.CreatedTime.UnixMilli()
	}, time.Second, time.Millisecond)

	secondStore := newMemoryStreamTestStore(t)
	afterRestart := streamInput("flow-a", streamName, 1, "after-restart")
	afterRestart.StreamCapacityBytes = 1 << 20
	require.NoError(t, secondStore.Write(context.Background(), afterRestart))
	message := readOneMessage(t, secondStore, "flow-a", streamName, resumeToken)
	require.Equal(t, "after-restart", message.Value.GetStringValue())
}

func TestStreamStoreBackendConfiguration(t *testing.T) {
	require.Equal(t, int32(1000), (&config.StreamStoreConfig{}).EffectiveMaxListMessagesPageSize())
	require.Equal(t, int32(17), (&config.StreamStoreConfig{MaxReadMessages: 17}).EffectiveMaxListMessagesPageSize())
	disabledStore, err := streamstore.New(&config.StreamStoreConfig{}, log.NewNoop())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, disabledStore.Close()) })
	disabledInput := streamInput("flow-a", "disabled", 0, "value")
	disabledInput.StreamCapacityBytes = 1024
	require.ErrorIs(t, disabledStore.Write(context.Background(), disabledInput), streamstore.ErrDisabled)

	_, err = streamstore.New(&config.StreamStoreConfig{RedisURL: streamTestRedisURL}, log.NewNoop())
	require.ErrorContains(t, err, "redisURL requires redis backend")
	_, err = streamstore.New(&config.StreamStoreConfig{
		Backend:  config.StreamStoreBackendMemory,
		RedisURL: streamTestRedisURL,
	}, log.NewNoop())
	require.ErrorContains(t, err, "memory backend does not use redisURL")
	_, err = streamstore.New(&config.StreamStoreConfig{
		Backend: config.StreamStoreBackendRedis,
	}, log.NewNoop())
	require.ErrorContains(t, err, "redis backend requires redisURL")
	_, err = streamstore.New(&config.StreamStoreConfig{
		Backend: "unsupported",
	}, log.NewNoop())
	require.ErrorContains(t, err, "unsupported stream store backend")
	_, err = streamstore.New(&config.StreamStoreConfig{MaxReadMessages: -1}, log.NewNoop())
	require.ErrorContains(t, err, "maxReadMessages must be positive")
}

type streamingStepWorker struct {
	dexpb.UnimplementedWorkerServiceServer
	executeCalls       atomic.Int32
	restoredHeartbeats chan string
}

func (worker *streamingStepWorker) InvokeWaitForMethod(
	_ *dexpb.InvokeWaitForMethodRequest,
	stream grpc.ServerStreamingServer[dexpb.InvokeWaitForMethodOutput],
) error {
	outputs := []*dexpb.InvokeWaitForMethodOutput{
		{Output: &dexpb.InvokeWaitForMethodOutput_Heartbeat{
			Heartbeat: &dexpb.StepMethodHeartbeat{Value: stringValue("wait-checkpoint")},
		}},
		waitForStreamOutput("progress", "wait-1"),
		waitForStreamOutput("progress", "wait-2"),
		waitForStreamOutput("other", "wait-other"),
		{Output: &dexpb.InvokeWaitForMethodOutput_Result{
			Result: &dexpb.InvokeWaitForMethodResponse{},
		}},
	}
	for _, output := range outputs {
		if err := stream.Send(output); err != nil {
			return err
		}
	}
	return nil
}

func (worker *streamingStepWorker) InvokeExecuteMethod(
	request *dexpb.InvokeExecuteMethodRequest,
	stream grpc.ServerStreamingServer[dexpb.InvokeExecuteMethodOutput],
) error {
	attempt := worker.executeCalls.Add(1)
	if attempt > 1 {
		worker.restoredHeartbeats <- request.GetContext().GetLastHeartbeatValue().GetStringValue()
	}
	if attempt == 1 {
		if err := stream.Send(&dexpb.InvokeExecuteMethodOutput{
			Output: &dexpb.InvokeExecuteMethodOutput_Heartbeat{
				Heartbeat: &dexpb.StepMethodHeartbeat{Value: stringValue("execute-checkpoint")},
			},
		}); err != nil {
			return err
		}
	} else if attempt == 2 {
		if err := stream.Send(&dexpb.InvokeExecuteMethodOutput{
			Output: &dexpb.InvokeExecuteMethodOutput_Heartbeat{
				Heartbeat: &dexpb.StepMethodHeartbeat{},
			},
		}); err != nil {
			return err
		}
	}
	if err := stream.Send(executeStreamOutput("progress", fmt.Sprintf("execute-%d", attempt))); err != nil {
		return err
	}
	if attempt < 3 {
		return status.Error(codes.Unavailable, "retry after streamed progress")
	}
	if err := stream.Send(executeStreamOutput("other", "execute-other")); err != nil {
		return err
	}
	return stream.Send(&dexpb.InvokeExecuteMethodOutput{
		Output: &dexpb.InvokeExecuteMethodOutput_Result{
			Result: &dexpb.InvokeExecuteMethodResponse{
				StepDecision: &dexpb.StepDecision{CloseDecision: &dexpb.CloseDecision{
					CloseDecisionType: dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_GRACEFUL_COMPLETE,
				}},
			},
		},
	})
}

func waitForStreamOutput(streamName string, value string) *dexpb.InvokeWaitForMethodOutput {
	return &dexpb.InvokeWaitForMethodOutput{
		Output: &dexpb.InvokeWaitForMethodOutput_StreamWrite{
			StreamWrite: &dexpb.StepStreamWrite{
				StreamName:          streamName,
				StreamCapacityBytes: 1 << 20,
				Value:               stringValue(value),
			},
		},
	}
}

func executeStreamOutput(streamName string, value string) *dexpb.InvokeExecuteMethodOutput {
	return &dexpb.InvokeExecuteMethodOutput{
		Output: &dexpb.InvokeExecuteMethodOutput_StreamWrite{
			StreamWrite: &dexpb.StepStreamWrite{
				StreamName:          streamName,
				StreamCapacityBytes: 1 << 20,
				Value:               stringValue(value),
			},
		},
	}
}

func TestStreamStepWorkerStreamingTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	testStepWorkerStreaming(t, service.BackendTypeTemporal)
}

func TestStreamStepWorkerStreamingCadence(t *testing.T) {
	if !*cadenceIntegTest {
		t.Skip()
	}
	testStepWorkerStreaming(t, service.BackendTypeCadence)
}

func testStepWorkerStreaming(t *testing.T, backendType service.BackendType) {
	worker := &streamingStepWorker{restoredHeartbeats: make(chan string, 2)}
	workerTarget := startStreamingWorker(t, worker)
	runtime := startDexService(t, DexServiceTestConfig{
		BackendType: backendType,
		StreamStore: config.StreamStoreConfig{Backend: config.StreamStoreBackendMemory},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	flowID := "step-streaming-" + newRequestID()
	_, err := runtime.FlowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             flowID,
		FlowType:           "step-streaming-flow",
		FlowTimeoutSeconds: 25,
		StartStepType:      "stream-step",
		FlowStartOptions:   withWorkerTarget(nil, workerTarget),
		StepOptions: &dexpb.StepOptions{ExecuteRetryPolicy: &dexpb.RetryPolicy{
			InitialIntervalSeconds: 1,
			MaximumIntervalSeconds: 1,
			BackoffCoefficient:     1,
			MaximumAttempts:        3,
			TotalDurationSeconds:   20,
		}},
	})
	require.NoError(t, err)
	result, err := runtime.FlowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{
		FlowId:          flowID,
		WaitTimeSeconds: 25,
	})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, result.GetFlowStatus())
	require.Equal(t, int32(3), worker.executeCalls.Load())
	require.Equal(t, "execute-checkpoint", <-worker.restoredHeartbeats)
	require.Empty(t, <-worker.restoredHeartbeats)

	progressMessages := readFlowStreamMessages(t, ctx, runtime.FlowClient, flowID, "progress", 5)
	require.Equal(t, []string{
		"wait-1",
		"wait-2",
		"execute-1",
		"execute-2",
		"execute-3",
	}, protoStreamValues(progressMessages))
	require.Equal(t, []string{
		"#stream-step-1",
		"#stream-step-1",
		"#stream-step-1",
		"#stream-step-1",
		"#stream-step-1",
	}, protoStreamSources(progressMessages))
	otherMessages := readFlowStreamMessages(t, ctx, runtime.FlowClient, flowID, "other", 2)
	require.Equal(t, []string{"wait-other", "execute-other"}, protoStreamValues(otherMessages))
}

func readFlowStreamMessages(
	t *testing.T,
	ctx context.Context,
	flowClient dexpb.FlowServiceClient,
	flowID string,
	streamName string,
	count int,
) []*dexpb.StreamMessage {
	t.Helper()
	messages := make([]*dexpb.StreamMessage, 0, count)
	resumeToken := ""
	for len(messages) < count {
		response, err := flowClient.ReadStream(ctx, &dexpb.ReadStreamRequest{
			FlowId:          flowID,
			FlowType:        "step-streaming-flow",
			StreamName:      streamName,
			ResumeToken:     resumeToken,
			WaitTimeSeconds: 2,
		})
		require.NoError(t, err)
		messages = append(messages, response.GetMessage())
		resumeToken = response.GetMessage().GetResumeToken()
	}
	return messages
}

func protoStreamValues(messages []*dexpb.StreamMessage) []string {
	values := make([]string, len(messages))
	for index, message := range messages {
		values[index] = message.GetValue().GetStringValue()
	}
	return values
}

func protoStreamSources(messages []*dexpb.StreamMessage) []string {
	sources := make([]string, len(messages))
	for index, message := range messages {
		sources[index] = message.GetSource()
	}
	return sources
}

func TestStreamAPITemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	streamName := "api-" + newRequestID()
	redisClient := newStreamRedisClient(t)
	t.Cleanup(func() { deleteStreamTestKeys(t, redisClient, streamName) })
	runtime := startDexService(t, DexServiceTestConfig{
		BackendType: service.BackendTypeTemporal,
		StreamStore: config.StreamStoreConfig{
			Backend:         config.StreamStoreBackendRedis,
			RedisURL:        streamTestRedisURL,
			MaxMessageBytes: 64,
			MaxReadMessages: 2,
		},
	})
	flowClient := runtime.FlowClient
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	flowID := "nonexistent-" + newRequestID()
	_, err := flowClient.WriteStream(ctx, &dexpb.WriteStreamRequest{
		FlowId:              flowID,
		FlowType:            streamTestFlowType,
		StreamName:          streamName,
		StreamCapacityBytes: 1 << 20,
		Value:               stringValue("missing-key"),
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = flowClient.WriteStream(ctx, &dexpb.WriteStreamRequest{
		FlowId:              flowID,
		FlowType:            streamTestFlowType,
		StreamName:          streamName,
		StreamCapacityBytes: 1 << 20,
		Value:               stringValue("first"),
		Source:              "client-key",
	})
	require.NoError(t, err)
	response, err := flowClient.ReadStream(ctx, &dexpb.ReadStreamRequest{
		FlowId:          flowID,
		FlowType:        streamTestFlowType,
		StreamName:      streamName,
		WaitTimeSeconds: 2,
	})
	require.NoError(t, err)
	require.Equal(t, "first", response.GetMessage().GetValue().GetStringValue())
	require.Equal(t, "client-key", response.GetMessage().GetSource())
	require.NotEmpty(t, response.GetMessage().GetResumeToken())
	require.WithinDuration(t, time.Now(), response.GetMessage().GetCreatedTime().AsTime(), 5*time.Second)
	listed, err := flowClient.ListStreamMessages(ctx, &dexpb.ListStreamMessagesRequest{
		FlowId:     flowID,
		FlowType:   streamTestFlowType,
		StreamName: streamName,
		PageSize:   2,
	})
	require.NoError(t, err)
	require.Equal(t, []string{"first"}, protoStreamValues(listed.GetMessages()))
	require.Empty(t, listed.GetNextPageToken())
	_, err = flowClient.ListStreamMessages(ctx, &dexpb.ListStreamMessagesRequest{
		FlowId:     flowID,
		FlowType:   streamTestFlowType,
		StreamName: streamName,
		PageSize:   3,
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	chargedBytes, err := redisClient.Get(context.Background(), streamTestBaseKey(streamName)+":charged").Int64()
	require.NoError(t, err)
	_, err = flowClient.WriteStream(ctx, &dexpb.WriteStreamRequest{
		FlowId:              flowID,
		FlowType:            streamTestFlowType,
		StreamName:          streamName,
		StreamCapacityBytes: chargedBytes*2 - 1,
		Value:               stringValue("first"),
		Source:              "second-key",
	})
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Equal(t, int64(1), streamLength(t, redisClient, streamName))
	_, err = flowClient.WriteStream(ctx, &dexpb.WriteStreamRequest{
		FlowId:              flowID,
		FlowType:            streamTestFlowType,
		StreamName:          streamName,
		StreamCapacityBytes: 1 << 20,
		Value:               stringValue(string(make([]byte, 64))),
		Source:              "message-too-large",
	})
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Equal(t, int64(1), streamLength(t, redisClient, streamName))
	_, err = flowClient.WriteStream(ctx, &dexpb.WriteStreamRequest{
		FlowId:              flowID,
		FlowType:            streamTestFlowType,
		StreamName:          streamName,
		StreamCapacityBytes: 1,
		Value:               stringValue("too-large"),
		Source:              "too-large",
	})
	require.Equal(t, codes.ResourceExhausted, status.Code(err))

	_, err = flowClient.ReadStream(ctx, &dexpb.ReadStreamRequest{
		FlowId:          flowID,
		FlowType:        streamTestFlowType,
		StreamName:      streamName,
		ResumeToken:     "malformed",
		WaitTimeSeconds: 1,
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	var readFinished atomic.Bool
	readResult := make(chan *dexpb.ReadStreamResponse, 1)
	readErrors := make(chan error, 1)
	go func() {
		blockingResponse, readErr := flowClient.ReadStream(ctx, &dexpb.ReadStreamRequest{
			FlowId:          flowID,
			FlowType:        streamTestFlowType,
			StreamName:      streamName,
			ResumeToken:     response.GetMessage().GetResumeToken(),
			WaitTimeSeconds: 5,
		})
		if readErr != nil {
			readErrors <- readErr
		} else {
			readResult <- blockingResponse
		}
		readFinished.Store(true)
	}()
	_, err = flowClient.WriteStream(ctx, &dexpb.WriteStreamRequest{
		FlowId:              "different-flow",
		FlowType:            streamTestFlowType,
		StreamName:          streamName,
		StreamCapacityBytes: 1 << 20,
		Value:               stringValue("other"),
		Source:              "other-key",
	})
	require.NoError(t, err)
	require.Never(t, readFinished.Load, 200*time.Millisecond, 10*time.Millisecond)
	_, err = flowClient.WriteStream(ctx, &dexpb.WriteStreamRequest{
		FlowId:              flowID,
		FlowType:            streamTestFlowType,
		StreamName:          streamName,
		StreamCapacityBytes: 1 << 20,
		Value:               stringValue("second"),
		Source:              "run-id#step-id",
	})
	require.NoError(t, err)
	latestToken := ""
	select {
	case readErr := <-readErrors:
		require.NoError(t, readErr)
	case blockingResponse := <-readResult:
		require.Equal(t, "second", blockingResponse.GetMessage().GetValue().GetStringValue())
		require.Equal(t, "run-id#step-id", blockingResponse.GetMessage().GetSource())
		latestToken = blockingResponse.GetMessage().GetResumeToken()
	case <-ctx.Done():
		require.FailNow(t, "blocking ReadStream did not finish", ctx.Err())
	}
	_, err = flowClient.ReadStream(ctx, &dexpb.ReadStreamRequest{
		FlowId:          flowID,
		FlowType:        streamTestFlowType,
		StreamName:      streamName,
		ResumeToken:     latestToken,
		WaitTimeSeconds: 1,
	})
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	require.Equal(
		t,
		dexpb.ErrorSubStatus_ERROR_SUB_STATUS_LONG_POLL_TIME_OUT,
		grpcServiceErrorResponse(t, err).GetSubStatus(),
	)
}

func TestStreamFailureIsolationTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	runtime := startDexService(t, DexServiceTestConfig{
		BackendType: service.BackendTypeTemporal,
		StreamStore: config.StreamStoreConfig{
			Backend:  config.StreamStoreBackendRedis,
			RedisURL: "redis://127.0.0.1:1/0",
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := runtime.FlowClient.WriteStream(ctx, &dexpb.WriteStreamRequest{
		FlowId:              "flow",
		FlowType:            streamTestFlowType,
		StreamName:          "unavailable",
		StreamCapacityBytes: 1024,
		Value:               stringValue("value"),
		Source:              "key",
	})
	require.Equal(t, codes.Unavailable, status.Code(err))
	health, err := runtime.FlowClient.HealthCheck(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	require.Equal(t, "OK", health.GetCondition())
}

func TestStreamDisabledTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	runtime := startDexService(t, DexServiceTestConfig{BackendType: service.BackendTypeTemporal})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := runtime.FlowClient.WriteStream(ctx, &dexpb.WriteStreamRequest{
		FlowId:              "flow",
		FlowType:            streamTestFlowType,
		StreamName:          "disabled",
		StreamCapacityBytes: 1024,
		Value:               stringValue("value"),
		Source:              "key",
	})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func BenchmarkStreamStoreWrite(b *testing.B) {
	for _, payloadBytes := range []int{128, 4096, 65536} {
		b.Run(strconv.Itoa(payloadBytes), func(b *testing.B) {
			store, err := streamstore.New(&config.StreamStoreConfig{
				Backend:            config.StreamStoreBackendRedis,
				RedisURL:           streamTestRedisURL,
				TrimWorkers:        2,
				TrimTriggerPercent: 90,
				TrimTargetPercent:  80,
			}, log.NewNoop())
			require.NoError(b, err)
			defer func() { require.NoError(b, store.Close()) }()
			streamName := "benchmark-" + newRequestID()
			redisClient := newStreamRedisClient(b)
			defer deleteStreamTestKeys(b, redisClient, streamName)
			payload := string(make([]byte, payloadBytes))
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				input := streamInput("flow-"+strconv.Itoa(index%100), streamName, index, payload)
				input.StreamCapacityBytes = 1 << 40
				require.NoError(b, store.Write(context.Background(), input))
			}
		})
	}
}

func newStreamTestStore(t testing.TB) (*streamstore.Store, *redis.Client) {
	t.Helper()
	return newStreamTestStoreWithConfig(t, config.StreamStoreConfig{
		Backend:                       config.StreamStoreBackendRedis,
		RedisURL:                      streamTestRedisURL,
		EstimatedMessageOverheadBytes: 1,
		TrimTriggerPercent:            90,
		TrimTargetPercent:             80,
		BackgroundTrimBatchSize:       1,
		TrimLeaseTTL:                  2 * time.Second,
		TrimLeaseRetry:                5 * time.Millisecond,
		TrimBatchYieldTime:            100 * time.Microsecond,
		TrimWorkers:                   2,
	})
}

func newMemoryStreamTestStore(t testing.TB) *streamstore.Store {
	t.Helper()
	store, err := streamstore.New(&config.StreamStoreConfig{
		Backend:                       config.StreamStoreBackendMemory,
		EstimatedMessageOverheadBytes: 1,
		TrimTriggerPercent:            90,
		TrimTargetPercent:             80,
		BackgroundTrimBatchSize:       256,
		TrimBatchYieldTime:            100 * time.Microsecond,
		TrimWorkers:                   4,
	}, log.NewNoop())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

func newStreamTestStoreWithConfig(
	t testing.TB,
	streamStoreConfig config.StreamStoreConfig,
) (*streamstore.Store, *redis.Client) {
	t.Helper()
	store, err := streamstore.New(&streamStoreConfig, log.NewNoop())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store, newStreamRedisClient(t)
}

func newStreamRedisClient(t testing.TB) *redis.Client {
	t.Helper()
	options, err := redis.ParseURL(streamTestRedisURL)
	require.NoError(t, err)
	client := redis.NewClient(options)
	require.NoError(t, client.Ping(context.Background()).Err())
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	return client
}

func streamInput(flowID string, streamName string, index int, value string) streamstore.WriteInput {
	return streamstore.WriteInput{
		FlowID:     flowID,
		FlowType:   streamTestFlowType,
		StreamName: streamName,
		Value:      &dexpb.Value{Kind: &dexpb.Value_StringValue{StringValue: value}},
		Source:     fmt.Sprintf("public-%02d", index),
	}
}

func estimatedCharge(input streamstore.WriteInput, overhead int64) int64 {
	return int64(proto.Size(input.Value)+len(input.FlowID)+len(input.Source)) + overhead
}

func readAvailableMessages(
	t testing.TB,
	store *streamstore.Store,
	flowID string,
	streamName string,
) []*streamstore.Message {
	t.Helper()
	messages, err := availableStreamMessages(store, flowID, streamName)
	require.NoError(t, err)
	return messages
}

func availableStreamMessages(
	store *streamstore.Store,
	flowID string,
	streamName string,
) ([]*streamstore.Message, error) {
	var messages []*streamstore.Message
	resumeToken := ""
	for {
		readCtx, cancelRead := context.WithTimeout(context.Background(), 30*time.Millisecond)
		message, err := store.Read(readCtx, streamTestFlowType, flowID, streamName, resumeToken)
		cancelRead()
		if err != nil {
			if errors.Is(err, streamstore.ErrWaitTimeout) {
				return messages, nil
			}
			return nil, err
		}
		messages = append(messages, message)
		resumeToken, err = streamstore.EncodeResumeToken(
			streamTestFlowType,
			flowID,
			streamName,
			message.MessageID,
		)
		if err != nil {
			return nil, err
		}
	}
}

func waitForStreamMessageLimit(
	t testing.TB,
	store *streamstore.Store,
	flowID string,
	streamName string,
	maximumCount int,
) []*streamstore.Message {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	var messages []*streamstore.Message
	var readErr error
	for {
		messages, readErr = availableStreamMessages(store, flowID, streamName)
		if readErr == nil && len(messages) <= maximumCount {
			return messages
		}
		select {
		case <-deadline.C:
			require.NoError(t, readErr)
			require.LessOrEqual(t, len(messages), maximumCount)
			return nil
		case <-ticker.C:
		}
	}
}

func readOneMessage(
	t testing.TB,
	store *streamstore.Store,
	flowID string,
	streamName string,
	resumeToken string,
) *streamstore.Message {
	t.Helper()
	readCtx, cancelRead := context.WithTimeout(context.Background(), time.Second)
	defer cancelRead()
	message, err := store.Read(readCtx, streamTestFlowType, flowID, streamName, resumeToken)
	require.NoError(t, err)
	return message
}

func messageKeys(messages []*streamstore.Message) []string {
	keys := make([]string, len(messages))
	for index, message := range messages {
		keys[index] = message.Source
	}
	return keys
}

func streamLength(t testing.TB, client *redis.Client, streamName string) int64 {
	t.Helper()
	return streamLengthForFlowType(t, client, streamTestFlowType, streamName)
}

func streamLengthForFlowType(
	t testing.TB,
	client *redis.Client,
	flowType string,
	streamName string,
) int64 {
	t.Helper()
	length, err := client.XLen(
		context.Background(),
		streamTestBaseKeyForFlowType(flowType, streamName)+":fifo",
	).Result()
	require.NoError(t, err)
	return length
}

func requireStreamAccountingConsistent(t testing.TB, client *redis.Client, streamName string) {
	t.Helper()
	baseKey := streamTestBaseKey(streamName)
	entries, err := client.XRange(context.Background(), baseKey+":fifo", "-", "+").Result()
	require.NoError(t, err)
	var expectedBytes int64
	for _, entry := range entries {
		chargedValue, ok := entry.Values["c"]
		require.True(t, ok)
		chargedBytes, parseErr := strconv.ParseInt(fmt.Sprint(chargedValue), 10, 64)
		require.NoError(t, parseErr)
		expectedBytes += chargedBytes
	}
	actualBytes, err := client.Get(context.Background(), baseKey+":charged").Int64()
	require.NoError(t, err)
	require.Equal(t, expectedBytes, actualBytes)
}

func deleteStreamTestKeys(t testing.TB, client *redis.Client, streamName string) {
	t.Helper()
	deleteStreamTestKeysForFlowType(t, client, streamTestFlowType, streamName)
}

func deleteStreamTestKeysForFlowType(
	t testing.TB,
	client *redis.Client,
	flowType string,
	streamName string,
) {
	t.Helper()
	keys, err := client.Keys(
		context.Background(),
		streamTestBaseKeyForFlowType(flowType, streamName)+"*",
	).Result()
	require.NoError(t, err)
	if len(keys) > 0 {
		require.NoError(t, client.Del(context.Background(), keys...).Err())
	}
}

func streamTestBaseKey(streamName string) string {
	return streamTestBaseKeyForFlowType(streamTestFlowType, streamName)
}

func streamTestBaseKeyForFlowType(flowType string, streamName string) string {
	streamScope := fmt.Sprintf("%d:%s%d:%s", len(flowType), flowType, len(streamName), streamName)
	return fmt.Sprintf("dex:stream:v1:%x", sha256.Sum256([]byte(streamScope)))
}
