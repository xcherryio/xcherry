// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package streamstore

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service/common/log"
	"google.golang.org/protobuf/proto"
)

var (
	ErrDisabled           = errors.New("Stream Store is disabled")
	ErrMessageTooLarge    = errors.New("serialized Stream Value exceeds configured maxMessageBytes")
	ErrCapacityExceeded   = errors.New("Stream capacity is exhausted; retry after background trimming")
	ErrWaitTimeout        = errors.New("Stream read timed out")
	ErrInvalidResumeToken = errors.New("invalid Stream resume token")
	ErrInvalidPageSize    = errors.New("invalid Stream page size")
	ErrUnavailable        = errors.New("Stream Store backend is unavailable")
)

const resumeTokenVersion = 1

type WriteInput struct {
	FlowID              string
	FlowType            string
	StreamName          string
	StreamCapacityBytes int64
	Value               *dexpb.Value
	Source              string
}

type Message struct {
	Value       *dexpb.Value
	MessageID   string
	CreatedTime time.Time
	Source      string
}

type Store struct {
	cfg     *config.StreamStoreConfig
	backend backend
}

type backend interface {
	Close() error
	Write(context.Context, backendWriteInput) error
	Read(context.Context, string, string, string, string) (*Message, error)
	List(context.Context, string, string, string, string, int32) ([]*Message, bool, error)
}

type backendWriteInput struct {
	input                  WriteInput
	payload                []byte
	chargedBytes           int64
	capacityBytes          int64
	trimTriggerBytes       int64
	baseTrimTargetBytes    int64
	messageTrimTargetBytes int64
}

type redisBackend struct {
	client      *redis.Client
	coordinator *trimCoordinator
}

type resumeToken struct {
	Version    int    `json:"v"`
	FlowID     string `json:"f"`
	FlowType   string `json:"t"`
	StreamName string `json:"s"`
	MessageID  string `json:"i"`
}

// New builds the Stream Store.
func New(cfg *config.StreamStoreConfig, logger log.Logger) (*Store, error) {
	if cfg == nil {
		panic("Stream Store config must not be nil")
	}
	if logger == nil {
		panic("Stream Store logger must not be nil")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	store := &Store{cfg: cfg}
	switch cfg.EffectiveBackend() {
	case config.StreamStoreBackendDisabled:
		return store, nil
	case config.StreamStoreBackendMemory:
		store.backend = newMemoryBackend(cfg)
	case config.StreamStoreBackendRedis:
		options, err := redis.ParseURL(cfg.RedisURL)
		if err != nil {
			return nil, fmt.Errorf("parse Stream Store Redis URL: %w", err)
		}
		client := redis.NewClient(options)
		store.backend = &redisBackend{
			client:      client,
			coordinator: newTrimCoordinator(cfg, client, logger),
		}
	default:
		panic("validated Stream Store backend was not handled")
	}
	return store, nil
}

func (s *Store) Close() error {
	if s.backend == nil {
		return nil
	}
	return s.backend.Close()
}

func (s *Store) Write(ctx context.Context, input WriteInput) error {
	if s.backend == nil {
		return ErrDisabled
	}
	payload, err := proto.Marshal(input.Value)
	if err != nil {
		return fmt.Errorf("marshal Stream Value: %w", err)
	}
	if int64(len(payload)) > s.cfg.EffectiveMaxMessageBytes() {
		return ErrMessageTooLarge
	}
	chargedBytes, err := s.chargedBytes(input, len(payload))
	if err != nil {
		return err
	}
	trimTrigger := percentageOf(input.StreamCapacityBytes, s.cfg.EffectiveTrimTriggerPercent())
	baseTrimTarget := percentageOf(input.StreamCapacityBytes, s.cfg.EffectiveTrimTargetPercent())
	messageTrimTarget := baseTrimTarget
	if messageTrimTarget < chargedBytes {
		messageTrimTarget = chargedBytes
	}
	return s.backend.Write(ctx, backendWriteInput{
		input:                  input,
		payload:                payload,
		chargedBytes:           chargedBytes,
		capacityBytes:          input.StreamCapacityBytes,
		trimTriggerBytes:       trimTrigger,
		baseTrimTargetBytes:    baseTrimTarget,
		messageTrimTargetBytes: messageTrimTarget,
	})
}

func (s *Store) Read(
	ctx context.Context,
	flowType string,
	flowID string,
	streamName string,
	encodedResumeToken string,
) (*Message, error) {
	if s.backend == nil {
		return nil, ErrDisabled
	}
	messageID, err := decodeResumeToken(encodedResumeToken, flowType, flowID, streamName)
	if err != nil {
		return nil, err
	}
	deadline, hasDeadline := ctx.Deadline()
	if !hasDeadline {
		panic("Stream read context requires a deadline")
	}
	if time.Until(deadline) <= 0 {
		return nil, ErrWaitTimeout
	}
	return s.backend.Read(ctx, flowType, flowID, streamName, messageID)
}

func (s *Store) List(
	ctx context.Context,
	flowType string,
	flowID string,
	streamName string,
	pageSize int32,
	encodedBeforePageToken string,
) ([]*Message, bool, error) {
	if s.backend == nil {
		return nil, false, ErrDisabled
	}
	if pageSize < 1 || pageSize > s.cfg.EffectiveMaxListMessagesPageSize() {
		return nil, false, ErrInvalidPageSize
	}
	beforeMessageID, err := decodePageToken(
		encodedBeforePageToken,
		flowType,
		flowID,
		streamName,
	)
	if err != nil {
		return nil, false, err
	}
	return s.backend.List(ctx, flowType, flowID, streamName, beforeMessageID, pageSize)
}

func (b *redisBackend) Close() error {
	b.coordinator.Close()
	return b.client.Close()
}

func (b *redisBackend) Write(ctx context.Context, input backendWriteInput) error {
	scriptOutput, err := runWriteScript(ctx, b.client, writeScriptInput{
		keys:                   streamKeys(newStreamScope(input.input.FlowType, input.input.StreamName), input.input.FlowID),
		source:                 input.input.Source,
		payload:                input.payload,
		chargedBytes:           input.chargedBytes,
		capacityBytes:          input.capacityBytes,
		trimTriggerBytes:       input.trimTriggerBytes,
		baseTrimTargetBytes:    input.baseTrimTargetBytes,
		messageTrimTargetBytes: input.messageTrimTargetBytes,
	})
	if err != nil {
		return err
	}
	if scriptOutput.needsTrim {
		b.coordinator.Schedule(
			newStreamScope(input.input.FlowType, input.input.StreamName),
			scriptOutput.trimTargetBytes,
		)
	}
	switch scriptOutput.status {
	case writeScriptStatusSucceeded:
		return nil
	case writeScriptStatusCapacityExceeded:
		return ErrCapacityExceeded
	default:
		panic("validated Stream write status was not handled")
	}
}

func (b *redisBackend) Read(
	ctx context.Context,
	flowType string,
	flowID string,
	streamName string,
	messageID string,
) (*Message, error) {
	deadline, hasDeadline := ctx.Deadline()
	if !hasDeadline {
		panic("Stream read context requires a deadline")
	}
	result, err := b.client.XRead(ctx, &redis.XReadArgs{
		Streams: []string{streamKeys(newStreamScope(flowType, streamName), flowID).instance, messageID},
		Count:   1,
		Block:   time.Until(deadline),
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) || errors.Is(err, context.DeadlineExceeded) {
			return nil, ErrWaitTimeout
		}
		if errors.Is(err, context.Canceled) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: read message: %v", ErrUnavailable, err)
	}
	if len(result) != 1 || len(result[0].Messages) != 1 {
		return nil, fmt.Errorf("unexpected Redis XREAD result")
	}
	redisMessage := result[0].Messages[0]
	return messageFromRedis(redisMessage)
}

func (b *redisBackend) List(
	ctx context.Context,
	flowType string,
	flowID string,
	streamName string,
	beforeMessageID string,
	pageSize int32,
) ([]*Message, bool, error) {
	start := "+"
	if beforeMessageID != "" {
		start = "(" + beforeMessageID
	}
	redisMessages, err := b.client.XRevRangeN(
		ctx,
		streamKeys(newStreamScope(flowType, streamName), flowID).instance,
		start,
		"-",
		int64(pageSize)+1,
	).Result()
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, false, err
		}
		return nil, false, fmt.Errorf("%w: list messages: %v", ErrUnavailable, err)
	}
	hasMore := len(redisMessages) > int(pageSize)
	if hasMore {
		redisMessages = redisMessages[:pageSize]
	}
	messages := make([]*Message, 0, len(redisMessages))
	for _, redisMessage := range redisMessages {
		message, messageErr := messageFromRedis(redisMessage)
		if messageErr != nil {
			return nil, false, messageErr
		}
		messages = append(messages, message)
	}
	return messages, hasMore, nil
}

func messageFromRedis(redisMessage redis.XMessage) (*Message, error) {
	payload, err := redisFieldBytes(redisMessage.Values, "v")
	if err != nil {
		return nil, err
	}
	source, err := redisFieldString(redisMessage.Values, "s")
	if err != nil {
		return nil, err
	}
	value := &dexpb.Value{}
	if err := proto.Unmarshal(payload, value); err != nil {
		return nil, fmt.Errorf("unmarshal retained Stream Value: %w", err)
	}
	createdTime, err := createdTimeFromMessageID(redisMessage.ID)
	if err != nil {
		return nil, err
	}
	return &Message{
		Value:       value,
		MessageID:   redisMessage.ID,
		CreatedTime: createdTime,
		Source:      source,
	}, nil
}

func EncodeResumeToken(flowType string, flowID string, streamName string, messageID string) (string, error) {
	if _, _, err := parseMessageID(messageID); err != nil {
		return "", err
	}
	payload, err := json.Marshal(resumeToken{
		Version:    resumeTokenVersion,
		FlowID:     flowID,
		FlowType:   flowType,
		StreamName: streamName,
		MessageID:  messageID,
	})
	if err != nil {
		return "", fmt.Errorf("marshal Stream resume token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func (s *Store) chargedBytes(input WriteInput, payloadBytes int) (int64, error) {
	parts := []int64{
		int64(payloadBytes),
		int64(len(input.FlowID)),
		int64(len(input.Source)),
		s.cfg.EffectiveEstimatedMessageOverheadBytes(),
	}
	var total int64
	for _, part := range parts {
		if part > math.MaxInt64-total {
			return 0, ErrMessageTooLarge
		}
		total += part
	}
	return total, nil
}

func percentageOf(capacity int64, percent int32) int64 {
	percentage := int64(percent)
	return (capacity/100)*percentage + (capacity%100)*percentage/100
}

func decodeResumeToken(encoded string, flowType string, flowID string, streamName string) (string, error) {
	if encoded == "" {
		return "0-0", nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return "", ErrInvalidResumeToken
	}
	var token resumeToken
	if err := json.Unmarshal(payload, &token); err != nil {
		return "", ErrInvalidResumeToken
	}
	if token.Version != resumeTokenVersion ||
		token.FlowID != flowID ||
		token.FlowType != flowType ||
		token.StreamName != streamName {
		return "", ErrInvalidResumeToken
	}
	if _, _, err := parseMessageID(token.MessageID); err != nil {
		return "", ErrInvalidResumeToken
	}
	return token.MessageID, nil
}

func decodePageToken(encoded string, flowType string, flowID string, streamName string) (string, error) {
	if encoded == "" {
		return "", nil
	}
	return decodeResumeToken(encoded, flowType, flowID, streamName)
}

func createdTimeFromMessageID(messageID string) (time.Time, error) {
	milliseconds, _, err := parseMessageID(messageID)
	if err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(milliseconds), nil
}

func parseMessageID(messageID string) (int64, uint64, error) {
	parts := strings.Split(messageID, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid Stream message ID")
	}
	milliseconds, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || milliseconds < 0 {
		return 0, 0, fmt.Errorf("invalid Stream message ID")
	}
	sequence, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid Stream message ID")
	}
	return milliseconds, sequence, nil
}

type redisKeys struct {
	fifo         string
	chargedBytes string
	instance     string
	lease        string
}

// streamKeys returns the Redis keys for a Stream scope and Flow.
func streamKeys(scope streamScope, flowID string) redisKeys {
	scopeText := fmt.Sprintf(
		"%d:%s%d:%s", len(scope.flowType), scope.flowType, len(scope.streamName), scope.streamName,
	)
	streamDigest := fmt.Sprintf("%x", sha256.Sum256([]byte(scopeText)))
	base := "dex:stream:v1:" + streamDigest
	flowDigest := fmt.Sprintf("%x", sha256.Sum256([]byte(flowID)))
	return redisKeys{
		fifo:         base + ":fifo",
		chargedBytes: base + ":charged",
		instance:     base + ":instance:" + flowDigest,
		lease:        base + ":trim-lease",
	}
}

func redisFieldBytes(values map[string]any, field string) ([]byte, error) {
	value, ok := values[field]
	if !ok {
		return nil, fmt.Errorf("retained Stream message missing %s", field)
	}
	switch typed := value.(type) {
	case string:
		return []byte(typed), nil
	case []byte:
		return typed, nil
	default:
		return nil, fmt.Errorf("retained Stream field %s has type %T", field, value)
	}
}

func redisFieldString(values map[string]any, field string) (string, error) {
	value, err := redisFieldBytes(values, field)
	if err != nil {
		return "", err
	}
	return string(value), nil
}
