// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package attributestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/redis/go-redis/v9"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service/common/log"
	"github.com/superdurable/dex/service/common/log/tag"
)

type redisStore struct {
	client    *redis.Client
	keyPrefix string
	logger    log.Logger
	dsn       string
}

func openRedisStore(
	ctx context.Context,
	cfg config.AttributeStoreConfigEntry,
	logger log.Logger,
) (*redisStore, error) {
	options, err := redis.ParseURL(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse Redis URL: %w", err)
	}
	client := redis.NewClient(options)
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, closeRedisOnError(client, fmt.Errorf("ping Redis: %w", err))
	}
	return &redisStore{
		client:    client,
		keyPrefix: cfg.EffectiveKeyPrefix(),
		logger:    logger,
		dsn:       cfg.DSN,
	}, nil
}

func closeRedisOnError(client *redis.Client, sourceErr error) error {
	if closeErr := client.Close(); closeErr != nil {
		return errors.Join(sourceErr, closeErr)
	}
	return sourceErr
}

func (s *redisStore) writeBatch(
	ctx context.Context,
	flowID string,
	items []*dexpb.AttributeSyncItem,
) error {
	latest := make(map[string]*dexpb.Value, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		latest[item.GetKey()] = item.GetValue()
	}
	fields := make(map[string]any, len(latest))
	for name, value := range latest {
		if name == "" {
			s.logger.Error("skip Attribute Store item: empty Redis field")
			continue
		}
		converted, err := convertRedisValue(value)
		if err != nil {
			s.logger.Error("skip incompatible Attribute Store item", tag.AttributeName(name), tag.Error(err))
			continue
		}
		fields[name] = converted
	}
	if len(fields) == 0 {
		return nil
	}
	if err := s.client.HSet(ctx, s.keyPrefix+flowID, fields).Err(); err != nil {
		return redactStorageSecret(fmt.Errorf("execute Attribute Store upsert: %w", err), s.dsn)
	}
	return nil
}

// convertRedisValue returns the JSON encoding of value, or the raw payload of a non-JSON encoded object.
func convertRedisValue(value *dexpb.Value) ([]byte, error) {
	if value == nil || value.GetKind() == nil {
		return nil, fmt.Errorf("value is missing")
	}
	switch kind := value.GetKind().(type) {
	case *dexpb.Value_NullValue:
		return []byte("null"), nil
	case *dexpb.Value_StringValue:
		return json.Marshal(kind.StringValue)
	case *dexpb.Value_IntValue:
		return json.Marshal(kind.IntValue)
	case *dexpb.Value_DoubleValue:
		if math.IsNaN(kind.DoubleValue) || math.IsInf(kind.DoubleValue, 0) {
			return nil, fmt.Errorf("JSON cannot encode non-finite double value")
		}
		return json.Marshal(kind.DoubleValue)
	case *dexpb.Value_BoolValue:
		return json.Marshal(kind.BoolValue)
	case *dexpb.Value_ObjValue:
		object := kind.ObjValue
		if object == nil {
			return nil, fmt.Errorf("object is missing")
		}
		if object.GetEncoding() == "json" && !json.Valid(object.GetPayload()) {
			return nil, fmt.Errorf("JSON payload is invalid")
		}
		return object.GetPayload(), nil
	case *dexpb.Value_InternalBlobIdForStringValue, *dexpb.Value_InternalBlobIdForObjValue:
		return nil, fmt.Errorf("blob-backed value was not hydrated")
	default:
		return nil, fmt.Errorf("unsupported Attribute value")
	}
}

func (s *redisStore) close() error {
	return s.client.Close()
}
