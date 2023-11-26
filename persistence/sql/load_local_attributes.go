// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: BUSL-1.1

package sql

import (
	"context"
	"fmt"

	"github.com/xcherryio/apis/goapi/xcapi"
	"github.com/xcherryio/xcherry/common/ptr"
	"github.com/xcherryio/xcherry/extensions"
	"github.com/xcherryio/xcherry/persistence/data_models"
)

func (p sqlProcessStoreImpl) LoadLocalAttributes(ctx context.Context, request data_models.LoadLocalAttributesRequest) (*data_models.LoadLocalAttributesResponse, error) {
	if len(request.Request.KeysToLoadWithLock) != 0 &&
		request.Request.LockingPolicy != ptr.Any(xcapi.NO_LOCKING) {
		return nil, fmt.Errorf("locking policy %v is not supported", request.Request.LockingPolicy)
	}

	var noLockRows []extensions.LocalAttributeRow
	var err error
	if len(request.Request.KeysToLoadNoLock) > 0 {
		noLockRows, err = p.session.SelectLocalAttributes(
			ctx, request.ProcessExecutionId, request.Request.KeysToLoadNoLock)
		if err != nil {
			return nil, err
		}
	}

	var lockRows []extensions.LocalAttributeRow
	if len(request.Request.KeysToLoadWithLock) > 0 {
		lockRows, err = p.session.SelectLocalAttributes(
			ctx, request.ProcessExecutionId, request.Request.KeysToLoadWithLock)
		if err != nil {
			return nil, err
		}
	}

	var attributes []xcapi.KeyValue
	for _, row := range noLockRows {
		value, err := data_models.BytesToEncodedObject(row.Value)
		if err != nil {
			return nil, err
		}
		attributes = append(attributes, xcapi.KeyValue{
			Key:   row.Key,
			Value: value,
		})
	}
	for _, row := range lockRows {
		value, err := data_models.BytesToEncodedObject(row.Value)
		if err != nil {
			return nil, err
		}
		attributes = append(attributes, xcapi.KeyValue{
			Key:   row.Key,
			Value: value,
		})
	}

	return &data_models.LoadLocalAttributesResponse{
		Response: xcapi.LoadLocalAttributesResponse{
			Attributes: attributes,
		},
	}, nil
}
