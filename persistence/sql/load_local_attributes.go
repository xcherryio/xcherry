// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: BUSL-1.1

package sql

import (
	"context"
	"fmt"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/common/ptr"
	"github.com/xdblab/xdb/persistence/data_models"
)

func (p sqlProcessStoreImpl) LoadLocalAttributes(
	ctx context.Context,
	request data_models.LoadLocalAttributesRequest,
) (*data_models.LoadLocalAttributesResponse, error) {
	if len(request.Request.KeysToLoadWithLock) != 0 &&
		request.Request.LockingPolicy != ptr.Any(xdbapi.NO_LOCKING) {
		return nil, fmt.Errorf("locking policy %v is not supported", request.Request.LockingPolicy)
	}

	noLockRows, err := p.session.SelectLocalAttributes(
		ctx, request.ProcessExecutionId, request.Request.KeysToLoadNoLock)
	if err != nil {
		return nil, err
	}

	lockRows, err := p.session.SelectLocalAttributes(
		ctx, request.ProcessExecutionId, request.Request.KeysToLoadWithLock)
	if err != nil {
		return nil, err
	}

	var attributes []xdbapi.KeyValue
	for _, row := range noLockRows {
		value, err := data_models.BytesToEncodedObject(row.Value)
		if err != nil {
			return nil, err
		}
		attributes = append(attributes, xdbapi.KeyValue{
			Key:   row.Key,
			Value: value,
		})
	}
	for _, row := range lockRows {
		value, err := data_models.BytesToEncodedObject(row.Value)
		if err != nil {
			return nil, err
		}
		attributes = append(attributes, xdbapi.KeyValue{
			Key:   row.Key,
			Value: value,
		})
	}

	return &data_models.LoadLocalAttributesResponse{
		Response: xdbapi.LoadLocalAttributesResponse{
			Attributes: attributes,
		},
	}, nil
}
