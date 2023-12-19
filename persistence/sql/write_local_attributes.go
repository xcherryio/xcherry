// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: Apache-2.0

package sql

import (
	"context"

	"github.com/xcherryio/apis/goapi/xcapi"
	"github.com/xcherryio/xcherry/common/log/tag"
	"github.com/xcherryio/xcherry/common/uuid"
	"github.com/xcherryio/xcherry/extensions"
	"github.com/xcherryio/xcherry/persistence/data_models"
)

func (p sqlProcessStoreImpl) handleInitialLocalAttributesWrite(
	ctx context.Context,
	tx extensions.SQLTransaction,
	req xcapi.ProcessExecutionStartRequest,
	resp data_models.StartProcessResponse,
) error {
	if req.ProcessStartConfig == nil || req.ProcessStartConfig.LocalAttributeConfig == nil ||
		len(req.ProcessStartConfig.LocalAttributeConfig.InitialWrite) == 0 {
		return nil
	}

	attributes := req.ProcessStartConfig.LocalAttributeConfig.InitialWrite
	for i := range attributes {
		valueBytes, err := data_models.FromEncodedObjectIntoBytes(&attributes[i].Value)
		if err != nil {
			return err
		}
		row := extensions.LocalAttributeRow{
			ProcessExecutionId: resp.ProcessExecutionId,
			Key:                attributes[i].Key,
			Value:              valueBytes,
		}

		err = tx.InsertLocalAttribute(ctx, row)
		if err != nil {
			p.logger.Error("error on inserting local attribute", tag.Error(err))
			return err
		}
	}

	return nil
}

func (p sqlProcessStoreImpl) updateLocalAttributesIfNeeded(
	ctx context.Context, tx extensions.SQLTransaction,
	processExecutionId uuid.UUID,
	localAttributeToUpdate []xcapi.KeyValue,
) error {
	if len(localAttributeToUpdate) > 0 {
		for _, kv := range localAttributeToUpdate {
			valueBytes, err := data_models.FromEncodedObjectIntoBytes(&kv.Value)
			if err != nil {
				return err
			}
			row := extensions.LocalAttributeRow{
				ProcessExecutionId: processExecutionId,
				Key:                kv.Key,
				Value:              valueBytes,
			}
			err = tx.UpsertLocalAttribute(ctx, row)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
