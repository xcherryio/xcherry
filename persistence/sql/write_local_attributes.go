// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: BUSL-1.1

package sql

import (
	"context"
	"github.com/xcherryio/apis/goapi/xcapi"
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
			Namespace:          req.Namespace,
			ProcessId:          req.ProcessId,
		}

		err = tx.UpdateLocalAttribute(ctx, row)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p sqlProcessStoreImpl) updateLocalAttributesIfNeeded(
	ctx context.Context, tx extensions.SQLTransaction, config *data_models.InternalLocalAttributeConfig,
	processExecutionId uuid.UUID, namespace string, processId string,
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
				Namespace:          namespace,
				ProcessId:          processId,
			}
			err = tx.UpdateLocalAttribute(ctx, row)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
