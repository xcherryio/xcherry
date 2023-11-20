// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: BUSL-1.1

package data_models

import (
	"encoding/json"
	"github.com/xcherryio/apis/goapi/xcapi"
)

func getInternalGlobalAttributeConfig(req xcapi.ProcessExecutionStartRequest) *InternalGlobalAttributeConfig {
	if req.ProcessStartConfig != nil && req.ProcessStartConfig.GlobalAttributeConfig != nil {
		primaryKeys := map[string]xcapi.TableColumnValue{}
		for _, cfg := range req.ProcessStartConfig.GlobalAttributeConfig.TableConfigs {
			primaryKeys[cfg.TableName] = cfg.PrimaryKey
		}
		return &InternalGlobalAttributeConfig{
			TablePrimaryKeys: primaryKeys,
		}
	}
	return nil
}

func FromEncodedObjectIntoBytes(obj *xcapi.EncodedObject) ([]byte, error) {
	return json.Marshal(obj)
}

func BytesToEncodedObject(bytes []byte) (xcapi.EncodedObject, error) {
	var obj xcapi.EncodedObject
	err := json.Unmarshal(bytes, &obj)
	return obj, err
}

func FromCommandRequestToBytes(request xcapi.CommandRequest) ([]byte, error) {
	return json.Marshal(request)
}

func BytesToCommandRequest(bytes []byte) (xcapi.CommandRequest, error) {
	if bytes == nil {
		return xcapi.CommandRequest{}, nil
	}

	var request xcapi.CommandRequest
	err := json.Unmarshal(bytes, &request)
	return request, err
}
