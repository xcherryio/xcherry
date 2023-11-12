// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: BUSL-1.1

package data_models

import (
	"encoding/json"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
)

func getInternalGlobalAttributeConfig(req xdbapi.ProcessExecutionStartRequest) *InternalGlobalAttributeConfig {
	if req.ProcessStartConfig != nil && req.ProcessStartConfig.GlobalAttributeConfig != nil {
		primaryKeys := map[string]xdbapi.TableColumnValue{}
		for _, cfg := range req.ProcessStartConfig.GlobalAttributeConfig.TableConfigs {
			primaryKeys[cfg.TableName] = cfg.PrimaryKey
		}
		return &InternalGlobalAttributeConfig{
			TablePrimaryKeys: primaryKeys,
		}
	}
	return nil
}

func FromEncodedObjectIntoBytes(obj *xdbapi.EncodedObject) ([]byte, error) {
	return json.Marshal(obj)
}

func BytesToEncodedObject(bytes []byte) (xdbapi.EncodedObject, error) {
	var obj xdbapi.EncodedObject
	err := json.Unmarshal(bytes, &obj)
	return obj, err
}

func FromCommandRequestToBytes(request xdbapi.CommandRequest) ([]byte, error) {
	return json.Marshal(request)
}

func BytesToCommandRequest(bytes []byte) (xdbapi.CommandRequest, error) {
	if bytes == nil {
		return xdbapi.CommandRequest{}, nil
	}

	var request xdbapi.CommandRequest
	err := json.Unmarshal(bytes, &request)
	return request, err
}
