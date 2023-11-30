// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: BUSL-1.1

package data_models

import "github.com/xcherryio/apis/goapi/xcapi"

type InternalLocalAttributeConfig struct {
	AttributeKeys map[string]bool `json:"attributeKeys"`
}

func getInternalLocalAttributeConfig(req xcapi.ProcessExecutionStartRequest) *InternalLocalAttributeConfig {
	if req.ProcessStartConfig != nil && req.ProcessStartConfig.LocalAttributeConfig != nil {
		attributeKeys := map[string]bool{}
		keyValues := req.ProcessStartConfig.LocalAttributeConfig.InitialWrite
		for i := range keyValues {
			attributeKeys[keyValues[i].Key] = true
		}
		return &InternalLocalAttributeConfig{
			AttributeKeys: attributeKeys,
		}
	}
	return nil
}
