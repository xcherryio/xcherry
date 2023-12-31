// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package data_models

import (
	"encoding/json"
	"github.com/xcherryio/apis/goapi/xcapi"
)

func FromEncodedObjectIntoBytes(obj *xcapi.EncodedObject) ([]byte, error) {
	if obj == nil {
		// set this as default for
		// https://github.com/xcherryio/xcherry/issues/100
		return json.Marshal(xcapi.NewEncodedObject("", ""))
	}
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
	if len(bytes) == 0 {
		return xcapi.CommandRequest{}, nil
	}

	var request xcapi.CommandRequest
	err := json.Unmarshal(bytes, &request)
	return request, err
}
