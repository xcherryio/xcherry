// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package data_models

import "github.com/xcherryio/apis/goapi/xcapi"

type (
	AppDatabaseReadRequest struct {
		AppDatabaseConfig InternalAppDatabaseConfig
		Request           xcapi.AppDatabaseReadRequest
	}

	AppDatabaseReadResponse struct {
		Response xcapi.AppDatabaseReadResponse
	}
)
