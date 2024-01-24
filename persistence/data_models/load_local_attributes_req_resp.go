// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package data_models

import (
	"github.com/xcherryio/apis/goapi/xcapi"
	"github.com/xcherryio/xcherry/common/uuid"
)

type (
	LoadLocalAttributesRequest struct {
		ProcessExecutionId uuid.UUID
		Request            xcapi.LoadLocalAttributesRequest
	}

	LoadLocalAttributesResponse struct {
		Response xcapi.LoadLocalAttributesResponse
	}
)
