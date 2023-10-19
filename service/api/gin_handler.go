// Copyright 2023 XDBLab organization
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"encoding/json"
	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/persistence"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
)

type ginHandler struct {
	config config.Config
	logger log.Logger
	svc    Service
}

func newGinHandler(cfg config.Config, store persistence.ProcessStore, logger log.Logger) *ginHandler {
	svc := NewServiceImpl(cfg, store, logger)
	return &ginHandler{
		config: cfg,
		logger: logger,
		svc:    svc,
	}
}

func (h *ginHandler) StartProcess(c *gin.Context) {
	var req xdbapi.ProcessExecutionStartRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		invalidRequestSchema(c)
		return
	}
	var errResp *ErrorWithStatus
	var resp *xdbapi.ProcessExecutionStartResponse
	h.logger.Debug("received StartProcess API request", tag.Value(h.toJson(req)))
	defer func() {
		h.logger.Debug("responded StartProcess API request", tag.Value(h.toJson(resp)), tag.Value(h.toJson(errResp)))
	}()

	resp, errResp = h.svc.StartProcess(c.Request.Context(), req)

	if errResp != nil {
		c.JSON(errResp.StatusCode, errResp.Error)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (h *ginHandler) StopProcess(c *gin.Context) {
	var req xdbapi.ProcessExecutionStopRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		invalidRequestSchema(c)
		return
	}
	var err *ErrorWithStatus
	h.logger.Debug("received StopProcess API request", tag.Value(h.toJson(req)))
	defer func() {
		h.logger.Debug("responded StopProcess API request", tag.Value(h.toJson(err)))
	}()

	err = h.svc.StopProcess(c.Request.Context(), req)

	if err != nil {
		c.JSON(err.StatusCode, err.Error)
		return
	}

	c.JSON(http.StatusOK, struct{}{})
}

func (h *ginHandler) DescribeProcess(c *gin.Context) {
	var req xdbapi.ProcessExecutionDescribeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		invalidRequestSchema(c)
		return
	}
	var resp *xdbapi.ProcessExecutionDescribeResponse
	var errResp *ErrorWithStatus

	h.logger.Debug("received DescribeProcess API request", tag.Value(h.toJson(req)))
	defer func() {
		h.logger.Debug("responded DescribeProcess API request", tag.Value(h.toJson(resp)), tag.Value(h.toJson(errResp)))
	}()

	resp, errResp = h.svc.DescribeLatestProcess(c.Request.Context(), req)

	if errResp != nil {
		c.JSON(errResp.StatusCode, errResp.Error)
		return
	}

	c.JSON(http.StatusOK, resp)
	return
}

func (h *ginHandler) toJson(req any) string {
	str, err := json.Marshal(req)
	if err != nil {
		h.logger.Error("error when serializing request", tag.Error(err), tag.DefaultValue(req))
		return ""
	}
	return string(str)
}

func invalidRequestSchema(c *gin.Context) {
	c.JSON(http.StatusBadRequest, xdbapi.ApiErrorResponse{
		Detail: xdbapi.PtrString("invalid request schema"),
	})
}
