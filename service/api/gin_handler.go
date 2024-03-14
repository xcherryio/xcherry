// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"encoding/json"
	"github.com/xcherryio/apis/goapi/xcapi"
	"github.com/xcherryio/xcherry/common/log"
	"github.com/xcherryio/xcherry/common/log/tag"
	"github.com/xcherryio/xcherry/config"
	"github.com/xcherryio/xcherry/persistence"
	"net/http"

	"github.com/gin-gonic/gin"
)

type ginHandler struct {
	config config.Config
	logger log.Logger
	svc    Service
}

func newGinHandler(
	cfg config.Config,
	processStore persistence.ProcessStore,
	visibilityStore persistence.VisibilityStore,
	logger log.Logger,
) *ginHandler {
	svc := NewServiceImpl(cfg, processStore, visibilityStore, logger)
	return &ginHandler{
		config: cfg,
		logger: logger,
		svc:    svc,
	}
}

func (h *ginHandler) StartProcess(c *gin.Context) {
	var req xcapi.ProcessExecutionStartRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		invalidRequestSchema(c)
		return
	}
	var errResp *ErrorWithStatus
	var resp *xcapi.ProcessExecutionStartResponse
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
	var req xcapi.ProcessExecutionStopRequest
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
	var req xcapi.ProcessExecutionDescribeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		invalidRequestSchema(c)
		return
	}
	var resp *xcapi.ProcessExecutionDescribeResponse
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

func (h *ginHandler) PublishToLocalQueue(c *gin.Context) {
	var req xcapi.PublishToLocalQueueRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		invalidRequestSchema(c)
		return
	}
	if len(req.GetMessages()) == 0 {
		invalidRequestSchema(c)
		return
	}

	var err *ErrorWithStatus
	h.logger.Debug("received PublishToLocalQueue API request", tag.Value(h.toJson(req)))
	defer func() {
		h.logger.Debug("responded PublishToLocalQueue API request", tag.Value(h.toJson(err)))
	}()

	err = h.svc.PublishToLocalQueue(c.Request.Context(), req)

	if err != nil {
		c.JSON(err.StatusCode, err.Error)
		return
	}

	c.JSON(http.StatusOK, struct{}{})
}

func (h *ginHandler) Rpc(c *gin.Context) {
	var req xcapi.ProcessExecutionRpcRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		invalidRequestSchema(c)
		return
	}

	if req.GetRpcName() == "" {
		invalidRequestSchema(c)
		return
	}

	var err *ErrorWithStatus
	h.logger.Debug("received Rpc API request", tag.Value(h.toJson(req)))
	defer func() {
		h.logger.Debug("responded Rpc API request", tag.Value(h.toJson(err)))
	}()

	resp, errResp := h.svc.Rpc(c.Request.Context(), req)

	if errResp != nil {
		c.JSON(errResp.StatusCode, errResp.Error)
		return
	}

	c.JSON(http.StatusOK, resp)
}

func (h *ginHandler) ListProcessExecutions(c *gin.Context) {
	var req xcapi.ListProcessExecutionsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		invalidRequestSchema(c)
		return
	}

	var resp *xcapi.ListProcessExecutionsResponse
	var errResp *ErrorWithStatus
	h.logger.Debug("received ListProcessExecutions API request", tag.Value(h.toJson(req)))
	defer func() {
		h.logger.Debug("responded ListProcessExecutions API request", tag.Value(h.toJson(resp)), tag.Value(h.toJson(errResp)))
	}()

	resp, errResp = h.svc.ListProcessExecutions(c.Request.Context(), req)

	if errResp != nil {
		c.JSON(errResp.StatusCode, errResp.Error)
		return
	}

	c.JSON(http.StatusOK, resp)
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
	c.JSON(http.StatusBadRequest, xcapi.ApiErrorResponse{
		Details: xcapi.PtrString("invalid request schema"),
	})
}
