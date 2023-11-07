// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: BUSL-1.1

package async

import (
	"github.com/gin-gonic/gin"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/config"
	"net/http"
)

type ginHandler struct {
	config config.Config
	logger log.Logger
	svc    Service
}

func newGinHandler(cfg config.Config, svc Service, logger log.Logger) *ginHandler {
	return &ginHandler{
		config: cfg,
		logger: logger,
		svc:    svc,
	}
}

func (h *ginHandler) NotifyImmediateTasks(c *gin.Context) {
	var req xdbapi.NotifyImmediateTasksRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		invalidRequestSchema(c)
		return
	}
	err := h.svc.NotifyPollingImmediateTask(req)
	if err != nil {
		invalidRequestForError(c, err)
		return
	}

	successRespond(c)
}

func (h *ginHandler) NotifyTimerTasks(c *gin.Context) {
	var req xdbapi.NotifyTimerTasksRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		invalidRequestSchema(c)
		return
	}
	err := h.svc.NotifyPollingTimerTask(req)
	if err != nil {
		invalidRequestForError(c, err)
		return
	}

	successRespond(c)
}

func successRespond(c *gin.Context) {
	c.JSON(http.StatusOK, map[string]string{
		"message": "success",
	})
}
func invalidRequestSchema(c *gin.Context) {
	c.JSON(http.StatusBadRequest, xdbapi.ApiErrorResponse{
		Detail: xdbapi.PtrString("invalid request schema"),
	})
}

func invalidRequestForError(c *gin.Context, err error) {
	c.JSON(http.StatusBadRequest, xdbapi.ApiErrorResponse{
		Detail: xdbapi.PtrString(err.Error()),
	})
}
