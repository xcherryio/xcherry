// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: BUSL-1.1

package async

import (
	"github.com/gin-gonic/gin"
	"github.com/xcherryio/xcherry/common/log"
	"github.com/xcherryio/xcherry/config"
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
	var req xcapi.NotifyImmediateTasksRequest
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
	var req xcapi.NotifyTimerTasksRequest
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
	c.JSON(http.StatusBadRequest, xcapi.ApiErrorResponse{
		Detail: xcapi.PtrString("invalid request schema"),
	})
}

func invalidRequestForError(c *gin.Context, err error) {
	c.JSON(http.StatusBadRequest, xcapi.ApiErrorResponse{
		Detail: xcapi.PtrString(err.Error()),
	})
}
