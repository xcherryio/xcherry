// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package async

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/xcherryio/apis/goapi/xcapi"
	"github.com/xcherryio/xcherry/common/log"
	"github.com/xcherryio/xcherry/config"
	"net/http"
)

type ginHandler struct {
	config     config.Config
	logger     log.Logger
	svc        Service
	membership Membership
}

func newGinHandler(cfg config.Config, svc Service, membership Membership, logger log.Logger) *ginHandler {
	return &ginHandler{
		config:     cfg,
		logger:     logger,
		svc:        svc,
		membership: membership,
	}
}

func (h *ginHandler) NotifyImmediateTasks(c *gin.Context) {
	var req xcapi.NotifyImmediateTasksRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		invalidRequestSchema(c)
		return
	}

	if h.config.AsyncService.Mode == config.AsyncServiceModeCluster {
		targetServerAddress := h.membership.GetServerAddressFor(req.ShardId)
		if targetServerAddress != h.membership.GetServerAddress() {
			h.logger.Info(fmt.Sprintf("NotifyRemoteImmediateTaskAsyncInCluster: %s -> %s", h.membership.GetServerAddress(), targetServerAddress))

			h.svc.NotifyRemoteImmediateTaskAsyncInCluster(req, targetServerAddress)
			successRespond(c)
			return
		}
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

	if h.config.AsyncService.Mode == config.AsyncServiceModeCluster {
		targetServerAddress := h.membership.GetServerAddressFor(req.ShardId)
		if targetServerAddress != h.membership.GetServerAddress() {
			h.logger.Info(fmt.Sprintf("NotifyRemoteImmediateTaskAsyncInCluster: %s -> %s", h.membership.GetServerAddress(), targetServerAddress))

			h.svc.NotifyRemoteTimerTaskAsyncInCluster(req, targetServerAddress)
			successRespond(c)
			return
		}
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
		Details: xcapi.PtrString("invalid request schema"),
	})
}

func invalidRequestForError(c *gin.Context, err error) {
	c.JSON(http.StatusBadRequest, xcapi.ApiErrorResponse{
		Details: xcapi.PtrString(err.Error()),
	})
}
