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

func (h *ginHandler) NotifyWorkerTasks(c *gin.Context) {
	var req xdbapi.NotifyWorkerTasksRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		invalidRequestSchema(c)
		return
	}
	err := h.svc.NotifyPollingWorkerTask(req)
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
