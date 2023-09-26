package api

import (
	"encoding/json"
	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/config"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
)

type handler struct {
	config config.Config
	logger log.Logger
}

func newHandler(config config.Config, logger log.Logger) *handler {
	return &handler{
		config: config,
		logger: logger,
	}
}

func (h *handler) ApiV1ProcessStartPost(c *gin.Context) {
	var req xdbapi.ProcessExecutionStartRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		invalidRequestSchema(c)
		return
	}
	h.logger.Debug("received API request", tag.Value(h.toJson(req)))

	resp := gin.H{
		"message": "success!",
	}

	c.JSON(http.StatusOK, resp)
	return
}

func (h *handler) toJson(req any) string {
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
